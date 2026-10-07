/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::iter;

use buck2_core::soft_error;
use buck2_data::VersionControlRevision;
use buck2_error::ErrorTag;
use buck2_events::dispatch::EventDispatcher;
use buck2_events::dispatch::with_dispatcher_async;
use buck2_fs::async_fs_util;
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use buck2_util::properly_reaped_child::reap_on_drop_command;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio_stream::StreamExt;

/// Spawn tasks to collect version control information
/// and return a droppable handle that will cancel them on drop.
pub(crate) fn spawn_version_control_collector(
    dispatch: EventDispatcher,
    repo_root: AbsNormPathBuf,
) -> AbortOnDropHandle {
    let context_dispatch = dispatch.clone();
    let handle = tokio::spawn(with_dispatcher_async(context_dispatch, async move {
        let mut tasks = FuturesUnordered::<BoxFuture<VersionControlRevision>>::new();

        tasks.push(Box::pin(create_revision_data(
            &repo_root,
            RevisionDataType::CurrentRevision,
        )));
        tasks.push(Box::pin(create_revision_data(
            &repo_root,
            RevisionDataType::Status,
        )));

        while let Some(event) = tasks.next().await {
            if let Some(error) = &event.command_error {
                soft_error!(
                    "spawn_version_control_collector_failed",
                    buck2_error::buck2_error!(buck2_error::ErrorTag::Input, "{}", error),
                    quiet: true
                )
                .ok();
            }

            dispatch.instant_event(event);
        }
    }));

    AbortOnDropHandle { handle }
}

/// Abort the underlying task on drop.
pub(crate) struct AbortOnDropHandle {
    pub handle: tokio::task::JoinHandle<()>,
}

impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone, Copy, Debug)]
enum RepoVcs {
    Hg,
    Git,
    Jj,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
enum RevisionDataType {
    CurrentRevision,
    Status,
}

async fn create_revision_data(
    repo_root: &AbsNormPathBuf,
    revision_type: RevisionDataType,
) -> buck2_data::VersionControlRevision {
    let mut revision = buck2_data::VersionControlRevision::default();
    match repo_type(repo_root).await {
        RepoVcs::Hg => create_hg_data(&mut revision, revision_type, repo_root).await,
        RepoVcs::Git => create_git_data(&mut revision, revision_type, repo_root).await,
        RepoVcs::Jj => create_jj_data(&mut revision, revision_type, repo_root).await,
        RepoVcs::Unknown => {
            revision.command_error = Some("Unknown repository type".to_owned());
        }
    }
    revision
}

async fn create_hg_data(
    revision: &mut buck2_data::VersionControlRevision,
    revision_type: RevisionDataType,
    repo_root: &AbsNormPathBuf,
) {
    match revision_type {
        RevisionDataType::CurrentRevision => get_hg_revision(revision, repo_root).await,
        RevisionDataType::Status => get_hg_status(revision, repo_root).await,
    }
}

async fn get_hg_revision(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    // The contents of dirstate may be arbitrarily large, but the id is always
    // in the first 20 bytes, so we only need to read the first 20 bytes
    let mut buffer = [0; 20];
    let dirstate = repo_root
        .join(ForwardRelativePath::new(".hg").unwrap())
        .join(ForwardRelativePath::new("dirstate").unwrap());

    if let Err(e) = async_fs_util::read(&dirstate, &mut buffer).await {
        revision.command_error = Some(format!(
            "Failed to read the first 20 bytes of {}: {:#}",
            dirstate.display(),
            e.categorize_internal(),
        ));
        return;
    }

    let curr_revision = buffer.iter().map(|b| format!("{b:02x}")).collect();
    revision.hg_revision = Some(curr_revision);
}

async fn get_hg_status(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    // `hg status` returns if there are any local changes
    match run_hg(repo_root, &["status"]).await {
        Ok(stdout) => revision.has_local_changes = Some(!stdout.trim().is_empty()),
        Err(e) => revision.command_error = Some(format!("{e:#}")),
    };
}

fn display_command(program: &str, args: &[&str]) -> String {
    shlex::try_join(iter::once(program).chain(args.iter().copied()))
        .unwrap_or_else(|_| format!("{program} <argument contains NUL>"))
}

async fn run_hg(repo_root: &AbsNormPathBuf, args: &[&str]) -> buck2_error::Result<String> {
    let Some(repo_root) = repo_root.as_path().to_str() else {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Repository root is not valid utf8: {}",
            repo_root.as_path().display()
        ));
    };

    let mut full_args = vec!["--cwd", repo_root];
    full_args.extend_from_slice(args);
    let command = display_command("hg", &full_args);

    let output = match reap_on_drop_command("hg", &full_args, Some(&[("HGPLAIN", "1")])) {
        Ok(command) => command.output().await,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "reap_on_drop_command for `{command}` failed: {e}"
            ));
        }
    };

    let result = match output {
        Ok(result) => result,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Command `{command}` failed with error: {e:?}"
            ));
        }
    };

    if !result.status.success() {
        let stderr = match std::str::from_utf8(&result.stderr) {
            Ok(s) => s,
            Err(e) => {
                return Err(buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "`{command}` stderr is not utf8: {e}"
                ));
            }
        };
        return Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error code {}; stderr: {stderr}",
            result.status
        ));
    }

    match std::str::from_utf8(&result.stdout) {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "`{command}` stdout is not utf8: {e}"
        )),
    }
}

async fn create_git_data(
    revision: &mut buck2_data::VersionControlRevision,
    revision_type: RevisionDataType,
    repo_root: &AbsNormPathBuf,
) {
    match revision_type {
        RevisionDataType::CurrentRevision => get_git_revision(revision, repo_root).await,
        RevisionDataType::Status => get_git_status(revision, repo_root).await,
    }
}

async fn get_git_revision(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    // `git rev-parse HEAD` resolves the current commit hash. This handles
    // packed refs, detached HEADs, etc. without us having to parse `.git`.
    match run_git(repo_root, &["rev-parse", "HEAD"]).await {
        Ok(stdout) => revision.git_revision = Some(stdout.trim().to_owned()),
        Err(e) => revision.command_error = Some(format!("{e:#}")),
    }
}

async fn get_git_status(
    revision: &mut buck2_data::VersionControlRevision,
    repo_root: &AbsNormPathBuf,
) {
    // `git status --porcelain` prints one line per change; empty output means
    // there are no local changes. Note that this counts untracked files as local
    // changes (they show up as `??` lines), matching the behavior of `hg status`.
    match run_git(repo_root, &["status", "--porcelain"]).await {
        Ok(stdout) => revision.has_local_changes = Some(!stdout.trim().is_empty()),
        Err(e) => revision.command_error = Some(format!("{e:#}")),
    }
}

/// Run a `git` command rooted at `repo_root`, returning its stdout on success or
/// an error message describing the failure.
async fn run_git(repo_root: &AbsNormPathBuf, args: &[&str]) -> buck2_error::Result<String> {
    let Some(repo_root) = repo_root.as_path().to_str() else {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Repository root is not valid utf8: {}",
            repo_root.as_path().display()
        ));
    };

    // `-C <repo_root>` makes git operate on the repository regardless of the
    // daemon's current working directory.
    let mut full_args = vec!["-C", repo_root];
    full_args.extend_from_slice(args);
    let command = display_command("git", &full_args);

    // `GIT_OPTIONAL_LOCKS=0` prevents git from taking the index lock or
    // refreshing the index on disk, so we don't contend with a concurrent user
    // `git` invocation (e.g. for `git status`).
    let output = match reap_on_drop_command("git", &full_args, Some(&[("GIT_OPTIONAL_LOCKS", "0")]))
    {
        Ok(command) => command.output().await,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "reap_on_drop_command for `{command}` failed: {e}"
            ));
        }
    };

    let result = match output {
        Ok(result) => result,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Command `{command}` failed with error: {e:?}"
            ));
        }
    };

    if !result.status.success() {
        let stderr = match std::str::from_utf8(&result.stderr) {
            Ok(s) => s,
            Err(e) => {
                return Err(buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "`{command}` stderr is not utf8: {e}"
                ));
            }
        };
        return Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error code {}; stderr: {stderr}",
            result.status
        ));
    }

    match std::str::from_utf8(&result.stdout) {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "`{command}` stdout is not utf8: {e}"
        )),
    }
}

async fn create_jj_data(
    revision: &mut VersionControlRevision,
    revision_type: RevisionDataType,
    repo_root: &AbsNormPathBuf,
) {
    match revision_type {
        RevisionDataType::CurrentRevision => get_jj_revision(revision, repo_root).await,
        RevisionDataType::Status => get_jj_status(repo_root).await,
    }
}

/// Get all the necessary information for Buck2 for a jj repository.
async fn get_jj_revision(revision: &mut VersionControlRevision, repo_root: AbsNormPathBuf) {
    // Get the change id and the commit hash of the jj revision. This respects people using
    // Megamerges (octopus merges with more than one parent).
    const JJ_CHANGE_ID_AND_REVISION_ID_TEMPLATE: &str = r#"change_id ++ "\t" if(parents.len() > 1, parents.map(|p| p.commit_id).join(","), commit_id)"#;
    // Get the workspace name and its repo root so we can determine if we're in the workspace.
    const WORKSPACE_NAME_ROOT_TEMPLATE: &str = r#"name ++ "\t" ++ root"#;

    revision.is_colocated = async_fs_util::metadata(repo_root.join(".git"))
        .await
        .is_ok_and(|dir| dir.is_dir());

    let vcs_info = match run_jj(
        &repo_root,
        [
            "log",
            "-G",
            "-r @",
            "-T",
            JJ_CHANGE_ID_AND_REVISION_ID_TEMPLATE,
        ],
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            revision.command_error = format!("{e:#}");
            return;
        }
    };
    let workspace_info = match run_jj(
        &repo_root,
        ["log", "-G", "-r @", "-T", WORKSPACE_NAME_ROOT_TEMPLATE],
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            revision.command_error = format!("{e:#}");
            return;
        }
    };

    let (Some(change_id_and_revisions)) = vcs_info.split_once('\t');
    let revisions: Vec<&str> = change_id_and_revisions.1.split(',').iter().collect();
    let revision_id = if revisions.len() == 0 {
        revisions[0]
    } else {
        let s = revisions.iter().collect::<String>();
        // Allow the revision_id to represent up to 3 revisions for megamerges
        // 40 characters for the hash and a comma, 3 * 41 = 123.
        s.truncate(128);
        s
    };
    revision.jj_change_id = Some(change_id_and_revisions.0);
    revision.jj_revision_id = Some(revision_id);
    let workspaces: Vec<(&str, &str)> = workspace_info
        .lines()
        .flat_map(|line| line.split_once('\t'))
        .collect();

    let Some(repo_root) = repo_root.as_path().to_str() else {
        revision.command_error = Some(format!(
            "Repository root is not valid utf8: {}",
            repo_root.as_path().display()
        ));
    };

    let matched = workspaces
        .iter()
        .find(|(_, root)| repo_root == Some(*root))
        // If we cannot find the the associated workspace then fallback to "default".
        .map_or("default".to_owned(), |(_, name)| name.to_owned());

    revision.jj_workspace = Some(matched);
}

async fn get_jj_status(revision: &mut VersionControlRevision, repo_root: &AbsNormPathBuf) {
    const TEMPLATE: &str = r#"if(empty,"clean","dirty")"#;

    let str = match run_jj(repo_root, &["-G -r @ -T", TEMPLATE]).await {
        Ok(str) => str.trim(),
        Err(e) => revision.command_error = Some(format!("{e:#}")),
    };
    revision.has_local_changes = match str {
        "dirty" => true,
        "clean" => false,
        _ => panic!("jj template returned an unexpected value"),
    };
}

async fn run_jj(repo_root: &AbsNormPathBuf, args: &[&str]) -> buck2_error::Result<String> {
    let Some(repo_root) = repo_root.as_path().to_str() else {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Repository root is not valid utf8: {}",
            repo_root.as_path().display()
        ));
    };

    // `-R <repo_root>` makes jj operate on the repository regardless of the
    // daemon's current working directory. Also jj doesn't have an equivalent to HGPLAIN=1 yet, so
    // disable colors and the pager manually.
    let mut full_args = vec![
        "-R",
        repo_root,
        "--ignore-working-copy",
        "--color=never",
        "--no-pager",
    ];
    full_args.extend_from_slice(args);
    let command = display_command("jj", &full_args);

    let output = match reap_on_drop_command("jj", &full_args, None) {
        Ok(command) => command.output().await,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "reap_on_drop_command for `{command}` failed: {e}"
            ));
        }
    };

    let result = match output {
        Ok(result) => result,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Command `{command}` failed with error: {e:?}"
            ));
        }
    };

    if !result.status.success() {
        let stderr = match std::str::from_utf8(&result.stderr) {
            Ok(s) => s,
            Err(e) => {
                return Err(buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "`{command}` stderr is not utf8: {e}"
                ));
            }
        };
        return Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error code {}; stderr: {stderr}",
            result.status
        ));
    }

    match std::str::from_utf8(&result.stdout) {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "`{command}` stdout is not utf8: {e}"
        )),
    }
}

async fn repo_type(repo_root: &AbsNormPathBuf) -> RepoVcs {
    let (hg_metadata, git_metadata, jj_metadata) = tokio::join!(
        async_fs_util::metadata(repo_root.join(ForwardRelativePath::new(".hg").unwrap())),
        async_fs_util::metadata(repo_root.join(ForwardRelativePath::new(".git").unwrap())),
        async_fs_util::metadata(repo_root.join(ForwardRelativePath::new(".jj").unwrap()))
    );

    let is_hg = hg_metadata.is_ok_and(|output| output.is_dir());
    // `.git` can be a symlink or a file with contents like:
    //
    //     gitdir: /home/dog/buck2/.git/worktrees/buck3
    let is_git = git_metadata.is_ok();
    // `.jj` is a directory either containing the primary workspaces operation log and more or
    // only containing a file pointer linking to the primary workspaces.
    let is_jj = jj_metadata.is_ok_and(|dir| dir.is_dir());

    if is_hg {
        RepoVcs::Hg
    } else if is_jj {
        RepoVcs::Jj
    } else if is_git {
        RepoVcs::Git
    } else {
        RepoVcs::Unknown
    }
}

#[cfg(test)]
mod tests {
    use std::str::pattern::Pattern;

    use super::*;

    const JJ_TEST_USER: &str = r#"user.name="Buck Test" user.email="buck-test@example.com""#;
    /// Run a `jj` command in `repo_root`, panicking on failure. Used to set up
    /// and inspect test repositories.
    async fn jj(repo_root: &AbsNormPathBuf, args: &[&str]) -> String {
        run_jj(repo_root, args)
            .await
            .unwrap_or_else(|e| panic!("jj command failed: {e}"))
    }

    /// Initialize an empty jj repo with a deterministic identity so that commits
    /// succeed even in environments without a global git config.
    async fn init_jj_repo(repo_root: &AbsNormPathBuf, colocate: bool) {
        let colocate = if colocate {
            "--colocate"
        } else {
            "--no-colocate"
        };
        jj(repo_root, &["git init", colocate]).await;
        jj(repo_root, &["config", "set", "user.name", "Buck Test"]).await;
        jj(
            repo_root,
            &["config", "set", "user.email", "buck-test@example.com"],
        )
        .await;
    }

    /// Run a `git` command in `repo_root`, panicking on failure. Used to set up
    /// and inspect test repositories.
    async fn git(repo_root: &AbsNormPathBuf, args: &[&str]) -> String {
        run_git(repo_root, args)
            .await
            .unwrap_or_else(|e| panic!("git command failed: {e}"))
    }

    /// Initialize an empty git repo with a deterministic identity so that commits
    /// succeed even in environments without a global git config.
    async fn init_git_repo(repo_root: &AbsNormPathBuf) {
        git(repo_root, &["init", "-q"]).await;
        git(repo_root, &["config", "user.name", "Buck Test"]).await;
        git(
            repo_root,
            &["config", "user.email", "buck-test@example.com"],
        )
        .await;
        git(repo_root, &["config", "commit.gpgsign", "false"]).await;
    }

    fn temp_repo_root(temp_dir: &tempfile::TempDir) -> buck2_error::Result<AbsNormPathBuf> {
        AbsNormPathBuf::try_from(temp_dir.path().to_path_buf())
    }

    fn write_file(repo_root: &AbsNormPathBuf, name: &str, contents: &str) {
        let repo_path = std::path::Path::new(repo_root.as_path().to_str().unwrap());
        std::fs::write(repo_path.join(name), contents).unwrap();
    }

    #[tokio::test]
    async fn test_get_git_revision_matches_head() -> buck2_error::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_git_repo(&repo_root).await;
        write_file(&repo_root, "file.txt", "hello");
        git(&repo_root, &["add", "file.txt"]).await;
        git(&repo_root, &["commit", "-q", "-m", "initial"]).await;

        let head = git(&repo_root, &["rev-parse", "HEAD"]).await;
        let head = head.trim();

        let mut revision = VersionControlRevision::default();
        get_git_revision(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.git_revision.as_deref(), Some(head));
        let git_revision = revision.git_revision.unwrap();
        assert_eq!(git_revision.len(), 40);
        assert!(git_revision.chars().all(|c| c.is_ascii_hexdigit()));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_git_status_clean() -> buck2_error::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_git_repo(&repo_root).await;
        write_file(&repo_root, "file.txt", "hello");
        git(&repo_root, &["add", "file.txt"]).await;
        git(&repo_root, &["commit", "-q", "-m", "initial"]).await;

        let mut revision = VersionControlRevision::default();
        get_git_status(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_jj_revision_matches_head() -> buck2_error::Result<()> {
        if run_jj(&repo_root, &["version"]).await.is_err() {
            eprintln!("skipping: no jj on this machine");
            return Ok(());
        }
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_jj_repo(&repo_root, false).await;
        write_file(&repo_root, "file.txt", "hello");
        jj(&repo_root, &["status"]).await;
        jj(&repo_root, &["commit", "-m", "initial"]).await;

        let change_id = jj(&repo_root, &["log", "-r @", "-G", "-T", "change_id"]).await;
        let change_id = head.trim();

        let commit_id = jj(&repo_root, &["log", "-r @", "-G", "-T", "commit_id"]).await;
        commit_id = head.trim();

        let mut revision = VersionControlRevision::default();
        get_jj_revision(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.jj_change_id.as_deref(), Some(change_id));
        assert_eq!(revision.jj_revision_id.as_deref(), Some(commit_id));
        let jj_revision = revision.jj_revision.unwrap();
        let jj_change = revision.jj_change_id.unwrap();
        assert_eq!(jj_revision.len(), 40);
        assert_eq!(jj_change.len(), 32);
        assert!(jj_change.chars().all(|c| 'k'..'z'.is_contained_in(c)));
        assert!(jj_revision.chars().all(|c| c.is_ascii_hexdigit()));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_jj_status_clean() -> buck2_error::Result<()> {
        if run_jj(&repo_root, &["version"]).await.is_err() {
            eprintln!("skipping: no jj on this machine");
            return Ok(());
        }
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_jj_repo(&repo_root).await;
        write_file(&repo_root, "file.txt", "hello");
        jj(&repo_root, &["status"]).await;
        jj(&repo_root, &["commit", "-m", "initial"]).await;

        let mut revision = VersionControlRevision::default();
        get_jj_status(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(false));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_jj_status_dirty() -> buck2_error::Result<()> {
        if run_jj(&repo_root, &["version"]).await.is_err() {
            eprintln!("skipping: no jj on this machine");
            return Ok(());
        }
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_jj_repo(&repo_root).await;
        write_file(&repo_root, "file.txt", "hello");
        jj(&repo_root, &["status"]).await;
        jj(&repo_root, &["commit", "-m", "initial"]).await;
        write_file(&repo_root, "file.txt", "world");
        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(false));

        // Snapshot the working copy marking it as dirty.
        jj(&repo_root, &["status"]).await;
        let mut revision = VersionControlRevision::default();
        get_jj_status(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(true));
        Ok(())
    }

    #[tokio::test]
    async fn test_jj_colocation_detection() -> buck2_error::Result<()> {
        if run_jj(&repo_root, &["version"]).await.is_err() {
            eprintln!("skipping: no jj on this machine");
            return Ok(());
        }
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        init_jj_repo(&repo_root, true).await;
        write_file(&repo_root, "file.txt", "hello");
        jj(&repo_root, &["status"]).await;

        let mut revision = VersionControlRevision::default();
        get_jj_status(&mut revision, &repo_root).await;

        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(false));
        assert_eq!(revision.is_colocated, Some(true));
        assert_eq!(revision.jj_workspace, Some("default"));
        Ok(())
    }

    #[tokio::test]
    async fn test_repo_type_is_scoped_to_repo_root() -> buck2_error::Result<()> {
        let hg_dir = tempfile::tempdir()?;
        std::fs::create_dir(hg_dir.path().join(".hg"))?;
        let hg_root = temp_repo_root(&hg_dir)?;

        let git_dir = tempfile::tempdir()?;
        std::fs::create_dir(git_dir.path().join(".git"))?;
        let git_root = temp_repo_root(&git_dir)?;

        let jj_dir = tempfile::tempdir()?;
        std::fs::create_dir(git_dir.path().join(".jj"))?;
        let jj_root = temp_repo_root(&git_dir)?;

        assert!(matches!(repo_type(&hg_root).await, RepoVcs::Hg));
        assert!(matches!(repo_type(&git_root).await, RepoVcs::Git));
        assert!(matches!(repo_type(&jj_root).await, RepoVcs::Jj));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_hg_status_uses_repo_root() -> buck2_error::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let repo_root = temp_repo_root(&temp_dir)?;
        // Not every machine has an hg, GitHub's macOS and Windows runners don't.
        if run_hg(&repo_root, &["version"]).await.is_err() {
            eprintln!("skipping: no hg on this machine");
            return Ok(());
        }
        run_hg(&repo_root, &["init"])
            .await
            .expect("initializing the test hg repository should succeed");

        let mut revision = VersionControlRevision::default();
        get_hg_status(&mut revision, &repo_root).await;
        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(false));

        write_file(&repo_root, "untracked.txt", "changed");
        let mut revision = VersionControlRevision::default();
        get_hg_status(&mut revision, &repo_root).await;
        assert_eq!(revision.command_error, None);
        assert_eq!(revision.has_local_changes, Some(true));
        Ok(())
    }
}
