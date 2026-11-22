//! Talking to the build server: lane allocation, upload, lease, compile.
//!
//! # The concurrency contract
//!
//! Several invocations may build on one server at the same time, and none of
//! them waits for another. Four properties enforce that, and each one lives here
//! rather than in a runbook:
//!
//! 1. **A lane per concurrent build.** [`allocate`] picks a free lane on the
//!    server (`default`, then `default-2`, ...). Different lanes share nothing
//!    but the remote `CARGO_HOME`, which is content addressed and safe.
//! 2. **Uploads are private.** Sources go to a per-invocation staging directory,
//!    so two uploads can never rewrite each other, and a build that has to move
//!    to another lane reuses the same upload.
//! 3. **A lane is mutated only under its lease.** One ssh session takes an
//!    `flock` on `{lane_root}.lock`, mirrors the stage into the lane, and runs
//!    cargo. Contention is *resolved by moving to another lane*, not by queueing.
//! 4. **The CPU is partitioned, not oversubscribed.** Before compiling, a build
//!    counts how many lanes are already leased and exports a `CARGO_BUILD_JOBS`
//!    share of `cores / (others + 1)`. An explicit `CARGO_BUILD_JOBS` always wins.
//!
//! The toolchain is selected per invocation with `RUSTUP_TOOLCHAIN`; the
//! server's global rustup default is never written.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};
use log::{debug, info};

use crate::config::{shell_quote, shell_quote_literal, validate_exclude_pattern, Remote};
use crate::lane;

/// Exit code a remote build uses when the lane it was given is already leased.
///
/// `EX_TEMPFAIL` from `sysexits.h`: the build did not fail, it never started.
/// The client answers it by allocating another lane, not by waiting.
pub const BUSY_EXIT: u8 = 75;

/// Everything needed to move sources up and artifacts back down.
pub struct Session<'a> {
    pub remote: &'a Remote,
    /// Local directory whose contents get uploaded.
    pub project_root: &'a Path,
    /// Local workspace base, may sit below `project_root`.
    pub workspace_root: &'a Path,
    /// Lane this invocation owns.
    pub lane: String,
    /// Hash of the local checkout, which names this project's trees.
    pub project_hash: String,
    /// Remote directory that mirrors `project_root` in this lane.
    pub build_root: String,
    /// Remote directory that mirrors `workspace_root`.
    pub remote_workspace_root: String,
    /// Remote directory the cargo command runs in.
    pub remote_crate_dir: String,
    /// Local directory cargo would put `target` in.
    pub local_target_dir: PathBuf,
    /// Remote counterpart of `local_target_dir`, when it can be derived.
    pub remote_target_dir: Option<String>,
    /// Per-invocation staging directory, outside every lane.
    pub stage_root: String,
    /// Token that makes this invocation's staging directory unique.
    pub run_id: String,
    /// Wait for the lane instead of reporting [`BUSY_EXIT`].
    ///
    /// True only when the caller named a lane, which is a promise that this tree
    /// is the one they want; an automatically allocated lane is never waited
    /// for, it is replaced.
    pub wait_for_lane: bool,
    /// Patterns to keep out of the upload.
    pub upload_excludes: Vec<String>,
    pub transfer_hidden: bool,
    pub no_transfer_git: bool,
    /// Seconds to wait for a named lane before giving up.
    pub lock_timeout: u64,
    pub verbose: bool,
}

/// A lane the server reported as free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    pub lane: String,
}

/// Ask the build server which lane of this project is free right now.
///
/// Returns `Ok(None)` when every candidate lane is leased, which is the only
/// situation in which the caller has to decide between waiting and failing.
///
/// The probe releases each lock as it goes, so the answer is a hint rather than
/// a reservation: two clients can be told the same lane a moment apart. The
/// build itself re-takes the lease without waiting and answers [`BUSY_EXIT`], at
/// which point the client allocates again. That is what keeps the common path
/// free of a lock server, and the race bounded instead of open.
pub fn allocate(
    remote: &Remote,
    project_hash: &str,
    base: &str,
    max_lanes: usize,
) -> Result<Option<Allocation>> {
    let script = alloc_script(remote, project_hash, base, max_lanes);

    let mut cmd = Command::new("ssh");
    cmd.args(remote.ssh_args());
    if !remote
        .ssh_options
        .iter()
        .any(|option| option.starts_with("LogLevel="))
    {
        cmd.arg("-o").arg("LogLevel=ERROR");
    }
    cmd.arg(&remote.host).arg(script);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let output = cmd
        .output()
        .with_context(|| format!("failed to ask {} for a build lane", remote.host))?;
    if !output.status.success() {
        bail!(
            "could not allocate a build lane on {}: {}",
            remote.host,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lane_name = stdout
        .lines()
        .find_map(|line| line.strip_prefix("LANE="))
        .unwrap_or("")
        .trim()
        .to_string();

    if lane_name.is_empty() {
        debug!("every lane reported busy: {stdout}");
        return Ok(None);
    }
    lane::validate_lane(&lane_name).with_context(|| {
        format!(
            "the build server returned an unusable lane name {lane_name:?}; \
             is its `flock` working?"
        )
    })?;
    Ok(Some(Allocation { lane: lane_name }))
}

/// The script [`allocate`] runs. Split out so it can be asserted in tests.
///
/// The candidate names are generated in Rust so the overflow naming lives in one
/// place ([`lane::candidate_lane`]) and the shell only has to probe them.
fn alloc_script(remote: &Remote, project_hash: &str, base: &str, max_lanes: usize) -> String {
    let candidates: Vec<String> = (0..max_lanes.max(1))
        .map(|index| lane::candidate_lane(base, index))
        .collect();
    format!(
        concat!(
            "LANES={lanes}; H={hash}; CHOSEN=; ",
            "for N in {names}; do ",
            "R=\"$LANES/$N/$H\"; ",
            // A lane that has never been used cannot be leased by anyone.
            "if [ ! -d \"$R\" ]; then CHOSEN=$N; break; fi; ",
            // Without flock the box cannot arbitrate; take the first lane and
            // let the build itself warn about it.
            "if ! command -v flock >/dev/null 2>&1; then CHOSEN=$N; break; fi; ",
            "if ( exec 8>\"$R.lock\"; flock -n 8 ) 2>/dev/null; then CHOSEN=$N; break; fi; ",
            "done; printf 'LANE=%s\\n' \"$CHOSEN\"",
        ),
        lanes = shell_quote(&format!("{}/lanes", remote.temp_dir.trim_end_matches('/'))),
        hash = shell_quote(project_hash),
        names = candidates.join(" "),
    )
}

impl Session<'_> {
    /// Upload the project sources into this run's private staging directory.
    ///
    /// Nothing here is locked, because nothing here is shared: the destination
    /// carries the run id, so two concurrent uploads cannot observe each other.
    pub fn upload(&self) -> Result<()> {
        let staging = &self.stage_root;
        info!("Transferring sources to {}:{staging}", self.remote.host);
        let mut cmd = self.rsync_command();
        // The stage is fresh for every run, but keep --delete so a retry after a
        // crashed upload cannot leave a stale file behind and have it compiled.
        // Excluded paths are left in place: rsync does not delete them unless
        // `--delete-excluded` is set, and this command never sets it.
        cmd.arg("--delete");
        cmd.args(self.upload_filter_args()?);

        // rsync runs the destination path through the remote shell, so the
        // directory has to exist before the transfer starts.
        cmd.arg("--rsync-path")
            .arg(format!("mkdir -p {} && rsync", shell_quote(staging)));
        cmd.arg(format!("{}/", self.project_root.display()));
        cmd.arg(format!("{}:{}", self.remote.host, staging));

        self.run(&mut cmd, "transfer sources to the build server")
    }

    /// Copy `relative` out of the remote target directory back to the local one.
    ///
    /// `relative` is resolved against `target/` on both sides. `../simulations`
    /// therefore means the lane-root `simulations/` directory remotely and
    /// `<project>/simulations` locally, and those resolved paths are what rsync
    /// sees. rsync 3.2.7 follows `..` in the destination: `--delete` on
    /// `target/../../outside/` removes files in `outside/`. A path that
    /// resolves outside the build lane, or outside the local project, is
    /// refused before any transfer.
    pub fn download_target(&self, relative: &str) -> Result<()> {
        let remote_target = self
            .remote_target_dir
            .clone()
            .unwrap_or_else(|| format!("{}/target", self.remote_crate_dir.trim_end_matches('/')));
        let relative = relative.trim_matches('/');
        let remote_path = resolve_remote_copy_path(&remote_target, &self.build_root, relative)?;
        let local_path =
            resolve_local_copy_path(&self.local_target_dir, self.project_root, relative)?;

        // A path that stayed inside `target/` keeps today's transfer, including
        // a symlinked target directory. A path that climbed out with `..` must
        // not follow a symlink: `--delete` would then rewrite the link target.
        let climbed_out = !remote_is_within(&remote_target, &remote_path);
        if !local_path.starts_with(&self.local_target_dir) {
            reject_local_symlink_redirect(self.project_root, &local_path)?;
        }

        // `deps/` and `build/` hold intermediates that are either regenerated
        // locally or large enough to dominate transfer time. See PR #24.
        // The patterns are unanchored so they match inside every profile
        // directory, which is where cargo puts them.
        let mut cmd = self.rsync_command();
        cmd.arg("--delete");
        // The same user patterns as the upload. Copy-back is rooted at `target/`
        // (or at the selected subdirectory), so an unanchored `coil_cache/` also
        // covers `target/coil_cache`, while a sibling of `cli_db/` is outside a
        // `--copy-back=cli_db/` transfer entirely.
        cmd.args(self.copy_back_filter_args()?);

        // An empty `relative` means "the whole target directory". Anything else
        // is either a directory (copy its contents) or a single artifact (copy
        // the file itself), and rsync needs to be told which: a trailing slash
        // asserts a directory and fails with ENOTDIR on a binary.
        let (remote_arg, local_arg) = if relative.is_empty() {
            (
                format!(
                    "{}:{}/",
                    self.remote.host,
                    remote_target.trim_end_matches('/')
                ),
                format!("{}/", self.local_target_dir.display()),
            )
        } else if self.remote_is_dir(
            &remote_path,
            climbed_out.then_some(self.build_root.as_str()),
        )? {
            (
                format!("{}:{}/", self.remote.host, remote_path),
                format!("{}/", local_path.display()),
            )
        } else {
            // rsync will not create intermediate directories for a single file.
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create the local directory {}", parent.display())
                })?;
            }
            (
                format!("{}:{}", self.remote.host, remote_path),
                local_path.display().to_string(),
            )
        };
        cmd.arg(remote_arg);
        cmd.arg(local_arg);

        self.run(&mut cmd, "copy the target directory back")
    }

    /// True when `path` is a directory on the build server.
    ///
    /// `--copy-back` needs the answer to choose between "copy these contents"
    /// and "copy this file", and guessing wrong fails the transfer: a trailing
    /// slash on a file is `ENOTDIR`, and no trailing slash on an existing
    /// directory nests it inside itself on the next run.
    ///
    /// When `lane_root` is set, the probe also refuses a symlink on the way to
    /// `path`. rsync would follow it and could read a tree outside the lane.
    fn remote_is_dir(&self, path: &str, lane_root: Option<&str>) -> Result<bool> {
        let script = match lane_root {
            Some(root) => confined_remote_dir_script(root, path),
            None => format!("test -d {}", shell_quote(path)),
        };
        let mut cmd = Command::new("ssh");
        cmd.args(self.remote.ssh_args());
        if !self
            .remote
            .ssh_options
            .iter()
            .any(|option| option.starts_with("LogLevel="))
        {
            cmd.arg("-o").arg("LogLevel=ERROR");
        }
        cmd.arg(&self.remote.host).arg(script);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        if self.verbose {
            debug!("running: {cmd:?}");
        }
        let output = cmd
            .output()
            .with_context(|| format!("failed to run `ssh` to {}", self.remote.host))?;
        match output.status.code() {
            Some(0) => Ok(true),
            // The confinement probe's own failure. Exit 1 is `test -d` saying
            // "not a directory", and a failed ssh (255) still falls through to
            // the transfer error below: the artifact could not be located.
            Some(2) => {
                let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
                if detail.is_empty() {
                    bail!("copy-back path {path} is not confined to the build lane");
                }
                bail!("{detail}");
            }
            _ => Ok(false),
        }
    }

    /// Copy the workspace `Cargo.lock` back, if the remote produced one.
    pub fn download_lock_file(&self) -> Result<()> {
        info!("Transferring Cargo.lock back to the client");
        let mut cmd = self.rsync_command();
        // `--ignore-missing-args` keeps a lock-less project (a virtual workspace
        // manifest, for instance) from failing the whole run.
        cmd.arg("--ignore-missing-args");
        cmd.arg(format!(
            "{}:{}/Cargo.lock",
            self.remote.host, self.remote_workspace_root
        ));
        cmd.arg(format!("{}/Cargo.lock", self.workspace_root.display()));
        self.run(&mut cmd, "copy Cargo.lock back")
    }

    /// Mirror the staged sources into this lane and run cargo there.
    ///
    /// One ssh session does all of it: lease the lane, move the stage into
    /// place, compile, release. Doing it in one session is what makes the lease
    /// leak-free — when the client goes away, the remote shell dies and the
    /// kernel drops the lock with it.
    ///
    /// `build_env` holds `KEY=value` pairs, `command` is the cargo subcommand and
    /// `args` the arguments for it.
    pub fn build(
        &self,
        build_env: &[String],
        command: &str,
        args: &[String],
    ) -> Result<std::process::ExitStatus> {
        let script = self.build_script(build_env, command, args)?;
        info!("Starting remote build in lane {}", self.lane);

        let mut cmd = Command::new("ssh");
        cmd.args(self.remote.ssh_args());
        // Only ask for a pseudo terminal when we have one to give. With `-t`
        // ssh reports success even when the remote command failed, which hides
        // build errors from scripts and CI.
        if is_interactive_terminal() {
            cmd.arg("-t");
        }
        // Build output is on stderr already; ssh's own chatter would only get in the
        // way. Skip it when the user asked for a specific LogLevel.
        if !self
            .remote
            .ssh_options
            .iter()
            .any(|option| option.starts_with("LogLevel="))
        {
            cmd.arg("-o").arg("LogLevel=ERROR");
        }
        cmd.arg(&self.remote.host).arg(script);
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        if self.verbose {
            debug!("running: {cmd:?}");
        }

        cmd.status()
            .with_context(|| format!("failed to run `ssh` to {}", self.remote.host))
    }

    /// Compose the shell script executed on the build server.
    fn build_script(&self, build_env: &[String], command: &str, args: &[String]) -> Result<String> {
        let root = &self.build_root;
        let lock = format!("{root}.lock");
        let incoming = lane::incoming_root(&self.remote.temp_dir, &self.project_hash);
        let ident = shell_quote(&format!("{} {}", self.lane, self.run_id));

        let mut script = String::new();

        if self.remote.shell_env {
            script.push_str(user_shell_env_script());
        }

        // Issue #26: `source` is not POSIX, so on a dash/sh build host it fails
        // and the rest of the line runs without the environment set up. `.` is
        // portable, and the file is only sourced when it actually exists.
        // These files run after the login-shell PATH import, so `-e` adds or
        // replaces scripts without discarding `nvcc` from the user's profile.
        for script_path in &self.remote.env {
            script.push_str(&format!(
                "[ -f {path} ] && . {path}; ",
                path = shell_quote(script_path)
            ));
        }

        script.push_str(&format!(
            "mkdir -p {parent} {root} || exit 3; ",
            parent = shell_quote(
                root.rsplit_once('/')
                    .map(|(parent, _)| parent)
                    .unwrap_or(root)
            ),
            root = shell_quote(root),
        ));

        // `flock` is util-linux; a BSD build host may not have it. Losing the
        // lease is a real loss of safety, so say so rather than pretend.
        script.push_str("if command -v flock >/dev/null 2>&1; then ");
        script.push_str(&self.job_share_script());
        script.push_str(&self.lease_script(&lock, &ident));
        script.push_str(&format!(
            "else echo {} >&2; fi; ",
            shell_quote(
                "remote-3000: warning: flock is not installed on the build server; \
                 concurrent builds of this lane are not serialised"
            )
        ));

        script.push_str(&self.toolchain_script());

        // Move the stage into the lane. This is the only mutation of a shared
        // tree, and it runs under the lease above.
        //
        // User excludes are repeated here, not only on the upload. The stage was
        // built without those paths, so a mirror `--delete` that did not exclude
        // them would remove the copies that already live on the lane (a remote
        // `coil_cache/`, for example). `--delete` still removes everything else
        // that the stage does not have.
        let mirror =
            lane_mirror_rsync(&self.stage_root, root, &self.upload_excludes, self.verbose)?;
        script.push_str(&format!(
            "if ! {mirror}; then echo {msg} >&2; exit 4; fi; rm -rf {stage}; ",
            stage = shell_quote(&self.stage_root),
            msg = shell_quote(&format!(
                "remote-3000: failed to move the staged sources into {root}"
            )),
        ));

        // A crashed client leaves its stage behind; nothing else will clean it.
        script.push_str(&format!(
            "find {incoming} -mindepth 1 -maxdepth 1 -mmin +1440 -exec rm -rf {{}} + 2>/dev/null; ",
            incoming = shell_quote(&incoming),
        ));

        script.push_str(&format!(
            "cd {} || exit 3; ",
            shell_quote(&self.remote_crate_dir)
        ));

        // The toolchain is selected for this process tree only. Setting it here
        // instead of running `rustup default` is what stops one build from
        // re-pointing the compiler under every other build on the server, and it
        // leaves a project's own `rust-toolchain.toml` in charge when the user
        // asked for no particular toolchain.
        if let Some(toolchain) = &self.remote.toolchain {
            script.push_str(&format!("RUSTUP_TOOLCHAIN={} ", shell_quote(toolchain)));
        }

        for assignment in build_env {
            validate_env_assignment(assignment)?;
            script.push_str(assignment);
            script.push(' ');
        }

        script.push_str(&format!("cargo {}", shell_quote(command)));
        for arg in args {
            script.push(' ');
            script.push_str(&shell_quote(arg));
        }

        Ok(script)
    }

    /// Give this build a fair share of the box's cores.
    ///
    /// Concurrency without this is concurrency into a wall: four builds each
    /// running `jobs = 4` on four cores finish later than four builds running
    /// `jobs = 1`, and the extra threads cost memory. The share is measured, not
    /// configured — every other leased lane is one more consumer.
    ///
    /// An explicit `CARGO_BUILD_JOBS` in `--build-env` is applied to the cargo
    /// command itself and therefore wins over this export.
    fn job_share_script(&self) -> String {
        format!(
            concat!(
                "OTHERS=0; ",
                "for f in {lanes}/*/*.lock; do ",
                "[ -e \"$f\" ] || continue; ",
                "if ! ( exec 8>\"$f\"; flock -n 8 ) 2>/dev/null; then OTHERS=$((OTHERS+1)); fi; ",
                "done; ",
                "CORES=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1); ",
                "JOBS=$(awk '/^\\[/{{sec=$0}} /^[[:space:]]*jobs[[:space:]]*=/ && sec==\"[build]\"",
                "{{gsub(/[^0-9]/,\"\"); print; exit}}' ",
                "\"${{CARGO_HOME:-$HOME/.cargo}}/config.toml\" 2>/dev/null); ",
                "[ -n \"$JOBS\" ] || JOBS=$CORES; ",
                "SHARE=$(( CORES / (OTHERS + 1) )); [ \"$SHARE\" -lt 1 ] && SHARE=1; ",
                "[ \"$SHARE\" -gt \"$JOBS\" ] && SHARE=$JOBS; ",
                "export CARGO_BUILD_JOBS=$SHARE; ",
            ),
            lanes = shell_quote(&format!(
                "{}/lanes",
                self.remote.temp_dir.trim_end_matches('/')
            )),
        )
    }

    /// The lease on this lane.
    ///
    /// Exclusive: a lane's sources and `target/` are exactly what two concurrent
    /// builds would corrupt. An automatically allocated lane is never waited
    /// for — the caller moves to another lane — while a lane the user named is
    /// waited for, because naming one is a statement about which tree is wanted.
    fn lease_script(&self, lock: &str, ident: &str) -> String {
        let lock = shell_quote(lock);
        let timeout = self.lock_timeout;
        // Every branch that fails to acquire ends in `exit BUSY_EXIT`. A branch
        // that merely warned would fall through and run the compile unleased,
        // which is the one failure this whole design exists to prevent.
        let on_busy = if self.wait_for_lane && timeout > 0 {
            format!(
                "if ! flock -w {timeout} 9; then echo \"remote-3000: lane {lane} is still busy after {timeout}s; use another --lane, or --lock-timeout 0 to fail fast\" >&2; exit {busy}; fi;",
                lane = shell_quote(&self.lane),
                busy = BUSY_EXIT,
            )
        } else {
            format!(
                "echo \"remote-3000: lane {lane} is still busy; pass --lane to claim a tree of your own\" >&2; exit {busy};",
                lane = shell_quote(&self.lane),
                busy = BUSY_EXIT,
            )
        };
        format!(
            "exec 9>{lock} || exit 3; \
             if ! flock -n 9; then \
               echo \"remote-3000: lane {lane} is busy (held by $(cat {lock}.owner 2>/dev/null || echo unknown))\" >&2; \
               {on_busy} \
             fi; \
             printf '%s\\n' {ident} >\"{lock}.owner\"; \
             trap 'rm -f {lock}.owner' EXIT HUP INT TERM; ",
            lane = shell_quote(&self.lane),
        )
    }

    /// Install and select the requested toolchain without touching the default.
    ///
    /// rustup already *is* the multi-toolchain manager: one `RUSTUP_HOME` holds
    /// any number of toolchains and `RUSTUP_TOOLCHAIN` picks one for a single
    /// process tree. What it cannot do is guess that a version nobody installed
    /// should be fetched, so a missing toolchain is installed here — announced,
    /// idempotent, and without becoming anyone's default.
    fn toolchain_script(&self) -> String {
        let Some(toolchain) = &self.remote.toolchain else {
            return String::new();
        };
        format!(
            "if command -v rustup >/dev/null 2>&1 && ! rustup toolchain list 2>/dev/null | grep -q '^{tc}'; then \
               echo {msg} >&2; \
               rustup toolchain install {tc} --profile default --no-self-update || exit 3; \
             fi; ",
            tc = shell_quote(toolchain),
            msg = shell_quote(&format!(
                "remote-3000: toolchain {toolchain} is not installed on the build server; installing it now"
            )),
        )
    }

    /// rsync filter arguments for the upload into the per-run stage.
    fn upload_filter_args(&self) -> Result<Vec<String>> {
        let mut args = vec!["--exclude".to_string(), "target".to_string()];
        if !self.transfer_hidden {
            args.push("--exclude".to_string());
            args.push(".*".to_string());
        }
        if self.no_transfer_git {
            // `.git` is hidden, so this only matters together with --transfer-hidden.
            args.push("--exclude".to_string());
            args.push(".git".to_string());
        }
        args.extend(self.user_exclude_args()?);
        Ok(args)
    }

    /// rsync filter arguments for copy-back.
    ///
    /// Built-in skips drop cargo's intermediates. User patterns are appended so
    /// an excluded cache inside `target/` is neither downloaded nor deleted.
    fn copy_back_filter_args(&self) -> Result<Vec<String>> {
        let mut args = Vec::new();
        for pattern in ["deps/", "build/", ".fingerprint/", "incremental/"] {
            args.push("--exclude".to_string());
            args.push(pattern.to_string());
        }
        args.extend(self.user_exclude_args()?);
        Ok(args)
    }

    /// `--exclude PATTERN` pairs for the configured and CLI patterns.
    fn user_exclude_args(&self) -> Result<Vec<String>> {
        let mut args = Vec::with_capacity(self.upload_excludes.len() * 2);
        for pattern in &self.upload_excludes {
            validate_exclude_pattern(pattern)?;
            args.push("--exclude".to_string());
            args.push(pattern.clone());
        }
        Ok(args)
    }

    /// Base rsync invocation with flags shared by every transfer.
    fn rsync_command(&self) -> Command {
        let mut cmd = Command::new("rsync");
        // `-a` preserves permissions and times. `--partial` keeps an interrupted
        // transfer resumable without `--progress`, which prints one line per
        // file. Progress is opt-in via `-v`. `--info=progress2` is not used: the
        // rsync shipped with macOS does not have it (issue #2).
        cmd.arg("-a")
            .arg("--partial")
            .arg("--compress")
            .arg("--safe-links");
        if self.verbose {
            cmd.arg("--progress");
        }
        cmd.arg("-e").arg(self.remote.rsync_ssh_command());
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());
        cmd
    }

    /// Run an rsync command, turning its exit status into an error.
    fn run(&self, cmd: &mut Command, what: &str) -> Result<()> {
        if self.verbose {
            debug!("running: {cmd:?}");
        }
        let status = cmd
            .status()
            .with_context(|| format!("failed to run `rsync` to {what}"))?;
        if !status.success() {
            bail!(
                "failed to {what} (rsync exited with {})",
                status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            );
        }
        Ok(())
    }
}

/// The `rsync --delete` that copies the stage onto the persistent lane tree.
///
/// `target` is always excluded, which is what keeps the remote build directory.
/// Each user pattern is a single shell word (`--exclude='...'`) so a pattern
/// cannot be parsed as another rsync flag or as shell syntax. The trailing
/// slashes are outside the quotes on purpose: `'path with spaces'/` is one
/// shell word whose value ends in `/`.
fn lane_mirror_rsync(
    stage: &str,
    root: &str,
    excludes: &[String],
    verbose: bool,
) -> Result<String> {
    let mut cmd = String::from("rsync -a --partial");
    if verbose {
        cmd.push_str(" --progress");
    }
    cmd.push_str(" --delete --exclude=");
    cmd.push_str(&shell_quote_literal("target"));
    for pattern in excludes {
        validate_exclude_pattern(pattern)?;
        cmd.push_str(" --exclude=");
        cmd.push_str(&shell_quote_literal(pattern));
    }
    cmd.push(' ');
    cmd.push_str(&shell_quote(stage));
    cmd.push_str("/ ");
    cmd.push_str(&shell_quote(root));
    cmd.push('/');
    Ok(cmd)
}

/// Import `PATH` from the remote user's own login shell, then return to this script.
///
/// `ssh host command` starts a non-interactive, non-login shell, so it does not
/// read `.profile` or the interactive rc where a user normally adds `nvcc`.
/// Asking `$SHELL -ilc` to print `PATH` runs that startup in the user's shell
/// (bash, zsh, or dash), not by sourcing an rc file with `sh`. fish and csh are
/// skipped: they cannot execute the rest of this POSIX script.
///
/// Startup chatter goes to `/dev/null`. The probe's stderr is shown only when
/// the probe fails, so a missing shell is visible and the usual "no job control"
/// warning is not. `~/.cargo/env` and any `-e` files are sourced afterwards by
/// the caller, which is what puts `cargo` on `PATH` when the profile does not.
fn user_shell_env_script() -> &'static str {
    // One line on purpose: the end-to-end harness records each ssh argument as
    // a single log line, and a newline would hide the rest of the script.
    r#"_cargo_remote_path=; case "${SHELL:-}" in *fish*|*csh|*tcsh) ;; *) if [ -n "${SHELL:-}" ] && [ -x "${SHELL:-}" ]; then _cargo_remote_err=$(mktemp 2>/dev/null || echo "${TMPDIR:-/tmp}/cargo-remote-3000-shell-env.$$"); _cargo_remote_path=$("$SHELL" -ilc 'exec 1>&3; printf %s "$PATH"' 3>&1 >/dev/null 2>"$_cargo_remote_err") || { cat "$_cargo_remote_err" >&2; _cargo_remote_path=; }; rm -f "$_cargo_remote_err"; fi ;; esac; if [ -n "$_cargo_remote_path" ]; then PATH=$_cargo_remote_path; export PATH; fi; unset _cargo_remote_path _cargo_remote_err; "#
}

/// Resolve `relative` against the remote `target/` directory.
///
/// The result never contains a `..` component. A path that stays inside
/// `target/` is unchanged apart from that cleanup. A path that climbs out, such
/// as `../simulations`, must land strictly inside the lane root: rsync is given
/// that directory and nothing above it.
fn resolve_remote_copy_path(target: &str, lane_root: &str, relative: &str) -> Result<String> {
    if relative.is_empty() {
        return Ok(target.trim_end_matches('/').to_string());
    }
    validate_copy_back_relative(relative)?;
    let joined = format!("{}/{}", target.trim_end_matches('/'), relative);
    let normalized = normalize_remote_path(&joined);
    if remote_is_within(target, &normalized) || remote_is_strictly_inside(lane_root, &normalized) {
        return Ok(normalized);
    }
    bail!(
        "copy-back path {relative:?} resolves to {normalized}, which is outside the build lane ({})",
        lane_root.trim_end_matches('/')
    );
}

/// Resolve `relative` against the local target directory.
///
/// The same rule as [`resolve_remote_copy_path`], with the project root in
/// place of the lane. `../simulations` from `<project>/target` is
/// `<project>/simulations`, which is the only directory `--delete` may touch.
fn resolve_local_copy_path(target: &Path, project_root: &Path, relative: &str) -> Result<PathBuf> {
    if relative.is_empty() {
        return Ok(target.to_path_buf());
    }
    validate_copy_back_relative(relative)?;
    let normalized = normalize_local_path(&target.join(relative));
    if normalized.starts_with(target) || local_is_strictly_inside(project_root, &normalized) {
        return Ok(normalized);
    }
    bail!(
        "copy-back path {relative:?} resolves to {}, which is outside the project ({})",
        normalized.display(),
        project_root.display()
    );
}

fn validate_copy_back_relative(relative: &str) -> Result<()> {
    if relative.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
        bail!(
            "copy-back path {relative:?} contains a control character; \
             it is passed to rsync as a path"
        );
    }
    Ok(())
}

/// Collapse `.` and `..` in a remote path, which always uses `/`.
///
/// A leading `/` stays the root, and a leading `~` is kept as a component so
/// `~/remote-builds/...` still expands on the build server. `..` that would
/// climb past either is dropped rather than emitted: the caller then sees a
/// path that fails the lane-root check.
fn normalize_remote_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last().copied() {
                // `~` is the remote home, not a directory this path may climb
                // past. Drop the extra `..` so the result cannot be
                // `~/../somewhere`.
                Some("~") => {}
                Some("..") => parts.push(".."),
                Some(_) => {
                    parts.pop();
                }
                None if absolute => {}
                None => parts.push(".."),
            },
            other => parts.push(other),
        }
    }
    if absolute {
        format!("/{}", parts.join("/"))
    } else if parts.is_empty() {
        String::new()
    } else {
        parts.join("/")
    }
}

fn normalize_local_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else {
                    out.push(component);
                }
            }
            other => out.push(other),
        }
    }
    out
}

fn remote_is_within(root: &str, path: &str) -> bool {
    let root = root.trim_end_matches('/');
    path == root || path.starts_with(&format!("{root}/"))
}

fn remote_is_strictly_inside(root: &str, path: &str) -> bool {
    let root = root.trim_end_matches('/');
    !root.is_empty() && path.starts_with(&format!("{root}/"))
}

fn local_is_strictly_inside(root: &Path, path: &Path) -> bool {
    path.starts_with(root) && path != root
}

/// Refuse a symlink between the project root and `path`.
///
/// `--delete` follows a symlink used as the destination and rewrites whatever
/// it points at. A real `simulations/` directory is left alone; a link there is
/// not a directory this transfer may delete through.
fn reject_local_symlink_redirect(project_root: &Path, path: &Path) -> Result<()> {
    let rest = path.strip_prefix(project_root).with_context(|| {
        format!(
            "copy-back path {} is outside the project {}",
            path.display(),
            project_root.display()
        )
    })?;
    let mut current = project_root.to_path_buf();
    for component in rest.components() {
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                bail!(
                    "copy-back destination {} is a symlink; refusing to follow it, \
                     because --delete would change the link target instead of {}",
                    current.display(),
                    path.display()
                );
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect copy-back path {}", current.display())
                });
            }
        }
    }
    Ok(())
}

/// Remote `test -d` that also refuses to walk a symlink out of the lane.
///
/// One ssh argument, and one line: the end-to-end harness records each ssh
/// argument as a single log line. Exit 2 is the confinement failure; exit 1 is
/// left for `test -d`.
fn confined_remote_dir_script(root: &str, path: &str) -> String {
    let root = root.trim_end_matches('/');
    let rest = path
        .strip_prefix(root)
        .unwrap_or(path)
        .trim_start_matches('/');
    format!(
        "cur={root}; rest={rest}; while [ -n \"$rest\" ]; do part=${{rest%%/*}}; case \"$rest\" in */*) rest=${{rest#*/}} ;; *) rest= ;; esac; [ -z \"$part\" ] && continue; if [ \"$part\" = \"..\" ] || [ \"$part\" = \".\" ]; then echo \"remote-3000: copy-back refuses a '$part' path component\" >&2; exit 2; fi; cur=\"$cur/$part\"; if [ -L \"$cur\" ]; then echo \"remote-3000: $cur is a symlink; copy-back will not follow it out of the build lane\" >&2; exit 2; fi; if [ ! -e \"$cur\" ]; then break; fi; done; test -d {path}",
        root = shell_quote(root),
        rest = shell_quote(rest),
        path = shell_quote(path),
    )
}

/// Reject anything that is not a plain `KEY=value` assignment.
///
/// These values are interpolated into a shell script, so a stray quote or
/// `$(...)` would be command injection. Being strict here is the point.
fn validate_env_assignment(assignment: &str) -> Result<()> {
    let Some((key, _)) = assignment.split_once('=') else {
        bail!("invalid build environment entry {assignment:?}, expected KEY=value");
    };
    let valid = !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid {
        bail!(
            "invalid build environment variable name {key:?}, \
             expected an uppercase-style identifier such as RUST_BACKTRACE"
        );
    }

    // The value is interpolated into the remote script unquoted, so it must
    // not contain anything the shell could interpret.
    let value = assignment
        .split_once('=')
        .map(|(_, value)| value)
        .unwrap_or("");
    let safe = value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_./:,+@= ".contains(&b));
    if !safe {
        bail!(
            "unsupported value {value:?} for build environment variable {key:?}; \
             allowed characters are letters, digits and - _ . / : , + @ = and spaces"
        );
    }
    Ok(())
}

/// True when stdout is a terminal, which is when ssh should get a pty.
fn is_interactive_terminal() -> bool {
    #[cfg(unix)]
    {
        use std::io::IsTerminal;
        std::io::stdout().is_terminal()
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::RemoteOpts;
    use crate::config::Config;

    fn remote() -> Remote {
        Config::default()
            .get_remote(&RemoteOpts {
                host: Some("user@build-host".to_string()),
                toolchain: Some("1.95.0".to_string()),
                ..Default::default()
            })
            .unwrap()
    }

    fn session(root: &Path) -> Session<'_> {
        let remote = Box::leak(Box::new(remote()));
        Session {
            remote,
            project_root: root,
            workspace_root: root,
            lane: "default".to_string(),
            project_hash: "12345".to_string(),
            build_root: "/tmp/remote-builds/lanes/default/12345".to_string(),
            remote_workspace_root: "/tmp/remote-builds/lanes/default/12345".to_string(),
            remote_crate_dir: "/tmp/remote-builds/lanes/default/12345".to_string(),
            local_target_dir: root.join("target"),
            remote_target_dir: Some("/tmp/remote-builds/lanes/default/12345/target".to_string()),
            stage_root: "/tmp/remote-builds/incoming/12345/run-1".to_string(),
            run_id: "run-1".to_string(),
            wait_for_lane: false,
            upload_excludes: vec![],
            transfer_hidden: false,
            no_transfer_git: false,
            lock_timeout: 3600,
            verbose: false,
        }
    }

    #[test]
    fn env_assignments_must_look_like_shell_variables() {
        assert!(validate_env_assignment("RUST_BACKTRACE=1").is_ok());
        assert!(validate_env_assignment("CC=clang").is_ok());
        assert!(validate_env_assignment("RUST_BACKTRACE").is_err());
        assert!(validate_env_assignment("=1").is_err());
        assert!(validate_env_assignment("A B=1").is_err());
        // Command injection attempts are rejected rather than escaped.
        assert!(validate_env_assignment("A=$(id)").is_err());
        assert!(validate_env_assignment("A='x'").is_err());
    }

    #[test]
    fn build_script_sources_env_posix_style() {
        // Issue #26: `source` is a bashism and silently did nothing on sh hosts.
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s
            .build_script(
                &["RUST_BACKTRACE=1".to_string()],
                "build",
                &["--release".to_string()],
            )
            .unwrap();
        assert!(
            script.contains("[ -f ~/.cargo/env ] && . ~/.cargo/env;"),
            "{script}"
        );
        assert!(!script.contains(" source "), "{script}");
        assert!(
            script.contains("cd /tmp/remote-builds/lanes/default/12345 || exit 3;"),
            "{script}"
        );
        assert!(script.ends_with("cargo build --release"), "{script}");
    }

    #[test]
    fn the_toolchain_is_selected_per_invocation_never_globally() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        // The defect this replaces: `rustup default` wrote the server's global
        // default, so two builds with different toolchains re-pointed each
        // other's compiler.
        assert!(!script.contains("rustup default"), "{script}");
        assert!(
            script.contains("RUSTUP_TOOLCHAIN=1.95.0 cargo check"),
            "{script}"
        );
    }

    #[test]
    fn without_a_requested_toolchain_the_server_default_is_left_alone() {
        // A project's own `rust-toolchain.toml` must win over nothing at all.
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        let remote = Box::leak(Box::new(Remote {
            toolchain: None,
            ..(*s.remote).clone()
        }));
        s.remote = remote;
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(!script.contains("RUSTUP_TOOLCHAIN"), "{script}");
        assert!(!script.contains("rustup"), "{script}");
    }

    #[test]
    fn a_missing_toolchain_is_installed_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(
            script.contains("rustup toolchain install 1.95.0 --profile default --no-self-update"),
            "{script}"
        );
        assert!(script.contains("grep -q '^1.95.0'"), "{script}");
    }

    #[test]
    fn an_automatic_lane_never_waits_and_never_runs_unleased() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(script.contains("flock -n 9"), "{script}");
        // Contention is answered by moving lanes, not by queueing.
        assert!(!script.contains("flock -w"), "{script}");
        // And a failed acquisition must exit, never fall through to the build.
        assert!(!script.contains("if false"), "{script}");
        assert!(script.contains(&format!("exit {BUSY_EXIT}")), "{script}");
    }

    #[test]
    fn the_lease_really_excludes_a_second_build() {
        // A string assertion cannot prove exclusion, so run the composed lease
        // under sh with a real flock. Linux-only: BSD and macOS have no flock(1).
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        if !cfg!(target_os = "linux") {
            return;
        }
        let has_flock = Command::new("sh")
            .args(["-c", "command -v flock"])
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false);
        if !has_flock {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("lane.lock");
        let owner = dir.path().join("lane.lock.owner");
        let s = session(dir.path());
        let lease = s.lease_script(lock.to_str().unwrap(), "'test-ident'");

        // Hold the lease for a while, in its own process.
        let mut holder = Command::new("sh")
            .arg("-c")
            .arg(format!("{lease} exec sleep 30"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start the lease holder");

        let deadline = Instant::now() + Duration::from_secs(5);
        while !owner.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(owner.exists(), "the holder never took the lease");

        let second = Command::new("sh")
            .arg("-c")
            .arg(format!("{lease} echo ACQUIRED"))
            .output()
            .expect("failed to run the second build");
        assert_eq!(
            second.status.code(),
            Some(BUSY_EXIT as i32),
            "a leased lane must refuse the second build: {}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&second.stdout).contains("ACQUIRED"),
            "the second build ran without a lease"
        );

        holder.kill().unwrap();
        holder.wait().unwrap();

        // Killing the holder releases the lease: the kernel drops it with the
        // process, which is why a dead client cannot wedge the lane.
        let third = Command::new("sh")
            .arg("-c")
            .arg(format!("{lease} echo ACQUIRED"))
            .output()
            .expect("failed to run the third build");
        assert!(
            third.status.success(),
            "the lease did not survive the holder's death: {}",
            String::from_utf8_lossy(&third.stderr)
        );
    }

    #[test]
    fn a_named_lane_waits_for_its_tree() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        s.wait_for_lane = true;
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(script.contains("flock -w 3600 9"), "{script}");
    }

    #[test]
    fn a_zero_timeout_fails_fast_instead_of_queueing() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        s.wait_for_lane = true;
        s.lock_timeout = 0;
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(!script.contains("flock -w 0"), "{script}");
        assert!(script.contains(&format!("exit {BUSY_EXIT}")), "{script}");
    }

    #[test]
    fn the_build_holds_a_lease_that_is_leak_free() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(
            script.contains("exec 9>/tmp/remote-builds/lanes/default/12345.lock"),
            "{script}"
        );
        // The trap removes only the descriptive file; the lock itself is
        // released by the process exiting, which a killed client also does.
        assert!(script.contains("trap 'rm -f"), "{script}");
    }

    #[test]
    fn the_lease_is_taken_before_the_shared_tree_is_touched() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        let lease = script.find("flock -n 9").unwrap();
        let mirror = script
            .find("rsync -a --partial --delete --exclude='target'")
            .unwrap();
        assert!(lease < mirror, "lease must precede the mirror: {script}");
    }

    #[test]
    fn the_mirror_preserves_the_lane_target_directory() {
        // Without `--exclude target` the mirror would delete the warm cache
        // that makes remote builds worth doing.
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        let mirror = script
            .split("rsync -a --partial --delete")
            .nth(1)
            .expect("a mirror step");
        assert!(mirror.starts_with(" --exclude='target' "), "{script}");
    }

    #[test]
    fn user_excludes_are_quoted_in_the_lane_mirror() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        s.upload_excludes = vec![
            "coil_cache/".to_string(),
            "my cache/".to_string(),
            "it's/".to_string(),
            "$(touch pwned)".to_string(),
        ];
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(script.contains("--exclude='coil_cache/'"), "{script}");
        assert!(script.contains("--exclude='my cache/'"), "{script}");
        assert!(script.contains(r"--exclude='it'\''s/'"), "{script}");
        assert!(script.contains("--exclude='$(touch pwned)'"), "{script}");
        // The payload stays inside the single-quoted pattern. There is no
        // unquoted `$(` for the remote shell to evaluate.
        let unquoted = script.replace("--exclude='$(touch pwned)'", "");
        assert!(!unquoted.contains("$(touch"), "{script}");
    }

    #[test]
    fn a_control_character_exclude_never_reaches_the_remote_script() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        s.upload_excludes = vec!["bad\npattern".to_string()];
        let err = s.build_script(&[], "check", &[]).unwrap_err().to_string();
        assert!(err.contains("control character"), "{err}");
    }

    #[test]
    fn sources_are_staged_outside_every_lane() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(
            script.contains(
                "/tmp/remote-builds/incoming/12345/run-1/ /tmp/remote-builds/lanes/default/12345/"
            ),
            "{script}"
        );
        assert!(
            script.contains("rm -rf /tmp/remote-builds/incoming/12345/run-1"),
            "{script}"
        );
    }

    #[test]
    fn the_build_takes_a_fair_share_of_the_cores() {
        let dir = tempfile::tempdir().unwrap();
        let s = session(dir.path());
        let script = s.build_script(&[], "check", &[]).unwrap();
        assert!(script.contains("OTHERS=0;"), "{script}");
        assert!(
            script.contains("SHARE=$(( CORES / (OTHERS + 1) ))"),
            "{script}"
        );
        assert!(
            script.contains("export CARGO_BUILD_JOBS=$SHARE;"),
            "{script}"
        );
        // The user's own value is applied to the command and must win.
        let explicit = s
            .build_script(&["CARGO_BUILD_JOBS=2".to_string()], "check", &[])
            .unwrap();
        assert!(
            explicit.contains("CARGO_BUILD_JOBS=2 cargo check"),
            "{explicit}"
        );
    }

    #[test]
    fn allocation_offers_every_lane_then_gives_up() {
        let script = alloc_script(&remote(), "abc123", "default", 3);
        assert!(
            script.contains("for N in default default-2 default-3;"),
            "{script}"
        );
        // An unused lane is taken without a probe; a used one is probed.
        assert!(script.contains("[ ! -d \"$R\" ]"), "{script}");
        assert!(script.contains("flock -n 8"), "{script}");
        assert!(script.contains("printf 'LANE=%s\\n'"), "{script}");
        // Nothing waits: allocation is a survey, not a queue.
        assert!(!script.contains("flock -w"), "{script}");
    }

    #[test]
    fn allocation_is_per_project() {
        let a = alloc_script(&remote(), "aaa", "default", 8);
        let b = alloc_script(&remote(), "bbb", "default", 8);
        assert_ne!(a, b);
        assert!(a.contains("H=aaa"), "{a}");
    }

    fn linux_rsync() -> bool {
        if !cfg!(target_os = "linux") {
            return false;
        }
        Command::new("rsync")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn write_file(path: &std::path::Path, body: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    fn read_file(path: &std::path::Path) -> String {
        std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
    }

    fn run_rsync(args: &[String], src: &std::path::Path, dst: &std::path::Path) {
        let mut cmd = Command::new("rsync");
        cmd.arg("-a").arg("--delete").args(args);
        cmd.arg(format!("{}/", src.display()));
        cmd.arg(format!("{}/", dst.display()));
        let output = cmd.output().expect("rsync should be installed");
        assert!(
            output.status.success(),
            "rsync failed ({:?}): {}",
            cmd,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn run_mirror(stage: &std::path::Path, lane: &std::path::Path, excludes: &[String]) {
        let script = lane_mirror_rsync(
            stage.to_str().unwrap(),
            lane.to_str().unwrap(),
            excludes,
            false,
        )
        .unwrap();
        let output = Command::new("sh")
            .arg("-c")
            .arg(&script)
            .current_dir(stage)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "mirror failed: {script}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn excluded_remote_cache_survives_while_stale_test_data_is_deleted() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let lane = dir.path().join("lane");
        // Different lengths: rsync -a skips a file whose size and mtime both
        // match, and these writes can land in the same second.
        write_file(&stage.join("src/lib.rs"), "new code from the client\n");
        write_file(&stage.join("testdata/kept.txt"), "still here\n");
        // A local cache must not be uploaded over the remote one.
        write_file(&stage.join("coil_cache/LOCAL"), "do not upload\n");
        write_file(&lane.join("src/lib.rs"), "old\n");
        write_file(&lane.join("testdata/stale.bin"), "obsolete test data\n");
        write_file(&lane.join("coil_cache/MARKER"), "remote cache\n");
        write_file(
            &lane.join("target/coil_cache/MARKER"),
            "remote target cache\n",
        );
        write_file(&lane.join("target/debug/app"), "warm artifact\n");

        let excludes = vec!["coil_cache/".to_string()];
        run_mirror(&stage, &lane, &excludes);

        assert_eq!(
            read_file(&lane.join("src/lib.rs")),
            "new code from the client\n"
        );
        assert_eq!(read_file(&lane.join("testdata/kept.txt")), "still here\n");
        assert!(
            !lane.join("testdata/stale.bin").exists(),
            "non-excluded test data should be deleted"
        );
        assert_eq!(
            read_file(&lane.join("coil_cache/MARKER")),
            "remote cache\n",
            "root coil_cache must survive and must not be replaced"
        );
        assert!(!lane.join("coil_cache/LOCAL").exists());
        assert_eq!(
            read_file(&lane.join("target/coil_cache/MARKER")),
            "remote target cache\n"
        );
        assert_eq!(read_file(&lane.join("target/debug/app")), "warm artifact\n");
        assert!(!dir.path().join("pwned").exists());
    }

    #[test]
    fn without_an_exclude_delete_still_removes_a_root_cache() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let lane = dir.path().join("lane");
        write_file(&stage.join("src/lib.rs"), "new code from the client\n");
        write_file(&lane.join("src/lib.rs"), "old\n");
        write_file(&lane.join("coil_cache/MARKER"), "remote cache\n");
        write_file(&lane.join("testdata/stale.bin"), "obsolete\n");
        write_file(&lane.join("target/coil_cache/MARKER"), "inside target\n");

        run_mirror(&stage, &lane, &[]);

        assert_eq!(
            read_file(&lane.join("src/lib.rs")),
            "new code from the client\n"
        );
        assert!(!lane.join("coil_cache").exists());
        assert!(!lane.join("testdata/stale.bin").exists());
        assert_eq!(
            read_file(&lane.join("target/coil_cache/MARKER")),
            "inside target\n",
            "the built-in target exclude still preserves target/coil_cache"
        );
    }

    #[test]
    fn exclude_patterns_are_data_for_rsync_not_shell_commands() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let lane = dir.path().join("lane");
        write_file(&stage.join("src/lib.rs"), "new code from the client\n");
        write_file(&lane.join("obsolete.dat"), "gone\n");
        write_file(&lane.join("my cache/MARKER"), "spaces\n");
        write_file(&lane.join("name'quote/MARKER"), "quote\n");
        write_file(&lane.join("anchored/MARKER"), "root anchor\n");
        write_file(&lane.join("nested/anchored/MARKER"), "not at root\n");
        write_file(&lane.join("notes.keep"), "wildcard\n");
        write_file(&lane.join("notes.tmp"), "not excluded\n");
        write_file(&lane.join("coil_cache/MARKER"), "cache\n");

        let excludes = vec![
            "coil_cache/".to_string(),
            "my cache/".to_string(),
            "name'quote/".to_string(),
            "/anchored/".to_string(),
            "*.keep".to_string(),
            "coil_cache/; touch pwned".to_string(),
            "$(touch pwned)".to_string(),
            "--delete".to_string(),
        ];
        run_mirror(&stage, &lane, &excludes);

        assert_eq!(
            read_file(&lane.join("src/lib.rs")),
            "new code from the client\n"
        );
        assert!(!lane.join("obsolete.dat").exists());
        assert!(!lane.join("notes.tmp").exists());
        assert!(!lane.join("nested/anchored/MARKER").exists());
        assert_eq!(read_file(&lane.join("my cache/MARKER")), "spaces\n");
        assert_eq!(read_file(&lane.join("name'quote/MARKER")), "quote\n");
        assert_eq!(read_file(&lane.join("anchored/MARKER")), "root anchor\n");
        assert_eq!(read_file(&lane.join("notes.keep")), "wildcard\n");
        assert_eq!(read_file(&lane.join("coil_cache/MARKER")), "cache\n");
        assert!(!dir.path().join("pwned").exists());
        assert!(!lane.join("pwned").exists());
        assert!(!stage.join("pwned").exists());
    }

    #[test]
    fn an_excluded_cache_is_not_uploaded_and_other_deletions_still_happen() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let stage = dir.path().join("stage");
        write_file(&project.join("src/lib.rs"), "source\n");
        write_file(&project.join("coil_cache/HUGE"), "do not upload\n");
        write_file(&project.join("target/debug/local"), "local target\n");
        write_file(
            &project.join("target/coil_cache/LOCAL"),
            "local target cache\n",
        );
        write_file(&stage.join("stale_test.dat"), "left by an earlier sync\n");
        write_file(&stage.join("coil_cache/ALREADY"), "stage leftover\n");

        let root = dir.path().join("meta");
        write_file(&root.join("Cargo.toml"), "");
        let mut s = session(&root);
        s.upload_excludes = vec!["coil_cache/".to_string()];
        let filters = s.upload_filter_args().unwrap();
        run_rsync(&filters, &project, &stage);

        assert_eq!(read_file(&stage.join("src/lib.rs")), "source\n");
        assert!(!stage.join("stale_test.dat").exists());
        assert!(!stage.join("target").exists());
        assert!(
            !stage.join("coil_cache/HUGE").exists(),
            "the project cache must not be uploaded"
        );
        // The upload exclude also protects a leftover excluded directory in the
        // stage, which is what a reused watch-mode stage relies on.
        assert_eq!(
            read_file(&stage.join("coil_cache/ALREADY")),
            "stage leftover\n"
        );
        assert!(!filters.iter().any(|arg| arg.contains("delete-excluded")));
    }

    #[test]
    fn selective_cli_db_copy_back_does_not_fetch_or_delete_the_cache() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let remote_target = dir.path().join("remote-target");
        let local_target = dir.path().join("local-target");
        let project_cache = dir.path().join("coil_cache");
        write_file(&remote_target.join("cli_db/row"), "from remote\n");
        write_file(
            &remote_target.join("coil_cache/FROM-REMOTE"),
            "must not download\n",
        );
        write_file(&local_target.join("cli_db/row"), "old local\n");
        write_file(&local_target.join("cli_db/extra.dat"), "local only\n");
        write_file(
            &local_target.join("coil_cache/PRECIOUS"),
            "local target cache\n",
        );
        write_file(&project_cache.join("PRECIOUS"), "project root cache\n");

        let root = dir.path().join("meta");
        write_file(&root.join("Cargo.toml"), "");
        let mut s = session(&root);
        s.upload_excludes = vec!["coil_cache/".to_string()];
        let filters = s.copy_back_filter_args().unwrap();
        run_rsync(
            &filters,
            &remote_target.join("cli_db"),
            &local_target.join("cli_db"),
        );

        assert_eq!(read_file(&local_target.join("cli_db/row")), "from remote\n");
        assert!(
            !local_target.join("cli_db/extra.dat").exists(),
            "non-excluded files inside the selected path are still deleted"
        );
        assert_eq!(
            read_file(&local_target.join("coil_cache/PRECIOUS")),
            "local target cache\n"
        );
        assert!(!local_target.join("coil_cache/FROM-REMOTE").exists());
        assert_eq!(
            read_file(&project_cache.join("PRECIOUS")),
            "project root cache\n"
        );
    }

    #[test]
    fn whole_target_copy_back_leaves_an_excluded_cache_untouched() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let remote_target = dir.path().join("remote-target");
        let local_target = dir.path().join("local-target");
        write_file(&remote_target.join("cli_db/row"), "from remote\n");
        write_file(
            &remote_target.join("coil_cache/FROM-REMOTE"),
            "must not download\n",
        );
        write_file(&remote_target.join("deps/crate.rmeta"), "intermediate\n");
        write_file(&local_target.join("cli_db/row"), "old\n");
        write_file(&local_target.join("stale.txt"), "delete me\n");
        write_file(
            &local_target.join("coil_cache/PRECIOUS"),
            "keep local bytes\n",
        );
        write_file(&local_target.join("deps/old.rmeta"), "local intermediate\n");

        let root = dir.path().join("meta");
        write_file(&root.join("Cargo.toml"), "");
        let mut s = session(&root);
        s.upload_excludes = vec!["coil_cache/".to_string()];
        let filters = s.copy_back_filter_args().unwrap();
        run_rsync(&filters, &remote_target, &local_target);

        assert_eq!(read_file(&local_target.join("cli_db/row")), "from remote\n");
        assert!(!local_target.join("stale.txt").exists());
        assert_eq!(
            read_file(&local_target.join("coil_cache/PRECIOUS")),
            "keep local bytes\n"
        );
        assert!(!local_target.join("coil_cache/FROM-REMOTE").exists());
        // Built-in copy-back excludes still protect cargo intermediates.
        assert_eq!(
            read_file(&local_target.join("deps/old.rmeta")),
            "local intermediate\n"
        );
        assert!(!local_target.join("deps/crate.rmeta").exists());
    }

    #[test]
    fn quiet_rsync_omits_progress_until_verbose() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        s.verbose = false;
        let quiet = format!("{:?}", s.rsync_command());
        assert!(quiet.contains("--partial"), "{quiet}");
        assert!(!quiet.contains("--progress"), "{quiet}");
        assert!(!quiet.contains("-P"), "{quiet}");
        s.verbose = true;
        let verbose = format!("{:?}", s.rsync_command());
        assert!(verbose.contains("--progress"), "{verbose}");

        let quiet_mirror =
            lane_mirror_rsync("/stage", "/lane", &["coil_cache/".into()], false).unwrap();
        assert!(quiet_mirror.starts_with("rsync -a --partial --delete "));
        assert!(!quiet_mirror.contains("--progress"), "{quiet_mirror}");
        let verbose_mirror =
            lane_mirror_rsync("/stage", "/lane", &["coil_cache/".into()], true).unwrap();
        assert!(
            verbose_mirror.contains("rsync -a --partial --progress --delete "),
            "{verbose_mirror}"
        );
    }

    #[test]
    fn shell_env_can_be_turned_off_without_dropping_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = session(dir.path());
        let on = s.build_script(&[], "build", &[]).unwrap();
        assert!(on.contains("\"$SHELL\" -ilc"), "{on}");
        assert!(
            on.contains("[ -f ~/.cargo/env ] && . ~/.cargo/env;"),
            "{on}"
        );
        let remote = Box::leak(Box::new(Remote {
            shell_env: false,
            ..(*s.remote).clone()
        }));
        s.remote = remote;
        let off = s.build_script(&[], "build", &[]).unwrap();
        assert!(!off.contains("-ilc"), "{off}");
        assert!(
            off.contains("[ -f ~/.cargo/env ] && . ~/.cargo/env;"),
            "{off}"
        );
    }

    #[test]
    fn user_shell_path_reaches_cargo_and_stays_off_stdout() {
        if !cfg!(target_os = "linux") {
            return;
        }
        let home = tempfile::tempdir().unwrap();
        let home = home.path();
        std::fs::create_dir_all(home.join(".cargo/bin")).unwrap();
        std::fs::create_dir_all(home.join("cuda/bin")).unwrap();
        std::fs::write(
            home.join(".cargo/env"),
            "export PATH=\"$HOME/.cargo/bin:$PATH\"\n",
        )
        .unwrap();
        std::fs::write(
            home.join(".bashrc"),
            "case $- in\n  *i*) ;;\n  *) return ;;\nesac\nexport PATH=\"$HOME/cuda/bin:$PATH\"\necho bashrc-stdout\n",
        )
        .unwrap();
        std::fs::write(
            home.join(".profile"),
            "echo profile-stdout\nif [ -n \"$BASH_VERSION\" ]; then [ -f \"$HOME/.bashrc\" ] && . \"$HOME/.bashrc\"; fi\n",
        )
        .unwrap();
        std::fs::write(home.join("cuda/bin/nvcc"), "#!/bin/sh\necho nvcc\n").unwrap();
        std::fs::write(
            home.join(".cargo/bin/cargo"),
            "#!/bin/sh\nprintf 'NVCC=%s\\n' \"$(command -v nvcc || echo NO_NVCC)\"\nexit \"${CARGO_EXIT:-0}\"\n",
        )
        .unwrap();
        for name in ["cuda/bin/nvcc", ".cargo/bin/cargo"] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(home.join(name), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }

        let with_shell = format!(
            "{}[ -f ~/.cargo/env ] && . ~/.cargo/env; cargo",
            user_shell_env_script()
        );
        let output = Command::new("bash")
            .arg("-c")
            .arg(&with_shell)
            .env_clear()
            .env("HOME", home)
            .env("PATH", "/usr/bin:/bin")
            .env("SHELL", "/bin/bash")
            .env("TERM", "xterm")
            .output()
            .expect("bash");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stdout.contains(&format!("NVCC={}/cuda/bin/nvcc", home.display())),
            "stdout={stdout}\nstderr={stderr}"
        );
        assert!(!stdout.contains("bashrc-stdout"), "{stdout}");
        assert!(!stdout.contains("profile-stdout"), "{stdout}");
        assert!(!stdout.contains("sudo"), "{stdout}");
        assert!(!stderr.contains("no job control"), "{stderr}");

        let without = "[ -f ~/.cargo/env ] && . ~/.cargo/env; cargo";
        let bare = Command::new("bash")
            .arg("-c")
            .arg(without)
            .env_clear()
            .env("HOME", home)
            .env("PATH", "/usr/bin:/bin")
            .env("SHELL", "/bin/bash")
            .env("TERM", "xterm")
            .output()
            .unwrap();
        let bare_out = String::from_utf8_lossy(&bare.stdout);
        assert!(bare_out.contains("NVCC=NO_NVCC"), "{bare_out}");

        let failing = Command::new("bash")
            .arg("-c")
            .arg(&with_shell)
            .env_clear()
            .env("HOME", home)
            .env("PATH", "/usr/bin:/bin")
            .env("SHELL", "/bin/bash")
            .env("TERM", "xterm")
            .env("CARGO_EXIT", "9")
            .output()
            .unwrap();
        assert_eq!(failing.status.code(), Some(9));
    }

    #[test]
    fn copy_back_parent_path_maps_into_the_lane_and_rejects_escapes() {
        let lane = "/tmp/remote-builds/lanes/default/12345";
        let target = format!("{lane}/target");
        assert_eq!(
            resolve_remote_copy_path(&target, lane, "../simulations").unwrap(),
            format!("{lane}/simulations")
        );
        assert_eq!(
            resolve_remote_copy_path(&target, lane, "cli_db/").unwrap(),
            format!("{lane}/target/cli_db")
        );
        assert_eq!(resolve_remote_copy_path(&target, lane, "").unwrap(), target);
        // A `..` that cancels an intermediate directory is the same destination.
        assert_eq!(
            resolve_remote_copy_path(&target, lane, "foo/../../simulations").unwrap(),
            format!("{lane}/simulations")
        );

        for relative in ["..", "../..", "../../etc", "../simulations/../../outside"] {
            let err = resolve_remote_copy_path(&target, lane, relative)
                .unwrap_err()
                .to_string();
            assert!(err.contains("outside the build lane"), "{relative}: {err}");
            assert!(!err.contains("\n"), "{err}");
        }

        // The default temp dir keeps its tilde so the remote shell can expand it,
        // and `..` still cannot climb past that lane.
        let tilde_lane = "~/remote-builds/lanes/default/abc";
        let tilde_target = format!("{tilde_lane}/target");
        assert_eq!(
            resolve_remote_copy_path(&tilde_target, tilde_lane, "../simulations").unwrap(),
            format!("{tilde_lane}/simulations")
        );
        let tilde_escape =
            resolve_remote_copy_path(&tilde_target, tilde_lane, "../../../../../../etc")
                .unwrap_err()
                .to_string();
        assert!(tilde_escape.contains("resolves to ~/etc"), "{tilde_escape}");
        assert!(
            tilde_escape.contains("outside the build lane"),
            "{tilde_escape}"
        );

        let project = Path::new("/proj");
        let local_target = Path::new("/proj/target");
        assert_eq!(
            resolve_local_copy_path(local_target, project, "../simulations").unwrap(),
            PathBuf::from("/proj/simulations")
        );
        assert_eq!(
            resolve_local_copy_path(local_target, project, "release/app").unwrap(),
            PathBuf::from("/proj/target/release/app")
        );
        assert_eq!(
            resolve_local_copy_path(local_target, project, "").unwrap(),
            local_target
        );
        for relative in ["..", "../..", "../../outside"] {
            let err = resolve_local_copy_path(local_target, project, relative)
                .unwrap_err()
                .to_string();
            assert!(err.contains("outside the project"), "{relative}: {err}");
        }
        assert!(resolve_remote_copy_path(&target, lane, "bad\nname").is_err());
    }

    #[test]
    fn copy_back_of_a_parent_directory_deletes_only_inside_that_directory() {
        if !linux_rsync() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let lane = dir.path().join("lane");
        write_file(&project.join("Cargo.toml"), "[package]\nname = \"demo\"\n");
        write_file(&project.join("src/lib.rs"), "pub fn demo() {}\n");
        write_file(&project.join("notes.txt"), "project root file\n");
        write_file(&project.join("other/keep.txt"), "sibling dir\n");
        write_file(&project.join("target/debug/app"), "local artifact\n");
        write_file(
            &project.join("target/cli_db/local-only"),
            "stay in target\n",
        );
        // Different lengths: rsync -a skips a file whose size and mtime both
        // match, and these writes can land in the same second.
        write_file(&project.join("simulations/old.txt"), "old sim\n");
        write_file(&project.join("simulations/stale.txt"), "delete me\n");

        write_file(
            &lane.join("simulations/old.txt"),
            "new simulation content\n",
        );
        write_file(&lane.join("simulations/extra.txt"), "added from the lane\n");
        write_file(&lane.join("secret.txt"), "lane root secret\n");
        write_file(&lane.join("target/secret.txt"), "target secret\n");
        write_file(&lane.join("elsewhere/x.txt"), "not simulations\n");

        let lane_root = lane.display().to_string();
        let remote =
            resolve_remote_copy_path(&format!("{lane_root}/target"), &lane_root, "../simulations")
                .unwrap();
        let local =
            resolve_local_copy_path(&project.join("target"), &project, "../simulations").unwrap();
        assert_eq!(remote, format!("{lane_root}/simulations"));
        assert_eq!(local, project.join("simulations"));
        assert!(!remote.contains(".."), "{remote}");
        assert!(local
            .components()
            .all(|component| component != std::path::Component::ParentDir));

        let meta = dir.path().join("meta");
        write_file(&meta.join("Cargo.toml"), "");
        let mut session = session(&meta);
        session.upload_excludes = vec!["coil_cache/".to_string()];
        let filters = session.copy_back_filter_args().unwrap();
        run_rsync(&filters, Path::new(&remote), &local);

        assert_eq!(
            read_file(&project.join("simulations/old.txt")),
            "new simulation content\n"
        );
        assert_eq!(
            read_file(&project.join("simulations/extra.txt")),
            "added from the lane\n"
        );
        assert!(
            !project.join("simulations/stale.txt").exists(),
            "--delete still removes files inside simulations/"
        );
        assert_eq!(read_file(&project.join("notes.txt")), "project root file\n");
        assert_eq!(read_file(&project.join("other/keep.txt")), "sibling dir\n");
        assert_eq!(
            read_file(&project.join("target/debug/app")),
            "local artifact\n"
        );
        assert_eq!(
            read_file(&project.join("target/cli_db/local-only")),
            "stay in target\n"
        );
        assert!(!project.join("secret.txt").exists());
        assert_eq!(read_file(&lane.join("secret.txt")), "lane root secret\n");
        assert_eq!(
            read_file(&lane.join("target/secret.txt")),
            "target secret\n"
        );
        assert_eq!(
            read_file(&lane.join("elsewhere/x.txt")),
            "not simulations\n"
        );
    }

    #[test]
    fn copy_back_refuses_a_symlink_that_would_redirect_delete() {
        if !cfg!(unix) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let outside = dir.path().join("outside");
        write_file(&project.join("target/debug/app"), "artifact\n");
        write_file(&outside.join("secret.txt"), "do not delete\n");
        std::os::unix::fs::symlink(&outside, project.join("simulations")).unwrap();

        let err = reject_local_symlink_redirect(&project, &project.join("simulations"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("symlink"), "{err}");
        assert!(err.contains("simulations"), "{err}");
        assert_eq!(
            read_file(&outside.join("secret.txt")),
            "do not delete\n",
            "refusing the symlink must not touch the link target"
        );

        // A real directory at the same place is the path --delete is allowed to use.
        std::fs::remove_file(project.join("simulations")).unwrap();
        write_file(&project.join("simulations/old.txt"), "sim\n");
        reject_local_symlink_redirect(&project, &project.join("simulations")).unwrap();
    }

    #[test]
    fn remote_dir_probe_refuses_to_follow_a_symlink_out_of_the_lane() {
        if !cfg!(unix) {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let lane = dir.path().join("lane");
        let outside = dir.path().join("outside");
        write_file(&lane.join("simulations/old.txt"), "sim\n");
        write_file(&outside.join("secret.txt"), "secret\n");
        let simulations = lane.join("simulations");
        let script =
            confined_remote_dir_script(lane.to_str().unwrap(), simulations.to_str().unwrap());
        assert!(
            !script.contains('\n'),
            "the probe is one ssh argument: {script}"
        );

        let ok = Command::new("sh").arg("-c").arg(&script).output().unwrap();
        assert!(
            ok.status.success(),
            "a real directory is accepted: {}",
            String::from_utf8_lossy(&ok.stderr)
        );

        std::fs::remove_dir_all(&simulations).unwrap();
        std::os::unix::fs::symlink(&outside, &simulations).unwrap();
        let refused = Command::new("sh").arg("-c").arg(&script).output().unwrap();
        assert_eq!(
            refused.status.code(),
            Some(2),
            "stderr: {}",
            String::from_utf8_lossy(&refused.stderr)
        );
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("symlink"),
            "{}",
            String::from_utf8_lossy(&refused.stderr)
        );
        assert_eq!(read_file(&outside.join("secret.txt")), "secret\n");

        let missing = confined_remote_dir_script(
            lane.to_str().unwrap(),
            lane.join("absent").to_str().unwrap(),
        );
        let not_dir = Command::new("sh").arg("-c").arg(&missing).output().unwrap();
        assert_eq!(
            not_dir.status.code(),
            Some(1),
            "a missing path is 'not a directory', not a confinement failure: {}",
            String::from_utf8_lossy(&not_dir.stderr)
        );
    }
}
