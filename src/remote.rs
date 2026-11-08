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
    pub fn download_target(&self, relative: &str) -> Result<()> {
        let remote_target = self
            .remote_target_dir
            .clone()
            .unwrap_or_else(|| format!("{}/target", self.remote_crate_dir));

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
        let relative = relative.trim_matches('/');
        let (remote_path, local_path) = if relative.is_empty() {
            (
                format!("{}:{}/", self.remote.host, remote_target),
                format!("{}/", self.local_target_dir.display()),
            )
        } else if self.remote_is_dir(&format!("{remote_target}/{relative}"))? {
            (
                format!("{}:{}/{}/", self.remote.host, remote_target, relative),
                format!("{}/{}/", self.local_target_dir.display(), relative),
            )
        } else {
            let local = self.local_target_dir.join(relative);
            // rsync will not create intermediate directories for a single file.
            if let Some(parent) = local.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create the local directory {}", parent.display())
                })?;
            }
            (
                format!("{}:{}/{}", self.remote.host, remote_target, relative),
                local.display().to_string(),
            )
        };
        cmd.arg(remote_path);
        cmd.arg(local_path);

        self.run(&mut cmd, "copy the target directory back")
    }

    /// True when `path` is a directory on the build server.
    ///
    /// `--copy-back` needs the answer to choose between "copy these contents"
    /// and "copy this file", and guessing wrong fails the transfer: a trailing
    /// slash on a file is `ENOTDIR`, and no trailing slash on an existing
    /// directory nests it inside itself on the next run.
    fn remote_is_dir(&self, path: &str) -> Result<bool> {
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
        cmd.arg(&self.remote.host)
            .arg(format!("test -d {}", shell_quote(path)));
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        if self.verbose {
            debug!("running: {cmd:?}");
        }
        // `test -d` exits 1 for "not a directory". A failed ssh (255) also lands
        // here and surfaces as a transfer error below, which is accurate enough:
        // the artifact could not be located.
        Ok(cmd
            .status()
            .with_context(|| format!("failed to run `ssh` to {}", self.remote.host))?
            .success())
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

        // Issue #26: `source` is not POSIX, so on a dash/sh build host it fails
        // and the rest of the line runs without the environment set up. `.` is
        // portable, and the file is only sourced when it actually exists.
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
        let mirror = lane_mirror_rsync(&self.stage_root, root, &self.upload_excludes)?;
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
        // `-a` preserves permissions and times, `-P` keeps partial transfers
        // resumable and shows per-file progress. `-P` is deliberately used
        // instead of `--info=progress2`, which only exists in rsync >= 3.1 and
        // made the BSD rsync shipped with macOS fail outright (issue #2).
        cmd.arg("-aP").arg("--compress").arg("--safe-links");
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
fn lane_mirror_rsync(stage: &str, root: &str, excludes: &[String]) -> Result<String> {
    let mut cmd = format!(
        "rsync -a --delete --exclude={}",
        shell_quote_literal("target")
    );
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
        let mirror = script.find("rsync -a --delete --exclude='target'").unwrap();
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
            .split("rsync -a --delete")
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
        let script =
            lane_mirror_rsync(stage.to_str().unwrap(), lane.to_str().unwrap(), excludes).unwrap();
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
}
