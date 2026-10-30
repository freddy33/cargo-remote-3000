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

use crate::config::{shell_quote, Remote};
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
        cmd.arg("--delete");
        // Never upload a local target directory; the remote has its own.
        cmd.arg("--exclude").arg("target");

        if !self.transfer_hidden {
            cmd.arg("--exclude").arg(".*");
        }
        if self.no_transfer_git {
            // `.git` is hidden, so this only matters together with --transfer-hidden.
            cmd.arg("--exclude").arg(".git");
        }
        for pattern in &self.upload_excludes {
            cmd.arg("--exclude").arg(pattern);
        }

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
        cmd.arg("--exclude").arg("deps/");
        cmd.arg("--exclude").arg("build/");
        cmd.arg("--exclude").arg(".fingerprint/");
        cmd.arg("--exclude").arg("incremental/");

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
        script.push_str(&format!(
            "if ! rsync -a --delete --exclude target {stage}/ {root}/; then echo {msg} >&2; exit 4; fi; rm -rf {stage}; ",
            stage = shell_quote(&self.stage_root),
            root = shell_quote(root),
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
        let mirror = script.find("rsync -a --delete --exclude target").unwrap();
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
        assert!(mirror.starts_with(" --exclude target "), "{script}");
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
}
