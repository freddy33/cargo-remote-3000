//! cargo-remote-3000: run `cargo` on a remote build server and bring the
//! artifacts home.

mod cli;
mod config;
mod lane;
mod project;
mod remote;
mod watch;

use std::path::Path;
use std::process::ExitCode;

use anyhow::{bail, Result};
use clap::Parser;
use log::{debug, info};

use cli::{Cli, Command, Remote as RemoteArgs};
use config::Config;
use project::Project;
use remote::{Session, BUSY_EXIT};

/// Exit codes. A failing remote build propagates its own status instead.
mod exit {
    pub const USAGE: u8 = 2;
    pub const SETUP: u8 = 3;
    pub const TRANSFER: u8 = 4;
}

/// How many times a build may lose its lane to a concurrent build before the
/// tool gives up. Each retry picks a different lane, so this is a "something is
/// very wrong" bound rather than a queue.
const LANE_RETRIES: usize = 3;

/// An error plus the exit code it should produce.
struct Failure {
    code: u8,
    error: anyhow::Error,
}

impl Failure {
    fn new(code: u8, error: anyhow::Error) -> Self {
        Self { code, error }
    }
}

impl From<Failure> for ExitCode {
    fn from(value: Failure) -> Self {
        ExitCode::from(value.code)
    }
}

type Outcome = std::result::Result<ExitCode, Failure>;

fn main() -> ExitCode {
    init_logging();

    match run() {
        Ok(code) => code,
        Err(failure) => {
            // `{:#}` prints the whole context chain, which is what makes a
            // nested transfer error understandable.
            eprintln!("error: {:#}", failure.error);
            ExitCode::from(failure.code)
        }
    }
}

/// Set up logging before arguments are parsed.
///
/// `RUST_LOG` wins; otherwise `-v` on the command line raises the level to
/// debug, which is what prints each rsync and ssh invocation.
fn init_logging() {
    let verbose = std::env::args().any(|arg| arg == "-v" || arg == "--verbose");
    let level = match std::env::var("RUST_LOG").ok().as_deref() {
        Some("trace") | Some("debug") => log::LevelFilter::Debug,
        Some("warn") => log::LevelFilter::Warn,
        Some("error") => log::LevelFilter::Error,
        _ if verbose => log::LevelFilter::Debug,
        _ => log::LevelFilter::Info,
    };
    let _ = env_logger::Builder::new().filter_level(level).try_init();
}

fn run() -> Outcome {
    let Command::Remote(args) = Cli::parse().command;

    if args.verbose > 0 {
        debug!("parsed arguments: {args:?}");
    }

    let project = Project::resolve(&args).map_err(|error| Failure::new(exit::SETUP, error))?;

    let conf =
        Config::load(project.project_root()).map_err(|error| Failure::new(exit::SETUP, error))?;
    let remote = conf
        .get_remote(&args.remote)
        .map_err(|error| Failure::new(exit::SETUP, error))?;

    let project_hash = lane::project_hash(project.project_root());
    // A lane the caller asked for by name is a statement about which tree they
    // want, so it is kept and waited for. Without one, the server is asked for a
    // free lane and the build moves rather than queues.
    let requested = requested_lane(&args, &remote).map_err(|e| Failure::new(exit::USAGE, e))?;
    let run_id = lane::run_id();

    // A watch session owns its lane for as long as it runs, so it waits for it
    // rather than moving on every rebuild.
    if args.watch {
        let session = make_session(
            &remote,
            &project,
            &project_hash,
            requested.as_deref().unwrap_or(lane::DEFAULT_LANE),
            &run_id,
            true,
            &args,
        );
        return run_watch(&args, &session);
    }

    // Upload before a lane is chosen. The staging directory is lane independent,
    // so losing a lane race costs nothing but a retry.
    let mut session = make_session(
        &remote,
        &project,
        &project_hash,
        requested.as_deref().unwrap_or(lane::DEFAULT_LANE),
        &run_id,
        requested.is_some(),
        &args,
    );
    session
        .upload()
        .map_err(|error| Failure::new(exit::TRANSFER, error))?;

    let mut attempts = 0usize;
    let status = loop {
        let status = session
            .build(&args.build_env, &args.command, &args.args)
            .map_err(|error| Failure::new(exit::TRANSFER, error))?;

        if status.code() != Some(BUSY_EXIT as i32) || requested.is_some() {
            break status;
        }
        if attempts >= LANE_RETRIES {
            return Err(Failure::new(
                exit::TRANSFER,
                anyhow::anyhow!(
                    "every build lane for this project was taken by another build \
                     {attempts} times in a row; raise --max-lanes, or pass --lane to \
                     claim a tree of your own"
                ),
            ));
        }
        attempts += 1;

        let allocation =
            remote::allocate(&remote, &project_hash, lane::DEFAULT_LANE, remote.max_lanes)
                .map_err(|error| Failure::new(exit::TRANSFER, error))?;
        let Some(allocation) = allocation else {
            return Err(Failure::new(
                exit::TRANSFER,
                anyhow::anyhow!(
                    "all {} lanes of this project are in use by other builds; wait for \
                     one to finish, raise --max-lanes, or pass --lane to claim a tree",
                    remote.max_lanes
                ),
            ));
        };
        info!(
            "Lane {} was taken by another build; moving to lane {}",
            session.lane, allocation.lane
        );
        session = make_session(
            &remote,
            &project,
            &project_hash,
            &allocation.lane,
            &run_id,
            false,
            &args,
        );
    };

    copy_back(&args, &session).map_err(|error| Failure::new(exit::TRANSFER, error))?;

    if !status.success() {
        info!("Remote build exited with status {status}");
        // Mirror the remote status so CI and shell scripts see the truth.
        let code = status
            .code()
            .and_then(|c| u8::try_from(c).ok())
            .unwrap_or(exit::SETUP);
        return Ok(ExitCode::from(code));
    }

    Ok(ExitCode::SUCCESS)
}

/// Build the session description for one lane.
fn make_session<'a>(
    remote: &'a config::Remote,
    project: &'a Project,
    project_hash: &str,
    lane: &str,
    run_id: &str,
    wait_for_lane: bool,
    args: &RemoteArgs,
) -> Session<'a> {
    let build_root = lane::lane_root(&remote.temp_dir, lane, project_hash);
    Session {
        remote,
        project_root: project.project_root(),
        workspace_root: project.workspace_root(),
        lane: lane.to_string(),
        project_hash: project_hash.to_string(),
        build_root: build_root.clone(),
        remote_workspace_root: join_remote(&build_root, project.workspace_relative()),
        remote_crate_dir: join_remote(&build_root, project.crate_relative()),
        local_target_dir: project.local_target_dir().to_path_buf(),
        remote_target_dir: project.remote_target_dir(&build_root),
        stage_root: lane::staging_root(&remote.temp_dir, project_hash, run_id),
        run_id: run_id.to_string(),
        wait_for_lane,
        upload_excludes: remote.exclude.clone(),
        transfer_hidden: args.transfer_hidden,
        no_transfer_git: args.no_transfer_git,
        lock_timeout: remote.lock_timeout,
        verbose: args.verbose > 0,
    }
}

/// Copy artifacts and the lock file home.
fn copy_back(args: &RemoteArgs, session: &Session) -> Result<()> {
    if let Some(relative) = &args.copy_back {
        let relative = relative.as_deref().unwrap_or("");
        info!(
            "Copying target/{relative} back to {}",
            session.local_target_dir.display()
        );
        session.download_target(relative)?;
    }

    if !args.no_copy_lock {
        // A missing lock file is not a reason to fail the build.
        if let Err(error) = session.download_lock_file() {
            debug!("skipping Cargo.lock transfer: {error:#}");
        }
    }

    Ok(())
}

/// Watch mode: one build, then a rebuild on every source change.
fn run_watch(args: &RemoteArgs, session: &Session) -> Outcome {
    if args.copy_back.is_some() {
        return Err(Failure::new(
            exit::USAGE,
            anyhow::anyhow!(
                "--watch rebuilds continuously and the artifacts stay on the build \
                 server, so --copy-back is not supported in watch mode"
            ),
        ));
    }

    let run_build = || -> Result<()> {
        session.upload()?;
        let status = session.build(&args.build_env, &args.command, &args.args)?;
        if !status.success() {
            bail!("remote build failed with {status}");
        }
        Ok(())
    };

    if let Err(error) = run_build() {
        // A first build failure is worth reporting loudly.
        return Err(Failure::new(exit::TRANSFER, error));
    }

    match watch::watch(session.project_root, &session.upload_excludes, run_build) {
        Ok(rebuilds) => {
            info!("Watch session ended after {rebuilds} rebuilds");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Err(Failure::new(exit::SETUP, error)),
    }
}

/// Resolve the lane the caller asked for by name, if any.
///
/// `--lane` wins over `$CARGO_REMOTE_3000_LANE`, which wins over the config file.
/// The environment variable matters because it lets a fleet give every agent its
/// own lane once, in its shell, without threading a flag through every Makefile.
/// No name at all is the interesting case: it means "give me any free tree".
fn requested_lane(args: &RemoteArgs, remote: &config::Remote) -> Result<Option<String>> {
    let lane = args
        .remote
        .lane
        .clone()
        .or_else(|| {
            std::env::var("CARGO_REMOTE_3000_LANE")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| remote.lane.clone());

    if let Some(name) = &lane {
        lane::validate_lane(name)?;
    }
    Ok(lane)
}

/// Join a remote path fragment onto a remote base directory.
fn join_remote(base: &str, relative: &Path) -> String {
    if relative.as_os_str().is_empty() {
        return base.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        relative.to_string_lossy()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_remote_handles_the_workspace_root_itself() {
        assert_eq!(join_remote("/remote/abc", Path::new("")), "/remote/abc");
        assert_eq!(
            join_remote("/remote/abc", Path::new("crates/foo")),
            "/remote/abc/crates/foo"
        );
    }
}
