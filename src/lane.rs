//! Lane layout, run identifiers and lane-name rules.
//!
//! # Why lanes exist
//!
//! A lane is one independent source tree plus `target/` on the build server. It
//! is the unit of concurrency, and it exists because the alternative — one tree
//! per local checkout, shared by every invocation — cannot run two builds at the
//! same time. cargo-remote does not lock the tree, the upload is
//! `rsync --delete`, and cargo holds its own exclusive build-directory lock, so
//! a second build either rewrites the first one's sources mid-compile or sits in
//! an invisible wait.
//!
//! # No build waits for another
//!
//! Lanes are allocated automatically: an invocation with no lane of its own
//! takes `default`, and if that is busy it takes `default-2`, `default-3`, and
//! so on up to [`DEFAULT_MAX_LANES`]. Concurrency therefore costs nothing to
//! ask for — a fleet of agents in one checkout gets one tree each without any
//! configuration and without queueing.
//!
//! Every lane stays warm between builds, so the overflow lane a build landed in
//! yesterday is a fast tree for whoever lands in it today. A lane is only
//! *named* when the caller wants a specific one (`--lane`, or
//! `$CARGO_REMOTE_3000_LANE`), and a named lane is deliberate: it waits rather
//! than silently relocating.
//!
//! # Layout
//!
//! ```text
//! {temp_dir}/lanes/{lane}/{hash}        one tree per lane
//! {temp_dir}/lanes/{lane}/{hash}.lock   its lease
//! {temp_dir}/incoming/{hash}/{run_id}   per-run staging, lane independent
//! ```
//!
//! Staging is deliberately outside the lane: uploads never touch a shared tree,
//! and a build that has to move to another lane (because another invocation won
//! the race for the one it picked) reuses the same upload.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};

/// Longest accepted lane name. Keeps remote paths well inside filesystem limits.
const MAX_LANE_LEN: usize = 64;

/// Lane used when the caller names none.
pub const DEFAULT_LANE: &str = "default";

/// How many lanes one project may fan out to before a build has to wait.
///
/// This is a safety rail, not a queue: eight concurrent compiles of the same
/// workspace on one server is already past the point where more concurrency
/// helps. Below it, nothing ever waits.
pub const DEFAULT_MAX_LANES: usize = 8;

/// Reject lane names that could escape the temp directory or confuse a shell.
///
/// A lane name becomes a path component on the build server and is interpolated
/// into the remote shell script, so the allowed set is deliberately narrow:
/// letters, digits, `.`, `_` and `-`, starting with a letter or digit, and never
/// `.` or `..` as a whole name.
pub fn validate_lane(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("lane name must not be empty");
    }
    if name.len() > MAX_LANE_LEN {
        bail!(
            "lane name {:?} is {} characters long; the limit is {MAX_LANE_LEN}",
            name,
            name.len()
        );
    }
    if name == "." || name == ".." {
        bail!("lane name {name:?} is a directory reference, not a name");
    }
    if name.contains("..") {
        bail!("lane name {name:?} must not contain `..`");
    }
    let mut bytes = name.bytes();
    let first = bytes.next().unwrap_or(b'0');
    if !first.is_ascii_alphanumeric() {
        bail!("lane name {name:?} must start with a letter or digit");
    }
    if let Some(bad) = bytes.find(|b| !(b.is_ascii_alphanumeric() || b"._-".contains(b))) {
        bail!(
            "lane name {name:?} contains the unsupported character {:?}; \
             use letters, digits, `.`, `_` or `-`",
            bad as char
        );
    }
    Ok(())
}

/// The lane a candidate index refers to.
///
/// Index 0 is the base lane itself; later indexes append a suffix, so overflow
/// lanes read as `default-2`, `default-3`, ... and are still valid lane names.
pub fn candidate_lane(base: &str, index: usize) -> String {
    if index == 0 {
        base.to_string()
    } else {
        format!("{base}-{}", index + 1)
    }
}

/// Stable hash of the local checkout path.
///
/// Hashing the path means two checkouts of the same project on different
/// machines still land in one remote directory, and re-running after a reboot
/// reuses the previous target directory instead of rebuilding from scratch.
pub fn project_hash(project_root: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    project_root.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// The root of every lane for one build server.
///
/// `~` is left for the *remote* shell to expand: the build host's home
/// directory has nothing to do with the local one.
fn lanes_root(temp_dir: &str) -> String {
    format!("{}/lanes", temp_dir.trim_end_matches('/'))
}

/// Remote tree for one lane of one project.
pub fn lane_root(temp_dir: &str, lane: &str, project_hash: &str) -> String {
    format!("{}/{lane}/{project_hash}", lanes_root(temp_dir))
}

/// Remote directory holding the per-run staging trees of one project.
///
/// Lane independent on purpose: the upload happens before a lane is won, and a
/// build that loses a lane race reuses the same staged sources on its retry.
pub fn incoming_root(temp_dir: &str, project_hash: &str) -> String {
    format!("{}/incoming/{project_hash}", temp_dir.trim_end_matches('/'))
}

/// Per-invocation staging directory.
pub fn staging_root(temp_dir: &str, project_hash: &str, run_id: &str) -> String {
    format!("{}/{run_id}", incoming_root(temp_dir, project_hash))
}

/// A token unique to this invocation on this machine.
///
/// Only has to be unique among concurrent uploads of one project, so the pid
/// plus a nanosecond timestamp is enough. Deliberately free of `:` and `/`: it
/// becomes a path component on the build server.
pub fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{}-{:x}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash() -> String {
        project_hash(Path::new("/home/me/project"))
    }

    #[test]
    fn every_lane_gets_its_own_tree() {
        let root = lane_root("/data/builds", "alice", &hash());
        assert_eq!(root, format!("/data/builds/lanes/alice/{}", hash()));
    }

    #[test]
    fn two_lanes_of_one_project_never_share_a_tree() {
        let a = lane_root("/t", "alice", &hash());
        let b = lane_root("/t", "bob", &hash());
        assert_ne!(a, b);
    }

    #[test]
    fn two_projects_in_one_lane_never_share_a_tree() {
        let a = lane_root("/t", "alice", &project_hash(Path::new("/p/one")));
        let b = lane_root("/t", "alice", &project_hash(Path::new("/p/two")));
        assert_ne!(a, b);
    }

    #[test]
    fn candidate_lanes_are_ordered_and_valid() {
        assert_eq!(candidate_lane("default", 0), "default");
        assert_eq!(candidate_lane("default", 1), "default-2");
        assert_eq!(candidate_lane("alice", 2), "alice-3");
        // Overflow must not produce a name the validator would reject.
        validate_lane(&candidate_lane("default", DEFAULT_MAX_LANES - 1)).unwrap();
    }

    #[test]
    fn every_candidate_is_a_distinct_tree() {
        let roots: Vec<String> = (0..DEFAULT_MAX_LANES)
            .map(|i| lane_root("/t", &candidate_lane("default", i), &hash()))
            .collect();
        let unique: std::collections::HashSet<&String> = roots.iter().collect();
        assert_eq!(unique.len(), roots.len(), "{roots:#?}");
    }

    #[test]
    fn staging_is_lane_independent_and_outside_every_lane() {
        // A retry on a different lane must reuse the upload, and no lane's
        // `--delete` mirror may be able to reach the stage.
        let stage = staging_root("/data/builds", &hash(), "run-1");
        assert_eq!(stage, format!("/data/builds/incoming/{}/run-1", hash()));
        for i in 0..DEFAULT_MAX_LANES {
            let lane = candidate_lane("default", i);
            let root = lane_root("/data/builds", &lane, &hash());
            assert!(!stage.starts_with(&format!("{root}/")), "{stage} in {root}");
        }
    }

    #[test]
    fn temp_dir_trailing_slashes_are_normalised() {
        assert_eq!(
            lane_root("/data/builds/", "alice", &hash()),
            lane_root("/data/builds", "alice", &hash())
        );
        assert!(!lane_root("/data/builds/", "alice", &hash()).contains("//"));
        assert!(!staging_root("/d/", &hash(), "r").contains("//"));
    }

    #[test]
    fn the_tilde_is_left_for_the_remote_shell() {
        let root = lane_root("~/remote-builds", "alice", &hash());
        assert!(root.starts_with("~/remote-builds/lanes/alice/"), "{root}");
    }

    #[test]
    fn run_ids_are_unique_across_calls() {
        assert_ne!(run_id(), run_id());
    }

    #[test]
    fn run_ids_are_safe_path_components() {
        let id = run_id();
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b)),
            "{id}"
        );
    }

    #[test]
    fn reasonable_lane_names_are_accepted() {
        for name in ["alice", "m2-core", "agent_7", "pr24", "1.95", "a"] {
            validate_lane(name).unwrap_or_else(|e| panic!("{name} should be valid: {e}"));
        }
        validate_lane(&"a".repeat(MAX_LANE_LEN)).unwrap();
    }

    #[test]
    fn path_and_shell_hazards_are_rejected() {
        for name in [
            "", ".", "..", "../etc", "a/b", "a b", "a;b", "a$(id)", "a'b", "a\nb", "-lead",
            "_lead", ".hidden",
        ] {
            assert!(validate_lane(name).is_err(), "{name:?} should be rejected");
        }
        assert!(validate_lane(&"a".repeat(MAX_LANE_LEN + 1)).is_err());
    }
}
