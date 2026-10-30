---
name: cargo-remote-build
description: Use when compiling Rust on a shared remote build server over SSH with cargo-remote-3000, when several agents or CI jobs build at the same time on one host, when a build reports a busy lane or a timeout, or when a build needs a Rust toolchain the host does not have installed. Covers lanes, no-wait concurrency, toolchain profiles, and the traps that make remote builds look broken.
---

# Remote Rust builds with cargo-remote-3000

One SSH-reachable host builds Rust projects on request, and several builds may
run on it at the same time. This skill is the operational contract. The tool
enforces it, so most of the work is *not* working around the tool.

Nothing here is specific to a hosting provider. The build server needs a POSIX
shell, `rsync`, `flock` (util-linux) and a Rust toolchain reachable from a
non-login shell. Any host that gives you those over SSH will do.

## The one rule

Every remote compile goes through `cargo remote-3000`, or through a documented
project target that wraps it (`make remote-test`, `make check` with a `CARGO`
override, and so on).

Do **not**:

- run `cargo` directly on the build host, or keep a shell there for building;
- create your own lock file, queue, or per-agent build directory — that is the
  tool's job, and a private one deadlocks against every other agent's;
- clean anything on the host that is not your own project's lane;
- start, stop, resize, or otherwise manage the host from a build context. Host
  lifecycle belongs to whoever owns the host, not to the build.

## What makes concurrency safe

Five properties, all of them automatic:

1. **A lane per concurrent build.** A lane is one source tree plus its own
   `target/` on the host. An invocation with no lane of its own takes `default`;
   if that is busy it takes `default-2`, `default-3`, and so on. **Builds do not
   queue for a lane; they move to another one.**
2. **Private uploads.** Sources are uploaded to a per-run staging directory
   outside every lane, so two uploads can never overwrite each other. The stage
   is lane-independent, so a build that moves lanes does not upload again.
3. **A lease per lane.** Mirroring the sources and compiling happen under one
   exclusive `flock` on `{lane}/{project-hash}.lock`. A killed client releases it
   by dying: the remote shell goes with it.
4. **A fair share of the CPU.** Before compiling, a build counts the lanes that
   are already leased and exports `CARGO_BUILD_JOBS = cores / (others + 1)`, so
   four concurrent builds get four cores between them instead of sixteen threads.
   An explicit `CARGO_BUILD_JOBS` in `--build-env` always wins.
5. **A toolchain per invocation.** `RUSTUP_TOOLCHAIN` is set for that build's
   process tree only. The host's global rustup default is never written, so one
   build cannot re-point another's compiler.

Everything except the compile itself is shared and safe: the crates.io download
cache is content addressed and cargo locks it.

## Commands

```bash
# the plain build
cargo remote-3000 -r <remote> -d <toolchain> -- build --release

# check, clippy, test — nothing comes home without --copy-back
cargo remote-3000 -r <remote> -d <toolchain> -- check --workspace --all-targets
cargo remote-3000 -r <remote> -d <toolchain> -- test --workspace

# one artifact back to the local target directory
cargo remote-3000 -r <remote> -d <toolchain> -- build -p my-app -c=debug/my-app
```

`--` before the cargo arguments is not optional. cargo-remote parses its own
flags *after* the cargo subcommand too, and `-p` is its `--ssh-port`, so
`... test -p my-app` fails with `invalid value 'my-app' for '--ssh-port <PORT>'`.

Long builds block, so run them detached and watch the log rather than holding a
terminal:

```bash
setsid nohup sh -c 'cargo remote-3000 -r <remote> -d <toolchain> -- test --workspace \
  > /tmp/remote.log 2>&1; echo "EXIT=$?" >> /tmp/remote.log' >/dev/null 2>&1 &
```

## Lanes

| Flag / environment | Meaning |
| --- | --- |
| nothing | take any free lane: `default`, then `default-2`, ... |
| `-l alice` | claim the lane `alice` and **wait** for it if busy |
| `CARGO_REMOTE_3000_LANE=alice` | same as `-l alice`, set once per shell |
| `--max-lanes N` | how far a project may fan out (default 8) |
| `--lock-timeout S` | seconds to wait for a named lane (default 3600); `0` fails immediately |

Choose **no lane** unless you have a reason. An automatically allocated lane is
never waited for: it is replaced. Name a lane only when you want one particular
warm tree — a long-running watch session, a benchmark that must compare like with
like, or a CI job that should not silently change trees between runs.

Exit code **75** means "the lane was taken; the build never started". The tool
reacts to it by allocating another lane, so you only see it if every lane is
already in use, which is the one case where more concurrency of this project
cannot help.

## Toolchain profiles

One host can hold as many toolchains as you like: rustup installs them into a
shared `RUSTUP_HOME`, and each build selects one. There is no global profile to
keep in sync and nothing to switch before a build.

- `-d 1.95.0` (or `--toolchain`, or a `toolchain` key in the config file) selects
  a version for that build.
- A toolchain the host does not have is installed on demand, announced on stderr,
  and does not become anyone's default. The first build after a new version is
  therefore slower than the rest.
- With no toolchain requested, the host's own default applies — and if the
  repository has a `rust-toolchain.toml`, rustup honours it. Prefer that file for
  a per-repository pin: it travels with the code and needs no flags.
- If the host installs Rust through something else (mise, nix, a distro package),
  pass no `-d`; the tool only needs a working `cargo` on the host's `PATH`.

## Pitfalls

- **A build that "hangs" is usually not the host.** `Blocking waiting for file
  lock on build directory` means two builds share a lane, which only happens if
  something bypassed the wrapper or named the same lane twice. Stop and report;
  do not retry in a loop.
- **A slow first build is not broken.** A cold dependency graph is 20–40 minutes
  (SQLite, RocksDB, WASM stacks). Check that the host is still compiling before
  concluding anything.
- **Path dependencies outside the upload do not travel.** cargo resolves path
  dependencies for every platform, so a crate pointing at `../../libs` fails
  before compiling anything. Use `-w <ancestor>` to upload a wider tree.
- **`--copy-back` is expensive.** `-c` alone copies the whole target tree; prefer
  `-c=<path>` for one artifact. `deps/`, `build/`, `.fingerprint/` and
  `incremental/` are never copied back.
- **The config `env` key is a list of files to source, not `KEY=VALUE`.** Setting
  it to a variable name replaces `~/.cargo/env` and every build then fails with
  `cargo: command not found`. Use `-b KEY=value` for variables.
- **A toolchain key is honoured, a typo is not.** An unknown key in the config is
  silently ignored, so a pin that lives in a misspelled field reads like a
  guarantee and pins nothing. Verify with `-v`.
- **`flock` missing on the host** prints a warning and drops the lease. Builds
  still run, but two builds of one lane can then interfere. Report it; do not
  build a workaround.
- **Do not reclaim disk yourself.** Lane trees are warm caches; deleting another
  project's lane is indistinguishable from deleting its build. Ask the host owner.

## Verification

```bash
# what the tool will actually do
cargo remote-3000 --help

# two builds at once, proving they got different trees
cargo remote-3000 -r <remote> -d <toolchain> -- check 2>&1 | grep 'Build path' &
cargo remote-3000 -r <remote> -d <toolchain> -- check 2>&1 | grep 'Build path' &
wait
# expect two different ".../lanes/<lane>/<hash>" paths

# confirm the toolchain is per-build, not global
cargo remote-3000 -r <remote> -d <toolchain> -v -- check 2>&1 | grep RUSTUP_TOOLCHAIN
```

A green remote build proves the compile. It does not prove the host was idle, the
tree was warm, or another agent's lane was untouched — say what you ran, not what
you assume.
