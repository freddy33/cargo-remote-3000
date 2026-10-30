# Changelog

Earlier releases are recorded by their git tags (`v0.1.0` … `v0.4.0`).

## 0.5.0 — concurrent builds

Several builds can now share one build server without interfering and without
queueing. The unit of concurrency is a **lane**: one source tree plus its own
`target/`.

### Added

- **Lanes.** A build with no lane of its own takes `default`, then `default-2`,
  `default-3`, … up to `--max-lanes` (default 8). Contention moves a build to
  another lane instead of making it wait.
- `-l, --lane <NAME>` and `CARGO_REMOTE_3000_LANE`: claim one particular lane and
  wait for it, for watch sessions, benchmarks and CI.
- `--max-lanes <N>` and `--lock-timeout <SECS>`, the latter with `0` meaning
  "fail immediately instead of waiting".
- Per-run staging under `<temp_dir>/incoming/<hash>/<run-id>/`, outside every
  lane, so concurrent uploads cannot collide and a build that moves lanes does
  not upload again.
- Admission-free CPU sharing: `CARGO_BUILD_JOBS` is derived from the number of
  lanes already leased (`cores / (others + 1)`); an explicit value still wins.
- A bundled, hosting-service-agnostic agent skill in
  [`cargo-remote-build-skill/SKILL.md`](cargo-remote-build-skill/SKILL.md).
- A `toolchain` key in the config file, which selects the toolchain for builds
  using that remote.

### Changed

- Remote layout is now `<temp_dir>/lanes/<lane>/<hash>` (was
  `<temp_dir>/<hash>`). Existing warm trees can be moved with
  `mkdir -p <temp_dir>/lanes/default && mv <temp_dir>/<hash> <temp_dir>/lanes/default/`.
- `-d/--rustup-default` (alias `--toolchain`) now sets `RUSTUP_TOOLCHAIN` for one
  build instead of running `rustup default`, so a build can no longer re-point
  every other build's compiler on the same server.
- With no toolchain requested, the server's default applies and a project's
  `rust-toolchain.toml` takes precedence; the tool no longer implies `stable`.
- A missing toolchain is installed on demand, announced, and does not become the
  server's default.

### Fixed

- `toolchain` in the config file was silently ignored: it parsed, pinned nothing,
  and read like a guarantee.
- `--copy-back=<file>` failed with `ENOTDIR` because the remote path was always
  emitted with a trailing slash. The path is now probed and a file is copied as a
  file, a directory as its contents.
