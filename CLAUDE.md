# Working in this repo

## Build & lint efficiency (read first)

Full workspace builds, lints, and release builds in this repo are **very slow and expensive** (a clean `cargo build --release` or workspace-wide `cargo build`/`clippy` runs ~20+ minutes). Optimize for as few large builds as possible:

- **Batch all code changes first, then build once at the end.** Don't build/lint after every individual edit. Make the complete set of related changes across all files, then run a single build/test/lint pass. Saving the large build for the end of a change set is the default.
- **Prefer incremental, targeted cargo over workspace-wide.** Scope every command to the specific crate (and test/bench) you're touching:
  - Compile check: `cargo check -p <crate>` (e.g. `-p cayenne -p runtime`) — not `cargo check` (whole workspace).
  - Tests: `cargo test -p cayenne --lib <name_filter>` — not the full suite, while iterating.
  - Lint: `cargo clippy -p cayenne --no-deps` — scoped, not workspace-wide.
  - Use `cargo check` / `cargo build` (incremental, debug) to validate compilation before ever reaching for `--release`.
- **`cargo build --release -p spiced` is the expensive one — do it once**, only after the code is final and `cargo check`/targeted tests are green. Don't release-build to "see if it works"; use a targeted `cargo check` for that.
- **Sccache gotcha for benches/forks:** sccache points at an unwritable volume here and breaks the `aws-lc-sys` C compile. Bypass it for any cargo that hits that path: `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ SCCACHE_DIR=$HOME/.cache/sccache cargo …`.
- **One cargo at a time per target dir.** A second cargo invocation blocks on the target lock; two heavy builds/benches contend and (for benches) contaminate timings. Serialize them.
