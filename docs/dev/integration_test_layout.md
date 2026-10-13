# Integration test layout

Each crate compiles its integration tests (`tests/`) into **one test binary**, `integration`, with one module per test file. A test gets a binary of its own only for a reason that is about the process or the link, never because it changes process-global state.

## Why one binary

Cargo builds every top-level `tests/<name>.rs` as a separate executable, and each one statically links the crate and its whole dependency graph. With one binary per file the link step and the disk footprint grow with the number of test files, not with the code under test.

Measured on `cayenne` when its 104 test binaries were folded into one (#14976; 18-core macOS, `cargo test -p cayenne --no-run --tests`, two runs each):

| | Before (104 binaries) | After (1 binary) |
|---|---|---|
| Rebuild after a library edit, wall | 888 s / 1000 s | 116 s / 128 s |
| Rebuild after a library edit, CPU | 6723 s / 7078 s | 398 s / 413 s |
| Rebuild after a test-file edit, wall | 31.5 s | 38.0 s |
| Test executables on disk | 69.2 GB | 3.0 GB |

The one cost is that editing a test file relinks the whole binary instead of one small one.

## The layout

```toml
# Cargo.toml
[package]
autotests = false   # compile only the test targets declared below

[[test]]
name = "integration"
path = "tests/integration.rs"
```

```rust
// tests/integration.rs: one `mod` per test file
mod common;
mod refresh_test;
#[cfg(feature = "postgres")]
mod postgres_cdc_test;
```

- **Gate a module with `#[cfg(feature = …)]` on its `mod` line**, not with `required-features` on a target. `required-features` would drop the whole binary when the feature is off.
- **Shared helpers live in `tests/common/mod.rs`** (or any directory module) and are declared once in `tests/integration.rs`. Test modules reach them through `crate::common`, never with a second `mod common;`.
- **A test file in a subdirectory** (`tests/cdc/heartbeat.rs`) is compiled only if its parent module declares it (`mod heartbeat;` in `tests/cdc/mod.rs`).
- **Snapshot names change with the module path.** insta derives the name from `module_path!()`, so snapshots of a test in `integration`'s `foo_test` module are named `integration__foo_test__…`.

`make lint-rust` runs `scripts/check_module_reachability.py`. It fails on any `.rs` file under a crate's `src/`, `tests/`, `benches/` or `examples/` that no target's module tree reaches, so a new test file that is never declared fails lint instead of silently running nothing.

## Process-global state: nextest, not a separate binary

nextest runs every test in a process of its own, so process-global state does not leak between tests: environment variables, a CPU or memory budget, a meter provider, a `tracing` subscriber. Plain `cargo test` runs a binary's tests as threads of one process, where that state does leak.

A test that changes such state stays in `integration`. It calls the crate's `common::require_process_per_test(why)` (`utils::require_process_per_test` in `runtime`) before changing anything. Under `cargo test` the guard fails with instructions instead of corrupting its neighbours:

```rust
#[test]
fn budget_is_applied_at_startup() {
    common::require_process_per_test("this test sets the process-wide memory budget");
    // …
}
```

CI runs integration tests with `cargo nextest run`; `make test-integration` does too. Run one module locally with:

```bash
cargo nextest run -p cayenne --test integration -E 'test(/^refresh_test::/)'
```

## When a test gets its own binary

Only for a reason that one process or one link cannot satisfy. Each separate `[[test]]` carries a comment in `Cargo.toml` naming its reason:

- **A custom `#[global_allocator]`.** Only one can be linked into a binary, so it would apply to every test. Heap-measurement tests that share one counting allocator share one binary (`cayenne`'s `isolated`, `runtime`'s `results_cache_growth`, `vortex`'s `footer_cache_accounting`).
- **A custom `main`** (`harness = false`).
- **Libraries that cannot share a process.** chDB and DuckDB cannot both be linked into one process, so `cayenne`'s chDB result-correctness lane is its own binary.
- **macOS can't load runtime's `integration` binary in the sign-off gate.** On the macOS sign-off runner, loading it aborts with `dyld cache '(null)' not loaded: syscall to map cache into shared region failed`. Tests the gate runs therefore stay out of it (`rate_control`, `metrics`, `cayenne_runtime_tuning`).
- **A CI step of its own**, with its own credentials, feature set or retry budget (`runtime`'s `integration_aws_sdk`, `integration_snapshot_refresh`, `integration_models`, `retention_oom`).

## nextest selection and overrides

Select a module of the combined binary by test-name prefix:

```text
binary_id(=cayenne::integration) & test(/^mutation_property_test::/)
```

Use the same form in `NEXTEST_FILTER` (`Makefile`), in workflow `-E` filters, and in `.config/nextest.toml` overrides. `.github/scripts/check_nextest_config.py` keys per-module rules as `<package>::integration::<module>` (for example `runtime::integration::result_correctness`). An override written for a whole binary applies to every module in it.
