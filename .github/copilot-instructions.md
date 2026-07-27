# Spice.ai Agent Instructions

Spice is a SQL query, search, and LLM-inference engine in Rust for data apps and agents: federated SQL, data acceleration/materialization, vector/keyword/full-text search, and AI inference via industry-standard APIs. Rust CLI (`bin/spice`) + runtime daemon (`bin/spiced`), built on Apache DataFusion, Arrow, and DuckDB; configured via Spicepods (YAML). Core principle: developer experience first — bring data and AI to the application, not the other way around.

## Data correctness — absolute top priority

As an AI-native database, query results can NEVER be wrong. Correctness supersedes performance, developer experience, and feature velocity. Verify transformations preserve integrity (row counts, key values); rigorously test NULLs, empty sets, boundaries, type coercions, and overflow; when uncertain, return a structured error instead of possibly-wrong data. Never corrupt data or drop errors silently.

## Build, test, lint (expensive — read first)

Full workspace and release builds take 20–35 minutes. Minimize large builds:

- **Batch all related edits first, then run one build/lint/test pass at the end.** Never build after each edit.
- **Scope cargo to touched crates**: `cargo check -p <crate>`, `cargo test -p <crate> --lib <filter>`, `cargo clippy -p <crate> --no-deps`. Validate compilation with incremental `cargo check`; run `cargo build --release -p spiced` once, only after everything is green.
- **New crates must opt into the workspace lints**: clippy lint levels live in `[workspace.lints.clippy]` in the root `Cargo.toml` (pedantic + `unwrap_used`/`expect_used`/`clone_on_ref_ptr`/…). Every member crate inherits them via `[lints]\nworkspace = true` in its `Cargo.toml` — add this to any new crate, or scoped `cargo clippy -p <crate>` and rust-analyzer will silently under-lint it. Additional shared lint config belongs in `[workspace.lints.*]` in the root `Cargo.toml` (a crate can’t inherit with `workspace = true` and also define per-crate `[lints.*]` overrides). `make lint-rust` re-applies the flags over `--workspace` as a backstop, so a forgotten opt-in fails CI, not silently ships.
- **Fix the feature set for the whole session.** Cargo re-fingerprints incremental artifacts on the exact `--features` (and profile, and `RUSTFLAGS`/wrapper), and features flow through the entire dependency graph — so alternating flag sets between `check`/`test`/`clippy`, or diverging from the `make lint-rust` gate's `--features adbc,…,release,…`, silently forces full recompiles. At the start of a branch, identify the features your touched crates need, reuse the *same* `--features` on every `cargo` invocation, and scope the gate to match: `make lint-rust-fix PACKAGES="<crates>" FEATURES="<same set>"`. Keep the profile and the sccache-bypass env constant for the same reason. (`clippy` and `check` still recompile the workspace crates when alternated even with matching features — deps are reused, so the win is still large.)
- **One cargo invocation at a time**: a second blocks on the target-dir lock, and concurrent heavy builds contaminate bench timings.
- **Lint covers tests too**: `make lint-rust` runs a second clippy pass over `--tests` with pedantic lints, so test code and its doc comments must pass (e.g. `doc_markdown`: backtick product names like `PostgreSQL`, `DuckDB`). Green tests ≠ lint green; the scoped clippy above now inherits the same lint *levels*, but `make lint-rust` is still the gate (it adds the full release feature set + the `--tests` pass) — run `make lint-rust-fix` before pushing.
- **Sign off after pushing** (the script attests your pushed HEAD and refuses an unpushed branch): `make signoff` first target-lints and unit-tests crates touched by the branch (`make lint-rust PACKAGES=… FEATURES=…`, `make nextest-packages PACKAGES=… FEATURES=…`, with features taken from a workspace `cargo metadata` resolve so a scoped run compiles the crate the way the workspace does — a crate's own defaults are not what it ships with; `runtime` declares no `default` at all) so your own crate fails fast, then runs the full gate (`make lint-rust` + `make build-cli-dev nextest`) and records the attestation that gates merge-queue entry — a single `Attestation` PR check; the full suite then runs once in the merge queue (see `docs/dev/ci_signoff.md`). `Attestation` auto-passes without a sign-off for branches with no Rust-affecting files, pure reverts, and single-commit Dependabot bumps. Use `make signoff-remote` to dispatch the same sequence on a self-hosted runner. Keep the *same* `--features`/profile as the `make lint-rust` gate across your other local `cargo` commands to avoid re-fingerprinting and full rebuilds (see the fixed-feature-set bullet above).
- **If sccache fails** (unwritable volume breaks the `aws-lc-sys` C compile): `env -u RUSTC_WRAPPER -u RUSTC_WORKSPACE_WRAPPER CC=cc CXX=c++ cargo …`.

```bash
make install-dev        # Dev build (faster); release: make install
SPICED_CUSTOM_FEATURES="postgres sqlite" make build-runtime
make test               # Unit tests
make test-integration   # Needs credentials (.env or `spice login`)
make lint-rust-fix      # Auto-fix lint issues
```

## Git & PRs

- **Never force-push** — not on `trunk`, not on feature branches, not with `--force-with-lease` (it can't see pushes since your last fetch). Force-pushing destroys collaborator commits and orphans PR review history. Instead: `git pull --rebase` or merge, then push normally; fix reviewed history with follow-up commits and squash on merge; never `--amend` after pushing.
- Never bypass hooks or signing (`--no-verify`, `--no-gpg-sign`) — fix the underlying failure.
- Investigate before destructive ops (`reset --hard`, `checkout --`, `clean -f`): unfamiliar files or branches may be in-progress work.
- Branch from `trunk`, link the issue, add tests. Style: `docs/dev/style_guide.md`, `docs/dev/error_handling.md`.
- If a PR's checks stop triggering (only ~2 checks appear), check for a merge conflict first (`mergeStateStatus: DIRTY`) — merge `trunk` into the branch to re-trigger.
- **PR descriptions** *may* describe the old behavior and what was wrong with it — that context is what makes the `git` history worth reading. But never use internal/local tracking labels in a PR (title, body, or commits): phase/step numbers, plan-item IDs, or any shorthand coined in a planning doc or working session (e.g. `PR 6.1`, `Phase 3`, `step 2b`) mean nothing to a reviewer or a future reader and must stay in your local notes.
- **Code comments describe how the code works or *why it is the way it is* — never how it *used to* work** (that is what `git` history is for). Drop "previously/originally/historically/moved from…" narration. A comment may cite a GitHub issue when it adds context the code can't — especially a regression test that exists because of that issue (`// regression test for #NNNN`).
- **Use standard, discoverable terminology** in names and comments — the term someone would search for — not a coinage from a specific conversation or plan.

## Architecture

- **Separate Tokio runtimes** isolate the HTTP server (health checks, endpoints) from query execution (DataFusion is CPU/IO heavy and shares one thread pool). Never share runtime handles; `/health` must respond quickly regardless of query load.
- **Layout**: source in `crates/` — most-touched: `runtime/` (orchestration), `data_components/` (`TableProvider` impls), `app/` (Spicepod parsing), `datafusion/` (extensions), `llms/`, `search/`; acceleration engines in `runtime-acceleration/` and `cayenne/` (native CDC-fed accelerator); per-concern `runtime-*` crates. Authoritative map: workspace `members` in root `Cargo.toml`.
- **Crate layering (enforced)**: the ~120 crates form tiers — `foundation` (near-dependency-free utils/primitives) → `shared-utility` (always-shipped libraries the runtime builds on: `data_components`, `cayenne`, `llms`, `search`, `runtime-*` libs) → `runtime` (the orchestrator) → `extension-utility` (connector-only building blocks `runtime` must not touch; an empty target slot today) → `extension` (`connector-*`, `spice-cloud`) → `binary` (`spiced`/`spice`, `tools/*`). The two utility tiers split the old `domain` tier by *who may build on the crate*: `runtime-*` may depend only on `shared-utility`; extensions may depend on both. **Dependencies flow downward only**; `layers.toml` assigns every crate and `scripts/check_crate_layers.py` fails CI on any upward *normal* edge (dev-deps exempt). Adding/splitting a crate → add a line to `layers.toml` and run the check. When a low-tier crate needs something from a higher tier, push the *type/trait* down, never the dependency up. Crate naming: prefix = the subsystem that *satisfies* a contract (`data-*` = data sources + the contracts they implement; `runtime-*` = the engine + the services it implements; `-api` suffix = interface crate; foundation utils take no prefix). NOTE: `connector-*` currently sit in `extension` (above `runtime`) because the `DataConnector` trait/registry live in `runtime`; the target inverts this (connectors below `runtime`). Full model, naming rules, target state, and method: `docs/dev/crate_layering.md`.
- **A feature is a crate, not a `[feature]`**: prefer a crate per optional capability over a cargo feature inside a shared crate. Conditional compilation (`#[cfg(feature=…)]`, `dep:` gates) belongs **only in the stitch binaries** (`spiced`/`spice`), which pick which crates to link (e.g. `postgres = ["dep:data-postgres"]` on `spiced`). Library crates (`runtime`, `data_components`, …) should carry **zero** capability features — features re-fingerprint the whole graph on every set change and unify silently, whereas crate selection compiles once and caches. Don't add a new capability feature to a library crate; add a crate and gate it from `spiced`. (Current gap: `runtime` has 54 features, `data_components` 30 — legacy, collapsing with the `data_components` dissolution.)
- **Extension points** (`docs/EXTENSIBILITY.md`): Data Connector, Data Accelerator, Catalog Connector, Secret Store, Model, Embedding.
- **Acceleration wraps**: `AcceleratedTable` → `FederatedTable` → connector `TableProvider`.
- **Cayenne doc**: `docs/cayenne/` holds a breadth-first technical reference for the `cayenne` crate (`cayenne.md`, source-grounded against `crates/cayenne`), built to a PDF artifact by `.github/workflows/cayenne_doc.yml`. **A PR that changes `crates/cayenne` behavior, config params, the metastore schema, or the CDC/compaction flows must be reviewed for whether it merits a `docs/cayenne/cayenne.md` update — make that update in the same PR where practical, and add a *Document changelog* row referencing the reviewed commit.** The doc must stay source-accurate; never let it state something the code no longer does.

## Rust standards

Workspace is edition 2024, rust-version 1.96.1 — use stable features and modern std APIs through 1.96; don't code to older subsets. Runtime is 64-bit minimum: assume `usize` is at least (never exactly) 64 bits. New `.rs` files need the copyright header (`Copyright 2024-2026 The Spice.ai OSS Authors`; vendored code in `crates/vendor/` exempt).

### Error handling (critical)

- SNAFU error enums (`derive(Snafu, Debug)`) with `pub type Result<T, E = Error> = std::result::Result<T, E>;`.
- No `.unwrap()`/`.expect()` outside tests — use `?`, `ensure!` (preferred over `if` + `return Err`), `.context(...)`. In tests, use `.expect("descriptive message")`, never `.unwrap()`.
- No `assert!` in non-test code (use error handling or `debug_assert!`; compile-time assertions excepted). `unreachable!`/`todo!`/`unimplemented!` only for provably unreachable code — callable-but-unfinished paths must return a typed error (`DataFusionError::NotImplemented`, a `NotImplementedSnafu` variant) so callers degrade gracefully.
- Don't suppress lints in production code — fix the cause (delete dead code, restructure). Where truly unavoidable, use `#[expect(...)]`, not `#[allow(...)]` (the `allow_attributes` lint is denied); justified suppressions are fine in tests/benches/examples/build scripts.

```rust
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to connect to {connector}: {source}"))]
    ConnectionFailed { connector: String, source: std::io::Error },
}
ensure!(!data.is_empty(), DataEmptySnafu);
let value = option.context(ValueMissingSnafu)?;
```

**User-facing messages**: `Failed to {action} {resource} {name} ({connector}): {specific_error}` — simple but specific language, always name the dataset/model/catalog, give an actionable fix with a docs link, no internal concepts ("read provider", "table source").

```rust
#[snafu(display(
    "Failed to register dataset {dataset_name} ({connector}): Invalid file format. \
    Expected '.csv' but found '.parquet'. \
    Update 'file_format' parameter. See: https://spiceai.org/docs/components/data-connectors"
))]
```

### Async & blocking (critical)

Async code must reach an `.await` at least every ~100µs — blocking a runtime thread stalls all tasks (and `/health`, so Kubernetes kills spiced).

- `tokio::time::sleep` not `std::thread::sleep`; `tokio::fs` not `std::fs`; async DB clients via pools.
- Blocking I/O → `tokio::task::spawn_blocking`; CPU-bound work → `rayon::spawn` sending the result back over a `tokio::sync::oneshot`; long-running background work → dedicated `std::thread::spawn`.
- Never hold a lock across `.await`.
- Trait async methods: default to `#[async_trait]` — most internal traits (`DataConnector`, `Chat`, `Embed`, `SecretStore`, …) are used as `Arc<dyn Trait>` and native async-fn-in-trait isn't dyn-safe. Native AFIT only on non-dyn helper traits.
- Avoid the `stream!` macro (breaks rust-analyzer, hard to debug) — prefer manual `Stream` impls; document why when unavoidable.
- Shutdown via `CancellationToken` (`crates/runtime-async/src/cancellable_task.rs`); lazy globals via `std::sync::LazyLock`/`OnceLock` in new code (existing `once_cell` is fine to leave).

### Locking

- `parking_lot` over std locks; `RwLock` when reads ≫ writes; lock the smallest field (`Arc<RwLock<HashMap<..>>>` inside a struct, not the whole struct); drop guards ASAP with explicit scopes.
- Prefer lock-free (`Arc<AtomicU64>`, `DashMap`) and shard by key to reduce contention; document lock ordering to prevent deadlocks.

### Performance & memory

- **Zero-copy Arrow**: `RecordBatch::slice()` shares buffers; `ArrayRef`/`Arc<dyn Array>` in signatures (refcount clone); `as_any().downcast_ref`; avoid `.to_data()`, `.clone()`, or collecting array values into `Vec`s.
- **Use `arrow::compute::*` kernels** (SIMD-optimized) instead of manual loops; structure unavoidable hot loops for auto-vectorization (`chunks_exact`, branch-free bodies). Vortex arrays for compressed data when memory ≫ compute.
- **Stay streaming**: process `RecordBatchStream` batch-by-batch; `try_collect()` materializes the whole dataset (OOM risk). Default 8192-row batches are cache-friendly.
- **Push down** filters/projections (`TableProvider::supports_filters_pushdown`); partition to core count.
- **Propagate statistics across all layers where possible**: implement `TableProvider::statistics()` (and `ExecutionPlan` statistics) and forward them through every wrapper — accelerated tables, federation, views, partition providers — so the optimizer sees row counts and min/max; a layer returning `None` silently degrades plans. Correctness caveat: report stats as exact only when provably exact (downgrade to inexact for mutable/overlay data — see `statistics_to_inexact` in `cayenne`), since DataFusion may substitute exact stats into results.
- **Minimize allocation**: reuse buffers (`clear()` keeps capacity), `&str`/`&[T]` in signatures, `Cow<str>`, iterate instead of intermediate collections, `Vec::with_capacity`, `SmallVec` for small vectors.
- **`Arc::clone` is cheap, not free** (lint: `clone_on_ref_ptr`): pass `&Arc<T>` when not taking ownership; don't clone in hot loops.
- **Always use connection pools** (`deadpool` async / `r2d2` sync): pool creation never fails; errors surface on `.get().await`.

### SQL & data safety

- Prefer DataFusion `DataFrame` APIs over raw SQL for runtime-internal queries; never interpolate user input into SQL — parameterize or escape.
- Validate transformations (e.g. `ensure!` input/output row counts match); NULL propagation must match SQL semantics — `Option<T>`, return a structured error on unexpected NULL rather than panicking.
- Be explicit about type coercions and preserve fidelity; test aggregations against empty sets, NULLs, and overflow; verify JOIN NULL-key and duplicate-row semantics; document sort stability for equal values.

### Logging

`tracing::` macros only — never the `log::` crate. Keep every log/error message on a single line: no embedded newlines or `\`-continuations that insert them.

### User-facing configuration

- **No boolean params in user-facing config** (Spicepod fields, connector `params`, CLI flags): a bool can't grow a third state and hides which value means "on". Use `#[serde(rename_all = "snake_case")]` enums whose variants describe behavior, mirroring precedent: `on_zero_results: return_empty|use_source`, `unsupported_type_action: error|warn|ignore|string`, `ready_state: on_load|on_registration|on_schema_resolved`, `check_availability: auto|disabled`, `on_schema_change: block|fail|append_new_columns|sync_all_columns`. Default (`#[default]`) to the conservative, back-compat-preserving variant. Booleans remain fine in internal, non-config code.
- When behavior depends on the connector/engine, add a capability/trait method defaulting to the universally-safe modes, validate config against it, and return a structured configuration error for unsupported modes — never silently ignore. Forward the method through every wrapper (next section).

## Trait evolution & wrapper delegation (critical)

A new trait method with a default impl silently no-ops in wrapper/decorator impls — it compiles, then regresses at runtime (real case [#10460](https://github.com/spiceai/spiceai/pull/10460): defaulted `register_object_stores` on `DataConnector` was never forwarded by `EmbeddingConnector`/`FullTextConnector`/`DeferredConnector`, so cluster executors hit `BareRedirect` S3 errors). When adding or changing a trait method:

1. Find every wrapper impl: `rg -n "impl\s+(\w+\s+for\s+)?<TraitName>\b" crates/`. Known wrapped traits: `DataConnector` (`EmbeddingConnector`, `FullTextConnector`, `DeferredConnector`); `TableProvider` (`AcceleratedTable`, `FederatedTable`, view/sink/partition providers — including `statistics()`); `Read`, `ReadWrite`, `Catalog`, `SecretStore`, `Chat`, `Embed`, `Nql`.
2. Explicitly forward the method in each wrapper — inheriting the default is almost always a bug, even a no-op default, because the inner type may have meaningful behavior.
3. Prefer no default impl on traits with known wrappers so the compiler surfaces them; if back-compat forces a default, comment the forwarding requirement on the trait method.
4. Add an integration test exercising the wrapped path — unit tests don't catch defaulted no-ops.

## Testing

- **Spicepod naming**: `{connector[variant]}-{accelerator[variant]}-{test_variant}`; non-accelerated must use the `-federated` suffix. Examples: `s3[parquet]-federated`, `mysql-duckdb[file]-on_zero_results`.
- **testoperator** is the benchmark/test harness: `cargo run -p testoperator -- run bench -p test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate` (also `run throughput … --concurrency 25`).
- **No fixed sleeps as readiness waits**: poll the actual condition with a bounded timeout, short interval, and a failure message carrying the last observed state — `runtime_ready_check[_with_timeout]`, `wait_until_true`, `util::retry` with `FibonacciBackoffBuilder`, health/ping probes, `SELECT 1`, refresh notifiers, result polling. Fixed sleeps only when time itself is under test (TTL, backoff, cron, rate limits) — keep them short and explain them.
- **Insta snapshots**: always named `.snap` files — `insta::assert_snapshot!("name", value)` — never inline `@r"…"` (bloats sources, diffs poorly). Regenerate with `INSTA_UPDATE=1 cargo test` and review; never hand-edit `.snap` files.
- **Debugging integration tests**: the runtime test harness uses a thread-local tracing subscriber, so logs from worker threads (e.g. dataset loads) are dropped — use `eprintln!` for debug output, not `tracing`.

## Feature flags & dependencies

Heavy connector deps are optional `spiced` features. When adding one: make the dependency optional (`dep:newdb-client`), wire the feature (`newdb = ["runtime/newdb", "data_components/newdb"]`), gate code with `#[cfg(feature = "newdb")]`, and update `bin/spiced/Cargo.toml` plus Makefile lint targets.

Git deps in `Cargo.toml`: always a full 40-character SHA (reproducible, unambiguous); for spiceai forks add a branch comment. Forks live in the spiceai org (e.g. `datafusion-table-providers` pins `spiceai/datafusion-table-providers`, not datafusion-contrib).

```toml
duckdb = { git = "https://github.com/spiceai/duckdb-rs.git", rev = "<full 40-char sha>" } # branch: spice
```

## Adding a data connector

1. `data_components/src/{connector}.rs` — `TableProvider` impl
2. `runtime/src/dataconnector/{connector}.rs` — factory; register in `runtime/src/dataconnector/mod.rs`
3. Feature-gate as above; add an integration test in `test/spicepods/{connector}/`; document in README.md

For any feature: check whether it needs a new extension point; test correctness edge cases (NULLs, empty sets, boundaries, coercions, large datasets); no blocking in async; follow the error-message format; update user docs; lint green.

## Setup & references

`brew install rust cmake protobuf && make install-dev`; `export PATH="$PATH:$HOME/.spice/bin"`. Copy `.vscode/settings.json.template` → `.vscode/settings.json` (gitignored): rust-analyzer runs clippy with `-Dclippy::pedantic -Dclippy::unwrap_used -Dclippy::clone_on_ref_ptr`, so lints fail locally, not just in CI.

Key docs: `docs/PRINCIPLES.md`, `docs/EXTENSIBILITY.md`, `docs/dev/style_guide.md`, `docs/dev/error_handling.md`, `CONTRIBUTING.md`, `docs/decisions/`, `docs/threat_models/`; [Spice docs](https://spiceai.org/docs), [Cookbook](https://github.com/spiceai/cookbook).
