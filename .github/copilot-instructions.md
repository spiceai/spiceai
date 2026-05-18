# Spice.ai GitHub Copilot Instructions

## Project Overview

Spice is a SQL query, search, and LLM-inference engine in Rust for data apps and agents. Provides federated SQL querying, data acceleration/materialization, search (vector, keyword, full-text), and AI inference via industry-standard APIs.

**Architecture**: Rust CLI (`bin/spice`) + Rust runtime daemon (`bin/spiced`). Built on Apache DataFusion, Arrow, and DuckDB.

**Core Principle**: Developer Experience First — Bring data and AI/ML to your application, not the other way around.

### ⚠️ DATA CORRECTNESS — ABSOLUTE TOP PRIORITY ⚠️

**As an AI-native database and search engine, data correctness is non-negotiable and the highest priority of this project.**

- **Data can NEVER be wrong**: Every query must return correct results. Incorrect data is unacceptable under any circumstances.
- **Correctness over performance**: Never sacrifice data accuracy for speed or convenience. A slow correct answer is infinitely better than a fast wrong one.
- **Verify transformations**: Any data transformation, aggregation, or computation must preserve data integrity.
- **Test edge cases rigorously**: NULL handling, boundary conditions, type coercions, and overflow scenarios must all produce correct results.
- **When in doubt, fail safely**: If there's any uncertainty about data correctness, return an error rather than potentially incorrect data.
- **No silent data corruption**: Always surface errors visibly rather than returning subtly wrong results.

**This principle supersedes all other considerations including performance, developer experience, and feature velocity.**

### Git Workflow

**Never force push** — not on `trunk`, not on feature branches, not even with `--force-with-lease`. Always merge or rebase normally, then push without force.

- **Why force-push is banned**: It silently destroys collaborator commits and orphans PR review history (comments lose their anchor, reviewers re-read the entire diff, CI re-runs). `--force-with-lease` only protects against the *latest fetch* — it cannot see commits a collaborator pushed since you fetched, so it does not make force-push safe on shared branches.
- **What to do instead of force-push**:
  - Branch out of date with `trunk`? `git pull --rebase` or `git merge trunk`, then `git push` normally.
  - Want to fix history on a branch with open review? Add a follow-up commit and squash on merge — don't rewrite published commits.
  - Pre-commit hook failed and the commit didn't actually land? Re-stage, fix, create a *new* commit. Never `--amend` after pushing.
- **Never bypass hooks** (`--no-verify`, etc.). If a hook fails, fix the underlying issue — these checks exist because earlier failures escaped review. Likewise, don't bypass required commit signing (e.g. `--no-gpg-sign`) just to get a commit through.
- **Investigate before destroying**: unfamiliar files, branches, or lock files may represent in-progress work. Don't `git reset --hard`, `checkout --`, or `clean -f` to "make it go away" — find the root cause first.

### Runtime Architecture - Separate Tokio Runtimes

**Separate runtime instances for:**

- **HTTP Server**: Health checks, query endpoints (must stay responsive)
- **Query Processing**: DataFusion planning and execution (CPU/IO intensive)

**Why**: DataFusion uses one thread pool for all operations. Large queries can block HTTP server, causing K8s health check failures. Separate runtimes isolate concerns.

**Rules**: Never share runtime handles; HTTP endpoints (especially `/health`) must respond quickly regardless of query load.

**References**: [DataFusion Thread Pools](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/thread_pools.rs#L18), [Thread Scheduling](https://docs.rs/datafusion/latest/datafusion/index.html#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes)

## Build & Test Commands

```bash
make install            # Release build
make install-dev        # Dev build (faster)
SPICED_CUSTOM_FEATURES="postgres sqlite" make build-runtime  # Custom features

make lint-rust-fix      # Auto-fix Rust issues
make test               # Unit tests
make test-integration   # Integration tests (needs .env or spice login)

# Benchmarks with testoperator
cargo run -p testoperator -- run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate
```

## Rust Coding Standards

### Configuration & User-Facing Parameters

- **Avoid boolean parameters in user-facing configuration** (Spicepod fields, connector `params`, CLI flags, public API options). Booleans paint you into a corner the moment a third state is needed and force readers to know which value means "on": `on_schema_change: append_new_columns` reads better than `schema_evolution_enabled: true`, and leaves room for `block`, `fail`, `sync_all_columns`, … without breaking changes.
- **Prefer named enum variants** (`#[serde(rename_all = "snake_case")]`). Pick verbs/states that describe behavior, not capability — `block` / `fail` / `append_new_columns` / `sync_all_columns`, not `disabled` / `enabled` / `auto`. Default to the conservative or back-compat-preserving variant via `#[derive(Default)]` + `#[default]` so the safe behavior is what users get when they omit the field.
- **Mirror precedent** already in the codebase: `on_zero_results: return_empty|use_source`, `unsupported_type_action: error|warn|ignore|string`, `ready_state: on_load|on_registration|on_schema_resolved`, `check_availability: auto|disabled`, `on_schema_change: block|fail|append_new_columns|sync_all_columns`. Reach for an existing enum-shaped pattern before inventing a new boolean.
- **Make each connector or engine explicitly opt in** when a setting depends on implementation behavior. Add a trait or capability method whose default returns only the modes that work universally, validate user configuration against that list, and surface a structured configuration error instead of silently ignoring unsupported modes. Audit every wrapper/decorator impl (`EmbeddingConnector`, `FullTextConnector`, `DeferredConnector`, …) to forward new trait methods to the inner connector; see "Trait Evolution & Wrapper Delegation" below.
- **Booleans are still fine in internal, non-config code paths** (struct fields, function arguments, in-memory flags, test helpers). This rule is about *what users type into YAML / pass on the CLI*, not about Rust's primitive types.

### Rust Version Baseline

- **Workspace Rust version is 1.94.1**: Treat Rust 1.94.1 as the minimum supported compiler version for workspace code unless a specific crate or integration explicitly documents a different constraint.
- **Use stable Rust features through 1.94.1**: Prefer stable language, standard library, and Cargo features available in Rust 1.94.1 when they improve correctness, clarity, ergonomics, or maintainability.
- **Do not code to an older Rust subset by default**: Avoid workarounds for pre-1.94 compilers unless there is a concrete compatibility requirement.
- **Prefer modern std APIs over manual patterns**: When a newer stable standard-library API expresses the intent more clearly or avoids extra allocation or unsafe code, use it.

### Error Handling (CRITICAL)

- **Use SNAFU**: Derive `Snafu` and `Debug` on error enums
- **NO `.unwrap()`/`.expect()` in non-test code**: Use `?` operator or `match`
- **In tests**: Use `.expect("descriptive message")` instead of `.unwrap()`
- **`unreachable!()` / `unimplemented!()` / `todo!()`**: Only for *provably unreachable* code. Never for unfinished-but-callable code — they panic at runtime, which violates the data-correctness rule of failing safely with a structured error. For not-yet-implemented method bodies, return a typed error (`DataFusionError::NotImplemented("...")`, an `Err(NotImplementedSnafu { ... })` variant, etc.) so callers can degrade gracefully or surface a useful message
- **Use `ensure!` macro**: Preferred over `if` + `return Err`
- **Define `Result` type alias**: `pub type Result<T, E = Error> = std::result::Result<T, E>;`
- **Don't use `assert!()` (or related) macros in non-test code**: Prefer proper error handling, or marking with `unreachable!()` if the assertion is truly unreachable. Alternatively, make the assertion a `debug_assert!()` assertion to only fire in debug builds instead of release builds. `assert!()` macros can have case-by-case exceptions, for example for compile-time assertions that would prevent a build from being released to begin with.
- **Don't `#[allow(...)]` warnings in production/library/runtime code**. `#[allow(dead_code)]`, `#[allow(unused)]`, `#[allow(unused_imports)]`, `#[allow(unused_variables)]`, `#[allow(clippy::*)]` and friends paper over real issues that the project's lint denials (`-Dwarnings`, `unwrap_used`, `expect_used`, `clone_on_ref_ptr`, `pedantic`) exist to surface. Fix the underlying issue — delete unused code, drop unused imports, restructure to satisfy the lint — rather than suppressing the warning. Well-justified `#[allow(...)]` (or preferably `#[expect(...)]`, which fails when the lint stops firing) is acceptable in **tests, benches, examples, and build scripts** when the alternative is meaningfully worse (e.g., a test fixture with intentionally unused fields, or a bench that intentionally calls `.expect()`).

```rust
// GOOD
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to connect to {connector}: {source}"))]
    ConnectionFailed { connector: String, source: std::io::Error },
}
ensure!(!data.is_empty(), DataEmptySnafu);
let value = option.context(ValueMissingSnafu)?;

// Tests only
#[cfg(test)]
fn test() { let value = option.expect("descriptive message"); }
```

### Streams

- **AVOID `stream!` macro**: Breaks rust-analyzer, hard to debug. Use manual `Stream` implementations or `async_stream::stream` sparingly; document why when unavoidable.

## Performance & Memory

### Zero-Copy Operations

- **Prefer zero-copy** with Arrow arrays: avoid `.to_data()`, `.clone()`, conversions
- **Use `Arc<dyn Array>`** for type-erased arrays (cheap clone, shares buffers)
- **Use `RecordBatch::slice()`** instead of filtering/copying
- **Prefer `ArrayRef`** in function signatures over owned arrays

```rust
// GOOD
let subset = batch.slice(offset, length);  // Shares buffers
let int_array = array.as_any().downcast_ref::<Int32Array>()?;
let shared: ArrayRef = Arc::clone(&array);  // Just refcount++

// BAD
let values: Vec<i32> = array.values().iter().copied().collect();  // Avoid
```

### SIMD & Hardware Acceleration

- **Let Arrow/DataFusion handle SIMD**: Auto-vectorizes for arm64/amd64
- **Use `arrow::compute::*` kernels**: SIMD-optimized (add, filter, cast, etc.)
- **Structure loops for auto-vectorization**: Cache-aligned chunks (64 bytes), no branches in tight loops, use `slice::chunks_exact()`
- **Vortex arrays**: For compressed data when memory >> compute cost
- **Apple Metal**: Consider DataFusion::gpu extensions for macOS/iOS

```rust
// GOOD - Arrow kernels
use arrow::compute::add;
let result = add(&left_array, &right_array)?;

// GOOD - auto-vectorizable
let sum: i64 = int_array.values().iter().map(|&v| i64::from(v)).sum();

// BAD - manual loop when kernel exists
for i in 0..array.len() { result.push(array.value(i) + other.value(i)); }
```

### Architecture-Specific

- **arm64** (Apple Silicon, Graviton): NEON SIMD auto-enabled
- **amd64** (Intel/AMD): AVX2/AVX-512 when available
- **Production builds**: `RUSTFLAGS="-C target-cpu=native" cargo build --release`
- **Distributed binaries**: Default build (runtime CPU detection)

### DataFusion Query Performance

- **Partition data**: Align with CPU core count
- **Use `ParquetExec` directly**: Pushes down filters/projections
- **Keep streaming**: Don't collect streams early (`RecordBatchStream`)
- **Enable predicate pushdown**: Implement `TableProvider::supports_filters_pushdown`
- **Batch sizing**: Default 8192 rows is cache-friendly

```rust
// GOOD - streaming
let stream = table_provider.scan(...).await?;
while let Some(batch) = stream.next().await { process_batch(batch?)?; }

// BAD - materializes entire dataset (OOM risk)
let all_batches: Vec<RecordBatch> = stream.try_collect().await?;
```

### SQL & Query Safety (CRITICAL for Data Correctness)

- **Favor DataFrame APIs over raw SQL for internal queries**: Use DataFusion's `DataFrame` API for runtime-internal queries—it's type-safe, composable, and avoids SQL parsing overhead
- **Never trust user input in SQL**: Always use parameterized queries or proper escaping
- **Validate query results**: When transforming data, verify row counts and key values are preserved
- **Handle NULL correctly**: Use `Option<T>` appropriately; NULL propagation must match SQL semantics
- **Be explicit about type coercions**: Arrow/DataFusion type casting must preserve data fidelity
- **Test aggregations**: SUM, COUNT, AVG must handle empty sets, NULLs, and overflows correctly
- **Verify JOIN semantics**: Ensure correct handling of NULL keys and duplicate rows
- **ORDER BY stability**: Document whether sort is stable when values are equal

```rust
// GOOD - explicit NULL handling
let value: Option<i64> = row.get("amount");
let total = value.unwrap_or(0); // Only if business logic allows

// GOOD - validate transformations with runtime error handling
ensure!(
    input_batch.num_rows() == output_batch.num_rows(),
    RowCountMismatchSnafu {
        expected: input_batch.num_rows(),
        actual: output_batch.num_rows(),
    }
);

// GOOD - propagate error instead of panicking on NULL
let value: i64 = row.get("amount").context(AmountNullSnafu)?; // Returns a structured error if NULL
```

### Allocation Minimization

- **Reuse buffers**: `String::clear()`, `Vec::clear()` to keep capacity
- **Prefer `&str`/`&[T]`** in signatures over `String`/`Vec<T>`
- **Use `Cow<str>`**: When ownership might be needed but often isn't
- **Avoid intermediate collections**: Use iterators, collect only at end
- **Use `SmallVec`**: For small, stack-allocated vectors
- **Pre-allocate**: `Vec::with_capacity()`, array builders with hints

```rust
// GOOD - reuse buffer
let mut buffer = String::with_capacity(1024);
for item in items {
    buffer.clear();  // Keeps capacity
    write!(&mut buffer, "{}", item)?;
}

// GOOD - zero allocations
let sum: i64 = values.iter().filter(|&&x| x > 0).map(|&x| i64::from(x)).sum();

// BAD
for item in items { let buffer = format!("{}", item); }  // Allocs every iteration
```

### Fine-Grained Locking

- **Lock `Arc` contents**: Use `Arc<RwLock<T>>`, not `Arc<RwLock<EntireStruct>>`
- **Prefer `RwLock`**: When reads >> writes (common in query engines)
- **Minimize lock scopes**: Drop locks ASAP with explicit scopes
- **Use `parking_lot`**: Faster than std locks (already in deps)
- **Never hold locks across `.await`**: Causes deadlocks/stalls
- **Use lock-free**: `Arc<AtomicU64>`, `dashmap::DashMap` when possible
- **Partition data**: Shard by key to reduce contention
- **Document lock ordering**: Prevent deadlocks

```rust
use parking_lot::RwLock;
use std::sync::Arc;

// GOOD - fine-grained
struct Cache {
    entries: Arc<RwLock<HashMap<String, Data>>>,
    stats: Arc<AtomicU64>,  // Separate lock-free counter
}

async fn get(&self, key: &str) -> Option<Data> {
    let data = { self.entries.read().get(key).cloned() };  // Lock dropped
    self.stats.fetch_add(1, Ordering::Relaxed);
    data
}

// GOOD - DashMap (no single lock)
let cache: Arc<DashMap<String, Data>> = Arc::new(DashMap::new());

// BAD - lock across await
async fn bad(&self) {
    let data = self.data.lock();
    some_async_op().await;  // Still holding lock!
}
```

### Logging (CRITICAL)

- **Use `tracing::` for logging**: Use `tracing::info!`, `tracing::error!`, `tracing::debug!`, etc.
- **DO NOT use `log::`**: The project uses `tracing` crate, not `log` crate
- **DO NOT add newlines in log messages or error strings**: Keep all log/error messages on a single line

```rust
// GOOD
tracing::info!("Starting runtime");
tracing::error!("Failed to connect: {}", error);
tracing::warn!(attempt = 1, "Failed to initialize credentials; retrying");

// GOOD - long messages on single line
tracing::debug!("AWS credential provider initialized without credentials. Proceeding without authentication.");

// BAD - don't use log crate
log::info!("Starting runtime");

// BAD - don't add newlines in messages
tracing::error!(
    "Failed to connect: {}. \
     Please check your configuration.",
    error
);
```

### Async/Blocking Patterns (CRITICAL)

**Rule**: Async code should never spend a long time without reaching an `.await`.

- **Target**: No more than 10-100 microseconds between `.await` points
- **NEVER use blocking operations in async functions**:
  - ❌ `std::thread::sleep` → ✅ `tokio::time::sleep`
  - ❌ `std::fs` → ✅ `tokio::fs`
  - ❌ Blocking database calls → ✅ Use connection pools with async APIs

**Handling blocking operations:**

1. **For blocking I/O** (file system, synchronous DB clients):

   ```rust
   // Use spawn_blocking for synchronous operations
   let result = tokio::task::spawn_blocking(move || {
       // Blocking operations here (file I/O, synchronous DB calls)
       std::fs::read_to_string("file.txt")
   }).await?;
   ```

2. **For CPU-bound computations**:

   ```rust
   // Use rayon for parallel CPU work
   let (tx, rx) = tokio::sync::oneshot::channel();
   rayon::spawn(move || {
       let result = expensive_computation();
       let _ = tx.send(result);
   });
   let result = rx.await?;
   ```

3. **For long-running background tasks**: Spawn dedicated threads with `std::thread::spawn`

**Why this matters**: Blocking an async runtime thread prevents other tasks from running, causing cascading delays and poor throughput under load.

### Connection Pooling

- **Always use connection pools** for database connectors
- Pool creation should never fail - errors only on `.get()`
- Use `deadpool` or `r2d2` for async/sync pooling respectively

```rust
// GOOD - pool creation doesn't fail, errors on get
let pool = Pool::builder(manager).build()?;
// Later...
let conn = pool.get().await?; // Error only here

// BAD - don't create connections on-demand
let conn = create_connection().await?; // Creates new connection every time
```

### Arc/Rc Cloning

- **Avoid unnecessary `Arc`/`Rc` clones** (caught by `clippy::clone_on_ref_ptr`)
- `Arc::clone()` is cheap but not free - don't clone in hot loops unnecessarily
- When passing `Arc<T>` to functions, prefer `&Arc<T>` if you don't need ownership

```rust
// GOOD - function signature
fn process_data(data: &Arc<RecordBatch>) { ... }
```

### User-Facing Error Messages

**Format**: `Failed to {action} {resource_type} {name} ({connector}): {specific_error}`

1. Simple but specific language
2. Always include dataset/model/catalog name
3. Provide actionable steps (docs links, config fixes)
4. Exclude internal concepts ("read provider", "table source")

```rust
#[snafu(display(
    "Failed to register dataset {dataset_name} ({connector}): Invalid file format. \
    Expected '.csv' but found '.parquet'. \
    Update 'file_format' parameter. See: https://spiceai.org/docs/components/data-connectors"
))]
```

## Project Structure

- **Binaries**: `bin/spiced/` (runtime daemon), `bin/spice/` (CLI).
- **Crates**: source lives in `crates/`. Most-touched: `runtime/` (orchestration), `data_components/` (`TableProvider` impls), `app/` (Spicepod parsing), `datafusion/` (DataFusion extensions), `llms/`, `search/`, `model_components/`. Acceleration engines under `runtime-acceleration/`; per-concern `runtime-*` crates (`runtime-auth`, `runtime-secrets`, `runtime-parameters`, etc.). For the authoritative current map, see workspace `members` in the root `Cargo.toml`.
- **Extension points** (see `docs/EXTENSIBILITY.md`): Data Connector, Data Accelerator, Catalog Connector, Secret Store, Model, Embedding.

## Testing

### Spicepod Naming

Format: `{connector[variant]}-{accelerator[variant]}-{test_variant}`  
Non-accelerated MUST use `-federated` suffix.

Examples: `s3[parquet]-federated`, `mysql-duckdb[file]-on_zero_results`

### testoperator

```bash
testoperator run bench -p test.yaml -s spiced --query-set tpch --validate
testoperator run throughput -p test.yaml -s spiced --query-set tpch --concurrency 25
```

### Test Waits and Readiness

- **Avoid fixed sleeps/waits as readiness mechanisms in tests**: Don't add `tokio::time::sleep`, `std::thread::sleep`, or arbitrary time delays to wait for containers, services, background refreshes, streams, replication, caches, or eventual data visibility.
- **Prefer condition-driven waits**: Use `runtime_ready_check`, `runtime_ready_check_with_timeout`, `wait_until_true`, `util::retry` with `FibonacciBackoffBuilder`, protocol/application probes (`SELECT 1`, ping, health/ready endpoints, `list_tables`, metadata fetches), Docker health plus host-port checks, explicit completion signals (refresh notifiers, task handles, channels), or query/result polling that validates the actual expected state.
- **Bound every wait**: Polling loops must have a clear timeout, short polling interval, and useful failure message that includes the last observed error or state.
- **Fixed sleeps are acceptable only when time itself is the behavior under test**: Examples include TTL/cache expiry, cron/scheduler boundaries, backoff timing, timeout behavior, rate limiting, and cancellation/shutdown timing. Keep these sleeps as short as the test allows and make the reason obvious in the test.

### Snapshot Testing with Insta

- **ALWAYS use named `.snap` files** for snapshot assertions — never inline snapshots (`@r"..."` syntax)
  - Use `insta::assert_snapshot!("snapshot_name", value)` which stores snapshots in a `snapshots/` directory
  - Named snapshots are easier to review in PRs, diff cleanly, and avoid bloating test source files
- **NEVER manually edit snapshot files** (`.snap` files): Always use Insta to generate them
- Run tests with `INSTA_UPDATE=1` to regenerate snapshots: `INSTA_UPDATE=1 cargo test`
- Review generated snapshots carefully before accepting

```rust
// GOOD — named snapshot stored in snapshots/*.snap
insta::assert_snapshot!("query_plan", plan_fmt);
insta::assert_snapshot!("select_all_rows", rows_fmt);

// BAD — inline snapshot bloats test file, hard to diff in PRs
insta::assert_snapshot!(plan_fmt, @r#"..."#);
```

## Feature Flags

`spiced` uses optional heavy dependencies. When adding connectors:

1. Make dependency optional: `dep:newdb-client`
2. Add feature: `newdb = ["runtime/newdb", "data_components/newdb"]`
3. Gate code: `#[cfg(feature = "newdb")]`
4. Update Makefile lint targets

### Git Dependencies in Cargo.toml

- **Always use full 40-character SHA hashes** for git dependencies, never abbreviated SHAs
- Full SHAs ensure reproducible builds and avoid ambiguity
- **For spiceai forks**: Add a comment with the branch name to track the source

```toml
# GOOD - full SHA
datafusion = { git = "https://github.com/apache/datafusion.git", rev = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" }

# GOOD - spiceai fork with branch comment
duckdb = { git = "https://github.com/spiceai/duckdb-rs.git", rev = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" } # branch: spice

# BAD - abbreviated SHA
datafusion = { git = "https://github.com/apache/datafusion.git", rev = "a1b2c3d" }

# BAD - spiceai fork without branch comment
duckdb = { git = "https://github.com/spiceai/duckdb-rs.git", rev = "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0" }
```

## Development Workflow

### Setup (macOS/Linux)

```bash
brew install rust go cmake protobuf
make install-dev
export PATH="$PATH:$HOME/.spice/bin"
```

### VSCode Settings

`.vscode/settings.json` is gitignored; copy `.vscode/settings.json.template` (and see `CONTRIBUTING.md`) for the canonical config. Notable: rust-analyzer runs `clippy` with `-Dclippy::pedantic`, `-Dclippy::unwrap_used`, and `-Dclippy::clone_on_ref_ptr` (and `-Aclippy::module_name_repetitions`) — i.e. clippy lints fail your local check, not just CI.

### PR Process

- Branch from `trunk`, link issue, add tests
- Follow style guides: `docs/dev/style_guide.md`, `docs/dev/error_handling.md`

## Common Patterns

### Adding Data Connector

1. Create `data_components/src/{connector}.rs` (TableProvider)
2. Create `runtime/src/dataconnector/{connector}.rs` (factory)
3. Register in `runtime/src/dataconnector/mod.rs`
4. Gate with `#[cfg(feature = "...")]`
5. Update `bin/spiced/Cargo.toml` features
6. Add integration test in `test/spicepods/{connector}/`
7. Document in README.md

### DataFusion Integration

- Use `ensure!` for validation, not `expect`
- Push-down filters/projections for federation
- Acceleration wraps: `AcceleratedTable` → `FederatedTable` → connector `TableProvider`

### Async Patterns

- Use `tokio` runtime (see `bin/spiced/src/main.rs`).
- **Trait async methods**: prefer `#[async_trait]`. Native `async fn` in traits has been stable since Rust 1.75 and is fine for traits that *don't* need to be `dyn`-compatible — but most internal traits in this codebase (`DataConnector`, `Chat`, `Embed`, `SecretStore`, `Index`, `CacheProvider`, etc.) are stored as `Arc<dyn Trait>`, and native AFIT isn't `dyn`-safe without manual workarounds. Default to `async_trait` to keep the dyn path consistent; reach for native AFIT only on non-dyn helper traits.
- **Lazy globals**: prefer `std::sync::LazyLock` / `OnceLock` (modern stable Rust) over `lazy_static!` and `once_cell::sync::Lazy` for new code. Existing `once_cell` callsites are fine to leave.
- Use `CancellationToken` for shutdown (see `runtime/src/cancellable_task.rs`).

## Key Files

- `docs/PRINCIPLES.md`, `docs/EXTENSIBILITY.md`, `docs/dev/style_guide.md`, `docs/dev/error_handling.md`
- `CONTRIBUTING.md`, `Makefile`, `Cargo.toml`, `crates/runtime/src/lib.rs`

## Trait Evolution & Wrapper Delegation (CRITICAL)

When **adding a new method to a trait** — especially one with a default implementation — audit every wrapper/decorator implementation of that trait and ensure each one **forwards the call to its inner instance**. Default impls are silent traps for wrappers: they compile cleanly but no-op the behavior, causing regressions that often only surface in specific runtime configurations (e.g. cluster executors, accelerated tables, embedded/full-text wrappers).

This pattern caused real regressions, e.g. [#10460](https://github.com/spiceai/spiceai/pull/10460): `register_object_stores` was added to `DataConnector` with a no-op default impl, but `EmbeddingConnector`, `FullTextConnector`, and `DeferredConnector` were not updated to forward it. On cluster executors, object stores were silently never registered for affected datasets, causing `BareRedirect` errors against non-default-region S3 buckets.

**Reviewer checklist when a PR adds or changes a trait method:**

1. **Identify all wrapper/decorator impls** of the trait. Common Spice traits with wrappers include (non-exhaustive):
   - `DataConnector` — wrapped by `EmbeddingConnector`, `FullTextConnector`, `DeferredConnector`
   - `TableProvider` — wrapped by `AcceleratedTable`, `FederatedTable`, view providers, sink providers
   - `Read`, `ReadWrite`, `Catalog`, `SecretStore`, `Chat`, `Embed`, `Nql` — check for newtype/decorator wrappers in `data_components/`, `runtime/`, `llms/`, `model_components/`, `search/`
   - Search by ripgrep: `rg -n "impl\s+(\w+\s+for\s+)?<TraitName>\b" crates/`
2. **For every wrapper, verify the new method is explicitly implemented and delegates to the inner type.** Inheriting the default impl is almost always a bug for wrappers — even when the default is a no-op, since the wrapper's inner connector may have meaningful behavior.
3. **Flag PRs that add a defaulted trait method without touching corresponding wrapper impls.** Ask the author to confirm each wrapper has been audited, or to add explicit forwarding impls.
4. **Prefer no default impl on traits with known wrappers** when the correct behavior cannot be "do nothing". Forcing implementors to write the method makes wrappers visible at compile time. If a default is necessary for backward compatibility, leave a comment on the trait method pointing wrapper authors at the forwarding requirement.
5. **Call out missing test coverage**: regressions of this kind typically escape unit tests because the default impl compiles. Suggest an integration or cluster/executor test that exercises the wrapped path when feasible.

## Gotchas

1. Don't use `stream!` macro
2. Always use feature gates for optional connectors
3. Spicepod is YAML config format
4. Integration tests need credentials (`spice login` or `.env`)
5. testoperator is the test harness
6. Workspace uses Rust edition 2024 and rust-version 1.94.1; use stable Rust features available through 1.94.1 by default
7. New files should include copyright header. The current year is 2026. Required file types: `.rs`, `.go`
8. **Spice runtime (Rust) is 64-bit minimum**: The runtime requires at least 64-bit pointer size. Do not add 32-bit compatibility code. Code should assume `usize` is at least 64 bits but not assume it's exactly 64 bits (future 128-bit support).

## Adding Features Checklist

1. Check if needs new extension point
2. Make heavy dependencies optional via features
3. Add integration tests (testoperator or test-framework)
4. **Test data correctness edge cases**: NULLs, empty sets, boundary values, type coercions, large datasets
5. Update user docs (README.md, docs/)
6. Follow error message guidelines
7. Ensure clippy passes
8. Add to Makefile lint targets
9. Ensure no blocking ops in async context (`spawn_blocking` or `rayon`)

## References

- [Spice Docs](https://spiceai.org/docs), [Cookbook](https://github.com/spiceai/cookbook)
- [Architecture Decisions](docs/decisions/), [Threat Models](docs/threat_models/)
