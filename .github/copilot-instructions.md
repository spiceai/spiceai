# Spice.ai GitHub Copilot Instructions

## Project Overview

Spice is a SQL query, search, and LLM-inference engine in Rust for data apps and agents. Provides federated SQL querying, data acceleration/materialization, search (vector, keyword, full-text), and AI inference via industry-standard APIs.

**Architecture**: Go CLI (`bin/spice`) + Rust runtime daemon (`bin/spiced`). Built on Apache DataFusion, Arrow, and DuckDB.

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

### Error Handling (CRITICAL)

- **Use SNAFU**: Derive `Snafu` and `Debug` on error enums
- **NO `.unwrap()`/`.expect()` in non-test code**: Use `?` operator or `match`
- **In tests**: Use `.expect("descriptive message")` instead of `.unwrap()`
- **Use `unreachable!()`**: Only for provably impossible cases
- **Use `ensure!` macro**: Preferred over `if` + `return Err`
- **Define `Result` type alias**: `pub type Result<T, E = Error> = std::result::Result<T, E>;`
- **Don't use `assert!()` (or related) macros in non-test code**: Prefer proper error handling, or marking with `unreachable!()` if the assertion is truly unreachable. Alternatively, make the assertion a `debug_assert!()` assertion to only fire in debug builds instead of release builds. `assert!()` macros can have case-by-case exceptions, for example for compile-time assertions that would prevent a build from being released to begin with.

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

### Logging & Streams (CRITICAL)

- **Use `tracing::`** not `log::` (tracing::info!, tracing::error!, etc.)
- **AVOID `stream!` macro**: Breaks rust-analyzer, hard to debug

### Async/Blocking (CRITICAL)

**Rule**: Async code must reach `.await` within 10-100 microseconds.

**Never block async runtime**:

- ❌ `std::thread::sleep`/`std::fs`/blocking DB → ✅ `tokio::time::sleep`/`tokio::fs`/async pools

**Handling blocking operations**:

1. **Blocking I/O**: `tokio::task::spawn_blocking(move || { /* sync code */ }).await?`
2. **CPU-bound**: Use `rayon::spawn()` with `oneshot::channel()` for result
3. **Background tasks**: `std::thread::spawn()`

### Clippy (Enforced in CI)

**Errors**: `clippy::pedantic`, `clippy::unwrap_used`, `clippy::expect_used`, `clippy::clone_on_ref_ptr`  
**Allowed**: `clippy::module_name_repetitions`, `clippy::large_futures`

## Performance & Memory (CRITICAL)

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

### Connection Pooling & Arc Cloning

### Stream Handling (CRITICAL)

- **AVOID `stream!` macro**: Breaks rust-analyzer IDE hints and makes debugging harder
- **Use manual Stream implementations or `async_stream::stream` sparingly**: When unavoidable, document why

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

### Clippy Rules (Enforced in CI)

The following clippy rules are **errors** in CI (`-Dwarnings`):

- `clippy::pedantic` - All pedantic lints enabled
- `clippy::unwrap_used` - No `.unwrap()` calls
- `clippy::expect_used` - No `.expect()` calls (use proper error handling)
- `clippy::clone_on_ref_ptr` - Don't clone `Arc`/`Rc` unnecessarily

Allowed exceptions:

- `clippy::module_name_repetitions` - OK to have `module_name::ModuleName`
- `clippy::large_futures` - Allowed due to async complexity

### Performance and Memory Management

#### Zero-Copy Operations

- **Prefer zero-copy** when working with Arrow arrays
- Use `Arc<dyn Array>` for type-erased arrays (cheap to clone)
- Avoid unnecessary data copies between Arrow, DataFusion, and connectors

```rust
// GOOD - zero-copy sharing
let array: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
let shared = Arc::clone(&array); // Cheap reference count increment

// BAD - unnecessary copy
let copied = array.to_data().clone(); // Avoid unless necessary
```

#### Connection Pooling

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

#### Arc/Rc Cloning

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

### Binary Targets

- `bin/spiced/` - Runtime daemon (Rust)
- `bin/spice/` - CLI (Go)

### Core Crates

- `runtime/` - Orchestration, component init
- `data_components/` - TableProvider implementations
- `app/` - Spicepod parsing
- `datafusion/` - DataFusion extensions
- `llms/` - LLM inference
- `model_components/` - ML/LLM loading
- `search/` - Search functionality
- `test-framework/` - Testing utilities

### Runtime Sub-Crates

- `runtime-acceleration/` - Arrow, DuckDB, SQLite, PostgreSQL
- `runtime-auth/`, `runtime-datafusion-udfs/`, `runtime-secrets/`, `runtime-parameters/`

### Extension Points (see `docs/EXTENSIBILITY.md`)

1. Data Connector, 2. Data Accelerator, 3. Catalog Connector, 4. Secret Stores, 5. Models, 6. Embeddings

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

Use `INSTA_UPDATE=1` to regenerate snapshots.

## Feature Flags

`spiced` uses optional heavy dependencies. When adding connectors:

1. Make dependency optional: `dep:newdb-client`
2. Add feature: `newdb = ["runtime/newdb", "data_components/newdb"]`
3. Gate code: `#[cfg(feature = "newdb")]`
4. Update Makefile lint targets

## Development Workflow

### Setup (macOS/Linux)

```bash
brew install rust go cmake protobuf
make install-dev
export PATH="$PATH:$HOME/.spice/bin"
```

### VSCode Settings

```json
"[rust]": { "editor.defaultFormatter": "rust-lang.rust-analyzer", "editor.formatOnSave": true },
"rust-analyzer.check.command": "clippy",
"rust-analyzer.check.extraArgs": ["--", "-Dwarnings", "-Dclippy::expect_used", "-Dclippy::pedantic", "-Dclippy::unwrap_used", "-Dclippy::clone_on_ref_ptr", "-Aclippy::module_name_repetitions"]
```

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

- Use `tokio` runtime (see `bin/spiced/src/main.rs`)
- Use `async_trait` for trait async methods
- Use `CancellationToken` for shutdown (see `runtime/src/cancellable_task.rs`)

## Key Files

- `docs/PRINCIPLES.md`, `docs/EXTENSIBILITY.md`, `docs/dev/style_guide.md`, `docs/dev/error_handling.md`
- `CONTRIBUTING.md`, `Makefile`, `Cargo.toml`, `crates/runtime/src/lib.rs`

## Gotchas

1. Don't use `stream!` macro
2. Always use feature gates for optional connectors
3. Spicepod is YAML config format
4. Integration tests need credentials (`spice login` or `.env`)
5. testoperator is the test harness
6. Workspace uses Rust edition 2024
7. Allocator customizable (default: snmalloc, can use jemalloc/mimalloc)
8. New files should include copyright header. The current year is 2026. Required file types: `.rs`, `.go`

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
