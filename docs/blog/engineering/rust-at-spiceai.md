# Rust at Spice AI: Engineering a High-Performance Query Engine

> How we leverage Rust's safety guarantees and zero-cost abstractions to build a reliable, performant data platform

---

## 📚 Engineering at Spice AI Series

This article is part of our **Engineering at Spice AI** series, where we share technical deep-dives into the technologies and practices that power our SQL query, search, and inference engine.

- **Rust at Spice AI** *(You are here)*
- [Apache Arrow at Spice AI](apache-arrow-at-spiceai.md) — Arrow as our core data format
- [Apache DataFusion at Spice AI](apache-datafusion-at-spiceai.md) — Our SQL query engine foundation
- [DuckDB at Spice AI](duckdb-at-spiceai.md) — Embedded analytics and acceleration
- [Apache Iceberg at Spice AI](apache-iceberg-at-spiceai.md) — Open table format integration
- [Vortex at Spice AI](vortex-at-spiceai.md) — Columnar compression for Cayenne
- [Apache Ballista at Spice AI](apache-ballista-at-spiceai.md) — Distributed query execution

---

## Table of Contents

- [Why Rust?](#why-rust)
- [The Tradeoffs](#the-tradeoffs)
- [Rust Version and Edition](#rust-version-and-edition)
- [Error Handling with SNAFU](#error-handling-with-snafu)
- [Async Patterns and Runtime Architecture](#async-patterns-and-runtime-architecture)
- [Memory Management and Zero-Copy](#memory-management-and-zero-copy)
- [Clippy and Code Quality](#clippy-and-code-quality)
- [Project Structure: 57+ Crates](#project-structure-57-crates)
- [Performance Patterns](#performance-patterns)
- [Lessons Learned](#lessons-learned)
- [Getting Started Contributing](#getting-started-contributing)

---

Spice is a federated SQL query, search, and LLM-inference engine built from the ground up in Rust. We chose Rust not because it's trendy, but because it solves the fundamental challenges of building data infrastructure: memory safety without garbage collection pauses, fearless concurrency, and zero-cost abstractions that let us write high-level code that compiles to efficient machine code.

This post shares how we use Rust at Spice AI—our conventions, patterns, and lessons learned building a production query engine.

## Why Rust?

Data infrastructure has unique requirements that make Rust an ideal choice:

1. **Memory Safety Without GC Pauses** — Query engines process millions of rows. Garbage collection pauses would destroy latency predictability. Rust's ownership system gives us memory safety through compile-time checks, not runtime collection.

2. **Predictable Performance** — When a query needs to complete in 5ms, we can't afford runtime surprises. Rust's zero-cost abstractions mean high-level code like iterators compile to efficient loops.

3. **Fearless Concurrency** — Processing multiple queries simultaneously requires safe concurrent access to shared state. Rust's type system prevents data races at compile time.

4. **Ecosystem Compatibility** — The Rust data ecosystem (Apache Arrow, DataFusion, DuckDB bindings) is mature and performant. We build on these foundations rather than reinventing them.

5. **Developer Experience** — Cargo, rust-analyzer, and the Rust toolchain provide an excellent development experience. The compiler catches entire classes of bugs before they reach production.

## The Tradeoffs

Rust isn't a free lunch. Here's what we traded for those benefits:

### Steep Learning Curve

Rust has a notoriously steep learning curve. Concepts like ownership, borrowing, and lifetimes are unfamiliar to developers from garbage-collected languages. New team members typically need 2-4 weeks of focused learning before becoming productive.

We mitigate this with:

- **Pair programming** — Experienced Rustaceans mentor newcomers
- **Code review focus** — Reviews emphasize idiomatic patterns, not just correctness
- **Internal documentation** — Our style guide and error handling docs help standardize patterns

### Slower Compilation

Rust's compile times are slow compared to Go or TypeScript. Our full workspace takes several minutes to build from scratch. Incremental builds are faster, but still noticeable.

We address this with:

- **Workspace splitting** — 57+ smaller crates mean changes only rebuild affected code
- **Feature flags** — Heavy dependencies are optional, reducing build scope
- **sccache** — Shared compilation cache across developers and CI
- **cargo-nextest** — Parallel test execution

### Async Complexity

Async Rust is powerful but adds cognitive overhead. Unlike Go's goroutines, Rust async requires understanding `Pin`, `Future`, lifetimes in async contexts, and runtime selection. The `async-trait` crate adds boilerplate (though this improved in Edition 2024).

The payoff: explicit control over blocking vs. non-blocking code, and no hidden runtime costs.

### Smaller Talent Pool

Rust developers are harder to find than Python, JavaScript, or even Go developers. The language is younger and less widely taught.

However, we've found that Rust's reputation attracts engineers who care deeply about correctness and performance—exactly the mindset needed for data infrastructure.

### Ecosystem Gaps

While Rust's data ecosystem is strong (Arrow, DataFusion, DuckDB), some areas have fewer options than mature ecosystems. We sometimes need to:

- Contribute to upstream projects to add features
- Maintain forks for version compatibility
- Build bindings to C libraries ourselves

We see this as investment: our contributions benefit the broader community, and we gain deep understanding of our dependencies.

### Is It Worth It?

For Spice, absolutely. The bugs Rust prevents—use-after-free, data races, null pointer dereferences—would be devastating in a query engine handling production workloads. The upfront cost of learning Rust pays dividends in operational stability.

As the [Discord engineering team wrote](https://discord.com/blog/why-discord-is-switching-from-go-to-rust): "Rust gives us the ability to be incredibly resource efficient and reliable while still being productive."

## Rust Version and Edition

We stay current with Rust's evolution:

```toml
# Cargo.toml
[workspace.package]
rust-version = "1.91"
edition = "2024"
```

**Edition 2024** brings several improvements we leverage:

- **Improved `async` semantics** — Cleaner async trait definitions
- **Enhanced pattern matching** — More expressive match guards
- **Updated default lints** — Stricter out-of-the-box checking

We update our minimum Rust version regularly to take advantage of new language features and standard library improvements. Our CI builds against both the MSRV and stable Rust.

## Error Handling with SNAFU

Rust's error handling is powerful but can be verbose. We use [SNAFU](https://docs.rs/snafu/latest/snafu/) to reduce boilerplate while maintaining actionable, user-friendly error messages.

### Why SNAFU?

SNAFU (Situation Normal: All Fouled Up) provides:

- **Derive macros** for generating error enums
- **Context selectors** that capture additional information at the error site
- **Display formatting** for human-readable messages
- **Source chaining** for wrapping underlying errors

### Our Error Conventions

Every module defines its own error types:

```rust
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to register dataset {dataset_name} ({connector}): {source}"
    ))]
    DatasetRegistration {
        dataset_name: String,
        connector: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Invalid parameter value for '{parameter}': expected {expected}, got '{actual}'"
    ))]
    InvalidParameter {
        parameter: String,
        expected: String,
        actual: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
```

### Using `ensure!` Instead of `if` + `return Err`

The `ensure!` macro provides concise condition checking:

```rust
// Good: Resembles assert!, more concise
ensure!(!data.is_empty(), DataEmptySnafu);

// Bad: Verbose, easy to forget braces
if data.is_empty() {
    return Err(Error::DataEmpty {});
}
```

### Using `context` for Error Wrapping

The `context` method adds information when converting errors:

```rust
// Good: Adds context about what operation failed
let schema = table.schema()
    .context(UnableToGetSchemaSnafu { table_name: &name })?;

// Bad: Loses context about which table
let schema = table.schema()
    .map_err(|e| Error::UnableToGetSchema { source: e })?;
```

### User-Facing Error Messages

Our error messages follow these guidelines:

1. **Simple but specific language** — "Failed to connect" not "Connection establishment unsuccessful"
2. **Include the affected resource** — "Failed to register dataset `customers` (postgresql)" not "Failed to register dataset"
3. **Provide actionable guidance** — Link to documentation, suggest configuration changes
4. **Exclude internal concepts** — Users don't need to know about "read providers" or "table sources"

```rust
#[snafu(display(
    "Failed to register dataset {dataset_name} ({connector}): Invalid file format. \
    Expected '.csv' but found '.parquet'. \
    Update the 'file_format' parameter. \
    See: https://spiceai.org/docs/components/data-connectors"
))]
```

### The Critical Rule: No `unwrap()` or `expect()` in Non-Test Code

This isn't just style preference—it's about production reliability. The infamous [Cloudflare outage of July 2019](https://blog.cloudflare.com/details-of-the-cloudflare-outage-on-july-2-2019/) was caused by a regex that triggered excessive backtracking, but the root cause was a `.unwrap()` that panicked when the regex engine timed out. A single `unwrap()` in the wrong place brought down a significant portion of the internet for 27 minutes.

We've seen similar patterns in our own development. Early in Spice's history, a `.unwrap()` on a configuration parsing result caused crashes when users provided unexpected input. The fix was simple—use `?` instead—but the lesson stuck: **every `unwrap()` is a latent production incident**.

We enforce this with Clippy:

```rust
// NEVER in production code
let value = option.unwrap();          // Panics on None
let value = result.expect("oops");    // Panics on Err

// Always use proper error handling
let value = option.context(ValueMissingSnafu)?;
let value = result.context(OperationFailedSnafu)?;

// Only in tests
#[cfg(test)]
mod tests {
    fn test_foo() {
        let value = option.expect("test setup should provide value");
    }
}
```

## Async Patterns and Runtime Architecture

Spice runs on [Tokio](https://tokio.rs/), the most widely-used async runtime for Rust. But we've learned that naive async usage can cause serious problems for data infrastructure.

### The Problem: Blocking the Runtime

DataFusion uses cooperative multitasking—tasks voluntarily yield control by reaching `.await` points. If a task does CPU-intensive work without yielding, it starves other tasks:

```rust
// BAD: Blocks the runtime, starves other queries
async fn process_batch(batch: RecordBatch) -> Result<RecordBatch> {
    // This could take seconds for large batches
    let result = expensive_cpu_computation(&batch);  // No .await!
    Ok(result)
}
```

### The Rule: Reach `.await` Within 10-100 Microseconds

We follow this discipline:

```rust
// Good: CPU work happens on dedicated pool
async fn process_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let result = expensive_cpu_computation(&batch);
        let _ = tx.send(result);
    });
    rx.await.context(ComputationFailedSnafu)?
}
```

### Separate Tokio Runtimes

We maintain separate Tokio runtime instances:

- **HTTP Server Runtime** — Handles health checks and API requests
- **Query Processing Runtime** — Executes DataFusion queries

Why? DataFusion queries can consume all CPU time, blocking the HTTP server from responding to health checks. Kubernetes then kills the pod thinking it's unresponsive. Separate runtimes isolate these concerns:

```rust
// HTTP runtime must always respond to /health
// Query runtime can be fully loaded with long queries
// They don't interfere with each other
```

### Handling Blocking Operations

Different strategies for different blocking types:

**Blocking I/O (file system, sync DB clients):**

```rust
let result = tokio::task::spawn_blocking(move || {
    std::fs::read_to_string("config.toml")
}).await??;
```

**CPU-bound computations:**

```rust
let (tx, rx) = tokio::sync::oneshot::channel();
rayon::spawn(move || {
    let result = compress_batch(&batch);
    let _ = tx.send(result);
});
let compressed = rx.await?;
```

**Long-running background tasks:**

```rust
std::thread::spawn(move || {
    // This never blocks the async runtime
    run_background_maintenance();
});
```

### Never Hold Locks Across `.await`

This causes deadlocks and priority inversion:

```rust
// BAD: Lock held across await
async fn bad_pattern(&self) {
    let guard = self.data.lock();
    some_async_operation().await;  // Other tasks waiting for lock are blocked!
    drop(guard);
}

// Good: Minimize lock scope
async fn good_pattern(&self) {
    let data = {
        let guard = self.data.lock();
        guard.clone()  // Lock dropped here
    };
    some_async_operation().await;
}
```

We use `parking_lot` locks which are faster than std locks and panic if you try to use them across await points in certain configurations.

## Memory Management and Zero-Copy

Query engines process massive datasets. Naive memory management leads to either out-of-memory errors or unnecessary copying that destroys performance.

### Apache Arrow as the Foundation

All data in Spice flows as Arrow `RecordBatch` objects—columnar, immutable, and designed for zero-copy:

```rust
// RecordBatches are cheap to "clone" - just Arc increments
let shared_batch = Arc::clone(&batch);

// Slicing shares the underlying buffer
let subset = batch.slice(offset, length);  // No data copy

// Type erasure through Arc<dyn Array>
let column: ArrayRef = Arc::clone(batch.column(0));  // Reference counting
```

### Buffer Reuse Patterns

We reuse allocations where possible:

```rust
// Good: Reuse buffer, clear contents
let mut buffer = String::with_capacity(1024);
for item in items {
    buffer.clear();  // Keeps capacity
    write!(&mut buffer, "{}", item)?;
    process(&buffer);
}

// Bad: New allocation every iteration
for item in items {
    let buffer = format!("{}", item);  // Allocates each time
    process(&buffer);
}
```

### Pre-allocation with Capacity Hints

Arrow builders accept capacity hints:

```rust
// Good: Pre-allocate based on expected size
let mut builder = StringBuilder::with_capacity(expected_rows, expected_bytes);

// Bad: Grows buffer repeatedly
let mut builder = StringBuilder::new();
```

### Avoid Intermediate Collections

Use iterators instead of collecting into vectors:

```rust
// Good: Zero intermediate allocations
let sum: i64 = values.iter()
    .filter(|&&x| x > 0)
    .map(|&x| i64::from(x))
    .sum();

// Bad: Creates intermediate Vec
let positive: Vec<_> = values.iter().filter(|&&x| x > 0).collect();
let sum: i64 = positive.iter().map(|&x| i64::from(x)).sum();
```

## Clippy and Code Quality

We use [Clippy](https://doc.rust-lang.org/stable/clippy/) extensively with strict settings. Our CI fails on any Clippy warning.

### Configuration

```toml
# clippy.toml
allow-expect-in-tests = true
future-size-threshold = 45520
```

### Enforced Lints

These lints are errors (not warnings):

| Lint                       | Purpose                             |
| -------------------------- | ----------------------------------- |
| `clippy::pedantic`         | All pedantic-level checks enabled   |
| `clippy::unwrap_used`      | No `.unwrap()` in non-test code     |
| `clippy::expect_used`      | No `.expect()` in non-test code     |
| `clippy::clone_on_ref_ptr` | Avoid unnecessary `Arc`/`Rc` clones |

### Allowed Exceptions

Some lints are intentionally disabled:

```rust
// Module name repetitions are acceptable
// module_name::ModuleName is fine
#[allow(clippy::module_name_repetitions)]

// Large futures are accepted due to async complexity
#[allow(clippy::large_futures)]
```

### Running Clippy Locally

```bash
# Run clippy with our settings
make lint-rust

# Auto-fix what can be fixed
make lint-rust-fix
```

## Project Structure: 57+ Crates

Spice uses a multi-crate workspace structure. This provides:

- **Faster incremental builds** — Change one crate, rebuild only what depends on it
- **Clear dependency boundaries** — Crates enforce module boundaries
- **Optional feature composition** — Heavy dependencies are feature-gated

### Key Crates

| Crate                         | Purpose                                          |
| ----------------------------- | ------------------------------------------------ |
| `bin/spiced`                  | Runtime daemon entry point                       |
| `crates/runtime`              | Core orchestration and component initialization  |
| `crates/data_components`      | TableProvider implementations for data sources   |
| `crates/runtime-datafusion`   | DataFusion integration and session configuration |
| `crates/runtime-acceleration` | Acceleration engines (DuckDB, SQLite, Arrow)     |
| `crates/search`               | Vector, keyword, and full-text search            |
| `crates/llms`                 | LLM inference providers                          |

### Feature Flags for Heavy Dependencies

Not every deployment needs every connector. We use Cargo features to make dependencies optional:

```toml
# bin/spiced/Cargo.toml
[features]
default = ["duckdb", "postgres"]
duckdb = ["runtime/duckdb", "data_components/duckdb"]
postgres = ["runtime/postgres", "data_components/postgres"]
snowflake = ["runtime/snowflake", "data_components/snowflake"]
```

Code is gated accordingly:

```rust
#[cfg(feature = "duckdb")]
mod duckdb_connector;

#[cfg(feature = "duckdb")]
pub fn create_duckdb_provider() -> impl TableProvider {
    // ...
}
```

## Performance Patterns

### Fine-Grained Locking

Query engines have high read concurrency. We optimize for this:

```rust
// Good: RwLock for read-heavy workloads
use parking_lot::RwLock;

struct Cache {
    entries: Arc<RwLock<HashMap<String, Data>>>,
    stats: Arc<AtomicU64>,  // Lock-free for simple counters
}

async fn get(&self, key: &str) -> Option<Data> {
    // Read lock: multiple readers allowed
    let data = { self.entries.read().get(key).cloned() };
    self.stats.fetch_add(1, Ordering::Relaxed);
    data
}
```

### DashMap for Concurrent Access

When possible, we avoid explicit locking entirely:

```rust
use dashmap::DashMap;

// DashMap: sharded, concurrent HashMap
let cache: Arc<DashMap<String, Data>> = Arc::new(DashMap::new());

// No external lock needed
cache.insert(key, value);
let data = cache.get(&key);
```

### Connection Pooling

Database connections are expensive to create. We always use connection pools:

```rust
// Pool creation never fails—errors happen on get()
let pool = Pool::builder(manager).build()?;

// Later, when we need a connection:
let conn = pool.get().await?;  // Error only here if pool exhausted
```

### SIMD and Arrow Compute Kernels

We let Arrow handle low-level optimizations:

```rust
use arrow::compute::add;

// Good: Arrow's add kernel uses SIMD
let result = add(&left_array, &right_array)?;

// Bad: Manual loop misses SIMD opportunities
for i in 0..array.len() {
    result.push(left.value(i) + right.value(i));
}
```

## Lessons Learned

After building a production query engine in Rust, here are our key takeaways:

### 1. The Compiler is Your Friend

Rust's strict compiler catches bugs that would be production incidents in other languages. Embrace the compiler's feedback—it's saving you from 3 AM pages.

### 2. Async is Powerful but Tricky

Async Rust enables high-throughput concurrent systems, but requires discipline. Know when to use `spawn_blocking`, when to use separate runtimes, and never hold locks across await points.

### 3. Zero-Copy Requires Architectural Thinking

You can't retrofit zero-copy into an existing system. We designed around Arrow from the start, ensuring data flows as immutable, reference-counted batches.

### 4. Error Messages Matter

Users don't read source code. Every error message should explain what went wrong and how to fix it. SNAFU makes this tractable.

### 5. Feature Flags Enable Modularity

Heavy dependencies shouldn't penalize users who don't need them. Feature flags let us build a minimal binary or a fully-loaded one from the same codebase.

### 6. Invest in Tooling

rust-analyzer, Clippy, and cargo-watch make Rust development productive. We maintain strict Clippy settings because catching issues early is cheaper than debugging production.

## Getting Started Contributing

Want to contribute to Spice? Here's how to get started:

### Setup (macOS/Linux)

```bash
# Install dependencies
brew install rust go cmake protobuf

# Clone and build
git clone https://github.com/spiceai/spiceai
cd spiceai
make install-dev

# Add to PATH
export PATH="$PATH:$HOME/.spice/bin"
```

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires credentials)
make test-integration
```

### VS Code Configuration

We recommend these rust-analyzer settings:

```json
{
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer",
    "editor.formatOnSave": true
  },
  "rust-analyzer.check.command": "clippy",
  "rust-analyzer.check.extraArgs": [
    "--", "-Dwarnings",
    "-Dclippy::expect_used",
    "-Dclippy::pedantic",
    "-Dclippy::unwrap_used",
    "-Dclippy::clone_on_ref_ptr",
    "-Aclippy::module_name_repetitions"
  ]
}
```

### Key Files to Read

- [CONTRIBUTING.md](../../CONTRIBUTING.md) — Contribution guidelines
- [docs/dev/style_guide.md](../dev/style_guide.md) — Rust style conventions
- [docs/dev/error_handling.md](../dev/error_handling.md) — Error handling guidelines
- [docs/PRINCIPLES.md](../PRINCIPLES.md) — Project principles

---

## Conclusion

Rust enables us to build a high-performance, reliable query engine without sacrificing developer productivity. The language's safety guarantees, zero-cost abstractions, and excellent tooling make it ideal for data infrastructure.

Our conventions—SNAFU for errors, strict Clippy settings, separate async runtimes, zero-copy Arrow data flow—evolved from real production challenges. We hope sharing them helps other teams building similar systems.

Want to learn more? Check out the other posts in our Engineering at Spice AI series, or join us on [GitHub](https://github.com/spiceai/spiceai).

---

## References

- [The Rust Programming Language](https://doc.rust-lang.org/book/)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [SNAFU Documentation](https://docs.rs/snafu/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [Apache Arrow Rust](https://arrow.apache.org/rust/)
- [Cloudflare Outage Post-Mortem (July 2019)](https://blog.cloudflare.com/details-of-the-cloudflare-outage-on-july-2-2019/) — Why `unwrap()` in production is dangerous
- [Discord: Why We Switched from Go to Rust](https://discord.com/blog/why-discord-is-switching-from-go-to-rust)
- [Figma: Rust in Production](https://www.figma.com/blog/rust-in-production-at-figma/)
- [AWS: Sustainability with Rust](https://aws.amazon.com/blogs/opensource/sustainability-with-rust/)


