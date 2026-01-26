# Spice.ai Rust SDK - GitHub Copilot Instructions

## Project Overview

This is the official Rust SDK for Spice.ai, providing a client library for connecting to and querying Spice.ai runtime instances via Apache Arrow Flight SQL.

**Architecture:** Arrow Flight SQL client built on Apache Arrow 57, tonic gRPC, and rustls TLS.

**Core Principle:** Developer Experience First — Simple, type-safe, and performant API for querying Spice.ai.

## Build & Test Commands

```bash
cargo build              # Dev build
cargo build --release    # Release build
cargo test               # Run all tests
cargo clippy --all-features  # Lint check
cargo fmt                # Format code
cargo fmt --check        # Check formatting
```

## Rust Coding Standards

### Error Handling (CRITICAL)

- Use SNAFU: Derive `Snafu` and `Debug` on error enums
- NO `.unwrap()`/`.expect()` in library code: Use `?` operator or `match`
- In tests: Use `.expect("descriptive message")` instead of `.unwrap()`
- Use `ensure!` macro: Preferred over `if` + `return Err`
- Define `Result` type alias: `pub type Result<T, E = Error> = std::result::Result<T, E>;`
- Don't use `assert!()` macros in non-test code: Prefer proper error handling

```rust
// GOOD
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to connect to {endpoint}: {source}"))]
    ConnectionFailed { endpoint: String, source: tonic::transport::Error },
}
ensure!(!data.is_empty(), DataEmptySnafu);
let value = option.context(ValueMissingSnafu)?;

// Tests only
#[cfg(test)]
fn test() { let value = option.expect("descriptive message"); }
```

### Logging (CRITICAL)

- Use `tracing::` for logging: Use `tracing::info!`, `tracing::error!`, `tracing::debug!`, etc.
- DO NOT use `log::`: The project uses `tracing` crate
- DO NOT add newlines in log messages or error strings

```rust
// GOOD
tracing::info!("Connecting to Spice.ai endpoint");
tracing::error!("Failed to execute query: {}", error);

// BAD - don't use log crate
log::info!("Starting runtime");
```

### Async/Blocking (CRITICAL)

Rule: Async code must reach `.await` within 10-100 microseconds.

Never block async runtime:

- ❌ `std::thread::sleep` → ✅ `tokio::time::sleep`
- ❌ `std::fs` → ✅ `tokio::fs`
- ❌ Blocking operations → ✅ `tokio::task::spawn_blocking`

```rust
// GOOD - use spawn_blocking for sync operations
let result = tokio::task::spawn_blocking(move || {
    // Blocking operations here
}).await?;

// BAD - blocking in async context
async fn bad() {
    std::thread::sleep(Duration::from_secs(1)); // Blocks runtime!
}
```

### Clippy (Enforced in CI)

Errors: `clippy::pedantic`, `clippy::unwrap_used`, `clippy::expect_used`, `clippy::clone_on_ref_ptr`

Allowed: `clippy::module_name_repetitions`, `clippy::large_futures`

## Performance & Memory (CRITICAL)

### Zero-Copy Operations

- Prefer zero-copy with Arrow arrays: avoid `.to_data()`, `.clone()`, conversions
- Use `Arc<dyn Array>` for type-erased arrays (cheap clone, shares buffers)
- Use `RecordBatch::slice()` instead of filtering/copying
- Prefer `ArrayRef` in function signatures over owned arrays

```rust
// GOOD
let subset = batch.slice(offset, length);  // Shares buffers
let shared: ArrayRef = Arc::clone(&array);  // Just refcount++

// BAD
let values: Vec<i32> = array.values().iter().copied().collect();  // Avoid
```

### Stream Handling

- AVOID `stream!` macro: Breaks rust-analyzer IDE hints
- Keep streaming: Don't collect streams early (`RecordBatchStream`)

```rust
// GOOD - streaming
while let Some(batch) = stream.next().await {
    process_batch(batch?)?;
}

// BAD - materializes entire dataset (OOM risk)
let all_batches: Vec<RecordBatch> = stream.try_collect().await?;
```

### Allocation Minimization

- Reuse buffers: `String::clear()`, `Vec::clear()` to keep capacity
- Prefer `&str`/`&[T]` in signatures over `String`/`Vec<T>`
- Use `Cow<str>`: When ownership might be needed but often isn't
- Pre-allocate: `Vec::with_capacity()`, array builders with hints

### Arc/Rc Cloning

- Avoid unnecessary `Arc`/`Rc` clones (caught by `clippy::clone_on_ref_ptr`)
- `Arc::clone()` is cheap but not free - don't clone in hot loops
- When passing `Arc<T>` to functions, prefer `&Arc<T>` if you don't need ownership

```rust
// GOOD - function signature
fn process_data(data: &Arc<RecordBatch>) { ... }
```

## Project Structure

```
src/
├── lib.rs          # Public API exports
├── client.rs       # SpiceClient implementation
├── config.rs       # Configuration and constants
├── flight.rs       # Arrow Flight SQL client
├── tls.rs          # TLS/rustls configuration
└── util.rs         # Utilities (backoff, retry)

tests/
└── client_test.rs  # Integration tests
```

## Development Workflow

### VSCode Settings

```json
"[rust]": { "editor.defaultFormatter": "rust-lang.rust-analyzer", "editor.formatOnSave": true },
"rust-analyzer.check.command": "clippy",
"rust-analyzer.check.extraArgs": ["--", "-Dwarnings", "-Dclippy::expect_used", "-Dclippy::pedantic", "-Dclippy::unwrap_used", "-Dclippy::clone_on_ref_ptr", "-Aclippy::module_name_repetitions"]
```

### PR Process

- Branch from `trunk`, link issue, add tests
- Ensure clippy passes with no warnings
- Run `cargo fmt` before committing
- Add integration tests for new functionality

## User-Facing Error Messages

Format: `Failed to {action}: {specific_error}`

1. Simple but specific language
2. Provide actionable context
3. Exclude internal implementation details

```rust
#[snafu(display("Failed to connect to Spice.ai at {endpoint}: {source}"))]
ConnectionFailed { endpoint: String, source: tonic::transport::Error },
```

## Gotchas

1. Don't use `stream!` macro - breaks rust-analyzer
2. Workspace uses Rust edition 2024
3. Integration tests need `SCP_SPICEAI_TPCH_API_KEY` environment variable
4. Local tests require a running Spice runtime at `localhost:50051`
5. Use `rustls` with `aws-lc-rs` crypto provider (not ring by default)
6. Arrow Flight requires proper TLS configuration for cloud endpoints

## Testing

### Environment Variables

- `SCP_SPICEAI_TPCH_API_KEY`: API key for cloud TPCH dataset tests
- Local tests connect to `localhost:50051`

### Test Categories

- Unit tests: `cargo test --lib`
- Integration tests: `cargo test --test client_test`
  - Cloud tests require API key
  - Local tests require running Spice runtime

## Key Dependencies

| Crate          | Purpose                       |
| -------------- | ----------------------------- |
| `arrow`        | Apache Arrow arrays and types |
| `arrow-flight` | Arrow Flight SQL protocol     |
| `tonic`        | gRPC client                   |
| `rustls`       | TLS implementation            |
| `tokio`        | Async runtime                 |
| `snafu`        | Error handling                |
| `tracing`      | Logging                       |

## References

- [Spice.ai Docs](https://spiceai.org/docs)
- [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
- [Rust SDK on crates.io](https://crates.io/crates/spiceai)
