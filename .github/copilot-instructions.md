# Spice.ai GitHub Copilot Instructions

## Project Overview

Spice is a SQL query, search, and LLM-inference engine written in Rust for data apps and agents. It provides federated SQL querying, data acceleration/materialization, search (vector, keyword, full-text), and AI inference through industry-standard APIs.

**Architecture**: Hybrid Go (CLI: `bin/spice`) + Rust (runtime daemon: `bin/spiced`). The runtime is built on Apache DataFusion, Arrow, and DuckDB.

**Core Principle**: Developer Experience First — Bring data and AI/ML to your application, not the other way around.

## Essential Build & Test Commands

```bash
# Build everything (release mode)
make install

# Build in dev mode (faster compilation)
make install-dev

# Build with custom features (important for lightweight builds)
SPICED_CUSTOM_FEATURES="postgres sqlite" make build-runtime

# Run linting (auto-fix Rust issues)
make lint-rust-fix
make lint  # Check without fixing

# Run tests
make test                    # Unit tests (cargo test --all --lib)
make test-integration        # Integration tests (requires .env or spice login)
make test-integration-models # Model integration tests

# Benchmarks with testoperator
cargo run -p testoperator -- run bench -p ./test/spicepods/tpch/sf1/federated/duckdb.yaml -s spiced -d ./.data --query-set tpch --validate
```

## Rust Coding Standards

### Error Handling (CRITICAL)

- **Use SNAFU for all errors**: Derive `Snafu` and `Debug` on error enums
- **NO `.unwrap()`**: Use `.expect("reason")` or `unreachable!()` instead
- **Use `ensure!` macro**: Preferred over manual `if` + `return Err`
- **Define errors in originating module**: Keep `Error` enum with the code that uses it
- **Always define `Result` type alias**: `pub type Result<T, E = Error> = std::result::Result<T, E>;`

```rust
// GOOD
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to connect to {connector}: {source}"))]
    ConnectionFailed { connector: String, source: std::io::Error },
}

ensure!(!data.is_empty(), DataEmptySnafu);

// BAD - avoid unwrap
let value = option.unwrap();

// GOOD - use expect or handle properly
let value = option.expect("value must be present at this point");
```

### Stream Handling (CRITICAL)

- **AVOID `stream!` macro**: Breaks rust-analyzer IDE hints and makes debugging harder
- **Use manual Stream implementations or `async_stream::stream` sparingly**: When unavoidable, document why

### Logging (CRITICAL)

- **Use `tracing::` for logging**: Use `tracing::info!`, `tracing::error!`, `tracing::debug!`, etc.
- **DO NOT use `log::`**: The project uses `tracing` crate, not `log` crate

```rust
// GOOD
tracing::info!("Starting runtime");
tracing::error!("Failed to connect: {}", error);

// BAD - don't use log crate
log::info!("Starting runtime");
```

### Clippy Rules (Enforced in CI)

The following clippy rules are **errors** in CI (`-Dwarnings`):

- `clippy::pedantic` - All pedantic lints enabled
- `clippy::unwrap_used` - No `.unwrap()` calls
- `clippy::expect_used` - No `.expect()` calls (use proper error handling)
- `clippy::clone_on_ref_ptr` - Don't clone `Arc`/`Rc` unnecessarily

Allowed exceptions:

- `clippy::module_name_repetitions` - OK to have `module_name::ModuleName`
- `clippy::large_futures` - Allowed due to async complexity

### User-Facing Error Messages

1. **Use simple but specific language**: "Failed to read from dataset mytable (duckdb)" not "Unable to get read provider"
2. **Specify affected resource**: Always include dataset/model/catalog name
3. **Provide actionable steps**: Link to docs, suggest config fixes
4. **Exclude internal concepts**: Don't mention "read provider", "table source", etc.
5. **Format**: `Failed to {action} {resource_type} {name} ({connector}): {specific_error}`

Example:

```rust
#[snafu(display(
    "Failed to register dataset {dataset_name} ({connector}): Invalid file format. \
    Expected '.csv' but found '.parquet'. \
    Update the 'file_format' parameter in your spicepod. \
    See: https://spiceai.org/docs/components/data-connectors"
))]
```

## Project Structure & Key Components

### Binary Targets

- `bin/spiced/` - Runtime daemon (Rust) - The main engine
- `bin/spice/` - CLI tool (Go) - User-facing commands

### Core Crates (High-Level)

- `crates/runtime/` - Main runtime logic, orchestration, component initialization
- `crates/data_components/` - DataFusion `TableProvider` implementations for all connectors
- `crates/app/` - Application model, spicepod parsing
- `crates/datafusion/` - DataFusion extensions and optimizations
- `crates/llms/` - LLM inference (chat completions, embeddings)
- `crates/model_components/` - ML/LLM model loading and serving
- `crates/search/` - Search functionality (vector, text, keyword)
- `crates/test-framework/` - Testing utilities, Spicetest framework

### Runtime Sub-Crates (Modular Runtime Features)

- `runtime-acceleration/` - Data acceleration engines (Arrow, DuckDB, SQLite, PostgreSQL)
- `runtime-auth/` - Authentication and authorization
- `runtime-datafusion-udfs/` - User-defined functions
- `runtime-secrets/` - Secret store integrations
- `runtime-parameters/` - Parameter resolution and templates

### Extension Points (See `docs/EXTENSIBILITY.md`)

1. **Data Connector** (`crates/runtime/src/dataconnector/mod.rs`): Source data from external systems
2. **Data Accelerator** (`crates/runtime/src/databackend.rs`): Local storage engines (Arrow, DuckDB, SQLite, PostgreSQL)
3. **Catalog Connector**: External catalog integration (Iceberg, Unity, Glue)
4. **Secret Stores** (`crates/runtime/src/secrets.rs`): Secure credential storage
5. **Models** (`crates/model_components/`): ML/LLM model sources and inference
6. **Embeddings** (`crates/llms/src/embeddings/`): Vector embedding generation

## Testing Conventions

### Spicepod Naming (Critical for Integration Tests)

Format: `{connector[variant]}-{accelerator[variant]}-{test_variant}`

Examples:

- `s3[parquet]-federated` - S3 with no acceleration
- `mysql-duckdb[file]-on_zero_results` - MySQL with DuckDB file acceleration
- `file[csv]-arrow-refresh_append` - File connector with Arrow acceleration

**Rule**: Non-accelerated connectors MUST use `-federated` suffix.

### testoperator Usage

```bash
# Run benchmark with validation (TPC-H scale factor 1)
testoperator run bench -p test/spicepods/tpch/sf1/federated/duckdb.yaml \
  -s spiced -d ./.data --query-set tpch --validate

# Throughput test with 25 concurrent workers
testoperator run throughput -p benchmarks/file_tpch.yaml \
  -s spiced -d ./.data --query-set tpch --concurrency 25

# Load test for specific duration
testoperator run load -p test.yaml -s spiced --duration 60
```

Snapshots auto-generate for TPC-H/TPC-DS queries. Use `INSTA_UPDATE=1` to regenerate.

## Feature Flags & Build System

### Cargo Features (Important!)

`spiced` uses **optional heavy dependencies**. Default features in `bin/spiced/Cargo.toml`:

```toml
default = ["duckdb", "postgres", "sqlite", "mysql", ...]
```

When adding a new connector:

1. Make dependency optional: `dep:newdb-client`
2. Add feature to workspace crates: `newdb = ["runtime/newdb", "data_components/newdb"]`
3. Gate code with `#[cfg(feature = "newdb")]`
4. Update `Makefile` lint targets to include new feature

**Goal**: Minimize unused code warnings when building with `SPICED_CUSTOM_FEATURES`.

## Development Workflow

### Initial Setup (macOS/Linux)

See `CONTRIBUTING.md` for full setup. Quick start:

```bash
brew install rust go cmake protobuf  # macOS
make install-dev
export PATH="$PATH:$HOME/.spice/bin"
spice init test-app && cd test-app && spice run
```

### VSCode Configuration

Add to User Settings JSON for auto-format and clippy-on-save:

```json
"[rust]": {
  "editor.defaultFormatter": "rust-lang.rust-analyzer",
  "editor.formatOnSave": true
},
"rust-analyzer.check.command": "clippy",
"rust-analyzer.check.extraArgs": [
  "--", "-Dwarnings", "-Dclippy::expect_used", "-Dclippy::pedantic",
  "-Dclippy::unwrap_used", "-Dclippy::clone_on_ref_ptr",
  "-Aclippy::module_name_repetitions"
],
"rust-analyzer.cargo.target": "aarch64-apple-darwin"  // or your arch
```

### PR & Release Process

- **Branch from `trunk`**: All changes start here
- **Link issue**: PRs require associated issue (bug/proposal)
- **Tests required**: Code changes need tests
- **Follow style guides**: `docs/dev/style_guide.md`, `docs/dev/error_handling.md`
- **Release branches**: `release/X.Y` for minor releases, patch updates cherry-picked from trunk

## Common Patterns & Idioms

### Adding a Data Connector

1. Create `crates/data_components/src/{connector}.rs` implementing `TableProvider`
2. Create `crates/runtime/src/dataconnector/{connector}.rs` with factory function
3. Add to `crates/runtime/src/dataconnector/mod.rs` connector registry
4. Gate with `#[cfg(feature = "connector_name")]`
5. Update `bin/spiced/Cargo.toml` default features
6. Add integration test in `test/spicepods/{connector}/`
7. Document in README.md connector table

### DataFusion Integration

- Spice extends DataFusion with custom `TableProvider` implementations
- Use `ensure!` for plan validation, not `expect`
- Push-down filters/projections when possible for federation
- Acceleration wraps federated tables: `AcceleratedTable` → `FederatedTable` → connector `TableProvider`

### Async Patterns

- Use `tokio` runtime (configured in `bin/spiced/src/main.rs`)
- Prefer `async_trait` for trait async methods
- Use `CancellationToken` for graceful shutdown (see `runtime/src/cancellable_task.rs`)

## Key Files to Reference

- `docs/PRINCIPLES.md` - First principles guiding decisions
- `docs/EXTENSIBILITY.md` - Extension point interfaces
- `docs/dev/style_guide.md` - Rust style conventions
- `docs/dev/error_handling.md` - Error message guidelines
- `CONTRIBUTING.md` - Setup, build options, PR workflow
- `Makefile` - All build/test/lint targets
- `Cargo.toml` - Workspace dependencies and versions
- `crates/runtime/src/lib.rs` - Runtime entry point and orchestration

## Gotchas & Important Notes

1. **Don't use `stream!` macro** - Breaks IDE tooling
2. **Always use feature gates** - `#[cfg(feature = "...")]` for optional connectors
3. **Spicepod is the config format** - YAML files in `spicepod.yml` define datasets, models, acceleration
4. **Integration tests need credentials** - Run `spice login` or create `.env` file
5. **testoperator is the test harness** - Don't write manual benchmark scripts
6. **Cargo workspace uses edition 2024** - Use latest Rust features
7. **Allocator can be customized** - Default is snmalloc, can use jemalloc/mimalloc via features

## When Adding New Features

1. Check if it requires a new **extension point** (connector, accelerator, etc.)
2. Make dependencies **optional via features** if heavy
3. Add **integration tests** via testoperator or `test-framework`
4. Update **user-facing documentation** (README.md, docs/)
5. Follow **error message guidelines** for user experience
6. Ensure **clippy passes** with strict rules before PR
7. Add to **Makefile lint targets** if new features added

## Questions? References

- [Spice Docs](https://spiceai.org/docs)
- [Cookbook Recipes](https://github.com/spiceai/cookbook)
- [Architecture Decisions](docs/decisions/)
- [Threat Models](docs/threat_models/)
