# Data Connector Extraction Plan

## Problem Statement

The compilation for the `spiced` binary and the `runtime` crate is very long:
- Initial build: 20+ minutes
- Incremental rebuild: 5-7 minutes

The majority of time is spent linking the runtime crate against all dependencies. Even small changes cause the entire runtime crate (~67K lines) to rebuild.

## Solution Overview

Split data connectors into separate crates using `linkme` distributed slices for dynamic registration at link time. This allows:
1. Incremental builds to only rebuild changed connector crates
2. Feature-gated connectors to be completely excluded from compilation
3. Better parallelization of crate compilation

## End Goal Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           spiced (binary)                           │
│  Dependencies: runtime, connector-* (all optional)                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│ connector-      │       │ connector-      │       │ connector-      │
│ postgres        │       │ mysql           │       │ kafka           │
└────────┬────────┘       └────────┬────────┘       └────────┬────────┘
         │                         │                         │
         ├─────────────────────────┼─────────────────────────┤
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────────────────────────────┐
│ data_components │       │                runtime                   │
│ (lean, shared)  │       │                                         │
│                 │       │ - DataConnector trait                   │
│ - cdc types     │       │ - Registration macro                    │
│ - Read/ReadWrite│       │ - Core acceleration logic               │
│ - poly provider │       │                                         │
│ - delete logic  │       │ NO connector-specific dependencies      │
└─────────────────┘       └─────────────────────────────────────────┘
```

**Key Goals:**
1. **`runtime` has NO connector-specific dependencies** - no `runtime/postgres`, `runtime/mysql`, etc.
2. **`data_components` becomes lean** - only shared core types, no connector-specific modules
3. **Connector crates own their logic** - code from both `runtime/dataconnector/{name}.rs` AND `data_components/{name}/` moves into `connector-{name}`
4. **`spiced` features are simple** - just `postgres = ["connector-postgres"]`

## Current State: `data_components` Structure

### Core Shared Modules (Keep in `data_components`)

| Module | Purpose |
|--------|---------|
| `cdc` | ChangeBatch, ChangesStream, ChangeOperation, StreamError |
| `delete` | DeletionTableProvider, get_deletion_provider |
| `poly` | PolyTableProvider |
| `arrow` | ArrowFactory |
| `object` | Object store helpers |
| `refresh_skip` | should_skip_refresh_for_table_provider |
| `rate_limit` | Rate limiting utilities |
| `Read` trait | Table provider read trait |
| `ReadWrite` trait | Table provider read/write trait |
| `RefreshableCatalogProvider` | Catalog refresh trait |

### Connector-Specific Modules (Move to Connector Crates)

| Module | Feature | Target Crate |
|--------|---------|--------------|
| `postgres` | `postgres` | `connector-postgres` |
| `mysql` | `mysql` | `connector-mysql` |
| `clickhouse` | `clickhouse` | `connector-clickhouse` |
| `mssql` | `mssql` | `connector-mssql` |
| `snowflake` | `snowflake` | `connector-snowflake` |
| `duckdb` | `duckdb` | `connector-duckdb` |
| `mongodb` | `mongodb` | `connector-mongodb` |
| `oracle` | `oracle` | `connector-oracle` |
| `spark_connect` | `spark_connect` | `connector-spark` |
| `dynamodb` | `dynamodb` | `connector-dynamodb` |
| `scylladb` | `scylladb` | `connector-scylladb` |
| `flightsql` | `flightsql` | `connector-flightsql` |
| `databricks` | `databricks` | `connector-databricks` |
| `delta_lake` | `delta_lake` | `connector-delta-lake` |
| `sharepoint` | `sharepoint` | `connector-sharepoint` |
| `odbc` | `odbc` | `connector-odbc` |
| `imap` | `imap` | `connector-imap` |
| `kafka` | `kafka` | `connector-kafka` |
| `debezium` | `debezium` | `connector-debezium` |
| `debezium_kafka` | `debezium` | `connector-debezium` |
| `sqlite` | `sqlite` | `connector-sqlite` |
| `turso` | `turso` | `connector-turso` |
| `s3_vectors` | `s3_vectors` | TBD (embeddings-related) |
| `git` | (always) | `connector-git` |
| `github` | (always) | `connector-github` |
| `graphql` | (always) | `connector-graphql` |
| `http` | (always) | `connector-https` |
| `flight` | (always) | `connector-flightsql` or shared |
| `iceberg` | (always) | `connector-iceberg` |
| `spice_cloud` | (always) | `connector-spiceai` |
| `unity_catalog` | (always) | `connector-databricks` or shared catalog crate |

## Migration Phases

### Phase 1: Extract Connectors (Current Work)

Extract connectors to separate crates. Each connector crate temporarily depends on `data_components` with the appropriate feature.

**Status**: In progress (postgres done)

### Phase 2: Feature-Gate Error Handling in Runtime

Fix the connector-specific error handling in `accelerated_table/refresh_task/changes.rs`:

```rust
// Current (forces runtime to know about kafka)
if let cdc::StreamError::Kafka(KafkaError::...) = err { ... }

// After (only compiled when feature enabled)
#[cfg(any(feature = "kafka", feature = "debezium"))]
if let cdc::StreamError::Kafka(KafkaError::...) = err { ... }
```

### Phase 3: Minimize `data_components/{name}` Modules

Due to the orphan rule, trait implementations for external types MUST stay in `data_components`. 
However, we can minimize these modules to just the bare minimum:

```rust
// data_components/src/postgres.rs (MINIMAL - just trait impls)
#[async_trait]
impl Read for PostgresTableFactory {
    async fn table_provider(&self, table_reference: TableReference) -> ... {
        self.table_provider(table_reference).await  // delegates to inherent method
    }
}

#[async_trait]
impl DeletionTableProvider for PostgresTableWriter {
    // Deletion logic stays here
}
```

The bulk of connector logic lives in `connector-{name}` crate. The `data_components` module only contains trait implementations that can't be moved.

### Phase 4: Keep Lean `data_components` Trait Modules

The connector-specific modules in `data_components` remain but are minimal (just trait impls).
These modules still require feature flags because they depend on `datafusion-table-providers/{name}`.

**End state for `data_components`:**
- Shared types: `cdc`, `delete`, `poly`, `Read`, `ReadWrite`, etc. (no feature flags)
- Connector trait modules: `postgres`, `mysql`, etc. (feature-gated, minimal - just trait impls)

### Phase 5: Remove `runtime/{name}` Features

Update spiced features to only depend on connector crates:

```toml
# Before
postgres = ["connector-postgres", "runtime/postgres"]

# After
postgres = ["connector-postgres"]
```

Remove all connector-specific features from `runtime/Cargo.toml`.

## Completed Work

### ✅ Postgres Connector Extracted

**Location**: `crates/data-connectors/connector-postgres/`

1. Created `connector-postgres` crate with full implementation
2. Uses `runtime::register_data_connector!` macro for linkme registration
3. Removed duplicate code from `runtime/src/dataconnector/postgres.rs`
4. Updated `spiced` to depend on `connector-postgres` when postgres feature enabled
5. Made `ConnectorParams` fields public for external connector access

**Key Design Decision**: The connector calls `PostgresTableFactory` methods directly (e.g., `self.postgres_factory.table_provider(...)`) rather than going through the `Read`/`ReadWrite` traits. This avoids the orphan rule constraint.

**Note**: The `data_components/postgres` module CANNOT be fully moved into the connector crate due to the Rust orphan rule (see below). It stays in `data_components` with just the trait implementations.

### ✅ MySQL Connector Extracted

**Location**: `crates/data-connectors/connector-mysql/`

1. Created `connector-mysql` crate with full implementation
2. Uses `runtime::register_data_connector!` macro for linkme registration
3. Removed duplicate code from `runtime/src/dataconnector/mysql.rs`
4. Updated `spiced` to depend on `connector-mysql` when mysql feature enabled

### ✅ ClickHouse Connector Extracted

**Location**: `crates/data-connectors/connector-clickhouse/`

1. Created `connector-clickhouse` crate with full implementation
2. Uses `runtime::register_data_connector!` macro for linkme registration
3. Removed duplicate code from `runtime/src/dataconnector/clickhouse.rs`
4. Updated `spiced` to depend on `connector-clickhouse` when clickhouse feature enabled

**Key Difference from Postgres/MySQL**: ClickHouse's `ClickhouseTableFactory` is defined in `data_components` (not `datafusion-table-providers`), so the `Read` trait impl is in the same crate as the type. This means the connector can use the trait method directly without orphan rule issues.

### ✅ MSSQL Connector Extracted

**Location**: `crates/data-connectors/connector-mssql/`

1. Created `connector-mssql` crate with full implementation
2. Uses `runtime::register_data_connector!` macro for linkme registration
3. Removed duplicate code from `runtime/src/dataconnector/mssql.rs`
4. Updated `spiced` to depend on `connector-mssql` when mssql feature enabled

**Key Note**: MSSQL's `SqlServerTableProvider` is defined in `data_components` (not `datafusion-table-providers`), similar to ClickHouse.

### Commits Created

1. `fix: add missing feature-gate for AWS Secrets Manager error variant`
2. `refactor: make ConnectorParams fields public for external connectors`
3. `feat: extract postgres connector to separate crate`
4. `feat: extract mysql connector to separate crate`
5. `feat: extract clickhouse connector to separate crate`
6. `feat: extract mssql connector to separate crate`

## Blocking Issues

### Issue 1: Rust Orphan Rule (CRITICAL CONSTRAINT)

**Problem**: The `DeletionTableProvider`, `Read`, and `ReadWrite` traits are defined in `data_components`, but the types they're implemented for (e.g., `PostgresTableFactory`, `PostgresTableWriter`) are defined in `datafusion-table-providers`. 

Due to Rust's orphan rule, trait implementations can only exist in crates that define either:
- The trait itself, OR
- The type it's implemented for

**Consequence**: The trait implementations in `data_components/{connector}.rs` **MUST stay in `data_components`**. They cannot be moved to connector crates.

**Workaround Applied**: In connector crates, call the underlying inherent methods directly instead of using the traits:

```rust
// Instead of this (requires trait impl):
match Read::table_provider(&self.postgres_factory, path).await { ... }

// Do this (calls inherent method directly):
match self.postgres_factory.table_provider(path).await { ... }
```

**Long-term Solutions**:
1. Move traits to a separate `connector-traits` crate that `datafusion-table-providers` can depend on
2. Use newtype wrappers in connector crates (adds complexity)
3. Keep thin trait impl modules in `data_components` (current approach)

### Issue 2: Connector-Specific Error Handling in Runtime

**File**: `crates/runtime/src/accelerated_table/refresh_task/changes.rs`

Runtime has hardcoded pattern matching on Kafka and DynamoDB error types:

```rust
#[cfg(feature = "dynamodb")]
use data_components::dynamodb::stream::StreamError as DynamoDBStreamError;
#[cfg(any(feature = "debezium", feature = "kafka"))]
use data_components::kafka::{Error as KafkaError, ...};

// Usage (NOT feature-gated):
if let cdc::StreamError::Kafka(KafkaError::...) = err { ... }
if let cdc::StreamError::DynamoDB(DynamoDBStreamError::...) = err { ... }
```

**Solution**: Feature-gate the usage sites, not just the imports.

### Issue 2: `cdc::StreamError` Enum Variants

The `cdc::StreamError` enum in `data_components` has connector-specific variants:

```rust
pub enum StreamError {
    Kafka(kafka::Error),       // Requires kafka feature
    DynamoDB(dynamodb::Error), // Requires dynamodb feature
    // ...
}
```

**Solution**: Make these variants feature-gated in the enum definition.

## Remaining Connectors to Migrate

### High Priority (Feature-Gated, Heavy Dependencies)

| Connector | Feature Flag | Status | Notes |
|-----------|--------------|--------|-------|
| ✅ postgres | `postgres` | Done | |
| ✅ mysql | `mysql` | Done | Similar to postgres |
| ✅ clickhouse | `clickhouse` | Done | |
| ✅ mssql | `mssql` | Done | |
| ✅ snowflake | `snowflake` | Done | |
| ✅ duckdb | `duckdb` | Done | Heavy libduckdb dependency |
| ✅ mongodb | `mongodb` | Done | |
| ✅ oracle | `oracle` | Done | |
| ✅ spark | `spark` | Done | |
| ☐ dynamodb | `dynamodb` | | Has streaming error handling |
| ✅ scylladb | `scylladb` | Done | |

### Medium Priority (Feature-Gated, Lighter Dependencies)

| Connector | Feature Flag | Status | Notes |
|-----------|--------------|--------|-------|
| ☐ flightsql | `flightsql` | | |
| ☐ databricks | `databricks` | | |
| ☐ dremio | `dremio` | | |
| ☐ delta_lake | `delta_lake` | | |
| ☐ sharepoint | `sharepoint` | | |
| ☐ odbc | `odbc` | | |
| ☐ nfs | `nfs` | | |
| ☐ smb | `smb` | | |
| ☐ imap | `imap` | | |
| ☐ ftp | `ftp` | | Also includes sftp |

### Streaming Connectors (Require Error Handling Fix First)

| Connector | Feature Flag | Status | Notes |
|-----------|--------------|--------|-------|
| ☐ kafka | `kafka` | Blocked | Needs error handling fix |
| ☐ debezium | `debezium` | Blocked | Shares kafka error handling |

### Low Priority (Always Compiled, Light Dependencies)

| Connector | Status | Notes |
|-----------|--------|-------|
| ☐ s3 | | Object store connector |
| ☐ abfs | | Azure Blob connector |
| ☐ file | | Local file connector |
| ☐ https | | HTTP connector |
| ☐ git | | Git connector |
| ☐ github | | GitHub connector |
| ☐ graphql | | GraphQL connector |
| ☐ iceberg | | Iceberg connector |
| ☐ glue | | AWS Glue catalog |
| ☐ memory | | In-memory connector |
| ☐ localpod | | Local spicepod connector |
| ☐ spiceai | | Spice.ai cloud connector |
| ☐ deferred | | Deferred loading connector |
| ☐ sink | | Sink connector |

## Migration Pattern

### 1. Create Connector Crate

```
crates/data-connectors/connector-{name}/
├── Cargo.toml
└── src/
    └── lib.rs
```

### 2. Cargo.toml Template

```toml
[package]
name = "connector-{name}"
edition.workspace = true
version.workspace = true
rust-version.workspace = true
license.workspace = true
description = "{Name} data connector for Spice.ai runtime"

[dependencies]
async-trait.workspace = true
datafusion.workspace = true
datafusion-table-providers = { workspace = true, features = ["{name}", "{name}-federation"] }
linkme.workspace = true
paste.workspace = true
runtime = { path = "../../runtime" }
secrecy.workspace = true
snafu.workspace = true

# NOTE: data_components is NOT needed here!
# The connector calls table factory methods directly, not through Read/ReadWrite traits.
# Trait implementations stay in data_components/{name}.rs due to orphan rule.

[features]
default = []
{name}-write = []
```

### 3. Move Implementation

1. Copy `runtime/src/dataconnector/{name}.rs` to `connector-{name}/src/lib.rs`
2. Update imports from `crate::` to `runtime::`
3. Add module documentation
4. Add `register_data_connector!("{name}", {Name}Factory);` at end of file
5. Remove the module from `runtime/src/dataconnector/mod.rs`

### 4. Update spiced (Temporary)

```toml
# bin/spiced/Cargo.toml
[dependencies]
connector-{name} = { path = "../../crates/data-connectors/connector-{name}", optional = true }

[features]
# Temporary: still need runtime/{name} until Phase 5
{name} = ["connector-{name}", "runtime/{name}"]
```

### 5. Verify

```bash
cargo check -p spiced --features {name}
cargo clippy -p connector-{name}
```

## Verification Commands

```bash
# Build with all default features
cargo build -p spiced

# Build with just postgres
cargo build -p spiced --no-default-features --features postgres

# Check a specific connector crate
cargo check -p connector-postgres

# Run clippy on connector crates
cargo clippy -p connector-postgres
```
