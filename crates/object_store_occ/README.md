# object_store_occ

A Rust library for storing and retrieving serializable structs in object storage with **Optimistic Concurrency Control (OCC)** using conditional writes (ETags/If-Match headers). This enables linearizable writes for multiple writers without locks or coordination services.

## Features

- **Type-safe object storage**: Store any `Serialize + Deserialize` Rust struct as JSON in object stores
- **Optimistic concurrency control**: Uses ETags and conditional writes to detect concurrent modifications
- **Conflict detection**: Clear API for handling insert vs update vs conflict scenarios
- **Local caching**: Optional in-memory cache with refresh capabilities
- **Zero external state**: No database or coordination service required—uses object store as source of truth

## Quick Start

```rust
use object_store_occ::{ObjectState, WriteResult};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Transaction {
    foo: String,
    bar: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create object store (S3, GCS, Azure, local filesystem, etc.)
    let store = AmazonS3Builder::from_env()
        .with_bucket_name("my-state-bucket")
        .build()?;

    // Create typed object state manager with a key prefix
    let state: ObjectState<Transaction> = ObjectState::new(Arc::new(store))
        .with_prefix("v1/transactions/");

    // Insert or update with automatic conflict detection
    let tx = Transaction { foo: "hello".into(), bar: 67 };
    
    match state.insert_or_update("customer_42", &tx).await? {
        WriteResult::Inserted => println!("Created new record"),
        WriteResult::Updated => println!("Successfully updated"),
        WriteResult::Conflict { current } => {
            // Another writer modified the object - decide how to resolve
            if current.bar >= tx.bar {
                println!("Remote version is newer, skipping update");
            } else {
                // Retry with merged state...
            }
        }
    }

    // Read directly from object store (always fresh)
    let current: Option<Transaction> = state.get("customer_42").await?;

    // Read from local cache (fast, may be stale)
    let cached: Option<&Transaction> = state.get_cached("customer_42");

    // Refresh cache from object store
    state.refresh().await?;

    Ok(())
}
```

## API Reference

### `ObjectState<T>`

The main entry point for typed object storage with OCC.

#### Construction

```rust
// Basic construction
let state: ObjectState<T> = ObjectState::new(store);

// With key prefix (recommended for namespacing)
let state = ObjectState::new(store).with_prefix("v1/mytype/");
```

#### Writing

```rust
/// Insert a new object. Fails if key already exists.
async fn insert(&self, key: &str, value: &T) -> Result<InsertResult>;

/// Update an existing object with OCC. Fails if key doesn't exist.
async fn update(&self, key: &str, value: &T) -> Result<UpdateResult<T>>;

/// Insert a new object or update existing with OCC (convenience wrapper)
async fn insert_or_update(&self, key: &str, value: &T) -> Result<WriteResult<T>>;
```

#### Reading

```rust
/// Get object from directly from object store (fresh read)
async fn get(&self, key: &str) -> Result<Option<T>>;

/// Get object from local cache (fast, may be stale). 
fn get_cached(&self, key: &str) -> Option<&T>;

/// List all keys with the configured prefix
async fn list_keys(&self) -> Result<Vec<String>>;

/// Refresh local cache from object store. Retrieves all keys, not just those currently cached. 
async fn refresh(&self) -> Result<()>;
```

### `WriteResult<T>`

Result of an `insert_or_update()` operation:

```rust
pub enum WriteResult<T> {
    /// Object was newly created
    Inserted,
    
    /// Object was successfully updated (ETag matched)
    Updated,
    
    /// Concurrent modification detected - contains current remote value
    Conflict { current: T },
}
```

### `InsertResult`

Result of an `insert()` operation:

```rust
pub enum InsertResult {
    /// Object was created
    Ok,
    
    /// Object already exists
    AlreadyExists,
}
```

### `UpdateResult<T>`

Result of an `update()` operation:

```rust
pub enum UpdateResult<T> {
    /// Update succeeded (ETag matched)
    Ok,
    
    /// Object doesn't exist
    NotFound,
    
    /// Concurrent modification - contains current value
    Conflict { current: T },
}
```

## Concurrency Control

This crate uses **optimistic concurrency control** via HTTP conditional requests:

1. **On read**: Store the ETag (entity tag) from the response
2. **On write**: Send `If-Match: <etag>` header with the stored ETag
3. **On conflict**: Object store returns `412 Precondition Failed`

This provides **linearizable writes** without locks or coordination services.

### Supported Object Stores

| Store | OCC Support | Notes |
|-------|-------------|-------|
| Amazon S3 | ✅ Full | Native conditional writes |
| Google Cloud Storage | ✅ Full | Generation-based versioning |
| Azure Blob Storage | ✅ Full | ETag-based |
| Local Filesystem | ⚠️ Limited | Uses file locking (best-effort) |

## Design Principles

1. **Correctness over performance**: OCC ensures data integrity even under concurrent access
2. **No hidden state**: All state lives in the object store; cache is explicitly managed
3. **Explicit conflict handling**: Conflicts surface to the caller for domain-specific resolution
4. **Zero coordination**: No locks, leases, or external services required
