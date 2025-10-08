# Blocking Tokio Operations Audit - Fixes Applied

## Summary

This document describes the blocking operations that were found and fixed in the Rust codebase to prevent blocking the Tokio async runtime.

## Issues Found and Fixed

### 1. ✅ FIXED: `crates/ns_lookup/src/lib.rs`

**Issue**: Using `std::net::TcpStream::connect_timeout` which blocks the thread in an async function.

**Location**: Line 89 in `verify_ns_lookup_and_tcp_connect`

**Fix Applied**:

- Replaced `std::net::TcpStream` with `tokio::net::TcpStream`
- Replaced `connect_timeout` with `tokio::time::timeout` wrapper around async `connect`
- Added proper error handling for both connection errors and timeouts

**Before**:

```rust
if TcpStream::connect_timeout(&addr, Duration::from_secs(30)).is_ok() {
    return Ok(());
}
```

**After**:

```rust
match timeout(Duration::from_secs(30), TcpStream::connect(addr)).await {
    Ok(Ok(stream)) => {
        drop(stream);
        return Ok(());
    }
    Ok(Err(err)) => {
        tracing::debug!("Failed to connect to {addr}: {err}");
    }
    Err(_) => {
        tracing::debug!("Failed to connect to {addr}, connection timed out");
    }
}
```

### 2. ✅ FIXED: `crates/runtime-object-store/src/store/sftp.rs`

**Issue**: Multiple blocking I/O operations in async ObjectStore trait methods.

**Locations**:

- `get_opts` method (lines 145-220)
- `list` method (lines 237-285)

**Problems**:

- `TcpStream::connect_timeout` (blocking network I/O)
- SSH handshake operations (blocking)
- SFTP file reads (blocking I/O)
- Directory listings (blocking I/O)

**Fix Applied**:

- Wrapped all blocking operations in `tokio::task::spawn_blocking`
- Restructured `get_opts` to perform initial file metadata operations in a blocking task
- Modified stream generation to perform file reads in blocking tasks
- Restructured `list` to perform directory traversal entirely in a blocking task

**Impact**: These changes prevent blocking the Tokio runtime when accessing SFTP resources.

### 3. ✅ FIXED: `crates/event_stream/src/lib.rs`

**Issue**: Using `std::sync::RwLock` in a tracing layer that can be called from async contexts.

**Locations**: Throughout the `EventStreamStore` implementation

**Fix Applied**:

- Replaced `std::sync::RwLock` with `tokio::sync::RwLock`
- Changed `read()` and `write()` calls to `blocking_read()` and `blocking_write()`
- Removed complex error types for PoisonError since `tokio::sync::RwLock` doesn't poison
- Updated all lock acquisitions to use the blocking variants (appropriate for tracing callbacks)

**Rationale**:

- The tracing layer callbacks (`on_new_span`, `on_event`, `on_close`) are synchronous
- Using `blocking_read()`/`blocking_write()` is correct here as these are non-async contexts
- `tokio::sync::RwLock` is the correct choice because the data structure is accessed from both async and sync contexts

### 4. ✅ VERIFIED CORRECT: `crates/runtime/src/tracers.rs`

**Status**: No changes needed - existing code is correct.

**Locations**: Lines 46, 78, 103, 128 - `std::sync::Mutex` in macro expansions

**Analysis**:

- These mutexes are held for very short durations (single HashMap lookup/insert)
- No async operations are performed while holding the lock
- No `.await` points while lock is held
- This is explicitly allowed by Tokio documentation for very short critical sections
- Poison error handling is already implemented correctly

**Tokio Guidance**: "The std::sync primitives may be used in situations where the lock is not held across an .await point."

### 5. ✅ VERIFIED CORRECT: `crates/llms/src/embeddings/mod.rs`

**Status**: No changes needed - existing code is correct.

**Location**: Line 188 - `task::block_in_place` usage

**Analysis**:

- `embed_sync` function intentionally provides a sync interface to an async function
- Uses `tokio::task::block_in_place` which is the correct pattern
- `block_in_place` moves the current task to a blocking thread for the duration of the operation
- This is the recommended approach for calling async code from sync contexts

### 6. ✅ VERIFIED CORRECT: `crates/db_connection_pool/src/dbconnection/odbcconn.rs`

**Status**: No changes needed - existing code is correct.

**Location**: Lines 192-194

**Analysis**:

- Uses `tokio::task::spawn_blocking` for ODBC operations
- The `Handle::current().block_on` inside `spawn_blocking` is acceptable
- This is running in a dedicated blocking thread pool, not the async runtime

### 7. ✅ FIXED: `crates/spicepod/src/reader.rs`

**Issue**: Using `std::fs::File::open` (blocking I/O) in async trait method.

**Location**: `StdFileSystem::open` method (lines 65-75)

**Fix Applied**:

- Wrapped `std::fs::File::open` call in `tokio::task::spawn_blocking`
- Maintains async trait signature while preventing thread blocking

**Before**:

```rust
async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
    let file = std::fs::File::open(&path)
        .map_err(|e| format!("Failed to open file {}: {e}", path.display()))?;
    Ok(Box::new(file))
}
```

**After**:

```rust
async fn open(&self, path: PathBuf) -> Result<Box<dyn io::Read + Send + Sync>> {
    let path_clone = path.clone();
    let file = tokio::task::spawn_blocking(move || {
        std::fs::File::open(&path_clone)
    })
    .await
    .map_err(|e| format!("Failed to spawn blocking task: {e}"))?
    .map_err(|e| format!("Failed to open file {}: {e}", path.display()))?;
    Ok(Box::new(file))
}
```

### 8. ✅ FIXED: `crates/flightrepl/src/lib.rs`

**Issue**: Using `std::fs::read` (blocking I/O) in async `run` function.

**Location**: Line 220 in TLS certificate loading

**Fix Applied**:

- Wrapped `std::fs::read` call in `tokio::task::spawn_blocking`

**Before**:

```rust
let tls_root_certificate = std::fs::read(&tls_root_certificate_file).map_err(|e| {
    format!("Failed to read TLS root certificate from '{tls_root_certificate_file}': {e}...")
})?;
```

**After**:

```rust
let tls_root_certificate = tokio::task::spawn_blocking({
    let path = tls_root_certificate_file.clone();
    move || std::fs::read(&path)
})
.await
.map_err(|e| format!("Failed to spawn blocking task: {e}"))?
.map_err(|e| {
    format!("Failed to read TLS root certificate from '{tls_root_certificate_file}': {e}...")
})?;
```

### 9. ✅ FIXED: `crates/test-framework/src/spicetest/append/sources/file.rs`

**Issue**: Multiple blocking operations in async trait methods for test infrastructure.

**Locations**:

- `setup` method: `std::fs::exists`, `std::fs::remove_file`, `Connection::open` (DuckDB)
- `generate` method: `Connection::open` (DuckDB), `execute_batch` (blocking SQL)

**Fix Applied**:

- Wrapped file system operations in `tokio::task::spawn_blocking`
- Wrapped entire DuckDB connection and execution blocks in `spawn_blocking`
- Cloned necessary data before moving into blocking tasks

**Impact**: Even though this is test framework code, it's still executed on Tokio runtime and needs proper async handling.

## Testing Recommendations

1. **NS Lookup**: Test connection verification to various hosts
2. **SFTP**: Test file reading, listing, and error conditions
3. **Event Stream**: Test tracing event propagation in async contexts
4. **Integration**: Run existing test suites to ensure no regressions

## Additional Patterns Checked (No Issues Found)

- ❌ `std::thread::sleep` - None found in async contexts
- ❌ `std::sync::mpsc` blocking channels - None found in async contexts
- ❌ Blocking file I/O - Checked, all appropriate uses are in spawn_blocking
- ❌ `crossbeam` blocking operations - None problematic found
- ❌ `.wait()` on futures/threads - Checked, all correct

## Performance Impact

**Positive Impacts**:

1. Eliminates runtime thread starvation from blocking operations
2. Improves overall async task scheduling
3. Better resource utilization under load

**Neutral Impacts**:

1. SFTP operations now use thread pool, but were already blocking so no regression
2. Event stream uses async-aware locking, minimal overhead

## References

- Example PR: https://github.com/datafusion-contrib/datafusion-table-providers/pull/450/files
- Tokio Mutex Documentation: https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html
- Tokio spawn_blocking: https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html
