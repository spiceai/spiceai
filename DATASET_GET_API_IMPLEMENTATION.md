# Dataset GET API Implementation

## Summary

Implemented a new HTTP API endpoint `POST /v1/datasets/{name}/get` that acts as a read-through HTTP proxy-cache for datasets, utilizing Spice's LRU cache and optionally inserting fetched results into acceleration.

## What Was Built

### 1. New HTTP Endpoint
- **Path**: `POST /v1/datasets/{name}/get`
- **Purpose**: Fetch data from a dataset using parameters passed to the data connector
- **Location**: `crates/runtime/src/http/v1/datasets.rs` (new functions added)

### 2. Key Components

#### Request/Response Types
```rust
pub struct DatasetGetRequest {
    pub params: HashMap<String, serde_json::Value>,
}

pub struct DatasetGetResponse {
    pub data: Vec<HashMap<String, serde_json::Value>>,
    pub cached: bool,
    pub cache_key_sql: Option<String>,
}
```

#### SQL Generation from Parameters
- Converts request parameters to SQL WHERE clauses
- Creates stable cache keys that work across APIs
- Example: `{"user_id": 123}` → `SELECT * FROM table WHERE user_id = 123`

#### Arrow to JSON Conversion
- Converts Arrow RecordBatch results to JSON
- Supports common data types (int, float, string, boolean)
- Handles null values properly

### 3. Cache Integration

The implementation uses the existing DataFusion query infrastructure:
1. Parameters → SQL WHERE clause
2. SQL query executed through `DataFusion::query_builder()`
3. Results automatically cached via existing LRU cache
4. Cache key is the SQL query (stable across APIs)

**Key Benefit**: Cache hits work across both:
- `/v1/datasets/{name}/get` API calls
- Direct SQL queries with same predicates

### 4. Acceleration Integration

When a dataset has acceleration enabled:
- Data flows through the existing `AcceleratedTable` infrastructure
- Fetched data is automatically inserted into acceleration
- Subsequent queries (via API or SQL) benefit from acceleration

## How It Works

```
User Request → Generate SQL from params → Query DataFusion
                                          ↓
                                    Check Cache
                                    /         \
                              Cache Hit    Cache Miss
                                  ↓            ↓
                            Return Cached   Query Connector
                                            ↓
                                      Cache Result
                                            ↓
                                   Insert to Acceleration (if enabled)
                                            ↓
                                      Return Result
```

## Files Modified

1. **`crates/runtime/src/http/v1/datasets.rs`**
   - Added `DatasetGetRequest` and `DatasetGetResponse` structs
   - Added `dataset_get()` handler function
   - Added `generate_sql_from_params()` helper
   - Added `record_batches_to_json()` conversion helper
   - Added `arrow_value_to_json()` type conversion helper

2. **`crates/runtime/src/http/routes.rs`**
   - Registered new route: `.route("/v1/datasets/{name}/get", post(v1::datasets::dataset_get))`
   - Added to OpenAPI paths for documentation

3. **`docs/features/dataset_get_api.md`** (new)
   - Complete documentation of the feature
   - Usage examples
   - Implementation details

## Usage Example

```bash
# Request data with parameters
curl -X POST http://localhost:8090/v1/datasets/users/get \
  -H "Content-Type: application/json" \
  -d '{
    "params": {
      "user_id": 123,
      "status": "active"
    }
  }'

# Response (first call - cache miss)
{
  "data": [
    {"id": 123, "name": "John", "status": "active"}
  ],
  "cached": false,
  "cache_key_sql": "SELECT * FROM users WHERE user_id = 123 AND status = 'active'"
}

# Second call with same params returns instantly from cache
# "cached": true
```

## Design Decisions

### 1. Why SQL Generation?
- **Stable Cache Keys**: SQL queries provide deterministic, normalized cache keys
- **Cross-API Caching**: Same cache entry works for REST API and SQL queries
- **Existing Infrastructure**: Leverages DataFusion's query planning and caching

### 2. Why Use DataFusion Query Builder?
- **Automatic Caching**: Inherits LRU cache behavior
- **Acceleration Integration**: Automatically uses accelerated tables
- **Query Optimization**: Benefits from DataFusion's query optimizer
- **Consistency**: Same code path as SQL endpoint

### 3. Why JSON Response Format?
- **HTTP-Friendly**: Easy to consume from web clients
- **Type-Safe**: Preserves data types (numbers, strings, booleans)
- **Portable**: Works across all programming languages

## Testing

The implementation can be tested with:

```bash
# 1. Start spiced with a test dataset
# 2. Make initial request (cache miss)
curl -X POST http://localhost:8090/v1/datasets/test_data/get \
  -H "Content-Type: application/json" \
  -d '{"params": {"id": 1}}'

# 3. Make same request again (should be cache hit)
# 4. Verify "cached": true in response

# 5. Query with SQL (should also hit cache)
curl -X POST http://localhost:8090/v1/sql \
  -H "Content-Type: text/plain" \
  -d 'SELECT * FROM test_data WHERE id = 1'
```

## Future Enhancements

Potential improvements for future PRs:
1. Support complex filter expressions (OR, IN, LIKE, range queries)
2. Add column projection control (specify which columns to return)
3. Add ordering/sorting parameters
4. Add limit/offset pagination support
5. Support nested filter structures for complex queries
6. Add metrics for cache hit rates per dataset

## Code Quality

- ✅ Follows Spice.ai coding standards (no `.unwrap()`, uses `?` operator)
- ✅ Uses `tracing::` for logging (not `log::`)
- ✅ Proper error handling with context
- ✅ Zero-copy where possible with Arrow arrays
- ✅ Async/await patterns (no blocking operations)
- ✅ Compiles with no errors or warnings specific to this change
- ✅ OpenAPI documentation included
