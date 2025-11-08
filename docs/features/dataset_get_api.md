# Dataset GET API - Read-Through HTTP Proxy-Cache

## Overview

The `POST /v1/datasets/{name}/get` endpoint provides a read-through HTTP proxy-cache for datasets. It allows you to fetch data from a dataset by passing parameters to the underlying data connector, with automatic caching using Spice's LRU cache.

## Key Features

1. **Read-Through Cache**: Data is automatically cached using Spice's LRU cache on first request
2. **Stable Cache Keys**: Parameters are converted to SQL WHERE clauses, creating stable cache keys that work across both the `/get` API and SQL queries
3. **Automatic Acceleration**: If a dataset has acceleration enabled, fetched data is automatically stored in the acceleration layer
4. **Unified Caching**: Cache hits benefit both subsequent `/get` API calls and SQL queries with the same predicates

## API Endpoint

### `POST /v1/datasets/{name}/get`

Fetches data from a dataset using the data connector, with automatic caching.

**Path Parameters:**
- `name` (string, required): The name of the dataset to query

**Request Body:**
```json
{
  "params": {
    "key1": "value1",
    "key2": 123,
    "key3": true
  }
}
```

**Response:**
```json
{
  "data": [
    {"column1": "value1", "column2": 123},
    {"column1": "value2", "column2": 456}
  ],
  "cached": false,
  "cache_key_sql": "SELECT * FROM dataset_name WHERE key1 = 'value1' AND key2 = 123 AND key3 = true"
}
```

**Response Fields:**
- `data` (array): Array of objects representing the fetched rows
- `cached` (boolean): Whether the data was served from cache
- `cache_key_sql` (string, optional): The SQL query generated from params (used as cache key)

**Status Codes:**
- `200 OK`: Data retrieved successfully
- `404 Not Found`: Dataset not found
- `400 Bad Request`: Invalid parameters
- `500 Internal Server Error`: Failed to fetch data or process results

## How It Works

### 1. Parameter-to-SQL Conversion

The API converts request parameters to a SQL WHERE clause, creating a stable cache key:

**Input:**
```json
{
  "params": {
    "user_id": 123,
    "status": "active"
  }
}
```

**Generated SQL:**
```sql
SELECT * FROM users WHERE user_id = 123 AND status = 'active'
```

### 2. Cache Lookup

The generated SQL is used as a cache key. If a cached result exists:
- Data is returned immediately from cache
- `cached` field is `true` in response
- No query to the data connector is made

### 3. Cache Miss Handling

If no cached result exists:
- Query is executed against the data connector
- Results are cached for future requests
- If acceleration is enabled, data is automatically inserted into the acceleration layer
- `cached` field is `false` in response

### 4. Cross-API Cache Sharing

Because the cache key is the SQL query, cache hits work across APIs:
- `/get` API with params → SQL query cache hit
- SQL query → `/get` API cache hit
- Both benefit from the same LRU cache

## Example Usage

### Basic Request
```bash
curl -X POST http://localhost:8090/v1/datasets/users/get \
  -H "Content-Type: application/json" \
  -d '{
    "params": {
      "user_id": 123
    }
  }'
```

### Response (Cache Miss)
```json
{
  "data": [
    {"id": 123, "name": "John Doe", "status": "active"}
  ],
  "cached": false,
  "cache_key_sql": "SELECT * FROM users WHERE user_id = 123"
}
```

### Subsequent Request (Cache Hit)
Same request returns faster with `"cached": true`

### Equivalent SQL Query
```sql
SELECT * FROM users WHERE user_id = 123
```
This query will hit the same cache entry populated by the `/get` API call.

## Supported Parameter Types

- **String**: Converted to SQL string literal with proper escaping
- **Number**: Converted to SQL number literal
- **Boolean**: Converted to SQL boolean (`true`/`false`)
- **Null**: Converted to SQL `NULL`

## Implementation Details

### Location
- Endpoint handler: `crates/runtime/src/http/v1/datasets.rs`
- Route registration: `crates/runtime/src/http/routes.rs`

### Cache Integration
- Uses existing `DataFusion::query_builder()` infrastructure
- Automatic LRU cache management
- Cache key generation via SQL normalization
- Respects existing cache configuration (TTL, max size, etc.)

### Acceleration Integration
- If dataset has acceleration enabled, data flows through `AcceleratedTable`
- Refresh logic automatically handles cache population
- Supports all acceleration modes (append, full refresh, etc.)

## Benefits

1. **Reduced Latency**: Cached responses return in microseconds
2. **Lower Load**: Data connector is only queried on cache miss
3. **Unified API**: Same cache benefits both REST API and SQL queries
4. **Automatic Optimization**: Acceleration transparently improves performance
5. **Stable Keys**: Deterministic SQL generation ensures consistent caching

## Limitations

1. **Parameter Types**: Only supports basic JSON types (string, number, boolean, null)
2. **Query Complexity**: Parameters are AND-ed together (no OR, complex predicates)
3. **Projection**: Always selects all columns (`SELECT *`)
4. **Ordering**: No guaranteed order without ORDER BY in the underlying data connector

## Future Enhancements

Potential improvements:
- Support for complex filter expressions (OR, IN, LIKE, etc.)
- Column projection control
- Ordering/sorting parameters
- Limit/offset pagination
- Support for nested JSON objects as filter values
