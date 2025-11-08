# HTTP Data Connector with refresh_sql Filters

This example demonstrates how to use `refresh_sql` with filters in the HTTP data connector. When a refresh_sql query includes filters on the `_path`, `_query`, or `_body` columns, the HTTP connector will use those filters to construct the appropriate HTTP requests.

## Basic Example

### Spicepod Configuration

```yaml
datasets:
  - from: https://api.example.com
    name: api_data
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT _path, _query, content 
        FROM api_data 
        WHERE _path = '/api/v1/users'
```

In this example, when the dataset refreshes, it will fetch data from `https://api.example.com/api/v1/users`.

## Advanced Examples

### Multiple Paths with IN List

```yaml
datasets:
  - from: https://api.example.com
    name: multi_endpoint
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT _path, _query, content 
        FROM multi_endpoint 
        WHERE _path IN ('/api/v1/users', '/api/v1/posts', '/api/v1/comments')
```

This will make three separate HTTP requests:

- `https://api.example.com/api/v1/users`
- `https://api.example.com/api/v1/posts`
- `https://api.example.com/api/v1/comments`

The number of result rows depends on the response content:

- If each endpoint returns a **JSON array**, each array element becomes a separate row
- If each endpoint returns a **single JSON object**, you get one row per endpoint (3 rows total)
- If `/api/v1/users` returns 10 users, `/api/v1/posts` returns 5 posts, and `/api/v1/comments` returns 3 comments, you get 18 total rows

### Filters with Query Parameters

```yaml
datasets:
  - from: https://api.example.com
    name: paginated_data
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT _path, _query, content 
        FROM paginated_data 
        WHERE _path = '/api/v1/users' 
          AND _query IN ('page=1&limit=100', 'page=2&limit=100')
```

This will make two requests:

- `https://api.example.com/api/v1/users?page=1&limit=100`
- `https://api.example.com/api/v1/users?page=2&limit=100`

### Combined Filters (Cross Product)

```yaml
datasets:
  - from: https://api.example.com
    name: cross_product
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT _path, _query, content 
        FROM cross_product 
        WHERE _path IN ('/api/users', '/api/posts')
          AND _query IN ('status=active', 'status=inactive')
```

This creates a cross product, making four requests:

- `https://api.example.com/api/users?status=active`
- `https://api.example.com/api/users?status=inactive`
- `https://api.example.com/api/posts?status=active`
- `https://api.example.com/api/posts?status=inactive`

## Supported Filter Expressions

The HTTP connector's `refresh_sql` supports the following filter expressions on `_path`, `_query`, and `_body` columns:

1. **Equality (`=`)**: `WHERE _path = '/api/users'`
2. **IN Lists**: `WHERE _path IN ('/api/users', '/api/posts')`
3. **OR expressions**: `WHERE _path = '/api/users' OR _path = '/api/posts'`
4. **AND expressions**: `WHERE _path = '/api/users' AND _query = 'limit=10'`
5. **POST requests**: `WHERE _body = '{"key": "value"}'` (triggers POST with `http_post_content_type`)
6. **Combinations**: Complex combinations of the above

## How It Works

1. **Filter Pushdown**: When refresh_sql contains filters on `_path`, `_query`, or `_body` columns, DataFusion pushes these filters down to the HTTP table provider's `scan` method.

2. **Partition Extraction**: The `extract_partitions` method recursively analyzes the filter expressions to extract all unique `(_path, _query, _body)` combinations.

3. **HTTP Request Construction**: For each partition, the provider constructs the appropriate HTTP request by:
   - Appending the `_path` filter value to the base URL's path
   - Adding the `_query` filter value as the query string
   - Using POST method with `_body` content if `_body` filter is present

4. **Content Parsing**: Response content is parsed based on format:
   - **JSON arrays**: Each element becomes a separate row
   - **NDJSON (newline-delimited JSON)**: Each line becomes a separate row
   - **Single JSON object**: One row
   - **Other formats**: One row with the entire content

5. **Parallel Fetching**: Multiple partitions are fetched in parallel, improving performance for multi-endpoint scenarios.

## Performance Considerations

- **Caching**: The HTTP connector respects `Cache-Control` headers and caches responses when `max-age` is set.
- **Parallel Execution**: Multiple endpoints (from IN lists or OR expressions) are fetched in parallel.
- **Filter Selectivity**: Use specific filters to minimize unnecessary HTTP requests.

## Schema

The HTTP connector provides four metadata columns:

- `_path` (String, NOT NULL): The path portion of the URL used for this row's request
- `_query` (String, NOT NULL): The query string portion of the URL (empty string if none)
- `_body` (String, NOT NULL): The request body for POST requests (empty string if none)
- `content` (String, NOT NULL): The parsed content from the response

**Row Expansion**: When a response contains a JSON array or newline-delimited JSON (NDJSON), each item becomes a separate row with the same `_path`, `_query`, and `_body` values but different `content`.

## Notes

- If no `_path` filter is provided, the base URL's path is used as-is
- If no `_query` filter is provided, no query string is added
- If no `_body` filter is provided, GET method is used
- When `_body` filter is present, POST method is used with `http_post_content_type` parameter (default: `application/json`)
- The `file_format` parameter must be omitted or set to `json` or `auto` to use the filter-based approach
- For other formats (CSV, Parquet, etc.), use the listing table connector approach
- Use `http_max_retries` parameter to configure retry attempts (default: 3)
