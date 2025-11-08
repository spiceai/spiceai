# HTTP Data Connector with refresh_sql Filters

This example demonstrates how to use `refresh_sql` with filters in the HTTP data connector. When a refresh_sql query includes filters on the `path` or `query` columns, the HTTP connector will use those filters to construct the appropriate HTTP requests.

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
        SELECT path, query, content 
        FROM api_data 
        WHERE path = '/api/v1/users'
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
        SELECT path, query, content 
        FROM multi_endpoint 
        WHERE path IN ('/api/v1/users', '/api/v1/posts', '/api/v1/comments')
```

This will make three separate HTTP requests:

- `https://api.example.com/api/v1/users`
- `https://api.example.com/api/v1/posts`
- `https://api.example.com/api/v1/comments`

### Filters with Query Parameters

```yaml
datasets:
  - from: https://api.example.com
    name: paginated_data
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT path, query, content 
        FROM paginated_data 
        WHERE path = '/api/v1/users' 
          AND query IN ('page=1&limit=100', 'page=2&limit=100')
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
        SELECT path, query, content 
        FROM cross_product 
        WHERE path IN ('/api/users', '/api/posts')
          AND query IN ('status=active', 'status=inactive')
```

This creates a cross product, making four requests:

- `https://api.example.com/api/users?status=active`
- `https://api.example.com/api/users?status=inactive`
- `https://api.example.com/api/posts?status=active`
- `https://api.example.com/api/posts?status=inactive`

## Supported Filter Expressions

The HTTP connector's `refresh_sql` supports the following filter expressions on `path` and `query` columns:

1. **Equality (`=`)**: `WHERE path = '/api/users'`
2. **IN Lists**: `WHERE path IN ('/api/users', '/api/posts')`
3. **OR expressions**: `WHERE path = '/api/users' OR path = '/api/posts'`
4. **AND expressions**: `WHERE path = '/api/users' AND query = 'limit=10'`
5. **Combinations**: Complex combinations of the above

## How It Works

1. **Filter Pushdown**: When refresh_sql contains filters on `path` or `query` columns, DataFusion pushes these filters down to the HTTP table provider's `scan` method.

2. **Partition Extraction**: The `extract_partitions` method recursively analyzes the filter expressions to extract all unique (path, query) combinations.

3. **HTTP Request Construction**: For each partition, the provider constructs the appropriate URL by:
   - Appending the `path` filter value to the base URL's path
   - Adding the `query` filter value as the query string

4. **Parallel Fetching**: Multiple partitions are fetched in parallel, improving performance for multi-endpoint scenarios.

## Performance Considerations

- **Caching**: The HTTP connector respects `Cache-Control` headers and caches responses when `max-age` is set.
- **Parallel Execution**: Multiple endpoints (from IN lists or OR expressions) are fetched in parallel.
- **Filter Selectivity**: Use specific filters to minimize unnecessary HTTP requests.

## Schema

The HTTP connector provides three columns:

- `path` (String, NOT NULL): The path portion of the URL
- `query` (String, NULLABLE): The query string portion of the URL
- `content` (String, NOT NULL): The response body as text

## Notes

- If no `path` filter is provided, the base URL's path is used as-is
- If no `query` filter is provided, no query string is added
- The `file_format` parameter must be omitted or set to `json` to use the filter-based approach
- For other formats (CSV, Parquet, etc.), use the listing table connector approach
