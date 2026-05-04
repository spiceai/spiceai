# HTTP Data Connector with refresh_sql Filters

This example demonstrates how to use `refresh_sql` with filters in the HTTP data connector. When a refresh_sql query includes filters on the `request_path`, `request_query`, `request_body`, or `request_headers` columns, the HTTP connector will use those filters to construct the appropriate HTTP requests.

## Basic Example

### Spicepod Configuration

```yaml
runtime:
  params:
    http_max_concurrent_requests: 8
    http_requests_per_second_limit: 10
    http_requests_per_minute_limit: 300

datasets:
  - from: https://api.example.com
    name: api_data
    params:
      max_concurrent_requests: 4
      requests_per_second_limit: 2
      requests_per_minute_limit: 60
      rate_control_jitter_min: 2ms
      rate_control_jitter_max: 8ms
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT request_path, request_query, content 
        FROM api_data 
        WHERE request_path = '/api/v1/users'
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
        SELECT request_path, request_query, content 
        FROM multi_endpoint 
        WHERE request_path IN ('/api/v1/users', '/api/v1/posts', '/api/v1/comments')
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
        SELECT request_path, request_query, content 
        FROM paginated_data 
        WHERE request_path = '/api/v1/users' 
          AND request_query IN ('page=1&limit=100', 'page=2&limit=100')
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
        SELECT request_path, request_query, content 
        FROM cross_product 
        WHERE request_path IN ('/api/users', '/api/posts')
          AND request_query IN ('status=active', 'status=inactive')
```

This creates a cross product, making four requests:

- `https://api.example.com/api/users?status=active`
- `https://api.example.com/api/users?status=inactive`
- `https://api.example.com/api/posts?status=active`
- `https://api.example.com/api/posts?status=inactive`

### Dynamic Request Headers

```yaml
datasets:
  - from: https://api.example.com
    name: sandbox_data
    params:
      request_header_filters: enabled
      request_header_allowlist: x-sandbox-id
      max_request_partitions: 10000
    acceleration:
      enabled: true
      refresh_mode: full
      refresh_sql: |
        SELECT request_headers, content
        FROM sandbox_data
        WHERE request_headers IN (
          '{"x-sandbox-id":"sandbox-1"}',
          '{"x-sandbox-id":"sandbox-2"}'
        )
```

This makes one request per `request_headers` value, setting the allowed header values on each request before combining the results.

## Supported Filter Expressions

The HTTP connector's `refresh_sql` supports the following filter expressions on `request_path`, `request_query`, `request_body`, and `request_headers` columns:

1. **Equality (`=`)**: `WHERE request_path = '/api/users'`
2. **IN Lists**: `WHERE request_path IN ('/api/users', '/api/posts')`
3. **OR expressions** (single column only): `WHERE request_path = '/api/users' OR request_path = '/api/posts'`. OR across **different** filter columns is not supported — use separate queries (e.g. `UNION ALL`).
4. **AND expressions**: `WHERE request_path = '/api/users' AND request_query = 'limit=10'`
5. **POST requests**: `WHERE request_body = '{"key": "value"}'` (triggers POST with `http_post_content_type`)
6. **Dynamic headers**: `WHERE request_headers IN ('{"x-sandbox-id":"sandbox-1"}', '{"x-sandbox-id":"sandbox-2"}')`
7. **Combinations**: Complex combinations of the above

## How It Works

1. **Filter Pushdown**: When refresh_sql contains filters on `request_path`, `request_query`, `request_body`, or `request_headers` columns, DataFusion pushes these filters down to the HTTP table provider's `scan` method.

2. **Partition Extraction**: The `extract_partitions` method recursively analyzes the filter expressions to extract all unique `(request_path, request_query, request_body, request_headers)` combinations.

3. **HTTP Request Construction**: For each partition, the provider constructs the appropriate HTTP request by:
   - Appending the `request_path` filter value to the base URL's path
   - Adding the `request_query` filter value as the query string
   - Using POST method with `request_body` content if `request_body` filter is present
   - Parsing `request_headers` as a JSON object and setting the allowlisted headers on the request

4. **Content Parsing**: Response content is parsed based on format:
   - **JSON arrays**: Each element becomes a separate row
   - **NDJSON (newline-delimited JSON)**: Each line becomes a separate row
   - **Single JSON object**: One row
   - **Other formats**: One row with the entire content

5. **Parallel Fetching**: Multiple partitions are fetched in parallel, improving performance for multi-endpoint scenarios.

## Performance Considerations

- **Caching**: The HTTP connector respects `Cache-Control` headers and caches responses when `max-age` is set.
- **Parallel Execution**: Multiple endpoints (from IN lists or single-column OR expressions) are fetched in parallel.
- **Filter Selectivity**: Use specific filters to minimize unnecessary HTTP requests.
- **Partition Limits**: Use `max_request_partitions` to cap the number of HTTP requests created from cross-product filters.

## Schema

The HTTP connector provides metadata columns:

- `request_path` (String, NOT NULL): The path portion of the URL used for this row's request
- `request_query` (String, NULL): The query string portion of the URL. When no query string is provided, the current provider emits an empty string rather than SQL `NULL`.
- `request_body` (String, NULL): The request body for POST requests. When no request body is provided, the current provider emits an empty string rather than SQL `NULL`.
- `request_headers` (String, NULL): The JSON request headers object used for this row's request. When no request-specific headers are provided, the current provider emits an empty string rather than SQL `NULL`.
- `content` (String, NOT NULL): The parsed content from the response

**Row Expansion**: When a response contains a JSON array or newline-delimited JSON (NDJSON), each item becomes a separate row with the same `request_path`, `request_query`, `request_body`, and `request_headers` values but different `content`.

## Notes

- If no `request_path` filter is provided, the base URL's path is used as-is
- If no `request_query` filter is provided, no query string is added
- If no `request_body` filter is provided, GET method is used
- If no `request_headers` filter is provided, only the static `http_headers` configuration is used
- When `request_body` filter is present, POST method is used with `http_post_content_type` parameter (default: `application/json`)
- When `request_headers` filter is present, `request_header_filters` must be `enabled`, and every header name must be listed in `request_header_allowlist`
- The `file_format` parameter must be omitted or set to `json` or `auto` to use the filter-based approach
- For other formats (CSV, Parquet, etc.), use the listing table connector approach
- Use `max_retries` parameter to configure retry attempts (default: 3)
- Use `retry_backoff_method` parameter to configure retry strategy: 'fibonacci' (default), 'linear', or 'exponential'
- Use `retry_max_duration` parameter to limit the total time spent retrying (e.g., '30s', '5m')
- Use `retry_jitter` parameter to add randomization to retry delays (0.0 to 1.0, default: 0.3)
- Use `runtime.params.http_max_concurrent_requests` to set a default concurrent request limit for HTTP-based connectors; use dataset `max_concurrent_requests` to override it for a specific dataset/origin
- Limits are shared by upstream origin (`scheme://host:port`), so five datasets targeting the same API with a limit of `5` share five permits rather than each getting five
- Use `runtime.params.http_requests_per_second_limit` and `runtime.params.http_requests_per_minute_limit` for default request-rate budgets; use dataset `requests_per_second_limit` and `requests_per_minute_limit` for per-dataset/origin overrides
- Use `runtime.params.http_rate_control_jitter_min` and `runtime.params.http_rate_control_jitter_max` for default jitter controls; use dataset `rate_control_jitter_min` and `rate_control_jitter_max` to override them. Set both to `0ms` to disable rate-control jitter
- HTTP rate-control parameters apply to dynamic JSON HTTP API datasets and HTTP-family connectors such as GraphQL. Structured HTTP file datasets that route through the listing connector (`csv`, `parquet`, `arrow`, `avro`, `jsonl`, `ndjson`, and similar formats) currently reject these parameters; omit the runtime defaults for those sources or use a dynamic JSON HTTP API dataset
- HTTP 429 responses and rate-limit cooldown headers are honored automatically and shared by HTTP datasets with the same origin. Supported cooldown hints include `Retry-After`, `retry-after-ms`, `x-retry-after-ms`, and exhausted-quota reset headers such as `RateLimit-Remaining: 0` with `RateLimit-Reset` or common `X-RateLimit-Reset` variants

## Rate-Control Metrics

HTTP rate-control metrics are auto-registered and available through `/metrics`, `runtime.metrics`, and OTLP exporters with the dataset `name` attribute. HTTP connector datasets use `dataset_http_{metric_name}` and GraphQL connector datasets use `dataset_graphql_{metric_name}`. Because rate-control state is shared by upstream origin, each origin is emitted once using the first successfully initialized dataset that claims metrics for that origin to avoid double-counting shared counters. They can be disabled individually in the owning dataset `metrics` section with `enabled: false`.

| Metric                                    | Type    | Description                                                                |
| ----------------------------------------- | ------- | -------------------------------------------------------------------------- |
| `inflight_operations`                     | Gauge   | Current HTTP requests holding a rate-control permit                        |
| `rate_control_max_concurrent_requests`    | Gauge   | Configured concurrency limit; `0` means disabled                           |
| `rate_control_requests_per_second_limit`  | Gauge   | Configured requests-per-second limit; `0` means disabled                   |
| `rate_control_requests_per_minute_limit`  | Gauge   | Configured requests-per-minute limit; `0` means disabled                   |
| `rate_control_jitter_min_ms`              | Gauge   | Configured minimum request jitter in milliseconds                          |
| `rate_control_jitter_max_ms`              | Gauge   | Configured maximum request jitter in milliseconds                          |
| `rate_control_available_permits`          | Gauge   | Current available concurrency permits                                      |
| `rate_control_acquisitions_total`         | Counter | Total rate-control permits acquired                                        |
| `rate_control_acquire_errors_total`       | Counter | Total rate-control permit acquisition errors                               |
| `rate_control_wait_duration_ms`           | Counter | Cumulative time spent waiting for rate-control permits, quotas, and jitter |
| `rate_limit_retry_after_updates_total`    | Counter | Total upstream cooldown hints accepted from `Retry-After` or reset headers |
| `rate_limit_retry_after_waits_total`      | Counter | Total waits caused by upstream cooldown hints                              |
| `rate_limit_retry_after_wait_duration_ms` | Counter | Cumulative time spent waiting because of upstream cooldown hints           |
| `rate_limit_retry_after_remaining_ms`     | Gauge   | Current remaining upstream cooldown                                        |
