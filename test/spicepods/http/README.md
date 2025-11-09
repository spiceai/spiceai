# HTTP Data Connector Integration Tests

This directory contains integration tests for the HTTP data connector using various public APIs and static files.

## Test Spicepods

### basic.yaml (httpbin.org)

Basic HTTP connector functionality tests using httpbin.org - a simple HTTP request & response service:

#### 1. httpbin_json - Basic GET Request

- **Endpoint**: `https://httpbin.org/json`
- **Expected Behavior**: Returns a JSON object
- **Filter**: `request_path = '/json'`

#### 2. httpbin_get - GET with Query Parameters

- **Endpoint**: `https://httpbin.org/get?param1=value1&param2=value2`
- **Expected Behavior**: Echoes back query parameters in response
- **Filter**: `request_path = '/get' AND request_query = 'param1=value1&param2=value2'`

#### 3. httpbin_multiple - Multiple Endpoints with OR

- **Endpoints**: `/json` OR `/uuid`
- **Expected Behavior**: Results from both endpoints
- **Filter**: `request_path = '/json' OR request_path = '/uuid'`

#### 4. httpbin_status - Multiple Status Codes with IN

- **Endpoints**: `/status/200`, `/status/201`
- **Expected Behavior**: Empty responses with different status codes
- **Filter**: `request_path IN ('/status/200', '/status/201')`

### tvmaze.yaml (TVMaze API)

A comprehensive test spicepod with multiple datasets demonstrating different HTTP connector patterns using the TVMaze API (<https://api.tvmaze.com>), which is free and doesn't require authentication.

#### 1. show_details - Single JSON Object

- **Endpoint**: `https://api.tvmaze.com/shows/169`
- **Returns**: Single JSON object with Breaking Bad show details
- **Expected Behavior**: One row in the result
- **Filter**: `request_path = '/shows/169'`

#### 2. people_search - JSON Array (Multiple Objects)

- **Endpoint**: `https://api.tvmaze.com/search/people?q=michael`
- **Returns**: JSON array with multiple people search results
- **Expected Behavior**: Each array element becomes a separate row in the result
- **Filter**: `request_path = '/search/people' AND request_query = 'q=michael'`

#### 3. combined_data - OR Filter Across Different Endpoints

- **Endpoints**:
  - `https://api.tvmaze.com/search/people?q=michael` (array)
  - `https://api.tvmaze.com/shows/169` (single object)
- **Expected Behavior**: Multiple rows from the search results + one row from the show details
- **Filter**: Combined OR expression with different path/query combinations

#### 4. multiple_shows - IN List with Multiple Paths

- **Endpoints**: Multiple show IDs (169, 170, 171)
- **Returns**: Three show detail objects
- **Expected Behavior**: Three rows, one for each show
- **Filter**: `request_path IN ('/shows/169', '/shows/170', '/shows/171')`

### static_files.yaml

Tests for accessing static files directly (CSV, JSON):

#### 1. iris_csv - Direct CSV File Access

- **URL**: https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv
- **Format**: CSV with explicit `file_format` parameter
- **Expected Behavior**: CSV is parsed and accessible via SQL (150 rows, 5 columns)

#### 2. json_array - Direct JSON Array File Access

- **URL**: Public JSON array file
- **Format**: JSON with explicit `file_format` parameter
- **Expected Behavior**: JSON array is parsed and accessible

#### 3. tips_csv - Another CSV Example

- **URL**: https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv
- **Format**: CSV with explicit `file_format` parameter
- **Expected Behavior**: CSV is parsed (244 rows, restaurant tips data)

### post_requests.yaml

Tests for HTTP POST requests with `request_body` filter and custom content-types:

#### 1. create_post - POST with JSON Body

- **Endpoint**: JSONPlaceholder API for creating posts
- **Content-Type**: `application/json` (explicitly set)
- **Filter**: `request_body = '{"title": "foo", "body": "bar", "userId": 1}'`
- **Expected Behavior**: POST request sends JSON data

#### 2. post_form - POST with Form-Encoded Data

- **Endpoint**: httpbin.org/post
- **Content-Type**: `application/x-www-form-urlencoded` (custom)
- **Filter**: `request_body = 'key1=value1&key2=value2'`
- **Expected Behavior**: POST request sends form data

#### 3. post_json_default - POST with Default Content-Type

- **Endpoint**: httpbin.org/post
- **Content-Type**: Default (`application/json`)
- **Filter**: `request_body = '{"test": "data"}'`
- **Expected Behavior**: POST request defaults to JSON content-type

#### 4. multiple_posts - Multiple POST Requests with IN Clause

- **Endpoint**: JSONPlaceholder API
- **Filter**: `request_body IN (...)`
- **Expected Behavior**: Multiple POST requests, one for each body value

## Query Test Files

- **basic_queries.yaml**: Tests for basic.yaml datasets (httpbin.org)
- **queries.yaml**: Tests for tvmaze.yaml datasets
- **static_files_queries.yaml**: Tests for static_files.yaml datasets
- **post_requests_queries.yaml**: Tests for post_requests.yaml datasets

## Running the Tests

### Manual Testing

Start spiced with a test spicepod:

```bash
cd test/spicepods/http

# Test basic HTTP functionality
spiced -p basic.yaml

# Test TVMaze API
spiced -p tvmaze.yaml

# Test static files
spiced -p static_files.yaml

# Test POST requests
spiced -p post_requests.yaml
```

Then query the data:

```sql
-- Single object test
SELECT * FROM show_details;

-- Multi-object test (array expansion)
SELECT * FROM people_search;

-- Combined OR filter test
SELECT request_path, request_query, COUNT(*) as row_count
FROM combined_data
GROUP BY request_path, request_query;

-- IN list test
SELECT request_path, content FROM multiple_shows;
```

### Automated Testing with testoperator

Run tests with specific query sets:

```bash
# Test basic HTTP functionality
cargo run -p testoperator -- run \
  -p test/spicepods/http/basic.yaml \
  -s spiced \
  --query-set test/spicepods/http/basic_queries.yaml

# Test TVMaze datasets
cargo run -p testoperator -- run \
  -p test/spicepods/http/tvmaze.yaml \
  -s spiced \
  --query-set test/spicepods/http/queries.yaml

# Test static file access
cargo run -p testoperator -- run \
  -p test/spicepods/http/static_files.yaml \
  -s spiced \
  --query-set test/spicepods/http/static_files_queries.yaml

# Test POST requests
cargo run -p testoperator -- run \
  -p test/spicepods/http/post_requests.yaml \
  -s spiced \
  --query-set test/spicepods/http/post_requests_queries.yaml

# Run all HTTP tests
for pod in basic tvmaze static_files post_requests; do
  cargo run -p testoperator -- run \
    -p test/spicepods/http/${pod}.yaml \
    -s spiced \
    --query-set test/spicepods/http/${pod/_/_queries}.yaml
done
```

### Unit and Integration Tests

Run the HTTP provider integration tests:

```bash
# Run all HTTP provider tests
cargo test -p data_components http_provider

# Run specific test
cargo test -p data_components test_http_post_with_jsonrequest_body

# Run with output
cargo test -p data_components http_provider -- --nocapture
```

The integration tests in `crates/data_components/tests/http_provider_test.rs` cover:

- **Basic HTTP GET requests** with path and query parameters
- **JSON array expansion** (multiple rows from single response)
- **POST requests** with `request_body` filter
- **Custom content-types** for POST requests
- **OR and IN expressions** for multiple endpoints/bodies
- **Retry logic** for transient failures
- **Base URL with path components**

## Test Coverage

| Scenario                            | Spicepod              | Unit Test | Integration Test                               |
| ----------------------------------- | --------------------- | --------- | ---------------------------------------------- |
| Single JSON object (GET)            | ✅ tvmaze.yaml        | ✅        | ✅ test_tvmaze_single_object                   |
| JSON array expansion (GET)          | ✅ tvmaze.yaml        | ✅        | ✅ test_tvmaze_multi_object                    |
| OR filter across endpoints          | ✅ tvmaze.yaml        | ✅        | ✅ test_tvmaze_combined_or_filter              |
| IN list with multiple paths         | ✅ tvmaze.yaml        | ✅        | ✅ test_tvmaze_in_listrequest_paths            |
| POST with JSON body                 | ✅ post_requests.yaml | ✅        | ✅ test_http_post_with_jsonrequest_body        |
| POST with custom content-type       | ✅ post_requests.yaml | -         | ✅ test_http_post_with_custom_content_type     |
| POST with IN list (multiple bodies) | ✅ post_requests.yaml | -         | ✅ test_http_post_multiple_bodies              |
| Direct CSV file access              | ✅ static_files.yaml  | -         | -                                              |
| Direct JSON file access             | ✅ static_files.yaml  | -         | -                                              |
| Direct Parquet file access          | ✅ static_files.yaml  | -         | -                                              |
| Query parameters                    | -                     | ✅        | ✅ test_http_provider_withrequest_query_params |
| Base URL with path                  | -                     | ✅        | ✅ test_http_provider_with_baserequest_path    |

## Expected Results

### show_details (Single Object)

Should return exactly 1 row with the Breaking Bad show details:

```text
request_path        | request_query | content
-------------|--------|------------------------------------------------
/shows/169   |        | {"id":169,"name":"Breaking Bad","type":"Scr..."}
```

### Combined Test (tvmaze_data)

Should return all results from both endpoints:

```sql
SELECT request_path, request_query, COUNT(*) as rows
FROM tvmaze_data
GROUP BY request_path, request_query;
```

Expected output:

```text
/shows/169   |        | {"id":169,"name":"Breaking Bad","type":"Scr..."}
```

### people_search (Array Expansion)

Should return multiple rows (10+ results), each representing a person matching "michael":

```text
request_path              | request_query      | content
-------------------|-------------|--------------------------------------------------
/search/people     | q=michael   | {"score":13.0,"person":{"id":1,"name":"Mike..."}}
/search/people     | q=michael   | {"score":12.5,"person":{"id":2,"name":"Mich..."}}
...
```

### combined_data (OR Filter)

Should return multiple rows from both endpoints:

```text
request_path              | request_query      | rows
-------------------|-------------|-----
/search/people     | q=michael   | 10+
/shows/169         |             | 1
```

### multiple_shows (IN List)

Should return exactly 3 rows, one for each show:

```text
request_path        | content
-------------|------------------------------------------------
/shows/169   | {"id":169,"name":"Breaking Bad"...}
/shows/170   | {"id":170,"name":"The Borgias"...}
/shows/171   | {"id":171,"name":"Suits"...}
```

## Notes

- These tests use a public API that doesn't require authentication
- The TVMaze API is free and doesn't have strict rate limits for reasonable use
- Tests demonstrate core HTTP connector features:
  - Filter pushdown on `request_path` and `request_query` columns
  - JSON array expansion (multi-object)
  - Single JSON object handling
  - Combined OR filters for multiple endpoints
  - IN list filters for multiple paths
  - Acceleration with `refresh_sql`
