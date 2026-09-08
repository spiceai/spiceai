# HTTP Data Connector Integration Tests

This directory contains integration tests for the HTTP data connector using various public APIs and static files.

## Running Integration Tests

The HTTP connector uses the built-in integration test suite:

```bash
cargo run -p testoperator -- run bench \
  -p ./test/spicepods/http/basic.yaml \
  -s spiced \
  --query-set scenario \
  --scenario-query-file test/scenario/http/basic.yaml
```

Integration test queries are defined in `crates/test-framework/src/queries/integration_http/` following the TPC-H pattern.

## Test Spicepods

### basic.yaml

Primary spicepod for integration tests using httpbin.org:

- **httpbin_json**: Basic GET request returning JSON object
- **httpbin_get**: GET with query parameters
- **httpbin_status_200/201**: Multiple status code endpoints

Designed to work with the `integration[http]` query set.

### static_files.yaml

Tests direct file access (CSV, JSON, Parquet):

- **iris_csv**: CSV file access (150 rows, 5 columns)
- **json_array**: JSON array file parsing
- **tips_csv**: Restaurant tips dataset (244 rows)

### post_requests.yaml

Tests HTTP POST request functionality:

- **create_post**: POST with JSON body
- **post_form**: POST with form-encoded data
- **post_json_default**: POST with default content-type
- **multiple_posts**: Multiple POST requests with IN clause

## Manual Testing

Start spiced with a test spicepod:

```bash
cd test/spicepods/http
spiced basic.yaml
```

Then query the data:

```sql
SELECT * FROM httpbin_json;
SELECT * FROM httpbin_get;
```

## Unit and Integration Tests

Run the HTTP provider integration tests:

```bash
# Run all HTTP provider tests
cargo test -p data_components http_provider

# Run with output
cargo test -p data_components http_provider -- --nocapture
```

The integration tests in `crates/data_components/tests/http_provider_test.rs` cover:

- Basic HTTP GET requests with path and query parameters
- JSON array expansion (multiple rows from single response)
- POST requests with custom content-types
- OR and IN expressions for multiple endpoints
- Retry logic for transient failures

## Test Coverage

| Feature                   | Spicepod              | Unit Test | Integration Query Set |
| ------------------------- | --------------------- | --------- | --------------------- |
| Single JSON object (GET)  | ✅ basic.yaml         | ✅        | ✅ integration[http]  |
| GET with query parameters | ✅ basic.yaml         | ✅        | ✅ integration[http]  |
| POST with JSON body       | ✅ post_requests.yaml | ✅        | -                     |
| POST with custom type     | ✅ post_requests.yaml | ✅        | -                     |
| Direct CSV file access    | ✅ static_files.yaml  | -         | -                     |
| Direct JSON file access   | ✅ static_files.yaml  | -         | -                     |
