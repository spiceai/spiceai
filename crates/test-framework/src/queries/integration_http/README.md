# HTTP Connector Integration Tests

This directory contains the integration test queries for the HTTP connector, following the same pattern as TPC-H and TPC-DS.

## Test Queries

The queries test various HTTP connector features:

1. **q1_single_object.sql** - Tests fetching a single JSON object from an API endpoint
2. **q2_multi_object.sql** - Tests JSON array expansion (one request → multiple rows)
3. **q3_combined_endpoints.sql** - Tests combining data from multiple endpoints with OR filters
4. **q4_multiple_ids.sql** - Tests IN clause with multiple endpoint paths
5. **q5_verify_structure.sql** - Tests JSON field extraction and structure validation
6. **q6_count_all.sql** - Tests simple aggregation over fetched data

## Running Tests

### Using testoperator

```bash
# Run all HTTP integration tests
cargo run -p testoperator -- run bench \
  -p ./test/spicepods/http/tvmaze.yaml \
  -s spiced \
  --query-set integration[http]

# With validation (when expected results are defined)
cargo run -p testoperator -- run bench \
  -p ./test/spicepods/http/tvmaze.yaml \
  -s spiced \
  --query-set integration[http] \
  --validate
```

## Test Data Source

The tests use the TVMaze API (https://api.tvmaze.com) which provides:

- Show information (Breaking Bad, etc.)
- People search functionality
- Free, no authentication required
- Stable test data

## Adding New Tests

To add a new HTTP connector test:

1. Create a new SQL file: `q{N}_{description}.sql`
2. Add the query to `mod.rs` in the `get_queries()` function
3. Update the spicepod in `test/spicepods/http/tvmaze.yaml` if needed
4. Add expected results in `validation/integration_http/q{N}.csv` (optional)

Follow the TPC-H pattern for consistency.
