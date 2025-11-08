# Scenario Query Sets for testoperator

The testoperator now supports scenario query sets in addition to the standard benchmarks (TPC-H, TPC-DS, ClickBench).

## Overview

Scenario query sets allow you to define your own SQL queries with optional validation in a YAML file. This is useful for:

- Testing connector-specific functionality
- Creating custom benchmarks for your data
- Validating query results against expected outputs
- Running integration tests with real APIs

## Query File Format

```yaml
name: my_scenario_queries  # Optional name for the query set
queries:
  - name: query_1
    sql: SELECT * FROM my_table WHERE id = 1
    expected_results:
      row_count: 1
  
  - name: query_2
    sql: SELECT id, name FROM users ORDER BY id
    expected_results:
      columns: "id, name"
      rows:
        - "1, Alice"
        - "2, Bob"
  
  - name: query_3
    sql: SELECT COUNT(*) FROM large_table
    expected_results: results/query_3.csv
```

## Expected Results Formats

### 1. Row Count Only

Use when you only care about the number of rows returned:

```yaml
expected_results:
  row_count: 100
```

### 2. Structured Data (Recommended)

Define columns and rows as comma-delimited strings:

```yaml
expected_results:
  columns: "id, name, age"
  rows:
    - "1, Alice, 30"
    - "2, Bob, 25"
    - "3, Charlie, "  # Empty value for NULL
```

Type inference is automatic based on CSV parsing.

### 3. Inline CSV String (Legacy)

Embed CSV data as a multi-line string:

```yaml
expected_results: |
  id,name,age
  1,Alice,30
  2,Bob,25
```

### 4. External CSV File

Reference a CSV file (path relative to the query file):

```yaml
expected_results: results/expected_output.csv
```

## Running Scenario Query Sets

### Basic Usage

```bash
cargo run -p testoperator -- run bench \
  --spicepod test/spicepods/http/tvmaze.yaml \
  --spiced-bin spiced \
  --query-set scenario \
  --scenario-query-file test/spicepods/http/queries.yaml \
  --validate
```

### With Throughput Testing

```bash
cargo run -p testoperator -- run throughput \
  --spicepod test/spicepods/http/tvmaze.yaml \
  --spiced-bin spiced \
  --query-set scenario \
  --scenario-query-file test/spicepods/http/queries.yaml \
  --concurrency 10
```

### With Load Testing

```bash
cargo run -p testoperator -- run load \
  --spicepod test/spicepods/http/tvmaze.yaml \
  --spiced-bin spiced \
  --query-set scenario \
  --scenario-query-file test/spicepods/http/queries.yaml \
  --test-hours 1
```

## Example: HTTP Connector Tests

See `queries.yaml` in this directory for a complete example of testing the HTTP connector with the TVMaze API.

The query set includes:

1. Single JSON object retrieval
2. JSON array expansion validation
3. Combined OR filters across endpoints
4. IN list with multiple paths
5. Content structure verification with json_get
6. Row count validation

## Validation

When `--validate` is used:

- **Row count validation**: Compares actual vs expected row counts
- **Data validation**: Compares actual vs expected data values
- **Schema validation**: Ensures column types match (with some flexibility for type equivalence)

### Validation Results

- ✅ **Pass**: Query returned expected results
- ❌ **Fail**: One of the following:
  - No expected results defined
  - No results returned
  - Schema mismatch
  - Row count mismatch
  - Data value mismatch

## Tips

1. **Start simple**: Begin with row_count validation, then add data validation
2. **Use ORDER BY**: For data validation, ensure results are deterministic with ORDER BY clauses
3. **Relative paths**: CSV file paths are resolved relative to the query YAML file location
4. **Column order matters**: Ensure row values match column order in the comma-delimited columns string
5. **NULL handling**: Leave value empty in CSV row (e.g., `"1, Alice, "` for NULL age)
6. **Type inference**: Data types are inferred automatically via CSV parsing
7. **Structured format**: Prefer the structured `columns`/`rows` format over inline CSV strings for better readability

## Limitations

- Scenario query sets don't support parameterized queries yet (unlike TPC-H parameterized)
- Schema equivalence is lenient (e.g., Int32 ≈ Int64) but not all type conversions are supported
- Large expected results should use external CSV files rather than inline data
