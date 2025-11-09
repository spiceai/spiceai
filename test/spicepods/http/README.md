# HTTP Data Connector Integration Tests

This directory contains integration tests for the HTTP data connector using the TVMaze API (<https://api.tvmaze.com>), which is a free, public API that doesn't require authentication.

## Test Spicepod

### tvmaze.yaml

A comprehensive test spicepod with multiple datasets demonstrating different HTTP connector patterns:

#### 1. show_details - Single JSON Object

- **Endpoint**: `https://api.tvmaze.com/shows/169`
- **Returns**: Single JSON object with Breaking Bad show details
- **Expected Behavior**: One row in the result
- **Filter**: `_path = '/shows/169'`

#### 2. people_search - JSON Array (Multiple Objects)

- **Endpoint**: `https://api.tvmaze.com/search/people?q=michael`
- **Returns**: JSON array with multiple people search results
- **Expected Behavior**: Each array element becomes a separate row in the result
- **Filter**: `_path = '/search/people' AND _query = 'q=michael'`

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
- **Filter**: `_path IN ('/shows/169', '/shows/170', '/shows/171')`

## Running the Tests

### Manual Testing

Start spiced with the test spicepod:

```bash
cd test/spicepods/http
spiced --http 0.0.0.0:50051 --flight 0.0.0.0:50052 -p tvmaze.yaml
```

Then query the data:

```sql
-- Single object test
SELECT * FROM show_details;

-- Multi-object test (array expansion)
SELECT * FROM people_search;

-- Combined OR filter test
SELECT _path, _query, COUNT(*) as row_count
FROM combined_data
GROUP BY _path, _query;

-- IN list test
SELECT _path, content FROM multiple_shows;
```

### Automated Testing with testoperator

```bash
# Test all datasets
cargo run -p testoperator -- run \
  -p test/spicepods/http/tvmaze.yaml \
  -s spiced
```

## Expected Results

### show_details (Single Object)

Should return exactly 1 row with the Breaking Bad show details:

```text
_path        | _query | content
-------------|--------|------------------------------------------------
/shows/169   |        | {"id":169,"name":"Breaking Bad","type":"Scr..."}
```

### Combined Test (tvmaze_data)

Should return all results from both endpoints:

```sql
SELECT _path, _query, COUNT(*) as rows
FROM tvmaze_data
GROUP BY _path, _query;
```

Expected output:

```text
/shows/169   |        | {"id":169,"name":"Breaking Bad","type":"Scr..."}
```

### people_search (Array Expansion)

Should return multiple rows (10+ results), each representing a person matching "michael":

```text
_path              | _query      | content
-------------------|-------------|--------------------------------------------------
/search/people     | q=michael   | {"score":13.0,"person":{"id":1,"name":"Mike..."}}
/search/people     | q=michael   | {"score":12.5,"person":{"id":2,"name":"Mich..."}}
...
```

### combined_data (OR Filter)

Should return multiple rows from both endpoints:

```text
_path              | _query      | rows
-------------------|-------------|-----
/search/people     | q=michael   | 10+
/shows/169         |             | 1
```

### multiple_shows (IN List)

Should return exactly 3 rows, one for each show:

```text
_path        | content
-------------|------------------------------------------------
/shows/169   | {"id":169,"name":"Breaking Bad"...}
/shows/170   | {"id":170,"name":"The Borgias"...}
/shows/171   | {"id":171,"name":"Suits"...}
```

## Notes

- These tests use a public API that doesn't require authentication
- The TVMaze API is free and doesn't have strict rate limits for reasonable use
- Tests demonstrate core HTTP connector features:
  - Filter pushdown on `_path` and `_query` columns
  - JSON array expansion (multi-object)
  - Single JSON object handling
  - Combined OR filters for multiple endpoints
  - IN list filters for multiple paths
  - Acceleration with `refresh_sql`
