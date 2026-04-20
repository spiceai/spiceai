# Schema Decomposition (`json_object`)

Schema decomposition lets you reshape a source that has many — or highly
nested — fields into a small, stable set of top-level columns plus a
single catch-all JSON column. Declared columns are projected as their own
top-level fields; every other key on each row is collected into a sorted
JSON object and stored (as a string) in the catch-all column.

This is useful when:

- The source emits wide or deeply nested records but only a few fields are
  needed as discrete columns (partition/sort keys, titles, IDs, …).
- You want the remaining fields preserved losslessly for downstream SQL
  keyword search, vector/semantic search, or lineage — without having to
  enumerate every nested path.
- The upstream schema evolves and you don't want to redeploy the Spicepod
  every time a new optional field appears.

Schema decomposition is supported by the following data connectors:

- [DynamoDB](#dynamodb)
- [HTTP](#http)

## Semantics

- Declare your desired columns under `columns:` on the dataset.
- Mark **exactly one** column with `metadata.json_object: "*"`. That
  column becomes the catch-all; its type is `STRING` containing a JSON
  object.
- Every other declared column is a static top-level field. Its value is
  taken verbatim from the source record when present; missing keys are
  `NULL`.
- All remaining keys from each source row are gathered into the catch-all
  column as a JSON object with keys sorted alphabetically (stable and
  diff-friendly).
- Only `"*"` is supported as the marker value today. Multiple marked
  columns, or any other value, is rejected at load time with an
  actionable configuration error.

> **Data correctness.** No data from the source row is silently dropped:
> every key lands either in a declared static column or in the catch-all
> JSON. Declared static columns that are absent from a given row are
> surfaced as SQL `NULL` rather than omitted.

## DynamoDB

The [DynamoDB Data Connector](https://spiceai.org/docs/components/data-connectors/dynamodb)
infers its Arrow schema from a sample of the table. For wide items where
only the partition/sort keys (and perhaps a few attributes) are needed as
discrete columns, use schema decomposition to consolidate everything else
into one JSON column.

### Example

```yaml
datasets:
  - from: dynamodb:my_table
    name: my_table
    columns:
      - name: PK
      - name: SK
      - name: data_json
        metadata:
          json_object: '*' # Captures all other attributes as JSON
```

Given a DynamoDB table with attributes `PK`, `SK`, `name`, `email`, and
`status`, the resulting table is:

| PK   | SK     | data_json                                                        |
| ---- | ------ | ---------------------------------------------------------------- |
| pk_1 | sort_1 | `{"email":"alice@example.com","name":"Alice","status":"active"}` |
| pk_2 | sort_2 | `{"email":"bob@example.com","name":"Bob","status":"inactive"}`   |

### Behavior

- Every declared static column (`PK`, `SK` above) must exist in the
  inferred DynamoDB schema. If a declared column is missing, the
  connector fails fast with `Columns not found in table schema: …`;
  increase `schema_infer_max_records` or fix the column name.
- The catch-all JSON is built from the original DynamoDB `AttributeValue`
  types: strings stay strings, numbers become JSON numbers, maps/lists
  are nested JSON objects/arrays, binary values are base64-encoded.
- Decomposition is applied before Arrow conversion, so the accelerator
  (if any) sees only the decomposed schema.

## HTTP

The [HTTP Data Connector](https://spiceai.org/docs/components/data-connectors/https)
returns a fixed schema by default (`request_path`, `request_query`,
`request_body`, `content`, `response_status`, `response_headers`,
`fetched_at`) where the raw response body sits in `content`. When the
endpoint returns JSON rows (either a top-level array, NDJSON, or a single
object) you can instead **replace** that schema with your own set of
columns using schema decomposition.

### Example — TVmaze shows API

[TVmaze](https://www.tvmaze.com/api) is a free, unauthenticated TV
metadata API that returns JSON objects with ~20 fields per show —
including deeply nested `schedule`, `rating`, `network`, `externals`,
`image`, and `_links` objects. It's a great real-world fit for schema
decomposition: you typically only want a handful of columns as first-
class fields (id, name, language, premiered, status) and the rest kept
verbatim for downstream search and analysis.

#### Minimal decomposition

```yaml
datasets:
  - from: https://api.tvmaze.com/shows
    name: tvmaze_shows
    params:
      pagination_query_params: 'page={page}'
      pagination_page_size: 250 # TVmaze returns 250 shows/page
    columns:
      - name: id
      - name: name
      - name: language
      - name: premiered
      - name: status
      - name: details
        metadata:
          json_object: '*' # Everything else (schedule, rating, network, …)
```

Each row becomes:

| id  | name                 | language | premiered  | status | details                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| --- | -------------------- | -------- | ---------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `Under the Dome`     | English  | 2013-06-24 | Ended  | `{"_links":{…},"averageRuntime":60,"externals":{…},"genres":["Drama","Science-Fiction","Thriller"],"image":{…},"network":{"country":{…},"id":2,"name":"CBS",…},"officialSite":"http://…","premiered":null,"rating":{"average":6.5},"runtime":60,"schedule":{"days":["Thursday"],"time":"22:00"},"summary":"<p>…</p>","type":"Scripted","updated":1704794065,"url":"https://www.tvmaze.com/shows/1/under-the-dome","webChannel":null,"weight":98}` |
| 2   | `Person of Interest` | English  | 2011-09-22 | Ended  | `{…}`                                                                                                                                                                                                                                                                                                                                                                                                                                             |

Notes:

- The TVmaze `/shows` endpoint is a top-level JSON array; the HTTP
  connector automatically emits one row per array element.
- Pagination uses TVmaze's `?page=N` convention — when a page returns
  fewer than `pagination_page_size` rows, the connector stops.
- Every nested field (`rating`, `network`, `image`, `_links`, …) is
  preserved inside `details` as sorted JSON, so it can be projected at
  query time with standard JSON SQL functions.

#### Querying the decomposed dataset

Once loaded, the static columns behave like normal columns and the
catch-all is queryable with JSON functions from your accelerator (shown
here with DuckDB):

```sql
-- Top English-language shows by TVmaze rating
SELECT
  id,
  name,
  premiered,
  CAST(json_extract(details, '$.rating.average') AS DOUBLE) AS rating,
  json_extract_string(details, '$.network.name')             AS network
FROM tvmaze_shows
WHERE language = 'English'
  AND status = 'Running'
ORDER BY rating DESC NULLS LAST
LIMIT 10;

-- Flatten genres into one row per (show, genre) for search / lineage
SELECT
  s.id,
  s.name,
  genre.value AS genre
FROM tvmaze_shows s,
     LATERAL UNNEST(
       CAST(json_extract(s.details, '$.genres') AS VARCHAR[])
     ) AS genre;
```

#### Accelerating with embeddings

Accelerate the decomposed dataset and attach an embedding to the
catch-all column so semantic search works over every nested field
without enumerating them:

```yaml
datasets:
  - from: https://api.tvmaze.com/shows
    name: tvmaze_shows
    params:
      pagination_query_params: 'page={page}'
      pagination_page_size: 250
    acceleration:
      enabled: true
      engine: duckdb
      mode: file
      refresh_mode: full
      refresh_check_interval: 24h
    columns:
      - name: id
      - name: name
      - name: language
      - name: premiered
      - name: status
      - name: details
        embeddings:
          - from: text_embedder # defined under `embeddings:` at the root
        metadata:
          json_object: '*'
```

### Example — single-show endpoint

TVmaze `/shows/{id}` returns a single JSON object. Decomposition works
the same way — the row count is just 1 per request:

```yaml
datasets:
  - from: https://api.tvmaze.com
    name: tvmaze_show
    params:
      allowed_request_paths: '/shows/*' # enable path filter pushdown
    columns:
      - name: id
      - name: name
      - name: premiered
      - name: details
        metadata:
          json_object: '*'
```

```sql
SELECT id, name, premiered,
       json_extract_string(details, '$.network.name') AS network
FROM tvmaze_show
WHERE request_path = '/shows/1';
```

> **Note.** `request_path` is not present in the decomposed schema, so
> this query shape requires the decomposition-less default HTTP schema.
> To combine path filtering with decomposition, point `from:` directly at
> the specific resource URL (e.g. `https://api.tvmaze.com/shows/1`) per
> dataset.

### Behavior

- When at least one `columns:` entry has `metadata.json_object: "*"`, the
  HTTP provider swaps its default fixed schema for one built from
  `columns:` in declaration order. All decomposed columns are `Utf8`
  (nullable).
- Each row from the response is decomposed:
  - JSON arrays are flattened to one row per element (same as default
    HTTP connector behavior).
  - Paginated endpoints are supported — decomposition is applied per
    page using the existing `pagination_*` parameters (including
    `pagination_data_pointer`).
  - For JSON object rows, declared static columns are populated from
    matching top-level keys; missing keys are `NULL`; all other keys go
    into the catch-all as a sorted JSON object.
  - Non-object rows (bare arrays/primitives) are preserved verbatim in
    the catch-all; all declared static columns are `NULL`.
- Static column values come directly from JSON:
  - JSON string → the string (unquoted)
  - JSON null → SQL `NULL`
  - JSON number / boolean / array / object → JSON text
- The `content`, `request_path`, `response_status`, … metadata columns
  are **not** available when decomposition is enabled. If you need them,
  don't enable decomposition — use a view on top of the default schema
  instead.

## Building a normalized attributes view

Once the source is decomposed into `(id, …, catch_all_json)`, you can
normalize it into a flattened attribute list with a view. For example,
to expose every `(show_id, json_path, value)` triple from TVmaze for
full-text / lineage tracking, use your accelerator's JSON functions
(`json_extract`, `UNNEST`, recursive CTEs) over the `details` column —
no custom UDF required.

If you accelerate the decomposed dataset (e.g. with DuckDB), embeddings
declared on the accelerated view work as usual — both the raw catch-all
JSON and any derived attributes view can participate in vector or
full-text search.

## Troubleshooting

| Symptom                                                                 | Cause / fix                                                                                                                                        |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Multiple columns have 'json_object' metadata defined: …`               | Only one column may be marked. Remove the extra `json_object: "*"` entries.                                                                        |
| `Column 'X' has invalid 'json_object' value: …. Only '*' is supported.` | Change the marker to the string `"*"`. Other patterns/selectors aren't supported yet.                                                              |
| `Columns not found in table schema: …` (DynamoDB)                       | A declared static column doesn't exist in the sampled DynamoDB items. Fix the name or raise `schema_infer_max_records`.                            |
| HTTP `content`/`response_status` columns are missing                    | Expected — decomposition replaces the default HTTP schema. Remove the `json_object` marker, or project those fields from a non-decomposed dataset. |
| Catch-all column is `NULL`                                              | The row had no keys outside the declared static columns. This is correct behavior.                                                                 |
