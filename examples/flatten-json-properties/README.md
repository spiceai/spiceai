# `flatten_json_properties` — Searchable Attributes Index

Turn a dataset of JSON-Schema-shaped documents into a flat, embeddable,
vector-searchable attributes index — entirely from `spicepod.yaml`, with no
pre-processing service.

This recipe is the worked example from issue
[#10399](https://github.com/spiceai/spiceai/issues/10399).

## What you'll build

A Spicepod that:

1. Ingests a catalog of JSON-Schema documents (one row per schema).
2. Defines a view that calls `flatten_json_properties(body)` on each schema,
   producing one row per field across all schemas.
3. Accelerates that view into DuckDB for sub-second query latency.
4. Embeds the per-field `description` column so you can run vector / hybrid
   search across field descriptions.

Query results look like:

```sql
-- Find fields that mention "customer demographics" across every schema.
SELECT schema_id, path, name, description
FROM vector_search('attributes', 'customer demographics signal', 10);
```

## Files

- `spicepod.yaml` — the Spicepod manifest.
- `sample_schemas.json` — three sample JSON-Schema documents, enough to see
  the view populate without standing up an API.

## Running locally

1. Start Spice in this directory:

   ```bash
   spice run
   ```

2. In another terminal, connect to the SQL REPL:

   ```bash
   spice sql
   ```

3. Inspect the raw catalog:

   ```sql
   SELECT id, title FROM schemas;
   ```

4. Inspect the flattened attributes view:

   ```sql
   SELECT schema_id, path, name, type, description
   FROM attributes
   ORDER BY schema_id, path
   LIMIT 20;
   ```

5. Try a vector search over field descriptions:

   ```sql
   SELECT schema_id, path, name, description
   FROM vector_search('attributes', 'customer email', 5);
   ```

## How it works

### 1. Ingest the catalog

The `schemas` dataset reads `sample_schemas.json` as-is, exposing `id`,
`title`, and `body` (the raw schema document) as columns.

### 2. Flatten with `flatten_json_properties`

`flatten_json_properties(body)` walks each schema's `properties` tree and
emits one row per field with these columns:

| column        | type       | description                                                          |
| ------------- | ---------- | -------------------------------------------------------------------- |
| `path`        | Utf8       | Dotted path, e.g. `user.address.street`                              |
| `parent_path` | Utf8       | Everything but the leaf                                              |
| `name`        | Utf8       | Leaf field name                                                      |
| `description` | Utf8       | From the field's `description` annotation                            |
| `type`        | Utf8       | `string`, `integer`, `object`, `array`, `map`, `ref`, …              |
| `required`    | Boolean    | Inferred from the ancestor's `required:[...]`                        |
| `format`      | Utf8       | e.g. `date-time`, `uuid`                                             |
| `enum_values` | List<Utf8> | Present when the field declares `enum`                               |
| `metadata`    | Utf8       | Full field spec JSON — query with `json_get(metadata, '$.x-custom')` |

The function handles `items.properties` (arrays of objects),
`additionalProperties` maps, `allOf` / `oneOf` / `anyOf` merge, and local
`$ref` pointers with cycle detection. External `$ref` URIs are emitted as a
row with `type = 'ref'` and are never dereferenced.

### 3. Per-row LATERAL via UNNEST

The view uses the scalar form of `flatten_json_properties` combined with
`UNNEST`, which gives row-level evaluation:

```sql
SELECT s.id AS schema_id, a.*
FROM schemas s,
     UNNEST(flatten_json_properties(s.body)) AS a
```

The UDTF form (`FROM flatten_json_properties('{...}')`) exists for ad-hoc
testing with literal inputs.

### 4. Acceleration + embeddings

`acceleration.enabled: true` materializes the view into DuckDB on a refresh
schedule. The `description` column has an `embeddings:` block so each row's
description is embedded once and stored next to it, making vector search a
single-hop lookup.

### 5. Options

Pass named options to tune the walker:

```sql
SELECT *
FROM flatten_json_properties(
    '{"properties": {"a": {"type": "string"}}}',
    max_depth       => 16,
    max_rows        => 10000,
    include_internal => true,      -- also emit object/array/map rows
    path_style      => 'dot',
    dialect         => 'json-schema',
    expand_maps     => true,       -- emit `parent.[*].child` for maps (dot style)
    map_wildcard    => '[*]'       -- customize the wildcard segment
);
```

| option             | type | default       | notes                                                                                                                        |
| ------------------ | ---- | ------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `max_depth`        | UInt | `32`          | walk stops past this depth                                                                                                   |
| `max_rows`         | UInt | `100000`      | per-document row cap                                                                                                         |
| `max_bytes`        | UInt | `8_388_608`   | input size limit (8 MiB)                                                                                                     |
| `dialect`          | Utf8 | `json-schema` | `json-schema` \| `openapi`                                                                                                   |
| `include_internal` | Bool | `false`       | emit container rows (`object`, `array`, `map`)                                                                               |
| `path_style`       | Utf8 | `dot`         | `dot` (`a.b.c`) \| `json-pointer` (`/a/b/c`)                                                                                 |
| `expand_maps`      | Bool | `false`       | walk through `additionalProperties` and encode the map's dynamic key as a wildcard segment in the path (JSONPath convention) |
| `map_wildcard`     | Utf8 | `[*]`         | wildcard segment inserted when `expand_maps = true`; must be non-empty                                                       |

#### Expanding maps (`Map<String, Array<Object>>` example)

By default, `flatten_json_properties` collapses `additionalProperties` onto
the parent path: a map field `labels` with value schema
`{properties: {value: string}}` emits a single leaf at `labels.value`. That's
the right answer for shallow `Map<String, Scalar>` shapes, but it hides
structure when the map's value is itself a complex type.

Turn on `expand_maps` to preserve the indirection as a wildcard segment in
the path. Given a `Map<String, Array<Object>>` like an `identityMap`:

```json
{
  "properties": {
    "identityMap": {
      "type": "object",
      "additionalProperties": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "authenticatedState": {"type": "string"},
            "id":                 {"type": "string"},
            "primary":            {"type": "boolean"}
          }
        }
      }
    }
  }
}
```

```sql
SELECT path, type
FROM flatten_json_properties(:schema, expand_maps => true)
ORDER BY path;
```

| path                                 | type    |
| ------------------------------------ | ------- |
| `identityMap.[*].authenticatedState` | string  |
| `identityMap.[*].id`                 | string  |
| `identityMap.[*].primary`            | boolean |

Arrays still collapse `items.properties.*` onto the parent path, so a
map-of-array-of-object surfaces as one wildcard segment between the map
name and the inner leaves — matching JSONPath-style addressing.

### 6. `json_tree` — generic alternative

If your input isn't JSON-Schema-shaped, reach for `json_tree`. It's a
schema-agnostic recursive walker with DuckDB/SQLite-compatible output (cols
`key`, `value`, `type`, `atom`, `id`, `parent`, `fullkey`, `path`):

```sql
SELECT key, type, atom, fullkey
FROM json_tree('{"a": [1, 2], "b": {"c": "hi"}}');
```

## Telemetry

The walker emits the following OpenTelemetry counters, scraped by any
configured metrics exporter:

- `flatten_json_properties_invocations_total{dialect}`
- `flatten_json_properties_rows_emitted_total`
- `flatten_json_properties_errors_total{kind}` where
  `kind ∈ {parse, depth_exceeded, row_cap_hit, cycle, input_too_large}`
- `json_tree_invocations_total` / `json_tree_rows_emitted_total` /
  `json_tree_errors_total{kind}`

## See also

- Issue: https://github.com/spiceai/spiceai/issues/10399
- PR: https://github.com/spiceai/spiceai/pull/10406
