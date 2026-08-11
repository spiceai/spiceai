# Elasticsearch DataFusion Filter Pushdown

Translates Apache DataFusion SQL filter expressions into Elasticsearch query DSL clauses so that
predicates evaluate inside Elasticsearch (in a non-scoring `bool.filter` context) instead of being
fetched and re-filtered in DataFusion.

## Design

Two entry points are two views over a single internal translation pass, so they never disagree:

- `classify_filter(schema, expr) -> TableProviderFilterPushDown` — for a `TableProvider`'s
  `supports_filters_pushdown`.
- `translate_filter(schema, expr) -> Option<serde_json::Value>` — the emitted Elasticsearch
  clause for the scan.

## Exactness invariant (correctness-critical)

- **Exact** — the emitted clause matches the SQL predicate exactly under Elasticsearch semantics;
  DataFusion drops the predicate above the scan.
- **Inexact** — the clause matches a *superset* of the predicate; DataFusion keeps the predicate
  above the scan and re-checks every returned row.
- The clause is **never** a subset — that would silently drop matching rows.

When a predicate cannot be proven exact, the pass downgrades to Inexact or Unsupported, never a
wrong Exact.

## Predicate → Elasticsearch DSL

| SQL predicate | Elasticsearch clause | Exactness |
|---|---|---|
| `col = v` (integer/boolean/keyword) | `term` | Exact |
| `col = v` (float / analyzed `text`) | `term` (on `.keyword` for text) | Inexact |
| `col IN (..)` | `terms` | Exact/Inexact (as `=`) |
| `col < v`, `<=`, `>`, `>=` (integer) | `range` | Exact |
| `col < v`, .. (float / keyword) | `range` | Inexact |
| `col BETWEEN a AND b` | `range` with `gte`/`lte` | Exact/Inexact (as range) |
| `col IS NULL` | `bool.must_not` of `exists` | Inexact |
| `col IS NOT NULL` | `exists` | Inexact |
| `col LIKE 'x%'` (keyword) | `prefix` | Inexact |
| `col <> v`, `NOT p`, `NOT IN`, `NOT BETWEEN` | `bool.must_not` (only if the base is Exact) | Inexact |
| `a AND b` | `bool.filter` | Exact if both Exact |
| `a OR b` | `bool.should` + `minimum_should_match: 1` | Exact if both Exact |

Anything else — a predicate on an unmapped/non-indexed column, a type-mismatched literal, a
negation of a superset clause, a partial `OR`, or a date/timestamp comparison — is Unsupported.

## Field types

`EsFilterSchema` records which columns are filterable and how their values map to the DSL. Only
columns that are actually indexed in Elasticsearch (`index: true`) may be registered; a filter on
a non-indexed field would make Elasticsearch reject or mis-answer the query.

- `EsFilterSchema::from_connector_schema` — for an externally-managed index (the SQL connector
  path) when only the derived Arrow schema is available: numeric and boolean columns only, since
  Arrow does not preserve the `text`-vs-`keyword` distinction for strings.
- `EsFilterSchema::from_mapping` — for an externally-managed index when the real Elasticsearch
  mapping is available (see `data_components::elasticsearch::schema::mapping_to_filter_schema`):
  `keyword`/`wildcard`/`constant_keyword` columns are exact-filterable, and `text` columns with a
  keyword-typed multi-field sibling are inexact-filterable against that sibling. Prefer this over
  `from_connector_schema` whenever the mapping is available.
- `EsFilterSchema::from_spice_managed` — for a Spice-managed search index, where string columns
  carry a `.keyword` sub-field and the filterable column set is known.
