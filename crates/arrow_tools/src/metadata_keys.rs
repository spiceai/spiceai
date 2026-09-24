/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Canonical Arrow metadata keys.
//!
//! Connectors write these onto the schemas and fields they produce; the runtime
//! and the `pg_catalog` views read them back. They live here, below both, so a
//! reader and a writer of the same key agree on it without one depending on the
//! other.

/// Schema-level metadata key for foreign key relationships.
///
/// The value is a JSON array of objects, each describing one foreign key constraint:
/// ```json
/// [
///   {
///     "columns": ["customer_id"],
///     "foreign_table": "catalog.public.customers",
///     "foreign_columns": ["id"]
///   }
/// ]
/// ```
///
/// `foreign_table` is a fully-qualified `catalog.schema.table` name whose
/// components are quoted following `PostgreSQL` `quote_ident` semantics
/// (quoted only when required, doubling any embedded `"`). This keeps the
/// name unambiguous — and round-trippable via `TableReference::parse_str` —
/// when a component legally contains a `.`, e.g. `catalog."my.schema".table`.
pub const FOREIGN_KEYS_METADATA_KEY: &str = "foreign_keys";

/// Canonical Arrow metadata key for user-facing table and column descriptions.
pub const DESCRIPTION_METADATA_KEY: &str = "description";

/// Canonical Arrow field metadata key for the source-native column type.
pub const SOURCE_TYPE_METADATA_KEY: &str = "source_type";

/// Canonical Arrow field metadata key marking source partition columns.
pub const PARTITION_METADATA_KEY: &str = "partition";

/// Canonical Arrow field metadata key marking source clustering columns.
///
/// Values are one-based ordinals when the source reports clustering order.
pub const CLUSTERING_METADATA_KEY: &str = "clustering";

/// Canonical Arrow schema metadata key for a source-native clustering expression.
pub const CLUSTERING_KEY_METADATA_KEY: &str = "clustering_key";

/// Schema-level metadata key for an inferred primary key (schema inference).
///
/// The value is a JSON array of column names in key order, e.g. `["tenant_id","id"]`.
/// Emitted by connectors that perform schema inference and consumed by the
/// runtime to fill `acceleration.primary_key` when the user left it unset.
pub const INFERRED_PRIMARY_KEY_METADATA_KEY: &str = "spice.inferred_primary_key";

/// Schema-level metadata key for inferred secondary indexes (schema inference).
///
/// The value is a JSON array of objects, each describing one index:
/// `[{ "columns": ["email"], "unique": true }]`.
pub const INFERRED_INDEXES_METADATA_KEY: &str = "spice.inferred_indexes";

/// Schema-level metadata key for inferred sort/clustering columns (schema inference).
///
/// The value is a JSON array of objects in sort order, each with a direction:
/// `[{ "column": "created_at", "desc": true }, { "column": "id", "desc": false }]`.
pub const INFERRED_SORT_COLUMNS_METADATA_KEY: &str = "spice.inferred_sort_columns";

/// Schema-level metadata key for the rough estimated row count (schema inference).
///
/// The value is a base-10 integer string. This is a catalog estimate (e.g. Postgres
/// `pg_class.reltuples`), not a precise count, and is surfaced as table statistics.
pub const INFERRED_ROW_COUNT_METADATA_KEY: &str = "spice.inferred_row_count";

/// Schema-level metadata key for the rough estimated table byte size (schema inference).
///
/// The value is a base-10 integer string of bytes (e.g. Postgres `pg_relation_size`).
pub const INFERRED_TABLE_BYTES_METADATA_KEY: &str = "spice.inferred_table_bytes";

/// Schema-level metadata key for the source's declared distribution/shard key
/// (schema inference): Postgres partition-key columns or the `MongoDB`
/// shard-key fields.
///
/// The value is a JSON array of column names in key order: `["region", "id"]`.
pub const INFERRED_SHARD_KEY_METADATA_KEY: &str = "spice.inferred_shard_key";

/// Schema-level metadata key for rough per-column statistics (extended schema
/// inference), e.g. from Postgres `pg_stats`.
///
/// The value is a JSON array of objects:
/// `[{ "column": "created_at", "distinct_count": 100000, "correlation": 0.99 }]`.
pub const INFERRED_COLUMN_STATS_METADATA_KEY: &str = "spice.inferred_column_stats";

/// Arrow schema metadata key the HTTP connector sets on every batch it
/// produces (via `HttpTableProvider::schema_with_fetch_status`) to the real,
/// per-fetch HTTP status as a string (e.g. `"503"`) — not a fixed sentinel.
///
/// `refresh_mode: caching` (and the independent, runtime-wide SQL results
/// cache) calls `cache::batches_cacheable` on every fetch to tell a
/// transient origin failure from real data, regardless of connector.
/// Inferring that a batch came from the HTTP connector by its column names
/// and types alone is not sound: an unrelated `refresh_mode: caching`
/// dataset (e.g. a `localpod` table) can legitimately have its own
/// `response_status: UInt16` and `_fetched_at` columns, and a business value
/// of `503` in that column is not an origin failure. This key's mere
/// presence on the *schema* (not a field) is the provenance signal that
/// tells the two cases apart, and it survives a schema rebuilt into a
/// narrower, JSON-decomposed shape regardless of which columns that schema
/// keeps.
pub const HTTP_RESPONSE_STATUS_METADATA_KEY: &str = "spice.http_response_status";

/// `ExecutionPlan::metrics()` counter name the HTTP connector's `HttpExec`
/// increments once per fetch/page it turned into a successful batch that
/// actually carried a retryable status (5xx/429) — the same "retryable but
/// not zero rows" case `HTTP_RESPONSE_STATUS_METADATA_KEY` and the
/// `response_status` column exist to report, but readable even when a user
/// projection (e.g. `SELECT rank FROM http_data`) prunes `response_status`
/// out of the batch before it ever reaches `cache::batches_cacheable`.
/// Metrics live on the plan tree, not the batch schema, so column pruning
/// cannot remove them; a plan-metrics walk from the root, summed by this
/// name, is how a caller checks for this failure past an arbitrary
/// projection. Named for the fetch/page it counts, not the rows in it — one
/// increment can stand for a batch of any row count, including zero.
pub const HTTP_TRANSIENT_FAILURE_METRIC_NAME: &str = "http_transient_failure_fetches";
