/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Connector-agnostic application of **extended schema inference**.
//!
//! When a dataset sets `schema_inference: extended`, a connector emits the source
//! table's inferred primary key, secondary indexes, and sort/clustering order as
//! Arrow schema metadata (see [`data_components::inferred_schema`]). This module
//! reads that metadata back and fills any acceleration settings the user left
//! unset — applied early (before registration), so every refresh mode, including
//! CDC (`refresh_mode: changes`), observes the inferred values.

use std::collections::BTreeSet;

use arrow::datatypes::SchemaRef;
use data_components::inferred_schema::InferredSchema;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, constraints::UpsertOptions,
};

use super::acceleration::{Acceleration, Engine, IndexType, OnConflictBehavior, RefreshMode};

/// Fill acceleration settings the user left unset using `inferred` source metadata.
///
/// Gap-filling only — never overrides a user-configured value:
/// - **primary key**: applied when none is configured; also seeds an `upsert`
///   `on_conflict` on that key so the accelerator upserts by PK and CDC can route
///   `UPDATE`/`DELETE` events.
/// - **indexes**: applied when none are configured (skipping any that merely
///   duplicate the primary key).
/// - **sort columns**: applied when no engine sort param is configured, routed to
///   the engine-appropriate `acceleration.params` key.
/// - **shard key** (Cayenne only): the source's declared partition/shard key is
///   applied as `cayenne_shard_key_columns` when the user set none and it differs
///   from the primary key.
///
/// Columns absent from `effective_schema` (e.g. projected away by `refresh_sql`)
/// are skipped so we never produce a constraint the accelerator would reject.
pub fn apply_inferred_schema(
    acceleration: &mut Acceleration,
    inferred: &InferredSchema,
    effective_schema: &SchemaRef,
    dataset_name: &str,
) {
    if inferred.is_empty() {
        return;
    }

    let has_column = |name: &str| effective_schema.field_with_name(name).is_ok();

    // 1) Primary key.
    if acceleration.primary_key.is_none() && !inferred.primary_key.is_empty() {
        if inferred.primary_key.iter().all(|c| has_column(c)) {
            let pk = ColumnReference::new(inferred.primary_key.clone());
            acceleration.primary_key = Some(pk.clone());
            // Seed an upsert on_conflict for the inferred primary key unless the user
            // configured one for it specifically (any on_conflict entries the user set
            // for other columns are preserved). The accelerator would derive this
            // anyway; declaring it explicitly is also what `refresh_mode: changes`
            // requires to apply UPDATE events as upserts on the primary key.
            acceleration
                .on_conflict
                .entry(pk)
                .or_insert(OnConflictBehavior::Upsert(UpsertOptions::default()));
            tracing::debug!(
                dataset = %dataset_name,
                primary_key = ?inferred.primary_key,
                "Applied inferred primary key"
            );
        } else {
            tracing::debug!(
                dataset = %dataset_name,
                primary_key = ?inferred.primary_key,
                "Skipping inferred primary key; column(s) absent from the accelerated schema"
            );
        }
    }

    // The effective primary-key column set (user-provided or just-inferred), used
    // to drop a secondary index that merely re-states the primary key. Compared as
    // an unordered set because `ColumnReference` stores columns sorted.
    let pk_set: Option<BTreeSet<String>> = acceleration
        .primary_key
        .as_ref()
        .map(|pk| pk.iter().map(ToString::to_string).collect());

    // 2) Secondary indexes — only when the user configured none.
    let mut applied_indexes = 0usize;
    if acceleration.indexes.is_empty() {
        for index in &inferred.indexes {
            if !index.columns.iter().all(|c| has_column(c)) {
                continue;
            }
            let index_set: BTreeSet<String> = index.columns.iter().cloned().collect();
            if pk_set.as_ref() == Some(&index_set) {
                continue; // duplicates the primary key
            }
            let index_type = if index.unique {
                IndexType::Unique
            } else {
                IndexType::Enabled
            };
            acceleration
                .indexes
                .entry(ColumnReference::new(index.columns.clone()))
                .or_insert(index_type);
            applied_indexes += 1;
        }
    }

    // 3) Sort columns — only when no engine sort param is configured. Whether a
    // change-stream dataset gets one is engine-dependent (decided inside
    // `apply_inferred_sort`): for DuckDB/Arrow the inferred sort drives the
    // refresh itself, which is a no-op for a change stream and risks perturbing
    // its initial snapshot, so it stays disabled. Cayenne is the exception — its
    // `cayenne_sort_columns` sorts the background COMPACTION rewrite, not the
    // change stream, so it applies even in changes mode and is what keeps
    // per-file zone maps tight for listing-time pruning on heavy CDC tables.
    let is_changes = acceleration.refresh_mode == Some(RefreshMode::Changes);
    let applied_sort = apply_inferred_sort(acceleration, inferred, effective_schema, is_changes);

    // 4) Shard key — Cayenne only: route the source's declared distribution key
    // (Postgres partition key / MongoDB shard key) to `cayenne_shard_key_columns`
    // so intra-write sharding and parallel compaction merges hash-cluster files
    // along the source's dominant dimension instead of the primary key.
    let applied_shard_key = apply_inferred_shard_key(acceleration, inferred, effective_schema);

    tracing::debug!(
        dataset = %dataset_name,
        indexes = applied_indexes,
        sort_applied = applied_sort,
        shard_key_applied = applied_shard_key,
        "Applied extended schema inference to acceleration settings"
    );
}

/// Inject the inferred sort order into the engine-appropriate `acceleration.params`
/// key, unless the user already configured one. Returns whether a value was set.
fn apply_inferred_sort(
    acceleration: &mut Acceleration,
    inferred: &InferredSchema,
    effective_schema: &SchemaRef,
    is_changes: bool,
) -> bool {
    if inferred.sort_columns.is_empty() {
        return false;
    }

    // Sort is advisory, so filter to columns present in the accelerated schema.
    let present: Vec<&data_components::inferred_schema::InferredSortColumn> = inferred
        .sort_columns
        .iter()
        .filter(|sc| effective_schema.field_with_name(&sc.column).is_ok())
        .collect();
    if present.is_empty() {
        return false;
    }

    let engine = acceleration.engine.to_unpartitioned();
    let key = match engine {
        Engine::DuckDB => "on_refresh_sort_columns",
        Engine::Arrow => "sort_columns",
        Engine::Cayenne => "cayenne_sort_columns",
        // Sqlite / Turso / PostgreSQL accelerators have no sort param.
        _ => return false,
    };

    // For `refresh_mode: changes`, skip engines whose sort param drives the
    // refresh itself (DuckDB `on_refresh_sort_columns`, Arrow `sort_columns`): a
    // refresh-time sort is a no-op for a change stream and risks perturbing the
    // initial snapshot. Cayenne is the exception — `cayenne_sort_columns` sorts
    // the background compaction rewrite, not the change stream (which stays
    // unsorted by design), so it applies and keeps per-file zone maps tight for
    // listing-time pruning on the heavy update tables.
    if is_changes && engine != Engine::Cayenne {
        return false;
    }

    // Respect any user-configured sort param (Cayenne also accepts `sort_columns`).
    let user_configured = acceleration.params.contains_key(key)
        || (engine == Engine::Cayenne && acceleration.params.contains_key("sort_columns"));
    if user_configured {
        return false;
    }

    let value = if engine == Engine::Cayenne {
        // Cayenne sorts by bare column names and ignores direction.
        present
            .iter()
            .map(|sc| sc.column.clone())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        present
            .iter()
            .map(|sc| {
                let mut spec = format!("{} {}", sc.column, if sc.desc { "DESC" } else { "ASC" });
                // Carry the source's declared NULLS placement; when unknown the
                // engine default applies.
                match sc.nulls_first {
                    Some(true) => spec.push_str(" NULLS FIRST"),
                    Some(false) => spec.push_str(" NULLS LAST"),
                    None => {}
                }
                spec
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    acceleration.params.insert(key.to_string(), value);
    true
}

/// Inject the inferred distribution/shard key into `cayenne_shard_key_columns`,
/// unless the user already configured one. Returns whether a value was set.
///
/// Cayenne-only: the param drives intra-write hash sharding, which other engines
/// don't have. Skipped when the key equals the effective primary key (already the
/// engine default), when any key column is absent from the accelerated schema (a
/// partial key would cluster by a different dimension than the source declared),
/// or when the source statistics prove the key is constant (hashing a constant
/// routes every row to one shard, serializing the encode fan-out for nothing).
/// A sorted table renders the key inert — the engine forces a single serial
/// writer — but it is still recorded so the layout follows the source if the
/// sort is later removed.
fn apply_inferred_shard_key(
    acceleration: &mut Acceleration,
    inferred: &InferredSchema,
    effective_schema: &SchemaRef,
) -> bool {
    if inferred.shard_key.is_empty() {
        return false;
    }
    if acceleration.engine.to_unpartitioned() != Engine::Cayenne {
        return false;
    }
    // Respect a user-configured shard key (Cayenne also accepts `shard_key_columns`).
    if acceleration
        .params
        .contains_key("cayenne_shard_key_columns")
        || acceleration.params.contains_key("shard_key_columns")
    {
        return false;
    }
    if !inferred
        .shard_key
        .iter()
        .all(|column| effective_schema.field_with_name(column).is_ok())
    {
        return false;
    }
    // Equal to the effective primary key (user-set or just-inferred) — the
    // engine already hash-clusters by the PK, so the param would be redundant.
    // Compare in order: hash clustering is order-sensitive, so a shard key with
    // the same columns as the PK but a different column order is NOT redundant.
    if let Some(pk) = &acceleration.primary_key {
        let pk_cols: Vec<String> = pk.iter().map(ToString::to_string).collect();
        if pk_cols == inferred.shard_key {
            return false;
        }
    }
    // Known-constant key: every column's inferred distinct count is below 2.
    let known_constant = inferred.shard_key.iter().all(|column| {
        inferred
            .column_stats
            .iter()
            .find(|stats| &stats.column == column)
            .and_then(|stats| stats.distinct_count)
            .is_some_and(|distinct| distinct < 2)
    });
    if known_constant {
        return false;
    }

    acceleration.params.insert(
        "cayenne_shard_key_columns".to_string(),
        inferred.shard_key.join(", "),
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::inferred_schema::{InferredIndex, InferredSortColumn};
    use std::sync::Arc;

    fn schema(cols: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            cols.iter()
                .map(|c| Field::new(*c, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn accel(engine: Engine) -> Acceleration {
        Acceleration {
            engine,
            ..Acceleration::default()
        }
    }

    fn col_ref(cols: &[&str]) -> ColumnReference {
        ColumnReference::new(cols.iter().map(|c| (*c).to_string()).collect())
    }

    fn sort(column: &str, desc: bool) -> InferredSortColumn {
        InferredSortColumn {
            column: column.to_string(),
            desc,
            nulls_first: None,
        }
    }

    #[test]
    fn applies_primary_key_and_seeds_upsert() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id", "name"]), "ds");

        assert_eq!(acc.primary_key, Some(col_ref(&["id"])));
        assert!(matches!(
            acc.on_conflict.get(&col_ref(&["id"])),
            Some(OnConflictBehavior::Upsert(_))
        ));
    }

    #[test]
    fn seeds_primary_key_upsert_alongside_other_on_conflict() {
        // A user-configured on_conflict for a non-PK column must not prevent the
        // inferred primary key from getting its own upsert (CDC requires it).
        let mut acc = accel(Engine::DuckDB);
        acc.on_conflict.insert(
            col_ref(&["name"]),
            OnConflictBehavior::Upsert(UpsertOptions::default()),
        );
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id", "name"]), "ds");

        assert_eq!(acc.primary_key, Some(col_ref(&["id"])));
        assert!(matches!(
            acc.on_conflict.get(&col_ref(&["id"])),
            Some(OnConflictBehavior::Upsert(_))
        ));
        // The user's other on_conflict entry is preserved.
        assert!(acc.on_conflict.contains_key(&col_ref(&["name"])));
    }

    #[test]
    fn respects_user_primary_key() {
        let mut acc = accel(Engine::DuckDB);
        acc.primary_key = Some(col_ref(&["user_id"]));
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id", "user_id"]), "ds");

        assert_eq!(acc.primary_key, Some(col_ref(&["user_id"])));
        // User configured no on_conflict and we didn't infer the PK, so it stays empty.
        assert!(acc.on_conflict.is_empty());
    }

    #[test]
    fn skips_primary_key_with_missing_column() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            primary_key: vec!["absent".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id"]), "ds");
        assert!(acc.primary_key.is_none());
    }

    #[test]
    fn applies_unique_and_non_unique_indexes() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            indexes: vec![
                InferredIndex {
                    columns: vec!["email".to_string()],
                    unique: true,
                },
                InferredIndex {
                    columns: vec!["age".to_string()],
                    unique: false,
                },
            ],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["email", "age"]), "ds");

        assert_eq!(
            acc.indexes.get(&col_ref(&["email"])),
            Some(&IndexType::Unique)
        );
        assert_eq!(
            acc.indexes.get(&col_ref(&["age"])),
            Some(&IndexType::Enabled)
        );
    }

    #[test]
    fn skips_index_equal_to_primary_key() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            indexes: vec![InferredIndex {
                columns: vec!["id".to_string()],
                unique: true,
            }],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id"]), "ds");
        assert!(
            acc.indexes.is_empty(),
            "PK-duplicate index should be skipped"
        );
    }

    #[test]
    fn skips_inferred_indexes_when_user_configured_any() {
        let mut acc = accel(Engine::DuckDB);
        acc.indexes
            .insert(col_ref(&["existing"]), IndexType::Enabled);
        let inferred = InferredSchema {
            indexes: vec![InferredIndex {
                columns: vec!["email".to_string()],
                unique: true,
            }],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["existing", "email"]), "ds");
        assert_eq!(acc.indexes.len(), 1);
        assert!(acc.indexes.contains_key(&col_ref(&["existing"])));
    }

    #[test]
    fn duckdb_sort_param_carries_direction() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true), sort("id", false)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at", "id"]), "ds");
        assert_eq!(
            acc.params
                .get("on_refresh_sort_columns")
                .map(String::as_str),
            Some("created_at DESC, id ASC")
        );
    }

    #[test]
    fn arrow_sort_param_carries_direction() {
        let mut acc = accel(Engine::Arrow);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true), sort("id", false)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at", "id"]), "ds");
        assert_eq!(
            acc.params.get("sort_columns").map(String::as_str),
            Some("created_at DESC, id ASC")
        );
    }

    #[test]
    fn cayenne_sort_param_uses_bare_names() {
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true), sort("id", false)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at", "id"]), "ds");
        assert_eq!(
            acc.params.get("cayenne_sort_columns").map(String::as_str),
            Some("created_at, id")
        );
    }

    #[test]
    fn respects_user_sort_param() {
        let mut acc = accel(Engine::DuckDB);
        acc.params
            .insert("on_refresh_sort_columns".to_string(), "custom".to_string());
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at"]), "ds");
        assert_eq!(
            acc.params
                .get("on_refresh_sort_columns")
                .map(String::as_str),
            Some("custom")
        );
    }

    #[test]
    fn sqlite_has_no_sort_param() {
        let mut acc = accel(Engine::Sqlite);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at"]), "ds");
        assert!(acc.params.is_empty());
    }

    #[test]
    fn filters_missing_sort_columns() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true), sort("absent", false)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at"]), "ds");
        assert_eq!(
            acc.params
                .get("on_refresh_sort_columns")
                .map(String::as_str),
            Some("created_at DESC")
        );
    }

    #[test]
    fn does_not_apply_sort_for_changes_refresh_mode_on_refresh_time_engines() {
        // DuckDB/Arrow apply the inferred sort as a *refresh-time* sort, which is
        // a no-op for a change stream and risks perturbing its initial snapshot —
        // so it stays disabled in changes mode. (Cayenne is the exception; see
        // `applies_sort_for_changes_refresh_mode_on_cayenne`.)
        let mut acc = accel(Engine::DuckDB);
        acc.refresh_mode = Some(RefreshMode::Changes);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at"]), "ds");
        assert!(acc.params.is_empty());
    }

    #[test]
    fn applies_sort_for_changes_refresh_mode_on_cayenne() {
        // Cayenne's `cayenne_sort_columns` sorts the background COMPACTION rewrite,
        // not the change stream, so an inferred sort SHOULD be applied even in
        // changes mode — this is what keeps per-file zone maps tight for
        // listing-time pruning on heavy CDC tables.
        let mut acc = accel(Engine::Cayenne);
        acc.refresh_mode = Some(RefreshMode::Changes);
        let inferred = InferredSchema {
            sort_columns: vec![sort("created_at", true), sort("id", false)],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at", "id"]), "ds");
        assert_eq!(
            acc.params.get("cayenne_sort_columns").map(String::as_str),
            Some("created_at, id")
        );
    }

    #[test]
    fn empty_inferred_is_noop() {
        let mut acc = accel(Engine::DuckDB);
        apply_inferred_schema(&mut acc, &InferredSchema::default(), &schema(&["id"]), "ds");
        assert!(acc.primary_key.is_none());
        assert!(acc.indexes.is_empty());
        assert!(acc.params.is_empty());
    }

    #[test]
    fn duckdb_sort_param_carries_nulls_placement() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            sort_columns: vec![
                InferredSortColumn {
                    column: "created_at".to_string(),
                    desc: true,
                    nulls_first: Some(true),
                },
                InferredSortColumn {
                    column: "id".to_string(),
                    desc: false,
                    nulls_first: Some(false),
                },
            ],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["created_at", "id"]), "ds");
        assert_eq!(
            acc.params
                .get("on_refresh_sort_columns")
                .map(String::as_str),
            Some("created_at DESC NULLS FIRST, id ASC NULLS LAST")
        );
    }

    #[test]
    fn applies_shard_key_for_cayenne_when_distinct_from_primary_key() {
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            shard_key: vec!["tenant_id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id", "tenant_id"]), "ds");
        assert_eq!(
            acc.params
                .get("cayenne_shard_key_columns")
                .map(String::as_str),
            Some("tenant_id")
        );
    }

    #[test]
    fn skips_shard_key_equal_to_primary_key() {
        // The engine already hash-clusters by the PK; an equal shard key is
        // redundant config.
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            shard_key: vec!["id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id"]), "ds");
        assert!(!acc.params.contains_key("cayenne_shard_key_columns"));
    }

    #[test]
    fn applies_shard_key_that_reorders_primary_key_columns() {
        // Hash clustering is order-sensitive, so a shard key with the same
        // columns as the PK but a different column order is NOT redundant.
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            primary_key: vec!["a".to_string(), "b".to_string()],
            shard_key: vec!["b".to_string(), "a".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["a", "b"]), "ds");
        assert_eq!(
            acc.params
                .get("cayenne_shard_key_columns")
                .map(String::as_str),
            Some("b, a")
        );
    }

    #[test]
    fn respects_user_shard_key_param() {
        let mut acc = accel(Engine::Cayenne);
        acc.params
            .insert("shard_key_columns".to_string(), "custom".to_string());
        let inferred = InferredSchema {
            shard_key: vec!["tenant_id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["tenant_id"]), "ds");
        assert!(!acc.params.contains_key("cayenne_shard_key_columns"));
    }

    #[test]
    fn skips_shard_key_on_non_cayenne_engines() {
        let mut acc = accel(Engine::DuckDB);
        let inferred = InferredSchema {
            shard_key: vec!["tenant_id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["tenant_id"]), "ds");
        assert!(acc.params.is_empty());
    }

    #[test]
    fn skips_shard_key_with_missing_column() {
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            shard_key: vec!["tenant_id".to_string(), "absent".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["tenant_id"]), "ds");
        assert!(!acc.params.contains_key("cayenne_shard_key_columns"));
    }

    #[test]
    fn skips_shard_key_known_to_be_constant() {
        use data_components::inferred_schema::InferredColumnStats;

        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            shard_key: vec!["region".to_string()],
            column_stats: vec![InferredColumnStats {
                column: "region".to_string(),
                distinct_count: Some(1),
                correlation: None,
            }],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["region"]), "ds");
        assert!(!acc.params.contains_key("cayenne_shard_key_columns"));

        // With a real cardinality (or none known) the key applies.
        let mut acc = accel(Engine::Cayenne);
        let inferred = InferredSchema {
            shard_key: vec!["region".to_string()],
            column_stats: vec![InferredColumnStats {
                column: "region".to_string(),
                distinct_count: Some(12),
                correlation: None,
            }],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["region"]), "ds");
        assert_eq!(
            acc.params
                .get("cayenne_shard_key_columns")
                .map(String::as_str),
            Some("region")
        );
    }

    #[test]
    fn applies_shard_key_for_changes_refresh_mode() {
        // CDC tables compact too — the shard key clusters their parallel merge
        // outputs, so changes mode must not gate it.
        let mut acc = accel(Engine::Cayenne);
        acc.refresh_mode = Some(RefreshMode::Changes);
        let inferred = InferredSchema {
            primary_key: vec!["id".to_string()],
            shard_key: vec!["tenant_id".to_string()],
            ..InferredSchema::default()
        };
        apply_inferred_schema(&mut acc, &inferred, &schema(&["id", "tenant_id"]), "ds");
        assert_eq!(
            acc.params
                .get("cayenne_shard_key_columns")
                .map(String::as_str),
            Some("tenant_id")
        );
    }
}
