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

//! Runtime-side helpers for `on_schema_change` widening schema evolution.
//!
//! The classifier itself lives in [`arrow_tools::schema_evolution`]; this module
//! holds the runtime policy gate ([`evolution_allowed`]), the v1 engine support
//! matrix, the constraint-column extraction consulted by the classifier's
//! constraint guard, and the canonical-ordering helper used at registration.
//!
//! `on_schema_change: block` (the default) must run today's code paths verbatim
//! everywhere — every caller of these helpers guards on `policy != Block` first.

use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use arrow_tools::schema_evolution::WideningPlan;
use datafusion::common::{Constraint, Constraints};
use opentelemetry::KeyValue;

use crate::dataset::{
    DatasetSpec, OnSchemaChange,
    acceleration::{Acceleration, Engine, Mode, RefreshMode},
};

static SCHEMA_EVOLUTION_METER: std::sync::LazyLock<opentelemetry::metrics::Meter> =
    std::sync::LazyLock::new(|| opentelemetry::global::meter("schema_evolution"));

pub static SCHEMA_EVOLUTION_DETECTED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        SCHEMA_EVOLUTION_METER
        .u64_counter("schema_evolution_detected")
        .with_description(
            "Schema changes detected between an incoming source schema and the stored/accelerator schema.",
        )
        .build()
    });

pub static SCHEMA_EVOLUTION_APPLIED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        SCHEMA_EVOLUTION_METER
            .u64_counter("schema_evolution_applied")
            .with_description(
                "Schema evolutions applied to the accelerator or cached source schema.",
            )
            .build()
    });

pub static SCHEMA_EVOLUTION_FAILED: std::sync::LazyLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::LazyLock::new(|| {
        SCHEMA_EVOLUTION_METER
        .u64_counter("schema_evolution_failed")
        .with_description(
            "Schema changes that were not applied: incompatible, blocked by policy, or requiring a restart.",
        )
        .build()
    });

#[must_use]
pub fn schema_evolution_labels(
    dataset: &str,
    kind: &'static str,
    action: &'static str,
) -> [KeyValue; 3] {
    [
        KeyValue::new("dataset", dataset.to_string()),
        KeyValue::new("kind", kind),
        KeyValue::new("action", action),
    ]
}

/// Dominant change kind of a widening plan for the `kind` metric label.
#[must_use]
pub fn widening_plan_kind(plan: &WideningPlan) -> &'static str {
    if !plan.widened_columns.is_empty() {
        "widened_types"
    } else if !plan.relaxed_nullability.is_empty() {
        "nullability"
    } else {
        "added_columns"
    }
}

/// `on_schema_change` evolution-set gate: `append_new_columns` evolves
/// added-nullable-columns-only plans; `sync_all_columns` evolves the full
/// widening set (added columns + lossless type widening + nullability relax);
/// `block`/`fail` never evolve.
#[must_use]
pub fn evolution_allowed(policy: OnSchemaChange, plan: &WideningPlan) -> bool {
    match policy {
        OnSchemaChange::AppendNewColumns => plan.is_additive_only(),
        // `drop_and_recreate` evolves the full widening set in place exactly like
        // `sync_all_columns`; it additionally recreates the table for changes that
        // cannot be applied in place (handled at the registration call site, gated
        // on `refresh_mode: full`).
        OnSchemaChange::SyncAllColumns | OnSchemaChange::DropAndRecreate => true,
        OnSchemaChange::Block | OnSchemaChange::Fail => false,
    }
}

/// Whether `policy` recreates the accelerated table (drop + recreate with the new
/// schema) for a schema change that cannot be applied in place. Only
/// `drop_and_recreate` does. The actual recreate is further gated at the call site on
/// `refresh_mode: full` (a full refresh re-fetches every row, so dropping is lossless)
/// and a recreate-capable engine ([`engine_supports_recreate`]).
#[must_use]
pub fn policy_recreates_on_incompatible(policy: OnSchemaChange) -> bool {
    matches!(policy, OnSchemaChange::DropAndRecreate)
}

/// Engines with a v1 [`data_accelerator_api::DataAccelerator::evolve_table_schema`]
/// implementation. Partitioned engines are excluded: each partition table would
/// need the DDL and the partition provider pins its schema. Arrow (memory-only)
/// and Postgres rely on the trait default and degrade to block-equivalent.
#[must_use]
pub fn engine_supports_in_place_evolution(engine: Engine) -> bool {
    matches!(
        engine,
        Engine::DuckDB | Engine::Sqlite | Engine::Turso | Engine::Cayenne
    )
}

/// Engines whose [`data_accelerator_api::DataAccelerator::drop_table`] actually drops the
/// stored table, so the accelerated table can be dropped and recreated with a new schema (the
/// `on_schema_change: drop_and_recreate` path). Today this is exactly the in-place-evolution
/// set: the four engines with a real `drop_table` (DuckDB/SQLite/Turso/Cayenne) are the same
/// four with `evolve_table_schema`; Arrow is memory-only (a full refresh rebuilds it) and
/// Postgres / partitioned engines have a no-op `drop_table`. Delegate to keep the two in sync;
/// if a future engine gains one capability but not the other, split this back into its own match.
#[must_use]
pub fn engine_supports_recreate(engine: Engine) -> bool {
    engine_supports_in_place_evolution(engine)
}

/// Whether a schema change that cannot be applied in place should DROP and RECREATE the
/// accelerated table (rather than defer/reject). True for `mode: file_update` when refreshes
/// are enabled (`refresh_mode != disabled`), and for `on_schema_change: drop_and_recreate`
/// under `refresh_mode: full` on a recreate-capable engine ([`engine_supports_recreate`]).
///
/// This is the single source of truth for the recreate decision: registration
/// (`handle_schema_difference`) gates the actual drop+recreate on it, and the initial-load
/// and reload schema-mismatch gates use it to decide whether to bypass the deferred-mismatch
/// retry loop and let registration recreate the table. Consulting one helper everywhere keeps
/// those sites from drifting (e.g. one omitting the engine check and waving a mismatch through
/// that registration then refuses to recreate).
#[must_use]
pub fn recreates_on_schema_mismatch(
    acceleration: &Acceleration,
    on_schema_change: OnSchemaChange,
    refresh_mode: RefreshMode,
) -> bool {
    let is_file_update =
        acceleration.mode == Mode::FileUpdate && refresh_mode != RefreshMode::Disabled;
    let policy_recreate = policy_recreates_on_incompatible(on_schema_change)
        && refresh_mode == RefreshMode::Full
        && engine_supports_recreate(acceleration.engine);
    is_file_update || policy_recreate
}

/// Emit a `task_history` event for a schema-evolution outcome so the change is
/// queryable in `spice.runtime.task_history` (the runtime event system), alongside the
/// `schema_evolution_*` metric counters. A successful evolution (`error == false`)
/// records `captured_output`; a rejected or failed one (`error == true`) records an
/// error on the task span. `action` is the lifecycle stage — e.g. `applied`,
/// `recreated`, `fail_policy`, `blocked_by_policy`, `incompatible`, `restart_required`.
pub fn emit_schema_evolution_event(dataset_name: &str, action: &str, change: &str, error: bool) {
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "accelerated_schema_evolution",
        input = %dataset_name,
    );
    if error {
        tracing::error!(target: "task_history", parent: &span, action, "schema evolution {action}: {change}");
    } else {
        tracing::info!(
            target: "task_history",
            parent: &span,
            action,
            captured_output = %format!("{action}: {change}"),
            "schema evolution {action}",
        );
    }
}

/// Column names referenced by the dataset's primary key / unique / index
/// constraints, merged from the acceleration settings (`primary_key`, `indexes`,
/// `on_conflict`) and any provider-derived constraints (positional indices
/// resolved against `provider_schema`). These feed the classifier's constraint
/// guard: widening or relaxing the nullability of any of them is Incompatible
/// because engines persist typed key encodings.
#[must_use]
pub fn dataset_constraint_columns(
    dataset: &DatasetSpec,
    provider_constraints: Option<&Constraints>,
    provider_schema: &Schema,
) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    if let Some(acceleration) = &dataset.acceleration {
        if let Some(primary_key) = &acceleration.primary_key {
            columns.extend(primary_key.iter().map(ToString::to_string));
        }
        for index in acceleration.indexes.keys() {
            columns.extend(index.iter().map(ToString::to_string));
        }
        for on_conflict in acceleration.on_conflict.keys() {
            columns.extend(on_conflict.iter().map(ToString::to_string));
        }
    }
    if let Some(constraints) = provider_constraints {
        for constraint in constraints.iter() {
            let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
            for &index in indices {
                if let Some(field) = provider_schema.fields().get(index) {
                    columns.push(field.name().clone());
                }
            }
        }
    }
    columns.sort_unstable();
    columns.dedup();
    columns
}

/// Reorders `schema`'s fields to the `checkpoint` field order (name-based),
/// appending fields absent from the checkpoint at the end in their original
/// relative order. Returns `None` when the order already matches.
///
/// Registration must use checkpoint order whenever a checkpoint exists so that
/// positional engine paths (`verify_schema`, `DuckDB`'s positional
/// `INSERT … SELECT *`) stay aligned with the stored table across restarts.
/// Field definitions are taken from `schema` — only the order changes.
#[must_use]
pub fn reorder_to_checkpoint_order(checkpoint: &Schema, schema: &SchemaRef) -> Option<SchemaRef> {
    let mut reordered: Vec<FieldRef> = Vec::with_capacity(schema.fields().len());
    for checkpoint_field in checkpoint.fields() {
        if let Some(field) = schema
            .fields()
            .iter()
            .find(|f| f.name() == checkpoint_field.name())
        {
            reordered.push(Arc::clone(field));
        }
    }
    for field in schema.fields() {
        if !checkpoint.fields().iter().any(|f| f.name() == field.name()) {
            reordered.push(Arc::clone(field));
        }
    }

    let already_ordered = reordered.len() == schema.fields().len()
        && reordered
            .iter()
            .zip(schema.fields())
            .all(|(reordered_field, field)| reordered_field.name() == field.name());
    if already_ordered {
        return None;
    }

    Some(Arc::new(Schema::new_with_metadata(
        reordered,
        schema.metadata().clone(),
    )))
}

/// Restricts `schema` to the fields whose names appear in `allowed`, preserving
/// `schema`'s field order and metadata.
///
/// The canonical checkpoint schema keeps the full source field set (so the
/// restart-time block gate matches the source by name), but for `refresh_sql`
/// datasets that set includes columns the accelerator never materializes. Schema
/// evolution must compare only the materialized (refresh-schema) columns, or those
/// non-materialized columns would be mis-classified as removed. A no-op when the
/// column sets already match (the common, non-`refresh_sql` case).
#[must_use]
pub fn restrict_schema_to(schema: &Schema, allowed: &Schema) -> SchemaRef {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .filter(|field| allowed.field_with_name(field.name()).is_ok())
        .map(Arc::clone)
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn widening_plan(
        added: Vec<FieldRef>,
        widened: Vec<arrow_tools::schema_evolution::ColumnWidening>,
        relaxed: Vec<String>,
    ) -> WideningPlan {
        WideningPlan {
            added_columns: added,
            widened_columns: widened,
            relaxed_nullability: relaxed,
            evolved_schema: Arc::new(Schema::empty()),
        }
    }

    #[test]
    fn evolution_allowed_per_policy() {
        let additive = widening_plan(
            vec![Arc::new(Field::new("c", DataType::Utf8, true))],
            vec![],
            vec![],
        );
        let widening = widening_plan(
            vec![],
            vec![arrow_tools::schema_evolution::ColumnWidening {
                name: "a".to_string(),
                from: DataType::Int32,
                to: DataType::Int64,
            }],
            vec![],
        );
        let relaxing = widening_plan(vec![], vec![], vec!["a".to_string()]);

        assert!(evolution_allowed(
            OnSchemaChange::AppendNewColumns,
            &additive
        ));
        assert!(!evolution_allowed(
            OnSchemaChange::AppendNewColumns,
            &widening
        ));
        assert!(!evolution_allowed(
            OnSchemaChange::AppendNewColumns,
            &relaxing
        ));
        assert!(evolution_allowed(OnSchemaChange::SyncAllColumns, &additive));
        assert!(evolution_allowed(OnSchemaChange::SyncAllColumns, &widening));
        assert!(evolution_allowed(OnSchemaChange::SyncAllColumns, &relaxing));
        // `drop_and_recreate` evolves the full widening set in place like `sync_all_columns`.
        assert!(evolution_allowed(
            OnSchemaChange::DropAndRecreate,
            &additive
        ));
        assert!(evolution_allowed(
            OnSchemaChange::DropAndRecreate,
            &widening
        ));
        assert!(evolution_allowed(
            OnSchemaChange::DropAndRecreate,
            &relaxing
        ));
        assert!(!evolution_allowed(OnSchemaChange::Block, &additive));
        assert!(!evolution_allowed(OnSchemaChange::Fail, &additive));
    }

    #[test]
    fn only_drop_and_recreate_recreates_on_incompatible() {
        assert!(policy_recreates_on_incompatible(
            OnSchemaChange::DropAndRecreate
        ));
        assert!(!policy_recreates_on_incompatible(
            OnSchemaChange::SyncAllColumns
        ));
        assert!(!policy_recreates_on_incompatible(
            OnSchemaChange::AppendNewColumns
        ));
        assert!(!policy_recreates_on_incompatible(OnSchemaChange::Block));
        assert!(!policy_recreates_on_incompatible(OnSchemaChange::Fail));
    }

    #[test]
    fn engine_support_matrix() {
        assert!(engine_supports_in_place_evolution(Engine::DuckDB));
        assert!(engine_supports_in_place_evolution(Engine::Sqlite));
        assert!(engine_supports_in_place_evolution(Engine::Turso));
        assert!(engine_supports_in_place_evolution(Engine::Cayenne));
        assert!(!engine_supports_in_place_evolution(Engine::Arrow));
        assert!(!engine_supports_in_place_evolution(
            Engine::PartitionedArrow
        ));
        assert!(!engine_supports_in_place_evolution(Engine::PostgreSQL));
    }

    #[test]
    fn recreate_engine_matrix() {
        // Recreate requires a real `drop_table`: same engine set as in-place evolution.
        assert!(engine_supports_recreate(Engine::DuckDB));
        assert!(engine_supports_recreate(Engine::Sqlite));
        assert!(engine_supports_recreate(Engine::Turso));
        assert!(engine_supports_recreate(Engine::Cayenne));
        assert!(!engine_supports_recreate(Engine::Arrow));
        assert!(!engine_supports_recreate(Engine::PartitionedArrow));
        assert!(!engine_supports_recreate(Engine::PostgreSQL));
    }

    #[test]
    fn reorder_matches_checkpoint_order_and_appends_new_fields() {
        let checkpoint = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Float64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false),
        ]));

        let reordered =
            reorder_to_checkpoint_order(&checkpoint, &schema).expect("order should change");
        let names: Vec<&str> = reordered
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["a", "b", "c"]);
    }

    #[test]
    fn reorder_returns_none_when_order_already_matches() {
        let checkpoint = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]));
        assert!(reorder_to_checkpoint_order(&checkpoint, &schema).is_none());
    }

    #[test]
    fn reorder_keeps_schema_field_definitions() {
        // Order comes from the checkpoint; field types/nullability come from `schema`.
        let checkpoint = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false),
        ]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::LargeUtf8, true),
        ]));

        let reordered =
            reorder_to_checkpoint_order(&checkpoint, &schema).expect("order should change");
        assert_eq!(reordered.field(0).name(), "b");
        assert_eq!(reordered.field(0).data_type(), &DataType::LargeUtf8);
        assert_eq!(reordered.field(1).name(), "a");
        assert_eq!(reordered.field(1).data_type(), &DataType::Int64);
        assert!(reordered.field(1).is_nullable());
    }
}
