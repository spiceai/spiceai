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

use std::{pin::Pin, sync::Arc};

use arrow_schema::SchemaRef;
use arrow_tools::{
    schema_evolution::is_widening_cast,
    type_rewrite::{TypeRewriteRules, normalize_dictionary_types, rewrite_data_type},
};
use data_components::index_maintenance::perform_index_maintenance;
use datafusion::{
    catalog::TableProvider, execution::RecordBatchStream, logical_expr::dml::InsertOp,
    physical_plan::collect, prelude::SessionContext,
};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_table_partition::provider::PartitionTableProvider;
use spice_table::{Index, SpiceTable, WriteWindow};
use util::RetryError;

use crate::accelerated::{
    refresh_task::changes::schema_evolution_first_warn,
    refresh_task::retry_from_df_error,
    sink::{finalize_indexes, prepare_indexes, rollback_indexes},
};
use runtime_acceleration::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_component::schema_evolution::{SCHEMA_EVOLUTION_DETECTED, schema_evolution_labels};
use runtime_datafusion::error::find_datafusion_root;

/// What casting `input_schema` to `target_schema` does to each column.
#[derive(Debug, Default)]
struct SchemaCastChanges {
    /// Columns in the input that the target does not have; their values are dropped.
    dropped: Vec<String>,
    /// Columns whose target type is neither equal nor a lossless widening, and which
    /// the acceleration engine's own rewrites do not account for.
    narrowed: Vec<String>,
    /// Columns whose target type is exactly what the engine's creation-time rewrites
    /// produce from the input type. The cast still happens - precision outside what
    /// the engine can store is not retained - but the acceleration is not stale, and
    /// no `on_schema_change` policy can change the stored type.
    engine_normalized: Vec<String>,
}

impl SchemaCastChanges {
    fn is_empty(&self) -> bool {
        self.dropped.is_empty() && self.narrowed.is_empty() && self.engine_normalized.is_empty()
    }
}

/// Classifies what casting `input_schema` to `target_schema` would lose. Dictionary
/// encodings are normalized away first so a dictionary unwrap is not reported.
///
/// `engine_type_rewrites` are the rewrites the acceleration engine itself applies at
/// table creation, declared by `DataAccelerator::type_rewrite_rules`. A column whose
/// target type is what those rules produce is reported as `engine_normalized` rather
/// than `narrowed`: the engine derives that type from this very input, so the
/// acceleration is not lagging the source and no schema change can make the two
/// agree. It is still reported, because the cast is real - Cayenne/Vortex keeps
/// timestamps at microsecond precision, so a genuinely sub-microsecond source value
/// does not survive the write.
fn narrowing_schema_cast_changes(
    input_schema: &SchemaRef,
    target_schema: &SchemaRef,
    engine_type_rewrites: TypeRewriteRules,
) -> SchemaCastChanges {
    // Hot path: this runs once per insert. Identical schemas (the common
    // no-schema-change case) cannot narrow, so skip the two
    // `normalize_dictionary_types` allocations entirely. A dictionary-only
    // difference is neither pointer- nor structurally equal and still falls
    // through to the normalized comparison below (correctly reported as no-op).
    if Arc::ptr_eq(input_schema, target_schema) || input_schema == target_schema {
        return SchemaCastChanges::default();
    }
    let input = normalize_dictionary_types(input_schema);
    let target = normalize_dictionary_types(target_schema);

    let mut changes = SchemaCastChanges::default();
    for input_field in input.fields() {
        // `Fields::find` rather than `Schema::field_with_name`: the miss is an ordinary
        // outcome here (a dropped column), and `field_with_name` builds a `Vec` of every
        // field name plus a formatted error to report it.
        let Some((_, target_field)) = target.fields().find(input_field.name()) else {
            changes.dropped.push(input_field.name().clone());
            continue;
        };
        let from = input_field.data_type();
        let to = target_field.data_type();
        if from == to || is_widening_cast(from, to) {
            continue;
        }
        let description = format!("{}: {from} -> {to}", input_field.name());
        // Ask the rules about this one type rather than rewriting the whole schema:
        // only a column that already looks narrowed can be explained by them.
        if rewrite_data_type(from, engine_type_rewrites) == *to {
            changes.engine_normalized.push(description);
        } else {
            changes.narrowed.push(description);
        }
    }
    changes
}

/// Plan-level check (once per insert, not per batch) for silent narrowing through
/// [`SchemaCastScanExec`]: warns unconditionally - for every `on_schema_change`
/// policy - when the cast target drops columns present in the input plan schema or
/// casts them to a type that is not a lossless widening.
///
/// A cast the engine's own rewrites account for is reported separately and only
/// once per dataset and change, because it repeats on every refresh for the life of
/// the table and no operator action will stop it.
fn warn_on_narrowing_schema_cast(
    dataset: &str,
    input_schema: &SchemaRef,
    target_schema: &SchemaRef,
    engine_type_rewrites: TypeRewriteRules,
) {
    let changes = narrowing_schema_cast_changes(input_schema, target_schema, engine_type_rewrites);
    if changes.is_empty() {
        return;
    }

    if !changes.engine_normalized.is_empty() {
        let normalized = changes.engine_normalized.join(", ");
        if schema_evolution_first_warn(format!("{dataset}|engine_normalized|{normalized}")) {
            tracing::warn!(
                dataset = %dataset,
                "TableSink: the acceleration engine for {dataset} cannot store the source type of [{normalized}] and rewrote it when the table was created, so every write casts to the stored type and precision beyond it is not retained. The acceleration is not stale and `on_schema_change` cannot change the stored type; accelerate with an engine that supports the source type if the extra precision matters. See: https://spiceai.org/docs/components/data-accelerators"
            );
            SCHEMA_EVOLUTION_DETECTED.add(
                1,
                &schema_evolution_labels(dataset, "engine_normalized", "sink_engine_type_rewrite"),
            );
        }
    }

    if changes.dropped.is_empty() && changes.narrowed.is_empty() {
        return;
    }

    // Each case carries its own remediation, kept beside the change it answers so the
    // two cannot drift apart. Only the dropped-column case is answered by
    // `on_schema_change`: it adopts a source change when the change is a lossless
    // widening, which a narrowing is not by construction, so advising it for a narrowed
    // column sends operators through settings that cannot alter the stored type.
    let mut cases: Vec<(String, &str)> = Vec::new();
    if !changes.dropped.is_empty() {
        cases.push((
            format!("dropping columns [{}]", changes.dropped.join(", ")),
            "Set `on_schema_change` and restart Spice to evolve the accelerated table with the new columns",
        ));
    }
    if !changes.narrowed.is_empty() {
        cases.push((
            format!("narrowing column types [{}]", changes.narrowed.join(", ")),
            "The narrowed columns need an accelerated table built for the source's types; `on_schema_change` adopts only lossless widenings and will not change them",
        ));
    }
    let described: Vec<&str> = cases.iter().map(|(change, _)| change.as_str()).collect();
    let remedies: Vec<&str> = cases.iter().map(|(_, remedy)| *remedy).collect();
    tracing::warn!(
        dataset = %dataset,
        "TableSink: the accelerated table schema for {dataset} is behind the incoming data; the write cast is {}. Values in these columns are silently lost or lossily cast. {}. See: https://spiceai.org/docs/components/data-accelerators",
        described.join(" and "),
        remedies.join(". "),
    );
    SCHEMA_EVOLUTION_DETECTED.add(
        1,
        &schema_evolution_labels(dataset, "incompatible", "sink_narrowing_cast"),
    );
}

#[derive(Debug)]
pub(crate) struct TableSink {
    pub(super) table_provider: Arc<dyn TableProvider>,
    /// Additional indexes that receive write lifecycle hooks (`on_write_start`,
    /// `on_write_failed`, `on_write_complete`) but are **not** stored in the
    /// accelerator-side [`IndexLayer`].
    ///
    /// Used for external indexes (e.g. Elasticsearch) that must be maintained on
    /// the accelerator write path but must never be visible to the query optimizer.
    pub(super) sink_indexes: Vec<Arc<dyn Index + Send + Sync>>,
    /// The acceleration engine's own type rewrites, used to tell an engine-imposed
    /// type from a stale acceleration schema. Empty for engines that store the
    /// incoming types verbatim.
    pub(super) engine_type_rewrites: TypeRewriteRules,
    /// Names the dataset in the schema-cast diagnostics, and scopes the once-only
    /// engine-rewrite notice so two datasets of the same shape each get one.
    pub(super) dataset_name: String,
}

impl TableSink {
    pub fn new(table_provider: Arc<dyn TableProvider>, dataset_name: String) -> Self {
        Self {
            table_provider,
            sink_indexes: vec![],
            engine_type_rewrites: &[],
            dataset_name,
        }
    }

    pub fn with_sink_indexes(mut self, indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        self.sink_indexes = indexes;
        self
    }

    pub fn with_engine_type_rewrites(mut self, rules: TypeRewriteRules) -> Self {
        self.engine_type_rewrites = rules;
        self
    }

    async fn providers_for_write_hooks(&self) -> Vec<Arc<dyn TableProvider>> {
        if let Some(p) = self.table_provider.downcast_ref::<PartitionTableProvider>() {
            p.partition_table_providers().await
        } else {
            vec![Arc::clone(&self.table_provider)]
        }
    }

    pub async fn insert_into(
        &self,
        record_batch_stream: Pin<Box<dyn RecordBatchStream + Send>>,
        overwrite: InsertOp,
    ) -> Result<(), RetryError<crate::accelerated::Error>> {
        let start = std::time::Instant::now();
        tracing::debug!(
            "TableSink::insert_into starting (overwrite: {:?})",
            overwrite
        );

        let ctx = SessionContext::new();
        let target_schema = self.table_provider.schema();
        warn_on_narrowing_schema_cast(
            &self.dataset_name,
            &record_batch_stream.schema(),
            &target_schema,
            self.engine_type_rewrites,
        );
        let streaming_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(record_batch_stream));
        let cast_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(streaming_plan, target_schema));

        tracing::debug!("TableSink: calling table_provider.insert_into to create execution plan");
        let plan_start = std::time::Instant::now();
        let insertion_plan = match self
            .table_provider
            .insert_into(&ctx.state(), cast_plan, overwrite)
            .await
        {
            Ok(plan) => {
                tracing::debug!(
                    "TableSink: insert_into returned execution plan in {:.2}s",
                    plan_start.elapsed().as_secs_f64()
                );
                plan
            }
            Err(e) => {
                tracing::error!("TableSink: insert_into failed to create plan: {e}");
                // Should not retry if we are unable to create execution plan to insert data
                return Err(RetryError::permanent(
                    crate::accelerated::Error::FailedToWriteData {
                        source: find_datafusion_root(e),
                    },
                ));
            }
        };

        tracing::debug!(
            "TableSink: executing insertion plan with collect() - this will read source and write to accelerator"
        );

        let providers_before_write = self.providers_for_write_hooks().await;

        // Collect all indexes that need write-lifecycle hooks: those embedded in
        // an index layer on the accelerator and any extra sink_indexes (e.g.
        // Elasticsearch indexes maintained externally to the accelerator storage).
        let provider_indexes_before: Vec<Arc<dyn Index + Send + Sync>> = providers_before_write
            .iter()
            .filter_map(|p| p.downcast_ref::<SpiceTable>())
            .flat_map(SpiceTable::indexes)
            .map(Arc::clone)
            .collect();

        // A replacing write drops source rows by not re-sending them, so an index backed by its
        // own store has to be told to clear rather than upsert (#12066).
        prepare_indexes(
            "TableSink",
            provider_indexes_before
                .iter()
                .chain(self.sink_indexes.iter()),
            WriteWindow::from(overwrite),
        )
        .await
        .map_err(retry_from_df_error)?;

        let collect_start = std::time::Instant::now();
        if let Err(e) = collect(insertion_plan, ctx.task_ctx()).await {
            tracing::debug!(
                "TableSink: collect() failed after {:.2}s: {e}",
                collect_start.elapsed().as_secs_f64()
            );
            run_on_write_failed(&providers_before_write, &self.sink_indexes).await;
            return Err(retry_from_df_error(e));
        }

        // Perform post-write index maintenance (e.g., rebuild hash indexes) if the table supports it.
        // For partitioned tables each partition holds its own IndexedMemTable, so we iterate over
        // all partition providers after the insert has created any new partitions.
        let providers_after_write = self.providers_for_write_hooks().await;
        for provider in &providers_after_write {
            match perform_index_maintenance(provider.as_ref()).await {
                Ok(true) => {
                    tracing::debug!("TableSink: index maintenance completed successfully");
                }
                Ok(false) => {
                    // Table doesn't support index maintenance - this is expected for most tables
                }
                Err(e) => {
                    tracing::warn!(
                        "TableSink: index maintenance failed after write: {e}. Index may be stale until next refresh."
                    );
                    // Don't fail the write - data was successfully written, index rebuild is best-effort
                }
            }
        }

        // Call on_write_complete on every index in all providers and on sink_indexes.
        // Uses IF NOT EXISTS semantics: creates index after overwrite (new table),
        // no-op after append (index already exists). CDC skips this path entirely.
        let provider_indexes_after: Vec<Arc<dyn Index + Send + Sync>> = providers_after_write
            .iter()
            .filter_map(|p| p.downcast_ref::<SpiceTable>())
            .flat_map(SpiceTable::indexes)
            .map(Arc::clone)
            .collect();

        finalize_indexes(
            "TableSink",
            provider_indexes_after
                .iter()
                .chain(self.sink_indexes.iter()),
        )
        .await
        .map_err(retry_from_df_error)?;

        tracing::debug!(
            "TableSink::insert_into completed in {:.2}s (collect phase: {:.2}s)",
            start.elapsed().as_secs_f64(),
            collect_start.elapsed().as_secs_f64()
        );
        Ok(())
    }
}

async fn run_on_write_failed(
    providers: &[Arc<dyn TableProvider>],
    sink_indexes: &[Arc<dyn Index + Send + Sync>],
) {
    let provider_indexes: Vec<Arc<dyn Index + Send + Sync>> = providers
        .iter()
        .filter_map(|p| p.downcast_ref::<SpiceTable>())
        .flat_map(SpiceTable::indexes)
        .map(Arc::clone)
        .collect();

    rollback_indexes(
        "TableSink",
        provider_indexes.iter().chain(sink_indexes.iter()),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use cayenne::CAYENNE_TYPE_REWRITE_RULES;

    /// `DuckDB`'s normalization of a timezone-aware timestamp to microseconds — the
    /// engine rewrite these tests exercise. Spelled out rather than imported because
    /// the `DuckDB` accelerator sits above this crate.
    static DUCKDB_LIKE_RULES: TypeRewriteRules =
        &[&arrow_tools::type_rewrite::TimestampTzToMicrosecond];

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn narrowing_cast_detects_dropped_and_narrowed_columns() {
        let input = schema(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]);
        let target = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let changes = narrowing_schema_cast_changes(&input, &target, &[]);
        assert_eq!(changes.dropped, ["c".to_string()]);
        assert_eq!(changes.narrowed, ["a: Int64 -> Int32".to_string()]);
        assert!(changes.engine_normalized.is_empty());
    }

    #[test]
    fn narrowing_cast_ignores_widening_extra_target_columns_and_dictionary_unwrap() {
        let input = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "b",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        // Target widens `a`, stores `b` as plain Utf8, and carries an extra
        // accelerator-side column - none of these lose data on the write path.
        let target = schema(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]);

        let changes = narrowing_schema_cast_changes(&input, &target, &[]);
        assert!(changes.is_empty(), "{changes:?}");
    }

    /// `DuckDB`'s `TIMESTAMPTZ` is always microsecond-precision, so a Postgres
    /// `timestamptz` source column - inferred as `Timestamp(ns, "UTC")` - is stored
    /// and read back as µs. That cast is the engine's own, not the acceleration
    /// having fallen behind the source.
    #[test]
    fn engine_timestamp_rewrite_is_not_a_narrowing() {
        let input = schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]);
        let target = schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]);

        let changes = narrowing_schema_cast_changes(&input, &target, DUCKDB_LIKE_RULES);
        assert!(changes.dropped.is_empty(), "{:?}", changes.dropped);
        assert!(changes.narrowed.is_empty(), "{:?}", changes.narrowed);
        // Not silent: the cast still happens, so it is reported as the engine's own
        // normalization rather than as the acceleration being stale.
        assert_eq!(
            changes.engine_normalized,
            [r#"created_at: Timestamp(ns, "UTC") -> Timestamp(µs, "UTC")"#.to_string()]
        );

        // Without the engine's rules the same pair is a narrowing - which is what
        // makes the assertion above evidence rather than a schema that never differed.
        let without_rules = narrowing_schema_cast_changes(&input, &target, &[]);
        assert_eq!(
            without_rules.narrowed,
            [r#"created_at: Timestamp(ns, "UTC") -> Timestamp(µs, "UTC")"#.to_string()]
        );
        assert!(without_rules.engine_normalized.is_empty());
    }

    /// The rewrite rules must not swallow a real narrowing that happens to sit beside
    /// one they explain.
    #[test]
    fn engine_rewrites_do_not_suppress_a_genuine_narrowing() {
        let input = schema(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("count", DataType::Int64, false),
            Field::new("only_in_source", DataType::Utf8, true),
        ]);
        let target = schema(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("count", DataType::Int32, false),
        ]);

        let changes = narrowing_schema_cast_changes(&input, &target, DUCKDB_LIKE_RULES);
        assert_eq!(changes.dropped, ["only_in_source".to_string()]);
        assert_eq!(changes.narrowed, ["count: Int64 -> Int32".to_string()]);
        assert_eq!(
            changes.engine_normalized,
            [r#"ts: Timestamp(ns, "UTC") -> Timestamp(µs, "UTC")"#.to_string()]
        );
    }

    /// The rules descend into nested types, matching how the engine rewrites a schema.
    #[test]
    fn engine_rewrites_apply_inside_nested_types() {
        let nested = |unit: TimeUnit| {
            DataType::Struct(
                vec![Field::new(
                    "at",
                    DataType::Timestamp(unit, Some("UTC".into())),
                    true,
                )]
                .into(),
            )
        };
        // Nanosecond, not Second: Second -> Microsecond is a lossless widening, so it
        // never reaches the engine-rewrite check and the test would pass vacuously.
        let input = schema(vec![Field::new(
            "event",
            nested(TimeUnit::Nanosecond),
            true,
        )]);
        let target = schema(vec![Field::new(
            "event",
            nested(TimeUnit::Microsecond),
            true,
        )]);

        let changes = narrowing_schema_cast_changes(&input, &target, DUCKDB_LIKE_RULES);
        assert!(changes.dropped.is_empty(), "{:?}", changes.dropped);
        assert!(changes.narrowed.is_empty(), "{:?}", changes.narrowed);
        assert_eq!(changes.engine_normalized.len(), 1);
    }

    /// An engine only excuses the rewrites it actually performs. `DuckDB` keeps the
    /// precision of a timezone-naive timestamp, so a µs target for a ns source there
    /// is a real narrowing and must still be reported.
    #[test]
    fn an_engine_rewrite_it_does_not_perform_is_still_a_narrowing() {
        let input = schema(vec![Field::new(
            "naive",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        let target = schema(vec![Field::new(
            "naive",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);

        let changes = narrowing_schema_cast_changes(&input, &target, DUCKDB_LIKE_RULES);
        assert_eq!(
            changes.narrowed,
            ["naive: Timestamp(ns) -> Timestamp(µs)".to_string()]
        );
        assert!(changes.engine_normalized.is_empty());
    }

    /// Cayenne stores whatever timestamp unit its source reports, so a µs accelerated
    /// column under a ns source is a table created before that was true - a genuine
    /// narrowing the operator can clear by recreating the table, not something the
    /// engine imposes. Regression test for
    /// <https://github.com/spiceai/spiceai/issues/13018>.
    #[test]
    fn cayenne_does_not_excuse_a_timestamp_narrowing() {
        let ts = |unit: TimeUnit| DataType::Timestamp(unit, Some("UTC".into()));
        let input = schema(vec![Field::new("created_at", ts(TimeUnit::Nanosecond), true)]);
        let target = schema(vec![Field::new(
            "created_at",
            ts(TimeUnit::Microsecond),
            true,
        )]);

        let changes = narrowing_schema_cast_changes(&input, &target, CAYENNE_TYPE_REWRITE_RULES);
        assert_eq!(
            changes.narrowed,
            [r#"created_at: Timestamp(ns, "UTC") -> Timestamp(µs, "UTC")"#.to_string()]
        );
        assert!(
            changes.engine_normalized.is_empty(),
            "{:?}",
            changes.engine_normalized
        );
    }
}
