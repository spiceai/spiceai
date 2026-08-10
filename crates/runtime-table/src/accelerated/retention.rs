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

use std::{sync::Arc, time::SystemTime};

use crate::accelerated::{
    DataRetentionFilter, Retention, refresh, refresh_task::collect_all_indexes,
};
use crate::federated::FederatedTable;
use crate::filter_converter::{TimestampFilterConvert, create_timestamp_filter_convert};
use arrow::array::{RecordBatch, UInt64Array};
use cache::Caching;
use datafusion::{
    catalog::TableProvider,
    logical_expr::Operator,
    physical_plan::collect,
    prelude::{Expr, SessionContext},
    sql::TableReference,
};
use runtime_component::dataset::TimeFormat;
use runtime_datafusion::{is_spice_internal_dataset, session_config::get_df_default_config};
use spice_table::{Index, LayerWalk, peel_to, resolve_keys_matching_predicate};
use runtime_object_store::registry::default_runtime_env;
use search::index::compound::{CompoundSearchIndex, CompoundVectorIndex};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

/// Peels to the provider a retention delete must execute against, bypassing
/// `IndexLayer::delete_from`'s index-aware handling — retention needs the
/// *warm-only* index scope (see [`warm_delete_target`]), a different policy from
/// that generic path's full/both-scope delete.
///
/// Each layer decides for itself whether [`LayerWalk::RetentionDelete`] may see
/// past it, so a layer with delete semantics of its own — or one redirecting to
/// the source side, which would send the delete to the wrong table entirely —
/// stops the peel rather than being silently skipped here.
fn strip_index_wrapper_layers(tbl: &Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    Arc::clone(peel_to(tbl, LayerWalk::RetentionDelete))
}

/// Resolves the index a retention delete should actually hit: for a compound (fallback-composed)
/// index, only the primary/warm side — retention trims the local warm window and must not reach
/// into the independently-managed secondary/fallback index's own lifecycle. `SearchIndex` itself
/// has no notion of "warm" (that's a compound-specific concept), so this downcasts explicitly
/// rather than dispatching through the trait. Every other index has just one backing store, so
/// the index itself is the target.
fn warm_delete_target(index: &Arc<dyn Index + Send + Sync>) -> Arc<dyn Index + Send + Sync> {
    if let Some(compound) = index.as_any().downcast_ref::<CompoundSearchIndex>() {
        return Arc::clone(compound.primary()) as Arc<dyn Index + Send + Sync>;
    }
    if let Some(compound) = index.as_any().downcast_ref::<CompoundVectorIndex>() {
        return Arc::clone(compound.primary()) as Arc<dyn Index + Send + Sync>;
    }
    Arc::clone(index)
}

pub(crate) async fn apply_retention_filters_once(
    dataset_name: &TableReference,
    accelerator: &Arc<dyn TableProvider>,
    federated: &FederatedTable,
    expr: Expr,
    io_runtime: &Handle,
) -> datafusion::error::Result<u64> {
    tracing::trace!("[retention] Expr before simplification: {expr:?}");

    let original_expr = format!("{expr:?}");
    let expr = match util::expr::simplify_expr(expr, &accelerator.schema()) {
        Ok(expr) => expr,
        Err(e) => {
            tracing::error!(
                "[retention] Upon checking retention policy for table '{dataset_name}', an error occurred when attempting to simplify the relevant retention expression '{original_expr}'. Error: {e}"
            );
            return Err(e);
        }
    };

    tracing::debug!("[retention] Expr: {expr:?}");

    let ctx = SessionContext::new_with_config_rt(
        get_df_default_config(),
        default_runtime_env(io_runtime.clone()),
    );

    // Resolve each attached index's matching rows (projected to the warm-delete target's own
    // required columns) *before* the delete below runs — the predicate is evaluated by scanning
    // `accelerator`'s current (pre-delete) rows, so there is nothing left to resolve once they're
    // gone. The actual per-index delete happens after the accelerator delete succeeds
    // (best-effort, see below).
    let indexes = collect_all_indexes(accelerator, federated);
    let mut warm_deletes: Vec<(Arc<dyn Index + Send + Sync>, RecordBatch)> = Vec::new();
    for index in &indexes {
        let target = warm_delete_target(index);
        match resolve_keys_matching_predicate(
            accelerator,
            &ctx.state(),
            vec![expr.clone()],
            &target.required_columns(),
        )
        .await
        {
            Ok(keys) if keys.num_rows() > 0 => warm_deletes.push((target, keys)),
            Ok(_) => {}
            Err(e) => tracing::warn!(
                "[retention] Failed to resolve keys for index '{}' (skipping its warm-index cleanup this cycle): {e}",
                target.name()
            ),
        }
    }

    let raw_accelerator = strip_index_wrapper_layers(accelerator);
    let plan = match raw_accelerator.delete_from(&ctx.state(), vec![expr]).await {
        Ok(plan) => plan,
        Err(e) => {
            tracing::error!("[retention] Error running retention check: {e}");
            return Err(e);
        }
    };

    let deleted = match collect(plan, ctx.task_ctx()).await {
        Ok(deleted) => deleted,
        Err(e) => {
            tracing::error!("[retention] Error running retention check: {e}");
            return Err(e);
        }
    };

    let num_records = deleted.first().map_or(0, |f| {
        f.column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map_or(0, |v| v.values().first().copied().unwrap_or(0))
    });

    for (target, keys) in warm_deletes {
        if let Err(e) = target.delete_by_keys(keys).await {
            tracing::error!(
                "[retention] Index '{}' failed to delete warm entries (best-effort, continuing): {e}",
                target.name()
            );
        }
    }

    log_retention_result(dataset_name, num_records);

    Ok(num_records)
}

impl super::AcceleratedTable {
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) async fn start_retention_check(
        dataset_name: TableReference,
        accelerator: Arc<dyn TableProvider>,
        federated: Arc<FederatedTable>,
        retention: Retention,
        caching: Option<Arc<Caching>>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) {
        let mut interval_timer = tokio::time::interval(retention.check_interval);
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut nullable_time_column_warning_emitted = false;

        loop {
            interval_timer.tick().await;

            // Lock the accelerator to protect concurrent access to the accelerator during cache/snapshot operations
            let _lock_guard = accelerator_write_mutex.lock().await;

            let mut exprs = Vec::new();

            // convert retention filters into data eviction expressions
            for filter in &retention.filters {
                match filter {
                    DataRetentionFilter::Expression { delete_expr } => {
                        log_retention_action(&dataset_name, "using SQL expression");
                        exprs.push(delete_expr.clone());
                    }
                    DataRetentionFilter::Time {
                        period,
                        time_column,
                        time_format,
                        time_partition_column,
                        time_partition_format,
                    } => {
                        let Some(converter) = create_timestamp_filter_converter(
                            &accelerator,
                            time_column,
                            *time_format,
                            time_partition_column.as_ref(),
                            *time_partition_format,
                        ) else {
                            tracing::error!(
                                "[retention] Failed to create timestamp filter converter for retention for dataset {dataset_name}",
                            );
                            continue;
                        };

                        // ACID / correctness warning for nullable time columns.
                        // `timestamp < cutoff` (and any <, >, = comparison) evaluates to NULL (false)
                        // when the timestamp is NULL. Therefore, rows with NULL in the retention
                        // time_column are *never* deleted by a time-based policy. This can lead to
                        // unbounded table growth if the source produces NULL-timestamp data that
                        // the user expects to be eventually cleaned up.
                        // We surface this explicitly so operators are not surprised by "leaky"
                        // retention. For full control, users can use an Expression filter with
                        // explicit NULL handling (e.g. `time < cutoff OR time IS NULL`).
                        if !nullable_time_column_warning_emitted
                            && accelerator
                                .schema()
                                .column_with_name(time_column)
                                .is_some_and(|(_, f)| f.is_nullable())
                        {
                            tracing::warn!(
                                "[retention] time_column '{time_column}' for dataset {dataset_name} is nullable. \
                                 Rows with NULL in this column will never satisfy `time < cutoff` and will not be deleted by retention. \
                                 This can cause the accelerated table to grow without bound if the source emits NULL timestamps. \
                                 Consider making the time column non-nullable or using a custom Expression retention filter."
                            );
                            nullable_time_column_warning_emitted = true;
                        }

                        let start = SystemTime::now() - *period;
                        let timestamp = refresh::get_timestamp(start);
                        let expr = converter.convert(timestamp, Operator::Lt);

                        let timestamp = if let Some(value) =
                            chrono::DateTime::from_timestamp((timestamp / 1_000_000_000) as i64, 0)
                        {
                            value.to_rfc3339()
                        } else {
                            tracing::warn!("[retention] Unable to convert timestamp");
                            continue;
                        };

                        log_retention_action(
                            &dataset_name,
                            &format!("where {time_column} < {timestamp}"),
                        );
                        exprs.push(Box::new(expr));
                    }
                }
            }

            // Combine all expressions into a single OR expression as time and SQL expressions are applied independently
            let Some(expr) = exprs.into_iter().map(|e| *e).reduce(Expr::or) else {
                tracing::warn!(
                    "[retention] No valid retention filters found for dataset {dataset_name}"
                );
                continue;
            };

            if let Ok(num_records) = apply_retention_filters_once(
                &dataset_name,
                &accelerator,
                &federated,
                expr,
                &io_runtime,
            )
            .await
                && num_records > 0
                && let Some(cache_provider) = caching.as_ref()
                && let Err(e) = cache_provider.invalidate_for_table(dataset_name.clone())
            {
                tracing::error!(
                    "Failed to invalidate cached results for dataset {}: {e}",
                    &dataset_name
                );
            }
        }
    }
}

fn create_timestamp_filter_converter(
    accelerator: &Arc<dyn TableProvider>,
    time_column: &str,
    time_format: Option<TimeFormat>,
    time_partition_column: Option<&String>,
    time_partition_format: Option<TimeFormat>,
) -> Option<TimestampFilterConvert> {
    let schema = accelerator.schema();
    let field = schema.column_with_name(time_column).map(|(_, f)| f);
    let partition_field = time_partition_column
        .as_ref()
        .and_then(|time_partition_column| {
            schema
                .column_with_name(time_partition_column.as_str())
                .map(|(_, f)| f)
        });

    create_timestamp_filter_convert(
        field.cloned(),
        Some(time_column.to_string()),
        time_format,
        partition_field.cloned(),
        time_partition_column.cloned(),
        time_partition_format,
    )
}

fn log_retention_action(dataset_name: &TableReference, filter_msg: &str) {
    let msg = format!("[retention] Evicting data for {dataset_name} {filter_msg}");
    if is_spice_internal_dataset(dataset_name) {
        tracing::trace!("{msg}");
    } else {
        tracing::info!("{msg}");
    }
}

fn log_retention_result(dataset_name: &TableReference, num_records: u64) {
    let message = format!("[retention] Evicted {num_records} records for {dataset_name}");

    if is_spice_internal_dataset(dataset_name) {
        tracing::trace!("{message}");
    } else {
        tracing::info!("{message}");
    }
}

#[cfg(test)]
mod tests {
    use crate::accelerated::AcceleratedTable;

    use super::*;
    use arrow::{
        array::{BooleanArray, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use data_components::arrow::write::MemTable;
    use datafusion::{catalog::TableProvider, physical_plan::collect, prelude::SessionContext};
    use tokio::time::{Duration, sleep};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("deleted", DataType::Boolean, true),
            Field::new("created_at", DataType::Utf8, true),
            Field::new("timestamp_col", DataType::Int64, false), // Unix timestamp in seconds
        ]))
    }

    fn create_test_data() -> RecordBatch {
        let schema = create_test_schema();

        // Create test data with different timestamps (some old, some recent)
        #[expect(clippy::cast_possible_wrap)]
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("to get current time")
            .as_secs() as i64;

        let old_timestamp = now_secs - (20 * 24 * 60 * 60); // 20 days ago
        let recent_timestamp = now_secs - (5 * 24 * 60 * 60); // 5 days ago
        let very_recent_timestamp = now_secs - (24 * 60 * 60); // 1 day ago

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let is_deleted_array = BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let created_at_array = StringArray::from(vec![
            Some("2023-01-01"),
            Some("2023-02-01"),
            Some("2023-03-01"),
            Some("2023-04-01"),
            Some("2023-05-01"),
        ]);
        let timestamp_array = Int64Array::from(vec![
            old_timestamp,
            old_timestamp,
            recent_timestamp,
            recent_timestamp,
            very_recent_timestamp,
        ]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(is_deleted_array),
                Arc::new(created_at_array),
                Arc::new(timestamp_array),
            ],
        )
        .expect("Failed to create test batch")
    }

    async fn test_retention_scenario(
        retention_sql: Option<&str>,
        retention_period: Option<Duration>,
        time_column: Option<&str>,
        expected_remaining_count: usize,
    ) {
        let batch = create_test_data();
        let schema = batch.schema();
        let initial_count = batch.num_rows();

        let mem_table =
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created");

        let accelerator = Arc::new(mem_table) as Arc<dyn TableProvider>;
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));

        // Create retention configuration
        let retention_delete_expr = retention_sql.map(|sql| {
            runtime_datafusion::retention_sql::parse_retention_sql(
                &TableReference::bare("test"),
                sql,
                accelerator.schema(),
            )
            .expect("Failed to parse retention SQL")
            .delete_expr
        });

        let retention = Retention::builder()
            .time_column(time_column.map(String::from))
            .time_format(Some(runtime_component::dataset::TimeFormat::UnixSeconds))
            .time_period(retention_period)
            .check_interval(Some(Duration::from_millis(100))) // Very short check interval for testing
            .enabled(true)
            .delete_expr(retention_delete_expr)
            .build()
            .expect("Retention should be configured");

        let dataset_name = TableReference::bare("test");
        let caching = None;

        // Start retention check in background
        let retention_task = tokio::spawn(AcceleratedTable::start_retention_check(
            dataset_name.clone(),
            Arc::clone(&accelerator),
            Arc::clone(&federated),
            retention,
            caching,
            Handle::current(),
            Arc::new(Mutex::new(())),
        ));

        // Wait for retention to run
        sleep(Duration::from_millis(500)).await;

        // Verify the result
        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = accelerator
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        let remaining_count: usize = result.iter().map(RecordBatch::num_rows).sum();

        assert_eq!(
            remaining_count, expected_remaining_count,
            "Test failed: Initial count: {initial_count}, Expected: {expected_remaining_count}, Actual: {remaining_count}"
        );

        retention_task.abort();
    }

    #[tokio::test]
    async fn test_retention_sql_only() {
        // Retention SQL only - delete records where deleted = true
        test_retention_scenario(
            Some("DELETE FROM test WHERE deleted = true"),
            None,
            None,
            3, // Should remain 3 records (where deleted = false)
        )
        .await;
    }

    #[tokio::test]
    async fn test_retention_period_only() {
        // Retention period only - delete records older than 10 days
        test_retention_scenario(
            None,
            Some(Duration::from_hours(240)), // 10 days
            Some("timestamp_col"),
            3, // Should remain 3 records (recent ones)
        )
        .await;
    }

    #[tokio::test]
    async fn test_retention_sql_and_period_sql() {
        // Both retention SQL and period - delete old records AND where deleted = true
        test_retention_scenario(
            Some("DELETE FROM test WHERE deleted = true"),
            Some(Duration::from_hours(240)),
            Some("timestamp_col"),
            2, // Should remain 2 records (where deleted = false and recent)
        )
        .await;
    }

    #[tokio::test]
    async fn test_retention_very_short_period() {
        // Very short retention period - delete almost everything
        test_retention_scenario(
            None,
            Some(Duration::from_hours(48)), // 2 days
            Some("timestamp_col"),
            1, // Should remain 1 record (very recent)
        )
        .await;
    }

    #[tokio::test]
    async fn test_retention_complex_sql() {
        // Complex retention SQL - delete records that are both old and marked as deleted
        test_retention_scenario(
                Some("DELETE FROM test WHERE deleted = true AND timestamp_col < to_unixtime(NOW() - INTERVAL '7 days')"),
                None,
                None,
                4, // Should remain 4 records (keeping non-deleted and recent deleted ones)
            )
            .await;
    }

    #[tokio::test]
    async fn test_retention_sql_never_matches() {
        // Retention SQL that never matches - nothing should be deleted
        test_retention_scenario(
            Some("DELETE FROM test WHERE created_at < '2023-01-01'"),
            None,
            None,
            5, // Should remain all 5 records
        )
        .await;
    }

    #[test]
    fn test_strip_index_wrapper_layers_unwraps_all_known_layers() {
        use data_components::MetadataEnrichedTableProvider;
        use spice_table::IndexLayer;

        let mem_table: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(create_test_schema(), vec![]).expect("mem table should be created"),
        );

        // IndexLayer alone.
        let wrapped: Arc<dyn TableProvider> =
            Arc::new(IndexLayer::new(Arc::clone(&mem_table)));
        assert!(
            strip_index_wrapper_layers(&wrapped)
                .downcast_ref::<MemTable>()
                .is_some(),
            "stripping an IndexLayer must yield the underlying MemTable"
        );

        // MetadataEnrichedTableProvider nested inside an IndexLayer (the shape
        // `table_provider_with_spicepod_metadata` produces).
        let metadata_enriched: Arc<dyn TableProvider> =
            Arc::new(MetadataEnrichedTableProvider::new(
                Arc::clone(&mem_table),
                std::collections::HashMap::from([("key".to_string(), "value".to_string())]),
            ));
        let wrapped_with_metadata: Arc<dyn TableProvider> =
            Arc::new(IndexLayer::new(metadata_enriched));
        assert!(
            strip_index_wrapper_layers(&wrapped_with_metadata)
                .downcast_ref::<MemTable>()
                .is_some(),
            "stripping must peel through a nested MetadataEnrichedTableProvider layer"
        );

        // A provider with no wrapper layers is returned unchanged.
        assert!(
            strip_index_wrapper_layers(&mem_table)
                .downcast_ref::<MemTable>()
                .is_some()
        );
    }
}
