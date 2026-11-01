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
    DataRetentionFilter, Retention, exact_string_time::pin_string_time_comparisons, refresh,
    refresh_task::collect_all_indexes,
};
use crate::federated::FederatedTable;
use crate::filter_converter::{TimestampFilterConvert, create_timestamp_filter_convert};
use arrow::array::{RecordBatch, UInt64Array};
use cache::Caching;
use datafusion::{
    catalog::TableProvider,
    common::TableReference,
    logical_expr::Operator,
    physical_plan::collect,
    prelude::{Expr, SessionContext},
};
use runtime_component::dataset::TimeFormat;
use runtime_datafusion::{is_spice_internal_dataset, session_config::get_df_default_config};
use runtime_object_store::registry::default_runtime_env;
use search::index::compound::{CompoundSearchIndex, CompoundVectorIndex};
use spice_table::{Index, LayerWalk, peel_to, resolve_keys_matching_predicate};
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

/// Whether `provider` (already peeled to its retention-delete target) evaluates a
/// delete's filter itself: a Cayenne table, or a partitioned table whose partitions
/// are Cayenne tables.
///
/// The Cayenne accelerator sits beneath a `PolyTableProvider` read/write split, which
/// stops every walk but the write walk, so the write walk finds the table the delete
/// reaches.
fn deletes_evaluate_with_datafusion(provider: &Arc<dyn TableProvider>) -> bool {
    let provider: &dyn std::any::Any = peel_to(provider, LayerWalk::Write).as_ref();
    provider.is::<cayenne::CayenneTableProvider>()
        || provider
            .downcast_ref::<runtime_table_partition::provider::PartitionTableProvider>()
            .is_some_and(|partitioned| {
                let creator: &dyn std::any::Any = partitioned.creator().as_ref();
                creator.is::<cayenne::CayennePartitionCreator>()
            })
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
    // An accelerator that renders the delete's filter into its own SQL parses a
    // string time column differently than `DataFusion`; pin each such comparison to
    // the strings `DataFusion` selects first (see `exact_string_time`). Cayenne
    // evaluates the filter itself, in each data file's Vortex scan or with `DataFusion`
    // where Vortex cannot express it, and its scans hide the rows its time retention
    // expires, so pinning there would find nothing to delete.
    let predicates = if deletes_evaluate_with_datafusion(&raw_accelerator) {
        vec![expr]
    } else {
        match pin_string_time_comparisons(accelerator, &ctx.state(), expr).await {
            Ok(predicates) => predicates,
            Err(e) => {
                tracing::error!("[retention] Error running retention check: {e}");
                return Err(e);
            }
        }
    };

    let mut num_records = 0;
    for predicate in predicates {
        let plan = match raw_accelerator
            .delete_from(&ctx.state(), vec![predicate])
            .await
        {
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

        num_records += deleted.first().map_or(0, |f| {
            f.column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map_or(0, |v| v.values().first().copied().unwrap_or(0))
        });
    }

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
            let configured = exprs.into_iter().map(|e| *e).reduce(Expr::or);

            let expr = if let Some(predicate) = retention.computed.as_ref() {
                match predicate.delete_expr(&accelerator, configured).await {
                    // Nothing to remove this tick is the ordinary case for a
                    // budget that is not yet exceeded, so it is not a warning.
                    Ok(None) => continue,
                    Ok(Some(expr)) => expr,
                    Err(e) => {
                        tracing::warn!(
                            "[retention] Failed to resolve what to delete for dataset {dataset_name}, so it keeps whatever is past its retention until the next check. Cause: {e}"
                        );
                        continue;
                    }
                }
            } else {
                let Some(expr) = configured else {
                    tracing::warn!(
                        "[retention] No valid retention filters found for dataset {dataset_name}"
                    );
                    continue;
                };
                expr
            };

            // Taken only around the delete. Resolving the predicate above can
            // query the accelerator, and holding this across that query would
            // stall every write for its duration.
            let _lock_guard = accelerator_write_mutex.lock().await;

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
                && let Err(e) = cache_provider
                    .invalidate_for_refresh(dataset_name.clone())
                    .await
            {
                tracing::error!(
                    "Failed to invalidate cached query plans and search results for dataset {} after retention, so they may reflect the previous data until they expire: {e}",
                    &dataset_name
                );
            }
        }
    }
}

pub(crate) fn create_timestamp_filter_converter(
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

        let retention = Retention::builder("test")
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

    #[tokio::test]
    async fn retention_sql_eviction_count_is_the_number_of_deleted_rows() {
        let batch = create_test_data();
        let schema = batch.schema();
        let mem_table =
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created");
        let accelerator = Arc::new(mem_table) as Arc<dyn TableProvider>;
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        let delete_expr = runtime_datafusion::retention_sql::parse_retention_sql(
            &TableReference::bare("test"),
            "DELETE FROM test WHERE deleted = true",
            accelerator.schema(),
        )
        .expect("parse retention SQL")
        .delete_expr;

        let deleted = apply_retention_filters_once(
            &TableReference::bare("test"),
            &accelerator,
            &federated,
            delete_expr,
            &Handle::current(),
        )
        .await
        .expect("retention delete");

        assert_eq!(
            deleted, 2,
            "two fixture rows have deleted = true; the logged eviction count must be that number"
        );
    }

    /// Cayenne's scans hide the rows its time retention expires. Retention must still
    /// physically delete an expired ISO8601 string row there, which pinning the
    /// comparison through such a scan would turn into a delete of nothing; the table is
    /// wrapped as the Cayenne accelerator wraps it, so the check that skips pinning
    /// sees what it sees in `spiced`. The table has no primary key, so it deletes by
    /// row position: from a data file, where Vortex cannot evaluate the comparison's
    /// cast and `DataFusion` does, and from the rows still held inline.
    #[tokio::test]
    async fn deletes_an_expired_string_time_row_from_a_cayenne_table_with_time_retention() {
        use cayenne::metadata::{CreateTableOptions, VortexConfig};
        use cayenne::{
            CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog,
            TimeRetentionFilterBuilder,
        };
        use datafusion::{datasource::memory::MemorySourceConfig, logical_expr::dml::InsertOp};
        use util::timestamp_filter::{TimestampFilterConvert, data_type_to_timestamp_format};

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog initialized");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Utf8, false),
        ]));
        let retention =
            TimeRetentionFilterBuilder::try_new("ts", 86_400, &schema).expect("retention builder");
        let ctx = SessionContext::new();
        let cayenne = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_time_retention_filter_builder(retention)
            .create(CreateTableOptions {
                table_name: "string_time_retention".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: temp_dir.path().join("data").display().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("Cayenne table");
        let insert = async |ids: Vec<i64>, times: Vec<&str>| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(times)),
                ],
            )
            .expect("batch");
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("input");
            let insert = cayenne
                .insert_into(&ctx.state(), input, InsertOp::Append)
                .await
                .expect("insert plan");
            collect(insert, ctx.task_ctx()).await.expect("insert");
        };
        // One expired and one live row in a Vortex data file, and another pair inline.
        insert(
            vec![1, 2],
            vec!["2021-05-01T00:00:00Z", "9999-01-01T00:00:00Z"],
        )
        .await;
        cayenne
            .checkpoint_inlined_data()
            .await
            .expect("the first rows are flushed to a data file");
        insert(
            vec![3, 4],
            vec!["2021-05-01T09:00:00+09:00", "9999-01-01T00:00:00-07:00"],
        )
        .await;

        let accelerator = cayenne_accelerator(cayenne);
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        let format = data_type_to_timestamp_format(&DataType::Utf8, None).expect("ISO8601");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None);
        let cutoff = refresh::get_timestamp(SystemTime::now() - Duration::from_hours(24));
        let retention_pass = async || {
            apply_retention_filters_once(
                &TableReference::bare("string_time_retention"),
                &accelerator,
                &federated,
                converter.convert(cutoff, Operator::Lt),
                &Handle::current(),
            )
            .await
            .expect("retention delete")
        };

        assert_eq!(
            retention_pass().await,
            2,
            "both 2021 rows are past the one-day retention and must be deleted, not only hidden: the one in the data file and the inline one"
        );
        assert_eq!(
            retention_pass().await,
            0,
            "a row retention already deleted is not deleted again"
        );
    }

    /// Deletes from a Cayenne table without a primary key delete exactly the rows an
    /// in-test oracle selects: over several data files of several chunks each, after an
    /// earlier delete left a deletion vector on them, with NULLs in the filter columns,
    /// and with `-0.0` and `0.0`, which SQL holds equal and Vortex does not. Only the
    /// conjuncts the read path would push down go into each file's scan, and every
    /// position comes from `row_idx()`, so a pruned chunk or an excluded row cannot
    /// shift the positions the deletion vector records.
    #[tokio::test]
    async fn cayenne_deletes_by_a_split_filter_match_an_oracle() {
        use cayenne::metadata::{CreateTableOptions, VortexConfig};
        use cayenne::{CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog};
        use datafusion::arrow::array::{Array, Float64Array};
        use datafusion::logical_expr::{col, dml::InsertOp, lit};
        use datafusion::{datasource::memory::MemorySourceConfig, prelude::Expr};

        #[derive(Clone, Copy)]
        struct Row {
            id: i64,
            v: Option<i64>,
            a: f64,
        }
        let rows: Vec<Row> = (0..120_000_i64)
            .map(|id| Row {
                id,
                v: (id % 7 != 0).then_some(id % 11),
                // `-0.0` on one row in five, a real difference from `b = 0.0` on another.
                a: match id % 5 {
                    0 => -0.0,
                    1 => 1.0,
                    _ => 0.0,
                },
            })
            .collect();

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog initialized");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let ctx = SessionContext::new();
        let cayenne = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .create(CreateTableOptions {
                table_name: "split_delete".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: temp_dir.path().join("data").display().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("Cayenne table");
        // Three data files of 40,000 rows.
        for file in rows.chunks(40_000) {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(file.iter().map(|r| r.id))),
                    Arc::new(Int64Array::from(
                        file.iter().map(|r| r.v).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from_iter_values(file.iter().map(|r| r.a))),
                    Arc::new(Float64Array::from_iter_values(file.iter().map(|_| 0.0))),
                ],
            )
            .expect("batch");
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("input");
            let insert = cayenne
                .insert_into(&ctx.state(), input, InsertOp::Append)
                .await
                .expect("insert plan");
            collect(insert, ctx.task_ctx()).await.expect("insert");
            cayenne
                .checkpoint_inlined_data()
                .await
                .expect("flush to a data file");
        }

        let mut live: Vec<Row> = rows.clone();
        let mut delete = async |filter: Expr, matches: &dyn Fn(&Row) -> bool| {
            let expected = live.iter().filter(|r| matches(r)).count();
            let plan = cayenne
                .delete_from(&ctx.state(), vec![filter.clone()])
                .await
                .expect("delete plan");
            let deleted = collect(plan, ctx.task_ctx()).await.expect("delete");
            let deleted = deleted[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("count")
                .value(0);
            assert_eq!(
                usize::try_from(deleted).expect("fits"),
                expected,
                "{filter} deletes the rows the oracle selects"
            );
            live.retain(|r| !matches(r));
        };

        // Vortex evaluates this one whole and leaves a deletion vector on the first file.
        delete(
            col("id")
                .gt_eq(lit(1_000_i64))
                .and(col("id").lt(lit(30_000_i64))),
            &|r| (1_000..30_000).contains(&r.id),
        )
        .await;
        // `id >= 20000` goes into the scan and `DataFusion` evaluates the arithmetic,
        // which is NULL, and deletes nothing, where `v` is NULL. Rows the first delete
        // took are not deleted twice.
        delete(
            col("id")
                .gt_eq(lit(20_000_i64))
                .and((col("v") % lit(3_i64)).eq(lit(0_i64))),
            &|r| r.id >= 20_000 && r.v.is_some_and(|v| v % 3 == 0),
        )
        .await;
        // SQL holds `-0.0` and `0.0` equal, so `a <> b` is false on those rows, which
        // Vortex, comparing floats by their bits, would delete: the comparison stays
        // in `DataFusion`, beside a conjunct Vortex cannot express and on its own.
        delete(
            col("a")
                .not_eq(col("b"))
                .and((col("id") % lit(2_i64)).eq(lit(0_i64))),
            &|r| r.a.abs() > 0.0 && r.id % 2 == 0,
        )
        .await;
        delete(col("a").not_eq(col("b")), &|r| r.a.abs() > 0.0).await;

        let plan = cayenne
            .scan(&ctx.state(), Some(&vec![0]), &[], None)
            .await
            .expect("scan plan");
        let mut survivors: Vec<i64> = collect(plan, ctx.task_ctx())
            .await
            .expect("scan")
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ids");
                (0..ids.len()).map(|i| ids.value(i)).collect::<Vec<_>>()
            })
            .collect();
        survivors.sort_unstable();
        let expected: Vec<i64> = live.iter().map(|r| r.id).collect();
        let first_difference = survivors
            .iter()
            .zip(&expected)
            .position(|(survivor, expected)| survivor != expected);
        assert_eq!(
            (survivors.len(), first_difference),
            (expected.len(), None),
            "the surviving rows are exactly the oracle's (count, first differing position)"
        );
    }

    /// Wall time of a retention check on a Cayenne table without a primary key whose
    /// time column is ISO 8601 text, over `RETENTION_BENCH_FILES` (20) data files of
    /// `RETENTION_BENCH_ROWS` (50000) rows, for two layouts:
    ///
    /// - spread: one-day retention, 1% of each file's rows expired, so no file can be
    ///   skipped — the worst case for pruning;
    /// - ordered: thirty-day retention over rows appended in time order across 31 days,
    ///   so only the oldest files hold expired rows.
    ///
    /// Each layout's first pass must delete exactly the expired rows. Then each of
    /// `RETENTION_BENCH_REPS` (1000) repetitions times three delete filters, in an
    /// order that rotates: the plain `ts < '<cutoff text>'` that takes Cayenne's
    /// file-based path (statistics only), the instant comparison alone, and the text
    /// bound joined to it by `AND`, as retention builds it. Prints P50/P99/max per filter. Run
    /// with `cargo test --release -p runtime-table --lib -- --ignored --nocapture
    /// retention_check_cost_on_iso_text`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "benchmark: run explicitly with --ignored"]
    async fn retention_check_cost_on_iso_text() {
        let env = |name: &str, default: usize| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };
        let reps = env("RETENTION_BENCH_REPS", 1000);
        let files = env("RETENTION_BENCH_FILES", 20);
        let rows_per_file = env("RETENTION_BENCH_ROWS", 50_000);
        for layout in [RetentionBenchLayout::Spread, RetentionBenchLayout::Ordered] {
            time_retention_checks(layout, reps, files, rows_per_file).await;
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum RetentionBenchLayout {
        Spread,
        Ordered,
    }

    async fn time_retention_checks(
        layout: RetentionBenchLayout,
        reps: usize,
        files: usize,
        rows_per_file: usize,
    ) {
        use cayenne::metadata::{CreateTableOptions, VortexConfig};
        use cayenne::{
            CayenneCatalog, CayenneTableProviderBuilder, MetadataCatalog,
            TimeRetentionFilterBuilder,
        };
        use datafusion::{datasource::memory::MemorySourceConfig, logical_expr::dml::InsertOp};
        use util::timestamp_filter::{TimestampFilterConvert, data_type_to_timestamp_format};

        const DAY: i64 = 86_400;
        let retention_days: i64 = match layout {
            RetentionBenchLayout::Spread => 1,
            RetentionBenchLayout::Ordered => 30,
        };
        let total_rows = i64::try_from(files * rows_per_file).expect("fits");
        let now = chrono::Utc::now().timestamp();
        let cutoff_secs = now - retention_days * DAY;
        // Seconds before `now` of row `id`.
        let seconds_ago = |id: i64| match layout {
            // One row in a hundred is two days old; the rest fall within 23 hours.
            RetentionBenchLayout::Spread if id % 100 == 0 => 2 * DAY,
            RetentionBenchLayout::Spread => id % 82_800,
            // Evenly spaced from 31 days ago to now, oldest first.
            RetentionBenchLayout::Ordered => 31 * DAY - id * 31 * DAY / total_rows,
        };

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = temp_dir.path().join("metadata");
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog initialized");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Utf8, false),
        ]));
        let retention = TimeRetentionFilterBuilder::try_new(
            "ts",
            u64::try_from(retention_days * DAY).expect("positive"),
            &schema,
        )
        .expect("retention builder");
        let ctx = SessionContext::new();
        let cayenne = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_time_retention_filter_builder(retention)
            .create(CreateTableOptions {
                table_name: "retention_bench".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: temp_dir.path().join("data").display().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("Cayenne table");

        let mut expired = 0_u64;
        let mut id = 0_i64;
        for _ in 0..files {
            let mut ids = Vec::with_capacity(rows_per_file);
            let mut times = Vec::with_capacity(rows_per_file);
            for _ in 0..rows_per_file {
                let time = now - seconds_ago(id);
                if time < cutoff_secs {
                    expired += 1;
                }
                ids.push(id);
                times.push(
                    chrono::DateTime::from_timestamp(time, 0)
                        .expect("in range")
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string(),
                );
                id += 1;
            }
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(times)),
                ],
            )
            .expect("batch");
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
                .expect("input");
            let insert = cayenne
                .insert_into(&ctx.state(), input, InsertOp::Append)
                .await
                .expect("insert plan");
            collect(insert, ctx.task_ctx()).await.expect("insert");
            cayenne
                .checkpoint_inlined_data()
                .await
                .expect("flush to a data file");
        }
        let data_files = walk_count_files(&temp_dir.path().join("data"), "vortex");

        let accelerator = cayenne_accelerator(cayenne);
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        let format = data_type_to_timestamp_format(&DataType::Utf8, None).expect("ISO8601");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None);
        let cutoff = u128::try_from(cutoff_secs).expect("after 1970") * 1_000_000_000;
        let conjoined = converter.convert(cutoff, Operator::Lt);
        let Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            op: Operator::And,
            right: instant,
            ..
        }) = &conjoined
        else {
            panic!("retention ANDs a text bound with the instant comparison: {conjoined}");
        };
        let instant = instant.as_ref().clone();
        let text = datafusion::prelude::col("ts").lt(datafusion::prelude::lit(
            util::timestamp_filter::nanos_to_iso8601_string(cutoff),
        ));
        let check = async |filter: &Expr| {
            let started = std::time::Instant::now();
            let deleted = apply_retention_filters_once(
                &TableReference::bare("retention_bench"),
                &accelerator,
                &federated,
                filter.clone(),
                &Handle::current(),
            )
            .await
            .expect("retention check");
            (started.elapsed(), deleted)
        };

        // The first pass deletes the expired rows, all of them: the text bound selects
        // a superset of what the instant comparison does. Every timed pass after it
        // finds nothing new, the steady state of a periodic check.
        let (first, deleted) = check(&conjoined).await;
        assert_eq!(
            deleted, expired,
            "{layout:?}: the first pass deletes every expired row"
        );
        eprintln!(
            "{layout:?}: {data_files} data files, {total_rows} rows, {retention_days}-day retention; first pass deleted {deleted} in {first:?}"
        );
        let arms: [(&str, &Expr); 3] = [
            ("text comparison alone (file-based path)", &text),
            ("instant comparison alone", &instant),
            ("text bound AND instant comparison", &conjoined),
        ];
        let mut samples: [Vec<std::time::Duration>; 3] =
            std::array::from_fn(|_| Vec::with_capacity(reps));
        for rep in 0..reps {
            for offset in 0..arms.len() {
                let arm = (rep + offset) % arms.len();
                let (elapsed, deleted) = check(arms[arm].1).await;
                assert_eq!(
                    deleted, 0,
                    "{layout:?}: a steady-state pass deletes nothing"
                );
                samples[arm].push(elapsed);
            }
        }
        for ((name, _), samples) in arms.iter().zip(samples.iter_mut()) {
            samples.sort_unstable();
            let at = |percent: usize| samples[(samples.len() * percent).div_ceil(100) - 1];
            eprintln!(
                "{layout:?} {name}: n={} p50={:?} p99={:?} max={:?}",
                samples.len(),
                at(50),
                at(99),
                samples[samples.len() - 1]
            );
        }
    }

    /// `cayenne` as the Cayenne accelerator hands it to the runtime: beneath a
    /// `PolyTableProvider` read/write split whose write side is the table.
    fn cayenne_accelerator(cayenne: cayenne::CayenneTableProvider) -> Arc<dyn TableProvider> {
        let cayenne = Arc::new(cayenne) as Arc<dyn TableProvider>;
        Arc::new(
            data_components::poly::PolyTableProvider::new_with_schema_metadata(
                Arc::clone(&cayenne),
                cayenne,
                std::collections::HashMap::new(),
            ),
        )
        .into_table() as Arc<dyn TableProvider>
    }

    /// Files under `dir`, recursively, whose extension is `extension`.
    fn walk_count_files(dir: &std::path::Path, extension: &str) -> usize {
        std::fs::read_dir(dir).map_or(0, |entries| {
            entries
                .flatten()
                .map(|entry| {
                    let path = entry.path();
                    if path.is_dir() {
                        walk_count_files(&path, extension)
                    } else {
                        usize::from(path.extension().is_some_and(|ext| ext == extension))
                    }
                })
                .sum()
        })
    }

    #[test]
    fn test_strip_index_wrapper_layers_unwraps_all_known_layers() {
        use data_components::MetadataEnrichedTableProvider;
        use spice_table::{IndexLayer, SpiceTable};

        let mem_table: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(create_test_schema(), vec![]).expect("mem table should be created"),
        );

        // Each case must yield the very provider the layers wrap — the one a
        // retention DELETE executes against — not merely some `MemTable`.

        // An index layer alone.
        let wrapped: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::new(IndexLayer::new()), Arc::clone(&mem_table));
        assert!(
            Arc::ptr_eq(&strip_index_wrapper_layers(&wrapped), &mem_table),
            "stripping an IndexLayer must yield the underlying MemTable"
        );

        // A metadata layer nested inside an index layer (the shape
        // `table_provider_with_spicepod_metadata` produces).
        let metadata_enriched: Arc<dyn TableProvider> = SpiceTable::over(
            Arc::new(MetadataEnrichedTableProvider::new(
                &mem_table,
                std::collections::HashMap::from([("key".to_string(), "value".to_string())]),
            )),
            Arc::clone(&mem_table),
        );
        let wrapped_with_metadata: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::new(IndexLayer::new()), metadata_enriched);
        assert!(
            Arc::ptr_eq(
                &strip_index_wrapper_layers(&wrapped_with_metadata),
                &mem_table
            ),
            "stripping must peel through a nested metadata layer"
        );

        // A provider with no wrapper layers is returned unchanged.
        assert!(
            Arc::ptr_eq(&strip_index_wrapper_layers(&mem_table), &mem_table),
            "a bare provider is its own retention target"
        );
    }
}
