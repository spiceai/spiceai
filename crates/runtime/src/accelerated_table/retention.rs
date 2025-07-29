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

use arrow::array::UInt64Array;
use cache::Caching;
use data_components::delete::get_deletion_provider;
use datafusion::{
    catalog::TableProvider, logical_expr::Operator, physical_plan::collect,
    prelude::SessionContext, sql::TableReference,
};

use crate::{
    accelerated_table::{DataRetentionFilter, Retention, refresh},
    component::dataset::TimeFormat,
    datafusion::{
        builder::get_df_default_config, filter_converter::TimestampFilterConvert,
        is_spice_internal_dataset,
    },
    object_store_registry::default_runtime_env,
};

impl super::AcceleratedTable {
    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn start_retention_check(
        dataset_name: TableReference,
        accelerator: Arc<dyn TableProvider>,
        retention: Retention,
        caching: Option<Arc<Caching>>,
    ) {
        let mut interval_timer = tokio::time::interval(retention.check_interval);

        loop {
            interval_timer.tick().await;

            if let Some(deleted_table_provider) = get_deletion_provider(Arc::clone(&accelerator)) {
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

                            let start = SystemTime::now() - *period;
                            let timestamp = refresh::get_timestamp(start);
                            let expr = converter.convert(timestamp, Operator::Lt);

                            let timestamp = if let Some(value) = chrono::DateTime::from_timestamp(
                                (timestamp / 1_000_000_000) as i64,
                                0,
                            ) {
                                value.to_rfc3339()
                            } else {
                                tracing::warn!("[retention] Unable to convert timestamp");
                                continue;
                            };

                            log_retention_action(
                                &dataset_name,
                                &format!("where {time_column} < {timestamp}"),
                            );
                            exprs.push(expr);
                        }
                    }
                }

                tracing::trace!("[retention] Exprs {exprs:?}");

                let ctx = SessionContext::new_with_config_rt(
                    get_df_default_config(),
                    default_runtime_env(),
                );

                let plan = deleted_table_provider
                    .delete_from(&ctx.state(), &exprs)
                    .await;
                match plan {
                    Ok(plan) => match collect(plan, ctx.task_ctx()).await {
                        Err(e) => {
                            tracing::error!("[retention] Error running retention check: {e}");
                        }
                        Ok(deleted) => {
                            let num_records = deleted.first().map_or(0, |f| {
                                f.column(0)
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .map_or(0, |v| v.values().first().map_or(0, |f| *f))
                            });

                            log_retention_result(&dataset_name, num_records);

                            if num_records > 0 {
                                if let Some(cache_provider) = caching.as_ref() {
                                    if let Err(e) =
                                        cache_provider.invalidate_for_table(dataset_name.clone())
                                    {
                                        tracing::error!(
                                            "Failed to invalidate cached results for dataset {}: {e}",
                                            &dataset_name
                                        );
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        tracing::error!("[retention] Error running retention check: {e}");
                    }
                }
            } else {
                tracing::error!("[retention] Accelerated table does not support delete");
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

    TimestampFilterConvert::create(
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
    use crate::accelerated_table::AcceleratedTable;

    use super::*;
    use arrow::{
        array::{BooleanArray, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use data_components::{arrow::write::MemTable, delete::DeletionTableProviderAdapter};
    use datafusion::{physical_plan::collect, prelude::SessionContext};
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
        #[allow(clippy::cast_possible_wrap)]
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

        let accelerator = Arc::new(DeletionTableProviderAdapter::new(Arc::new(mem_table)))
            as Arc<dyn TableProvider>;

        // Create retention configuration
        let retention_delete_expr = retention_sql.map(|sql| {
            crate::datafusion::retention_sql::parse_retention_sql(
                &TableReference::bare("test"),
                sql,
                accelerator.schema(),
            )
            .expect("Failed to parse retention SQL")
        });

        let retention = Retention::builder()
            .time_column(time_column.map(String::from))
            .time_format(Some(crate::component::dataset::TimeFormat::UnixSeconds))
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
            retention,
            caching,
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
            Some(Duration::from_secs(10 * 24 * 60 * 60)), // 10 days
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
            Some(Duration::from_secs(10 * 24 * 60 * 60)),
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
            Some(Duration::from_secs(2 * 24 * 60 * 60)), // 2 days
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
}
