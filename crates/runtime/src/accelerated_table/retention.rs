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
    catalog::TableProvider,
    logical_expr::Operator,
    physical_plan::collect,
    prelude::{Expr, SessionContext},
    sql::TableReference,
};

use crate::{
    accelerated_table::{Retention, refresh},
    datafusion::{
        builder::get_df_default_config, filter_converter::TimestampFilterConvert,
        is_spice_internal_dataset,
    },
    object_store_registry::default_runtime_env,
};

enum DataRetentionFilter {
    DeleteExpr(Expr),
    TimeColumn {
        converter: TimestampFilterConvert,
        time_column: String,
        retention_period: std::time::Duration,
    },
}

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
        let retention_filter = if let Some(delete_expr) = retention.delete_expr {
            DataRetentionFilter::DeleteExpr(delete_expr)
        } else {
            let Some(time_column) = &retention.time_column else {
                tracing::error!(
                    "[retention] The `time_column` parameter must be specified for retention"
                );
                return;
            };

            let Some(retention_period) = retention.period else {
                tracing::error!(
                    "[retention] The `retention_period` parameter must be specified for retention"
                );
                return;
            };

            let Some(converter) =
                create_timestamp_filter_converter(&retention, &accelerator, time_column)
            else {
                return;
            };
            DataRetentionFilter::TimeColumn {
                converter,
                time_column: time_column.clone(),
                retention_period,
            }
        };

        let mut interval_timer = tokio::time::interval(retention.check_interval);

        loop {
            interval_timer.tick().await;

            if let Some(deleted_table_provider) = get_deletion_provider(Arc::clone(&accelerator)) {
                let expr = match &retention_filter {
                    DataRetentionFilter::DeleteExpr(expr) => {
                        if is_spice_internal_dataset(&dataset_name) {
                            tracing::trace!(
                                "[retention] Evicting data for {dataset_name} with using retention sql expression"
                            );
                        } else {
                            tracing::info!(
                                "[retention] Evicting data for {dataset_name} with using retention sql expression"
                            );
                        }
                        expr.clone()
                    }
                    DataRetentionFilter::TimeColumn {
                        converter,
                        time_column,
                        retention_period,
                    } => {
                        let start = SystemTime::now() - *retention_period;
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

                        if is_spice_internal_dataset(&dataset_name) {
                            tracing::trace!(
                                "[retention] Evicting data for {dataset_name} where {time_column} < {}...",
                                timestamp
                            );
                        } else {
                            tracing::info!(
                                "[retention] Evicting data for {dataset_name} where {time_column} < {}...",
                                timestamp
                            );
                        }

                        expr
                    }
                };

                tracing::trace!("[retention] Expr {expr:?}");

                let ctx = SessionContext::new_with_config_rt(
                    get_df_default_config(),
                    default_runtime_env(),
                );

                let plan = deleted_table_provider
                    .delete_from(&ctx.state(), &vec![expr.clone()])
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

                            if is_spice_internal_dataset(&dataset_name) {
                                tracing::trace!(
                                    "[retention] Evicted {num_records} records for {dataset_name}"
                                );
                            } else {
                                tracing::info!(
                                    "[retention] Evicted {num_records} records for {dataset_name}"
                                );
                            }

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
    retention: &Retention,
    accelerator: &Arc<dyn TableProvider>,
    time_column: &str,
) -> Option<TimestampFilterConvert> {
    let schema = accelerator.schema();
    let field = schema.column_with_name(time_column).map(|(_, f)| f);
    let partition_field =
        retention
            .time_partition_column
            .as_ref()
            .and_then(|time_partition_column| {
                schema
                    .column_with_name(time_partition_column.as_str())
                    .map(|(_, f)| f)
            });

    TimestampFilterConvert::create(
        field.cloned(),
        Some(time_column.to_string()),
        retention.time_format,
        partition_field.cloned(),
        retention.time_partition_column.clone(),
        retention.time_partition_format,
    )
}
