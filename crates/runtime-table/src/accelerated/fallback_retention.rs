/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Inverse of configured retention predicates, applied to
//! `on_zero_results: use_source` fallback scans.

use std::time::SystemTime;

use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator};
use snafu::prelude::*;

use crate::accelerated::refresh;
use crate::accelerated::{DataRetentionFilter, Retention};
use crate::filter_converter::create_timestamp_filter_convert;
use runtime_component::dataset::TimeFormat;
use runtime_datafusion::retention_keep::{keep_expr_for_retention_delete, validate_keep_expr};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a retention policy cannot be inverted onto a source fallback scan.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "time-based retention compares `{time_column}` against the cutoff, but that column cannot be used as a source filter (unsupported type)"
    ))]
    UntranslatableTime { time_column: String },

    #[snafu(display(
        "the retention predicate cannot be planned against the source schema: {}",
        runtime_datafusion::error::format_datafusion_error(source)
    ))]
    UntranslatableExpr { source: DataFusionError },

    #[snafu(display(
        "retention is computed from accelerator size rather than a row predicate, so it has no inverse that the source can evaluate"
    ))]
    ComputedOnly,
}

/// Static retention filters whose inverse is applied to a federated fallback scan.
#[derive(Clone, Debug)]
pub struct FallbackRetentionKeep {
    filters: Vec<DataRetentionFilter>,
}

impl FallbackRetentionKeep {
    /// The invertible keep spec for `retention`, or `None` when nothing to invert.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ComputedOnly`] when the only policy is a computed
    /// predicate (no static `retention_sql` / `retention_period` filter).
    pub fn from_retention(retention: &Retention) -> Result<Option<Self>> {
        if retention.filters.is_empty() {
            ensure!(retention.computed.is_none(), ComputedOnlySnafu);
            return Ok(None);
        }
        Ok(Some(Self {
            filters: retention.filters.clone(),
        }))
    }

    /// Keep spec for a `retention_sql` delete predicate, including write-time
    /// application when the scheduled retention worker is not running.
    #[must_use]
    pub fn from_delete_expr(delete_expr: Expr) -> Self {
        Self {
            filters: vec![DataRetentionFilter::Expression {
                delete_expr: Box::new(delete_expr),
            }],
        }
    }

    /// Combine scheduled filters with a write-time `retention_sql` predicate.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ComputedOnly`] when the only configured policy is a
    /// computed predicate and there is no `retention_sql` to invert.
    pub fn from_configured(
        retention: Option<&Retention>,
        retention_sql_delete_expr: Option<Expr>,
    ) -> Result<Option<Self>> {
        let scheduled = match retention {
            Some(retention) => match Self::from_retention(retention) {
                Ok(keep) => keep,
                Err(Error::ComputedOnly) if retention_sql_delete_expr.is_some() => None,
                Err(err) => return Err(err),
            },
            None => None,
        };
        Ok(match (scheduled, retention_sql_delete_expr) {
            (Some(keep), Some(expr)) => Some(keep.with_delete_expr(expr)),
            (Some(keep), None) => Some(keep),
            (None, Some(expr)) => Some(Self::from_delete_expr(expr)),
            (None, None) => None,
        })
    }

    /// Add a `retention_sql` delete predicate unless `filters` already holds it.
    ///
    /// A scheduled policy built from the same `retention_sql` already carries
    /// the predicate. A second copy is not idempotent when the predicate is
    /// volatile: `random() < 0.25 OR random() < 0.25` draws twice per row and
    /// matches about 44% of rows, not the 25% the retention pass deletes.
    fn with_delete_expr(mut self, delete_expr: Expr) -> Self {
        let present = self.filters.iter().any(|filter| {
            matches!(
                filter,
                DataRetentionFilter::Expression { delete_expr: existing }
                    if **existing == delete_expr
            )
        });
        if !present {
            self.filters.push(DataRetentionFilter::Expression {
                delete_expr: Box::new(delete_expr),
            });
        }
        self
    }

    /// Time retention inverted onto fallback when no scheduled worker runs.
    ///
    /// Cayenne still hides expired rows at scan time without a ticker. `DuckDB`
    /// and Arrow do not, but applying the same cutoff on fallback is safe: a
    /// still-present expired accelerator row never takes this path.
    #[must_use]
    pub fn from_time(
        period: std::time::Duration,
        time_column: String,
        time_format: Option<TimeFormat>,
        time_partition_column: Option<String>,
        time_partition_format: Option<TimeFormat>,
    ) -> Self {
        Self {
            filters: vec![DataRetentionFilter::Time {
                period,
                time_column,
                time_format,
                time_partition_column,
                time_partition_format,
            }],
        }
    }

    /// Merge `keep` with a `time_column`-only cutoff.
    ///
    /// Use `apply` when the accelerator hides expired rows at scan time
    /// (Cayenne, ticker on or off) or when no scheduled worker runs. The
    /// inverted cutoff uses `time_column` only. Cayenne's scan-time keep
    /// ignores `time_partition_column`, so a partition-AND delete would keep
    /// expired rows whose partition is recent or NULL and resurrect them
    /// through fallback. Other engines keep the ticker's partition-AND via
    /// [`Self::from_configured`] when `apply` is false.
    #[must_use]
    pub fn with_time_column_keep(
        keep: Option<Self>,
        apply: bool,
        period: Option<std::time::Duration>,
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
    ) -> Option<Self> {
        let time = match (apply, period, time_column) {
            (true, Some(period), Some(time_column)) => Some(Self::from_time(
                period,
                time_column,
                time_format,
                None,
                None,
            )),
            _ => None,
        };
        match (keep, time) {
            (Some(keep), Some(time)) => Some(keep.merge(time)),
            (Some(keep), None) => Some(keep),
            (None, Some(time)) => Some(time),
            (None, None) => None,
        }
    }

    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        self.filters.extend(other.filters);
        self
    }

    /// Keep predicates matching the rows retention would leave in the accelerator.
    ///
    /// Time cutoffs are evaluated at the call, so a fallback uses "now" rather
    /// than the last retention tick.
    ///
    /// # Errors
    ///
    /// Returns an error if a time column cannot be converted or the combined
    /// predicate cannot be simplified against `schema`.
    pub fn keep_filters(&self, schema: &SchemaRef) -> Result<Vec<Expr>> {
        self.keep_filters_at(schema, SystemTime::now())
    }

    /// [`Self::keep_filters`] with every time cutoff measured back from `now`.
    fn keep_filters_at(&self, schema: &SchemaRef, now: SystemTime) -> Result<Vec<Expr>> {
        let mut delete_preds = Vec::with_capacity(self.filters.len());
        for filter in &self.filters {
            match filter {
                DataRetentionFilter::Expression { delete_expr } => {
                    delete_preds.push((**delete_expr).clone());
                }
                DataRetentionFilter::Time {
                    period,
                    time_column,
                    time_format,
                    time_partition_column,
                    time_partition_format,
                } => {
                    delete_preds.push(time_retention_delete_expr(
                        schema,
                        now,
                        *period,
                        time_column,
                        *time_format,
                        time_partition_column.as_ref(),
                        *time_partition_format,
                    )?);
                }
            }
        }

        let Some(combined) = delete_preds.into_iter().reduce(Expr::or) else {
            return Ok(Vec::new());
        };
        let keep = keep_expr_for_retention_delete(combined);
        util::expr::coerce_and_simplify_exprs([keep], schema).context(UntranslatableExprSnafu)
    }

    /// Fail if the keep predicates cannot be planned against `schema`.
    ///
    /// # Errors
    ///
    /// Returns an error when a time converter cannot be built or a keep
    /// expression cannot be compiled as a physical filter.
    pub fn validate(&self, schema: &SchemaRef) -> Result<()> {
        let keeps = self.keep_filters(schema)?;
        for keep in &keeps {
            validate_keep_expr(keep, schema).context(UntranslatableExprSnafu)?;
        }
        Ok(())
    }
}

fn time_retention_delete_expr(
    schema: &SchemaRef,
    now: SystemTime,
    period: std::time::Duration,
    time_column: &str,
    time_format: Option<TimeFormat>,
    time_partition_column: Option<&String>,
    time_partition_format: Option<TimeFormat>,
) -> Result<Expr> {
    let field = schema.column_with_name(time_column).map(|(_, f)| f.clone());
    let partition_field = time_partition_column.and_then(|column| {
        schema
            .column_with_name(column.as_str())
            .map(|(_, f)| f.clone())
    });
    let converter = create_timestamp_filter_convert(
        field,
        Some(time_column.to_string()),
        time_format,
        partition_field,
        time_partition_column.cloned(),
        time_partition_format,
    )
    .ok_or_else(|| Error::UntranslatableTime {
        time_column: time_column.to_string(),
    })?;
    let start = now - period;
    let timestamp = refresh::get_timestamp(start);
    Ok(converter.convert(timestamp, Operator::Lt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::{col, lit};
    use runtime_component::dataset::TimeFormat;
    use std::sync::Arc;
    use std::time::Duration;

    fn events_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("deleted", DataType::Boolean, true),
            Field::new("ts", DataType::Int64, true),
        ]))
    }

    fn keep_from_sql() -> FallbackRetentionKeep {
        FallbackRetentionKeep {
            filters: vec![DataRetentionFilter::Expression {
                delete_expr: Box::new(col("deleted").eq(lit(true))),
            }],
        }
    }

    #[test]
    fn validate_accepts_boolean_retention_sql() {
        keep_from_sql()
            .validate(&events_schema())
            .expect("deleted = true is a source filter");
    }

    #[tokio::test]
    async fn time_keep_filters_keep_recent_and_null_drop_old() {
        use arrow::array::{BooleanArray, Int64Array, RecordBatch};
        use datafusion::catalog::MemTable;
        use datafusion::prelude::SessionContext;

        let now = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_secs(),
        )
        .expect("unix seconds fit i64");
        let schema = events_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(false),
                ])),
                Arc::new(Int64Array::from(vec![Some(now), Some(now - 10_000), None])),
            ],
        )
        .expect("batch");

        let keep = FallbackRetentionKeep {
            filters: vec![DataRetentionFilter::Time {
                period: Duration::from_secs(3600),
                time_column: "ts".to_string(),
                time_format: Some(TimeFormat::UnixSeconds),
                time_partition_column: None,
                time_partition_format: None,
            }],
        };
        let keep_expr = keep
            .keep_filters(&schema)
            .expect("time keep")
            .into_iter()
            .next()
            .expect("one keep predicate");

        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("mem")),
        )
        .expect("register");
        let batches = ctx
            .table("t")
            .await
            .expect("table")
            .filter(keep_expr)
            .expect("filter")
            .collect()
            .await
            .expect("collect");
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id")
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 3],
            "time retention keeps recent and NULL timestamps and drops the expired row"
        );
    }

    #[test]
    fn validate_rejects_unsupported_time_column_type() {
        let keep = FallbackRetentionKeep {
            filters: vec![DataRetentionFilter::Time {
                period: Duration::from_secs(3600),
                time_column: "deleted".to_string(),
                time_format: None,
                time_partition_column: None,
                time_partition_format: None,
            }],
        };
        let err = keep
            .validate(&events_schema())
            .expect_err("boolean is not a time column");
        assert!(
            matches!(err, Error::UntranslatableTime { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn from_retention_refuses_computed_only() {
        let retention = Retention {
            filters: Vec::new(),
            check_interval: Duration::from_secs(1),
            computed: Some(Arc::new(RefuseComputed)),
        };
        let err = FallbackRetentionKeep::from_retention(&retention)
            .expect_err("computed-only has no inverse");
        assert!(matches!(err, Error::ComputedOnly));
    }

    #[test]
    fn from_configured_computed_only_without_sql_is_error() {
        let retention = Retention {
            filters: Vec::new(),
            check_interval: Duration::from_secs(1),
            computed: Some(Arc::new(RefuseComputed)),
        };
        let err = FallbackRetentionKeep::from_configured(Some(&retention), None)
            .expect_err("computed-only has no inverse");
        assert!(matches!(err, Error::ComputedOnly));
    }

    #[test]
    fn from_configured_uses_write_time_sql_when_unscheduled() {
        let keep = FallbackRetentionKeep::from_configured(None, Some(col("deleted").eq(lit(true))))
            .expect("write-time sql is invertible")
            .expect("a keep spec");
        keep.validate(&events_schema())
            .expect("deleted = true is a source filter");
    }

    #[test]
    fn from_configured_sql_covers_computed_only_scheduled() {
        let retention = Retention {
            filters: Vec::new(),
            check_interval: Duration::from_secs(1),
            computed: Some(Arc::new(RefuseComputed)),
        };
        let keep = FallbackRetentionKeep::from_configured(
            Some(&retention),
            Some(col("deleted").eq(lit(true))),
        )
        .expect("sql still inverts when the scheduled policy is computed-only")
        .expect("a keep spec");
        assert_eq!(keep.filters.len(), 1);
    }

    #[test]
    fn from_configured_inverts_scheduled_retention_sql_once() {
        // The scheduled policy is built from the same `retention_sql` the
        // caller passes, so it already carries the predicate. The simplifier
        // folds `p OR p` to `p` only when `p` is not volatile, so a volatile
        // predicate is the one a second copy would change.
        let delete_expr = datafusion::functions::expr_fn::random().lt(lit(0.25));
        let scheduled = Retention {
            filters: vec![DataRetentionFilter::Expression {
                delete_expr: Box::new(delete_expr.clone()),
            }],
            check_interval: Duration::from_secs(1),
            computed: None,
        };
        let keep =
            FallbackRetentionKeep::from_configured(Some(&scheduled), Some(delete_expr.clone()))
                .expect("retention_sql is invertible")
                .expect("a keep spec");
        let schema = events_schema();
        assert_eq!(
            keep.keep_filters(&schema).expect("keep"),
            FallbackRetentionKeep::from_delete_expr(delete_expr)
                .keep_filters(&schema)
                .expect("keep"),
            "fallback must invert the retention predicate once, as the retention pass evaluates it"
        );
    }

    #[test]
    fn with_time_column_keep_covers_ticker_off() {
        let keep = FallbackRetentionKeep::with_time_column_keep(
            None,
            true,
            Some(Duration::from_secs(3600)),
            Some("ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("time period without a ticker is still invertible");
        keep.validate(&events_schema())
            .expect("ts is a source time column");
        assert_eq!(keep.filters.len(), 1);
    }

    #[test]
    fn with_time_column_keep_does_not_duplicate_when_not_applied() {
        let scheduled = Retention {
            filters: vec![DataRetentionFilter::Time {
                period: Duration::from_secs(3600),
                time_column: "ts".to_string(),
                time_format: Some(TimeFormat::UnixSeconds),
                time_partition_column: None,
                time_partition_format: None,
            }],
            check_interval: Duration::from_secs(1),
            computed: None,
        };
        let keep = FallbackRetentionKeep::from_configured(Some(&scheduled), None)
            .expect("scheduled time is invertible")
            .expect("a keep spec");
        let merged = FallbackRetentionKeep::with_time_column_keep(
            Some(keep),
            false,
            Some(Duration::from_secs(3600)),
            Some("ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("scheduled keep is kept");
        assert_eq!(
            merged.filters.len(),
            1,
            "DuckDB/Arrow scheduled keep must not grow a second time-column cutoff"
        );
    }

    fn partitioned_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("partition_ts", DataType::Int64, true),
        ]))
    }

    async fn ids_matching(keep: &FallbackRetentionKeep, schema: &SchemaRef) -> Vec<i64> {
        use arrow::array::{Int64Array, RecordBatch};
        use datafusion::catalog::MemTable;
        use datafusion::prelude::SessionContext;

        let now = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_secs(),
        )
        .expect("unix seconds fit i64");
        let expired = now - 10_000;
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![
                    Some(now),
                    Some(expired),
                    Some(expired),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(now), None, Some(now), None])),
            ],
        )
        .expect("batch");
        let keep_expr = keep
            .keep_filters(schema)
            .expect("keep")
            .into_iter()
            .next()
            .expect("one keep predicate");
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(Arc::clone(schema), vec![vec![batch]]).expect("mem")),
        )
        .expect("register");
        let batches = ctx
            .table("t")
            .await
            .expect("table")
            .filter(keep_expr)
            .expect("filter")
            .collect()
            .await
            .expect("collect");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id")
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    #[tokio::test]
    async fn unscheduled_time_keep_matches_cayenne_without_partition() {
        let schema = partitioned_schema();
        let keep = FallbackRetentionKeep::with_time_column_keep(
            None,
            true,
            Some(Duration::from_secs(3600)),
            Some("ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("unscheduled time is invertible");
        let ids = ids_matching(&keep, &schema).await;
        assert_eq!(
            ids,
            vec![1, 4],
            "Cayenne scan-time keep uses only ts: expired rows stay out even when partition_ts is NULL (id=2) or recent (id=3); NULL ts is kept"
        );
    }

    #[tokio::test]
    async fn cayenne_scheduled_merge_matches_scan_time_keep() {
        let schema = partitioned_schema();
        let scheduled = FallbackRetentionKeep::from_time(
            Duration::from_secs(3600),
            "ts".to_string(),
            Some(TimeFormat::UnixSeconds),
            Some("partition_ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        );
        let keep = FallbackRetentionKeep::with_time_column_keep(
            Some(scheduled),
            true,
            Some(Duration::from_secs(3600)),
            Some("ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        )
        .expect("Cayenne still applies the time-column keep when the ticker runs");
        let ids = ids_matching(&keep, &schema).await;
        assert_eq!(
            ids,
            vec![1, 4],
            "merging Cayenne scan-time keep with the ticker AND must still drop expired ts"
        );
    }

    #[tokio::test]
    async fn cayenne_time_keep_truncates_subsecond_period() {
        use arrow::array::{Int64Array, RecordBatch};
        use datafusion::catalog::MemTable;
        use datafusion::prelude::SessionContext;

        // One instant for the fixture timestamps and the cutoff, so the result
        // does not depend on how long building the predicate takes.
        let now = SystemTime::now();
        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("unix millis fit i64");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![
                    Some(now_ms),
                    Some(now_ms - 1_200),
                    None,
                ])),
            ],
        )
        .expect("batch");

        let collect = |period: Duration| {
            let schema = Arc::clone(&schema);
            let batch = batch.clone();
            async move {
                let keep = FallbackRetentionKeep::from_time(
                    period,
                    "ts".to_string(),
                    Some(TimeFormat::UnixMillis),
                    None,
                    None,
                );
                let keep_expr = keep
                    .keep_filters_at(&schema, now)
                    .expect("keep")
                    .into_iter()
                    .next()
                    .expect("one keep predicate");
                let ctx = SessionContext::new();
                ctx.register_table(
                    "t",
                    Arc::new(
                        MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]])
                            .expect("mem"),
                    ),
                )
                .expect("register");
                let batches = ctx
                    .table("t")
                    .await
                    .expect("table")
                    .filter(keep_expr)
                    .expect("filter")
                    .collect()
                    .await
                    .expect("collect");
                batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("id")
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<i64>>()
            }
        };

        assert_eq!(
            collect(Duration::from_millis(1_500)).await,
            vec![1, 2, 3],
            "a 1.5s cutoff keeps the 1.2s-old row"
        );
        assert_eq!(
            collect(Duration::from_secs(Duration::from_millis(1_500).as_secs())).await,
            vec![1, 3],
            "Cayenne's as_secs truncation (1s) hides the 1.2s-old row that a 1.5s fallback would keep"
        );
    }

    #[tokio::test]
    async fn scheduled_time_keep_uses_partition_and() {
        let schema = partitioned_schema();
        let keep = FallbackRetentionKeep::from_time(
            Duration::from_secs(3600),
            "ts".to_string(),
            Some(TimeFormat::UnixSeconds),
            Some("partition_ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        );
        let ids = ids_matching(&keep, &schema).await;
        assert_eq!(
            ids,
            vec![1, 2, 3, 4],
            "scheduled ticker deletes only when ts AND partition_ts are both expired, so a recent or NULL partition keeps the row"
        );
    }

    #[derive(Debug)]
    struct RefuseComputed;

    #[async_trait::async_trait]
    impl crate::accelerated::RetentionPredicate for RefuseComputed {
        async fn delete_expr(
            &self,
            _accelerator: &Arc<dyn datafusion::catalog::TableProvider>,
            _configured: Option<Expr>,
        ) -> datafusion::error::Result<Option<Expr>> {
            Ok(None)
        }
    }
}
