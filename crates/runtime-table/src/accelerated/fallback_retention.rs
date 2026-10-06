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
            (Some(keep), Some(expr)) => Some(keep.merge(Self::from_delete_expr(expr))),
            (Some(keep), None) => Some(keep),
            (None, Some(expr)) => Some(Self::from_delete_expr(expr)),
            (None, None) => None,
        })
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
    let start = SystemTime::now() - period;
    let timestamp = refresh::get_timestamp(start);
    Ok(converter.convert(timestamp, Operator::Lt))
}

#[cfg(test)]
mod tests {
    use super::*;
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
