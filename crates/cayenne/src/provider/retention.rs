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

//! Time-based retention filter builder for Cayenne tables.
//!
//! The implementation is based on the shared timestamp filter converter,
//! ensuring correct type handling for retention filters across
//! supported time column types.

use arrow_schema::SchemaRef;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion_common::ScalarValue;
use snafu::prelude::*;
use util::timestamp_filter::{TimestampFilterConvert, data_type_to_timestamp_format};

/// Fold volatile functions such as `now()` in retention/delete filters to
/// literals for this execution pass.
///
/// `create_physical_expr` does not run [`ExprSimplifier`]. DataFusion's
/// `NowFunc::invoke` always errors (`invoke should not be called on a simplified
/// now() function`); the only legal path is `NowFunc::simplify`, which needs
/// `query_execution_start_time`. Call this immediately before physical planning,
/// once per pass — folding at table-open would freeze the cutoff for the
/// process lifetime.
///
/// [`ExprSimplifier`]: datafusion::optimizer::simplify_expressions::ExprSimplifier
pub(crate) fn simplify_filters_for_execution(
    filters: &[Expr],
    schema: &SchemaRef,
) -> datafusion_common::Result<Vec<Expr>> {
    filters
        .iter()
        .cloned()
        .map(|filter| util::expr::simplify_expr(filter, schema))
        .collect()
}

/// Errors from [`TimeRetentionFilterBuilder`] construction.
#[derive(Debug, Snafu)]
pub enum Error {
    /// The specified time column was not found in the table schema.
    #[snafu(display("Time column '{time_column}' not found in schema"))]
    ColumnNotFound {
        /// Name of the missing column.
        time_column: String,
    },

    /// The time column's data type is not supported for retention filtering.
    #[snafu(display(
        "Unsupported data type '{data_type}' for time-based retention on column '{time_column}'. Expected Timestamp, Date, or string (ISO 8601) type"
    ))]
    UnsupportedDataType {
        /// Name of the column with the unsupported type.
        time_column: String,
        /// The unsupported Arrow data type.
        data_type: arrow_schema::DataType,
    },

    /// The retention filter expression could not be parsed.
    #[snafu(display("Failed to parse retention filter expression: {detail}"))]
    ExpressionParse {
        /// Description of the parse failure.
        detail: String,
    },
}

/// Result type for [`TimeRetentionFilterBuilder`] operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Configuration for constructing time-based retention filter.
#[derive(Debug, Clone)]
pub struct TimeRetentionFilterBuilder {
    /// Pre-resolved timestamp filter converter.
    converter: TimestampFilterConvert,
    /// Retention period in seconds.
    retention_seconds: u64,
    /// Name of the time column used for retention filtering.
    column_name: String,
}

impl TimeRetentionFilterBuilder {
    /// Create a new builder from schema and configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the column is not found in the schema or its data
    /// type is not supported for retention filtering.
    pub fn try_new(
        time_column: impl Into<String>,
        retention_seconds: u64,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let time_column = time_column.into();
        let field = schema
            .column_with_name(&time_column)
            .map(|(_idx, f)| f)
            .context(ColumnNotFoundSnafu {
                time_column: &time_column,
            })?;
        let time_format = data_type_to_timestamp_format(field.data_type(), None).context(
            UnsupportedDataTypeSnafu {
                time_column: &time_column,
                data_type: field.data_type().clone(),
            },
        )?;
        let converter = TimestampFilterConvert::new(time_column.clone(), time_format, None, None);

        Ok(Self {
            converter,
            retention_seconds,
            column_name: time_column,
        })
    }

    /// Returns the name of the time column used for retention filtering.
    #[must_use]
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Build a **keep** filter: `col >= cutoff OR col IS NULL` (rows to retain at
    /// scan time).
    ///
    /// The `OR col IS NULL` disjunct keeps NULL-timestamp rows VISIBLE, matching
    /// the delete side, which never removes them: the per-row delete predicate
    /// `col < cutoff` is NULL (not matched) for a NULL timestamp, and the
    /// whole-file drop predicate `max(col) < threshold` is false when the file's
    /// max is NULL/absent. Without the disjunct, `NULL >= cutoff` is NULL under
    /// three-valued logic, so the read path would permanently hide rows retention
    /// never reclaims — violating the documented contract that NULL-timestamp rows
    /// are retained (`crates/runtime/src/accelerated_table/retention.rs`).
    #[must_use]
    pub fn keep_filter(&self) -> Expr {
        let cutoff_nanos = self.cutoff_nanos();
        self.converter
            .convert(cutoff_nanos, Operator::GtEq)
            .or(datafusion::logical_expr::col(&self.column_name).is_null())
    }

    /// Compute the cutoff timestamp in nanoseconds: `now() - retention_seconds`.
    #[expect(clippy::cast_sign_loss)]
    fn cutoff_nanos(&self) -> u128 {
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
        let retention_nanos = i64::try_from(self.retention_seconds)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000_000);
        now_nanos.saturating_sub(retention_nanos) as u128
    }
}

/// Comparison operators valid for retention filter expressions.
const RETENTION_COMPARISON_OPS: [Operator; 4] =
    [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq];

/// Extract column name and threshold from a simplified retention filter expression.
///
/// Expects the expression to be a binary comparison of the form
/// `column {<,<=,>,>=} literal` (after simplification). Returns
/// `(column_name, operator, threshold_scalar)` on success.
///
/// # Errors
///
/// Returns an error if:
/// - The expression is not a binary comparison.
/// - The left side is not a column reference.
/// - The right side is not a scalar literal.
/// - The operator is not a supported comparison (`<`, `<=`, `>`, `>=`).
pub(crate) fn extract_retention_column_and_threshold(
    expr: &Expr,
) -> Result<(String, Operator, ScalarValue)> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return Err(Error::ExpressionParse {
            detail: format!("Expected a binary expression for retention filter, got: {expr}"),
        });
    };

    if !RETENTION_COMPARISON_OPS.contains(op) {
        return Err(Error::ExpressionParse {
            detail: format!(
                "Unsupported operator '{op}' in retention filter. Expected one of: <, <=, >, >="
            ),
        });
    }

    let col_name = match left.as_ref() {
        Expr::Column(c) => c.name.clone(),
        other => {
            return Err(Error::ExpressionParse {
                detail: format!(
                    "Expected column reference on left side of retention filter, got: {other}"
                ),
            });
        }
    };

    let threshold = match right.as_ref() {
        Expr::Literal(scalar, _) => scalar.clone(),
        other => {
            return Err(Error::ExpressionParse {
                detail: format!(
                    "Expected scalar literal on right side of retention filter, got: {other}"
                ),
            });
        }
    };

    Ok((col_name, *op, threshold))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::col;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;
    use std::sync::Arc;

    #[test]
    fn test_keep_filter_timestamp_utc() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]));

        let builder = TimeRetentionFilterBuilder::try_new("event_time", 60, &schema)
            .expect("should create builder for Timestamp(ns, UTC)");
        let filter = builder.keep_filter();

        let filter_str = format!("{filter}");
        assert!(
            filter_str.contains("event_time"),
            "filter should reference event_time: {filter_str}"
        );
        assert!(
            filter_str.contains("IS NULL"),
            "keep filter must retain NULL-timestamp rows via an `IS NULL` disjunct: {filter_str}"
        );
    }

    #[test]
    fn test_keep_filter_timestamp_naive() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        let builder = TimeRetentionFilterBuilder::try_new("event_time", 60, &schema)
            .expect("should create builder for Timestamp(ns, None)");
        let filter = builder.keep_filter();
        let filter_str = format!("{filter}");
        assert!(
            filter_str.contains("event_time"),
            "filter should reference event_time: {filter_str}"
        );
    }

    #[test]
    fn test_unsupported_column_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "epoch_secs",
            DataType::Int64,
            false,
        )]));

        let err = TimeRetentionFilterBuilder::try_new("epoch_secs", 60, &schema)
            .expect_err("Int64 without unix scale should be unsupported");
        assert!(
            matches!(err, Error::UnsupportedDataType { .. }),
            "expected UnsupportedDataType, got: {err}"
        );
    }

    #[test]
    fn test_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let err = TimeRetentionFilterBuilder::try_new("nonexistent", 60, &schema)
            .expect_err("missing column should error");
        assert!(
            matches!(err, Error::ColumnNotFound { .. }),
            "expected ColumnNotFound, got: {err}"
        );
    }

    #[test]
    fn test_extract_retention_column_and_threshold_lt() {
        let expr = col("event_time").lt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1_000_000), None),
            None,
        ));
        let (col_name, op, threshold) =
            extract_retention_column_and_threshold(&expr).expect("should parse col < literal");
        assert_eq!(col_name, "event_time");
        assert_eq!(op, Operator::Lt);
        assert_eq!(
            threshold,
            ScalarValue::TimestampNanosecond(Some(1_000_000), None)
        );
    }

    #[test]
    fn test_extract_retention_column_and_threshold_gte() {
        let expr = col("ts").gt_eq(Expr::Literal(ScalarValue::Int64(Some(42)), None));
        let (col_name, op, _threshold) =
            extract_retention_column_and_threshold(&expr).expect("should parse col >= literal");
        assert_eq!(col_name, "ts");
        assert_eq!(op, Operator::GtEq);
    }

    #[test]
    fn test_extract_retention_rejects_unsupported_operator() {
        // Equality is not a valid retention comparison operator
        let expr = col("event_time").eq(Expr::Literal(ScalarValue::Int64(Some(42)), None));
        let err =
            extract_retention_column_and_threshold(&expr).expect_err("equality should be rejected");
        assert!(
            matches!(err, Error::ExpressionParse { .. }),
            "expected ExpressionParse, got: {err}"
        );

        // AND is not a comparison operator
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::And,
            right: Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        });
        let err =
            extract_retention_column_and_threshold(&expr).expect_err("AND should be rejected");
        assert!(
            matches!(err, Error::ExpressionParse { .. }),
            "expected ExpressionParse for AND, got: {err}"
        );
    }

    #[test]
    fn test_column_name_accessor() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]));
        let builder = TimeRetentionFilterBuilder::try_new("event_time", 60, &schema)
            .expect("should create builder");
        assert_eq!(builder.column_name(), "event_time");
    }

    /// Roundtrip: build a filter via [`TimeRetentionFilterBuilder`],
    /// simplify it (as the runtime does), then parse it back with
    /// [`extract_retention_column_and_threshold`].
    #[test]
    fn test_roundtrip_build_simplify_parse() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));

        let builder = TimeRetentionFilterBuilder::try_new("event_time", 3600, &schema)
            .expect("should create builder");

        // 1. Build the DELETE filter (`col < cutoff`) — this is what
        //    `extract_retention_column_and_threshold` actually parses on the write
        //    path. (The READ `keep_filter` is now `col >= cutoff OR col IS NULL`, a
        //    top-level OR the single-comparison parser intentionally rejects; its
        //    NULL-retention shape is covered by `test_keep_filter_timestamp_utc`
        //    and the end-to-end `retention_test`.)
        let filter = builder
            .converter
            .convert(builder.cutoff_nanos(), Operator::Lt);

        // 2. Simplify — mirrors what the runtime does before calling delete_from
        let simplified =
            util::expr::simplify_expr(filter, &schema).expect("simplification should succeed");

        // 3. Parse the simplified expression back
        let (col_name, op, threshold) = extract_retention_column_and_threshold(&simplified)
            .expect("should parse simplified delete filter");

        assert_eq!(
            col_name, "event_time",
            "column name should survive roundtrip"
        );
        assert_eq!(op, Operator::Lt, "delete filter uses <");

        // The threshold must be a concrete timestamp scalar (now() evaluated away)
        assert!(
            matches!(threshold, ScalarValue::TimestampMicrosecond(Some(_), _)),
            "threshold should be a resolved TimestampMicrosecond, got: {threshold}"
        );
    }

    /// `retention_sql` predicates such as `arrival_time < now() - INTERVAL '7 days'`
    /// must fold `now()` to a `TimestampNanosecond` literal per pass. Folding at
    /// table-open would freeze the cutoff for the process lifetime.
    #[test]
    fn test_simplify_filters_folds_now_minus_interval() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "arrival_time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]));
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).expect("schema conversion");
        let ctx = SessionContext::new();
        let expr = ctx
            .parse_sql_expr("arrival_time < now() - INTERVAL '7 days'", &df_schema)
            .expect("parse now() retention filter");

        let simplified = simplify_filters_for_execution(&[expr], &schema)
            .expect("simplification should succeed");
        assert_eq!(simplified.len(), 1, "one filter in, one filter out");

        let (col_name, op, threshold) = extract_retention_column_and_threshold(&simplified[0])
            .expect("simplified now() filter should be col < literal");
        assert_eq!(col_name, "arrival_time");
        assert_eq!(op, Operator::Lt);
        assert!(
            matches!(threshold, ScalarValue::TimestampNanosecond(Some(_), _)),
            "now() - INTERVAL should fold to TimestampNanosecond, got: {threshold}"
        );
    }
}
