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
use datafusion::logical_expr::{Expr, Operator};
use snafu::prelude::*;
use util::timestamp_filter::{data_type_to_timestamp_format, TimestampFilterConvert};

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
        let converter = TimestampFilterConvert::new(time_column, time_format, None, None);

        Ok(Self {
            converter,
            retention_seconds,
        })
    }

    /// Build a **keep** filter: `col >= cutoff` (rows to retain at scan time).
    #[must_use]
    pub fn keep_filter(&self) -> Expr {
        let cutoff_nanos = self.cutoff_nanos();
        self.converter.convert(cutoff_nanos, Operator::GtEq)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
}
