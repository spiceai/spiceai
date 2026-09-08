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

//! CQL (Cassandra Query Language) dialect for `DataFusion` unparser.
//!
//! CQL is fundamentally different from SQL in several ways:
//! - No `CASE WHEN` expressions
//! - No `COUNT(DISTINCT ...)` - must use separate DISTINCT + COUNT
//! - No subqueries
//! - No complex JOINs (only partition key joins in Cassandra 4.0+)
//! - No `NULLS FIRST/LAST` in ORDER BY
//! - Different aggregate function syntax
//! - Uses double quotes for identifiers
//!
//! This dialect configures the `DataFusion` unparser to generate CQL-compatible
//! syntax for the operations that CQL does support.

use std::sync::Arc;

use datafusion::sql::{
    sqlparser::ast::{self, TimezoneInfo, WindowFrameBound},
    unparser::dialect::{DateFieldExtractStyle, Dialect, IntervalStyle},
};

/// CQL dialect for `ScyllaDB`/Cassandra.
///
/// This dialect generates CQL-compatible SQL for operations that CQL supports.
/// Note: Many SQL constructs are not supported in CQL and will cause errors
/// if pushed down to `ScyllaDB`. The federation layer should handle rejecting
/// unsupported operations.
#[derive(Debug, Default)]
pub struct CqlDialect {}

impl CqlDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Dialect for CqlDialect {
    /// CQL uses double quotes for identifiers.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    /// CQL does not support NULLS FIRST/LAST in ORDER BY clauses.
    fn supports_nulls_first_in_sort(&self) -> bool {
        false
    }

    /// CQL does not use TIMESTAMP for DATE64.
    fn use_timestamp_for_date64(&self) -> bool {
        false
    }

    /// CQL uses a simpler interval style.
    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::SQLStandard
    }

    /// CQL uses TEXT for UTF8 strings.
    fn utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Text
    }

    /// CQL uses TEXT for large UTF8 strings.
    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Text
    }

    /// CQL doesn't have EXTRACT, use simpler style.
    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        DateFieldExtractStyle::Extract
    }

    /// CQL uses BIGINT for Int64.
    fn int64_cast_dtype(&self) -> ast::DataType {
        ast::DataType::BigInt(None)
    }

    /// CQL uses INT for Int32.
    fn int32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Int(None)
    }

    /// CQL uses TIMESTAMP for timestamp types.
    fn timestamp_cast_dtype(
        &self,
        _time_unit: &arrow::datatypes::TimeUnit,
        _tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        // CQL TIMESTAMP doesn't have timezone info
        ast::DataType::Timestamp(None, TimezoneInfo::None)
    }

    /// CQL uses DATE for date types.
    fn date32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Date
    }

    /// CQL supports column alias in table alias.
    fn supports_column_alias_in_table_alias(&self) -> bool {
        false
    }

    /// CQL does not support window functions with frames.
    /// `ScyllaDB` doesn't have window functions at all.
    fn window_func_support_window_frame(
        &self,
        _func_name: &str,
        _start_bound: &WindowFrameBound,
        _end_bound: &WindowFrameBound,
    ) -> bool {
        // CQL/ScyllaDB does not support window functions
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cql_dialect_identifier_quote() {
        let dialect = CqlDialect::new();
        assert_eq!(dialect.identifier_quote_style("test"), Some('"'));
    }

    #[test]
    fn test_cql_dialect_no_nulls_first() {
        let dialect = CqlDialect::new();
        assert!(!dialect.supports_nulls_first_in_sort());
    }

    #[test]
    fn test_cql_dialect_utf8_type() {
        let dialect = CqlDialect::new();
        assert_eq!(dialect.utf8_cast_dtype(), ast::DataType::Text);
    }

    #[test]
    fn test_cql_dialect_timestamp_type() {
        let dialect = CqlDialect::new();
        let ts_type = dialect.timestamp_cast_dtype(&arrow::datatypes::TimeUnit::Millisecond, &None);
        assert_eq!(ts_type, ast::DataType::Timestamp(None, TimezoneInfo::None));
    }
}
