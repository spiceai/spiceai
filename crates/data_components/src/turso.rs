/*
Copyright 2025 The Spice.ai OSS Authors

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

#![allow(clippy::doc_markdown)]

//! Turso (libSQL) data components for reading, writing, and deleting data.
//!
//! This module provides the core table provider, execution plan, and data sink
//! implementations for Turso databases. It handles:
//! - Reading data via `TursoTableProvider` and `TursoExec`
//! - Writing data via `TursoDataSink`
//! - Deleting data via `TursoDeletionSink`
//! - Connection pooling via `TursoConnectionPool`
//! - Federation support for cross-database queries
//!
//! # Timestamp Storage Strategy
//!
//! This module supports **two timestamp storage formats** to accommodate different use cases:
//!
//! ## Default: RFC3339 TEXT Format (Recommended)
//!
//! By default, all timestamps are stored as **RFC3339 TEXT strings** (e.g., "2024-01-01T00:00:00.123456789Z").
//! This format preserves all timestamp information without data loss:
//! - ✅ Full nanosecond precision preserved
//! - ✅ Timezone information preserved
//! - ✅ All Arrow timestamp types supported (Second, Millisecond, Microsecond, Nanosecond)
//! - ✅ Human-readable in database tools
//!
//! ## Optional: Integer Milliseconds Format (Performance)
//!
//! For performance-critical use cases, you can opt into storing timestamps as **INTEGER (milliseconds since Unix epoch)**
//! by setting the `internal_timestamp_format` parameter to `"integer_millis"` in your spicepod.yaml acceleration configuration.
//!
//! ### RFC3339 Format Benefits:
//!
//! - ✅ **No data loss**: Full precision and timezone information preserved
//! - ✅ **All types supported**: Second, Millisecond, Microsecond, Nanosecond timestamps all work
//! - ✅ **Human readable**: Easy to inspect and debug in database tools
//! - ✅ **Standard format**: RFC3339 is a widely-recognized ISO 8601 profile
//!
//! ### Integer Milliseconds Format Benefits:
//!
//! - ✅ **Performance**: Direct integer comparisons and arithmetic (faster than string parsing)
//! - ✅ **Compact storage**: 8 bytes vs ~30 bytes for RFC3339 strings
//! - ⚠️ **Millisecond precision only**: Sub-millisecond data is truncated (not rejected)
//! - ⚠️ **No timezone**: Timezone information is not preserved (UTC assumed)
//! - ⚠️ **Limited types**: Only Second and Millisecond timestamps supported (Micro/Nano rejected)
//!
//! ### RFC3339 Format (Default) - Writing:
//!
//! ```text
//! ✅ TimestampSecond(v, tz)      → TEXT "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+05:30"
//! ✅ TimestampMillisecond(v, tz) → TEXT "2024-01-01T00:00:00.123Z"
//! ✅ TimestampMicrosecond(v, tz) → TEXT "2024-01-01T00:00:00.123456Z"
//! ✅ TimestampNanosecond(v, tz)  → TEXT "2024-01-01T00:00:00.123456789Z"
//! ```
//!
//! ### Integer Milliseconds Format (Optional) - Writing:
//!
//! **Configuration**: Set `internal_timestamp_format: "integer_millis"` in spicepod.yaml acceleration params
//!
//! ```text
//! ✅ TimestampSecond(v, None)        → INTEGER (multiply by 1000 to get milliseconds)
//! ✅ TimestampMillisecond(v, None)   → INTEGER (store as-is, already in milliseconds)
//! ❌ TimestampMicrosecond(_, _)      → ERROR (sub-millisecond precision not supported)
//! ❌ TimestampNanosecond(_, _)       → ERROR (sub-millisecond precision not supported)
//! ❌ Timestamp*(_, Some(timezone))   → ERROR (timezone information cannot be preserved)
//! ```
//!
//! ### Reading from Database:
//!
//! The read path automatically detects the storage format (TEXT vs INTEGER) and converts
//! to the Arrow schema's expected timestamp type and unit.

use std::{any::Any, fmt, sync::Arc};

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, Float64Array, Int8Array, Int16Array,
        Int32Array, Int32Builder, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
        IntervalYearMonthArray, LargeBinaryArray, LargeStringArray, ListBuilder, MapBuilder,
        RecordBatch, StringArray, StringBuilder, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array,
    },
    datatypes::{
        DataType, Field, IntervalDayTime, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit, i256,
    },
};
use async_trait::async_trait;
use datafusion_table_providers::util::supported_functions::{
    FunctionSupport, contains_unsupported_functions,
};
use std::ops::ControlFlow;

use datafusion::{
    catalog::Session,
    common::SchemaExt,
    datasource::{
        TableProvider,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
    sql::{
        TableReference,
        sqlparser::ast::{
            BinaryOperator, DateTimeField, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr,
            Ident, ObjectName, ObjectNamePart, Value as SqlValue, ValueWithSpan, VisitMut,
            VisitorMut,
        },
        unparser::{
            Unparser,
            dialect::{Dialect, SqliteDialect},
        },
    },
};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource,
    sql::{
        RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
        ast_analyzer::{AstAnalyzer, AstAnalyzerRule},
    },
};
use futures::stream::{self, StreamExt, TryStreamExt};
use snafu::prelude::*;
use turso::{Builder, Connection, Database, Value as TursoValue};

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};

/// Conversion constants for timestamp storage and conversion.
///
/// # Timestamp Storage Strategy
///
/// This module standardizes on **milliseconds since Unix epoch (INTEGER)** as the canonical
/// storage format for all timestamp types in Turso/SQLite databases. This design choice:
///
/// - **Avoids parsing ambiguity**: No string parsing or format detection required
/// - **Ensures consistency**: Single source of truth for timestamp representation
/// - **Simplifies operations**: Arithmetic operations work directly on integers
/// - **Maximizes compatibility**: SQLite INTEGER type is universally supported
///
/// # Conversions
///
/// All Arrow timestamp types (Second, Millisecond, Microsecond, Nanosecond) are converted
/// to/from milliseconds during database operations:
///
/// - **Writing**: Convert Arrow timestamp → milliseconds → SQLite INTEGER
/// - **Reading**: Convert SQLite INTEGER → milliseconds → Arrow timestamp (with proper unit)
///
/// # Alternative Consideration
///
/// While the current implementation is robust, consider using **RFC3339 TEXT format** if:
/// - Human readability in database tools is a priority
/// - Interoperability with external systems requires string timestamps
/// - Timezone information needs to be preserved in the database
///
/// The integer-based approach is recommended for performance and simplicity in
/// acceleration workloads where timestamps are primarily used for filtering and sorting.
pub mod timestamp_conversion {
    /// Milliseconds per second (1,000)
    pub const MILLIS_PER_SECOND: i64 = 1_000;

    /// Microseconds per millisecond (1,000)
    pub const MICROS_PER_MILLI: i64 = 1_000;

    /// Nanoseconds per millisecond (1,000,000)
    pub const NANOS_PER_MILLI: i64 = 1_000_000;

    /// Nanoseconds per second (1,000,000,000)
    pub const NANOS_PER_SECOND: i64 = 1_000_000_000;

    /// Microseconds per second (1,000,000)
    pub const MICROS_PER_SECOND: i64 = 1_000_000;
}

/// Constants for type conversions
const DECIMAL_BASE: i64 = 10;
const BITS_PER_I32: i32 = 32;
const LOWER_32_MASK: i64 = 0xFFFF_FFFF;

/// Timestamp storage format for Turso databases
///
/// Determines how timestamp values are stored in the database:
/// - `Rfc3339`: Store as RFC3339 TEXT strings (default) - preserves full precision and timezone
/// - `IntegerMillis`: Store as INTEGER milliseconds - higher performance, millisecond precision only
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampFormat {
    /// RFC3339 TEXT format (e.g., "2024-01-01T00:00:00.123456789Z")
    /// - ✅ Preserves full nanosecond precision
    /// - ✅ Preserves timezone information
    /// - ✅ Human-readable in database tools
    /// - ⚠️ Slower performance (string parsing required)
    #[default]
    Rfc3339,

    /// INTEGER milliseconds since Unix epoch
    /// - ✅ Higher performance (direct integer operations)
    /// - ✅ Efficient storage
    /// - ⚠️ Millisecond precision only (sub-millisecond data truncated)
    /// - ⚠️ No timezone preservation (UTC assumed)
    /// - ⚠️ Less readable in database tools
    IntegerMillis,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Turso database error: {source}"))]
    TursoDatabaseError { source: turso::Error },

    #[snafu(display("Failed to get value at column index {col_idx} from row"))]
    MissingColumnValue { col_idx: usize },

    #[snafu(display("Failed to convert u64 value {value} to i64: value exceeds i64::MAX"))]
    UInt64Overflow { value: u64 },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Visitor that transforms INTERVAL expressions to SQLite-compatible datetime() calls
///
/// SQLite doesn't support INTERVAL literals, so we need to transform them to datetime() function calls.
/// This handles expressions like `NOW() - INTERVAL '1' DAY` → `datetime('now', '-1 day')`
struct IntervalTransformer;

impl VisitorMut for IntervalTransformer {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
        // Look for BinaryOp(left, +/-, Interval) patterns
        if let SqlExpr::BinaryOp { left, op, right } = expr
            && let SqlExpr::Interval(interval) = right.as_ref()
        {
            // Found an INTERVAL expression - transform it
            if let Some(transformed) = transform_interval_expr(left, op, interval) {
                *expr = transformed;
            }
        }
        ControlFlow::Continue(())
    }
}

/// Transform INTERVAL expressions to SQLite datetime() function calls
///
/// Handles patterns like:
/// - `column - INTERVAL '1' DAY` → `datetime(column, '-1 day')`
/// - `NOW() + INTERVAL '5' HOUR` → `datetime('now', '+5 hours')`
fn transform_interval_expr(
    base: &SqlExpr,
    op: &BinaryOperator,
    interval: &datafusion::sql::sqlparser::ast::Interval,
) -> Option<SqlExpr> {
    // Only handle +/- operators
    let sign = match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        _ => return None,
    };

    // Extract interval value and unit
    let value_expr = interval.value.as_ref();
    let unit = interval.leading_field.as_ref()?;

    // Get the numeric value from the interval
    let value_str = match value_expr {
        SqlExpr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s),
            ..
        }) => s,
        SqlExpr::Value(ValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => n,
        _ => return None,
    };

    // Map SQL interval units to SQLite datetime modifiers
    let unit_str = match unit {
        DateTimeField::Year => "years",
        DateTimeField::Month => "months",
        DateTimeField::Day => "days",
        DateTimeField::Hour => "hours",
        DateTimeField::Minute => "minutes",
        DateTimeField::Second => "seconds",
        _ => return None, // Unsupported unit
    };

    // Build the datetime modifier string: '+5 hours' or '-1 day'
    let modifier = format!("{sign}{value_str} {unit_str}");

    // Convert base expression
    let base_arg = match base {
        // Special case: NOW() → 'now'
        SqlExpr::Function(func) if is_now_function(func) => SqlExpr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString("now".to_string()),
            span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
        }),
        // Other expressions pass through as-is
        expr => expr.clone(),
    };

    // Build datetime(base, modifier) function call
    Some(SqlExpr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("datetime"))]),
        args: datafusion::sql::sqlparser::ast::FunctionArguments::List(
            datafusion::sql::sqlparser::ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(base_arg)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(ValueWithSpan {
                        value: SqlValue::SingleQuotedString(modifier),
                        span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
                    }))),
                ],
                clauses: vec![],
            },
        ),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: datafusion::sql::sqlparser::ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    }))
}

/// Check if a function is NOW() or CURRENT_TIMESTAMP
fn is_now_function(func: &Function) -> bool {
    if func.args
        != datafusion::sql::sqlparser::ast::FunctionArguments::List(
            datafusion::sql::sqlparser::ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![],
                clauses: vec![],
            },
        )
    {
        return false;
    }

    let name = func.name.to_string().to_uppercase();
    matches!(name.as_str(), "NOW" | "CURRENT_TIMESTAMP")
}

/// Connection pool for Turso databases
///
/// Manages connections to a Turso database (file-based or in-memory).
/// Supports MVCC (Multi-Version Concurrency Control) for concurrent transactions.
///
/// # Architecture
///
/// The pool maintains a shared `Arc<Database>` instance, and each call to `connect()`
/// creates a lightweight connection from this shared database. This is the recommended
/// pattern for high-frequency operations as it:
///
/// - Reuses the underlying database instance
/// - Provides efficient connection management
/// - Supports concurrent access with proper isolation
///
/// # Usage
///
/// ```rust,ignore
/// // Create pool once and share it
/// let pool = Arc::new(TursoConnectionPool::new(":memory:", false).await?);
///
/// // Use pool.connect() for each operation
/// let conn = pool.connect().await?;
/// ```
///
/// For production workloads, prefer using `TursoAccelerator::get_shared_pool()` which
/// caches pool instances per database file for even better performance.
#[derive(Debug)]
pub struct TursoConnectionPool {
    database: Arc<Database>,
    mvcc_enabled: bool,
    db_path: String,
    timestamp_format: TimestampFormat,
}

impl TursoConnectionPool {
    /// Creates a new connection pool for the given database path.
    ///
    /// # Arguments
    /// * `path` - Database path (":memory:" for in-memory, or file path for file-based)
    /// * `mvcc_enabled` - Whether to enable Multi-Version Concurrency Control
    pub async fn new(path: &str, mvcc_enabled: bool) -> Result<Self> {
        Self::new_with_timestamp_format(path, mvcc_enabled, TimestampFormat::default()).await
    }

    /// Creates a new connection pool with specified timestamp format.
    ///
    /// # Arguments
    /// * `path` - Database path (":memory:" for in-memory, or file path for file-based)
    /// * `mvcc_enabled` - Whether to enable Multi-Version Concurrency Control
    /// * `timestamp_format` - Format for storing timestamp values (RFC3339 or integer milliseconds)
    pub async fn new_with_timestamp_format(
        path: &str,
        mvcc_enabled: bool,
        timestamp_format: TimestampFormat,
    ) -> Result<Self> {
        let database = Builder::new_local(path)
            .with_mvcc(mvcc_enabled)
            .build()
            .await
            .context(TursoDatabaseSnafu)?;

        Ok(Self {
            database: Arc::new(database),
            mvcc_enabled,
            db_path: path.to_string(),
            timestamp_format,
        })
    }

    /// Establishes a new connection from the pool
    ///
    /// This method is lightweight and can be called frequently. Each connection
    /// shares the underlying database instance, making it efficient for high-frequency
    /// operations.
    #[allow(clippy::unused_async)]
    pub async fn connect(&self) -> Result<Connection> {
        self.database.connect().context(TursoDatabaseSnafu)
    }

    /// Returns true if MVCC (Multi-Version Concurrency Control) is enabled
    #[must_use]
    pub fn is_mvcc_enabled(&self) -> bool {
        self.mvcc_enabled
    }

    /// Returns true if this is an in-memory database
    #[must_use]
    pub fn is_memory_db(&self) -> bool {
        self.db_path == ":memory:"
    }

    /// Returns the database path
    #[must_use]
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Returns the timestamp format used for this connection pool
    #[must_use]
    pub fn timestamp_format(&self) -> TimestampFormat {
        self.timestamp_format
    }
}

/// Turso Table Provider for reading data
///
/// Implements DataFusion's `TableProvider` trait to enable querying Turso tables.
/// Supports filter pushdown, projection pushdown, and limit pushdown.
#[derive(Debug)]
pub struct TursoTableProvider {
    schema: SchemaRef,
    table_name: String,
    pool: Arc<TursoConnectionPool>,
    pub(crate) function_support: Option<FunctionSupport>,
}

impl TursoTableProvider {
    /// Creates a new Turso table provider
    ///
    /// # Arguments
    /// * `schema` - Arrow schema defining the table structure
    /// * `table_name` - Name of the table in the Turso database
    /// * `pool` - Connection pool for database access
    #[must_use]
    pub fn new(schema: SchemaRef, table_name: String, pool: Arc<TursoConnectionPool>) -> Self {
        Self {
            schema,
            table_name,
            pool,
            function_support: None,
        }
    }

    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    /// Converts Turso database rows to Arrow RecordBatch, matching the exact schema types.
    ///
    /// This function is critical for reading data from Turso - it must respect the schema's
    /// exact data types (e.g., `LargeUtf8` vs `Utf8`, Timestamp units) to avoid type mismatches.
    ///
    /// # Data Integrity During Reads
    ///
    /// **Overflow/Parse Failures → NULL**: When reading data, values that cannot be converted
    /// to the target type (e.g., INTEGER too large for Int8, invalid JSON) are converted to NULL.
    /// This design choice prioritizes query availability over failing entire result sets due to
    /// individual value conversion issues. This is standard behavior for database queries where
    /// some data may be malformed or out of range.
    ///
    /// **Write-time validation is critical**: To prevent bad data from entering the database,
    /// see `scalar_value_to_turso()` which enforces strict validation during INSERT operations.
    ///
    /// # Supported types
    /// - Integers: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
    /// - Floats: Float32, Float64
    /// - Strings: Utf8, LargeUtf8
    /// - Binary: Binary, LargeBinary
    /// - Boolean
    /// - Timestamps: All time units (Second, Millisecond, Microsecond, Nanosecond)
    /// - Dates: Date32, Date64
    /// - Time: Time32, Time64
    /// - Duration, Interval, Decimal128, Decimal256
    /// - Complex types: List, Map (serialized as JSON)
    #[allow(
        clippy::too_many_lines,
        clippy::match_same_arms,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation
    )]
    pub fn values_to_record_batch(
        rows: &[Vec<TursoValue>],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column: Arc<dyn arrow::array::Array> = match field.data_type() {
                DataType::Int8 => {
                    let values: Vec<Option<i8>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i8::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int8Array::from(values))
                }
                DataType::Int16 => {
                    let values: Vec<Option<i16>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i16::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int16Array::from(values))
                }
                DataType::Int32 => {
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i32::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int32Array::from(values))
                }
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::UInt8 => {
                    let values: Vec<Option<u8>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => u8::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(UInt8Array::from(values))
                }
                DataType::UInt16 => {
                    let values: Vec<Option<u16>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => u16::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(UInt16Array::from(values))
                }
                DataType::UInt32 => {
                    let values: Vec<Option<u32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => u32::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(UInt32Array::from(values))
                }
                DataType::UInt64 => {
                    let values: Vec<Option<u64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => u64::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arrow::array::UInt64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => Some(*f),
                            TursoValue::Integer(i) => Some(*i as f64),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Float32 => {
                    let values: Vec<Option<f32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => Some(*f as f32),
                            TursoValue::Integer(i) => Some(*i as f32),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arrow::array::Float32Array::from(values))
                }
                DataType::Utf8 => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => Some(s.clone()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::LargeUtf8 => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => Some(s.clone()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(LargeStringArray::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i != 0),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                DataType::Binary => {
                    let values: Vec<Option<&[u8]>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Blob(b) => Some(b.as_slice()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(BinaryArray::from(values))
                }
                DataType::LargeBinary => {
                    let values: Vec<Option<&[u8]>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Blob(b) => Some(b.as_slice()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(LargeBinaryArray::from(values))
                }
                DataType::Timestamp(unit, tz) => {
                    // Timestamps can be stored in two formats:
                    // 1. RFC3339 TEXT (default): Full precision + timezone preservation
                    // 2. INTEGER milliseconds (performance): Millisecond precision only
                    //
                    // The read path automatically detects and converts both formats to the
                    // schema's expected timestamp type and unit.
                    use timestamp_conversion::{
                        MICROS_PER_MILLI, MILLIS_PER_SECOND, NANOS_PER_MILLI,
                    };

                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(millis) => {
                                // Integer format: Convert from stored milliseconds to the target unit
                                Some(match unit {
                                    TimeUnit::Second => millis / MILLIS_PER_SECOND,
                                    TimeUnit::Millisecond => *millis,
                                    TimeUnit::Microsecond => millis * MICROS_PER_MILLI,
                                    TimeUnit::Nanosecond => millis * NANOS_PER_MILLI,
                                })
                            }
                            TursoValue::Text(rfc3339_str) => {
                                // RFC3339 TEXT format: Parse and convert to target unit
                                use chrono::DateTime;

                                // Parse RFC3339 string
                                if let Ok(dt) = DateTime::parse_from_rfc3339(rfc3339_str) {
                                    let timestamp_nanos = dt.timestamp_nanos_opt().unwrap_or(0);
                                    Some(match unit {
                                        TimeUnit::Second => timestamp_nanos / 1_000_000_000,
                                        TimeUnit::Millisecond => timestamp_nanos / 1_000_000,
                                        TimeUnit::Microsecond => timestamp_nanos / 1_000,
                                        TimeUnit::Nanosecond => timestamp_nanos,
                                    })
                                } else {
                                    // Parse failed, return NULL (lenient read behavior)
                                    None
                                }
                            }
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();

                    match unit {
                        TimeUnit::Second => Arc::new(
                            TimestampSecondArray::from(values).with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Millisecond => Arc::new(
                            TimestampMillisecondArray::from(values).with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Microsecond => Arc::new(
                            TimestampMicrosecondArray::from(values).with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Nanosecond => Arc::new(
                            TimestampNanosecondArray::from(values).with_timezone_opt(tz.clone()),
                        ),
                    }
                }
                DataType::Date32 => {
                    // Date32 stored as days since Unix epoch
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i32::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Date32Array::from(values))
                }
                DataType::Date64 => {
                    // Date64 stored as milliseconds since Unix epoch
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Date64Array::from(values))
                }
                DataType::Time32(unit) => {
                    // Time32 stored as INTEGER (milliseconds or seconds since midnight)
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i32::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    match unit {
                        TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from(values)),
                        TimeUnit::Second => Arc::new(Time32SecondArray::from(values)),
                        _ => {
                            // Fallback to string for unsupported time units
                            let values: Vec<Option<String>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => Some(i.to_string()),
                                    TursoValue::Null => None,
                                    _ => None,
                                })
                                .collect();
                            Arc::new(StringArray::from(values))
                        }
                    }
                }
                DataType::Time64(unit) => {
                    // Time64 stored as INTEGER (microseconds or nanoseconds since midnight)
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    match unit {
                        TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(values)),
                        TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(values)),
                        _ => {
                            // Fallback to string for unsupported time units
                            let values: Vec<Option<String>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => Some(i.to_string()),
                                    TursoValue::Null => None,
                                    _ => None,
                                })
                                .collect();
                            Arc::new(StringArray::from(values))
                        }
                    }
                }
                DataType::Duration(unit) => {
                    // Duration stored as INTEGER
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            _ => None,
                        })
                        .collect();
                    match unit {
                        TimeUnit::Second => Arc::new(DurationSecondArray::from(values)),
                        TimeUnit::Millisecond => Arc::new(DurationMillisecondArray::from(values)),
                        TimeUnit::Microsecond => Arc::new(DurationMicrosecondArray::from(values)),
                        TimeUnit::Nanosecond => Arc::new(DurationNanosecondArray::from(values)),
                    }
                }
                DataType::Interval(unit) => {
                    // Interval stored as INTEGER
                    use arrow::datatypes::IntervalUnit;

                    match unit {
                        IntervalUnit::YearMonth => {
                            let values: Vec<Option<i32>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => i32::try_from(*i).ok(),
                                    _ => None,
                                })
                                .collect();
                            Arc::new(IntervalYearMonthArray::from(values))
                        }
                        IntervalUnit::DayTime => {
                            let values: Vec<Option<IntervalDayTime>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => {
                                        // Unpack i64: upper 32 bits = days, lower 32 bits = milliseconds
                                        const BITS_PER_I32: i32 = 32;
                                        const LOWER_32_MASK: i64 = 0xFFFF_FFFF;

                                        let days = (*i >> BITS_PER_I32) as i32;
                                        let milliseconds = (*i & LOWER_32_MASK) as i32;
                                        Some(IntervalDayTime::new(days, milliseconds))
                                    }
                                    _ => None,
                                })
                                .collect();
                            Arc::new(IntervalDayTimeArray::from(values))
                        }
                        IntervalUnit::MonthDayNano => {
                            let values: Vec<Option<IntervalMonthDayNano>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Text(s) => {
                                        // Deserialize from JSON
                                        serde_json::from_str::<serde_json::Value>(s).ok().and_then(
                                            |v| {
                                                let months =
                                                    i32::try_from(v["months"].as_i64()?).ok()?;
                                                let days =
                                                    i32::try_from(v["days"].as_i64()?).ok()?;
                                                let nanoseconds = v["nanoseconds"].as_i64()?;
                                                Some(IntervalMonthDayNano::new(
                                                    months,
                                                    days,
                                                    nanoseconds,
                                                ))
                                            },
                                        )
                                    }
                                    _ => None,
                                })
                                .collect();
                            Arc::new(IntervalMonthDayNanoArray::from(values))
                        }
                    }
                }
                DataType::List(field) => {
                    // List stored as TEXT (JSON serialized)
                    // Reconstruct the list arrays from JSON - only support Int32 lists for now
                    if matches!(field.data_type(), DataType::Int32) {
                        const ESTIMATED_LIST_SIZE: usize = 3;
                        let mut list_builder =
                            ListBuilder::new(Int32Array::builder(rows.len() * ESTIMATED_LIST_SIZE));

                        for row in rows {
                            match &row[col_idx] {
                                TursoValue::Text(json_str) => {
                                    // Parse JSON array
                                    if let Ok(values) = serde_json::from_str::<Vec<i32>>(json_str) {
                                        for val in values {
                                            list_builder.values().append_value(val);
                                        }
                                        list_builder.append(true);
                                    } else {
                                        list_builder.append_null();
                                    }
                                }
                                _ => {
                                    list_builder.append_null();
                                }
                            }
                        }

                        Arc::new(list_builder.finish())
                    } else {
                        // For unsupported list element types, return empty list array
                        let mut list_builder = ListBuilder::new(Int32Array::builder(0));
                        for _ in rows {
                            list_builder.append_null();
                        }
                        Arc::new(list_builder.finish())
                    }
                }
                DataType::Map(entries_field, _sorted) => {
                    // Map stored as TEXT (JSON serialized)
                    // Reconstruct map arrays from JSON
                    // For now, only support Utf8 keys to Int32 values

                    // Extract field names from the schema's entries field
                    use arrow::array::MapFieldNames;
                    let field_names = if let DataType::Struct(fields) = entries_field.data_type() {
                        if fields.len() >= 2 {
                            MapFieldNames {
                                entry: entries_field.name().clone(),
                                key: fields[0].name().clone(),
                                value: fields[1].name().clone(),
                            }
                        } else {
                            MapFieldNames::default()
                        }
                    } else {
                        MapFieldNames::default()
                    };

                    let keys_builder = StringBuilder::new();
                    let values_builder = Int32Builder::new();
                    let mut map_builder =
                        MapBuilder::new(Some(field_names), keys_builder, values_builder);

                    for row in rows {
                        match &row[col_idx] {
                            TursoValue::Text(json_str) => {
                                // Parse JSON object
                                if let Ok(map) = serde_json::from_str::<
                                    serde_json::Map<String, serde_json::Value>,
                                >(json_str)
                                {
                                    for (key, value) in map {
                                        map_builder.keys().append_value(&key);
                                        if let Some(int_val) = value.as_i64() {
                                            #[allow(clippy::cast_possible_truncation)]
                                            let val = int_val as i32;
                                            map_builder.values().append_value(val);
                                        } else {
                                            map_builder.values().append_null();
                                        }
                                    }
                                    map_builder.append(true).map_err(|e| {
                                        Box::new(std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            format!("Failed to append map: {e}"),
                                        ))
                                            as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                } else {
                                    map_builder.append(false).map_err(|e| {
                                        Box::new(std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            format!("Failed to append null map: {e}"),
                                        ))
                                            as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                }
                            }
                            _ => {
                                map_builder.append(false).map_err(|e| {
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!("Failed to append null map: {e}"),
                                    ))
                                        as Box<dyn std::error::Error + Send + Sync>
                                })?;
                            }
                        }
                    }

                    Arc::new(map_builder.finish())
                }
                DataType::Decimal128(precision, scale) => {
                    // Decimal128 stored as REAL in database
                    // Convert back to i128 scaled value
                    const DECIMAL_BASE: i128 = 10;
                    #[allow(clippy::cast_sign_loss)]
                    let scale_factor = DECIMAL_BASE.pow(*scale as u32);
                    let values: Vec<Option<i128>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                #[allow(
                                    clippy::cast_possible_truncation,
                                    clippy::cast_precision_loss
                                )]
                                let scaled = (f * scale_factor as f64).round() as i128;
                                Some(scaled)
                            }
                            TursoValue::Integer(i) => {
                                // If stored as integer, scale it
                                Some(i128::from(*i) * scale_factor)
                            }
                            _ => None,
                        })
                        .collect();
                    Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(*precision, *scale)
                            .map_err(|e| {
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid decimal128 precision/scale: {e}"),
                                ))
                                    as Box<dyn std::error::Error + Send + Sync>
                            })?,
                    )
                }
                DataType::Decimal256(precision, scale) => {
                    // Decimal256 stored as REAL in database
                    // Convert back to i256 scaled value
                    const DECIMAL_BASE: i128 = 10;
                    #[allow(clippy::cast_sign_loss)]
                    let scale_factor = DECIMAL_BASE.pow(*scale as u32);
                    let values: Vec<Option<i256>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                #[allow(
                                    clippy::cast_possible_truncation,
                                    clippy::cast_precision_loss
                                )]
                                let scaled = (f * scale_factor as f64).round() as i128;
                                Some(i256::from_i128(scaled))
                            }
                            TursoValue::Integer(i) => {
                                // If stored as integer, scale it
                                let scaled = i128::from(*i) * scale_factor;
                                Some(i256::from_i128(scaled))
                            }
                            _ => None,
                        })
                        .collect();
                    Arc::new(
                        Decimal256Array::from(values)
                            .with_precision_and_scale(*precision, *scale)
                            .map_err(|e| {
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid decimal256 precision/scale: {e}"),
                                ))
                                    as Box<dyn std::error::Error + Send + Sync>
                            })?,
                    )
                }
                _ => {
                    // Default to string representation for unsupported types
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => Some(s.clone()),
                            TursoValue::Integer(i) => Some(i.to_string()),
                            TursoValue::Real(f) => Some(f.to_string()),
                            TursoValue::Null => None,
                            TursoValue::Blob(_) => Some("[BLOB]".to_string()),
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            columns.push(column);
        }

        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    }

    /// Returns AST analyzer rules for Turso-specific SQL transformations
    ///
    /// Turso uses `SQLite` dialect which doesn't support INTERVAL literals.
    /// This analyzer transforms INTERVAL expressions to SQLite-compatible `datetime()` calls.
    ///
    /// Examples:
    /// - `NOW() - INTERVAL '1' DAY` → `datetime('now', '-1 day')`
    /// - `timestamp + INTERVAL '5' HOUR` → `datetime(timestamp, '+5 hours')`
    fn turso_ast_analyzer() -> AstAnalyzerRule {
        Box::new(|mut ast| {
            // Transform INTERVAL expressions to SQLite datetime() calls
            transform_intervals(&mut ast);
            Ok(ast)
        })
    }
}

/// Transforms INTERVAL expressions in a SQL statement to SQLite-compatible `datetime()` calls
fn transform_intervals(statement: &mut datafusion::sql::sqlparser::ast::Statement) {
    let mut transformer = IntervalTransformer;
    let _ = statement.visit(&mut transformer);
}

#[async_trait]
impl TableProvider for TursoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let dialect = SqliteDialect {};
        let unparser = Unparser::new(&dialect);

        let mut filter_push_down = vec![];
        for filter in filters {
            match unparser.expr_to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }
        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Handle projection pushdown: create a schema with only the requested columns
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };

        Ok(Arc::new(TursoExec::new(
            Arc::clone(&projected_schema),
            self.table_name.clone(),
            Arc::clone(&self.pool),
            filters,
            limit,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Check that the input schema matches the table schema
        if let Err(e) = self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
        {
            return Err(DataFusionError::Execution(format!(
                "Inserting query must have the same schema as the table. {e}"
            )));
        }

        // Create the data sink for INSERT operations
        let sink = Arc::new(TursoDataSink::new(
            Arc::clone(&self.pool),
            self.table_name.clone(),
            Arc::clone(&self.schema),
        ));

        // Wrap in DataSinkExec to execute the insertion
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }
}

#[async_trait]
impl DeletionTableProvider for TursoTableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(TursoDeletionSink::new(
                Arc::clone(&self.pool),
                self.table_name.clone(),
                filters,
            )),
            &self.schema(),
        )))
    }
}

// Federation support for Turso
impl TursoTableProvider {
    /// Creates a federated table source for cross-database queries
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_name = TableReference::bare(self.table_name.clone());
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));

        Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_name),
            schema,
        ))
    }

    /// Creates a federated table provider that supports query federation
    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self));
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[async_trait]
impl SQLExecutor for TursoTableProvider {
    fn name(&self) -> &str {
        &self.table_name
    }

    fn compute_context(&self) -> Option<String> {
        None
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(SqliteDialect {})
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(AstAnalyzer::new(vec![Self::turso_ast_analyzer()]))
    }

    fn can_execute_plan(&self, plan: &LogicalPlan) -> bool {
        // Default to not federate if [`Self::function_support`] provided, otherwise true.
        self.function_support.as_ref().is_none_or(|func_supp| {
            !contains_unsupported_functions(plan, func_supp).unwrap_or(false)
        })
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let query = query.to_string();
        let schema_clone = Arc::clone(&schema);

        let fut = async move {
            let conn = pool.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to connect to Turso: {e}"))
            })?;

            let mut rows = conn
                .query(&query, ())
                .await
                .map_err(|e| DataFusionError::Execution(format!("Turso query failed: {e}")))?;

            let mut rows_vec: Vec<Vec<TursoValue>> = Vec::new();
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to fetch row: {e}")))?
            {
                let mut values = Vec::new();
                for i in 0..schema_clone.fields().len() {
                    let value = row.get_value(i).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to get value at index {i}: {e}"))
                    })?;
                    values.push(value);
                }
                rows_vec.push(values);
            }

            if rows_vec.is_empty() {
                return Ok(RecordBatch::new_empty(schema_clone));
            }

            TursoTableProvider::values_to_record_batch(&rows_vec, &schema_clone).map_err(|e| {
                DataFusionError::Execution(format!("Failed to convert Turso results: {e}"))
            })
        };
        let stream = futures::stream::once(fut).boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        let conn =
            self.pool.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to connect to Turso: {e}"))
            })?;

        // Query the table schema using SQLite's pragma
        let query = format!("PRAGMA table_info({table_name})");
        let mut rows = conn
            .query(&query, ())
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get table schema: {e}")))?;

        let mut fields = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to fetch schema row: {e}")))?
        {
            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            let col_name = row.get_value(1).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get column name: {e}"))
            })?;
            let col_type = row.get_value(2).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get column type: {e}"))
            })?;
            let not_null = row.get_value(3).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get notnull flag: {e}"))
            })?;

            if let (
                TursoValue::Text(col_name),
                TursoValue::Text(col_type),
                TursoValue::Integer(not_null),
            ) = (&col_name, &col_type, &not_null)
            {
                let data_type = match col_type.to_uppercase().as_str() {
                    "INTEGER" | "BLOB" => DataType::Int64,
                    "REAL" | "FLOAT" | "DOUBLE" => DataType::Float64,
                    _ => DataType::Utf8,
                };
                let nullable = *not_null == 0;
                fields.push(Field::new(col_name.as_str(), data_type, nullable));
            }
        }

        if fields.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Table '{table_name}' not found or has no columns"
            )));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

/// Execution plan for Turso queries
///
/// Handles query execution with pushdown support for projections, filters, and limits.
#[derive(Debug)]
pub struct TursoExec {
    schema: SchemaRef,
    table_name: String,
    pool: Arc<TursoConnectionPool>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl TursoExec {
    /// Creates a new Turso execution plan
    ///
    /// # Arguments
    /// * `schema` - Schema of the result set (may be projected)
    /// * `table_name` - Name of the table to query
    /// * `pool` - Connection pool
    /// * `filters` - Filter expressions to push down
    /// * `limit` - Optional row limit
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        table_name: String,
        pool: Arc<TursoConnectionPool>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            table_name,
            pool,
            filters: filters.to_vec(),
            limit,
            properties,
        }
    }

    /// Build the SQL query with projection, filters, and limit
    ///
    /// Note: Projection pushdown is handled by the schema parameter,
    /// which is already the projected schema from `scan()`.
    fn sql(&self) -> datafusion::error::Result<String> {
        // Build column list from projected schema
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let dialect = SqliteDialect {};
            let unparser = Unparser::new(&dialect);
            let filter_sqls: Vec<String> = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|ast| format!("{ast}")))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            format!(" WHERE {}", filter_sqls.join(" AND "))
        };

        let limit_expr = match self.limit {
            Some(limit) => format!(" LIMIT {limit}"),
            None => String::new(),
        };

        Ok(format!(
            "SELECT {} FROM {}{}{}",
            columns, self.table_name, where_expr, limit_expr
        ))
    }
}

impl DisplayAs for TursoExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let table_name = &self.table_name;
        let sql = self
            .sql()
            .unwrap_or_else(|_| format!("SELECT * FROM {table_name}"));
        write!(f, "TursoExec sql={sql}")
    }
}

impl ExecutionPlan for TursoExec {
    fn name(&self) -> &'static str {
        "TursoExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let schema = Arc::clone(&self.schema);
        let query = self.sql()?;

        let stream = async move {
            let conn = pool
                .connect()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(&query)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut all_rows = Vec::new();
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            {
                let mut values = Vec::new();
                for i in 0..schema.fields().len() {
                    let value = row
                        .get_value(i)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    values.push(value);
                }
                all_rows.push(values);
            }

            if all_rows.is_empty() {
                return Ok::<_, datafusion::error::DataFusionError>(stream::empty().boxed());
            }

            let batch = TursoTableProvider::values_to_record_batch(&all_rows, &schema)
                .map_err(datafusion::error::DataFusionError::External)?;

            Ok::<_, datafusion::error::DataFusionError>(
                stream::once(async move { Ok(batch) }).boxed(),
            )
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream::once(stream).try_flatten(),
        )))
    }
}

/// Deletion sink for DELETE operations
pub struct TursoDeletionSink {
    pool: Arc<TursoConnectionPool>,
    table_name: String,
    filters: Vec<Expr>,
}

impl TursoDeletionSink {
    /// Creates a new deletion sink
    #[must_use]
    pub fn new(pool: Arc<TursoConnectionPool>, table_name: String, filters: &[Expr]) -> Self {
        Self {
            pool,
            table_name,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for TursoDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Build WHERE clause using SQLite dialect unparser
        let where_clause = if self.filters.is_empty() {
            String::new()
        } else {
            let dialect = SqliteDialect {};
            let unparser = Unparser::new(&dialect);
            let filter_sqls: Vec<String> = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|ast| format!("{ast}")))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            format!(" WHERE {}", filter_sqls.join(" AND "))
        };

        let delete_sql = format!("DELETE FROM {}{}", self.table_name, where_clause);

        let conn = self.pool.connect().await?;
        let rows_affected = conn
            .execute(&delete_sql, ())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(rows_affected)
    }
}

/// Data sink for INSERT operations
#[derive(Debug)]
pub struct TursoDataSink {
    pool: Arc<TursoConnectionPool>,
    table_name: String,
    schema: SchemaRef,
}

impl DisplayAs for TursoDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TursoDataSink(table={})", self.table_name)
    }
}

impl TursoDataSink {
    /// Creates a new data sink for INSERT operations
    #[must_use]
    pub fn new(pool: Arc<TursoConnectionPool>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            pool,
            table_name,
            schema,
        }
    }

    /// Inserts a batch of records into the Turso database
    async fn insert_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let conn = self.pool.connect().await?;

        // Build column list and placeholders for prepared statement
        let columns: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let placeholders = (1..=columns.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name,
            columns.join(", "),
            placeholders
        );

        // Use a transaction to batch all inserts
        // If MVCC is enabled, use BEGIN CONCURRENT for better concurrency
        let begin_stmt = if self.pool.is_mvcc_enabled() {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN"
        };
        conn.execute(begin_stmt, ()).await?;

        // Prepare the statement once
        let mut stmt = conn.prepare(&insert_sql).await?;

        // Execute for each row using prepared statement (much faster than building SQL strings)
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = ScalarValue::try_from_array(column, row_idx)?;

                // Convert DataFusion ScalarValue to Turso Value
                let turso_value = scalar_value_to_turso(value, self.pool.timestamp_format())?;
                values.push(turso_value);
            }

            // Execute the prepared statement with parameters (fast!)
            stmt.execute(values).await?;
        }

        // Commit the transaction
        conn.execute("COMMIT", ()).await?;

        Ok(())
    }
}

#[async_trait]
impl DataSink for TursoDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let mut total_rows = 0u64;

        while let Some(batch) = data.next().await {
            let batch = batch?;
            total_rows += batch.num_rows() as u64;
            self.insert_batch(&batch)
                .await
                .map_err(datafusion::error::DataFusionError::External)?;
        }

        Ok(total_rows)
    }
}

/// Converts a timestamp value to Turso storage format (RFC3339 TEXT or INTEGER milliseconds).
///
/// # Arguments
/// * `value` - The timestamp value in its native unit
/// * `unit` - The time unit of the input value (Second, Millisecond, Microsecond, Nanosecond)
/// * `timezone` - Optional timezone string (e.g., "UTC", "+05:30")
/// * `format` - Storage format (Rfc3339 or `IntegerMillis`)
///
/// # RFC3339 Format (Default)
/// Preserves full precision and timezone information as TEXT:
/// - Second: "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+05:30"
/// - Millisecond: "2024-01-01T00:00:00.123Z"
/// - Microsecond: "2024-01-01T00:00:00.123456Z"
/// - Nanosecond: "2024-01-01T00:00:00.123456789Z"
///
/// # Integer Milliseconds Format (Performance)
/// Stores as INTEGER milliseconds, with limitations:
/// - Only Second and Millisecond units supported (without timezone)
/// - Microsecond/Nanosecond rejected (precision loss not acceptable)
/// - Timezone-aware timestamps rejected (timezone info loss not acceptable)
fn convert_timestamp_to_turso(
    value: i64,
    unit: TimeUnit,
    timezone: Option<&str>,
    format: TimestampFormat,
) -> Result<TursoValue, Box<dyn std::error::Error + Send + Sync>> {
    match format {
        TimestampFormat::Rfc3339 => {
            // Convert to RFC3339 string format
            use chrono::{DateTime, Utc};

            // Convert value to nanoseconds
            let nanos = match unit {
                TimeUnit::Second => value * timestamp_conversion::NANOS_PER_SECOND,
                TimeUnit::Millisecond => value * timestamp_conversion::NANOS_PER_MILLI,
                TimeUnit::Microsecond => value * 1_000,
                TimeUnit::Nanosecond => value,
            };

            // Split into seconds and subsecond nanos
            let secs = nanos / timestamp_conversion::NANOS_PER_SECOND;
            #[allow(clippy::cast_sign_loss)]
            let nsecs = (nanos % timestamp_conversion::NANOS_PER_SECOND) as u32;

            // Create NaiveDateTime using the new DateTime::from_timestamp API
            let naive = DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| format!("Invalid timestamp value: {value} {unit:?}"))?
                .naive_utc();

            // Format with timezone
            let rfc3339 = if let Some(tz_str) = timezone {
                // Parse and apply timezone
                if tz_str == "UTC" || tz_str == "Z" || tz_str == "+00:00" {
                    DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).to_rfc3339()
                } else {
                    // For other timezones, include the timezone in the output
                    // chrono doesn't support arbitrary timezone parsing, so we format with offset
                    format!("{}{}", naive.format("%Y-%m-%dT%H:%M:%S%.f"), tz_str)
                }
            } else {
                // No timezone specified, use UTC
                DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).to_rfc3339()
            };

            Ok(TursoValue::Text(rfc3339))
        }
        TimestampFormat::IntegerMillis => {
            // Integer milliseconds format - strict validation
            if let Some(tz) = timezone {
                return Err(format!(
                    "Timestamp with timezone '{tz}' not supported with integer_millis format - use rfc3339 format to preserve timezone information"
                ).into());
            }

            match unit {
                TimeUnit::Second => Ok(TursoValue::Integer(value * timestamp_conversion::MILLIS_PER_SECOND)),
                TimeUnit::Millisecond => Ok(TursoValue::Integer(value)),
                TimeUnit::Microsecond => Err(
                    "TimestampMicrosecond not supported with integer_millis format - use rfc3339 format to preserve sub-millisecond precision"
                        .to_string()
                        .into(),
                ),
                TimeUnit::Nanosecond => Err(
                    "TimestampNanosecond not supported with integer_millis format - use rfc3339 format to preserve sub-millisecond precision"
                        .to_string()
                        .into(),
                ),
            }
        }
    }
}

/// Converts a `DataFusion` `ScalarValue` to a Turso Value for database insertion.
///
/// # Timestamp Handling
///
/// Timestamp conversion depends on the configured `internal_timestamp_format`:
///
/// ## RFC3339 Format (Default)
/// All timestamp types converted to RFC3339 TEXT strings with full precision and timezone preservation:
/// - `TimestampSecond(v, tz)` → TEXT "2024-01-01T00:00:00Z" (or with timezone offset)
/// - `TimestampMillisecond(v, tz)` → TEXT "2024-01-01T00:00:00.123Z"
/// - `TimestampMicrosecond(v, tz)` → TEXT "2024-01-01T00:00:00.123456Z"
/// - `TimestampNanosecond(v, tz)` → TEXT "2024-01-01T00:00:00.123456789Z"
///
/// ## Integer Milliseconds Format (Performance)
/// Only Second/Millisecond without timezone supported, others rejected:
/// - `TimestampSecond(v, None)` → INTEGER (milliseconds)
/// - `TimestampMillisecond(v, None)` → INTEGER (milliseconds)
/// - `TimestampMicrosecond(_, _)` → ERROR
/// - `TimestampNanosecond(_, _)` → ERROR
/// - `Timestamp*(_, Some(_))` → ERROR
///
/// Configure via spicepod.yaml: `acceleration.params.internal_timestamp_format: "rfc3339"` or `"integer_millis"`
#[allow(clippy::too_many_lines, clippy::match_same_arms)]
fn scalar_value_to_turso(
    value: ScalarValue,
    timestamp_format: TimestampFormat,
) -> Result<TursoValue, Box<dyn std::error::Error + Send + Sync>> {
    use arrow::array::{Array, MapArray};

    let turso_value = match value {
        ScalarValue::Int64(Some(v)) => TursoValue::Integer(v),
        ScalarValue::Int32(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::Int16(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::Int8(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::UInt64(Some(v)) => {
            // UInt64 values exceeding i64::MAX cannot be stored in SQLite INTEGER
            // Fail explicitly to preserve data integrity rather than silently truncating
            TursoValue::Integer(i64::try_from(v).map_err(|_| {
                format!("UInt64 value {v} exceeds i64::MAX and cannot be stored in Turso INTEGER type. Consider using REAL or TEXT for large unsigned values.")
            })?)
        }
        ScalarValue::UInt32(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::UInt16(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::UInt8(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::Float64(Some(v)) => TursoValue::Real(v),
        ScalarValue::Float32(Some(v)) => TursoValue::Real(f64::from(v)),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => TursoValue::Text(v),
        ScalarValue::Boolean(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => TursoValue::Blob(v),

        // Timestamp conversions: Format depends on configuration
        ScalarValue::TimestampSecond(Some(v), tz) => {
            convert_timestamp_to_turso(v, TimeUnit::Second, tz.as_deref(), timestamp_format)?
        }
        ScalarValue::TimestampMillisecond(Some(v), tz) => {
            convert_timestamp_to_turso(v, TimeUnit::Millisecond, tz.as_deref(), timestamp_format)?
        }
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            convert_timestamp_to_turso(v, TimeUnit::Microsecond, tz.as_deref(), timestamp_format)?
        }
        ScalarValue::TimestampNanosecond(Some(v), tz) => {
            convert_timestamp_to_turso(v, TimeUnit::Nanosecond, tz.as_deref(), timestamp_format)?
        }

        ScalarValue::Date32(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::Date64(Some(v)) => TursoValue::Integer(v),
        ScalarValue::Time32Second(Some(v)) | ScalarValue::Time32Millisecond(Some(v)) => {
            TursoValue::Integer(i64::from(v))
        }
        ScalarValue::Time64Microsecond(Some(v)) | ScalarValue::Time64Nanosecond(Some(v)) => {
            TursoValue::Integer(v)
        }
        ScalarValue::DurationSecond(Some(v))
        | ScalarValue::DurationMillisecond(Some(v))
        | ScalarValue::DurationMicrosecond(Some(v))
        | ScalarValue::DurationNanosecond(Some(v)) => TursoValue::Integer(v),
        ScalarValue::IntervalYearMonth(Some(v)) => TursoValue::Integer(i64::from(v)),
        ScalarValue::IntervalDayTime(Some(v)) => {
            // IntervalDayTime has days (i32) and milliseconds (i32)
            // Pack into i64: upper 32 bits = days, lower 32 bits = milliseconds
            let packed =
                (i64::from(v.days) << BITS_PER_I32) | (i64::from(v.milliseconds) & LOWER_32_MASK);
            TursoValue::Integer(packed)
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            // IntervalMonthDayNano has 3 fields - serialize as JSON
            let json = serde_json::json!({
                "months": v.months,
                "days": v.days,
                "nanoseconds": v.nanoseconds
            });
            TursoValue::Text(json.to_string())
        }
        ScalarValue::Decimal128(Some(v), _, scale) => {
            // Convert decimal to float for storage as REAL
            #[allow(clippy::cast_precision_loss)]
            let scale_factor = (DECIMAL_BASE as f64).powi(i32::from(scale));
            #[allow(clippy::cast_precision_loss)]
            let v_f64 = v as f64;
            TursoValue::Real(v_f64 / scale_factor)
        }
        ScalarValue::Decimal256(Some(v), _, scale) => {
            // Convert decimal256 to float for storage as REAL
            #[allow(clippy::cast_precision_loss)]
            let scale_factor = (DECIMAL_BASE as f64).powi(i32::from(scale));
            let v_str = format!("{v}");
            let v_f64 = v_str
                .parse::<f64>()
                .map_err(|e| format!("Failed to parse Decimal256 value '{v_str}' as f64: {e}"))?;
            TursoValue::Real(v_f64 / scale_factor)
        }
        ScalarValue::List(list_arr) => {
            // Serialize list as JSON
            let mut json_values = Vec::new();
            for i in 0..list_arr.len() {
                if list_arr.is_null(i) {
                    json_values.push(serde_json::Value::Null);
                } else {
                    let elem = ScalarValue::try_from_array(list_arr.as_ref(), i)?;
                    match elem {
                        ScalarValue::Int32(Some(v)) => json_values.push(serde_json::Value::from(v)),
                        ScalarValue::Int64(Some(v)) => json_values.push(serde_json::Value::from(v)),
                        ScalarValue::Utf8(Some(v)) => json_values.push(serde_json::Value::from(v)),
                        _ => json_values.push(serde_json::Value::Null),
                    }
                }
            }
            TursoValue::Text(
                serde_json::to_string(&json_values)
                    .map_err(|e| format!("Failed to serialize List as JSON: {e}"))?,
            )
        }
        ScalarValue::Map(map_arr) => {
            // Map is a StructArray with "entries" containing keys and values
            // Serialize as JSON object
            let map_array = map_arr
                .as_ref()
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Expected MapArray",
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;

            let mut json_map = serde_json::Map::new();

            // Get keys and values from the map
            let keys = map_array.keys();
            let values = map_array.values();

            for i in 0..keys.len() {
                if !keys.is_null(i) && !values.is_null(i) {
                    // Extract key as string
                    let key_scalar = ScalarValue::try_from_array(keys.as_ref(), i)?;
                    let ScalarValue::Utf8(Some(key_str)) = key_scalar else {
                        continue;
                    };

                    // Extract value
                    let val_scalar = ScalarValue::try_from_array(values.as_ref(), i)?;
                    let val_json = match val_scalar {
                        ScalarValue::Int32(Some(v)) => serde_json::Value::from(v),
                        ScalarValue::Int64(Some(v)) => serde_json::Value::from(v),
                        _ => serde_json::Value::Null,
                    };

                    json_map.insert(key_str, val_json);
                }
            }

            TursoValue::Text(
                serde_json::to_string(&json_map)
                    .map_err(|e| format!("Failed to serialize Map as JSON: {e}"))?,
            )
        }
        _ => TursoValue::Null,
    };

    Ok(turso_value)
}
