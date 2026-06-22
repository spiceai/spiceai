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

use std::{fmt, sync::Arc};

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
use datafusion::{
    catalog::Session,
    common::{SchemaExt, utils::quote_identifier},
    datasource::{
        TableProvider,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
    sql::{
        TableReference,
        sqlparser::ast::{self as sqlast, VisitMut, VisitorMut},
        unparser::{
            Unparser,
            dialect::{
                CharacterLengthStyle, DateFieldExtractStyle, Dialect, IntervalStyle, SqliteDialect,
            },
        },
    },
};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource,
    sql::{
        AstAnalyzer, AstAnalyzerRule, LogicalOptimizer, RemoteTableRef, SQLExecutor,
        SQLFederationProvider, SQLTableSource,
    },
};
use datafusion_table_providers::sqlite::sqlite_interval::SQLiteIntervalVisitor;

use crate::function_support::{FunctionSupport, unfederate_plan_with_unsupported_functions};
use futures::stream::{self, StreamExt, TryStreamExt};
use snafu::prelude::*;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use turso::{Builder, Connection, Database, Value as TursoValue};
use turso_shared::{BEGIN_CONCURRENT_SQL, COMMIT_SQL, JOURNAL_MODE_SQL_LITERAL};

use crate::delete::{DeletionExec, DeletionSink};

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

/// Turso dialect wrapping `SqliteDialect` with Turso-specific overrides.
///
/// Differences from plain `SqliteDialect`:
/// - `now()` is rewritten to `datetime('now')` (libSQL has no `now()` function)
struct TursoDialect {
    inner: SqliteDialect,
}

impl TursoDialect {
    fn new() -> Self {
        Self {
            inner: SqliteDialect {},
        }
    }
}

impl Dialect for TursoDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn supports_qualify(&self) -> bool {
        self.inner.supports_qualify()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn date32_cast_dtype(&self) -> sqlast::DataType {
        self.inner.date32_cast_dtype()
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.inner.character_length_style()
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        self.inner.supports_column_alias_in_table_alias()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn timestamp_cast_dtype(
        &self,
        time_unit: &arrow::datatypes::TimeUnit,
        tz: &Option<Arc<str>>,
    ) -> sqlast::DataType {
        self.inner.timestamp_cast_dtype(time_unit, tz)
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> DataFusionResult<Option<sqlast::Expr>> {
        // Override: libSQL/Turso has no built-in now() function;
        // rewrite to datetime('now') which is the SQLite/libSQL equivalent.
        if func_name == "now" {
            return Ok(Some(unparser.scalar_function_to_sql(
                "datetime",
                &[Expr::Literal(
                    ScalarValue::Utf8(Some("now".to_string())),
                    None,
                )],
            )?));
        }
        self.inner
            .scalar_function_to_sql_overrides(unparser, func_name, args)
    }
}

/// Connection pool for Turso databases
///
/// Manages connections to a Turso database (file-based or in-memory).
/// Supports concurrent transactions using MVCC mode with `BEGIN CONCURRENT`.
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
/// let pool = Arc::new(TursoConnectionPool::new(":memory:").await?);
///
/// // Use pool.connect() for each operation
/// let conn = pool.connect().await?;
/// ```
///
/// Note: Concurrent transactions are enabled by configuring
/// `PRAGMA journal_mode = 'mvcc'` during pool creation.
/// This enables BEGIN CONCURRENT transactions for better concurrency.
///
/// For production workloads, prefer using `TursoAccelerator::get_shared_pool()` which
/// caches pool instances per database file for even better performance.
#[derive(Debug)]
pub struct TursoConnectionPool {
    database: Arc<Database>,
    db_path: String,
    timestamp_format: TimestampFormat,
    schema_lock: Arc<RwLock<()>>,
}

impl TursoConnectionPool {
    /// Creates a new connection pool for the given database path.
    ///
    /// # Arguments
    /// * `path` - Database path (":memory:" for in-memory, or file path for file-based)
    pub async fn new(path: &str) -> Result<Self> {
        Self::new_with_timestamp_format(path, TimestampFormat::default()).await
    }

    /// Creates a new connection pool with specified timestamp format.
    ///
    /// # Arguments
    /// * `path` - Database path (":memory:" for in-memory, or file path for file-based)
    /// * `timestamp_format` - Format for storing timestamp values (RFC3339 or integer milliseconds)
    ///
    /// Note: BEGIN CONCURRENT requires MVCC journal mode, which is configured during pool creation.
    pub async fn new_with_timestamp_format(
        path: &str,
        timestamp_format: TimestampFormat,
    ) -> Result<Self> {
        let database = Builder::new_local(path)
            .build()
            .await
            .context(TursoDatabaseSnafu)?;

        // BEGIN CONCURRENT requires MVCC journal mode for concurrent writers.
        let conn = database.connect().context(TursoDatabaseSnafu)?;
        conn.pragma_update("journal_mode", JOURNAL_MODE_SQL_LITERAL)
            .await
            .context(TursoDatabaseSnafu)?;

        Ok(Self {
            database: Arc::new(database),
            db_path: path.to_string(),
            timestamp_format,
            schema_lock: Arc::new(RwLock::new(())),
        })
    }

    /// Establishes a new connection from the pool
    ///
    /// This method is lightweight and can be called frequently. Each connection
    /// shares the underlying database instance, making it efficient for high-frequency
    /// operations.
    ///
    /// Per-connection performance PRAGMAs are applied on every connection here.
    /// Unlike `journal_mode = mvcc` (which persists on the database file and is set
    /// once at pool creation), `synchronous` and `cache_size` are per-connection and
    /// do not carry over — without this, every accelerator connection would default
    /// to `synchronous = FULL` (an extra fsync per commit). These are cheap in-memory
    /// settings, so they are net-positive even on hot paths.
    pub async fn connect(&self) -> Result<Connection> {
        let conn = self.database.connect().context(TursoDatabaseSnafu)?;

        // Wait for locks instead of immediately returning SQLITE_BUSY.
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .context(TursoDatabaseSnafu)?;

        // NORMAL synchronous: safe under WAL/MVCC, avoids FULL's extra per-commit fsync.
        conn.execute("PRAGMA synchronous = NORMAL", ())
            .await
            .context(TursoDatabaseSnafu)?;

        // 32MB page cache (negative value = kilobytes), matching the metastore connection.
        conn.execute("PRAGMA cache_size = -32768", ())
            .await
            .context(TursoDatabaseSnafu)?;

        Ok(conn)
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

    /// Holds schema changes out while a Turso write transaction is open.
    ///
    /// Turso returns "Database schema conflict" if a `BEGIN CONCURRENT` write
    /// transaction commits after another connection changes the schema. DML uses
    /// a shared guard so concurrent writers can still proceed; DDL uses the
    /// exclusive guard below.
    pub async fn acquire_schema_read_lock(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.schema_lock).read_owned().await
    }

    /// Acquires exclusive access for schema-changing statements such as
    /// `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE`, and `DROP TABLE`.
    pub async fn acquire_schema_write_lock(&self) -> OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.schema_lock).write_owned().await
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
    #[expect(
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
                                // Integer format: Convert from stored milliseconds to the target unit.
                                // Use checked arithmetic for Nanosecond to avoid silent overflow
                                // for dates outside ~1677-2262 (same range as timestamp_nanos_opt).
                                match unit {
                                    TimeUnit::Second => Some(millis / MILLIS_PER_SECOND),
                                    TimeUnit::Millisecond => Some(*millis),
                                    TimeUnit::Microsecond => millis.checked_mul(MICROS_PER_MILLI),
                                    TimeUnit::Nanosecond => millis.checked_mul(NANOS_PER_MILLI),
                                }
                            }
                            TursoValue::Text(rfc3339_str) => {
                                // RFC3339 TEXT format: Parse and convert to target unit.
                                // Use unit-appropriate methods to avoid i64 nanosecond overflow
                                // for dates outside ~1677-2262.
                                use chrono::DateTime;

                                // Parse RFC3339 string
                                if let Ok(dt) = DateTime::parse_from_rfc3339(rfc3339_str) {
                                    match unit {
                                        TimeUnit::Second => Some(dt.timestamp()),
                                        TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                                        TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                                        TimeUnit::Nanosecond => dt.timestamp_nanos_opt(),
                                    }
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
                    // Date32 stored as ISO-8601 date string (e.g. '1993-07-01')
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .ok_or_else(|| Error::MissingColumnValue { col_idx })?;
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                .ok()
                                .and_then(|d| {
                                    i32::try_from(d.signed_duration_since(epoch).num_days()).ok()
                                }),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Date32Array::from(values))
                }
                DataType::Date64 => {
                    // Date64 stored as ISO-8601 date string (e.g. '1993-07-01')
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .ok_or_else(|| Error::MissingColumnValue { col_idx })?;
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                .ok()
                                .map(|d| d.signed_duration_since(epoch).num_days() * 86_400_000),
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
                                            #[expect(clippy::cast_possible_truncation)]
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
                    #[expect(clippy::cast_sign_loss)]
                    let scale_factor = DECIMAL_BASE.pow(*scale as u32);
                    let values: Vec<Option<i128>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                #[expect(
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
                    #[expect(clippy::cast_sign_loss)]
                    let scale_factor = DECIMAL_BASE.pow(*scale as u32);
                    let values: Vec<Option<i256>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                #[expect(
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

    /// Returns AST analyzer rules for Turso-specific SQL transformations.
    ///
    /// Uses the shared `SQLiteIntervalVisitor` to convert INTERVAL literals to
    /// `CAST(date/datetime(col, modifier) AS TEXT)` calls.
    ///
    /// Also applies `TursoBetweenVisitor` to rewrite numeric `BETWEEN` expressions
    /// into explicit `CAST(... AS REAL)` comparisons, since Turso does not have the
    /// `decimal_cmp()` extension available in standard SQLite.
    fn turso_ast_analyzer() -> AstAnalyzerRule {
        Box::new(|mut ast| {
            let mut interval_visitor = SQLiteIntervalVisitor::default();
            let _ = ast.visit(&mut interval_visitor);
            let mut between_visitor = TursoBetweenVisitor;
            let _ = ast.visit(&mut between_visitor);
            Ok(ast)
        })
    }

    /// Returns `true` if the expression contains any subquery or outer reference column.
    fn contains_subquery_or_outer_ref(expr: &Expr) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        let mut found = false;
        let _ = expr.apply(|e| match e {
            Expr::ScalarSubquery(_)
            | Expr::InSubquery(_)
            | Expr::Exists(_)
            | Expr::OuterReferenceColumn(_, _) => {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        });
        found
    }
}

#[async_trait]
impl TableProvider for TursoTableProvider {
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
        let dialect = TursoDialect::new();
        let unparser = Unparser::new(&dialect);

        let mut filter_push_down = vec![];
        for filter in filters {
            // Expressions containing subqueries or outer references must not be pushed down.
            // For federated providers, subqueries are handled at the plan level and pushing them
            // into `TableScan.full_filters` is not beneficial. Subqueries may also reference tables
            // in other databases not accessible from this Turso connection, and outer references
            // refer to columns from an enclosing query that the table provider cannot resolve.
            let pushdown = if Self::contains_subquery_or_outer_ref(filter) {
                TableProviderFilterPushDown::Unsupported
            } else {
                match unparser.expr_to_sql(filter) {
                    Ok(_) => TableProviderFilterPushDown::Exact,
                    Err(_) => TableProviderFilterPushDown::Unsupported,
                }
            };
            filter_push_down.push(pushdown);
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
        overwrite: InsertOp,
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
            overwrite,
        ));

        // Wrap in DataSinkExec to execute the insertion
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(Arc::new(
            TursoDeletionSink::new(Arc::clone(&self.pool), self.table_name.clone(), &filters),
        ))))
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
        Some(self.pool.db_path().to_string())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(TursoDialect::new())
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        Some(AstAnalyzer::new(vec![Self::turso_ast_analyzer()]))
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let function_support = self.function_support.clone()?;
        Some(Box::new(move |plan| {
            unfederate_plan_with_unsupported_functions(plan, &function_support)
        }))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
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

        // Query the table schema using SQLite's pragma with quoted identifier
        let query = format!("PRAGMA table_info({})", quote_identifier(table_name));
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
                    "DATE_TEXT" => DataType::Date32,
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
    properties: Arc<PlanProperties>,
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
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

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
        // Build column list from projected schema, quoting identifiers to prevent injection and handle special characters.
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()).into_owned())
            .collect::<Vec<_>>()
            .join(", ");

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let dialect = TursoDialect::new();
            let unparser = Unparser::new(&dialect);
            let filter_sqls: Vec<String> = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|ast| format!("({ast})")))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            format!(" WHERE {}", filter_sqls.join(" AND "))
        };

        let limit_expr = match self.limit {
            Some(limit) => format!(" LIMIT {limit}"),
            None => String::new(),
        };

        Ok(format!(
            "SELECT {} FROM {}{}{}",
            columns,
            quote_identifier(&self.table_name),
            where_expr,
            limit_expr
        ))
    }
}

impl DisplayAs for TursoExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let table_name = &self.table_name;
        let sql = self
            .sql()
            .unwrap_or_else(|_| format!("SELECT * FROM {}", quote_identifier(table_name)));
        write!(f, "TursoExec sql={sql}")
    }
}

impl ExecutionPlan for TursoExec {
    fn name(&self) -> &'static str {
        "TursoExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
            let dialect = TursoDialect::new();
            let unparser = Unparser::new(&dialect);
            let filter_sqls: Vec<String> = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|ast| format!("({ast})")))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            format!(" WHERE {}", filter_sqls.join(" AND "))
        };

        let delete_sql = format!(
            "DELETE FROM {}{}",
            quote_identifier(&self.table_name),
            where_clause
        );

        let _schema_guard = self.pool.acquire_schema_read_lock().await;
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
    overwrite: InsertOp,
}

impl DisplayAs for TursoDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TursoDataSink(table={}, mode={:?})",
            self.table_name, self.overwrite
        )
    }
}

impl TursoDataSink {
    /// Creates a new data sink for INSERT operations
    #[must_use]
    pub fn new(
        pool: Arc<TursoConnectionPool>,
        table_name: String,
        schema: SchemaRef,
        overwrite: InsertOp,
    ) -> Self {
        Self {
            pool,
            table_name,
            schema,
            overwrite,
        }
    }

    fn insert_sql(&self) -> String {
        let columns: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()).into_owned())
            .collect();

        let placeholders = (1..=columns.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_identifier(&self.table_name),
            columns.join(", "),
            placeholders
        )
    }

    async fn rollback_write(conn: &Connection) {
        if let Err(error) = conn.execute("ROLLBACK", ()).await {
            tracing::debug!("Failed to rollback Turso write transaction: {error}");
        }
    }

    async fn overwrite_all(
        &self,
        mut data: SendableRecordBatchStream,
    ) -> datafusion::error::Result<u64> {
        let conn = self
            .pool
            .connect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let _schema_guard = self.pool.acquire_schema_read_lock().await;

        let delete_sql = format!("DELETE FROM {}", quote_identifier(&self.table_name));
        let insert_sql = self.insert_sql();

        let write_result = async {
            conn.execute(BEGIN_CONCURRENT_SQL, ())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            conn.execute(&delete_sql, ())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(&insert_sql)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut total_rows = 0u64;

            while let Some(batch) = data.next().await {
                let batch = batch?;
                total_rows += u64::try_from(batch.num_rows()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Unable to convert batch row count to u64: {e}"
                    ))
                })?;

                self.execute_insert_batch(&mut stmt, &batch).await?;
            }

            drop(stmt);

            conn.execute(COMMIT_SQL, ())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(total_rows)
        }
        .await;

        if write_result.is_err() {
            Self::rollback_write(&conn).await;
        }

        write_result
    }

    async fn append_all(
        &self,
        mut data: SendableRecordBatchStream,
    ) -> datafusion::error::Result<u64> {
        let conn = self
            .pool
            .connect()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let _schema_guard = self.pool.acquire_schema_read_lock().await;
        let insert_sql = self.insert_sql();

        let write_result = async {
            conn.execute(BEGIN_CONCURRENT_SQL, ())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut stmt = conn
                .prepare(&insert_sql)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut total_rows = 0u64;

            while let Some(batch) = data.next().await {
                let batch = batch?;
                total_rows += u64::try_from(batch.num_rows()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Unable to convert batch row count to u64: {e}"
                    ))
                })?;

                self.execute_insert_batch(&mut stmt, &batch).await?;
            }

            drop(stmt);

            conn.execute(COMMIT_SQL, ())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(total_rows)
        }
        .await;

        if write_result.is_err() {
            Self::rollback_write(&conn).await;
        }

        write_result
    }

    /// Inserts a batch of records using an already-open Turso transaction.
    async fn execute_insert_batch(
        &self,
        stmt: &mut turso::Statement,
        batch: &RecordBatch,
    ) -> datafusion::error::Result<()> {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = ScalarValue::try_from_array(column, row_idx)?;

                let turso_value = scalar_value_to_turso(value, self.pool.timestamp_format())
                    .map_err(DataFusionError::External)?;
                values.push(turso_value);
            }

            stmt.execute(values)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        Ok(())
    }
}

#[async_trait]
impl DataSink for TursoDataSink {
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        if self.overwrite == InsertOp::Overwrite {
            return self.overwrite_all(data).await;
        }

        self.append_all(data).await
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

            // Convert value to nanoseconds (checked to prevent overflow for dates beyond ~year 2262)
            let nanos = match unit {
                TimeUnit::Second => value
                    .checked_mul(timestamp_conversion::NANOS_PER_SECOND)
                    .ok_or_else(|| {
                        format!("Timestamp value {value}s overflows nanosecond conversion")
                    })?,
                TimeUnit::Millisecond => value
                    .checked_mul(timestamp_conversion::NANOS_PER_MILLI)
                    .ok_or_else(|| {
                    format!("Timestamp value {value}ms overflows nanosecond conversion")
                })?,
                TimeUnit::Microsecond => value.checked_mul(1_000).ok_or_else(|| {
                    format!("Timestamp value {value}us overflows nanosecond conversion")
                })?,
                TimeUnit::Nanosecond => value,
            };

            // Split into seconds and subsecond nanos
            let secs = nanos / timestamp_conversion::NANOS_PER_SECOND;
            #[expect(clippy::cast_sign_loss)]
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
#[expect(clippy::match_same_arms)]
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

        ScalarValue::Date32(Some(v)) => {
            // Store as ISO-8601 date string (matching SQLite accelerator behavior)
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid epoch date")?;
            let date = epoch + chrono::Duration::days(i64::from(v));
            TursoValue::Text(date.format("%Y-%m-%d").to_string())
        }
        ScalarValue::Date64(Some(v)) => {
            // Store as ISO-8601 date string (ms since epoch → date string)
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid epoch date")?;
            let date = epoch + chrono::Duration::milliseconds(v);
            TursoValue::Text(date.format("%Y-%m-%d").to_string())
        }
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
            #[expect(clippy::cast_precision_loss)]
            let scale_factor = (DECIMAL_BASE as f64).powi(i32::from(scale));
            #[expect(clippy::cast_precision_loss)]
            let v_f64 = v as f64;
            TursoValue::Real(v_f64 / scale_factor)
        }
        ScalarValue::Decimal256(Some(v), _, scale) => {
            // Convert decimal256 to float for storage as REAL
            #[expect(clippy::cast_precision_loss)]
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

/// An AST visitor that rewrites numeric `BETWEEN` expressions into explicit
/// `CAST(... AS REAL)` comparisons.
///
/// Turso (libSQL) inherits SQLite's type affinity rules, which can cause
/// text-based comparisons on decimal values stored as TEXT.  Standard SQLite
/// solves this with the `decimal_cmp()` C extension, but Turso does not
/// support loading C extensions.
///
/// This visitor rewrites:
/// ```sql
///   expr BETWEEN low AND high
/// ```
/// into:
/// ```sql
///   ROUND(expr, 10) >= ROUND(low, 10)
///     AND ROUND(expr, 10) <= ROUND(high, 10)
/// ```
/// (with the obvious inversion for `NOT BETWEEN`).
///
/// `ROUND(..., 10)` normalizes both sides to 10 decimal places, which
/// eliminates float arithmetic precision issues. For example, float64
/// `0.06 + 0.01 = 0.06999...` is less than the stored `0.07`, but
/// `ROUND(0.06 + 0.01, 10) = 0.07` matches `ROUND(0.07, 10)` exactly.
///
/// Only expressions where *both* bounds appear numeric (literal numbers,
/// unary-minus numbers, or arithmetic on numbers) are rewritten.
struct TursoBetweenVisitor;

/// Number of decimal places used by `ROUND()` to normalize float values.
const TURSO_ROUND_DECIMAL_PLACES: u8 = 10;

impl TursoBetweenVisitor {
    /// Wrap `expr` in `ROUND(expr, N)`.
    fn round_expr(expr: sqlast::Expr) -> sqlast::Expr {
        sqlast::Expr::Function(sqlast::Function {
            name: sqlast::ObjectName(vec![sqlast::ObjectNamePart::Identifier(
                sqlast::Ident::new("ROUND"),
            )]),
            args: sqlast::FunctionArguments::List(sqlast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(expr)),
                    sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(
                        sqlast::Expr::value(sqlast::Value::Number(
                            TURSO_ROUND_DECIMAL_PLACES.to_string(),
                            false,
                        )),
                    )),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: sqlast::FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    }

    /// Returns `true` if the expression looks like a numeric value or
    /// arithmetic expression that should use numeric comparison.
    fn is_numeric_expr(expr: &sqlast::Expr) -> bool {
        match expr {
            sqlast::Expr::Value(v) => matches!(v.value, sqlast::Value::Number(_, _)),
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::Minus | sqlast::UnaryOperator::Plus,
                expr,
            } => Self::is_numeric_expr(expr),
            sqlast::Expr::BinaryOp { left, right, .. } => {
                Self::is_numeric_expr(left) || Self::is_numeric_expr(right)
            }
            sqlast::Expr::Nested(inner) => Self::is_numeric_expr(inner),
            _ => false,
        }
    }
}

impl VisitorMut for TursoBetweenVisitor {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut sqlast::Expr) -> std::ops::ControlFlow<Self::Break> {
        if let sqlast::Expr::Between {
            expr: between_expr,
            negated,
            low,
            high,
        } = expr
            && Self::is_numeric_expr(low)
            && Self::is_numeric_expr(high)
        {
            let negated = *negated;
            let round_expr_low = Self::round_expr(*between_expr.clone());
            let round_expr_high = Self::round_expr(*between_expr.clone());
            let round_low = Self::round_expr(*low.clone());
            let round_high = Self::round_expr(*high.clone());

            if negated {
                // NOT BETWEEN  →  expr < low OR expr > high
                *expr = sqlast::Expr::BinaryOp {
                    left: Box::new(sqlast::Expr::BinaryOp {
                        left: Box::new(round_expr_low),
                        op: sqlast::BinaryOperator::Lt,
                        right: Box::new(round_low),
                    }),
                    op: sqlast::BinaryOperator::Or,
                    right: Box::new(sqlast::Expr::BinaryOp {
                        left: Box::new(round_expr_high),
                        op: sqlast::BinaryOperator::Gt,
                        right: Box::new(round_high),
                    }),
                };
            } else {
                // BETWEEN  →  expr >= low AND expr <= high
                *expr = sqlast::Expr::BinaryOp {
                    left: Box::new(sqlast::Expr::BinaryOp {
                        left: Box::new(round_expr_low),
                        op: sqlast::BinaryOperator::GtEq,
                        right: Box::new(round_low),
                    }),
                    op: sqlast::BinaryOperator::And,
                    right: Box::new(sqlast::Expr::BinaryOp {
                        left: Box::new(round_expr_high),
                        op: sqlast::BinaryOperator::LtEq,
                        right: Box::new(round_high),
                    }),
                };
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::sink::DataSink;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::sql::sqlparser::parser::Parser;

    async fn create_turso_sink(table_name: &str) -> (Arc<TursoConnectionPool>, TursoDataSink) {
        let pool = Arc::new(
            TursoConnectionPool::new(":memory:")
                .await
                .expect("in-memory Turso pool should be created"),
        );
        let conn = pool
            .connect()
            .await
            .expect("Turso connection should be created");
        conn.execute(
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT)",
                quote_identifier(table_name)
            ),
            (),
        )
        .await
        .expect("test table should be created");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let sink = TursoDataSink::new(
            Arc::clone(&pool),
            table_name.to_string(),
            schema,
            InsertOp::Append,
        );

        (pool, sink)
    }

    fn batch(schema: SchemaRef, ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .expect("record batch should be created")
    }

    fn stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("stream should be created"))
    }

    async fn query_rows(pool: &TursoConnectionPool, table_name: &str) -> Vec<(i64, String)> {
        let conn = pool
            .connect()
            .await
            .expect("Turso connection should be created");
        let mut rows = conn
            .query(
                format!(
                    "SELECT id, name FROM {} ORDER BY id",
                    quote_identifier(table_name)
                ),
                (),
            )
            .await
            .expect("query should execute");

        let mut values = Vec::new();
        while let Some(row) = rows.next().await.expect("row should be read") {
            let id = match row.get_value(0).expect("id should be present") {
                TursoValue::Integer(id) => id,
                other => panic!("expected integer id, got {other:?}"),
            };
            let name = match row.get_value(1).expect("name should be present") {
                TursoValue::Text(name) => name,
                other => panic!("expected text name, got {other:?}"),
            };
            values.push((id, name));
        }

        values
    }

    async fn query_ordered_ids(conn: &Connection, sql: &str) -> Vec<i64> {
        let mut rows = conn.query(sql, ()).await.expect("query should execute");

        let mut values = Vec::new();
        while let Some(row) = rows.next().await.expect("row should be read") {
            let id = match row.get_value(0).expect("id should be present") {
                TursoValue::Integer(id) => id,
                other => panic!("expected integer id, got {other:?}"),
            };
            values.push(id);
        }

        values
    }

    fn rewrite_between(sql: &str) -> String {
        let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).expect("valid SQL");
        let mut visitor = TursoBetweenVisitor;
        for stmt in &mut stmts {
            let _ = stmt.visit(&mut visitor);
        }
        stmts[0].to_string()
    }

    #[tokio::test]
    async fn test_turso_append_writes_multiple_stream_batches() {
        let table_name = "append_multi_batch";
        let (pool, sink) = create_turso_sink(table_name).await;
        let schema = Arc::clone(sink.schema());
        let data = stream(
            Arc::clone(&schema),
            vec![
                batch(Arc::clone(&schema), vec![1, 2], vec!["one", "two"]),
                batch(Arc::clone(&schema), vec![3], vec!["three"]),
            ],
        );

        let written = sink
            .write_all(data, &Arc::new(TaskContext::default()))
            .await
            .expect("append should succeed");

        assert_eq!(written, 3);
        assert_eq!(
            query_rows(&pool, table_name).await,
            vec![
                (1, "one".to_string()),
                (2, "two".to_string()),
                (3, "three".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn test_turso_append_rolls_back_all_batches_on_later_error() {
        let table_name = "append_atomic_error";
        let (pool, sink) = create_turso_sink(table_name).await;
        let schema = Arc::clone(sink.schema());
        let data = stream(
            Arc::clone(&schema),
            vec![
                batch(Arc::clone(&schema), vec![1], vec!["first"]),
                batch(Arc::clone(&schema), vec![1], vec!["duplicate"]),
            ],
        );

        let result = sink
            .write_all(data, &Arc::new(TaskContext::default()))
            .await;

        assert!(result.is_err(), "duplicate primary key should fail");
        assert_eq!(
            query_rows(&pool, table_name).await,
            Vec::<(i64, String)>::new(),
            "the first batch must roll back when a later batch in the same stream fails"
        );
    }

    #[tokio::test]
    async fn test_turso_schema_changes_wait_for_open_sink_write_transaction() {
        let table_name = "schema_gate_data";
        let (pool, sink) = create_turso_sink(table_name).await;
        let schema = Arc::clone(sink.schema());
        let first_batch = batch(Arc::clone(&schema), vec![1], vec!["one"]);

        let (stream_waiting_tx, stream_waiting_rx) = tokio::sync::oneshot::channel();
        let (finish_stream_tx, finish_stream_rx) = tokio::sync::oneshot::channel();
        let delayed_stream = futures::stream::unfold(
            (
                0_u8,
                Some(first_batch),
                Some(stream_waiting_tx),
                Some(finish_stream_rx),
            ),
            |(state, batch, stream_waiting_tx, finish_stream_rx)| async move {
                match state {
                    0 => Some((
                        Ok::<_, DataFusionError>(batch.expect("first batch should be available")),
                        (1, None, stream_waiting_tx, finish_stream_rx),
                    )),
                    1 => {
                        if let Some(stream_waiting_tx) = stream_waiting_tx {
                            let _ = stream_waiting_tx.send(());
                        }
                        if let Some(finish_stream_rx) = finish_stream_rx {
                            let _ = finish_stream_rx.await;
                        }
                        None
                    }
                    _ => None,
                }
            },
        );
        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            delayed_stream,
        ));

        let sink_task = tokio::spawn(async move {
            sink.write_all(data, &Arc::new(TaskContext::default()))
                .await
        });
        stream_waiting_rx
            .await
            .expect("sink stream should wait before commit");

        let ddl_pool = Arc::clone(&pool);
        let (ddl_started_tx, ddl_started_rx) = tokio::sync::oneshot::channel();
        let mut ddl_task = tokio::spawn(async move {
            ddl_started_tx
                .send(())
                .expect("DDL start notification should be delivered");
            let _schema_guard = ddl_pool.acquire_schema_write_lock().await;
            let conn = ddl_pool
                .connect()
                .await
                .expect("schema change connection should be created");
            conn.execute(
                "CREATE TABLE schema_gate_other (id INTEGER PRIMARY KEY)",
                (),
            )
            .await
        });

        ddl_started_rx
            .await
            .expect("DDL task should start before waiting for the schema lock");
        let ddl_wait =
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut ddl_task).await;
        assert!(
            ddl_wait.is_err(),
            "schema changes should wait until the sink write transaction commits"
        );

        finish_stream_tx
            .send(())
            .expect("stream finish signal should be delivered");
        let written = sink_task
            .await
            .expect("sink task should not panic")
            .expect("sink write should succeed");
        assert_eq!(written, 1);

        ddl_task
            .await
            .expect("DDL task should not panic")
            .expect("schema change should succeed after the sink commits");
        assert_eq!(
            query_rows(&pool, table_name).await,
            vec![(1, "one".to_string())]
        );
    }

    #[tokio::test]
    async fn test_turso_nulls_first_last_ordering() {
        let pool = TursoConnectionPool::new(":memory:")
            .await
            .expect("in-memory Turso pool should be created");
        let conn = pool
            .connect()
            .await
            .expect("Turso connection should be created");

        conn.execute(
            "CREATE TABLE null_sort_test (id INTEGER PRIMARY KEY, sort_value INTEGER)",
            (),
        )
        .await
        .expect("test table should be created");
        conn.execute(
            "INSERT INTO null_sort_test (id, sort_value) VALUES (1, 20), (2, NULL), (3, 10), (4, NULL)",
            (),
        )
        .await
        .expect("test rows should be inserted");

        assert_eq!(
            query_ordered_ids(
                &conn,
                "SELECT id FROM null_sort_test ORDER BY sort_value ASC NULLS FIRST, id ASC",
            )
            .await,
            vec![2, 4, 3, 1]
        );
        assert_eq!(
            query_ordered_ids(
                &conn,
                "SELECT id FROM null_sort_test ORDER BY sort_value ASC NULLS LAST, id ASC",
            )
            .await,
            vec![3, 1, 2, 4]
        );
    }

    #[test]
    fn test_turso_between_numeric_rewrite() {
        let input = "SELECT * FROM t WHERE x BETWEEN 0.05 AND 0.07";
        let result = rewrite_between(input);
        assert!(
            !result.contains("BETWEEN"),
            "BETWEEN should be rewritten, got: {result}"
        );
        assert!(
            result.contains("ROUND(x, 10) >= ROUND(0.05, 10)"),
            "should use ROUND: {result}"
        );
        assert!(
            result.contains("ROUND(x, 10) <= ROUND(0.07, 10)"),
            "should use ROUND: {result}"
        );
    }

    #[test]
    fn test_turso_between_arithmetic_bounds() {
        let input = "SELECT * FROM t WHERE x BETWEEN 0.06 - 0.01 AND 0.06 + 0.01";
        let result = rewrite_between(input);
        assert!(
            !result.contains("BETWEEN"),
            "BETWEEN with arithmetic bounds should be rewritten, got: {result}"
        );
        assert!(result.contains("ROUND"), "should contain ROUND: {result}");
    }

    #[test]
    fn test_turso_not_between_numeric_rewrite() {
        let input = "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10";
        let result = rewrite_between(input);
        assert!(
            !result.contains("BETWEEN"),
            "NOT BETWEEN should be rewritten, got: {result}"
        );
        assert!(
            result.contains(" OR "),
            "NOT BETWEEN should use OR: {result}"
        );
    }

    #[test]
    fn test_turso_between_non_numeric_unchanged() {
        let input = "SELECT * FROM t WHERE name BETWEEN 'a' AND 'z'";
        let result = rewrite_between(input);
        assert!(
            result.contains("BETWEEN"),
            "Non-numeric BETWEEN should be unchanged, got: {result}"
        );
    }

    #[test]
    fn test_turso_between_negative_bounds() {
        let input = "SELECT * FROM t WHERE x BETWEEN -1.5 AND 1.5";
        let result = rewrite_between(input);
        assert!(
            !result.contains("BETWEEN"),
            "BETWEEN with negative bounds should be rewritten, got: {result}"
        );
    }

    #[test]
    fn test_rfc3339_timestamp_outside_nanos_range() {
        use arrow::datatypes::{DataType, Field, TimeUnit};

        // Year 2300 is outside the i64 nanosecond range (~1677-2262).
        // Previously, timestamp_nanos_opt().unwrap_or(0) silently returned epoch 0.
        let rfc3339_date = "2300-01-01T00:00:00Z";
        let rows = vec![vec![TursoValue::Text(rfc3339_date.to_string())]];

        // Second unit should work for any reasonable date
        let schema_second = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_second)
            .expect("should succeed for Second unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampSecondArray>()
            .expect("should be TimestampSecondArray");
        assert!(
            !arr.is_null(0),
            "Second-precision timestamp should not be NULL for year 2300"
        );
        // 2300-01-01T00:00:00Z in seconds since epoch
        assert!(
            arr.value(0) > 10_000_000_000,
            "timestamp should be far in the future, not epoch 0"
        );

        // Millisecond unit should also work
        let schema_millis = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_millis)
            .expect("should succeed for Millisecond unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .expect("should be TimestampMillisecondArray");
        assert!(
            !arr.is_null(0),
            "Millisecond-precision timestamp should not be NULL for year 2300"
        );
        assert!(
            arr.value(0) > 10_000_000_000_000,
            "timestamp should be far in the future, not epoch 0"
        );

        // Microsecond unit should also work
        let schema_micros = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_micros)
            .expect("should succeed for Microsecond unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("should be TimestampMicrosecondArray");
        assert!(
            !arr.is_null(0),
            "Microsecond-precision timestamp should not be NULL for year 2300"
        );
        assert!(
            arr.value(0) > 10_000_000_000_000_000,
            "timestamp should be far in the future, not epoch 0"
        );

        // Nanosecond unit should return NULL (overflow) instead of epoch 0
        let schema_nanos = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_nanos)
            .expect("should succeed for Nanosecond unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .expect("should be TimestampNanosecondArray");
        // Nanosecond cannot represent year 2300 — should be NULL, not epoch 0
        assert!(
            arr.is_null(0),
            "Nanosecond-precision timestamp should be NULL for year 2300 (overflow)"
        );
    }

    #[test]
    fn test_convert_timestamp_to_turso_rfc3339_overflow_returns_error() {
        let result =
            convert_timestamp_to_turso(i64::MAX, TimeUnit::Second, None, TimestampFormat::Rfc3339);
        assert!(
            result.is_err(),
            "TimestampSecond i64::MAX should overflow when converting to nanoseconds"
        );

        let result = convert_timestamp_to_turso(
            i64::MAX,
            TimeUnit::Millisecond,
            None,
            TimestampFormat::Rfc3339,
        );
        assert!(
            result.is_err(),
            "TimestampMillisecond i64::MAX should overflow when converting to nanoseconds"
        );
    }

    #[test]
    fn test_integer_millis_timestamp_outside_nanos_range() {
        use arrow::datatypes::{DataType, Field, TimeUnit};

        // Year 2300 as milliseconds since epoch — outside the i64 nanosecond range (~1677-2262).
        // millis * 1_000_000 would overflow i64 and silently wrap in release builds.
        let millis_2300: i64 = 10_413_792_000_000;
        let rows = vec![vec![TursoValue::Integer(millis_2300)]];

        // Nanosecond unit: millis * 1_000_000 overflows — should produce NULL, not a wrapped value
        let schema_nanos = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_nanos)
            .expect("should succeed (overflow produces NULL, not error)");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .expect("should be TimestampNanosecondArray");
        assert!(
            arr.is_null(0),
            "Nanosecond-precision integer millis timestamp should be NULL for year 2300 (overflow)"
        );

        // Microsecond unit: millis * 1_000 is well within range — should produce a valid value
        let schema_micros = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_micros)
            .expect("should succeed for Microsecond unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("should be TimestampMicrosecondArray");
        assert!(
            !arr.is_null(0),
            "Microsecond-precision integer millis should not be NULL for year 2300"
        );
        assert_eq!(
            arr.value(0),
            millis_2300 * 1_000,
            "Microsecond value should be millis * 1000"
        );

        // Millisecond unit: identity — should always work
        let schema_millis = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let batch = TursoTableProvider::values_to_record_batch(&rows, &schema_millis)
            .expect("should succeed for Millisecond unit");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .expect("should be TimestampMillisecondArray");
        assert!(!arr.is_null(0), "Millisecond identity should not be NULL");
        assert_eq!(arr.value(0), millis_2300);
    }
}
