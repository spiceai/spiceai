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

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::Session,
    datasource::{
        TableProvider,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{
        CreateExternalTable, Expr, TableProviderFilterPushDown, TableType, dml::InsertOp,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
    sql::{
        TableReference,
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
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, fmt, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value as TursoValue};

use crate::{
    component::dataset::acceleration::Engine,
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path,
};

use super::{AccelerationSource, DataAccelerator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration initialization failed: {source}"))]
    AccelerationInitializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The \"turso_file\" acceleration parameter has an invalid extension. Expected one of \"{valid_extensions}\" but got \"{extension}\"."
    ))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display("The \"turso_file\" acceleration parameter value is a directory."))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid Turso acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Turso database error: {source}"))]
    TursoDatabaseError { source: turso::Error },

    #[snafu(display(
        "Remote Turso databases are not supported when using Turso as a file accelerator. Remote database support (turso_url, turso_auth_token) will be available when Turso is used as a data connector."
    ))]
    RemoteDatabaseNotSupported,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Connection pool for Turso databases
#[derive(Debug)]
pub struct TursoConnectionPool {
    database: Arc<Database>,
    mvcc_enabled: bool,
    db_path: String,
}

impl TursoConnectionPool {
    pub async fn new(path: &str, mvcc_enabled: bool) -> Result<Self> {
        // Turso supports both file and memory modes
        // Memory mode uses ":memory:" as the path
        // MVCC (Multi-Version Concurrency Control) can be enabled via configuration
        // When enabled, it supports BEGIN CONCURRENT transactions
        let database = Builder::new_local(path)
            .with_mvcc(mvcc_enabled)
            .build()
            .await
            .context(TursoDatabaseSnafu)?;

        Ok(Self {
            database: Arc::new(database),
            mvcc_enabled,
            db_path: path.to_string(),
        })
    }

    pub async fn connect(&self) -> Result<Connection> {
        self.database.connect().context(TursoDatabaseSnafu)
    }

    /// Returns true if MVCC (Multi-Version Concurrency Control) is enabled
    pub fn is_mvcc_enabled(&self) -> bool {
        self.mvcc_enabled
    }

    /// Returns true if this is a memory database
    pub fn is_memory_db(&self) -> bool {
        self.db_path == ":memory:"
    }

    /// Returns the database path
    pub fn db_path(&self) -> &str {
        &self.db_path
    }
}

/// Turso Table Provider for reading data
#[derive(Debug)]
pub struct TursoTableProvider {
    schema: SchemaRef,
    table_name: String,
    pool: Arc<TursoConnectionPool>,
}

impl TursoTableProvider {
    pub fn new(schema: SchemaRef, table_name: String, pool: Arc<TursoConnectionPool>) -> Self {
        Self {
            schema,
            table_name,
            pool,
        }
    }

    /// Converts Turso database rows to Arrow RecordBatch, matching the exact schema types.
    ///
    /// This function is critical for reading data from Turso - it must respect the schema's
    /// exact data types (e.g., LargeUtf8 vs Utf8, Timestamp units) to avoid type mismatches.
    ///
    /// Supported types:
    /// - Integers: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64
    /// - Floats: Float32, Float64
    /// - Strings: Utf8, LargeUtf8
    /// - Binary: Binary, LargeBinary
    /// - Boolean
    /// - Timestamps: All time units (Second, Millisecond, Microsecond, Nanosecond)
    /// - Dates: Date32, Date64
    fn values_to_record_batch(
        rows: &[Vec<TursoValue>],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::*;

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

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
                    // Timestamps are stored as INTEGER in Turso/SQLite in milliseconds
                    // We need to convert from milliseconds to the schema's expected unit
                    use arrow::datatypes::TimeUnit;

                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(millis) => {
                                // Convert from stored milliseconds to the target unit
                                Some(match unit {
                                    TimeUnit::Second => millis / 1000,
                                    TimeUnit::Millisecond => *millis,
                                    TimeUnit::Microsecond => millis * 1000,
                                    TimeUnit::Nanosecond => millis * 1_000_000,
                                })
                            }
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();

                    match unit {
                        TimeUnit::Second => Arc::new(
                            arrow::array::TimestampSecondArray::from(values)
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Millisecond => Arc::new(
                            arrow::array::TimestampMillisecondArray::from(values)
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Microsecond => Arc::new(
                            arrow::array::TimestampMicrosecondArray::from(values)
                                .with_timezone_opt(tz.clone()),
                        ),
                        TimeUnit::Nanosecond => Arc::new(
                            arrow::array::TimestampNanosecondArray::from(values)
                                .with_timezone_opt(tz.clone()),
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
                        arrow::datatypes::TimeUnit::Millisecond => {
                            Arc::new(arrow::array::Time32MillisecondArray::from(values))
                        }
                        arrow::datatypes::TimeUnit::Second => {
                            Arc::new(arrow::array::Time32SecondArray::from(values))
                        }
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
                        arrow::datatypes::TimeUnit::Microsecond => {
                            Arc::new(arrow::array::Time64MicrosecondArray::from(values))
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            Arc::new(arrow::array::Time64NanosecondArray::from(values))
                        }
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
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    match unit {
                        arrow::datatypes::TimeUnit::Second => {
                            Arc::new(arrow::array::DurationSecondArray::from(values))
                        }
                        arrow::datatypes::TimeUnit::Millisecond => {
                            Arc::new(arrow::array::DurationMillisecondArray::from(values))
                        }
                        arrow::datatypes::TimeUnit::Microsecond => {
                            Arc::new(arrow::array::DurationMicrosecondArray::from(values))
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            Arc::new(arrow::array::DurationNanosecondArray::from(values))
                        }
                    }
                }
                DataType::Interval(unit) => {
                    // Interval stored as INTEGER
                    match unit {
                        arrow::datatypes::IntervalUnit::YearMonth => {
                            let values: Vec<Option<i32>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => i32::try_from(*i).ok(),
                                    TursoValue::Null => None,
                                    _ => None,
                                })
                                .collect();
                            Arc::new(arrow::array::IntervalYearMonthArray::from(values))
                        }
                        arrow::datatypes::IntervalUnit::DayTime => {
                            use arrow::datatypes::IntervalDayTime;
                            let values: Vec<Option<IntervalDayTime>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Integer(i) => {
                                        // Unpack i64: upper 32 bits = days, lower 32 bits = milliseconds
                                        let days = (*i >> 32) as i32;
                                        let milliseconds = (*i & 0xFFFF_FFFF) as i32;
                                        Some(IntervalDayTime::new(days, milliseconds))
                                    }
                                    TursoValue::Null => None,
                                    _ => None,
                                })
                                .collect();
                            Arc::new(arrow::array::IntervalDayTimeArray::from(values))
                        }
                        arrow::datatypes::IntervalUnit::MonthDayNano => {
                            use arrow::datatypes::IntervalMonthDayNano;
                            let values: Vec<Option<IntervalMonthDayNano>> = rows
                                .iter()
                                .map(|row| match &row[col_idx] {
                                    TursoValue::Text(s) => {
                                        // Deserialize from JSON
                                        serde_json::from_str::<serde_json::Value>(s).ok().and_then(
                                            |v| {
                                                let months = v["months"].as_i64()? as i32;
                                                let days = v["days"].as_i64()? as i32;
                                                let nanoseconds = v["nanoseconds"].as_i64()?;
                                                Some(IntervalMonthDayNano::new(
                                                    months,
                                                    days,
                                                    nanoseconds,
                                                ))
                                            },
                                        )
                                    }
                                    TursoValue::Null => None,
                                    _ => None,
                                })
                                .collect();
                            Arc::new(arrow::array::IntervalMonthDayNanoArray::from(values))
                        }
                    }
                }
                DataType::List(field) => {
                    // List stored as TEXT (JSON serialized)
                    // Reconstruct the list arrays from JSON - only support Int32 lists for now
                    if matches!(field.data_type(), DataType::Int32) {
                        use arrow::array::ListBuilder;
                        let mut list_builder =
                            ListBuilder::new(arrow::array::Int32Array::builder(rows.len() * 3));

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
                                TursoValue::Null => {
                                    list_builder.append_null();
                                }
                                _ => {
                                    list_builder.append_null();
                                }
                            }
                        }

                        Arc::new(list_builder.finish())
                    } else {
                        // For unsupported list element types, return empty list array
                        use arrow::array::ListBuilder;
                        let mut list_builder =
                            ListBuilder::new(arrow::array::Int32Array::builder(0));
                        for _ in rows {
                            list_builder.append_null();
                        }
                        Arc::new(list_builder.finish())
                    }
                }
                DataType::Map(_, _sorted) => {
                    // Map stored as TEXT (JSON serialized)
                    // Reconstruct map arrays from JSON
                    use arrow::array::{Int32Builder, MapBuilder, StringBuilder};

                    // For now, only support Utf8 keys to Int32 values
                    let mut map_builder =
                        MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());

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
                                            map_builder.values().append_value(int_val as i32);
                                        } else {
                                            map_builder.values().append_null();
                                        }
                                    }
                                    map_builder.append(true).map_err(|e| {
                                        Box::new(std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            format!("Failed to append map: {}", e),
                                        ))
                                            as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                } else {
                                    map_builder.append(false).map_err(|e| {
                                        Box::new(std::io::Error::new(
                                            std::io::ErrorKind::InvalidData,
                                            format!("Failed to append null map: {}", e),
                                        ))
                                            as Box<dyn std::error::Error + Send + Sync>
                                    })?;
                                }
                            }
                            TursoValue::Null => {
                                map_builder.append(false).map_err(|e| {
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!("Failed to append null map: {}", e),
                                    ))
                                        as Box<dyn std::error::Error + Send + Sync>
                                })?;
                            }
                            _ => {
                                map_builder.append(false).map_err(|e| {
                                    Box::new(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        format!("Failed to append null map: {}", e),
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
                    let scale_factor = 10_i128.pow(*scale as u32);
                    let values: Vec<Option<i128>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                let scaled = (f * scale_factor as f64).round() as i128;
                                Some(scaled)
                            }
                            TursoValue::Integer(i) => {
                                // If stored as integer, scale it
                                Some(*i as i128 * scale_factor)
                            }
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(*precision, *scale)
                            .map_err(|e| {
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid decimal128 precision/scale: {}", e),
                                ))
                                    as Box<dyn std::error::Error + Send + Sync>
                            })?,
                    )
                }
                DataType::Decimal256(precision, scale) => {
                    // Decimal256 stored as REAL in database
                    // Convert back to i256 scaled value
                    use arrow::datatypes::i256;
                    let scale_factor = 10_i128.pow(*scale as u32);
                    let values: Vec<Option<i256>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => {
                                // Convert float to scaled integer
                                let scaled = (f * scale_factor as f64).round() as i128;
                                Some(i256::from_i128(scaled))
                            }
                            TursoValue::Integer(i) => {
                                // If stored as integer, scale it
                                let scaled = *i as i128 * scale_factor;
                                Some(i256::from_i128(scaled))
                            }
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(
                        Decimal256Array::from(values)
                            .with_precision_and_scale(*precision, *scale)
                            .map_err(|e| {
                                Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Invalid decimal256 precision/scale: {}", e),
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
        // When projection is Some([0, 2]), we select only columns at indices 0 and 2
        // The projected_schema will contain only those fields, which TursoExec will use
        // to build the SELECT clause with only the necessary columns
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
        // Turso does not support UPSERT/ON CONFLICT operations yet
        // Warn if overwrite mode is requested
        if !matches!(overwrite, InsertOp::Append) {
            tracing::warn!(
                "Turso accelerator does not support UPSERT/ON CONFLICT operations. InsertOp::{:?} will be treated as Append.",
                overwrite
            );
        }

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(TursoDataSink::new(
                Arc::clone(&self.pool),
                self.table_name.clone(),
                Arc::clone(&self.schema),
            )),
            None,
        )) as _)
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
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = TableReference::bare(self.table_name.clone());
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));

        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_name),
            schema,
        )))
    }

    /// Creates a federated table provider that supports query federation
    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }

    /// Returns AST analyzer rules for Turso-specific SQL transformations
    ///
    /// Turso uses SQLite dialect which doesn't support INTERVAL literals.
    ///
    /// TODO: Implement INTERVAL expression transformation
    /// SQLite doesn't support INTERVAL literals, so queries like:
    ///   `WHERE timestamp > NOW() - INTERVAL '1' DAY`
    /// should be transformed to:
    ///   `WHERE timestamp > datetime('now', '-1 day')`
    ///
    /// Implementation strategy:
    /// 1. Walk the AST recursively to find BinaryOp expressions with INTERVAL operands
    /// 2. Pattern match on: Expr::BinaryOp { left, op: Plus|Minus, right: Interval(...) }
    /// 3. Extract interval value and unit (Year|Month|Day|Hour|Minute|Second)
    /// 4. Construct SQLite datetime() function call with modifiers:
    ///    - datetime(left_expr, '+N unit') for addition
    ///    - datetime(left_expr, '-N unit') for subtraction
    /// 5. Replace the BinaryOp node with the Function node
    ///
    /// Note: Current datafusion_federation version (0.49.x) doesn't export
    /// transform_statement() or SQLFederationError helpers needed for this.
    /// Consider upgrading datafusion_federation or implementing manual AST walking.
    ///
    /// Example transformation:
    /// ```sql
    /// -- Input:  column + INTERVAL '5' DAY
    /// -- Output: datetime(column, '+5 days')
    ///
    /// -- Input:  NOW() - INTERVAL '2' HOUR  
    /// -- Output: datetime(NOW(), '-2 hours')
    /// ```
    fn turso_ast_analyzer(&self) -> AstAnalyzerRule {
        Box::new(|ast| {
            // Pass-through implementation until INTERVAL transformation is added
            // Most queries won't use INTERVAL literals with Turso, so this is safe
            Ok(ast)
        })
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
        Some(AstAnalyzer::new(vec![self.turso_ast_analyzer()]))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let query = query.to_string();
        let schema_clone = Arc::clone(&schema);

        let fut =
            async move {
                let conn = pool.connect().await.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to connect to Turso: {}", e))
                })?;

                let mut rows = conn.query(&query, ()).await.map_err(|e| {
                    DataFusionError::Execution(format!("Turso query failed: {}", e))
                })?;

                let mut rows_vec: Vec<Vec<TursoValue>> = Vec::new();
                while let Some(row) = rows.next().await.map_err(|e| {
                    DataFusionError::Execution(format!("Failed to fetch row: {}", e))
                })? {
                    let mut values = Vec::new();
                    for i in 0..schema_clone.fields().len() {
                        let value = row.get_value(i).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to get value at index {}: {}",
                                i, e
                            ))
                        })?;
                        values.push(value);
                    }
                    rows_vec.push(values);
                }

                if rows_vec.is_empty() {
                    return Ok(RecordBatch::new_empty(schema_clone));
                }

                TursoTableProvider::values_to_record_batch(&rows_vec, &schema_clone).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to convert Turso results: {}", e))
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
        let conn = self.pool.connect().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to connect to Turso: {}", e))
        })?;

        // Query the table schema using SQLite's pragma
        let query = format!("PRAGMA table_info({})", table_name);
        let mut rows = conn.query(&query, ()).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get table schema: {}", e))
        })?;

        let mut fields = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to fetch schema row: {}", e)))?
        {
            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            // We need to extract: name (index 1), type (index 2), notnull (index 3)
            let col_name = row.get_value(1).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get column name: {}", e))
            })?;
            let col_type = row.get_value(2).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get column type: {}", e))
            })?;
            let not_null = row.get_value(3).map_err(|e| {
                DataFusionError::Execution(format!("Failed to get notnull flag: {}", e))
            })?;

            if let (
                TursoValue::Text(col_name),
                TursoValue::Text(col_type),
                TursoValue::Integer(not_null),
            ) = (&col_name, &col_type, &not_null)
            {
                let data_type = match col_type.to_uppercase().as_str() {
                    "INTEGER" => DataType::Int64,
                    "REAL" | "FLOAT" | "DOUBLE" => DataType::Float64,
                    "TEXT" => DataType::Utf8,
                    "BLOB" => DataType::Binary,
                    _ => DataType::Utf8,
                };
                let nullable = *not_null == 0;
                fields.push(Field::new(col_name.as_str(), data_type, nullable));
            }
        }

        if fields.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Table '{}' not found or has no columns",
                table_name
            )));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

/// Execution plan for Turso queries
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
    /// Note: Projection pushdown is handled by the schema parameter passed to `new()`,
    /// which is already the projected schema created in `scan()` via `schema.project(indices)`.
    /// Therefore, iterating over `self.schema.fields()` gives us only the projected columns.
    fn sql(&self) -> datafusion::error::Result<String> {
        // Build column list from projected schema - this handles projection pushdown
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
        let sql = self
            .sql()
            .unwrap_or_else(|_| format!("SELECT * FROM {}", self.table_name));
        write!(f, "TursoExec sql={}", sql)
    }
}

impl ExecutionPlan for TursoExec {
    fn name(&self) -> &str {
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

/// Data sink for INSERT operations
#[derive(Debug)]
struct TursoDataSink {
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
    fn new(pool: Arc<TursoConnectionPool>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            pool,
            table_name,
            schema,
        }
    }

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
            .map(|i| format!("?{}", i))
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
                let turso_value = match value {
                    ScalarValue::Int64(Some(v)) => TursoValue::Integer(v),
                    ScalarValue::Int32(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::Int16(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::Int8(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::UInt64(Some(v)) => {
                        TursoValue::Integer(i64::try_from(v).unwrap_or(i64::MAX))
                    }
                    ScalarValue::UInt32(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::UInt16(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::UInt8(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::Float64(Some(v)) => TursoValue::Real(v),
                    ScalarValue::Float32(Some(v)) => TursoValue::Real(f64::from(v)),
                    ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                        TursoValue::Text(v)
                    }
                    ScalarValue::Boolean(Some(v)) => TursoValue::Integer(if v { 1 } else { 0 }),
                    ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => {
                        TursoValue::Blob(v)
                    }
                    ScalarValue::TimestampMillisecond(Some(v), _) => TursoValue::Integer(v),
                    ScalarValue::TimestampMicrosecond(Some(v), _) => TursoValue::Integer(v / 1000),
                    ScalarValue::TimestampNanosecond(Some(v), _) => {
                        TursoValue::Integer(v / 1_000_000)
                    }
                    ScalarValue::TimestampSecond(Some(v), _) => TursoValue::Integer(v * 1000),
                    ScalarValue::Date32(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::Date64(Some(v)) => TursoValue::Integer(v),
                    ScalarValue::Time32Second(Some(v))
                    | ScalarValue::Time32Millisecond(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::Time64Microsecond(Some(v))
                    | ScalarValue::Time64Nanosecond(Some(v)) => TursoValue::Integer(v),
                    ScalarValue::DurationSecond(Some(v))
                    | ScalarValue::DurationMillisecond(Some(v))
                    | ScalarValue::DurationMicrosecond(Some(v))
                    | ScalarValue::DurationNanosecond(Some(v)) => TursoValue::Integer(v),
                    ScalarValue::IntervalYearMonth(Some(v)) => TursoValue::Integer(i64::from(v)),
                    ScalarValue::IntervalDayTime(Some(v)) => {
                        // IntervalDayTime has days (i32) and milliseconds (i32)
                        // Pack into i64: upper 32 bits = days, lower 32 bits = milliseconds
                        let packed =
                            ((v.days as i64) << 32) | (v.milliseconds as i64 & 0xFFFF_FFFF);
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
                        let scale_factor = 10_f64.powi(scale as i32);
                        TursoValue::Real(v as f64 / scale_factor)
                    }
                    ScalarValue::Decimal256(Some(v), _, scale) => {
                        // Convert decimal256 to float for storage as REAL
                        // i256 doesn't have direct conversion to i128, so use string conversion
                        let scale_factor = 10_f64.powi(scale as i32);
                        let v_str = format!("{}", v);
                        let v_f64 = v_str.parse::<f64>().unwrap_or(0.0);
                        TursoValue::Real(v_f64 / scale_factor)
                    }
                    ScalarValue::List(list_arr) => {
                        // Serialize list as JSON
                        use arrow::array::Array;
                        let mut json_values = Vec::new();
                        for i in 0..list_arr.len() {
                            if list_arr.is_null(i) {
                                json_values.push(serde_json::Value::Null);
                            } else {
                                let elem = ScalarValue::try_from_array(list_arr.as_ref(), i)?;
                                match elem {
                                    ScalarValue::Int32(Some(v)) => {
                                        json_values.push(serde_json::Value::from(v))
                                    }
                                    ScalarValue::Int64(Some(v)) => {
                                        json_values.push(serde_json::Value::from(v))
                                    }
                                    ScalarValue::Utf8(Some(v)) => {
                                        json_values.push(serde_json::Value::from(v))
                                    }
                                    _ => json_values.push(serde_json::Value::Null),
                                }
                            }
                        }
                        TursoValue::Text(serde_json::to_string(&json_values).unwrap_or_default())
                    }
                    ScalarValue::Map(map_arr) => {
                        // Map is a StructArray with "entries" containing keys and values
                        // Serialize as JSON object
                        use arrow::array::{Array, MapArray};

                        let map_array = map_arr
                            .as_ref()
                            .as_any()
                            .downcast_ref::<MapArray>()
                            .ok_or_else(|| {
                                datafusion::error::DataFusionError::Internal(
                                    "Expected MapArray".to_string(),
                                )
                            })?;

                        let mut json_map = serde_json::Map::new();

                        // Get keys and values from the map
                        let keys = map_array.keys();
                        let values = map_array.values();

                        for i in 0..keys.len() {
                            if !keys.is_null(i) && !values.is_null(i) {
                                // Extract key as string
                                let key_scalar = ScalarValue::try_from_array(keys.as_ref(), i)?;
                                let key_str = match key_scalar {
                                    ScalarValue::Utf8(Some(k)) => k,
                                    _ => continue, // Skip non-string keys
                                };

                                // Extract value as int
                                let val_scalar = ScalarValue::try_from_array(values.as_ref(), i)?;
                                let val_json = match val_scalar {
                                    ScalarValue::Int32(Some(v)) => serde_json::Value::from(v),
                                    ScalarValue::Int64(Some(v)) => serde_json::Value::from(v),
                                    _ => serde_json::Value::Null,
                                };

                                json_map.insert(key_str, val_json);
                            }
                        }

                        TursoValue::Text(serde_json::to_string(&json_map).unwrap_or_default())
                    }
                    _ => TursoValue::Null,
                };
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

/// Deletion sink for DELETE operations
struct TursoDeletionSink {
    pool: Arc<TursoConnectionPool>,
    table_name: String,
    filters: Vec<Expr>,
}

impl TursoDeletionSink {
    fn new(pool: Arc<TursoConnectionPool>, table_name: String, filters: &[Expr]) -> Self {
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
        // Build WHERE clause using SQLite dialect unparser (before async)
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

pub struct TursoAccelerator {
    // Store connection pools for file-based databases
    pools: Arc<Mutex<std::collections::HashMap<String, Arc<TursoConnectionPool>>>>,
}

impl Default for TursoAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TursoAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Parses the `turso_mvcc` parameter from the acceleration configuration
    /// Returns true if MVCC should be enabled, false otherwise (default: disabled)
    fn parse_mvcc_enabled(&self, source: &dyn AccelerationSource) -> Result<bool> {
        if let Some(acceleration) = source.acceleration() {
            if let Some(mvcc_value) = acceleration.params.get("turso_mvcc") {
                match mvcc_value.as_str() {
                    "enabled" => Ok(true),
                    "disabled" => Ok(false),
                    _ => Err(Error::InvalidConfiguration {
                        detail: Arc::from(format!(
                            "Invalid 'turso_mvcc' value: '{}'. Expected 'enabled' or 'disabled'.",
                            mvcc_value
                        )),
                    }),
                }
            } else {
                // Default to disabled
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Returns the database path for a Turso accelerator.
    ///
    /// This function determines the appropriate database path based on the acceleration mode:
    /// - **Memory mode** (`!is_file_accelerated()`): Returns `":memory:"` for in-memory database
    /// - **File mode** (`is_file_accelerated()`): Returns a file path, which can be:
    ///   - User-specified via `turso_file` parameter, or
    ///   - Auto-generated default path: `{spice_data_dir}/{dataset_name}.turso`
    ///
    /// Note: This function will never return `":memory:"` when called with file mode.
    pub fn turso_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        // Check acceleration mode first
        if !source.is_file_accelerated() {
            // Memory mode: always use in-memory database
            return Ok(":memory:".to_string());
        }

        // File mode: determine the file path to use
        if let Some(acceleration) = source.acceleration() {
            let acceleration_params = &acceleration.params;

            // Remote databases are not supported as accelerators
            if acceleration_params.contains_key("turso_url")
                || acceleration_params.contains_key("turso_auth_token")
            {
                return Err(Error::RemoteDatabaseNotSupported);
            }

            // Use custom file path if specified
            if let Some(turso_file) = acceleration_params.get("turso_file") {
                return Ok(turso_file.clone());
            }

            // Generate default file path based on dataset name
            let data_directory = spice_data_base_path();
            let name_str = source.name().to_string().replace('/', "_");
            let file_name = format!("{}.turso", name_str);
            let path = PathBuf::from(data_directory).join(file_name);

            Ok(path.to_string_lossy().to_string())
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }

    /// Returns an existing `Turso` connection for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_connection(&self, source: &dyn AccelerationSource) -> Result<Connection> {
        let turso_file = self.turso_file_path(source)?;
        let mvcc_enabled = self.parse_mvcc_enabled(source)?;

        let db = Builder::new_local(&turso_file)
            .with_mvcc(mvcc_enabled)
            .build()
            .await
            .context(TursoDatabaseSnafu)?;

        db.connect().context(TursoDatabaseSnafu)
    }

    /// Returns the shared connection pool for a `Turso` database
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<TursoConnectionPool>> {
        let turso_file = self.turso_file_path(source)?;
        let mvcc_enabled = self.parse_mvcc_enabled(source)?;

        let mut pools = self.pools.lock().await;
        if let Some(pool) = pools.get(&turso_file) {
            Ok(Arc::clone(pool))
        } else {
            let pool = Arc::new(TursoConnectionPool::new(&turso_file, mvcc_enabled).await?);
            pools.insert(turso_file, Arc::clone(&pool));
            Ok(pool)
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("turso_file"),
    ParameterSpec::component("turso_mvcc")
        .description("Enable Multi-Version Concurrency Control (MVCC) for Turso database")
        .default("disabled")
        .one_of(&["enabled", "disabled"]),
    // Note: turso_url and turso_auth_token are not supported as accelerator parameters
    // They will be supported when Turso is implemented as a data connector
];

#[async_trait]
impl DataAccelerator for TursoAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "turso"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["turso", "db", "sqlite", "sqlite3"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.turso_file_path(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Turso,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            // Memory mode is never pre-initialized (always starts fresh)
            return false;
        }

        // Check if the file exists for file mode
        self.has_existing_file(source)
    }

    /// Initializes a Turso database for the dataset.
    ///
    /// Supports two acceleration modes:
    /// - **Memory mode**: Creates an in-memory database (path = ":memory:")
    /// - **File mode**: Creates a file-based database at the specified or default path
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Reject remote database configurations (not supported as accelerators)
        if let Some(acceleration) = source.acceleration()
            && (acceleration.params.contains_key("turso_url")
                || acceleration.params.contains_key("turso_auth_token"))
        {
            return Err(Error::RemoteDatabaseNotSupported.into());
        }

        let path = self.file_path(source)?;

        // Handle memory mode: no file operations needed
        if path == ":memory:" {
            self.get_connection(source).await?;
            return Ok(());
        }

        // Handle file mode: validate path and setup file-based database
        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("turso_file") {
                make_spice_data_directory()
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            } else if !self.is_valid_file(source) {
                if std::path::Path::new(&path).is_dir() {
                    return Err(Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(&path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }

            download_snapshot_if_needed(acceleration, source, PathBuf::from(path)).await;

            // Initialize the database file
            self.get_connection(source).await?;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_none(),
            super::InvalidConfigurationSnafu {
                msg: "Turso data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        // Determine the database path
        // When called with a source (from DataAccelerator trait), turso_file_path returns:
        //   - ":memory:" for memory mode (!is_file_accelerated())
        //   - A file path for file mode (is_file_accelerated())
        // When called without a source (standalone external table), use provided file or memory mode
        let db_path = if let Some(source) = source {
            self.turso_file_path(source)?
        } else if let Some(file) = cmd.options.get("file") {
            file.clone()
        } else {
            ":memory:".to_string()
        };

        // Get MVCC setting
        let mvcc_enabled = if let Some(source) = source {
            self.parse_mvcc_enabled(source)?
        } else {
            false // Default to disabled for external tables without source
        };

        // Get or create connection pool
        let pool = {
            let mut pools = self.pools.lock().await;
            if let Some(pool) = pools.get(&db_path) {
                Arc::clone(pool)
            } else {
                let new_pool = Arc::new(TursoConnectionPool::new(&db_path, mvcc_enabled).await?);
                pools.insert(db_path.clone(), Arc::clone(&new_pool));
                new_pool
            }
        };

        // Create the table if it doesn't exist
        let conn = pool.connect().await?;
        let table_name = cmd.name.table().to_string();

        // Build CREATE TABLE statement from schema
        let mut columns = Vec::new();
        for field in cmd.schema.fields() {
            let col_type = match field.data_type() {
                // Integer types map to SQLite INTEGER
                DataType::Int64
                | DataType::Int32
                | DataType::Int16
                | DataType::Int8
                | DataType::UInt64
                | DataType::UInt32
                | DataType::UInt16
                | DataType::UInt8 => "INTEGER",
                // Floating point types map to REAL
                DataType::Float64 | DataType::Float32 => "REAL",
                // String types map to TEXT
                DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
                // Binary types map to BLOB
                DataType::Binary | DataType::LargeBinary => "BLOB",
                // Boolean maps to INTEGER (0/1)
                DataType::Boolean => "INTEGER",
                // Temporal types map to INTEGER
                DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => "INTEGER",
                // Decimal types map to REAL
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "REAL",
                // Complex types (List, Struct, etc.) map to TEXT (JSON serialized)
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                    "TEXT"
                }
                // Default to TEXT for unsupported types (serialized as JSON or string)
                _ => "TEXT",
            };
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            columns.push(format!("{} {}{}", field.name(), col_type, nullable));
        }

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name,
            columns.join(", ")
        );

        conn.execute(&create_sql, ())
            .await
            .map_err(|e| Error::AccelerationCreationFailed {
                source: Box::new(e),
            })?;

        // Handle indexes if specified
        if let Some(indexes_str) = cmd.options.get("indexes") {
            if mvcc_enabled {
                // Indexes are not yet supported in MVCC mode
                tracing::warn!(
                    "Indexes are not yet supported in MVCC mode for Turso. Skipping index creation for table '{}'",
                    table_name
                );
            } else {
                // Parse the indexes option string
                use datafusion_table_providers::util::hashmap_from_option_string;
                let indexes = hashmap_from_option_string::<String, String>(indexes_str);

                // Create indexes
                for (column_ref_str, index_type_str) in indexes {
                    let index_type = crate::component::dataset::acceleration::IndexType::from(
                        index_type_str.as_str(),
                    );
                    let index_name = format!(
                        "idx_{}_{}",
                        table_name,
                        column_ref_str.replace(['(', ')', ' ', ','], "_")
                    );
                    let unique_clause = match &index_type {
                        crate::component::dataset::acceleration::IndexType::Unique => "UNIQUE ",
                        crate::component::dataset::acceleration::IndexType::Enabled => "",
                    };

                    let create_index_sql = format!(
                        "CREATE {}INDEX IF NOT EXISTS {} ON {} ({})",
                        unique_clause, index_name, table_name, column_ref_str
                    );

                    conn.execute(&create_index_sql, ()).await.map_err(|e| {
                        Error::AccelerationCreationFailed {
                            source: Box::new(e),
                        }
                    })?;

                    tracing::debug!(
                        "Created {}index '{}' on table '{}' for columns: {}",
                        if unique_clause.is_empty() {
                            ""
                        } else {
                            "unique "
                        },
                        index_name,
                        table_name,
                        column_ref_str
                    );
                }
            }
        }

        // Create the table provider
        let schema = Arc::new(Schema::new(
            cmd.schema
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
                .collect::<Vec<_>>(),
        ));

        let turso_provider = Arc::new(TursoTableProvider::new(schema, table_name, pool));

        // Wrap in PolyTableProvider for proper read/write separation
        // This allows the table to support both reading and writing operations
        let write_provider = Arc::clone(&turso_provider);
        let delete_provider = Arc::clone(&turso_provider);
        let read_provider = turso_provider as Arc<dyn TableProvider>;

        let table_provider = Arc::new(PolyTableProvider::new(
            write_provider,
            delete_provider,
            read_provider,
        ));

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        "turso"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runtime;
    use crate::component::dataset::acceleration::{Acceleration, Mode};
    use crate::component::dataset::builder::DatasetBuilder;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, cast, col, dml::InsertOp, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_turso_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "turso_file_accelerator_init".to_string(),
            "turso_file_accelerator_init",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = TursoAccelerator::new();
        assert!(!accelerator.is_initialized(&dataset));

        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        assert!(accelerator.is_initialized(&dataset));
        assert!(accelerator.file_path(&dataset).is_ok());

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn test_remote_params_rejected() {
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        // Test with turso_url
        let mut dataset =
            DatasetBuilder::try_new("turso_remote_test_url".to_string(), "turso_remote_test_url")
                .expect("Failed to create builder")
                .with_app(Arc::new(app.clone()))
                .with_runtime(Arc::new(rt.clone()))
                .build()
                .expect("Failed to build dataset");

        let mut params = HashMap::new();
        params.insert(
            "turso_url".to_string(),
            "libsql://test.turso.io".to_string(),
        );

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            params,
            ..Default::default()
        });

        let accelerator = TursoAccelerator::new();
        let result = accelerator.init(&dataset).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Remote Turso databases are not supported")
        );

        // Test with turso_auth_token
        let mut dataset2 = DatasetBuilder::try_new(
            "turso_remote_test_token".to_string(),
            "turso_remote_test_token",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        let mut params2 = HashMap::new();
        params2.insert("turso_auth_token".to_string(), "secret_token".to_string());

        dataset2.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            params: params2,
            ..Default::default()
        });

        let result2 = accelerator.init(&dataset2).await;
        assert!(result2.is_err());
        assert!(
            result2
                .unwrap_err()
                .to_string()
                .contains("Remote Turso databases are not supported")
        );
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_round_trip_turso() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_turso_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, None)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr3)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let table =
            get_deletion_provider(table).expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);
    }

    #[tokio::test]
    async fn test_projection_filter_limit_pushdown() {
        // Create a schema with multiple columns
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Int64, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_pushdown_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, None)
            .await
            .expect("table should be created");

        // Insert test data
        let id_arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_arr = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_arr = Int64Array::from(vec![100, 200, 300, 400, 500]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(name_arr), Arc::new(value_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);
        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Test 1: Projection pushdown - select only specific columns
        let projection = Some(vec![0_usize, 2_usize]); // id and value columns
        let scan_plan = table
            .scan(&ctx.state(), projection.as_ref(), &[], None)
            .await
            .expect("scan should be successful");

        // Verify the projected schema only contains the selected columns
        let projected_schema = scan_plan.schema();
        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "id");
        assert_eq!(projected_schema.field(1).name(), "value");

        // Test 2: Filter pushdown - add WHERE clause
        let filter = col("value").gt(lit(200_i64));
        let scan_with_filter = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("scan with filter should be successful");

        let result = collect(scan_with_filter, ctx.task_ctx())
            .await
            .expect("query with filter successful");

        // Should return 3 rows (value > 200: 300, 400, 500)
        assert_eq!(result[0].num_rows(), 3);

        // Test 3: Limit pushdown
        let scan_with_limit = table
            .scan(&ctx.state(), None, &[], Some(2))
            .await
            .expect("scan with limit should be successful");

        let result_with_limit = collect(scan_with_limit, ctx.task_ctx())
            .await
            .expect("query with limit successful");

        // Should return at most 2 rows
        let total_rows: usize = result_with_limit.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 2);

        // Test 4: Combined projection, filter, and limit
        let projection = Some(vec![1_usize]); // name column only
        let filter = col("id").gt(lit(2_i64));
        let limit = Some(2);

        let scan_combined = table
            .scan(&ctx.state(), projection.as_ref(), &[filter], limit)
            .await
            .expect("combined scan should be successful");

        // Verify schema has only the projected column
        let combined_schema = scan_combined.schema();
        assert_eq!(combined_schema.fields().len(), 1);
        assert_eq!(combined_schema.field(0).name(), "name");

        let result_combined = collect(scan_combined, ctx.task_ctx())
            .await
            .expect("combined query successful");

        // Should return at most 2 rows with id > 2 (Charlie, David)
        let total_rows: usize = result_combined.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 2);
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_sql_generation() {
        // Test SQL generation with various combinations
        let full_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Int64, false),
        ]));

        let pool = Arc::new(TursoConnectionPool {
            database: Arc::new(
                Builder::new_local(":memory:")
                    .with_mvcc(true)
                    .build()
                    .await
                    .expect("should create database"),
            ),
            mvcc_enabled: true,
            db_path: ":memory:".to_string(),
        });

        // Test 1: Full schema (no projection), no filter, no limit
        let exec1 = TursoExec::new(
            Arc::clone(&full_schema),
            "test_table".to_string(),
            Arc::clone(&pool),
            &[],
            None,
        );
        let sql1 = exec1.sql().expect("should generate SQL");
        assert!(sql1.contains("SELECT"));
        assert!(sql1.contains("\"id\""));
        assert!(sql1.contains("\"name\""));
        assert!(sql1.contains("\"value\""));
        assert!(sql1.contains("FROM test_table"));
        assert!(!sql1.contains("WHERE"));
        assert!(!sql1.contains("LIMIT"));

        // Test 1b: Projected schema (only id and name columns) - simulates projection pushdown
        let projected_schema = Arc::new(full_schema.project(&[0, 1]).expect("should project"));
        let exec1b = TursoExec::new(
            Arc::clone(&projected_schema),
            "test_table".to_string(),
            Arc::clone(&pool),
            &[],
            None,
        );
        let sql1b = exec1b.sql().expect("should generate SQL");
        assert!(sql1b.contains("SELECT"));
        assert!(sql1b.contains("\"id\""));
        assert!(sql1b.contains("\"name\""));
        assert!(
            !sql1b.contains("\"value\""),
            "Projected schema should not include 'value' column"
        );
        assert_eq!(
            sql1b, "SELECT \"id\", \"name\" FROM test_table",
            "SQL should only include projected columns"
        );

        // Test 2: With limit
        let exec2 = TursoExec::new(
            Arc::clone(&full_schema),
            "test_table".to_string(),
            Arc::clone(&pool),
            &[],
            Some(10),
        );
        let sql2 = exec2.sql().expect("should generate SQL");
        assert!(sql2.contains("LIMIT 10"));

        // Test 3: With filter
        let filter = col("id").gt(lit(5_i64));
        let exec3 = TursoExec::new(
            Arc::clone(&full_schema),
            "test_table".to_string(),
            Arc::clone(&pool),
            &[filter],
            None,
        );
        let sql3 = exec3.sql().expect("should generate SQL");
        assert!(sql3.contains("WHERE"));

        // Test 4: With projection, filter and limit - full pushdown test
        let projected_schema = Arc::new(full_schema.project(&[1]).expect("should project")); // Only "name" column
        let filter = col("id").gt(lit(5_i64));
        let exec4 = TursoExec::new(
            projected_schema,
            "test_table".to_string(),
            pool,
            &[filter],
            Some(5),
        );
        let sql4 = exec4.sql().expect("should generate SQL");
        assert!(sql4.contains("\"name\""), "Should contain projected column");
        assert!(
            !sql4.contains("\"id\""),
            "Should not contain non-projected id column in SELECT list"
        );
        assert!(
            !sql4.contains("\"value\""),
            "Should not contain non-projected value column"
        );
        assert!(sql4.contains("WHERE"), "Should have filter");
        assert!(sql4.contains("LIMIT 5"), "Should have limit");
        // Note: The WHERE clause will reference 'id' even though it's not in the projection
        // This is correct SQL behavior - you can filter on columns not in the SELECT list
    }

    #[tokio::test]
    async fn test_file_mode_turso_creation() {
        // Test that file mode creates a Turso database at a specified path
        let test_path = "/tmp/test_turso_file_mode.db";

        // Clean up if file exists from previous test
        let _ = std::fs::remove_file(test_path);
        let _ = std::fs::remove_file(format!("{}-wal", test_path));
        let _ = std::fs::remove_file(format!("{}-shm", test_path));
        let _ = std::fs::remove_file(format!("{}-log", test_path));

        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let mut options = HashMap::new();
        options.insert("file".to_string(), test_path.to_string());

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_file_mode_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, None)
            .await
            .expect("table should be created");

        // Verify the file was created
        assert!(
            std::path::Path::new(test_path).exists(),
            "Turso database file should be created at specified path"
        );

        // Test that we can insert and query data
        let id_arr = Int64Array::from(vec![1, 2, 3]);
        let name_arr = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(name_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Query back the data
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should be successful");

        let results = collect(scan, ctx.task_ctx())
            .await
            .expect("scan successful");

        assert_eq!(results.len(), 1, "should have 1 batch");
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3, "should have 3 rows");

        // Verify data
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id should be Int64Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be StringArray");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");

        // Clean up - drop the table first to close connections
        drop(table);
        drop(ctx);

        // Give a moment for connections to close
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Clean up all database files
        let _ = std::fs::remove_file(test_path);
        let _ = std::fs::remove_file(format!("{}-wal", test_path));
        let _ = std::fs::remove_file(format!("{}-shm", test_path));
        let _ = std::fs::remove_file(format!("{}-log", test_path));
    }

    #[tokio::test]
    async fn test_file_mode_turso_creation_default_path() {
        // Test that file mode creates a Turso database using default path when not specified
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "turso_default_path_test".to_string(),
            "turso_default_path_test",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = TursoAccelerator::new();

        // Initialize the accelerator
        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        // Verify initialization
        assert!(
            accelerator.is_initialized(&dataset),
            "accelerator should be initialized"
        );

        // Get the file path
        let file_path = accelerator
            .file_path(&dataset)
            .expect("should have file path");

        // Verify the file was created at the default location
        assert!(
            std::path::Path::new(&file_path).exists(),
            "Turso database file should be created at default path"
        );

        // Verify the path includes the dataset name
        assert!(
            file_path.contains("turso_default_path_test"),
            "File path should contain dataset name"
        );

        // Now test that we can create a table and use it
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("value", DataType::Utf8, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_default_path_table"),
            location: file_path.clone(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, None)
            .await
            .expect("table should be created");

        // Insert test data
        let id_arr = Int64Array::from(vec![10, 20, 30]);
        let value_arr = StringArray::from(vec!["A", "B", "C"]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(value_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Query back the data to verify it works
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should be successful");

        let results = collect(scan, ctx.task_ctx())
            .await
            .expect("scan successful");

        assert_eq!(results.len(), 1, "should have 1 batch");
        assert_eq!(results[0].num_rows(), 3, "should have 3 rows");

        // Clean up
        std::fs::remove_file(&file_path).ok();
    }

    #[tokio::test]
    async fn test_timestamp_unit_conversion() {
        // Test that timestamps are correctly converted between different units
        // All timestamps are stored as milliseconds in Turso, but should be
        // correctly scaled when reading back based on the schema's unit

        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        use arrow::datatypes::TimeUnit;

        // Test value: 2024-01-01 00:00:00 UTC
        // In different units:
        const TEST_TIMESTAMP_SECONDS: i64 = 1704067200;
        const TEST_TIMESTAMP_MILLIS: i64 = 1704067200000;
        const TEST_TIMESTAMP_MICROS: i64 = 1704067200000000;
        const TEST_TIMESTAMP_NANOS: i64 = 1704067200000000000;

        let ctx = SessionContext::new();

        // Test 1: TimestampSecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_seconds"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, None)
                .await
                .expect("table should be created");

            // Insert timestamp in seconds
            let ts_arr = TimestampSecondArray::from(vec![TEST_TIMESTAMP_SECONDS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("should be TimestampSecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_SECONDS,
                "TimestampSecond should round-trip correctly"
            );
        }

        // Test 2: TimestampMillisecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_millis"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, None)
                .await
                .expect("table should be created");

            // Insert timestamp in milliseconds
            let ts_arr = TimestampMillisecondArray::from(vec![TEST_TIMESTAMP_MILLIS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("should be TimestampMillisecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_MILLIS,
                "TimestampMillisecond should round-trip correctly"
            );
        }

        // Test 3: TimestampMicrosecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_micros"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, None)
                .await
                .expect("table should be created");

            // Insert timestamp in microseconds
            let ts_arr = TimestampMicrosecondArray::from(vec![TEST_TIMESTAMP_MICROS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("should be TimestampMicrosecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_MICROS,
                "TimestampMicrosecond should round-trip correctly"
            );
        }

        // Test 4: TimestampNanosecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_nanos"),
                location: String::new(),
                file_type: String::new(),
                table_partition_cols: vec![],
                if_not_exists: true,
                definition: None,
                order_exprs: vec![],
                unbounded: false,
                options: HashMap::new(),
                constraints: Constraints::new_unverified(vec![]),
                column_defaults: HashMap::default(),
                temporary: false,
            };

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, None)
                .await
                .expect("table should be created");

            // Insert timestamp in nanoseconds
            let ts_arr = TimestampNanosecondArray::from(vec![TEST_TIMESTAMP_NANOS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("should be TimestampNanosecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_NANOS,
                "TimestampNanosecond should round-trip correctly"
            );
        }
    }
}
