/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use duckdb_0_9_2::DuckdbConnectionManager;
use duckdb_0_9_2::ToSql;
use snafu::{prelude::*, ResultExt};

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb_0_9_2::Error },
}

pub struct LegacyDuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl<'a> DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'a dyn ToSql>
    for LegacyDuckDbConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<&dyn SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'a dyn ToSql>>
    {
        Some(self)
    }
}

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
    for LegacyDuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        LegacyDuckDbConnection { conn }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .context(DuckDBSnafu)?;

        let result: duckdb_0_9_2::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(Arc::new(upgrade_schema(&result.get_schema())))
    }

    fn query_arrow(&self, sql: &str, params: &[&dyn ToSql]) -> Result<SendableRecordBatchStream> {
        let mut stmt = self.conn.prepare(sql).context(DuckDBSnafu)?;

        let result: duckdb_0_9_2::Arrow<'_> = stmt.query_arrow(params).context(DuckDBSnafu)?;
        let schema = result.get_schema();
        let recs: Vec<duckdb_0_9_2::arrow::array::RecordBatch> = result.collect();

        let new_recs = recs.iter().map(upgrade_record_batch).collect::<Vec<_>>();

        Ok(Box::pin(MemoryStream::try_new(
            new_recs,
            Arc::new(upgrade_schema(&schema)),
            None,
        )?))
    }

    fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        let rows_modified = self.conn.execute(sql, params).context(DuckDBSnafu)?;
        Ok(rows_modified as u64)
    }
}
fn upgrade_schema(schema: &duckdb_0_9_2::arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            arrow::datatypes::Field::new(f.name(), map_data_type(f.data_type()), f.is_nullable())
        })
        .collect::<Vec<_>>();

    arrow::datatypes::Schema::new(fields)
}

fn upgrade_record_batch(
    record_batch: &duckdb_0_9_2::arrow::array::RecordBatch,
) -> arrow::array::RecordBatch {
    let mut buffer = std::io::Cursor::new(Vec::new());
    {
        let mut writer = arrow_49_0_0::ipc::writer::StreamWriter::try_new(
            &mut buffer,
            record_batch.schema().as_ref(),
        )
        .unwrap();
        writer.write(&record_batch).unwrap();
        writer.finish().unwrap();
    }
    let data = buffer.into_inner();

    let buffer = Cursor::new(data);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(buffer, None).unwrap();
    let new_record_batch = reader.next().unwrap().unwrap();

    return new_record_batch;
}

fn map_data_type(
    field_type: &duckdb_0_9_2::arrow::datatypes::DataType,
) -> arrow::datatypes::DataType {
    match field_type {
        duckdb_0_9_2::arrow::datatypes::DataType::Null => arrow::datatypes::DataType::Null,
        duckdb_0_9_2::arrow::datatypes::DataType::Boolean => arrow::datatypes::DataType::Boolean,

        duckdb_0_9_2::arrow::datatypes::DataType::Int8 => arrow::datatypes::DataType::Int8,
        duckdb_0_9_2::arrow::datatypes::DataType::Int16 => arrow::datatypes::DataType::Int16,
        duckdb_0_9_2::arrow::datatypes::DataType::Int32 => arrow::datatypes::DataType::Int32,
        duckdb_0_9_2::arrow::datatypes::DataType::Int64 => arrow::datatypes::DataType::Int64,

        duckdb_0_9_2::arrow::datatypes::DataType::UInt8 => arrow::datatypes::DataType::UInt8,
        duckdb_0_9_2::arrow::datatypes::DataType::UInt16 => arrow::datatypes::DataType::UInt16,
        duckdb_0_9_2::arrow::datatypes::DataType::UInt32 => arrow::datatypes::DataType::UInt32,
        duckdb_0_9_2::arrow::datatypes::DataType::UInt64 => arrow::datatypes::DataType::UInt64,

        duckdb_0_9_2::arrow::datatypes::DataType::Float16 => arrow::datatypes::DataType::Float16,
        duckdb_0_9_2::arrow::datatypes::DataType::Float32 => arrow::datatypes::DataType::Float32,
        duckdb_0_9_2::arrow::datatypes::DataType::Float64 => arrow::datatypes::DataType::Float64,

        duckdb_0_9_2::arrow::datatypes::DataType::Utf8 => arrow::datatypes::DataType::Utf8,
        duckdb_0_9_2::arrow::datatypes::DataType::LargeUtf8 => {
            arrow::datatypes::DataType::LargeUtf8
        }

        duckdb_0_9_2::arrow::datatypes::DataType::Date32 => arrow::datatypes::DataType::Date32,
        duckdb_0_9_2::arrow::datatypes::DataType::Date64 => arrow::datatypes::DataType::Date64,

        duckdb_0_9_2::arrow::datatypes::DataType::Timestamp(p1, p2) => {
            arrow::datatypes::DataType::Timestamp(map_time_unit(p1), p2.clone())
        }
        duckdb_0_9_2::arrow::datatypes::DataType::Time32(p1) => {
            arrow::datatypes::DataType::Time32(map_time_unit(p1))
        }
        duckdb_0_9_2::arrow::datatypes::DataType::Time64(p1) => {
            arrow::datatypes::DataType::Time64(map_time_unit(p1))
        }
        duckdb_0_9_2::arrow::datatypes::DataType::Interval(p1) => {
            arrow::datatypes::DataType::Interval(map_interval_unit(p1))
        }
        duckdb_0_9_2::arrow::datatypes::DataType::Duration(p1) => {
            arrow::datatypes::DataType::Duration(map_time_unit(p1))
        }

        duckdb_0_9_2::arrow::datatypes::DataType::Binary => arrow::datatypes::DataType::Binary,
        duckdb_0_9_2::arrow::datatypes::DataType::LargeBinary => {
            arrow::datatypes::DataType::LargeBinary
        }

        duckdb_0_9_2::arrow::datatypes::DataType::Decimal128(p1, p2) => {
            arrow::datatypes::DataType::Decimal128(*p1, *p2)
        }
        duckdb_0_9_2::arrow::datatypes::DataType::Decimal256(p1, p2) => {
            arrow::datatypes::DataType::Decimal256(*p1, *p2)
        }

        _ => {
            tracing::warn!("Unsupported data type: {field_type}");
            arrow::datatypes::DataType::Null
        }
    }
}

fn map_time_unit(
    time_unit: &duckdb_0_9_2::arrow::datatypes::TimeUnit,
) -> arrow::datatypes::TimeUnit {
    match time_unit {
        duckdb_0_9_2::arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Second,
        duckdb_0_9_2::arrow::datatypes::TimeUnit::Millisecond => {
            arrow::datatypes::TimeUnit::Millisecond
        }
        duckdb_0_9_2::arrow::datatypes::TimeUnit::Microsecond => {
            arrow::datatypes::TimeUnit::Microsecond
        }
        duckdb_0_9_2::arrow::datatypes::TimeUnit::Nanosecond => {
            arrow::datatypes::TimeUnit::Nanosecond
        }
    }
}

fn map_interval_unit(
    interval_unit: &duckdb_0_9_2::arrow::datatypes::IntervalUnit,
) -> arrow::datatypes::IntervalUnit {
    match interval_unit {
        duckdb_0_9_2::arrow::datatypes::IntervalUnit::YearMonth => {
            arrow::datatypes::IntervalUnit::YearMonth
        }
        duckdb_0_9_2::arrow::datatypes::IntervalUnit::DayTime => {
            arrow::datatypes::IntervalUnit::DayTime
        }
        duckdb_0_9_2::arrow::datatypes::IntervalUnit::MonthDayNano => {
            arrow::datatypes::IntervalUnit::MonthDayNano
        }
    }
}
