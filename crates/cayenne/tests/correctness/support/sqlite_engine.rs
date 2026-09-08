// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQLite reference engine for Cayenne result-correctness tests.
//!
//! Loads Arrow / parquet tables into an in-process `rusqlite` database and
//! returns query results as `RecordBatch`es for the harness compare path.
//! No feature gate: `rusqlite` is always linked by cayenne.

use std::path::Path;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use rusqlite::{Connection, types::ValueRef};

/// Open a temp SQLite DB and load named parquet tables from `parquet_dir`.
pub async fn load_sqlite_from_parquet(
    parquet_dir: &Path,
    tables: &[&str],
) -> (tempfile::TempDir, Connection) {
    let temp = tempfile::tempdir().expect("sqlite temp");
    let db_path = temp.path().join("parity.sqlite");
    let conn = Connection::open(&db_path).expect("sqlite open");
    conn.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
        .expect("sqlite pragma");

    let ctx = SessionContext::new();
    for table in tables {
        let path = parquet_dir.join(format!("{table}.parquet"));
        let path_str = path.to_string_lossy().into_owned();
        let df = ctx
            .read_parquet(path_str.as_str(), ParquetReadOptions::default())
            .await
            .unwrap_or_else(|e| panic!("sqlite load read parquet {table}: {e}"));
        let batches = df
            .collect()
            .await
            .unwrap_or_else(|e| panic!("sqlite load collect {table}: {e}"));
        create_and_insert(&conn, table, &batches);
    }
    (temp, conn)
}

/// Load in-memory RecordBatches as named tables.
pub fn load_sqlite_from_batches(tables: &[(&str, RecordBatch)]) -> (tempfile::TempDir, Connection) {
    let temp = tempfile::tempdir().expect("sqlite temp");
    let db_path = temp.path().join("parity.sqlite");
    let conn = Connection::open(&db_path).expect("sqlite open");
    conn.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
        .expect("sqlite pragma");
    for (name, batch) in tables {
        create_and_insert(&conn, name, std::slice::from_ref(batch));
    }
    (temp, conn)
}

fn create_and_insert(conn: &Connection, table: &str, batches: &[RecordBatch]) {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    let ddl = create_table_sql(table, &schema);
    conn.execute_batch(&ddl)
        .unwrap_or_else(|e| panic!("sqlite create {table}: {e}\n{ddl}"));

    if batches.iter().all(|b| b.num_rows() == 0) {
        return;
    }

    let cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let placeholders = (1..=cols.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = cols.join(", ");
    let insert_sql = format!("INSERT INTO {table} ({col_list}) VALUES ({placeholders})");

    let tx = conn.unchecked_transaction().expect("sqlite begin");
    {
        let mut stmt = tx
            .prepare(&insert_sql)
            .unwrap_or_else(|e| panic!("sqlite prepare insert {table}: {e}"));
        for batch in batches {
            insert_batch(&mut stmt, batch);
        }
    }
    tx.commit().expect("sqlite commit");
}

fn create_table_sql(table: &str, schema: &Schema) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let ty = arrow_to_sqlite_type(f.data_type());
            format!("{} {ty}", f.name())
        })
        .collect();
    format!("CREATE TABLE {table} ({})", cols.join(", "))
}

fn arrow_to_sqlite_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "INTEGER",
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "INTEGER",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "REAL",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BLOB",
        // Timestamps / dates stored as text for portable equality.
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => "TEXT",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "TEXT",
        _ => "TEXT",
    }
}

fn insert_batch(stmt: &mut rusqlite::Statement<'_>, batch: &RecordBatch) {
    let n = batch.num_rows();
    let ncols = batch.num_columns();
    for row in 0..n {
        let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(ncols);
        for col in 0..ncols {
            values.push(array_value_to_sqlite(batch.column(col).as_ref(), row, col));
        }
        let params: Vec<&dyn rusqlite::types::ToSql> = values
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();
        stmt.execute(params.as_slice())
            .unwrap_or_else(|e| panic!("sqlite insert row {row}: {e}"));
    }
}

fn array_value_to_sqlite(array: &dyn Array, row: usize, col: usize) -> rusqlite::types::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(row) {
        return rusqlite::types::Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().expect("bool");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().expect("i8");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().expect("i16");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().expect("i32");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().expect("i64");
            rusqlite::types::Value::Integer(a.value(row))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().expect("u8");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().expect("u16");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().expect("u32");
            rusqlite::types::Value::Integer(i64::from(a.value(row)))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().expect("u64");
            rusqlite::types::Value::Integer(a.value(row) as i64)
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().expect("f32");
            rusqlite::types::Value::Real(f64::from(a.value(row)))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().expect("f64");
            rusqlite::types::Value::Real(a.value(row))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().expect("utf8");
            rusqlite::types::Value::Text(a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8");
            rusqlite::types::Value::Text(a.value(row).to_string())
        }
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("utf8view");
            rusqlite::types::Value::Text(a.value(row).to_string())
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().expect("bin");
            rusqlite::types::Value::Blob(a.value(row).to_vec())
        }
        // Coercing an unconvertible type to a placeholder string would load
        // values into the oracle that the source data never had, so the
        // comparison could pass on corrupt data. Fail the load instead.
        other => panic!(
            "cannot load Arrow type {other:?} into the SQLite oracle (column {col}, row {row}): \
             add an explicit conversion arm to `array_value_to_sqlite`"
        ),
    }
}

/// Execute SQL on SQLite and collect results as a single `RecordBatch`.
pub fn sqlite_query_batches(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("sqlite prepare: {e}"))?;
    let col_count = stmt.column_count();
    if col_count == 0 {
        // DDL / empty result.
        return Ok(vec![]);
    }

    let names: Vec<String> = (0..col_count)
        .map(|i| {
            stmt.column_name(i)
                .map_or_else(|_| format!("col_{i}"), ToOwned::to_owned)
        })
        .collect();

    let mut rows = stmt.query([]).map_err(|e| format!("sqlite query: {e}"))?;

    // First pass: collect raw values to infer types from first non-null.
    let mut raw_rows: Vec<Vec<OwnedSqlValue>> = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("sqlite next: {e}"))? {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            vals.push(OwnedSqlValue::from_value_ref(
                row.get_ref(i).map_err(|e| format!("sqlite get {i}: {e}"))?,
            ));
        }
        raw_rows.push(vals);
    }

    // Match DuckDB/Cayenne: no rows ⇒ empty batch list (not a zero-row schema
    // batch). `compare_query_result_batches` treats `[]` vs `[]` as Pass and
    // `[]` vs `[0-row]` as NoAnswer.
    if raw_rows.is_empty() {
        return Ok(vec![]);
    }
    let schema = infer_schema(&names, &raw_rows);
    let batch = build_batch(Arc::clone(&schema), &raw_rows)?;
    Ok(vec![batch])
}

#[derive(Clone, Debug)]
enum OwnedSqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl OwnedSqlValue {
    fn from_value_ref(v: ValueRef<'_>) -> Self {
        match v {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(i) => Self::Integer(i),
            ValueRef::Real(f) => Self::Real(f),
            ValueRef::Text(t) => Self::Text(String::from_utf8_lossy(t).into_owned()),
            ValueRef::Blob(b) => Self::Blob(b.to_vec()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ColKind {
    Null,
    Int,
    Real,
    Text,
}

fn infer_schema(names: &[String], rows: &[Vec<OwnedSqlValue>]) -> SchemaRef {
    let mut kinds = vec![ColKind::Null; names.len()];
    for row in rows {
        for (i, v) in row.iter().enumerate() {
            let k = match v {
                OwnedSqlValue::Null => ColKind::Null,
                OwnedSqlValue::Integer(_) => ColKind::Int,
                OwnedSqlValue::Real(_) => ColKind::Real,
                OwnedSqlValue::Text(_) | OwnedSqlValue::Blob(_) => ColKind::Text,
            };
            kinds[i] = promote(kinds[i], k);
        }
    }
    let fields: Vec<Field> = names
        .iter()
        .zip(kinds.iter())
        .map(|(name, kind)| {
            let dt = match kind {
                ColKind::Null | ColKind::Int => DataType::Int64,
                ColKind::Real => DataType::Float64,
                ColKind::Text => DataType::Utf8,
            };
            // SQLite aggregates often return NULL on empty input — mark nullable.
            Field::new(name, dt, true)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn promote(a: ColKind, b: ColKind) -> ColKind {
    use ColKind::{Int, Null, Real, Text};
    match (a, b) {
        (x, Null) | (Null, x) => x,
        (Int, Int) => Int,
        (Real, Real) | (Int, Real) | (Real, Int) => Real,
        (Text, _) | (_, Text) => Text,
    }
}

fn build_batch(schema: SchemaRef, rows: &[Vec<OwnedSqlValue>]) -> Result<RecordBatch, String> {
    let n = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_i, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(n);
                for row in rows {
                    match &row[col_i] {
                        OwnedSqlValue::Null => b.append_null(),
                        OwnedSqlValue::Integer(i) => b.append_value(*i),
                        OwnedSqlValue::Real(f) => b.append_value(*f as i64),
                        OwnedSqlValue::Text(t) => {
                            if let Ok(i) = t.parse::<i64>() {
                                b.append_value(i);
                            } else {
                                b.append_null();
                            }
                        }
                        OwnedSqlValue::Blob(_) => b.append_null(),
                    }
                }
                columns.push(Arc::new(b.finish()));
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(n);
                for row in rows {
                    match &row[col_i] {
                        OwnedSqlValue::Null => b.append_null(),
                        OwnedSqlValue::Integer(i) => b.append_value(*i as f64),
                        OwnedSqlValue::Real(f) => b.append_value(*f),
                        OwnedSqlValue::Text(t) => {
                            if let Ok(f) = t.parse::<f64>() {
                                b.append_value(f);
                            } else {
                                b.append_null();
                            }
                        }
                        OwnedSqlValue::Blob(_) => b.append_null(),
                    }
                }
                columns.push(Arc::new(b.finish()));
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(n, n * 8);
                for row in rows {
                    match &row[col_i] {
                        OwnedSqlValue::Null => b.append_null(),
                        OwnedSqlValue::Integer(i) => b.append_value(i.to_string()),
                        OwnedSqlValue::Real(f) => b.append_value(f.to_string()),
                        OwnedSqlValue::Text(t) => b.append_value(t),
                        OwnedSqlValue::Blob(blob) => {
                            b.append_value(String::from_utf8_lossy(blob));
                        }
                    }
                }
                columns.push(Arc::new(b.finish()));
            }
            DataType::Boolean => {
                let mut b = BooleanBuilder::with_capacity(n);
                for row in rows {
                    match &row[col_i] {
                        OwnedSqlValue::Null => b.append_null(),
                        OwnedSqlValue::Integer(i) => b.append_value(*i != 0),
                        _ => b.append_null(),
                    }
                }
                columns.push(Arc::new(b.finish()));
            }
            other => {
                return Err(format!("sqlite result unsupported Arrow type {other:?}"));
            }
        }
    }
    RecordBatch::try_new(schema, columns).map_err(|e| format!("sqlite batch: {e}"))
}
