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

//! Spice **accelerators** vs **out-of-Spice** standalone engines — exact SQL
//! result equality (not performance).
//!
//! | Spice side | Standalone oracle |
//! |------------|-------------------|
//! | `DuckDB` accelerator (`DataAccelerator`) | `duckdb` crate |
//! | `SQLite` accelerator (`DataAccelerator`) | `rusqlite` crate |
//!
//! Cayenne vs standalone oracles live under `crates/cayenne/tests/result_correctness_*`.
//! Standalone DuckDB↔SQLite agreement (no Spice) lives under
//! `result_correctness_standalone_engines_test`.

#![recursion_limit = "256"]
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
// SQLite INTEGER widened into a Float64 column when the inferred column kind is
// real; fixture values are far inside f64's exact-integer range.
#![allow(clippy::cast_precision_loss)]

// Accelerator engines are their own crates and self-register through a linkme slice. Each
// integration test is a separate binary that links independently, and the linker drops an
// unreferenced slice static, so a binary exercising Cayenne must name the crate itself.
#[cfg(not(windows))]
use accelerator_cayenne as _;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::sql::TableReference;
use datafusion_table_providers::util::test::MockExec;
use test_framework::queries::Query;
use test_framework::queries::validation::{
    QueryValidationResult, RowOrder, compare_query_result_batches_with_sort_check,
};

// ---------------------------------------------------------------------------
// Shared micro fixtures (id, name, value) / (id, region)
// ---------------------------------------------------------------------------

fn fact_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

fn make_fact(rows: usize, groups: usize) -> RecordBatch {
    let group_count = groups.max(1);
    let ids: Vec<i64> = (0..rows as i64).collect();
    let names: Vec<String> = (0..rows)
        .map(|i| format!("group_{}", i % group_count))
        .collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 100).collect();
    RecordBatch::try_new(
        fact_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("fact")
}

fn make_dim(rows: usize) -> RecordBatch {
    const REGIONS: [&str; 4] = ["NA", "EU", "APAC", "LATAM"];
    let ids: Vec<i64> = (0..rows as i64).collect();
    let regions: Vec<&str> = (0..rows).map(|i| REGIONS[i % REGIONS.len()]).collect();
    RecordBatch::try_new(
        dim_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .expect("dim")
}

fn micro_queries() -> Vec<Query> {
    vec![
        Query::new(
            "micro_count_star".into(),
            "SELECT COUNT(*) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_sum_value".into(),
            "SELECT SUM(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_filter_sum".into(),
            "SELECT SUM(value) FROM t WHERE id BETWEEN 10 AND 50".into(),
            false,
        ),
        Query::new(
            "micro_groupby_name".into(),
            "SELECT name, COUNT(*), SUM(value) FROM t GROUP BY name".into(),
            false,
        ),
        Query::new(
            "micro_join_agg".into(),
            "SELECT d.region, SUM(t.value) FROM t JOIN d ON t.id = d.id GROUP BY d.region".into(),
            false,
        ),
        Query::new(
            "micro_pk_lookup".into(),
            "SELECT id, name, value FROM t WHERE id = 42".into(),
            false,
        ),
        Query::new(
            "micro_order_limit".into(),
            "SELECT id, name, value FROM t ORDER BY id LIMIT 10".into(),
            false,
        ),
    ]
}

fn create_cmd(name: &str, schema: SchemaRef) -> datafusion::logical_expr::CreateExternalTable {
    let df_schema = ToDFSchema::to_dfschema_ref(schema).expect("df schema");
    datafusion::logical_expr::CreateExternalTable {
        schema: df_schema,
        name: TableReference::bare(name),
        location: String::new(),
        file_type: String::new(),
        table_partition_cols: vec![],
        if_not_exists: true,
        or_replace: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: HashMap::new(),
        constraints: Constraints::new_unverified(vec![]),
        column_defaults: HashMap::default(),
        temporary: false,
    }
}

async fn insert_batch(table: &Arc<dyn TableProvider>, batch: RecordBatch) {
    let schema = batch.schema();
    let exec = MockExec::new(vec![Ok(batch)], schema);
    let ctx = SessionContext::new();
    let plan = table
        .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
        .await
        .expect("insert plan");
    collect(plan, ctx.task_ctx()).await.expect("insert collect");
}

/// Run SQL against Spice-accelerated tables via `DataFusion`.
///
/// Bare `SessionContext` + accelerator `TableProvider` hits a `DataFusion`
/// physical/logical schema mismatch on some aggregate shapes (`COUNT(*)`).
/// We therefore materialize each accelerator table through its **scan** path
/// into an in-memory table (still the accelerator's storage + scan plan), then
/// run the SQL against those scans. That exercises accelerator write+scan and
/// compares the resulting SQL answers to the standalone engine.
async fn spice_query(
    tables: &[(String, Arc<dyn TableProvider>)],
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    use datafusion::datasource::MemTable;

    let ctx = SessionContext::new();
    for (name, table) in tables {
        let plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .map_err(|e| format!("scan {name}: {e}"))?;
        let batches = collect(plan, ctx.task_ctx())
            .await
            .map_err(|e| format!("collect scan {name}: {e}"))?;
        // Prefer the physical schema from the scan (accelerators may rewrite
        // types vs the create-time logical schema).
        let schema = batches
            .first()
            .map_or_else(|| table.schema(), RecordBatch::schema);
        let mem = MemTable::try_new(schema, vec![batches])
            .map_err(|e| format!("memtable {name}: {e}"))?;
        ctx.register_table(name.as_str(), Arc::new(mem))
            .map_err(|e| format!("register {name}: {e}"))?;
    }
    let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
    df.collect().await.map_err(|e| format!("collect: {e}"))
}

fn compare(query: &Query, spice: &[RecordBatch], standalone: &[RecordBatch]) -> String {
    let sql_upper = query.sql.to_ascii_uppercase();
    let has_order = sql_upper.contains("ORDER BY");
    let has_limit = sql_upper.contains("LIMIT") || sql_upper.contains("OFFSET");
    // Multiset unless the row set itself depends on order, so tied rows under a
    // non-unique `ORDER BY` do not fail; the sort check below is tie-tolerant and
    // verifies each side against its own `ORDER BY` regardless.
    let order = if has_order && has_limit {
        RowOrder::Preserved
    } else {
        RowOrder::Multiset
    };
    match compare_query_result_batches_with_sort_check(
        &query.name,
        &query.sql,
        spice,
        standalone,
        order,
    ) {
        Ok(QueryValidationResult::Pass) => "PASS".into(),
        Ok(QueryValidationResult::Fail(reason)) => format!("FAIL {reason:?}"),
        Err(e) => format!("COMPARE_ERR {e}"),
    }
}

// ---------------------------------------------------------------------------
// Spice SQLite accelerator ↔ standalone rusqlite
// ---------------------------------------------------------------------------

#[cfg(feature = "sqlite")]
mod sqlite_accel {
    use super::{
        compare, create_cmd, insert_batch, make_dim, make_fact, micro_queries, spice_query,
    };
    use accelerator_sqlite::SqliteAccelerator;
    use arrow::array::RecordBatch;
    use data_accelerator_api::DataAccelerator;
    use datafusion::datasource::TableProvider;
    use rusqlite::Connection;
    use std::sync::Arc;

    fn load_standalone_sqlite(tables: &[(&str, RecordBatch)]) -> (tempfile::TempDir, Connection) {
        let temp = tempfile::tempdir().expect("sqlite temp");
        let path = temp.path().join("oracle.sqlite");
        let conn = Connection::open(&path).expect("open");
        conn.execute_batch("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;")
            .expect("pragma");
        for (name, batch) in tables {
            create_sqlite_table(&conn, name, batch);
            insert_sqlite_batch(&conn, name, batch);
        }
        (temp, conn)
    }

    fn create_sqlite_table(conn: &Connection, name: &str, batch: &RecordBatch) {
        let cols: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| {
                let ty = match f.data_type() {
                    arrow::datatypes::DataType::Int64
                    | arrow::datatypes::DataType::Int32
                    | arrow::datatypes::DataType::Boolean => "INTEGER",
                    arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                        "REAL"
                    }
                    _ => "TEXT",
                };
                format!("{} {ty}", f.name())
            })
            .collect();
        conn.execute_batch(&format!("CREATE TABLE {name} ({})", cols.join(", ")))
            .expect("create");
    }

    fn insert_sqlite_batch(conn: &Connection, name: &str, batch: &RecordBatch) {
        use arrow::array::{Array, Int64Array, StringArray};
        let schema = batch.schema();
        let col_names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let placeholders = (1..=col_names.len())
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {name} ({}) VALUES ({placeholders})",
            col_names.join(", ")
        );
        let mut stmt = conn.prepare(&sql).expect("prepare");
        for row in 0..batch.num_rows() {
            let mut values: Vec<rusqlite::types::Value> = Vec::new();
            for col in 0..batch.num_columns() {
                let arr = batch.column(col);
                if arr.is_null(row) {
                    values.push(rusqlite::types::Value::Null);
                    continue;
                }
                values.push(match arr.data_type() {
                    arrow::datatypes::DataType::Int64 => {
                        let a = arr.as_any().downcast_ref::<Int64Array>().expect("i64");
                        rusqlite::types::Value::Integer(a.value(row))
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        let a = arr.as_any().downcast_ref::<StringArray>().expect("utf8");
                        rusqlite::types::Value::Text(a.value(row).to_string())
                    }
                    other => panic!("unsupported insert type {other:?}"),
                });
            }
            let params: Vec<&dyn rusqlite::types::ToSql> = values
                .iter()
                .map(|v| v as &dyn rusqlite::types::ToSql)
                .collect();
            stmt.execute(params.as_slice()).expect("insert row");
        }
    }

    fn standalone_query(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>, String> {
        // Reuse cayenne-style conversion via a minimal path: fetch as text/int and build batches.
        // Prefer duck-simple: only types used by micro suite.
        use arrow::array::{Int64Builder, StringBuilder};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let col_count = stmt.column_count();
        if col_count == 0 {
            return Ok(vec![]);
        }
        let names: Vec<String> = (0..col_count)
            .map(|i| {
                stmt.column_name(i)
                    .map_or_else(|_| format!("col_{i}"), ToOwned::to_owned)
            })
            .collect();

        let mut rows_data: Vec<Vec<Option<rusqlite::types::Value>>> = Vec::new();
        let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut vals = Vec::with_capacity(col_count);
            for i in 0..col_count {
                vals.push(Some(
                    row.get::<_, rusqlite::types::Value>(i)
                        .map_err(|e| e.to_string())?,
                ));
            }
            rows_data.push(vals);
        }
        if rows_data.is_empty() {
            return Ok(vec![]);
        }

        // Infer Int64 vs Utf8 per column from first non-null.
        let mut kinds: Vec<&'static str> = vec!["int"; col_count];
        for row in &rows_data {
            for (i, v) in row.iter().enumerate() {
                if let Some(rusqlite::types::Value::Text(_)) = v {
                    kinds[i] = "text";
                } else if let Some(rusqlite::types::Value::Real(_)) = v {
                    kinds[i] = "real";
                }
            }
        }

        let fields: Vec<Field> = names
            .iter()
            .zip(kinds.iter())
            .map(|(n, k)| {
                let dt = match *k {
                    "text" => DataType::Utf8,
                    "real" => DataType::Float64,
                    _ => DataType::Int64,
                };
                Field::new(n, dt, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
        for (col_i, kind) in kinds.iter().enumerate() {
            match *kind {
                "text" => {
                    let mut b = StringBuilder::new();
                    for row in &rows_data {
                        match &row[col_i] {
                            Some(rusqlite::types::Value::Text(t)) => b.append_value(t),
                            Some(rusqlite::types::Value::Integer(i)) => {
                                b.append_value(i.to_string());
                            }
                            Some(rusqlite::types::Value::Real(f)) => b.append_value(f.to_string()),
                            Some(
                                rusqlite::types::Value::Null | rusqlite::types::Value::Blob(_),
                            )
                            | None => b.append_null(),
                        }
                    }
                    columns.push(Arc::new(b.finish()));
                }
                "real" => {
                    let mut b = arrow::array::Float64Builder::new();
                    for row in &rows_data {
                        match &row[col_i] {
                            Some(rusqlite::types::Value::Real(f)) => b.append_value(*f),
                            Some(rusqlite::types::Value::Integer(i)) => b.append_value(*i as f64),
                            _ => b.append_null(),
                        }
                    }
                    columns.push(Arc::new(b.finish()));
                }
                _ => {
                    let mut b = Int64Builder::new();
                    for row in &rows_data {
                        match &row[col_i] {
                            Some(rusqlite::types::Value::Integer(i)) => b.append_value(*i),
                            _ => b.append_null(),
                        }
                    }
                    columns.push(Arc::new(b.finish()));
                }
            }
        }
        Ok(vec![
            RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())?,
        ])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spice_sqlite_accel_vs_standalone_sqlite_micro() {
        let fact = make_fact(2_048, 64);
        let dim = make_dim(256);

        // Spice SQLite accelerator
        let accel = SqliteAccelerator::new();
        let t_table = accel
            .create_external_table(create_cmd("t", fact.schema()), None, vec![], None)
            .await
            .expect("create t");
        let d_table = accel
            .create_external_table(create_cmd("d", dim.schema()), None, vec![], None)
            .await
            .expect("create d");
        insert_batch(&t_table, fact.clone()).await;
        insert_batch(&d_table, dim.clone()).await;
        let spice_tables: Vec<(String, Arc<dyn TableProvider>)> =
            vec![("t".into(), t_table), ("d".into(), d_table)];

        // Standalone rusqlite (out of Spice)
        let (_tmp, conn) = load_standalone_sqlite(&[("t", fact), ("d", dim)]);

        let mut fails = Vec::new();
        for q in micro_queries() {
            let spice = spice_query(&spice_tables, &q.sql)
                .await
                .unwrap_or_else(|e| panic!("spice sql {}: {e}", q.name));
            let standalone = standalone_query(&conn, &q.sql)
                .unwrap_or_else(|e| panic!("sqlite sql {}: {e}", q.name));
            let status = compare(&q, &spice, &standalone);
            eprintln!(
                "spice-sqlite-accel vs standalone-sqlite / {} -> {status}",
                q.name
            );
            if !status.starts_with("PASS") {
                fails.push(format!("{}: {status}", q.name));
            }
        }
        assert!(
            fails.is_empty(),
            "Spice SQLite accelerator vs standalone SQLite failures: {fails:#?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Spice DuckDB accelerator ↔ standalone duckdb crate
// ---------------------------------------------------------------------------

#[cfg(feature = "duckdb")]
mod duckdb_accel {
    use super::{
        compare, create_cmd, insert_batch, make_dim, make_fact, micro_queries, spice_query,
    };
    use accelerator_duckdb::DuckDBAccelerator;
    use arrow::array::RecordBatch;
    use data_accelerator_api::DataAccelerator;
    use datafusion::datasource::TableProvider;
    use duckdb::Connection;
    use std::sync::Arc;

    fn load_standalone_duckdb(tables: &[(&str, RecordBatch)]) -> (tempfile::TempDir, Connection) {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let stage = tempfile::tempdir().expect("stage");
        for (name, batch) in tables {
            let path = stage.path().join(format!("{name}.parquet"));
            let file = std::fs::File::create(&path).expect("parquet file");
            let mut writer = ArrowWriter::try_new(
                file,
                batch.schema(),
                Some(WriterProperties::builder().build()),
            )
            .expect("writer");
            writer.write(batch).expect("write");
            writer.close().expect("close");
        }
        let temp = tempfile::tempdir().expect("duckdb temp");
        let db = temp.path().join("oracle.duckdb");
        let conn = Connection::open(&db).expect("open");
        for (name, _) in tables {
            let path = stage.path().join(format!("{name}.parquet"));
            conn.execute_batch(&format!(
                "CREATE TABLE {name} AS SELECT * FROM read_parquet('{}');",
                path.display()
            ))
            .unwrap_or_else(|e| panic!("load {name}: {e}"));
        }
        // `CREATE TABLE … AS SELECT` materialized every table into `db`, so the
        // staged parquet is no longer referenced and `stage` can drop here.
        (temp, conn)
    }

    fn standalone_query(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
        let batches: Vec<RecordBatch> = stmt.query_arrow([]).map_err(|e| e.to_string())?.collect();
        Ok(batches)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn spice_duckdb_accel_vs_standalone_duckdb_micro() {
        let fact = make_fact(2_048, 64);
        let dim = make_dim(256);

        let accel = DuckDBAccelerator::new();
        let t_table = accel
            .create_external_table(create_cmd("t", fact.schema()), None, vec![], None)
            .await
            .expect("create t");
        let d_table = accel
            .create_external_table(create_cmd("d", dim.schema()), None, vec![], None)
            .await
            .expect("create d");
        insert_batch(&t_table, fact.clone()).await;
        insert_batch(&d_table, dim.clone()).await;
        let spice_tables: Vec<(String, Arc<dyn TableProvider>)> =
            vec![("t".into(), t_table), ("d".into(), d_table)];

        let (_tmp, conn) = load_standalone_duckdb(&[("t", fact), ("d", dim)]);

        let mut fails = Vec::new();
        for q in micro_queries() {
            let spice = spice_query(&spice_tables, &q.sql)
                .await
                .unwrap_or_else(|e| panic!("spice sql {}: {e}", q.name));
            let standalone = standalone_query(&conn, &q.sql)
                .unwrap_or_else(|e| panic!("duckdb sql {}: {e}", q.name));
            let status = compare(&q, &spice, &standalone);
            eprintln!(
                "spice-duckdb-accel vs standalone-duckdb / {} -> {status}",
                q.name
            );
            if !status.starts_with("PASS") {
                fails.push(format!("{}: {status}", q.name));
            }
        }
        assert!(
            fails.is_empty(),
            "Spice DuckDB accelerator vs standalone DuckDB failures: {fails:#?}"
        );
    }
}
