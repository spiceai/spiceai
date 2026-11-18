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

use arrow::{
    array::{Array, Decimal128Array, Int32Array, StringArray},
    datatypes::DataType,
};
use duckdb_oracle::Connection;

#[test]
fn installed_driver_preserves_types_nulls_and_settings() {
    let conn = Connection::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
    conn.execute_batch("SET integer_division = true; CREATE TABLE t (id INTEGER, amount DECIMAL(18,3), name VARCHAR); INSERT INTO t VALUES (1, 123.456, 'hello'), (2, NULL, NULL);").expect("setup");
    let batches = conn.query("SELECT * FROM t ORDER BY id").expect("query");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(1).data_type(), &DataType::Decimal128(18, 3));
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("integer");
    assert_eq!(ids.values().as_ref(), &[1, 2]);
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal");
    assert_eq!(amounts.value(0), 123_456);
    assert!(amounts.is_null(1));
    let names = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("varchar");
    assert_eq!(names.value(0), "hello");
    assert!(names.is_null(1));
    let batches = conn.query("SELECT 5 / 2 AS n").expect("integer division");
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("integer division type");
    assert_eq!(values.value(0), 2);
    assert_eq!(
        conn.query("SELECT * FROM t WHERE FALSE")
            .expect("empty query")
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    let error = conn
        .query("SELECT missing FROM t")
        .expect_err("invalid SQL must fail");
    assert!(error.to_string().contains("missing"), "{error}");
}

#[test]
fn database_file_reopens_and_attach_uses_the_same_connection() {
    let path = std::env::temp_dir().join(format!(
        "duckdb-oracle-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    {
        let conn = Connection::open(&path).unwrap_or_else(|error| panic!("{error}"));
        conn.execute_batch("CREATE TABLE t AS SELECT 42::INTEGER AS n; ATTACH ':memory:' AS other; CREATE TABLE other.u AS SELECT 7::INTEGER AS n;").expect("file and attach setup");
        let batches = conn
            .query("SELECT t.n + u.n FROM t CROSS JOIN other.u")
            .expect("attached query");
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int")
                .value(0),
            49
        );
    }
    let conn = Connection::open(&path).unwrap_or_else(|error| panic!("{error}"));
    let batches = conn.query("SELECT n FROM t").expect("reopened file");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int")
            .value(0),
        42
    );
    drop(conn);
    std::fs::remove_file(path).expect("remove fixture");
}
