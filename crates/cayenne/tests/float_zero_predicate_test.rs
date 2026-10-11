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

//! `DataFusion` compares floats with `-0.0` and `0.0` equal, while the Vortex
//! scan a Cayenne file is read through orders them by IEEE 754 total order.
//! Every comparison a scan pushes down must still keep exactly the rows
//! `DataFusion` keeps.

#![allow(clippy::expect_used)]

use crate::common;

use common::lookup_index::{
    TableSpec, file_mode_config, memory_mode_config, open_table, overwrite, query, rendered,
};

use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Float16Type, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnv;

type F16 = <Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native;

/// Enough rows to be written to files.
const ROWS: usize = 20_000;

fn values() -> Vec<Option<f64>> {
    [
        Some(-0.0),
        Some(0.0),
        Some(1.5),
        Some(-2.5),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ]
    .into_iter()
    .cycle()
    .take(ROWS)
    .collect()
}

#[expect(clippy::cast_possible_truncation, reason = "narrowed on purpose")]
fn float_array(data_type: &DataType, values: &[Option<f64>]) -> ArrayRef {
    match data_type {
        DataType::Float16 => Arc::new(
            values
                .iter()
                .map(|v| v.map(F16::from_f64))
                .collect::<PrimitiveArray<Float16Type>>(),
        ),
        DataType::Float32 => Arc::new(
            values
                .iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        _ => Arc::new(values.iter().copied().collect::<Float64Array>()),
    }
}

/// `K` holds the values, `J` the same values with each zero's sign flipped, and
/// `I` an integer zero or one, so a cast of it to a float is a `0.0`.
fn batch(data_type: &DataType) -> RecordBatch {
    let values = values();
    let flipped: Vec<Option<f64>> = values
        .iter()
        .map(|v| v.map(|v| if v == 0.0 { -v } else { v }))
        .collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("K", data_type.clone(), true),
        Field::new("J", data_type.clone(), true),
        Field::new("I", DataType::Int32, false),
    ]));
    let ids = Int64Array::from_iter_values(0..i64::try_from(ROWS).expect("fits"));
    let ints = Int32Array::from_iter_values((0..ROWS).map(|row| i32::from(row % 3 == 0)));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            float_array(data_type, &values),
            float_array(data_type, &flipped),
            Arc::new(ints),
        ],
    )
    .expect("batch")
}

fn lookups(data_type: &DataType) -> Vec<String> {
    let mut sql = Vec::new();
    for zero in ["0.0", "-0.0"] {
        let zero = format!("arrow_cast({zero}, '{data_type}')");
        for op in ["=", "<>", "<", "<=", ">", ">="] {
            sql.push(format!("\"K\" {op} {zero}"));
            sql.push(format!("{zero} {op} \"K\""));
            sql.push(format!("CAST(\"I\" AS DOUBLE) {op} CAST({zero} AS DOUBLE)"));
        }
        sql.push(format!("\"K\" IN ({zero}, arrow_cast(1.5, '{data_type}'))"));
        sql.push(format!(
            "\"K\" NOT IN ({zero}, arrow_cast(1.5, '{data_type}'))"
        ));
        sql.push(format!("CASE \"K\" WHEN {zero} THEN 1 ELSE 0 END = 1"));
    }
    for op in ["=", "<>", "<", "<=", ">", ">="] {
        sql.push(format!("\"K\" {op} \"J\""));
    }
    sql.into_iter()
        .map(|predicate| format!("SELECT \"AutoId\" FROM t WHERE {predicate} ORDER BY \"AutoId\""))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_comparisons_with_zero_keep_the_rows_datafusion_keeps() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let mut mismatches = Vec::new();
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let batch = batch(&data_type);
        let oracle =
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).expect("oracle"));
        for (mode, config) in [
            ("file", file_mode_config()),
            ("memory", memory_mode_config()),
        ] {
            let name = format!("zeros_{}_{mode}", data_type.to_string().to_lowercase());
            let table = open_table(
                &fixture,
                Arc::clone(&runtime_env),
                TableSpec::new(&name, batch.schema(), &[]).config(config),
            )
            .await;
            overwrite(&table, vec![batch.clone()]).await;
            for sql in lookups(&data_type) {
                let expected = rendered(&query(&oracle, "t", &sql).await);
                let got = rendered(&query(&table, "t", &sql).await);
                if got != expected {
                    mismatches.push(format!(
                        "{data_type}, {mode} mode: {sql}: {} rows, expected {}",
                        got.len(),
                        expected.len()
                    ));
                }
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} lookups disagree with DataFusion:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
