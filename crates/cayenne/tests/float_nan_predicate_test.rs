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

//! `DataFusion` orders floats by IEEE 754 total order, so `NaN = NaN` holds, a
//! positive `NaN` sorts above `+inf` and a negative one below `-inf`. A scan
//! must keep exactly the rows `DataFusion` keeps for every comparison against
//! such a value, whatever its files' and segments' min/max statistics say.
//!
//! Regression test for #14719.

#![allow(clippy::expect_used)]

mod common;

use common::lookup_index::{
    TableSpec, explain_total, file_mode_config, memory_mode_config, open_table, overwrite, query,
    rendered,
};

use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array, PrimitiveArray};
use arrow::datatypes::{DataType, Field, Float16Type, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnv;

type F16 = <Float16Type as arrow::datatypes::ArrowPrimitiveType>::Native;

/// Enough rows to be written to files.
const ROWS: usize = 20_000;

/// The column's values in each layout a scan can see a `NaN` in.
fn layouts() -> Vec<(&'static str, Vec<Option<f64>>)> {
    // One `NaN` among finite values, as the issue reports it.
    let mut one_nan: Vec<Option<f64>> = (0..ROWS)
        .map(|row| (row % 97 != 0).then(|| f64::from(u32::try_from(row).expect("fits")) * 0.25))
        .collect();
    one_nan[ROWS / 2] = Some(f64::NAN);

    // Every kind of value, a negative `NaN` included.
    let mixed = [
        Some(1.5),
        Some(f64::NAN),
        Some(-2.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-f64::NAN),
        None,
    ]
    .into_iter()
    .cycle()
    .take(ROWS)
    .collect();

    // Nothing but `NaN` and NULL.
    let only_nan = [Some(f64::NAN), None, Some(f64::NAN)]
        .into_iter()
        .cycle()
        .take(ROWS)
        .collect();

    vec![
        ("one_nan", one_nan),
        ("mixed", mixed),
        ("only_nan", only_nan),
    ]
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

fn batch(data_type: &DataType, values: &[Option<f64>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("AutoId", DataType::Int64, false),
        Field::new("K", data_type.clone(), true),
    ]));
    let ids = Int64Array::from_iter_values(0..i64::try_from(values.len()).expect("fits"));
    RecordBatch::try_new(schema, vec![Arc::new(ids), float_array(data_type, values)])
        .expect("batch")
}

fn lookups(data_type: &DataType) -> Vec<String> {
    let typed = |value: &str| format!("arrow_cast({value}, '{data_type}')");
    let nan = typed("CAST('NaN' AS DOUBLE)");
    let negative_nan = typed("-CAST('NaN' AS DOUBLE)");
    // Above and below every finite value in the layouts, and within `Float16`.
    let above = typed("60000.0");
    let below = typed("-60000.0");
    let mut sql = Vec::new();
    for probe in [&nan, &negative_nan] {
        for op in ["=", "<>", "<", "<=", ">", ">="] {
            sql.push(format!("\"K\" {op} {probe}"));
            sql.push(format!("{probe} {op} \"K\""));
        }
        sql.push(format!("\"K\" IN ({probe}, {})", typed("1.5")));
        sql.push(format!("\"K\" NOT IN ({probe}, {})", typed("1.5")));
    }
    for bound in [&above, &below] {
        for op in ["<", "<=", ">", ">="] {
            sql.push(format!("\"K\" {op} {bound}"));
        }
    }
    sql.push(format!("\"K\" BETWEEN {above} AND {nan}"));
    sql.push("isnan(\"K\")".to_string());
    sql.push("NOT isnan(\"K\")".to_string());
    let mut sql: Vec<String> = sql
        .into_iter()
        .map(|predicate| format!("SELECT \"AutoId\" FROM t WHERE {predicate} ORDER BY \"AutoId\""))
        .collect();
    // Bounds that leave a NaN out must not answer an aggregate either.
    sql.push("SELECT min(\"K\"), max(\"K\"), count(\"K\") FROM t".to_string());
    sql.push("SELECT sum(\"K\") FROM t".to_string());
    sql
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_comparisons_with_nan_keep_the_rows_datafusion_keeps() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let mut mismatches = Vec::new();
    let mut nan_rows_compared = 0usize;
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        for (layout, values) in layouts() {
            let batch = batch(&data_type, &values);
            let oracle = Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).expect("oracle"),
            );
            for mode in ["file", "memory"] {
                let name = format!(
                    "nan_{layout}_{}_{mode}",
                    data_type.to_string().to_lowercase()
                );
                let spec = || {
                    TableSpec::new(&name, batch.schema(), &[]).config(if mode == "file" {
                        file_mode_config()
                    } else {
                        memory_mode_config()
                    })
                };
                let table = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;
                overwrite(&table, vec![batch.clone()]).await;
                // A file-mode table is read again after a reopen, so the bounds
                // come back from the persisted statistics rather than the write.
                let mut tables = vec![("written", table)];
                if mode == "file" {
                    tables.push((
                        "reopened",
                        open_table(&fixture, Arc::clone(&runtime_env), spec()).await,
                    ));
                }
                for (state, table) in &tables {
                    for sql in lookups(&data_type) {
                        let expected = rendered(&query(&oracle, "t", &sql).await);
                        if sql.contains("\"K\" = arrow_cast(CAST('NaN'") {
                            nan_rows_compared += expected.len();
                        }
                        let got = rendered(&query(table, "t", &sql).await);
                        if got != expected {
                            mismatches.push(format!(
                                "{data_type}, {layout}, {mode} mode, {state}: {sql}: {} rows, expected {}{}",
                                got.len(),
                                expected.len(),
                                if expected.len() == 1 {
                                    format!(" ({got:?}, expected {expected:?})")
                                } else {
                                    String::new()
                                }
                            ));
                        }
                    }
                }
            }
        }
    }
    // The oracle must have found NaN rows to compare, or every lookup above
    // agreed on an empty answer and compared nothing.
    assert!(
        nan_rows_compared > 0,
        "the oracle returned no row for `K = NaN` in any layout"
    );
    assert!(
        mismatches.is_empty(),
        "{} lookups disagree with DataFusion:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

/// Accounting for NaN must not cost a column that holds none its pruning: its
/// files are still skipped for a probe beyond its bounds, both as written and
/// after a reopen restores the bounds from the persisted statistics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn float_files_without_nan_are_still_pruned() {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let values: Vec<Option<f64>> = (0..ROWS)
        .map(|row| Some(f64::from(u32::try_from(row).expect("fits")) * 0.25))
        .collect();
    let batch = batch(&DataType::Float64, &values);
    let spec = || TableSpec::new("no_nan_float64", batch.schema(), &[]);
    let table = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;
    overwrite(&table, vec![batch.clone()]).await;
    let reopened = open_table(&fixture, Arc::clone(&runtime_env), spec()).await;

    let files_scanned = |plan: &[RecordBatch]| {
        let plan = arrow::util::pretty::pretty_format_batches(plan)
            .expect("plan renders")
            .to_string();
        assert!(
            plan.contains(" files_scanned="),
            "the plan reports no Cayenne file scan:\n{plan}"
        );
        explain_total(&plan, "files_scanned")
    };
    for (state, table) in [("written", &table), ("reopened", &reopened)] {
        let pruned = query(
            table,
            "t",
            "EXPLAIN ANALYZE SELECT \"AutoId\" FROM t WHERE \"K\" > 60000.0",
        )
        .await;
        assert_eq!(
            files_scanned(&pruned),
            0,
            "{state}: `K > 60000` read a file"
        );
        let kept = query(
            table,
            "t",
            "EXPLAIN ANALYZE SELECT \"AutoId\" FROM t WHERE \"K\" < 60000.0",
        )
        .await;
        assert!(
            files_scanned(&kept) > 0,
            "{state}: `K < 60000` read no file"
        );
        assert_eq!(
            rendered(&query(table, "t", "SELECT max(\"K\") FROM t").await),
            vec!["4999.75".to_string()],
            "{state}"
        );
    }
}
