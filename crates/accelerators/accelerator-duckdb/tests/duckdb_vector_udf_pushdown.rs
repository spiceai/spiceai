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

//! Does a vector UDF that `DuckDB` is allowed to evaluate answer the same as the
//! local kernel?
//!
//! Carving a Spice UDF out of the `DuckDB` federation deny-list means one call
//! has two implementations, chosen by where the table happens to live. Nothing
//! else checks that they agree, and a disagreement is not an error — it is a
//! different number for the same query. Issue #13088 was exactly that:
//! `cosine_distance` was carved out because `DuckDB` has an
//! `array_cosine_distance`, which returns `1 - cosine_similarity` over `[0, 2]`
//! where the UDF returns `(1 - cosine_similarity) / 2` over `[0, 1]` — twice the
//! distance for every non-identical pair, undetected because the two halves were
//! only ever tested apart.
//!
//! So this drives both halves in one process: the local kernel over an Arrow
//! batch, and the SQL the `DuckDB` dialect renders for the same call executed
//! against a real `DuckDB` holding the same rows.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, Float32Array, RecordBatch};
use arrow::datatypes::Float64Type;
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::Column;
use datafusion::logical_expr::{Expr, ScalarUDF, expr::ScalarFunction};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use duckdb::Connection;
use runtime_datafusion::dialect::{duckdb_native_function_names, new_duckdb_dialect};

/// Vector width the battery below is written against.
const DIM: usize = 4;

/// One `(lhs, rhs)` pair and what makes it worth asking about.
struct Case {
    label: &'static str,
    lhs: [f32; DIM],
    rhs: [f32; DIM],
}

/// Every input class where two implementations of the same metric can disagree
/// without either of them looking wrong on its own.
fn battery() -> Vec<Case> {
    let nan = f32::NAN;
    let inf = f32::INFINITY;
    vec![
        Case { label: "identical", lhs: [1.0, 0.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "orthogonal", lhs: [1.0, 0.0, 0.0, 0.0], rhs: [0.0, 1.0, 0.0, 0.0] },
        Case { label: "opposite", lhs: [1.0, 0.0, 0.0, 0.0], rhs: [-1.0, 0.0, 0.0, 0.0] },
        Case { label: "oblique", lhs: [1.0, 1.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "all_nonzero", lhs: [0.25, -0.5, 0.75, 1.5], rhs: [-1.25, 0.5, 2.0, -0.75] },
        // Undefined direction: a metric that normalizes has to invent an answer,
        // and the two implementations need not invent the same one.
        Case { label: "zero_lhs", lhs: [0.0, 0.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "zero_both", lhs: [0.0; DIM], rhs: [0.0; DIM] },
        // No defined distance at all: the local contract is NULL, and a pushdown
        // has to reproduce that rather than score the row.
        Case { label: "nan_lhs", lhs: [nan, 0.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "nan_rhs", lhs: [1.0, 0.0, 0.0, 0.0], rhs: [nan, 0.0, 0.0, 0.0] },
        Case { label: "nan_both", lhs: [nan, 0.0, 0.0, 0.0], rhs: [nan, 0.0, 0.0, 0.0] },
        Case { label: "pos_inf_lhs", lhs: [inf, 0.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "neg_inf_lhs", lhs: [-inf, 0.0, 0.0, 0.0], rhs: [1.0, 0.0, 0.0, 0.0] },
        Case { label: "nan_deep", lhs: [1.0, 2.0, 3.0, nan], rhs: [1.0, 2.0, 3.0, 4.0] },
    ]
}

/// The Spice vector UDFs, each paired with the fact this test needs about it:
/// whether it is currently allowed to federate to `DuckDB`.
///
/// `unclassified_overrides_are_a_test_failure` below makes this list exhaustive,
/// so a vector UDF cannot be added to the dialect's override list without
/// arriving here and being measured.
fn vector_udfs() -> Vec<(&'static str, ScalarUDF)> {
    vec![
        (
            runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME,
            ScalarUDF::from(runtime_datafusion_udfs::cosine_distance::CosineDistance::new()),
        ),
        (
            runtime_datafusion_udfs::inner_product::INNER_PRODUCT_UDF_NAME,
            ScalarUDF::from(runtime_datafusion_udfs::inner_product::InnerProduct::new()),
        ),
        (
            runtime_datafusion_udfs::l2_distance::L2_DISTANCE_UDF_NAME,
            ScalarUDF::from(runtime_datafusion_udfs::l2_distance::L2Distance::new()),
        ),
        (
            runtime_datafusion_udfs::l2_distance::L2_SQUARED_DISTANCE_UDF_NAME,
            ScalarUDF::from(runtime_datafusion_udfs::l2_distance::L2SquaredDistance::new()),
        ),
    ]
}

/// Entries in the dialect's override list that are not Spice vector UDFs, and so
/// are outside what this test can measure.
const NON_VECTOR_OVERRIDES: &[&str] = &[
    // `DataFusion`'s own built-in, not a Spice UDF — it carries no Spice contract
    // for this test to compare against.
    "array_distance",
    "rand",
    "regexp_like",
    "regexp_match",
    "regexp_replace",
    "regexp_count",
];

fn fsl_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float32, true))
}

fn fsl_column(rows: &[[f32; DIM]]) -> ArrayRef {
    let flat: Vec<f32> = rows.iter().flat_map(|r| r.iter().copied()).collect();
    let values = Arc::new(Float32Array::from(flat));
    let len = i32::try_from(DIM).expect("DIM fits in i32");
    Arc::new(
        FixedSizeListArray::try_new(fsl_field(), len, values, None)
            .expect("equal-length rows form a FixedSizeListArray"),
    )
}

/// The local answer for every case, in battery order. `None` is a NULL row.
async fn local_values(udf: &ScalarUDF, cases: &[Case]) -> Vec<Option<f64>> {
    let lhs: Vec<[f32; DIM]> = cases.iter().map(|c| c.lhs).collect();
    let rhs: Vec<[f32; DIM]> = cases.iter().map(|c| c.rhs).collect();
    let len = i32::try_from(DIM).expect("DIM fits in i32");
    let list_type = DataType::FixedSizeList(fsl_field(), len);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", list_type.clone(), true),
        Field::new("b", list_type, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![fsl_column(&lhs), fsl_column(&rhs)])
        .expect("columns match the schema");

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).expect("batch is readable");
    let call = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(udf.clone()),
        vec![
            Expr::Column(Column::new_unqualified("a")),
            Expr::Column(Column::new_unqualified("b")),
        ],
    ));
    let batches = df
        .select(vec![call.alias("d")])
        .expect("projection builds")
        .collect()
        .await
        .unwrap_or_else(|e| panic!("local {} evaluation failed: {e}", udf.name()));

    let mut out = Vec::with_capacity(cases.len());
    for batch in &batches {
        let column = batch.column(0).as_primitive::<Float64Type>();
        for row in 0..column.len() {
            out.push((!column.is_null(row)).then(|| column.value(row)));
        }
    }
    out
}

fn duckdb_literal(v: &[f32; DIM]) -> String {
    let elements: Vec<String> = v
        .iter()
        .map(|x| {
            if x.is_nan() {
                "'nan'::FLOAT".to_string()
            } else if x.is_infinite() {
                let sign = if *x < 0.0 { "-" } else { "" };
                format!("'{sign}inf'::FLOAT")
            } else {
                format!("CAST({x:?} AS FLOAT)")
            }
        })
        .collect();
    format!("[{}]::FLOAT[{DIM}]", elements.join(", "))
}

/// The federated answer for every case: the SQL the dialect renders for this
/// call, run against a `DuckDB` holding the same rows.
fn duckdb_values(udf: &ScalarUDF, cases: &[Case]) -> Vec<Option<f64>> {
    let dialect = new_duckdb_dialect();
    let unparser = Unparser::new(dialect.as_ref());
    let call = Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(udf.clone()),
        vec![
            Expr::Column(Column::new_unqualified("a")),
            Expr::Column(Column::new_unqualified("b")),
        ],
    ));
    let rendered = unparser
        .expr_to_sql(&call)
        .unwrap_or_else(|e| panic!("{} has no DuckDB rendering: {e}", udf.name()))
        .to_string();

    let conn = Connection::open_in_memory().expect("in-memory DuckDB opens");
    conn.execute_batch(&format!(
        "CREATE TABLE t (ord INTEGER, a FLOAT[{DIM}], b FLOAT[{DIM}]);"
    ))
    .expect("table creates");
    for (ord, case) in cases.iter().enumerate() {
        conn.execute_batch(&format!(
            "INSERT INTO t VALUES ({ord}, {}, {});",
            duckdb_literal(&case.lhs),
            duckdb_literal(&case.rhs)
        ))
        .unwrap_or_else(|e| panic!("row {} ({}) inserts: {e}", ord, case.label));
    }

    let sql = format!("SELECT CAST({rendered} AS DOUBLE) FROM t ORDER BY ord");
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("DuckDB rejected the rendered SQL `{sql}`: {e}"));
    let mut rows = stmt.query([]).expect("rendered query runs");
    let mut out = Vec::with_capacity(cases.len());
    while let Some(row) = rows.next().expect("row reads") {
        out.push(row.get::<_, Option<f64>>(0).expect("column reads"));
    }
    out
}

fn describe(v: Option<f64>) -> String {
    match v {
        None => "NULL".to_string(),
        Some(x) => format!("{x}"),
    }
}

/// Every vector UDF the dialect lets `DuckDB` evaluate must answer exactly what
/// the local kernel answers, on every input class in the battery.
#[tokio::test(flavor = "multi_thread")]
async fn duckdb_vector_udf_pushdown_matches_local() {
    let native = duckdb_native_function_names();
    let cases = battery();
    let mut measured = 0_usize;

    for (name, udf) in vector_udfs() {
        if !native.contains(&name) {
            // Denied, so there is only one implementation and nothing to compare.
            continue;
        }
        measured += 1;
        let local = local_values(&udf, &cases).await;
        let remote = duckdb_values(&udf, &cases);
        assert_eq!(
            local.len(),
            cases.len(),
            "{name}: local produced {} values for {} cases",
            local.len(),
            cases.len()
        );
        assert_eq!(
            remote.len(),
            cases.len(),
            "{name}: DuckDB produced {} values for {} cases",
            remote.len(),
            cases.len()
        );

        for (case, (l, r)) in cases.iter().zip(local.iter().zip(remote.iter())) {
            let agrees = match (l, r) {
                (None, None) => true,
                (Some(l), Some(r)) => (l - r).abs() <= 1e-6 * l.abs().max(1.0),
                _ => false,
            };
            assert!(
                agrees,
                "{name} answers differently depending on where the table lives: case \
                 '{}' is {} locally and {} pushed down to DuckDB. Either the rendering in \
                 runtime-datafusion::dialect::duckdb is not equivalent, or {name} does not \
                 belong in duckdb_scalar_overrides — see #13088.",
                case.label,
                describe(*l),
                describe(*r)
            );
        }
    }

    assert!(
        measured > 0,
        "no vector UDF federates to DuckDB, so this test measured nothing — if that is \
         intended, delete it rather than leaving it passing vacuously"
    );
}

/// A new entry in the dialect's override list has to be classified here before
/// it can ship, so it cannot skip the equivalence measurement by omission.
#[test]
fn unclassified_overrides_are_a_test_failure() {
    let known: Vec<&str> = vector_udfs()
        .into_iter()
        .map(|(name, _)| name)
        .chain(NON_VECTOR_OVERRIDES.iter().copied())
        .collect();
    for name in duckdb_native_function_names() {
        assert!(
            known.contains(&name),
            "`{name}` was added to duckdb_scalar_overrides but is classified nowhere in \
             this test. Carving a function out of the DuckDB deny-list gives it two \
             implementations; add it to `vector_udfs()` so its rendering is measured against \
             the local kernel, or to `NON_VECTOR_OVERRIDES` if it carries no Spice contract."
        );
    }
}

/// The rendering `cosine_distance` used to get, kept as an executable record of
/// why it no longer federates (#13088).
#[test]
fn duckdb_array_cosine_distance_is_not_this_udfs_metric() {
    let conn = Connection::open_in_memory().expect("in-memory DuckDB opens");
    // Orthogonal unit vectors: this UDF's contract is (1 - 0) / 2 = 0.5.
    let sql = "SELECT CAST(array_cosine_distance([1.0, 0.0]::FLOAT[2], [0.0, 1.0]::FLOAT[2]) \
               AS DOUBLE)";
    let mut stmt = conn.prepare(sql).expect("prepare");
    let mut rows = stmt.query([]).expect("query");
    let duckdb_answer: f64 = rows
        .next()
        .expect("row")
        .expect("one row")
        .get(0)
        .expect("column");
    assert!(
        (duckdb_answer - 1.0).abs() < 1e-6,
        "DuckDB's array_cosine_distance is expected to answer 1.0 for orthogonal unit \
         vectors (it returns 1 - cosine_similarity over [0, 2]); got {duckdb_answer}. If \
         DuckDB has changed to the [0, 1] convention, revisit whether cosine_distance can \
         federate again — see #13088."
    );
}
