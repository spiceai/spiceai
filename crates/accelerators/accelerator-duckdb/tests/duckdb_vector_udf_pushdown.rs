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

#![expect(
    clippy::expect_used,
    reason = "integration-test setup: a failure here is a broken test, not a runtime path"
)]

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

use arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, Float32Builder, RecordBatch};
use arrow::buffer::NullBuffer;
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

/// A vector operand: `None` is a NULL row, and a `None` element is a NULL slot
/// inside an otherwise present vector. Both are legal in an Arrow
/// `FixedSizeList<Float32>` column and both make the local kernel answer NULL.
type Operand = Option<[Option<f32>; DIM]>;

/// One `(lhs, rhs)` pair and what makes it worth asking about.
struct Case {
    label: &'static str,
    lhs: Operand,
    rhs: Operand,
}

/// What one side answered for one case. An engine that raises instead of
/// answering is its own outcome: it is neither a value nor a NULL, and folding it
/// into either would hide that a pushdown turns a NULL row into a failed query.
#[derive(Debug, PartialEq, Clone)]
enum Outcome {
    Value(Option<f64>),
    Error(String),
}

/// Shorthand for a fully-populated operand: a present row whose every element is
/// present.
#[expect(
    clippy::unnecessary_wraps,
    reason = "the Some is the meaning — an Operand's outer layer is row presence"
)]
fn dense(v: [f32; DIM]) -> Operand {
    Some(v.map(Some))
}

/// Every input class where two implementations of the same metric can disagree
/// without either of them looking wrong on its own.
fn battery() -> Vec<Case> {
    let nan = f32::NAN;
    let inf = f32::INFINITY;
    let cases = vec![
        Case {
            label: "identical",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "orthogonal",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: dense([0.0, 1.0, 0.0, 0.0]),
        },
        Case {
            label: "opposite",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: dense([-1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "oblique",
            lhs: dense([1.0, 1.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "all_nonzero",
            lhs: dense([0.25, -0.5, 0.75, 1.5]),
            rhs: dense([-1.25, 0.5, 2.0, -0.75]),
        },
        // Undefined direction: a metric that normalizes has to invent an answer,
        // and the two implementations need not invent the same one.
        Case {
            label: "zero_lhs",
            lhs: dense([0.0, 0.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "zero_both",
            lhs: dense([0.0; DIM]),
            rhs: dense([0.0; DIM]),
        },
        // No defined distance at all: the local contract is NULL, and a pushdown
        // has to reproduce that rather than score the row.
        Case {
            label: "nan_lhs",
            lhs: dense([nan, 0.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "nan_rhs",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: dense([nan, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "nan_both",
            lhs: dense([nan, 0.0, 0.0, 0.0]),
            rhs: dense([nan, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "pos_inf_lhs",
            lhs: dense([inf, 0.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "neg_inf_lhs",
            lhs: dense([-inf, 0.0, 0.0, 0.0]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "nan_deep",
            lhs: dense([1.0, 2.0, 3.0, nan]),
            rhs: dense([1.0, 2.0, 3.0, 4.0]),
        },
        // NULL is not a variant of "no defined distance" — it is a separate way
        // for a row to have no answer, and an engine may raise on it rather than
        // return NULL. `compute_fsl_f32` treats a null outer row and a null inner
        // slot identically (both append NULL), so a pushdown has to as well.
        Case {
            label: "null_row_lhs",
            lhs: None,
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "null_row_rhs",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: None,
        },
        Case {
            label: "null_row_both",
            lhs: None,
            rhs: None,
        },
        Case {
            label: "null_element_lhs",
            lhs: Some([None, Some(0.0), Some(0.0), Some(0.0)]),
            rhs: dense([1.0, 0.0, 0.0, 0.0]),
        },
        Case {
            label: "null_element_rhs",
            lhs: dense([1.0, 0.0, 0.0, 0.0]),
            rhs: Some([Some(1.0), Some(0.0), Some(0.0), None]),
        },
    ];
    cases
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

fn fsl_column(rows: &[Operand]) -> ArrayRef {
    let mut values = Float32Builder::new();
    let mut row_valid = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            None => {
                // A null row still occupies DIM child slots in a FixedSizeList.
                for _ in 0..DIM {
                    values.append_null();
                }
                row_valid.push(false);
            }
            Some(elements) => {
                for element in elements {
                    match element {
                        Some(x) => values.append_value(*x),
                        None => values.append_null(),
                    }
                }
                row_valid.push(true);
            }
        }
    }
    let len = i32::try_from(DIM).expect("DIM fits in i32");
    Arc::new(
        FixedSizeListArray::try_new(
            fsl_field(),
            len,
            Arc::new(values.finish()),
            Some(NullBuffer::from(row_valid)),
        )
        .expect("equal-length rows form a FixedSizeListArray"),
    )
}

/// The local answer for every case, in battery order.
async fn local_outcomes(udf: &ScalarUDF, cases: &[Case]) -> Vec<Outcome> {
    let lhs: Vec<Operand> = cases.iter().map(|c| c.lhs).collect();
    let rhs: Vec<Operand> = cases.iter().map(|c| c.rhs).collect();
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
    let collected = df
        .select(vec![call.alias("d")])
        .expect("projection builds")
        .collect()
        .await;
    let batches = match collected {
        Ok(batches) => batches,
        // The local kernel raising is itself an answer worth comparing: it would
        // mean neither side can evaluate the row.
        Err(e) => return vec![Outcome::Error(e.to_string()); cases.len()],
    };

    let mut out = Vec::with_capacity(cases.len());
    for batch in &batches {
        let column = batch.column(0).as_primitive::<Float64Type>();
        for row in 0..column.len() {
            out.push(Outcome::Value(
                (!column.is_null(row)).then(|| column.value(row)),
            ));
        }
    }
    out
}

fn duckdb_literal(operand: &Operand) -> String {
    let Some(elements) = operand else {
        return format!("NULL::FLOAT[{DIM}]");
    };
    let rendered: Vec<String> = elements
        .iter()
        .map(|element| match element {
            None => "NULL".to_string(),
            Some(x) if x.is_nan() => "'nan'::FLOAT".to_string(),
            Some(x) if x.is_infinite() => {
                let sign = if *x < 0.0 { "-" } else { "" };
                format!("'{sign}inf'::FLOAT")
            }
            Some(x) => format!("CAST({x:?} AS FLOAT)"),
        })
        .collect();
    format!("[{}]::FLOAT[{DIM}]", rendered.join(", "))
}

/// The federated answer for every case: the SQL the dialect renders for this
/// call, run against a `DuckDB` holding the same rows.
///
/// One statement per case, deliberately. `DuckDB` raising on a row aborts the
/// whole statement, so a single query would attribute one bad row's failure to
/// every case in the battery — and the point of the battery is to say *which*
/// input class diverges.
fn duckdb_outcomes(udf: &ScalarUDF, cases: &[Case]) -> Vec<Outcome> {
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
    cases
        .iter()
        .map(|case| {
            let sql = format!(
                "SELECT CAST({rendered} AS DOUBLE) FROM (SELECT {} AS a, {} AS b) t",
                duckdb_literal(&case.lhs),
                duckdb_literal(&case.rhs)
            );
            match conn
                .prepare(&sql)
                .and_then(|mut stmt| stmt.query([])?.next()?.map_or(Ok(None), |r| r.get(0)))
            {
                Ok(v) => Outcome::Value(v),
                Err(e) => Outcome::Error(e.to_string()),
            }
        })
        .collect()
}

fn describe(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Value(None) => "NULL".to_string(),
        Outcome::Value(Some(x)) => format!("{x}"),
        Outcome::Error(e) => format!("an error ({})", e.trim()),
    }
}

fn agrees(local: &Outcome, remote: &Outcome) -> bool {
    match (local, remote) {
        (Outcome::Value(None), Outcome::Value(None)) => true,
        (Outcome::Value(Some(l)), Outcome::Value(Some(r))) => {
            (l - r).abs() <= 1e-6 * l.abs().max(1.0)
        }
        _ => false,
    }
}

/// Every vector UDF the dialect lets `DuckDB` evaluate must answer exactly what
/// the local kernel answers, on every input class in the battery.
#[tokio::test(flavor = "multi_thread")]
async fn duckdb_vector_udf_pushdown_matches_local() {
    let native = duckdb_native_function_names();
    let cases = battery();
    // Only a UDF the dialect advertises has two implementations to compare; the
    // rest are evaluated locally and have nothing to disagree with.
    //
    // No Spice vector UDF is advertised today, so this is empty and the loop
    // below compares nothing. That is deliberate, not dead weight: it is the gate
    // that fires the moment one is added back. Re-adding either `cosine_distance`
    // or `inner_product` to `duckdb_scalar_overrides` makes this test fail,
    // naming the input class that diverges — which is how the neuter matrix for
    // #13088 showed it is armed rather than idle. The two tests below carry the
    // measured evidence for each denial, so this file does not rest on the loop
    // alone.
    let federating: Vec<(&str, ScalarUDF)> = vector_udfs()
        .into_iter()
        .filter(|(name, _)| native.contains(name))
        .collect();

    for (name, udf) in federating {
        let local = local_outcomes(&udf, &cases).await;
        let remote = duckdb_outcomes(&udf, &cases);
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
            assert!(
                agrees(l, r),
                "{name} answers differently depending on where the table lives: case \
                 '{}' is {} locally and {} pushed down to DuckDB. Either the rendering in \
                 runtime-datafusion::dialect::duckdb is not equivalent, or {name} does not \
                 belong in duckdb_scalar_overrides — see #13088.",
                case.label,
                describe(l),
                describe(r)
            );
        }
    }
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

/// `array_inner_product` raises on a NULL array element where the UDF answers
/// NULL, which is why `inner_product` no longer federates either (#13088).
///
/// This is the finding that ruled out repairing the pushdown in the rendering: an
/// expression wrapping the call cannot screen an exception raised while
/// evaluating its own argument, so there is no `nullif` or `CASE` that recovers
/// the UDF's answer here.
#[test]
fn duckdb_array_inner_product_rejects_a_null_element() {
    let conn = Connection::open_in_memory().expect("in-memory DuckDB opens");
    let sql = "SELECT array_inner_product([NULL, 1.0]::FLOAT[2], [1.0, 1.0]::FLOAT[2])";
    let outcome = conn.prepare(sql).and_then(|mut stmt| {
        stmt.query([])?
            .next()?
            .map_or(Ok(None), |r| r.get::<_, Option<f64>>(0))
    });
    match outcome {
        Err(e) => assert!(
            e.to_string().contains("can not contain NULL"),
            "expected DuckDB to reject a NULL array element; got a different error: {e}"
        ),
        Ok(v) => panic!(
            "DuckDB answered {v:?} for an array with a NULL element instead of raising. If it \
             now returns NULL, revisit whether inner_product can federate again — see #13088."
        ),
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
