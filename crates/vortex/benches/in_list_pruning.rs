// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Benchmark for the once-per-file statistics-pruning cost of an `IN (<list>)`
//! filter, isolated from any scan.
//!
//! Before a Vortex file (or any of its zones) is read, the pushed-down filter is
//! turned into a statistics predicate — `Expression::falsify` — and that
//! predicate is optimized before it is evaluated. None of this depends on how
//! many rows the file holds, so whatever it costs is paid per file (and again
//! per zone map) even when the file emits one row, which is why it is measured
//! apart from the kernel.
//!
//! The shape of the predicate is what decides that cost. A form with one
//! top-level conjunct per list element hands `Expression::try_optimize_recursive`
//! an `AND` of M terms, and its closing `find_between` pass compares every pair
//! of them; a form that stays a single top-level `OR` never enters that search.
//!
//! The three arms separate the translation from the pruning work:
//!
//! - `convert` is the `DataFusion` `InListExpr` -> Vortex `Expression` translation.
//! - `falsify` builds the statistics predicate from the converted filter.
//! - `falsify_then_optimize` adds the optimize pass the readers run on it.
//!
//! None of this depends on how many rows the file holds, so whatever it costs is
//! paid per file (and again per zone map) even when the file emits one row.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{InListExpr, Literal, col};
use vortex::VortexSessionDefault;
use vortex::dtype::{DType, Nullability, PType, StructFields};
use vortex::session::VortexSession;
use vortex::expr::not;
use vortex_datafusion::{DefaultExpressionConvertor, ExpressionConvertor};

/// Build-side list lengths. A few thousand brackets the size an analytical
/// join's build side reaches in practice; the sweep is what shows whether the cost is
/// linear in M or worse.
const LIST_LENS: [usize; 7] = [64, 150, 256, 512, 1024, 2048, 8192];

fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("skey", DataType::Int64, false)]))
}

/// The dtype of a row of the file the filter is pushed into — the scope
/// `falsify` rewrites the expression against.
fn scope() -> DType {
    DType::Struct(
        StructFields::new(
            ["skey"].into(),
            vec![DType::Primitive(PType::I64, Nullability::NonNullable)],
        ),
        Nullability::NonNullable,
    )
}

fn in_list_expr(schema: &Arc<Schema>, list_len: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(
        InListExpr::try_new(
            col("skey", schema).expect("skey column"),
            (0..list_len as i64)
                .map(|v| {
                    Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(v))))
                        as Arc<dyn PhysicalExpr>
                })
                .collect(),
            false,
            schema,
        )
        .expect("in list expr"),
    )
}

fn bench_in_list_pruning(c: &mut Criterion) {
    let session = VortexSession::default();
    let convertor = DefaultExpressionConvertor::default();
    let schema = arrow_schema();
    let scope = scope();

    let mut group = c.benchmark_group("in_list_pruning");
    for list_len in LIST_LENS {
        let df_expr = in_list_expr(&schema, list_len);
        let vortex_expr = convertor.convert(df_expr.as_ref()).expect("convert");
        let falsified = vortex_expr
            .falsify(&scope, &session)
            .expect("falsify")
            .expect("list_contains has a falsifier");

        // Whether the negated form — the shape Cayenne's tombstone exclusion
        // filter pushes — carries any of this cost at all. `ListContains`
        // registers only `falsify` rules and no `satisfy`, and falsifying a
        // `not(x)` requires satisfying `x`, so there should be no statistics
        // predicate to build or optimize here. Reported rather than assumed,
        // because it decides whether the falsifier work applies to the live
        // path or only to a positive `IN`.
        let negated_falsifier = not(vortex_expr.clone())
            .falsify(&scope, &session)
            .expect("falsify negated");
        eprintln!(
            "[M={list_len}] negated (NOT IN) falsifier: {}",
            match &negated_falsifier {
                Some(expr) => format!("Some({expr})"),
                None => "None (no zone pruning, and no predicate to optimize)".to_string(),
            }
        );

        group.bench_with_input(BenchmarkId::new("convert", list_len), &list_len, |b, _| {
            b.iter(|| convertor.convert(df_expr.as_ref()).expect("convert"));
        });
        group.bench_with_input(BenchmarkId::new("falsify", list_len), &list_len, |b, _| {
            b.iter(|| vortex_expr.falsify(&scope, &session).expect("falsify"));
        });
        group.bench_with_input(
            BenchmarkId::new("optimize_falsified", list_len),
            &list_len,
            |b, _| {
                b.iter(|| {
                    falsified
                        .optimize_recursive(&scope)
                        .expect("optimize falsified predicate")
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_in_list_pruning);
criterion_main!(benches);
