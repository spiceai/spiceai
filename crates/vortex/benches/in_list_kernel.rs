// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Kernel-level benchmark for `IN (<list>)` membership evaluation.
//!
//! A large `IN` list in ordinary SQL reaches Vortex's `list_contains` kernel
//! uncapped: `convert()` in `crates/vortex/src/convert/exprs.rs` maps
//! `DataFusion`'s `InListExpr` to `list_contains(lit(list), col)`, and the scan
//! evaluates it once per batch. What that costs as the list grows is what this
//! measures, isolated from any file or scan.
//!
//! The `_negated` arms are `NOT IN`, which `convert()` renders as
//! `not(list_contains(..))`. Its selectivity is inverted — nearly every row
//! survives — so it exercises the kernel without the output collapsing to a
//! handful of rows.
//!
//! `arrow_hashed_in_list` is `DataFusion`'s own hashed `InListExpr` over the
//! same batch: one pass, independent of the list length. It is the floor the
//! Vortex path has to reach, and the sweep over M is what shows whether the two
//! are the same shape.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::expressions::{Literal, col};
use datafusion_physical_expr::{PhysicalExpr, expressions::InListExpr};
use vortex::VortexSessionDefault;
use vortex::array::arrays::bool::BoolArrayExt;
use vortex::array::arrays::{BoolArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::array::{IntoArray, VortexSessionExecute};
use vortex::buffer::Buffer;
use vortex::dtype::{DType, Nullability, PType};
use vortex::expr::{list_contains, lit, not, root};
use vortex::scalar::Scalar;
use vortex::session::VortexSession;

/// One `DataFusion` batch. The kernel cost per batch is what the scan pays for
/// every batch it decodes, so the batch size — not the file size — is the
/// relevant N.
const BATCH_ROWS: usize = 8192;

/// Build-side list lengths. A few thousand brackets the size an analytical
/// join's build side reaches in practice; the ends of the sweep are there to show the
/// shape of the curve, not a realistic join.
const LIST_LENS: [usize; 10] = [1, 2, 3, 4, 8, 32, 150, 512, 2048, 8192];

/// Needle values: `0..BATCH_ROWS`, the probe-side key column.
fn needles() -> Vec<i64> {
    (0..BATCH_ROWS as i64).collect()
}

/// `list_len` values spread evenly across the needle range, so every value is
/// present in the batch and no zone-level or statistics-level shortcut applies.
/// Selectivity therefore rises with the list length for both engines alike.
fn list_values(list_len: usize) -> Vec<i64> {
    let step = (BATCH_ROWS / list_len).max(1) as i64;
    (0..list_len as i64)
        .map(|i| (i * step) % BATCH_ROWS as i64)
        .collect()
}

fn vortex_batch(needles: &[i64]) -> vortex::array::ArrayRef {
    PrimitiveArray::new(Buffer::copy_from(needles), Validity::NonNullable).into_array()
}

fn arrow_batch(needles: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(needles.to_vec()))])
        .expect("record batch")
}

fn bench_in_list_kernel(c: &mut Criterion) {
    let session = VortexSession::default();
    let needles = needles();
    let vortex_input = vortex_batch(&needles);
    let arrow_input = arrow_batch(&needles);
    let arrow_schema = arrow_input.schema();

    let mut group = c.benchmark_group("in_list_kernel");
    for list_len in LIST_LENS {
        let values = list_values(list_len);

        // Vortex: list_contains(lit(<list scalar>), root())
        let list_scalar = Scalar::list(
            Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
            values.iter().map(|v| Scalar::from(*v)).collect(),
            Nullability::Nullable,
        );
        let vortex_expr = list_contains(lit(list_scalar), root());

        // DataFusion: hashed InListExpr over the same values.
        let arrow_expr: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                col("id", &arrow_schema).expect("id column"),
                values
                    .iter()
                    .map(|v| {
                        Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                            *v,
                        )))) as Arc<dyn PhysicalExpr>
                    })
                    .collect(),
                false,
                &arrow_schema,
            )
            .expect("in list expr"),
        );

        let vortex_negated_expr = not(list_contains(
            lit(Scalar::list(
                Arc::new(DType::Primitive(PType::I64, Nullability::NonNullable)),
                values.iter().map(|v| Scalar::from(*v)).collect(),
                Nullability::Nullable,
            )),
            root(),
        ));

        // Both sides must agree on the surviving row count before either is timed.
        let mut ctx = session.create_execution_ctx();
        let vortex_hits = vortex_input
            .clone()
            .apply(&vortex_expr)
            .expect("apply list_contains")
            .execute::<BoolArray>(&mut ctx)
            .expect("execute list_contains")
            .bit_buffer_view()
            .true_count();
        let arrow_hits = match arrow_expr.evaluate(&arrow_input).expect("evaluate in list") {
            ColumnarValue::Array(array) => {
                datafusion::arrow::array::BooleanArray::from(array.to_data()).true_count()
            }
            ColumnarValue::Scalar(_) => panic!("in list should evaluate to an array"),
        };
        assert_eq!(
            vortex_hits, arrow_hits,
            "list_contains and InListExpr must select the same rows for M={list_len}"
        );
        assert_eq!(
            vortex_hits, list_len,
            "every list value should be present exactly once for M={list_len}"
        );

        // The negated form must be the exact complement: it is the shape the
        // Cayenne tombstone filter pushes, and a kernel that answered it
        // differently would drop live rows rather than merely run slowly.
        let mut ctx = session.create_execution_ctx();
        let negated_hits = vortex_input
            .clone()
            .apply(&vortex_negated_expr)
            .expect("apply negated list_contains")
            .execute::<BoolArray>(&mut ctx)
            .expect("execute negated list_contains")
            .bit_buffer_view()
            .true_count();
        assert_eq!(
            negated_hits,
            BATCH_ROWS - vortex_hits,
            "NOT IN must keep exactly the rows IN drops for M={list_len}"
        );

        group.bench_with_input(
            BenchmarkId::new("vortex_list_contains", list_len),
            &list_len,
            |b, _| {
                b.iter(|| {
                    let mut ctx = session.create_execution_ctx();
                    vortex_input
                        .clone()
                        .apply(&vortex_expr)
                        .expect("apply")
                        .execute::<BoolArray>(&mut ctx)
                        .expect("execute")
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("vortex_list_contains_negated", list_len),
            &list_len,
            |b, _| {
                b.iter(|| {
                    let mut ctx = session.create_execution_ctx();
                    vortex_input
                        .clone()
                        .apply(&vortex_negated_expr)
                        .expect("apply")
                        .execute::<BoolArray>(&mut ctx)
                        .expect("execute")
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("arrow_hashed_in_list", list_len),
            &list_len,
            |b, _| {
                b.iter(|| arrow_expr.evaluate(&arrow_input).expect("evaluate"));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_in_list_kernel);
criterion_main!(benches);
