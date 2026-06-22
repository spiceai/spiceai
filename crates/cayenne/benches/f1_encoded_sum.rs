//! F1 Inc-0 validation: does `vortex::aggregate_fn::sum` on an ENCODED Vortex
//! array beat the current scan path (decode to Arrow, then sum)?
//!
//! The scan today does `execute_arrow(chunk)` (full Arrow decode) + a separate
//! DataFusion `AggregateExec`. F1 would call `aggregate_fn::sum` on the encoded
//! chunk: it short-circuits on `array.statistics().get(Sum)` (O(1) when the
//! encoding knows its sum structurally) else a native accumulator.
//!
//! Cases (fresh arrays per rep so the cached Sum stat never carries over):
//!   constant         — structural fast-path (upper bound)
//!   plain_lowcard    — uncompressed, no cached stat (native accumulator)
//!   btrblocks_lowcard— REAL chunk encoding for a low-cardinality column
//!   btrblocks_random — REAL chunk encoding for a high-cardinality column
//! The btrblocks_* cases are the realistic ones (Vortex files store btrblocks).
//!
//! Run: cargo bench -p cayenne --bench f1_encoded_sum

use std::time::Instant;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::compute::sum as arrow_sum;
use vortex::VortexSessionDefault;
use vortex::aggregate_fn::fns::sum::sum as vx_sum;
use vortex::array::ArrayRef;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::{ConstantArray, PrimitiveArray};
use vortex::array::arrow::IntoArrowArray;
use vortex::session::VortexSession;
use vortex_btrblocks::BtrBlocksCompressor;

const N: usize = 1_000_000;
const REPS: usize = 30;

fn run(label: &str, agg_arrays: Vec<ArrayRef>, dec_arrays: Vec<ArrayRef>, session: &VortexSession) {
    let mut ctx = session.create_execution_ctx();
    let t0 = Instant::now();
    for a in &agg_arrays {
        let s = vx_sum(a, &mut ctx).expect("sum");
        std::hint::black_box(&s);
    }
    let agg_us = t0.elapsed().as_micros() as f64 / agg_arrays.len() as f64;

    let t1 = Instant::now();
    for a in &dec_arrays {
        let arrow = a.clone().into_arrow_preferred().expect("to arrow");
        let i64a = arrow
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        std::hint::black_box(arrow_sum(i64a));
    }
    let dec_us = t1.elapsed().as_micros() as f64 / dec_arrays.len() as f64;

    println!(
        "  {label:18}  agg_fn={agg_us:9.1} us   decode+sum={dec_us:9.1} us   speedup={:.2}x",
        dec_us / agg_us
    );
}

fn main() {
    let session = VortexSession::default();
    let constant = || ConstantArray::new(7i64, N).into_array();
    let plain_low = || PrimitiveArray::from_iter((0..N as i64).map(|i| i % 1000)).into_array();
    let btr = |seed_mod: i64| -> ArrayRef {
        let mut ctx = session.create_execution_ctx();
        let plain = PrimitiveArray::from_iter((0..N as i64).map(|i| {
            if seed_mod == 0 {
                // high-cardinality / "random" via a cheap LCG hash
                (i.wrapping_mul(2_654_435_761)) % 1_000_003
            } else {
                i % seed_mod
            }
        }))
        .into_array();
        BtrBlocksCompressor::default()
            .compress(&plain, &mut ctx)
            .expect("compress")
    };

    println!("F1 Inc-0: aggregate_fn::sum (encoded) vs decode->arrow->sum  (n={N}, reps={REPS})");
    run(
        "constant",
        (0..REPS).map(|_| constant()).collect(),
        (0..REPS).map(|_| constant()).collect(),
        &session,
    );
    run(
        "plain_lowcard",
        (0..REPS).map(|_| plain_low()).collect(),
        (0..REPS).map(|_| plain_low()).collect(),
        &session,
    );
    run(
        "btrblocks_lowcard",
        (0..REPS).map(|_| btr(64)).collect(),
        (0..REPS).map(|_| btr(64)).collect(),
        &session,
    );
    run(
        "btrblocks_random",
        (0..REPS).map(|_| btr(0)).collect(),
        (0..REPS).map(|_| btr(0)).collect(),
        &session,
    );
}
