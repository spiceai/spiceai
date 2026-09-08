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

#![allow(clippy::expect_used)]

//! Round-trip regression harness for the `chbench_q10` `c_city` divergence that
//! led to the #11826 revert (#11910).
//!
//! Writes a multi-chunk, CH-benCH-`customer`-shaped dataset through the exact
//! light delta-encoding scheme sets from
//! `cayenne::provider::delta_encoding::strategy_builder_for_level` (mirrored
//! here because that fn is `pub(crate)`), reads the file back through the
//! default session (the production read path registers no write strategy), and
//! compares cell-for-cell against the input.
//!
//! Column design targets the failure classes a dictionary/sparse/constant
//! misdecode could produce:
//! - dict-cardinality boundaries: 255/256/257 and 65535/65536/65537 distinct
//! - near-unique strings (the `c_city` shape; dict must be skipped)
//! - long out-of-line strings (`VarBinView` buffer path, the `c_data` shape)
//! - null-dominated sparse columns (patches path)
//! - constant columns
//! - repetitive ints (the `c_id` shape) and floats (dictionary candidates)
//! - sliced input batches (the coalesced-CDC apply shape)
//! - one jumbo multi-block batch (the large mem-tier checkpoint shape)
//!
//! Default sizing is CI-friendly (~74k rows; Vortex compresses per fixed 8K-row
//! block, so per-block coverage is identical to larger runs). Set
//! `LIGHT_ROUNDTRIP_LARGE=1` for the full 417k-row audit dataset.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef as ArrowArrayRef, AsArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use futures::StreamExt;
use vortex::VortexSessionDefault;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::{ArrayRef, VortexSessionExecute};
use vortex::arrow::FromArrowType;
use vortex::arrow::{ArrowSessionExt, FromArrowArray};
use vortex::buffer::ByteBufferMut;
use vortex::dtype::DType;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_btrblocks::schemes::{float, integer, string};
use vortex_btrblocks::{BtrBlocksCompressorBuilder, Scheme, SchemeExt};
use vortex_session::VortexSession;

// ---------------------------------------------------------------------------
// Scheme sets — MUST mirror crates/cayenne/src/provider/delta_encoding.rs
// ---------------------------------------------------------------------------

fn builder_with_schemes(schemes: &[&'static dyn Scheme]) -> BtrBlocksCompressorBuilder {
    schemes
        .iter()
        .fold(BtrBlocksCompressorBuilder::empty(), |builder, &scheme| {
            builder.with_new_scheme(scheme)
        })
}

/// Mirror of `delta_encoding::strategy_builder_for_level` (`pub(crate)` there).
fn strategy_builder_for_level(level: u8) -> Option<WriteStrategyBuilder> {
    if level >= 7 {
        return None;
    }
    let builder = match level {
        0 => BtrBlocksCompressorBuilder::empty(),
        1 => builder_with_schemes(&[
            &integer::SparseScheme,
            &float::NullDominatedSparseScheme,
            &string::NullDominatedSparseScheme,
        ]),
        2 => builder_with_schemes(&[&string::ZstdScheme]),
        3 => builder_with_schemes(&[
            &integer::SparseScheme,
            &integer::IntDictScheme,
            &integer::FoRScheme,
            &integer::BitPackingScheme,
            &integer::ZigZagScheme,
            &integer::RunEndScheme,
            &integer::SequenceScheme,
            &float::NullDominatedSparseScheme,
            &float::FloatDictScheme,
            &float::FloatRLEScheme,
            &string::NullDominatedSparseScheme,
            &string::StringDictScheme,
            &string::ZstdScheme,
        ]),
        _ => BtrBlocksCompressorBuilder::default().exclude_schemes([string::FSSTScheme.id()]),
    };
    Some(WriteStrategyBuilder::default().with_btrblocks_builder(builder))
}

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_usize(&mut self) -> usize {
        usize::try_from(self.next()).expect("runtime is 64-bit; u64 fits usize")
    }

    fn range(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.next_usize() % (hi - lo)
    }

    fn string(&mut self, min_len: usize, max_len: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let len = self.range(min_len, max_len + 1);
        (0..len)
            .map(|_| char::from(CHARSET[self.next_usize() % CHARSET.len()]))
            .collect()
    }
}

/// Pool of `k` distinct ~10-17 char strings.
fn string_pool(rng: &mut Rng, k: usize) -> Vec<String> {
    (0..k)
        .map(|i| format!("{}#{i:06}", rng.string(8, 10)))
        .collect()
}

type MakeColumn = Box<dyn FnMut(&mut Rng, usize) -> ArrowArrayRef>;

struct ColumnSpec {
    field: Field,
    make: MakeColumn,
}

fn columns(large: bool) -> Vec<ColumnSpec> {
    let mut specs: Vec<ColumnSpec> = Vec::new();

    // c_id shape: cycling 1..=3000 (integer-dictionary candidate)
    specs.push(ColumnSpec {
        field: Field::new("c_id", DataType::Int32, true),
        make: Box::new(|_rng, base| {
            Arc::new(Int32Array::from_iter_values((0..8192_usize).map(
                move |i| i32::try_from((base + i) % 3000 + 1).expect("bounded by 3000"),
            )))
        }),
    });

    // near-unique city shape (dict must be skipped; canonical path)
    specs.push(ColumnSpec {
        field: Field::new("c_city", DataType::Utf8, true),
        make: Box::new(|rng, base| {
            Arc::new(StringArray::from_iter_values(
                (0..8192_usize).map(|i| format!("{}{:08x}", rng.string(6, 14), base + i)),
            ))
        }),
    });

    // long out-of-line strings (c_data shape)
    specs.push(ColumnSpec {
        field: Field::new("c_data", DataType::Utf8, true),
        make: Box::new(|rng, _| {
            Arc::new(StringArray::from_iter_values(
                (0..8192_usize).map(|_| rng.string(120, 260)),
            ))
        }),
    });

    // dict-cardinality boundary pools (random assignment, not cycling, so any
    // internal re-chunking still sees high cardinality per compression unit).
    // Column names keep the boundary each probes at large scale; in CI mode the
    // pools cap at one block's worth of rows (8192) — per-8K-block cardinality
    // can't exceed that anyway, and the cap keeps default pool build cheap.
    for k in [255_usize, 256, 257, 65535, 65536, 65537, 100_000] {
        let pool_len = if large { k } else { k.min(8192) };
        let mut pool_rng = Rng(0xC0_FFEE ^ u64::try_from(k).expect("small"));
        let pool = string_pool(&mut pool_rng, pool_len);
        specs.push(ColumnSpec {
            field: Field::new(format!("b{k}"), DataType::Utf8, true),
            make: Box::new(move |rng, _| {
                Arc::new(StringArray::from_iter_values(
                    (0..8192_usize).map(|_| &pool[rng.next_usize() % pool.len()]),
                ))
            }),
        });
    }

    // null-dominated sparse int (85% null)
    specs.push(ColumnSpec {
        field: Field::new("sparse_i", DataType::Int64, true),
        make: Box::new(|rng, _| {
            Arc::new(
                (0..8192_usize)
                    .map(|_| {
                        (rng.next() % 100 < 15)
                            .then(|| i64::try_from(rng.next() >> 1).expect("shifted into range"))
                    })
                    .collect::<Int64Array>(),
            )
        }),
    });

    // null-dominated sparse string (85% null)
    specs.push(ColumnSpec {
        field: Field::new("sparse_s", DataType::Utf8, true),
        make: Box::new(|rng, _| {
            Arc::new(
                (0..8192_usize)
                    .map(|_| (rng.next() % 100 < 15).then(|| rng.string(10, 20)))
                    .collect::<StringArray>(),
            )
        }),
    });

    // constants
    specs.push(ColumnSpec {
        field: Field::new("const_s", DataType::Utf8, true),
        make: Box::new(|_, _| {
            Arc::new(StringArray::from_iter_values(
                (0..8192_usize).map(|_| "GOOD-GENERIC-BRAND"),
            ))
        }),
    });
    specs.push(ColumnSpec {
        field: Field::new("const_i", DataType::Int64, true),
        make: Box::new(|_, _| Arc::new(Int64Array::from_iter_values((0..8192_usize).map(|_| 42)))),
    });

    // repetitive floats (float-dictionary candidate) + random floats
    let fpool: Vec<f64> = {
        let mut r = Rng(0xF10A7);
        (0..500)
            .map(|_| f64::from(u32::try_from(r.next() % 1_000_000).expect("bounded")) / 100.0)
            .collect()
    };
    specs.push(ColumnSpec {
        field: Field::new("f_dict", DataType::Float64, true),
        make: Box::new(move |rng, _| {
            Arc::new(Float64Array::from_iter_values(
                (0..8192_usize).map(|_| fpool[rng.next_usize() % fpool.len()]),
            ))
        }),
    });
    specs.push(ColumnSpec {
        field: Field::new("f_rand", DataType::Float64, true),
        make: Box::new(|rng, _| {
            Arc::new(Float64Array::from_iter_values((0..8192_usize).map(|_| {
                f64::from_bits(0x3FF0_0000_0000_0000 | (rng.next() >> 12))
            })))
        }),
    });

    specs
}

/// Build the dataset: `n_small` 8192-row batches, 3 sliced batches, and one
/// jumbo batch of `jumbo_chunks` × 8192 rows (as a single `RecordBatch`).
fn build_batches(
    n_small: usize,
    jumbo_chunks: usize,
    large: bool,
) -> (SchemaRef, Vec<RecordBatch>) {
    let mut specs = columns(large);
    let schema: SchemaRef = Arc::new(Schema::new(
        specs.iter().map(|s| s.field.clone()).collect::<Vec<_>>(),
    ));
    let mut rng = Rng(0x5EED_CAFE);
    let mut batches = Vec::new();
    let mut base = 0_usize;

    let mut make_8k = |rng: &mut Rng, base: usize| -> RecordBatch {
        let cols: Vec<ArrowArrayRef> = specs.iter_mut().map(|s| (s.make)(rng, base)).collect();
        RecordBatch::try_new(Arc::clone(&schema), cols).expect("batch construction must succeed")
    };

    for _ in 0..n_small {
        let b = make_8k(&mut rng, base);
        base += 8192;
        batches.push(b);
    }

    // Sliced batches: the coalesced-CDC apply shape (arrays with offsets).
    let big = {
        let parts: Vec<RecordBatch> = (0..3)
            .map(|_| {
                let b = make_8k(&mut rng, base);
                base += 8192;
                b
            })
            .collect();
        concat_batches(&schema, &parts).expect("concat must succeed")
    };
    for i in 0..3 {
        batches.push(big.slice(i * 8192, 8192));
    }

    // Jumbo batch: the large mem-tier-checkpoint shape.
    let parts: Vec<RecordBatch> = (0..jumbo_chunks)
        .map(|_| {
            let b = make_8k(&mut rng, base);
            base += 8192;
            b
        })
        .collect();
    batches.push(concat_batches(&schema, &parts).expect("concat must succeed"));

    (schema, batches)
}

// ---------------------------------------------------------------------------
// Round trip + comparison
// ---------------------------------------------------------------------------

async fn roundtrip(
    level_label: &str,
    strategy: Option<WriteStrategyBuilder>,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) {
    let mut session = VortexSession::default();
    if let Some(s) = strategy {
        session = session.set(s);
    }

    let dtype = DType::from_arrow(Arc::clone(schema));
    let owned: Vec<RecordBatch> = batches.to_vec();
    let stream = futures::stream::iter(owned.into_iter().map(|rb| ArrayRef::from_arrow(rb, false)));
    let adapter = ArrayStreamAdapter::new(dtype, stream);

    let mut buf = ByteBufferMut::empty();
    let encode_start = std::time::Instant::now();
    session
        .write_options()
        .write(&mut buf, adapter)
        .await
        .expect("vortex write must succeed");
    let encode_ms = encode_start.elapsed().as_millis();

    let file_len = buf.len();
    eprintln!("[{level_label}] encode_ms={encode_ms}");

    // Read back with a *default* session (mirrors the production read path:
    // the write strategy is not registered on read).
    let read_session = VortexSession::default();
    let vxf = read_session
        .open_options()
        .open_buffer(buf)
        .expect("open must succeed");
    let stream = vxf
        .scan()
        .expect("scan must succeed")
        .into_array_stream()
        .expect("stream must succeed");
    futures::pin_mut!(stream);

    let mut got: Vec<RecordBatch> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("scan chunk must succeed");
        let mut ctx = read_session.create_execution_ctx();
        let arrow_session = ctx.session().clone();
        let arrow = arrow_session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .expect("arrow conversion must succeed");
        got.push(RecordBatch::from(arrow.as_struct().clone()));
    }

    let expected = concat_batches(schema, batches).expect("concat expected");
    let total_rows: usize = got.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        expected.num_rows(),
        total_rows,
        "[{level_label}] row count mismatch: wrote {} read {total_rows}",
        expected.num_rows(),
    );

    // Normalize read-back to the input arrow types, column by column, then
    // compare cell-by-cell (first mismatches reported with context).
    let mut mismatches = 0_usize;
    for (ci, field) in schema.fields().iter().enumerate() {
        let exp_col = expected.column(ci);
        let got_parts: Vec<ArrowArrayRef> = got
            .iter()
            .map(|b| {
                let c = b.column_by_name(field.name()).unwrap_or_else(|| {
                    panic!(
                        "[{level_label}] column {} missing in read-back",
                        field.name()
                    )
                });
                cast(c, field.data_type()).expect("cast to input type must succeed")
            })
            .collect();
        let got_refs: Vec<&dyn Array> = got_parts.iter().map(AsRef::as_ref).collect();
        let got_col = arrow::compute::concat(&got_refs).expect("concat got");

        for ri in 0..expected.num_rows() {
            let e_null = exp_col.is_null(ri);
            let g_null = got_col.is_null(ri);
            let equal = match (e_null, g_null) {
                (true, true) => true,
                (false, false) => {
                    array_value_to_string(exp_col, ri).expect("format expected cell")
                        == array_value_to_string(&got_col, ri).expect("format read-back cell")
                }
                _ => false,
            };
            if !equal {
                mismatches += 1;
                if mismatches <= 5 {
                    eprintln!(
                        "[{level_label}] MISMATCH col={} row={ri}: expected {:?} (null={e_null}) got {:?} (null={g_null})",
                        field.name(),
                        array_value_to_string(exp_col, ri).unwrap_or_default(),
                        array_value_to_string(&got_col, ri).unwrap_or_default(),
                    );
                }
            }
        }
    }

    eprintln!("[{level_label}] file_bytes={file_len} rows={total_rows} mismatches={mismatches}");
    assert_eq!(
        mismatches, 0,
        "[{level_label}] round-trip corruption detected ({mismatches} cells)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn light_levels_roundtrip_multichunk() {
    // CI default: 2 small + 3 sliced + one 4×8192 jumbo = 73,728 rows. Vortex
    // compresses per fixed 8K-row block, so per-block scheme coverage matches
    // the full audit dataset; LIGHT_ROUNDTRIP_LARGE=1 restores it (~417k rows).
    let large = std::env::var("LIGHT_ROUNDTRIP_LARGE").is_ok();
    let (n_small, jumbo) = if large { (16, 32) } else { (2, 4) };
    let (schema, batches) = build_batches(n_small, jumbo, large);
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    eprintln!(
        "dataset: {} batches, {total} rows, {} cols",
        batches.len(),
        schema.fields().len()
    );

    for level in [2_u8, 1, 3] {
        roundtrip(
            &format!("level-{level}"),
            strategy_builder_for_level(level),
            &schema,
            &batches,
        )
        .await;
    }
    // FULL control (session default cascade — the maintenance-write path).
    roundtrip("full", None, &schema, &batches).await;
}
