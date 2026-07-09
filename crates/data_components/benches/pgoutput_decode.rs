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

//! Decode microbench for the Postgres pgoutput CDC path, over a change stream
//! shaped like a TPC-H `orders` table (mixed int / numeric / date / string
//! columns).
//!
//! Three stages are measured per format so the deferred-parsing tradeoff is
//! directly quantifiable — all three run the *same* synthesized Relation + N
//! Insert messages:
//!   * `{fmt}`         — combined `Decoder::decode` + `build_change_batch`
//!                       (what the pump does eagerly today; kept under the bare
//!                       `text`/`binary` id so `--baseline text-before` still
//!                       compares against the pre-refactor number).
//!   * `{fmt}-decode`  — `Decoder::decode` only, i.e. the pump-side work that
//!                       STAYS on the shared pump after deferral (peel zero-copy
//!                       `Bytes` slices into buffered `DecodedChange`s).
//!   * `{fmt}-build`   — `build_change_batch` only, over pre-decoded changes,
//!                       i.e. the Arrow-typing + UTF-8 work that MOVES to the
//!                       per-dataset consumer under deferred parsing.
//! `build / (decode + build)` is the fraction of pump CPU deferral sheds — the
//! go/no-go signal for the deferred-parse change.
//!
//! The `text` and `binary` variants encode the *same logical rows* two ways, so
//! the numbers are directly comparable. Payload `Bytes` are built once, outside
//! the timed loop; the per-message `Bytes::clone` inside the loop is an O(1)
//! refcount bump — the same handoff the replication frame reader performs. The
//! `-build` stage decodes once outside the timed loop (the `Bytes` slices stay
//! valid because the source payload vectors outlive the loop).
//!
//! Run:
//!   cargo bench -p data_components --features postgres --bench pgoutput_decode
//! Compare against the pre-refactor text baseline:
//!   cargo bench -p data_components --features postgres --bench pgoutput_decode \
//!     -- --baseline text-before

#![allow(clippy::expect_used, clippy::cast_possible_truncation)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use data_components::postgres_replication::changes::{ChangeOp, DecodedChange, build_change_batch};
use data_components::postgres_replication::pgoutput::{DecodedMessage, Decoder, Relation};

const REL_ID: u32 = 16_384;

/// Postgres type OIDs used by the `orders` fixture.
mod oid {
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const NUMERIC: u32 = 1700;
    pub const DATE: u32 = 1082;
}

/// numeric typmod packs `((precision << 16) | scale) + VARHDRSZ(4)`.
fn numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | scale) + 4
}

/// (name, oid, typmod, is_key) for each `orders` column, in table order.
fn orders_columns() -> Vec<(&'static str, u32, i32, bool)> {
    vec![
        ("o_orderkey", oid::INT8, -1, true),
        ("o_custkey", oid::INT8, -1, false),
        ("o_orderstatus", oid::BPCHAR, -1, false),
        ("o_totalprice", oid::NUMERIC, numeric_typmod(15, 2), false),
        ("o_orderdate", oid::DATE, -1, false),
        ("o_orderpriority", oid::VARCHAR, -1, false),
        ("o_clerk", oid::VARCHAR, -1, false),
        ("o_shippriority", oid::INT4, -1, false),
        ("o_comment", oid::VARCHAR, -1, false),
    ]
}

/// Dataset Arrow schema matching what the Postgres provider exposes for
/// `orders`.
fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ]))
}

// ---- shared per-row logical values --------------------------------------

/// Deterministic logical values for row `i`, kept as native types so the text
/// and binary encoders can render the *same* row two ways.
struct OrderRow {
    orderkey: i64,
    custkey: i64,
    status: &'static str,
    dollars: u64,
    cents: u16,
    day: u32, // day-of-month in 1996-01
    priority: &'static str,
    clerk: String,
    shippriority: i32,
    comment: String,
}

fn order_row(i: usize) -> OrderRow {
    OrderRow {
        orderkey: i as i64 + 1,
        custkey: (i % 150_000) as i64 + 1,
        status: ["O", "F", "P"][i % 3],
        dollars: 10_000 + (i % 300_000) as u64,
        cents: (i % 100) as u16,
        day: 1 + (i % 27) as u32,
        priority: ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"][i % 5],
        clerk: format!("Clerk#{:09}", (i % 1000) + 1),
        shippriority: 0,
        comment: format!(
            "carefully regular deposits for order {} nag across the express requests",
            i + 1
        ),
    }
}

// ---- pgoutput Relation encoder ------------------------------------------

fn put_cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn encode_relation() -> Bytes {
    let mut out = vec![b'R'];
    out.extend_from_slice(&REL_ID.to_be_bytes());
    put_cstr(&mut out, "public");
    put_cstr(&mut out, "orders");
    out.push(b'd'); // replica identity DEFAULT
    let cols = orders_columns();
    out.extend_from_slice(&(cols.len() as u16).to_be_bytes());
    for (name, type_oid, typmod, is_key) in cols {
        out.push(u8::from(is_key)); // flags: 0x01 => key
        put_cstr(&mut out, name);
        out.extend_from_slice(&type_oid.to_be_bytes());
        out.extend_from_slice(&typmod.to_be_bytes());
    }
    Bytes::from(out)
}

// ---- text-format Insert encoder (pgoutput tuple tag `t`) -----------------

fn row_text(r: &OrderRow) -> Vec<String> {
    vec![
        r.orderkey.to_string(),
        r.custkey.to_string(),
        r.status.to_string(),
        format!("{}.{:02}", r.dollars, r.cents),
        format!("1996-01-{:02}", r.day),
        r.priority.to_string(),
        r.clerk.clone(),
        r.shippriority.to_string(),
        r.comment.clone(),
    ]
}

fn encode_insert_text(values: &[String]) -> Bytes {
    let mut out = vec![b'I'];
    out.extend_from_slice(&REL_ID.to_be_bytes());
    out.push(b'N');
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for v in values {
        out.push(b't');
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v.as_bytes());
    }
    Bytes::from(out)
}

// ---- binary-format Insert encoder (pgoutput tuple tag `b`) ---------------

/// Encode a Postgres binary `numeric` for `dollars.cents` at scale 2. Groups
/// are base-10000, most-significant first; the fractional group is `cents*100`.
fn enc_numeric_dollars_cents(dollars: u64, cents: u16) -> Vec<u8> {
    let mut int_groups: Vec<u16> = Vec::new();
    let mut n = dollars;
    while n > 0 {
        int_groups.push((n % 10_000) as u16);
        n /= 10_000;
    }
    int_groups.reverse();
    let g = int_groups.len();
    let mut digits = int_groups;
    digits.push(cents * 100);
    let weight: i16 = if g == 0 { -1 } else { g as i16 - 1 };

    let mut o = Vec::new();
    o.extend_from_slice(&(digits.len() as u16).to_be_bytes());
    o.extend_from_slice(&weight.to_be_bytes());
    o.extend_from_slice(&0u16.to_be_bytes()); // sign = positive
    o.extend_from_slice(&2u16.to_be_bytes()); // dscale
    for d in &digits {
        o.extend_from_slice(&d.to_be_bytes());
    }
    o
}

/// Days from the Postgres epoch (2000-01-01) to 1996-01-`day`.
fn pg_days_1996(day: u32) -> i32 {
    -1461 + (day as i32 - 1)
}

fn row_binary(r: &OrderRow) -> Vec<Vec<u8>> {
    vec![
        r.orderkey.to_be_bytes().to_vec(),
        r.custkey.to_be_bytes().to_vec(),
        r.status.as_bytes().to_vec(),
        enc_numeric_dollars_cents(r.dollars, r.cents),
        pg_days_1996(r.day).to_be_bytes().to_vec(),
        r.priority.as_bytes().to_vec(),
        r.clerk.as_bytes().to_vec(),
        r.shippriority.to_be_bytes().to_vec(),
        r.comment.as_bytes().to_vec(),
    ]
}

fn encode_insert_binary(values: &[Vec<u8>]) -> Bytes {
    let mut out = vec![b'I'];
    out.extend_from_slice(&REL_ID.to_be_bytes());
    out.push(b'N');
    out.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for v in values {
        out.push(b'b');
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    Bytes::from(out)
}

// ---- measured work -------------------------------------------------------

/// Pump-side work retained after deferral: decode the Relation and peel each
/// Insert's tuple into a buffered `DecodedChange` (zero-copy `Bytes` slices).
fn decode_only(relation: &Bytes, inserts: &[Bytes]) -> (Relation, Vec<DecodedChange>) {
    let mut decoder = Decoder::new();
    let rel: Relation = match decoder.decode(relation.clone()).expect("decode relation") {
        DecodedMessage::Relation(r) => r,
        other => panic!("expected Relation, got {other:?}"),
    };
    let mut changes: Vec<DecodedChange> = Vec::with_capacity(inserts.len());
    for msg in inserts {
        match decoder.decode(msg.clone()).expect("decode insert") {
            DecodedMessage::Insert { tuple, .. } => changes.push(DecodedChange {
                op: ChangeOp::Create,
                row: tuple,
            }),
            other => panic!("expected Insert, got {other:?}"),
        }
    }
    (rel, changes)
}

/// Pump-side work after increment 2 (raw-buffering): fully decode the (rare)
/// Relation to cache schema, then only PEEK each change message's kind + relation
/// id to route it — the tuple is left as raw bytes for the consumer to decode.
/// This is what the shared pump would do before buffering, so `route_peek` vs
/// `decode` is the pump per-event cost after vs before deferring the decode.
fn route_peek(relation: &Bytes, inserts: &[Bytes]) -> u32 {
    let mut decoder = Decoder::new();
    let _ = decoder.decode(relation.clone()).expect("decode relation");
    let mut acc = 0u32;
    for msg in inserts {
        // pgoutput I/U/D layout: tag = msg[0], relation_id = msg[1..5] (big-endian).
        let relid = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        acc = acc.wrapping_add(relid);
    }
    acc
}

/// Deferred work: Arrow-type + UTF-8 the pre-decoded changes into a batch. This
/// is what moves off the pump to the per-dataset consumer under deferred parse.
fn build_only(schema: &SchemaRef, rel: &Relation, changes: &[DecodedChange]) {
    let batch = build_change_batch(schema, rel, changes).expect("build batch");
    black_box(batch);
}

fn decode_and_build(schema: &SchemaRef, relation: &Bytes, inserts: &[Bytes]) {
    let (rel, changes) = decode_only(relation, inserts);
    build_only(schema, &rel, &changes);
}

fn bench_decode(c: &mut Criterion) {
    let schema = orders_schema();
    let mut group = c.benchmark_group("pgoutput_orders_decode");
    for &n in &[100usize, 1_000, 10_000] {
        let rows: Vec<OrderRow> = (0..n).map(order_row).collect();
        let relation = encode_relation();
        let text: Vec<Bytes> = rows
            .iter()
            .map(|r| encode_insert_text(&row_text(r)))
            .collect();
        let binary: Vec<Bytes> = rows
            .iter()
            .map(|r| encode_insert_binary(&row_binary(r)))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        for (fmt, inserts) in [("text", &text), ("binary", &binary)] {
            // Combined (eager pump path today). Bare `text`/`binary` id preserves
            // `--baseline text-before` comparability.
            group.bench_with_input(
                BenchmarkId::new(fmt, n),
                &(&relation, inserts),
                |b, (r, ins)| {
                    b.iter(|| decode_and_build(black_box(&schema), black_box(r), black_box(ins)));
                },
            );
            // Decode-only: pump work after increment 1 (decode -> DecodedChange,
            // build deferred to the consumer).
            group.bench_with_input(
                BenchmarkId::new(format!("{fmt}-decode"), n),
                &(&relation, inserts),
                |b, (r, ins)| {
                    b.iter(|| black_box(decode_only(black_box(r), black_box(ins))));
                },
            );
            // Route-peek: pump work after increment 2 (peek kind+relid only,
            // decode deferred to the consumer with the raw bytes).
            group.bench_with_input(
                BenchmarkId::new(format!("{fmt}-route_peek"), n),
                &(&relation, inserts),
                |b, (r, ins)| {
                    b.iter(|| black_box(route_peek(black_box(r), black_box(ins))));
                },
            );
            // Build-only: moves to the consumer. Decode once outside the timed loop.
            let (rel, changes) = decode_only(&relation, inserts);
            group.bench_with_input(
                BenchmarkId::new(format!("{fmt}-build"), n),
                &(rel, changes),
                |b, (rel, changes)| {
                    b.iter(|| build_only(black_box(&schema), black_box(rel), black_box(changes)));
                },
            );
        }
    }
    group.finish();
}

/// Per-event *routing* cost on the shared pump: peek the relation id, look it up
/// in the route map, and buffer the raw bytes into the per-relation txn map.
/// This is the only place the hasher choice matters (the decode bench's
/// `route_peek` stage deliberately omits the maps), so it compares std `HashMap`
/// (SipHash) vs `FxHashMap` for the two `u32`-keyed lookups. `M` relations, `N`
/// change messages round-robined across them, a fresh txn per iteration (one
/// transaction's worth).
fn bench_routing(c: &mut Criterion) {
    use rustc_hash::FxHashMap;
    use std::collections::HashMap;

    const M: u32 = 16; // subscribed relations (tables) on the shared slot
    const BASE_OID: u32 = 16_384;

    fn make_msgs(n: usize) -> Vec<Bytes> {
        (0..n)
            .map(|i| {
                let relid = BASE_OID + (i as u32 % M);
                let mut o = vec![b'I'];
                o.extend_from_slice(&relid.to_be_bytes());
                o.push(b'N');
                o.extend_from_slice(&1u16.to_be_bytes());
                o.push(b't');
                o.extend_from_slice(&1u32.to_be_bytes());
                o.push(b'x');
                Bytes::from(o)
            })
            .collect()
    }

    let mut group = c.benchmark_group("pgoutput_routing");
    for &n in &[1_000usize, 10_000] {
        let msgs = make_msgs(n);
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("siphash", n), &msgs, |b, msgs| {
            let mut routes: HashMap<u32, usize> = HashMap::new();
            for k in 0..M {
                routes.insert(BASE_OID + k, k as usize);
            }
            b.iter(|| {
                let mut txn: HashMap<u32, Vec<Bytes>> = HashMap::new();
                for msg in msgs {
                    let relid = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
                    if routes.get(&relid).is_some() {
                        txn.entry(relid).or_default().push(msg.clone());
                    }
                }
                black_box(&txn);
            });
        });

        group.bench_with_input(BenchmarkId::new("fxhash", n), &msgs, |b, msgs| {
            let mut routes: FxHashMap<u32, usize> = FxHashMap::default();
            for k in 0..M {
                routes.insert(BASE_OID + k, k as usize);
            }
            b.iter(|| {
                let mut txn: FxHashMap<u32, Vec<Bytes>> = FxHashMap::default();
                for msg in msgs {
                    let relid = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
                    if routes.get(&relid).is_some() {
                        txn.entry(relid).or_default().push(msg.clone());
                    }
                }
                black_box(&txn);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_decode, bench_routing);
criterion_main!(benches);
