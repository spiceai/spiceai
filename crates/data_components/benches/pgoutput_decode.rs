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
//! columns). Measures the full hot path: `Decoder::decode` over a synthesized
//! Relation + N Insert messages, then `build_change_batch` into Arrow.
//!
//! The `text` and `binary` variants encode the *same logical rows* two ways, so
//! the numbers are directly comparable. Payload `Bytes` are built once, outside
//! the timed loop; the per-message `Bytes::clone` inside the loop is an O(1)
//! refcount bump — the same handoff the replication frame reader performs.
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

fn decode_and_build(schema: &SchemaRef, relation: &Bytes, inserts: &[Bytes]) {
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
    let batch = build_change_batch(schema, &rel, &changes).expect("build batch");
    black_box(batch);
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
        group.bench_with_input(
            BenchmarkId::new("text", n),
            &(&relation, &text),
            |b, (r, ins)| {
                b.iter(|| decode_and_build(black_box(&schema), black_box(r), black_box(ins)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("binary", n),
            &(&relation, &binary),
            |b, (r, ins)| {
                b.iter(|| decode_and_build(black_box(&schema), black_box(r), black_box(ins)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
