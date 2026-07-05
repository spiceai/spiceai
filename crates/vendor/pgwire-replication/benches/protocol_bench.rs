//! Benchmarks for the protocol module.
//!
//! Run with: `cargo bench --bench protocol_bench`

use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::io::Cursor;

use pgwire_replication::lsn::Lsn;
use pgwire_replication::protocol::framing::{read_backend_message_into, MessageReader};
use pgwire_replication::protocol::messages::{parse_error_response, ErrorFields};
use pgwire_replication::protocol::replication::{encode_standby_status_update, parse_copy_data};

/// Generate a realistic XLogData payload
fn make_xlogdata_payload(data_size: usize) -> Bytes {
    let mut v = Vec::with_capacity(1 + 24 + data_size);
    v.push(b'w');
    v.extend_from_slice(&0x0123456789ABCDEFu64.to_be_bytes()); // wal_start
    v.extend_from_slice(&0xFEDCBA9876543210u64.to_be_bytes()); // wal_end
    v.extend_from_slice(&1234567890i64.to_be_bytes()); // server_time
    v.extend_from_slice(&vec![0x42u8; data_size]); // payload
    Bytes::from(v)
}

/// Generate a KeepAlive payload
fn make_keepalive_payload() -> Bytes {
    let mut v = Vec::with_capacity(18);
    v.push(b'k');
    v.extend_from_slice(&100i64.to_be_bytes());
    v.extend_from_slice(&200i64.to_be_bytes());
    v.push(1);
    Bytes::from(v)
}

/// Generate a realistic error response payload
fn make_error_payload() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"SERROR\0");
    payload.extend_from_slice(b"VFATAL\0");
    payload.extend_from_slice(b"C42P01\0");
    payload.extend_from_slice(b"Mrelation \"users\" does not exist\0");
    payload.extend_from_slice(b"Dtable was dropped in a previous migration\0");
    payload.extend_from_slice(b"Hcheck your migration scripts\0");
    payload.extend_from_slice(b"Fparse_relation.c\0");
    payload.extend_from_slice(b"L1234\0");
    payload.extend_from_slice(b"Rparseropen\0");
    payload.push(0);
    payload
}

fn bench_parse_xlogdata(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_xlogdata");

    for size in [64, 256, 1024, 4096, 16384] {
        let payload = make_xlogdata_payload(size);
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter(|| parse_copy_data(black_box(payload.clone())));
        });
    }

    group.finish();
}

fn bench_parse_keepalive(c: &mut Criterion) {
    let payload = make_keepalive_payload();

    c.bench_function("parse_keepalive", |b| {
        b.iter(|| parse_copy_data(black_box(payload.clone())));
    });
}

fn bench_encode_status_update(c: &mut Criterion) {
    c.bench_function("encode_standby_status_update", |b| {
        b.iter(|| {
            encode_standby_status_update(
                black_box(Lsn(0x123456789ABCDEF0)),
                black_box(1234567890),
                black_box(false),
            )
        });
    });
}

fn bench_parse_error_response(c: &mut Criterion) {
    let payload = make_error_payload();

    c.bench_function("parse_error_response", |b| {
        b.iter(|| parse_error_response(black_box(&payload)));
    });
}

fn bench_error_fields_parse(c: &mut Criterion) {
    let payload = make_error_payload();

    c.bench_function("ErrorFields::parse", |b| {
        b.iter(|| ErrorFields::parse(black_box(&payload)));
    });
}

/// Build a buffer of N back-to-back CopyData messages of `data_size` payload.
fn make_copy_data_stream(count: usize, data_size: usize) -> Vec<u8> {
    let payload = make_xlogdata_payload(data_size);
    let frame_len = (4 + payload.len()) as i32;

    let mut buf = Vec::with_capacity(count * (5 + payload.len()));
    for _ in 0..count {
        buf.push(b'd'); // CopyData tag
        buf.extend_from_slice(&frame_len.to_be_bytes());
        buf.extend_from_slice(&payload);
    }
    buf
}

/// Compare the throughput of the legacy `read_backend_message_into` (not
/// cancellation-safe) against the new `MessageReader::read` (cancellation-safe).
/// Both are driven against an in-memory `Cursor<Vec<u8>>` so the benchmark
/// isolates framing overhead from socket / scheduler effects.
fn bench_read_backend_message(c: &mut Criterion) {
    const COUNT: usize = 256;
    let mut group = c.benchmark_group("read_backend_message");

    for size in [64, 256, 1024, 4096] {
        let stream = make_copy_data_stream(COUNT, size);
        group.throughput(Throughput::Bytes(stream.len() as u64));

        // Legacy non-cancel-safe path (kept for compatibility).
        group.bench_with_input(
            BenchmarkId::new("read_backend_message_into", size),
            &stream,
            |b, stream| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut cur = Cursor::new(black_box(stream.as_slice()));
                        let mut buf = BytesMut::with_capacity(4096);
                        for _ in 0..COUNT {
                            let _msg = read_backend_message_into(&mut cur, &mut buf).await.unwrap();
                        }
                    });
                });
            },
        );

        // New cancellation-safe path used by the streaming loop.
        group.bench_with_input(
            BenchmarkId::new("MessageReader", size),
            &stream,
            |b, stream| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut cur = Cursor::new(black_box(stream.as_slice()));
                        let mut reader = MessageReader::new();
                        for _ in 0..COUNT {
                            let _msg = reader.read(&mut cur).await.unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_xlogdata,
    bench_parse_keepalive,
    bench_encode_status_update,
    bench_parse_error_response,
    bench_error_fields_parse,
    bench_read_backend_message,
);
criterion_main!(benches);
