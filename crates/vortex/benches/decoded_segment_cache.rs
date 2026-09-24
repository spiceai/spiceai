// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

#![expect(
    clippy::expect_used,
    reason = "a benchmark fixture cannot return an error through Criterion; setup and scan failures must stop the run with context"
)]
#![expect(
    clippy::from_iter_instead_of_collect,
    reason = "the Vortex array constructors make the intended physical array type explicit"
)]

//! Low-level Vortex-file scans with a warm encoded cache, with and without a
//! decoded-segment cache.
//!
//! The fixture opens a new file and reader tree for every iteration. This is
//! intentional: a reader tree keeps its own decoded array future, whereas the
//! process cache needs to remove that decode work across independent scans.

use std::sync::Arc;

use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::{StreamExt, pin_mut};
use moka::future::Cache;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use tokio::runtime::Runtime;
use vortex::VortexSessionDefault;
use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinArray};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::ByteBuffer;
use vortex::dtype::{DType, Nullability};
use vortex::error::VortexResult;
use vortex::expr::{col, eq, lit};
use vortex::file::{Footer, OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::VortexWrite;
use vortex::io::object_store::{ObjectStoreReadAt, ObjectStoreWrite};
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::segments::{DecodedSegmentCache, SegmentCache, SegmentId};
use vortex::session::VortexSession;

/// The dataset is a TPCH-like mix of numeric, categorical string, and comment
/// columns. Its encoded file is deliberately close to 16 MiB: large enough for
/// Vortex segment work to be representative, small enough for a practical
/// Criterion run.
const ROW_COUNT: usize = 320_000;
const LOOKUP_ORDER_KEY: i64 = 80_000;
const MEBIBYTE: u64 = 1024 * 1024;
/// Matches Cayenne's configured segment-cache budget: encoded and fully
/// decoded segments each receive half.
const TOTAL_CACHE_CAPACITY_BYTES: u64 = 128 * 1024 * 1024;
const CACHE_CAPACITY_BYTES: u64 = TOTAL_CACHE_CAPACITY_BYTES / 2;

/// A one-file encoded cache for the low-level benchmark. Production Cayenne
/// qualifies this key with object-store and path identity; the benchmark owns
/// exactly one immutable object, so the segment identifier is sufficient here.
struct EncodedSegmentCache(Cache<SegmentId, ByteBuffer>);

impl EncodedSegmentCache {
    fn new() -> Self {
        Self(
            Cache::builder()
                .max_capacity(CACHE_CAPACITY_BYTES)
                .weigher(|_, buffer: &ByteBuffer| {
                    u32::try_from(buffer.len().min(u32::MAX as usize)).unwrap_or(u32::MAX)
                })
                .build(),
        )
    }

    async fn weighted_size(&self) -> u64 {
        self.0.run_pending_tasks().await;
        self.0.weighted_size()
    }
}

#[async_trait]
impl SegmentCache for EncodedSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ByteBuffer>> {
        Ok(self.0.get(&id).await)
    }

    async fn put(&self, id: SegmentId, buffer: ByteBuffer) -> VortexResult<()> {
        self.0.insert(id, buffer).await;
        Ok(())
    }
}

/// A one-file decoded cache paired with [`EncodedSegmentCache`].
struct BenchmarkDecodedSegmentCache(Cache<SegmentId, ArrayRef>);

impl BenchmarkDecodedSegmentCache {
    fn new() -> Self {
        Self(
            Cache::builder()
                .max_capacity(CACHE_CAPACITY_BYTES)
                .weigher(|_, array: &ArrayRef| {
                    u32::try_from(array.nbytes().min(u64::from(u32::MAX))).unwrap_or(u32::MAX)
                })
                .build(),
        )
    }

    async fn weighted_size(&self) -> u64 {
        self.0.run_pending_tasks().await;
        self.0.weighted_size()
    }
}

#[async_trait]
impl DecodedSegmentCache for BenchmarkDecodedSegmentCache {
    async fn get(&self, id: SegmentId) -> VortexResult<Option<ArrayRef>> {
        Ok(self.0.get(&id).await)
    }

    async fn put(&self, id: SegmentId, array: ArrayRef) -> VortexResult<()> {
        self.0.insert(id, array).await;
        Ok(())
    }
}

struct CachePair {
    encoded: Arc<EncodedSegmentCache>,
    decoded: Arc<BenchmarkDecodedSegmentCache>,
}

impl CachePair {
    fn new() -> Self {
        Self {
            encoded: Arc::new(EncodedSegmentCache::new()),
            decoded: Arc::new(BenchmarkDecodedSegmentCache::new()),
        }
    }

    fn encoded(&self) -> Arc<dyn SegmentCache> {
        Arc::<EncodedSegmentCache>::clone(&self.encoded)
    }

    fn decoded(&self) -> Arc<dyn DecodedSegmentCache> {
        Arc::<BenchmarkDecodedSegmentCache>::clone(&self.decoded)
    }
}

struct Fixture {
    session: VortexSession,
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
    footer: Footer,
}

fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread Tokio runtime")
}

fn row_id(index: usize) -> i64 {
    i64::try_from(index).expect("benchmark row index fits i64") + 1
}

fn tpch_comment(index: usize) -> String {
    let mut state = u64::try_from(index)
        .expect("benchmark row index fits u64")
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    let mut comment = String::with_capacity(80);
    comment.push_str("special requests ");
    for _ in 0..63 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        comment.push(char::from(
            b'a' + u8::try_from(state % 26).expect("letter index fits u8"),
        ));
    }
    comment
}

fn build_fixture(runtime: &Runtime) -> Fixture {
    runtime.block_on(async {
        let session = VortexSession::default().with_tokio();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("decoded-segment-cache-tpch.vortex");
        let orderkeys = PrimitiveArray::from_iter((0..ROW_COUNT).map(row_id)).into_array();
        let customerkeys = PrimitiveArray::from_iter(
            (0..ROW_COUNT).map(|index| (row_id(index) * 7_919) % 150_000),
        )
        .into_array();
        let partkeys = PrimitiveArray::from_iter((0..ROW_COUNT).map(|index| {
            i32::try_from((row_id(index) * 13) % 200_000).expect("part key fits i32")
        }))
        .into_array();
        let quantities = PrimitiveArray::from_iter(
            (0..ROW_COUNT)
                .map(|index| i32::try_from((row_id(index) % 50) + 1).expect("quantity fits i32")),
        )
        .into_array();
        let extended_prices = PrimitiveArray::from_iter((0..ROW_COUNT).map(|index| {
            f64::from(
                i32::try_from((row_id(index) * 17) % 100_000)
                    .expect("extended price input fits i32"),
            ) / 100.0
        }))
        .into_array();
        let discounts = PrimitiveArray::from_iter((0..ROW_COUNT).map(|index| {
            f64::from(i32::try_from(row_id(index) % 10).expect("discount input fits i32")) / 100.0
        }))
        .into_array();
        let ship_dates = PrimitiveArray::from_iter((0..ROW_COUNT).map(|index| {
            i32::try_from(9_000 + (row_id(index) % 2_500)).expect("ship date fits i32")
        }))
        .into_array();
        let string_dtype = DType::Utf8(Nullability::NonNullable);
        let order_statuses = VarBinArray::from_iter(
            (0..ROW_COUNT).map(|index| {
                Some(match index % 3 {
                    0 => "O",
                    1 => "F",
                    _ => "P",
                })
            }),
            string_dtype.clone(),
        )
        .into_array();
        let priorities = VarBinArray::from_iter(
            (0..ROW_COUNT).map(|index| {
                Some(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"][index % 5])
            }),
            string_dtype.clone(),
        )
        .into_array();
        let ship_modes = VarBinArray::from_iter(
            (0..ROW_COUNT).map(|index| Some(["AIR", "RAIL", "SHIP", "TRUCK"][index % 4])),
            string_dtype.clone(),
        )
        .into_array();
        let comments = VarBinArray::from_iter(
            (0..ROW_COUNT).map(|index| Some(tpch_comment(index))),
            string_dtype,
        )
        .into_array();
        let table = StructArray::try_new(
            [
                "orderkey",
                "customerkey",
                "partkey",
                "quantity",
                "extendedprice",
                "discount",
                "shipdate",
                "orderstatus",
                "priority",
                "shipmode",
                "comment",
            ]
            .into(),
            vec![
                orderkeys,
                customerkeys,
                partkeys,
                quantities,
                extended_prices,
                discounts,
                ship_dates,
                order_statuses,
                priorities,
                ship_modes,
                comments,
            ],
            ROW_COUNT,
            Validity::NonNullable,
        )
        .expect("build the benchmark table")
        .into_array();

        let mut writer = ObjectStoreWrite::new(Arc::clone(&store), &path)
            .await
            .expect("create in-memory Vortex writer");
        let summary = session
            .write_options()
            .write(&mut writer, table.to_array_stream())
            .await
            .expect("write the benchmark Vortex file");
        writer.shutdown().await.expect("finish the benchmark file");
        assert!(
            (12 * MEBIBYTE..=24 * MEBIBYTE).contains(&summary.size()),
            "TPCH-like benchmark file should be approximately 16 MiB, got {} bytes",
            summary.size()
        );
        eprintln!(
            "decoded-segment-cache TPCH-like fixture: {} bytes ({:.2} MiB)",
            summary.size(),
            bytes_as_f64(summary.size()) / bytes_as_f64(MEBIBYTE),
        );
        let footer_reader = Arc::new(ObjectStoreReadAt::new(
            Arc::clone(&store),
            path.clone(),
            session.handle(),
        ));
        let footer = session
            .open_options()
            .with_file_size(summary.size())
            .open_read(footer_reader)
            .await
            .expect("read benchmark Vortex footer")
            .footer()
            .clone();

        Fixture {
            session,
            store,
            path,
            file_size: summary.size(),
            footer,
        }
    })
}

async fn memory_ratio(caches: &CachePair) -> (u64, u64) {
    (
        caches.encoded.weighted_size().await,
        caches.decoded.weighted_size().await,
    )
}

fn bytes_as_f64(bytes: u64) -> f64 {
    f64::from(u32::try_from(bytes).expect("benchmark byte count fits u32"))
}

fn print_memory_ratio(shape: &str, encoded_bytes: u64, decoded_bytes: u64) {
    assert!(
        encoded_bytes > 0,
        "the warm decoded-cache scan must first cache encoded bytes"
    );
    assert!(
        decoded_bytes > 0,
        "the warm decoded-cache scan must cache a decoded segment"
    );
    eprintln!(
        "decoded-segment-cache memory ratio ({shape}): encoded={encoded_bytes}B decoded={decoded_bytes}B ratio={:.2}x",
        bytes_as_f64(decoded_bytes) / bytes_as_f64(encoded_bytes),
    );
}

async fn scan_file(
    fixture: &Fixture,
    encoded_cache: Arc<dyn SegmentCache>,
    decoded_cache: Option<Arc<dyn DecodedSegmentCache>>,
) -> u64 {
    let reader = Arc::new(ObjectStoreReadAt::new(
        Arc::clone(&fixture.store),
        fixture.path.clone(),
        fixture.session.handle(),
    ));
    let mut open_options = fixture
        .session
        .open_options()
        .with_file_size(fixture.file_size)
        .with_footer(fixture.footer.clone())
        .with_segment_cache(encoded_cache);
    if let Some(decoded_cache) = decoded_cache {
        open_options = open_options.with_decoded_segment_cache(decoded_cache);
    }

    let file = open_options
        .open_read(reader)
        .await
        .expect("open benchmark Vortex file");
    let stream = file
        .scan()
        .expect("build benchmark scan")
        .with_filter(eq(col("orderkey"), lit(LOOKUP_ORDER_KEY)))
        .into_array_stream()
        .expect("execute benchmark scan");
    pin_mut!(stream);

    let mut row_count = 0_u64;
    while let Some(array) = stream.next().await {
        row_count +=
            u64::try_from(array.expect("read benchmark array").len()).expect("row count fits u64");
    }
    row_count
}

fn warm_scan(runtime: &Runtime, fixture: &Fixture, caches: &CachePair, decoded: bool) -> u64 {
    runtime.block_on(scan_file(
        fixture,
        caches.encoded(),
        decoded.then(|| caches.decoded()),
    ))
}

fn bench_decoded_segment_cache(c: &mut Criterion) {
    let runtime = build_runtime();
    let fixture = build_fixture(&runtime);
    let encoded_only = CachePair::new();
    let decoded = CachePair::new();

    let encoded_rows = warm_scan(&runtime, &fixture, &encoded_only, false);
    let decoded_rows = warm_scan(&runtime, &fixture, &decoded, true);
    assert_eq!(
        encoded_rows, 1,
        "encoded scan finds the requested order key"
    );
    assert_eq!(
        decoded_rows, 1,
        "decoded scan finds the requested order key"
    );

    let (encoded_bytes, decoded_bytes) = runtime.block_on(memory_ratio(&decoded));
    print_memory_ratio("TPCH-like lookup", encoded_bytes, decoded_bytes);

    let mut group = c.benchmark_group("decoded_segment_cache_scan");
    group.bench_function("encoded_cache_only", |b| {
        b.iter(|| warm_scan(&runtime, &fixture, &encoded_only, false));
    });
    group.bench_function("encoded_and_decoded_cache", |b| {
        b.iter(|| warm_scan(&runtime, &fixture, &decoded, true));
    });
    group.finish();
}

criterion_group!(benches, bench_decoded_segment_cache);
criterion_main!(benches);
