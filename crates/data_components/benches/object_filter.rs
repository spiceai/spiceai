#![allow(clippy::expect_used)]

use chrono::{DateTime, Utc};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_components::object::filter::filter_object_meta;
use datafusion::prelude::{col, lit};
use object_store::ObjectMeta;
use object_store::path::Path;
use std::hint::black_box;

fn create_test_meta(
    location: &str,
    last_modified: DateTime<Utc>,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(location),
        last_modified,
        size,
        e_tag,
        version,
    }
}

// Benchmark: Filtering with no filters (baseline)
fn bench_filter_no_filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_no_filters");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0_u64..size)
            .map(|i| {
                create_test_meta(
                    &format!("file_{i:06}.txt"),
                    now,
                    i * 1024,
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("empty_filters", size),
            &metas,
            |b, metas| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(&[]), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Simple size filtering
fn bench_filter_by_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_by_size");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0_u64..size)
            .map(|i| {
                create_test_meta(
                    &format!("file_{i:06}.txt"),
                    now,
                    i * 1024,
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        // Filter for files > halfway point
        let threshold = (size / 2) * 1024;
        let filters = vec![col("size").gt(lit(threshold))];

        group.bench_with_input(
            BenchmarkId::new("gt_threshold", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Combined filters (size AND location pattern)
fn bench_filter_combined(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_combined");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0_u64..size)
            .map(|i| {
                let prefix = if i % 3 == 0 {
                    "data"
                } else if i % 3 == 1 {
                    "logs"
                } else {
                    "tmp"
                };
                create_test_meta(
                    &format!("{prefix}/file_{i:06}.txt"),
                    now,
                    i * 1024,
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        let threshold = (size / 2) * 1024;
        let filters = vec![
            col("size").gt(lit(threshold)),
            col("location").like(lit("data%")),
        ];

        group.bench_with_input(
            BenchmarkId::new("size_and_location", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Complex filter with OR conditions
fn bench_filter_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_complex");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0_u64..size)
            .map(|i| {
                let etag = if i % 5 == 0 {
                    Some(format!("etag_{i}"))
                } else {
                    None
                };
                let version = if i % 7 == 0 {
                    Some(format!("v{i}"))
                } else {
                    None
                };
                create_test_meta(&format!("file_{i:06}.txt"), now, i * 1024, etag, version)
            })
            .collect();

        // (size < 200KB OR e_tag IS NOT NULL) AND version IS NOT NULL
        let filters = vec![
            col("size")
                .lt(lit(200_000u64))
                .or(col("e_tag").is_not_null())
                .and(col("version").is_not_null()),
        ];

        group.bench_with_input(
            BenchmarkId::new("complex_boolean", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Timestamp filtering
fn bench_filter_by_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_by_timestamp");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0..size)
            .map(|i| {
                // Spread timestamps over 30 days
                let timestamp = now - chrono::Duration::days(i64::from(i % 30));
                create_test_meta(
                    &format!("file_{i:06}.txt"),
                    timestamp,
                    u64::try_from(i * 1024).expect("i32 to u64 conversion"),
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        // Filter for files modified in last 7 days
        let cutoff = now - chrono::Duration::days(7);
        let filters = vec![col("last_modified").gt(datafusion::prelude::Expr::Literal(
            datafusion::scalar::ScalarValue::TimestampMillisecond(
                Some(cutoff.timestamp_millis()),
                Some("UTC".into()),
            ),
            None,
        ))];

        group.bench_with_input(
            BenchmarkId::new("recent_files", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: High selectivity (most items match)
fn bench_filter_high_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_high_selectivity");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0..size)
            .map(|i| {
                create_test_meta(
                    &format!("file_{i:06}.txt"),
                    now,
                    u64::try_from(i * 1024).expect("i32 to u64 conversion"),
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        // Filter that matches 95% of items
        let threshold = (size * 5 / 100) * 1024;
        let filters = vec![col("size").gt(lit(
            u64::try_from(threshold).expect("i32 to u64 conversion"),
        ))];

        group.bench_with_input(
            BenchmarkId::new("95pct_match", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Low selectivity (few items match)
fn bench_filter_low_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_low_selectivity");
    let now = Utc::now();

    for size in [100, 1_000, 10_000, 50_000] {
        let metas: Vec<ObjectMeta> = (0..size)
            .map(|i| {
                create_test_meta(
                    &format!("file_{i:06}.txt"),
                    now,
                    u64::try_from(i * 1024).expect("i32 to u64 conversion"),
                    Some(format!("etag_{i}")),
                    Some(format!("v{i}")),
                )
            })
            .collect();

        // Filter that matches only 5% of items
        let threshold = (size * 95 / 100) * 1024;
        let filters = vec![col("size").gt(lit(
            u64::try_from(threshold).expect("i32 to u64 conversion"),
        ))];

        group.bench_with_input(
            BenchmarkId::new("5pct_match", size),
            &(metas, filters),
            |b, (metas, filters)| {
                b.iter(|| {
                    let result = filter_object_meta(black_box(filters), black_box(metas));
                    black_box(result).expect("Should succeed");
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_filter_no_filters,
    bench_filter_by_size,
    bench_filter_combined,
    bench_filter_complex,
    bench_filter_by_timestamp,
    bench_filter_high_selectivity,
    bench_filter_low_selectivity,
);
criterion_main!(benches);
