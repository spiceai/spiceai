/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Regression bench: amplification cost of clearing the full
//! [`scan_listing_tables`] cache on every CDC commit.
//!
//! Older versions of
//! [`CayenneTableProvider::publish_current_snapshot_files_changed_under_held_fence`]
//! cleared the entire scan-listing-table cache on every staged-append commit.
//! The cache holds one [`Arc<ListingTable>`] per
//! (`snapshot_id`, `target_partitions`, `collect_statistics`) tuple, populated
//! lazily on the scan path via
//! `CayenneTableProvider::scan_listing_table_for_config`
//! (`crates/cayenne/src/provider/table.rs:7126-7163`).
//!
//! Every PK-conflict-handled write inserts a fresh snapshot into
//! `protected_snapshots` (`publish_written_snapshot_with_sequence`,
//! `provider/table.rs:3058-3080`), so a table that absorbs upserts between
//! compactions has N protected snapshots whose listing-table entries are
//! still valid (their on-disk file set has not changed). A full cache clear
//! evicts those entries too, so the next scan rebuilds `N + 1`
//! `ListingTable`s (current snapshot + every protected snapshot).
//!
//! The production path now retains entries whose snapshot IDs did not change:
//! invalidate only the entries that became stale, preserve the rest. This is
//! the same pattern Cayenne already uses for
//! the runtime's per-URL `list_files_cache` (which `invalidate_list_files_cache`
//! also targets at the current snapshot only).
//!
//! ## What this bench measures
//!
//! Pure shape — no Cayenne setup, no metastore. Two lanes per protected
//! snapshot count:
//!
//! - `full_clear_baseline/<snapshots>` — mirrors the old behavior: clear
//!   the cache, then rebuild `N + 1` `ListingTable` instances (one per
//!   snapshot) using the same `ListingTable::try_new` path the production
//!   `scan_listing_table_for_config` exercises. Models the next scan after
//!   one CDC commit when `N` protected snapshots exist.
//! - `targeted_retain_protected/<snapshots>` — models current behavior: clone the
//!   `Arc<ListingTable>` for each non-current snapshot (cache hit), and
//!   rebuild only the current snapshot's entry. Wall time is `N`
//!   `Arc::clone` plus one `ListingTable::try_new`.
//!
//! The gap visualizes the per-scan-after-write overhead avoided by targeted
//! invalidation. Per-scan, not per-write: writes are paced by their own cost,
//! but every scan after a full clear pays the rebuild fee.
//!
//! `cargo bench --bench scan_listing_cache_invalidation -p cayenne`.

#![expect(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};

/// Protected-snapshot counts that bracket realistic upsert workloads.
///
/// - `0`: baseline — only the current snapshot is rebuilt per scan.
/// - `4`: below the default protected-snapshot maintenance threshold but
///   already enough to show cache rebuild amplification.
/// - `16`: long-running upsert workload between compactions.
/// - `64`: pathological — large backlog of protected snapshots.
const SNAPSHOT_COUNTS: &[usize] = &[0, 4, 16, 64];

fn bench_schema() -> SchemaRef {
    // Match the typical small Cayenne schema used by other benches
    // (`vs_duckdb_helpers/common.rs::schema()`).
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn build_listing_table(url: &ListingTableUrl, schema: SchemaRef) -> Arc<ListingTable> {
    // Use ParquetFormat as a stand-in for VortexFormat: both are
    // `Arc<dyn FileFormat>`, both are constructed once per call, and the
    // dominant cost (`ListingOptions::new` + `ListingTableConfig::new` +
    // `ListingTable::try_new`) is identical for both formats. The bench
    // is measuring per-rebuild overhead at the listing-table layer, not
    // per-file-format-specific decoding.
    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let options = ListingOptions::new(format);
    let config = ListingTableConfig::new(url.clone())
        .with_listing_options(options)
        .with_schema(schema);
    Arc::new(ListingTable::try_new(config).expect("listing table should build"))
}

fn make_snapshot_url(table_dir: &std::path::Path, snapshot_id: &str) -> ListingTableUrl {
    let dir = table_dir.join(snapshot_id);
    std::fs::create_dir_all(&dir).expect("snapshot dir should be creatable");
    let dir_path = dir.to_string_lossy();
    let url = format!("file://{}/", dir_path.trim_end_matches('/'));
    ListingTableUrl::parse(&url).expect("listing url should parse")
}

fn bench_scan_listing_cache_invalidation(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_listing_cache_invalidation");
    group.sample_size(50);

    let tempdir = tempfile::tempdir().expect("temp dir");
    let table_dir = tempdir.path().to_path_buf();
    let schema = bench_schema();

    for &count in SNAPSHOT_COUNTS {
        // Pre-construct URLs for the current snapshot + `count` protected snapshots.
        // The URL parsing + dir creation happen once outside the timed region;
        // only the per-iteration ListingTable construction is measured.
        let current_url = make_snapshot_url(&table_dir, "current");
        let protected_urls: Vec<ListingTableUrl> = (0..count)
            .map(|i| make_snapshot_url(&table_dir, &format!("protected_{i}")))
            .collect();

        // Warm cache: pre-built protected snapshot entries that the
        // targeted-retain lane reuses via Arc::clone.
        let cached_protected: Vec<Arc<ListingTable>> = protected_urls
            .iter()
            .map(|url| build_listing_table(url, Arc::clone(&schema)))
            .collect();

        // Lane A — historical full-clear behavior: every entry rebuilt.
        group.bench_with_input(
            BenchmarkId::new("full_clear_baseline", count),
            &count,
            |b, _| {
                b.iter(|| {
                    // Current snapshot rebuild — unavoidable after a commit.
                    let current = build_listing_table(&current_url, Arc::clone(&schema));
                    black_box(&current);
                    // Protected snapshot rebuilds — eliminated by the fix.
                    for url in &protected_urls {
                        let table = build_listing_table(url, Arc::clone(&schema));
                        black_box(&table);
                    }
                });
            },
        );

        // Lane B — current behavior: protected entries survive, only the
        // current snapshot rebuilds.
        group.bench_with_input(
            BenchmarkId::new("targeted_retain_protected", count),
            &count,
            |b, _| {
                b.iter(|| {
                    let current = build_listing_table(&current_url, Arc::clone(&schema));
                    black_box(&current);
                    for cached in &cached_protected {
                        // Cache hit — Arc::clone is all that's paid.
                        let table = Arc::clone(cached);
                        black_box(&table);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_scan_listing_cache_invalidation);
criterion_main!(benches);
