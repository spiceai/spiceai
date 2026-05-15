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

//! Pure-CPU Criterion benchmark for [`cayenne::provider::compaction::pick_candidates`].
//!
//! The picker runs on the hot write path after every Vortex flush. This bench
//! validates that even for large directories the picker stays O(n log n) and
//! fast in absolute terms.

#![allow(clippy::expect_used)]

use std::hint::black_box;

use cayenne::provider::compaction::{CompactionPickerConfig, FileEntry, pick_candidates};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn synthetic_files(count: usize) -> Vec<FileEntry<String>> {
    // Distribute sizes across a range that exercises both Small and Mid tiers
    // for a 128 MiB target. Sizes cycle from 1 MiB to 100 MiB.
    (0..count)
        .map(|idx| {
            let mib = 1 + ((idx * 37) % 100) as u64;
            FileEntry {
                path: format!("data_{idx:06}.vortex"),
                size_bytes: mib * 1024 * 1024,
            }
        })
        .collect()
}

fn bench_pick_candidates(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_picker_pick_candidates");
    let cfg = CompactionPickerConfig::new(8, 32, 128 * 1024 * 1024);

    for &count in &[10_usize, 100, 1_000, 10_000] {
        let files = synthetic_files(count);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &files, |b, files| {
            b.iter(|| {
                let candidate = pick_candidates(black_box(files), black_box(&cfg));
                black_box(candidate);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pick_candidates);
criterion_main!(benches);
