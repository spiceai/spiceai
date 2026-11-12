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

//! Prototype microbenchmark for the object-native metastore's open decision
//! 3.4: what shifting a lost race's buffered `seq`s costs (option (a), "shift
//! the buffer"), against a retry's GET plus CAS.
//!
//! A shift re-stamps every key the losing writer's window touched at a higher
//! `seq`, in the structures that hold per-key `seq`s:
//! - the PK keyset's OCC stamps (`CachedPkKeyset::record_sequence`, per-key max);
//! - the deletion index (`DeletionIndex::extend_max`, per-key max on both sides).
//!
//! Mem-tier segments hold one `seq` pair each, so their shift is O(segments)
//! and is not measured. Run:
//!
//! ```sh
//! cargo test -p cayenne --release --lib seq_shift_bench -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use crate::provider::deletion_index::DeletionIndex;
use crate::provider::pk_index::{CachedPkKeyset, RowLocation, pk_digest};

fn percentile(sorted: &[Duration], p: f64) -> Option<Duration> {
    #[expect(clippy::cast_precision_loss, reason = "sample counts are small")]
    let beyond = sorted.len() as f64 * (1.0 - p);
    if beyond < 10.0 {
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "rank of a percentile"
    )]
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

fn report(label: &str, mut samples: Vec<Duration>) {
    samples.sort();
    let fmt = |d: Option<Duration>| {
        d.map_or_else(
            || "n/a".to_string(),
            |d| format!("{:.3}", d.as_secs_f64() * 1e3),
        )
    };
    println!(
        "SHIFT {label} n={} p50_ms={} p99_ms={} p999_ms={} max_ms={}",
        samples.len(),
        fmt(percentile(&samples, 0.5)),
        fmt(percentile(&samples, 0.99)),
        fmt(percentile(&samples, 0.999)),
        fmt(samples.last().copied())
    );
}

/// Committed state the window shifts against: `base` keys already in the
/// keyset and the deletion index.
#[test]
#[ignore = "microbenchmark; run with --ignored --release"]
fn seq_shift_bench() {
    let base: i64 = 2_000_000;
    let delta = 1_000_000_i64;
    for (window, samples) in [
        (10_000_usize, 11_000_usize),
        (100_000, 1_100),
        (1_000_000, 1_100),
    ] {
        let window_i64 = i64::try_from(window).expect("window fits i64");
        // The keyset: base committed keys and the window's keys, each stamped
        // at a `seq`; the window's keys get re-stamped by the shift.
        let converter =
            crate::row_converter::RowConverter::new(vec![crate::row_converter::SortField::new(
                arrow::datatypes::DataType::Int64,
            )])
            .expect("row converter");
        let column: arrow::array::ArrayRef = std::sync::Arc::new(
            arrow::array::Int64Array::from_iter_values(0..base + window_i64),
        );
        let rows = converter.convert_columns(&[column]).expect("rows");
        let mut keyset = CachedPkKeyset::with_capacity(rows.num_rows());
        let mut window_digests = Vec::with_capacity(window);
        for (key, row) in rows.iter().enumerate() {
            let owned = row.owned();
            let digest = pk_digest(&owned);
            keyset.insert_with_digest(digest, owned, RowLocation::MEM_TIER);
            keyset.record_sequence(digest, i64::try_from(key).expect("key"));
            if i64::try_from(key).expect("key") >= base {
                window_digests.push(digest);
            }
        }

        // The deletion index: base committed tombstones; the window upserted
        // every one of its keys (a delete at seq s and a re-insert at s + 1).
        let committed = DeletionIndex::empty().extend_max_deletes((0..base).map(|k| (k, k)));
        let window_conflicts: Vec<(i64, i64, i64)> = (0..window_i64)
            .map(|i| (base / 2 + i, base + 2 * i, base + 2 * i + 1))
            .collect();
        let buffered = committed.extend_max(std::iter::empty(), window_conflicts.iter().copied());

        let mut keyset_samples = Vec::with_capacity(samples);
        let mut deletion_samples = Vec::with_capacity(samples);
        let mut total_samples = Vec::with_capacity(samples);
        for round in 0..samples {
            let shift = delta * (i64::try_from(round).expect("round") + 1);
            let start = Instant::now();
            for (offset, digest) in window_digests.iter().enumerate() {
                keyset.record_sequence(*digest, base + i64::try_from(offset).expect("o") + shift);
            }
            let keyset_done = Instant::now();
            let shifted = buffered.extend_max(
                std::iter::empty(),
                window_conflicts
                    .iter()
                    .map(|(pk, d, i)| (*pk, d + shift, i + shift)),
            );
            let end = Instant::now();
            std::hint::black_box(&shifted);
            keyset_samples.push(keyset_done - start);
            deletion_samples.push(end - keyset_done);
            total_samples.push(end - start);
        }
        report(
            &format!("window={window} base={base} keyset"),
            keyset_samples,
        );
        report(
            &format!("window={window} base={base} deletion_index"),
            deletion_samples,
        );
        report(&format!("window={window} base={base} total"), total_samples);
    }
}
