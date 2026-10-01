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

#![allow(
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]

//! How long a compaction-sized run takes to finish, and to merge, split by
//! phase.
//!
//! `RUN_FINISH_KEY`:
//! - `service` (default): one `Utf8` key, `svc-` and 32 hex digits of a hash
//!   of the id, like the `spiced` freshness bench's `'svc-' || md5(id)`.
//! - `tenant_service`: `(id % 997 i64, SV{id:032x} utf8)`.
//! - `id`: the id as one `Int64`, an exact word.
//!
//! Rows are added in a shuffled order, `RUN_FINISH_FILE_ROWS` (default
//! 1,192,000) to a file. `RUN_FINISH_ROWS` is a comma-separated list of sizes
//! (default `1192000,5960000,17880000`), `RUN_FINISH_REPS` (default 3).
//!
//! `RUN_FINISH_ORDER=key` adds the rows in key order instead, so the sorted
//! segments do not interleave (a time-ordered append indexed on its time).
//!
//! `RUN_FINISH_MODE`:
//! - `finish` (default): one write's run over all the files.
//! - `merge`: one run per file, merged to one run by repeated `merge_step`.
//! - `restore`: one write's run over all the files, read back from its
//!   persisted bytes (`IndexRun::from_bytes`), as a reopened table does.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::DataType;
use key_index::tiered::{IndexRun, RunBuilder, TieredIndex};
use key_index::{KeyEncoder, KeyField};

mod common;
use common::{env, mix};

/// Present keys each finish rep probes.
const PROBES: usize = 200_000;

fn tenant_service() -> bool {
    std::env::var("RUN_FINISH_KEY").is_ok_and(|key| key == "tenant_service")
}

/// A single `Int64` key, the id itself: an exact word.
fn id_key() -> bool {
    std::env::var("RUN_FINISH_KEY").is_ok_and(|key| key == "id")
}

fn encoder() -> KeyEncoder {
    let fields = if id_key() {
        vec![KeyField::new(DataType::Int64, false)]
    } else if tenant_service() {
        vec![
            KeyField::new(DataType::Int64, false),
            KeyField::new(DataType::Utf8, false),
        ]
    } else {
        vec![KeyField::new(DataType::Utf8, false)]
    };
    KeyEncoder::new(fields).expect("encoder")
}

/// A shuffled permutation of `0..n`, deterministic.
fn shuffled(n: usize) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..i64::try_from(n).expect("fits")).collect();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        ids.swap(i, (state % (i as u64 + 1)) as usize);
    }
    ids
}

fn columns(chunk: &[i64]) -> Vec<ArrayRef> {
    if id_key() {
        return vec![Arc::new(Int64Array::from(chunk.to_vec()))];
    }
    if tenant_service() {
        return vec![
            Arc::new(Int64Array::from(
                chunk.iter().map(|id| id % 997).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                chunk
                    .iter()
                    .map(|id| format!("SV{id:032x}"))
                    .collect::<Vec<_>>(),
            )),
        ];
    }
    vec![Arc::new(StringArray::from(
        chunk
            .iter()
            .map(|&id| {
                let id = id.cast_unsigned();
                format!("svc-{:016x}{:016x}", mix(id), mix(id ^ 0x5555))
            })
            .collect::<Vec<_>>(),
    ))]
}

/// Adds `ids` to `builder` as rows of `file`, 8,192 to a batch.
fn add(builder: &mut RunBuilder, file: &str, ids: &[i64]) {
    for (chunk_no, chunk) in ids.chunks(8192).enumerate() {
        builder
            .add_batch(file, (chunk_no * 8192) as u64, &columns(chunk))
            .expect("batch");
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// Min / max over the reps, and ns per row at the min.
fn stat(samples: &mut [Duration], rows: usize) -> String {
    samples.sort_unstable();
    format!(
        "{:>8.1} / {:>8.1} ms ({:>5.0} ns/row)",
        ms(samples[0]),
        ms(samples[samples.len() - 1]),
        samples[0].as_nanos() as f64 / rows as f64
    )
}

fn finish(ids: &[i64], file_rows: usize, reps: usize) {
    let rows = ids.len();
    let (mut add_time, mut total, mut publish, mut probe) = (vec![], vec![], vec![], vec![]);
    let (mut working, mut resident) = (0, 0);
    // Present keys to probe, the same every rep.
    let probes: Vec<Vec<u8>> = (0..PROBES)
        .map(|i| {
            let id = ids[(i * 7_919) % rows];
            let columns = columns(&[id]);
            let encoder = encoder();
            let mut key = Vec::new();
            encoder
                .bind(&columns)
                .expect("bind")
                .encode_row(0, &mut key);
            key
        })
        .collect();
    for _ in 0..reps {
        let started = Instant::now();
        let mut builder = RunBuilder::new(encoder());
        for (file_no, file) in ids.chunks(file_rows).enumerate() {
            add(&mut builder, &format!("f{file_no}.vortex"), file);
        }
        add_time.push(started.elapsed());
        working = builder.heap_bytes();
        let started = Instant::now();
        let run = builder.finish().expect("finish");
        total.push(started.elapsed());
        assert_eq!(run.len(), rows);
        resident = run.heap_bytes();
        // Publishing it into an index, as a write does before it is visible.
        let index = TieredIndex::new(encoder());
        let started = Instant::now();
        index.publish(vec![run], &[]);
        publish.push(started.elapsed());
        let mut found = 0;
        let started = Instant::now();
        for key in &probes {
            index.candidates(key, |_| found += 1);
        }
        probe.push(started.elapsed());
        assert!(found >= PROBES, "every probed key is present");
    }
    let mb = |bytes: usize| bytes as f64 / 1e6;
    println!(
        "finish rows={rows} files={} reps={reps}  (min / max)",
        rows.div_ceil(file_rows)
    );
    println!(
        "  build working memory          {:.1} MB ({:.1} B/row)",
        mb(working),
        working as f64 / rows as f64
    );
    println!(
        "  run resident                  {:.1} MB ({:.1} B/row)",
        mb(resident),
        resident as f64 / rows as f64
    );
    println!(
        "  add_batch                     {}",
        stat(&mut add_time, rows)
    );
    println!("  finish                        {}", stat(&mut total, rows));
    println!(
        "  publish into an empty index   {}",
        stat(&mut publish, rows)
    );
    println!(
        "  {PROBES} probes of present keys  {}",
        stat(&mut probe, PROBES)
    );
}

fn merge(ids: &[i64], file_rows: usize, reps: usize) {
    let rows = ids.len();
    let mut per_file = Vec::new();
    let mut merge_total = Vec::new();
    let mut merge_all = Vec::new();
    let mut steps = 0;
    let mut left = 0;
    for _ in 0..reps {
        let index = TieredIndex::new(encoder());
        let per_file_runs = || -> Vec<_> {
            ids.chunks(file_rows)
                .enumerate()
                .map(|(file_no, file)| {
                    let mut builder = RunBuilder::new(encoder());
                    add(&mut builder, &format!("f{file_no}.vortex"), file);
                    builder.finish().expect("finish")
                })
                .collect()
        };
        let started = Instant::now();
        let runs = per_file_runs();
        per_file.push(started.elapsed());
        index.publish(runs, &[]);
        let started = Instant::now();
        steps = 0;
        while index.merge_step().expect("merge") {
            steps += 1;
        }
        merge_total.push(started.elapsed());
        left = index.view().runs();
        let all = TieredIndex::new(encoder());
        all.publish(per_file_runs(), &[]);
        let started = Instant::now();
        all.merge_all().expect("merge all");
        merge_all.push(started.elapsed());
        assert_eq!(all.view().runs(), 1);
    }
    println!(
        "merge rows={rows} runs={} reps={reps}  (min / max)",
        rows.div_ceil(file_rows)
    );
    println!(
        "  per-file runs, add + finish   {}",
        stat(&mut per_file, rows)
    );
    println!(
        "  merge_step policy             {}  {steps} steps, stops at {left} runs",
        stat(&mut merge_total, rows)
    );
    println!(
        "  merge_all (one pass)          {}",
        stat(&mut merge_all, rows)
    );
}

/// Replays the initial load's publishes: five 1.19M-row appends, then a
/// compaction's run over all rows so far that retires every earlier file, three
/// times, then the appends that follow; reports each publish's time.
fn load(file_rows: usize) {
    let run_over = |first: usize, rows: usize, file: &str| {
        let ids: Vec<i64> = (first..first + rows)
            .map(|id| i64::try_from(id).expect("fits"))
            .collect();
        let mut builder = RunBuilder::new(encoder());
        add(&mut builder, file, &ids);
        builder.finish().expect("finish")
    };
    let index = TieredIndex::new(encoder());
    let mut files: Vec<String> = Vec::new();
    let mut rows = 0;
    for compaction in 0..3 {
        for append in 0..5 {
            let file = format!("a{compaction}-{append}.vortex");
            let run = run_over(rows, file_rows, &file);
            rows += file_rows;
            let started = Instant::now();
            index.publish(vec![run], &[]);
            println!(
                "  publish append   {:>9} rows: {:>7.1} ms",
                file_rows,
                ms(started.elapsed())
            );
            files.push(file);
        }
        let file = format!("c{compaction}.vortex");
        let run = run_over(0, rows, &file);
        // As Cayenne does: the rewrite's run is published before it is
        // visible, and its inputs retire at the next reconcile.
        let retired: Vec<&str> = files.iter().map(String::as_str).collect();
        let started = Instant::now();
        index.publish(vec![run], &[]);
        println!(
            "  publish rewrite  {rows:>9} rows: {:>7.1} ms",
            ms(started.elapsed())
        );
        let started = Instant::now();
        index.publish(vec![], &retired);
        println!(
            "  retire its inputs               {:>7.1} ms",
            ms(started.elapsed())
        );
        files = vec![file];
    }
}

/// Reads one write's run back from its persisted bytes `reps` times.
fn restore(ids: &[i64], file_rows: usize, reps: usize) {
    let mut builder = RunBuilder::new(encoder());
    for (file, chunk) in ids.chunks(file_rows).enumerate() {
        add(&mut builder, &format!("f{file}.vortex"), chunk);
    }
    let bytes = builder.finish().expect("finish").to_bytes();
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        let run = IndexRun::from_bytes(&bytes).expect("restore");
        samples.push(started.elapsed());
        assert_eq!(run.len(), ids.len());
    }
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    println!(
        "restore {:>9} rows, {:>6.1} MB: p50 {:>7.2} ms, p99 {:>7.2} ms, max {:>7.2} ms ({:.1} ns/row at p99)",
        ids.len(),
        bytes.len() as f64 / 1e6,
        ms(at(0.5)),
        ms(at(0.99)),
        ms(samples[samples.len() - 1]),
        at(0.99).as_nanos() as f64 / ids.len() as f64
    );
}

fn main() {
    let sizes: Vec<usize> = std::env::var("RUN_FINISH_ROWS")
        .unwrap_or_else(|_| "1192000,5960000,17880000".to_string())
        .split(',')
        .map(|size| size.trim().parse().expect("row count"))
        .collect();
    let reps = env("RUN_FINISH_REPS", 3);
    let file_rows = env("RUN_FINISH_FILE_ROWS", 1_192_000);
    let mode = std::env::var("RUN_FINISH_MODE").unwrap_or_else(|_| "finish".to_string());
    let key_order = std::env::var("RUN_FINISH_ORDER").is_ok_and(|order| order == "key");
    for rows in sizes {
        let mut ids = shuffled(rows);
        if key_order {
            let mut keyed: Vec<(Vec<u8>, i64)> = ids
                .chunks(8192)
                .flat_map(|chunk| {
                    let encoder = encoder();
                    let columns = columns(chunk);
                    let bound = encoder.bind(&columns).expect("bind");
                    (0..chunk.len())
                        .map(|row| {
                            let mut key = Vec::new();
                            bound.encode_row(row, &mut key);
                            (key, chunk[row])
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            keyed.sort_unstable();
            ids = keyed.into_iter().map(|(_, id)| id).collect();
        }
        match mode.as_str() {
            "load" => {
                load(file_rows);
                return;
            }
            "merge" => merge(&ids, file_rows, reps),
            "restore" => restore(&ids, file_rows, reps),
            _ => finish(&ids, file_rows, reps),
        }
    }
}
