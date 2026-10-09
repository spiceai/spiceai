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
    clippy::doc_markdown,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

//! `IN`-list probes of a tiered index: 1M rows in 1, 4 or 16 runs, probed
//! with lists of 16, 256 and 2048 keys (half present), one key at a time
//! against [`IndexView::candidates_batch`]. Reports ns per key at the calls'
//! p50 / p99.
//!
//! `cargo bench -p key-index --bench in_list`; `BENCH_SHAPES` narrows the
//! key shapes.

use std::time::Instant;

use key_index::KeyEncoder;
use key_index::tiered::{IndexView, RunBuilder, TieredIndex};

mod common;
use common::{Counting, Shape, ids, mix, p50_p99};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const ROWS: usize = 1_000_000;

fn main() {
    println!(
        "| key type | runs | keys per list | one at a time, ns/key p50 / p99 | batched, ns/key p50 / p99 |"
    );
    println!("|---|---|---|---|---|");
    for shape in Shape::selected() {
        for runs in [1, 4, 16] {
            run(shape, runs);
        }
    }
}

fn run(shape: Shape, runs: usize) {
    let all: Vec<u64> = ids(ROWS * 2);
    let (stored, absent) = all.split_at(ROWS);
    let (fields, _) = shape.columns(&[0]);
    let encoder = KeyEncoder::new(fields).expect("key types");
    let index = TieredIndex::new(encoder.clone());
    let per_run = ROWS / runs;
    let mut built = Vec::new();
    for r in 0..runs {
        let (_, columns) = shape.columns(&stored[r * per_run..(r + 1) * per_run]);
        let mut builder = RunBuilder::new(encoder.clone());
        builder
            .add_batch(&format!("file-{r}"), 0, &columns)
            .expect("add");
        built.push(builder.finish().expect("run"));
    }
    index.publish(built, &[]);
    let view = index.view();
    let present = shape.keys(stored);
    let missing = shape.keys(absent);
    for list in [16, 256, 2048] {
        let calls = (200_000 / list).max(50);
        let (mut single_ns, mut batch_ns) = (Vec::new(), Vec::new());
        for c in 0..calls {
            let keys: Vec<&[u8]> = (0..list)
                .map(|i| {
                    let pick = (mix((c * list + i) as u64 ^ 0x1515) % ROWS as u64) as usize;
                    if i % 2 == 0 {
                        present[pick].as_slice()
                    } else {
                        missing[pick].as_slice()
                    }
                })
                .collect();
            // Whichever runs second finds the keys' runs already in cache, so
            // the two take turns going first.
            let mut single = 0_usize;
            let mut batched = 0_usize;
            let mut time_single = || {
                let start = Instant::now();
                for key in &keys {
                    view.candidates(key, |_| single += 1);
                }
                start.elapsed().as_nanos() as f64 / list as f64
            };
            let mut time_batched = || {
                let start = Instant::now();
                batch(&view, &keys, &mut batched);
                start.elapsed().as_nanos() as f64 / list as f64
            };
            if c % 2 == 0 {
                single_ns.push(time_single());
                batch_ns.push(time_batched());
            } else {
                batch_ns.push(time_batched());
                single_ns.push(time_single());
            }
            assert_eq!(
                single, batched,
                "the batch finds what the keys find one by one"
            );
            assert_eq!(single, list / 2, "every present key has one row");
        }
        println!(
            "| {} | {runs} | {list} | {} | {} |",
            shape.name(),
            p50_p99(&mut single_ns, 0),
            p50_p99(&mut batch_ns, 0),
        );
    }
}

fn batch(view: &IndexView, keys: &[&[u8]], found: &mut usize) {
    view.candidates_batch(keys, |_, _| *found += 1);
}
