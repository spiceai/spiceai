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

//! The cost of applying one small record against the size of the state: `T`
//! tables of `F` data files each, then `N` records that each advance one
//! table's sequence number, round robin. Prints the apply latency tail.
//!
//! ```sh
//! cargo run --release -p cayenne-metastore-actions --example apply_bench -- <tables> <files> <samples> [in-place]
//! ```
//!
//! `in-place` measures [`DatasetState::apply_in_place`] instead of the atomic
//! [`DatasetState::apply`].

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "an experiment binary computing percentiles"
)]

use std::time::Instant;

use cayenne_metastore_actions::{
    Action, DatasetState, Kind, Record,
    proto::{AddFiles, AdvanceSeq, CreateTable, FileEntry},
};

fn arg(index: usize, default: usize) -> usize {
    std::env::args()
        .nth(index)
        .map_or(default, |value| value.parse().expect("a number"))
}

fn main() {
    let (tables, files, samples) = (arg(1, 20), arg(2, 10_000), arg(3, 10_000));
    let in_place = std::env::args().nth(4).as_deref() == Some("in-place");
    let mut state = DatasetState::default();
    for t in 0..tables {
        let table_id = format!("table-{t}");
        state
            .apply(&Record {
                actions: vec![
                    Action::new(Kind::CreateTable(CreateTable {
                        table_id: table_id.clone(),
                        name: format!("name-{t}"),
                        current_snapshot_id: "s".into(),
                        ..CreateTable::default()
                    })),
                    Action::new(Kind::AddFiles(AddFiles {
                        table_id,
                        snapshot_id: "s".into(),
                        files: (0..files)
                            .map(|f| FileEntry {
                                path: format!("data/{t}/{f:08}.vortex"),
                                rows: 8192,
                                size: 1 << 20,
                                min_seq: 0,
                                max_seq: 0,
                                digest: None,
                            })
                            .collect(),
                        upsert: false,
                    })),
                ],
            })
            .expect("load");
    }
    let mut micros = Vec::with_capacity(samples);
    for n in 0..samples {
        let record = Record {
            actions: vec![Action::new(Kind::AdvanceSeq(AdvanceSeq {
                table_id: format!("table-{}", n % tables),
                to: i64::try_from(n).expect("small"),
            }))],
        };
        let start = Instant::now();
        if in_place {
            state.apply_in_place(&record).expect("apply");
        } else {
            state.apply(&record).expect("apply");
        }
        micros.push(start.elapsed().as_secs_f64() * 1e6);
    }
    micros.sort_by(f64::total_cmp);
    let at = |q: f64| micros[((micros.len() as f64 * q).ceil() as usize).min(micros.len()) - 1];
    println!(
        "in_place={in_place} tables={tables} files_per_table={files} n={} p99_us={:.1} p99.9_us={:.1} max_us={:.1}",
        micros.len(),
        at(0.99),
        at(0.999),
        micros[micros.len() - 1]
    );
}
