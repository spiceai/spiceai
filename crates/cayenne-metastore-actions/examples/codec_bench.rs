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

//! What protobuf encoding costs beside the rest of a commit and a cold open.
//!
//! For a state of `T` tables with `F` data files each (every file with its
//! statistics), measures:
//! - one checkpoint-shaped commit record: encode and decode;
//! - the checkpoint root: building the root record from the state, encoding it,
//!   decoding it, and applying the decoded record to an empty state (the cold
//!   open's rebuild).
//!
//! ```sh
//! cargo run --release -p cayenne-metastore-actions --example codec_bench -- <tables> <files> <commit samples> <root samples>
//! ```

#![expect(
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "an experiment binary computing percentiles"
)]

use std::time::Instant;

use cayenne_metastore_actions::{
    Action, DatasetState, Kind, ObjRef, Record,
    proto::{
        AddFiles, AdvanceSourcePosition, CreateTable, FileEntry, FileStat, SetFileStats, SetGroup,
        SetTableStats,
    },
    state_as_root,
};
use prost::Message;

fn arg(index: usize, default: usize) -> usize {
    std::env::args()
        .nth(index)
        .map_or(default, |value| value.parse().expect("a number"))
}

fn file(t: usize, f: usize) -> (FileEntry, FileStat) {
    let path = format!("data/table-{t}/{f:08}-0193a1b2c3d4e5f6.vortex");
    (
        FileEntry {
            path: path.clone(),
            rows: 8192,
            size: 1 << 20,
            min_seq: i64::try_from(f).expect("small"),
            max_seq: i64::try_from(f).expect("small") + 8192,
            digest: None,
        },
        FileStat {
            snapshot_id: "s".into(),
            path,
            size: 1 << 20,
            rows: 8192,
            blob: Some(ObjRef::of(format!("{t}/{f}").as_bytes())),
        },
    )
}

/// One CDC checkpoint's record: a new file with its statistics, the protected
/// snapshot, the table statistics, and the source position.
fn checkpoint_record(t: usize, f: usize) -> Record {
    let table_id = format!("table-{t}");
    let (entry, stat) = file(t, f);
    Record {
        actions: vec![
            Action::new(Kind::AddFiles(AddFiles {
                table_id: table_id.clone(),
                snapshot_id: "s".into(),
                files: vec![entry],
                upsert: false,
            })),
            Action::new(Kind::SetFileStats(SetFileStats {
                table_id: table_id.clone(),
                stat: Some(stat),
            })),
            Action::new(Kind::SetGroup(SetGroup {
                table_id: table_id.clone(),
                snapshot_id: format!("g{f}"),
                seq: i64::try_from(f).expect("small"),
            })),
            Action::new(Kind::SetTableStats(SetTableStats {
                table_id,
                blob: Some(ObjRef::of(b"stats")),
                num_rows: 1_000_000,
                ndv: None,
                exact: true,
            })),
            Action::new(Kind::AdvanceSourcePosition(AdvanceSourcePosition {
                source: format!("spice_sys_applied_lsn/table-{t}"),
                partition: String::new(),
                position: br#"{"data":"{\"lsn\":123456789,\"source\":\"pg:5432/db/public.t\"}","updated_at":1793400000}"#.to_vec(),
                clear: false,
            })),
        ],
    }
}

fn tail(name: &str, mut micros: Vec<f64>) {
    micros.sort_by(f64::total_cmp);
    let n = micros.len();
    let at = |q: f64| {
        if (n as f64) * (1.0 - q) < 10.0 {
            "n/a".to_string()
        } else {
            format!("{:.1}", micros[((n as f64 * q).ceil() as usize).min(n) - 1])
        }
    };
    println!(
        "{name}: n={n} p99_us={} p99.9_us={} max_us={:.1}",
        at(0.99),
        at(0.999),
        micros[n - 1]
    );
}

fn time<T>(work: impl FnOnce() -> T) -> (T, f64) {
    let start = Instant::now();
    let out = work();
    (out, start.elapsed().as_secs_f64() * 1e6)
}

fn main() {
    let (tables, files, commits, roots) =
        (arg(1, 20), arg(2, 1000), arg(3, 100_000), arg(4, 1_000));
    let mut state = DatasetState::default();
    for t in 0..tables {
        let table_id = format!("table-{t}");
        let mut actions = vec![Action::new(Kind::CreateTable(CreateTable {
            table_id: table_id.clone(),
            name: format!("name-{t}"),
            current_snapshot_id: "s".into(),
            ..CreateTable::default()
        }))];
        let (entries, stats): (Vec<_>, Vec<_>) = (0..files).map(|f| file(t, f)).unzip();
        actions.push(Action::new(Kind::AddFiles(AddFiles {
            table_id: table_id.clone(),
            snapshot_id: "s".into(),
            files: entries,
            upsert: false,
        })));
        actions.extend(stats.into_iter().map(|stat| {
            Action::new(Kind::SetFileStats(SetFileStats {
                table_id: table_id.clone(),
                stat: Some(stat),
            }))
        }));
        state.apply_in_place(&Record { actions }).expect("load");
    }

    let record = checkpoint_record(0, files + 1);
    let size = record.encoded_len();
    let (mut encode, mut decode) = (Vec::new(), Vec::new());
    for _ in 0..commits {
        let (bytes, us) = time(|| record.encode_to_vec());
        encode.push(us);
        let (_, us) = time(|| Record::decode(bytes.as_slice()).expect("decode"));
        decode.push(us);
    }
    println!("tables={tables} files_per_table={files}");
    println!("commit record bytes={size}");
    tail("commit encode", encode);
    tail("commit decode", decode);

    let (mut build, mut encode, mut decode, mut apply) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut root_bytes = 0;
    for _ in 0..roots {
        let (root, us) = time(|| state_as_root(&state));
        build.push(us);
        let (bytes, us) = time(|| root.encode_to_vec());
        encode.push(us);
        root_bytes = bytes.len();
        let (decoded, us) = time(|| Record::decode(bytes.as_slice()).expect("decode"));
        decode.push(us);
        let (_, us) = time(|| {
            let mut opened = DatasetState::default();
            opened.apply_in_place(&decoded).expect("apply");
            opened
        });
        apply.push(us);
    }
    println!("root bytes={root_bytes}");
    tail("root build from state", build);
    tail("root encode", encode);
    tail("root decode", decode);
    tail("root apply to empty state", apply);
}
