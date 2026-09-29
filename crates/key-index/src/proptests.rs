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

//! Property tests: generated operation sequences against a model of the rows
//! an index must answer, run builds against a naive sort, hostile persisted
//! bytes, and varint round-trips. A failure shrinks to a minimal case.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use proptest::prelude::*;

use crate::tiered::{Candidate, IndexRun, RunBuilder, TieredIndex, UNSEEN_GRACE};
use crate::{KeyEncoder, KeyField, varint};

fn encoder() -> KeyEncoder {
    KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key")
}

fn column(keys: &[i64]) -> Vec<ArrayRef> {
    vec![Arc::new(Int64Array::from(keys.to_vec()))]
}

fn encoded(key: i64) -> Vec<u8> {
    let columns = column(&[key]);
    let encoder = encoder();
    let bound = encoder.bind(&columns).expect("bind");
    let mut out = Vec::new();
    bound.encode_row(0, &mut out);
    out
}

/// One operation on an index. Files are named by the order they are created.
#[derive(Debug, Clone)]
enum Op {
    /// A write of new files, each holding these keys at positions `0..`, also
    /// retiring the live files at these indices into the live list.
    Publish {
        files: Vec<Vec<i64>>,
        retire: Vec<usize>,
    },
    /// A read-back of new files, live only where the flag is set.
    PublishVisible {
        files: Vec<(Vec<i64>, bool)>,
    },
    /// A reconcile against the live files, dropping those at these indices.
    Reconcile {
        drop: Vec<usize>,
    },
    /// A reconcile against only the live files a reconcile has already seen,
    /// so every file written but never seen counts toward its grace.
    ReconcileSeenOnly,
    MergeStep,
    MergeAll,
    RebuildFilter,
}

fn keys() -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0_i64..24, 0..12)
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (prop::collection::vec(keys(), 1..3), prop::collection::vec(0_usize..16, 0..3))
            .prop_map(|(files, retire)| Op::Publish { files, retire }),
        2 => prop::collection::vec((keys(), any::<bool>()), 1..3)
            .prop_map(|files| Op::PublishVisible { files }),
        3 => prop::collection::vec(0_usize..16, 0..3).prop_map(|drop| Op::Reconcile { drop }),
        3 => Just(Op::ReconcileSeenOnly),
        1 => Just(Op::MergeStep),
        1 => Just(Op::MergeAll),
        1 => Just(Op::RebuildFilter),
    ]
}

/// What a file is in the model: its rows, whether it is live, and whether a
/// reconcile has seen it live (or how many reconciles have not).
struct ModelFile {
    keys: Vec<i64>,
    live: bool,
    seen: Option<u8>,
}

/// Applies `ops` to an index over `encoder` and to the model, and after each
/// op compares every key's candidates with the model's live rows.
fn run_ops(ops: &[Op], encoder: &KeyEncoder, exact: bool) -> Result<(), TestCaseError> {
    let index = TieredIndex::new(encoder.clone());
    let mut model: BTreeMap<String, ModelFile> = BTreeMap::new();
    let mut next_file = 0;
    let mut name = || {
        next_file += 1;
        format!("f{next_file}")
    };
    let build = |file: &str, keys: &[i64]| -> IndexRun {
        let mut builder = RunBuilder::new(encoder.clone());
        builder.add_file(file).expect("file");
        if !keys.is_empty() {
            builder.add_batch(file, 0, &column(keys)).expect("batch");
        }
        builder.finish().expect("finish")
    };
    for (step, op) in ops.iter().enumerate() {
        let live: Vec<String> = model
            .iter()
            .filter(|(_, file)| file.live)
            .map(|(name, _)| name.clone())
            .collect();
        match op {
            Op::Publish { files, retire } => {
                let retired: Vec<String> = retire
                    .iter()
                    .filter_map(|&i| live.get(i).cloned())
                    .collect();
                let mut runs = Vec::new();
                for keys in files {
                    let file = name();
                    runs.push(build(&file, keys));
                    model.insert(
                        file,
                        ModelFile {
                            keys: keys.clone(),
                            live: true,
                            seen: Some(0),
                        },
                    );
                }
                let retired_refs: Vec<&str> = retired.iter().map(String::as_str).collect();
                index.publish(runs, &retired_refs);
                for file in &retired {
                    if let Some(entry) = model.get_mut(file) {
                        entry.live = false;
                    }
                }
            }
            Op::PublishVisible { files } => {
                let mut runs = Vec::new();
                let mut visible: HashSet<String> = live.iter().cloned().collect();
                for (keys, is_live) in files {
                    let file = name();
                    runs.push(build(&file, keys));
                    if *is_live {
                        visible.insert(file.clone());
                    }
                    model.insert(
                        file,
                        ModelFile {
                            keys: keys.clone(),
                            live: *is_live,
                            seen: None,
                        },
                    );
                }
                let visible_refs: HashSet<&str> = visible.iter().map(String::as_str).collect();
                index.publish_visible(runs, &visible_refs);
            }
            Op::Reconcile { .. } | Op::ReconcileSeenOnly => {
                let dropped: HashSet<String> = if let Op::Reconcile { drop } = op {
                    drop.iter().filter_map(|&i| live.get(i).cloned()).collect()
                } else {
                    // Never-seen files are left out of the live set.
                    live.iter()
                        .filter(|file| model.get(*file).is_some_and(|entry| entry.seen.is_some()))
                        .cloned()
                        .collect()
                };
                let now: HashSet<&str> = live
                    .iter()
                    .filter(|file| !dropped.contains(*file))
                    .map(String::as_str)
                    .collect();
                index.reconcile(&now);
                for (file, entry) in &mut model {
                    if !entry.live {
                        continue;
                    }
                    let present = now.contains(file.as_str());
                    let next = if present || entry.seen.is_none() {
                        None
                    } else {
                        entry.seen.map(|unseen| unseen.saturating_add(1))
                    };
                    let was_seen = entry.seen.is_none();
                    entry.live =
                        present || (!was_seen && next.is_some_and(|unseen| unseen < UNSEEN_GRACE));
                    entry.seen = next;
                }
            }
            Op::MergeStep => {
                index.merge_step().expect("merge step");
            }
            Op::MergeAll => {
                index.merge_all().expect("merge all");
            }
            Op::RebuildFilter => {
                index.rebuild_overfull_filter();
            }
        }
        for key in -1..25_i64 {
            let expected: BTreeSet<(String, u64)> = model
                .iter()
                .filter(|(_, file)| file.live)
                .flat_map(|(name, file)| {
                    file.keys
                        .iter()
                        .enumerate()
                        .filter(|&(_, &k)| k == key)
                        .map(|(position, _)| (name.clone(), position as u64))
                        .collect::<Vec<_>>()
                })
                .collect();
            let mut got = BTreeSet::new();
            index.candidates(&encoded(key), |Candidate { file, position }| {
                got.insert((file.to_string(), position));
            });
            if exact {
                prop_assert_eq!(&got, &expected, "step {} ({:?}), key {}", step, op, key);
            } else {
                prop_assert!(
                    expected.is_subset(&got),
                    "step {} ({:?}), key {}: candidates {:?} miss rows of {:?}",
                    step,
                    op,
                    key,
                    got,
                    expected
                );
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Every key's candidates are exactly its rows in the live files, after
    /// every step of any sequence of writes, read-backs, reconciles, merges
    /// and filter rebuilds.
    #[test]
    fn candidates_match_a_model(ops in prop::collection::vec(op(), 1..30)) {
        run_ops(&ops, &encoder(), true)?;
    }

    /// With 2-bit words, keys share words: candidates are then a superset of
    /// every key's live rows, never missing one.
    #[test]
    fn colliding_candidates_never_miss_a_row(ops in prop::collection::vec(op(), 1..30)) {
        run_ops(&ops, &encoder().with_word_bits(2), false)?;
    }

    /// A run holds exactly the rows added to it, in word order, whatever the
    /// batches, files, positions and repeated keys.
    #[test]
    fn a_run_holds_exactly_its_rows(
        files in prop::collection::vec(
            prop::collection::vec((0_i64..40, 0_u64..1 << 40), 0..60),
            1..4,
        )
    ) {
        let encoder = encoder();
        let mut builder = RunBuilder::new(encoder.clone());
        let mut expected: BTreeSet<(u64, String, u64)> = BTreeSet::new();
        for (file_no, rows) in files.iter().enumerate() {
            let file = format!("f{file_no}");
            builder.add_file(&file).expect("file");
            // A row has one position in its file.
            let mut used = HashSet::new();
            let rows: Vec<(i64, u64)> = rows.iter().copied().filter(|&(_, p)| used.insert(p)).collect();
            if rows.is_empty() {
                continue;
            }
            let keys: Vec<i64> = rows.iter().map(|&(k, _)| k).collect();
            let positions: Vec<u64> = rows.iter().map(|&(_, p)| p).collect();
            builder.add_batch_at(&file, &positions, &column(&keys)).expect("batch");
            for &(key, position) in &rows {
                expected.insert((encoder.key_word(&encoded(key)), file.clone(), position));
            }
        }
        let run = builder.finish().expect("finish");
        let mut got = Vec::new();
        run.for_each_row(|word, file, position| got.push((word, file.to_string(), position)));
        prop_assert!(got.windows(2).all(|pair| pair[0].0 <= pair[1].0), "rows not in word order");
        let got: BTreeSet<(u64, String, u64)> = got.into_iter().collect();
        prop_assert_eq!(got, expected);
    }

    /// Arbitrary bytes are rejected or read, never a panic.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = IndexRun::from_bytes(&bytes);
    }

    /// A run's bytes changed anywhere and then given a valid checksum (so the
    /// structural checks, not the checksum, have to catch it) are rejected or
    /// read into a run whose lookups do not panic.
    #[test]
    fn resealed_corruptions_never_panic(
        keys in prop::collection::vec(0_i64..40, 1..80),
        at in any::<prop::sample::Index>(),
        flip in 1_u8..,
    ) {
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch("f", 0, &column(&keys)).expect("batch");
        let bytes = builder.finish().expect("finish").to_bytes();
        let mut body = bytes[..bytes.len() - 8].to_vec();
        let i = 12 + at.index(body.len() - 12);
        body[i] ^= flip;
        crate::persist::seal(&mut body);
        if let Ok(run) = IndexRun::from_bytes(&body) {
            for key in 0..40 {
                run.lookup(encoder().key_word(&encoded(key)), |_, _| {});
            }
            run.for_each_row(|_, _, _| {});
        }
    }

    /// Every `u64` round-trips through a varint.
    #[test]
    fn varints_round_trip(values in prop::collection::vec(any::<u64>(), 0..64)) {
        let mut bytes = Vec::new();
        for &value in &values {
            varint::put(&mut bytes, value);
        }
        let mut at = 0;
        for &value in &values {
            prop_assert_eq!(varint::get(&bytes, &mut at), Some(value));
        }
        prop_assert_eq!(at, bytes.len());
    }
}
