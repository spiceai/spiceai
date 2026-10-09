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
//! bytes, and varint round-trips.
//!
//! Each test runs a fixed number of seeded cases, and a failure names its
//! seed and inputs, so it replays exactly. `KEY_INDEX_PROPTEST_SCALE`
//! multiplies every test's case count (default 1).

use std::collections::{BTreeMap, BTreeSet, HashSet};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::test_support::{column, encoded, encoder};
use crate::tiered::{Candidate, IndexRun, RunBuilder, TieredIndex, UNSEEN_GRACE};
use crate::{KeyEncoder, varint};

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

/// Cases each test runs, times `KEY_INDEX_PROPTEST_SCALE`.
fn cases(base: u64) -> u64 {
    let scale = std::env::var("KEY_INDEX_PROPTEST_SCALE")
        .ok()
        .and_then(|scale| scale.parse::<u64>().ok())
        .unwrap_or(1);
    (base * scale).max(1)
}

/// Runs `case` for each of `cases(base)` seeds with an RNG seeded from it,
/// naming the seed of a case that panics so it replays exactly.
fn for_seeds(base: u64, mut case: impl FnMut(u64, &mut StdRng)) {
    for seed in 0..cases(base) {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            case(seed, &mut StdRng::seed_from_u64(seed));
        }));
        if let Err(panic) = outcome {
            eprintln!("failing seed: {seed}");
            std::panic::resume_unwind(panic);
        }
    }
}

/// `len` in `lengths` values drawn by `value`.
fn vec_of<T>(
    rng: &mut StdRng,
    lengths: std::ops::Range<usize>,
    mut value: impl FnMut(&mut StdRng) -> T,
) -> Vec<T> {
    let len = rng.random_range(lengths);
    (0..len).map(|_| value(rng)).collect()
}

fn keys(rng: &mut StdRng) -> Vec<i64> {
    vec_of(rng, 0..12, |rng| rng.random_range(0_i64..24))
}

/// One operation, weighted 4:2:3:3:1:1:1 in the order of [`Op`]'s variants.
fn op(rng: &mut StdRng) -> Op {
    match rng.random_range(0..15) {
        0..4 => Op::Publish {
            files: vec_of(rng, 1..3, keys),
            retire: vec_of(rng, 0..3, |rng| rng.random_range(0_usize..16)),
        },
        4..6 => Op::PublishVisible {
            files: vec_of(rng, 1..3, |rng| (keys(rng), rng.random_bool(0.5))),
        },
        6..9 => Op::Reconcile {
            drop: vec_of(rng, 0..3, |rng| rng.random_range(0_usize..16)),
        },
        9..12 => Op::ReconcileSeenOnly,
        12 => Op::MergeStep,
        13 => Op::MergeAll,
        _ => Op::RebuildFilter,
    }
}

fn ops(rng: &mut StdRng) -> Vec<Op> {
    vec_of(rng, 1..30, op)
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
fn run_ops(ops: &[Op], encoder: &KeyEncoder, exact: bool) -> Result<(), String> {
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
            if exact && got != expected {
                return Err(format!(
                    "step {step} ({op:?}), key {key}: candidates {got:?}, expected {expected:?}"
                ));
            }
            if !expected.is_subset(&got) {
                return Err(format!(
                    "step {step} ({op:?}), key {key}: candidates {got:?} miss rows of {expected:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Every key's candidates are exactly its rows in the live files, after every
/// step of any sequence of writes, read-backs, reconciles, merges and filter
/// rebuilds.
#[test]
fn candidates_match_a_model() {
    for_seeds(256, |seed, rng| {
        let ops = ops(rng);
        if let Err(failure) = run_ops(&ops, &encoder(), true) {
            panic!("seed {seed}: {failure}\nops: {ops:?}");
        }
    });
}

/// With 2-bit words, keys share words: candidates are then a superset of
/// every key's live rows, never missing one.
#[test]
fn colliding_candidates_never_miss_a_row() {
    for_seeds(256, |seed, rng| {
        let ops = ops(rng);
        if let Err(failure) = run_ops(&ops, &encoder().with_word_bits(2), false) {
            panic!("seed {seed}: {failure}\nops: {ops:?}");
        }
    });
}

/// A run holds exactly the rows added to it, in word order, whatever the
/// batches, files, positions and repeated keys.
#[test]
fn a_run_holds_exactly_its_rows() {
    let encoder = encoder();
    for_seeds(256, |seed, rng| {
        let files = vec_of(rng, 1..4, |rng| {
            vec_of(rng, 0..60, |rng| {
                (
                    rng.random_range(0_i64..40),
                    rng.random_range(0_u64..1 << 40),
                )
            })
        });
        let mut builder = RunBuilder::new(encoder.clone());
        let mut expected: BTreeSet<(u64, String, u64)> = BTreeSet::new();
        for (file_no, rows) in files.iter().enumerate() {
            let file = format!("f{file_no}");
            builder.add_file(&file).expect("file");
            // A row has one position in its file.
            let mut used = HashSet::new();
            let rows: Vec<(i64, u64)> = rows
                .iter()
                .copied()
                .filter(|&(_, p)| used.insert(p))
                .collect();
            if rows.is_empty() {
                continue;
            }
            let keys: Vec<i64> = rows.iter().map(|&(k, _)| k).collect();
            let positions: Vec<u64> = rows.iter().map(|&(_, p)| p).collect();
            builder
                .add_batch_at(&file, &positions, &column(&keys))
                .expect("batch");
            for &(key, position) in &rows {
                expected.insert((encoder.key_word(&encoded(key)), file.clone(), position));
            }
        }
        let run = builder.finish().expect("finish");
        let mut got = Vec::new();
        run.for_each_row(|word, file, position| got.push((word, file.to_string(), position)));
        assert!(
            got.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "seed {seed}: rows not in word order"
        );
        let got: BTreeSet<(u64, String, u64)> = got.into_iter().collect();
        assert_eq!(got, expected, "seed {seed}");
    });
}

/// Arbitrary bytes are rejected or read, never a panic.
#[test]
fn arbitrary_bytes_never_panic() {
    for_seeds(256, |_seed, rng| {
        let bytes = vec_of(rng, 0..512, RngExt::random::<u8>);
        let _ = IndexRun::from_bytes(&bytes);
    });
}

/// A run's bytes changed anywhere and then given a valid checksum (so the
/// structural checks, not the checksum, have to catch it) are rejected or
/// read into a run whose lookups do not panic.
#[test]
fn resealed_corruptions_never_panic() {
    for_seeds(256, |_seed, rng| {
        let keys = vec_of(rng, 1..80, |rng| rng.random_range(0_i64..40));
        let mut builder = RunBuilder::new(encoder());
        builder.add_batch("f", 0, &column(&keys)).expect("batch");
        let bytes = builder.finish().expect("finish").to_bytes();
        let mut body = bytes[..bytes.len() - 8].to_vec();
        let i = 12 + rng.random_range(0..body.len() - 12);
        body[i] ^= rng.random_range(1_u8..=u8::MAX);
        crate::persist::seal(&mut body);
        if let Ok(run) = IndexRun::from_bytes(&body) {
            for key in 0..40 {
                run.lookup(encoder().key_word(&encoded(key)), |_, _| {});
            }
            run.for_each_row(|_, _, _| {});
        }
    });
}

/// Every `u64` round-trips through a varint, at every width.
#[test]
fn varints_round_trip() {
    for_seeds(256, |seed, rng| {
        let values = vec_of(rng, 0..64, |rng| {
            rng.random::<u64>() >> rng.random_range(0..64)
        });
        let mut bytes = Vec::new();
        for &value in &values {
            varint::put(&mut bytes, value);
        }
        let mut at = 0;
        for &value in &values {
            assert_eq!(varint::get(&bytes, &mut at), Some(value), "seed {seed}");
        }
        assert_eq!(at, bytes.len(), "seed {seed}");
    });
}
