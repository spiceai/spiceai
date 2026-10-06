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

use std::collections::{BTreeMap, BTreeSet};

use arrow_array::StringArray;
use arrow_schema::DataType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::*;
use crate::KeyField;
use crate::test_support::{column, encoded, encoder, run_of, word};

/// Model: key -> set of (file, position) rows in live files.
#[derive(Default)]
struct Model {
    rows: BTreeMap<i64, BTreeSet<(String, u64)>>,
}

fn check(index: &TieredIndex, model: &Model, live: &BTreeSet<String>, keys: i64) {
    let view = index.view();
    // The batched lookup returns exactly the per-key candidates, for keys
    // in no particular order, with a duplicate and a missing key.
    let mut batch: Vec<i64> = (0..keys).rev().collect();
    batch.push(keys / 2);
    batch.push(keys + 7);
    let encoded_batch: Vec<Vec<u8>> = batch.iter().map(|&k| encoded(k)).collect();
    let borrowed: Vec<&[u8]> = encoded_batch.iter().map(Vec::as_slice).collect();
    let mut batched: Vec<Vec<String>> = vec![Vec::new(); batch.len()];
    view.candidates_batch(&borrowed, |i, candidate| {
        batched[i].push(format!("{candidate:?}"));
    });
    for (i, key) in borrowed.iter().enumerate() {
        let mut single = Vec::new();
        view.candidates(key, |candidate| single.push(format!("{candidate:?}")));
        let (mut a, mut b) = (batched[i].clone(), single);
        a.sort();
        b.sort();
        assert_eq!(a, b, "batched candidates of key {}", batch[i]);
    }
    for k in 0..keys {
        let mut files = BTreeSet::new();
        view.candidates(&encoded(k), |Candidate { file, position }| {
            // A reader discards candidates of files it does not scan.
            if live.contains(file) {
                assert!(
                    files.insert((file.to_string(), position)),
                    "duplicate candidate"
                );
            }
        });
        assert_eq!(
            files,
            model.rows.get(&k).cloned().unwrap_or_default(),
            "file rows of key {k}"
        );
    }
}

/// A large run — rows arriving shuffled, in uneven batches, across
/// files, with repeated keys — answers every key exactly as a model of
/// the rows does.
#[test]
fn a_large_run_matches_a_model() {
    let mut rng = StdRng::seed_from_u64(0x5E6_3E27);
    let rows = 7 * 65_536 + 123;
    let mut builder = RunBuilder::new(encoder());
    let mut model: BTreeMap<i64, BTreeSet<(String, u64)>> = BTreeMap::new();
    let mut written = 0;
    let mut file_no = 0;
    while written < rows {
        let file = format!("f{file_no}.vortex");
        let batch = rng.random_range(1..=2 * 65_536).min(rows - written);
        let keys: Vec<i64> = (0..batch).map(|_| rng.random_range(0..5_000)).collect();
        for (row, &key) in keys.iter().enumerate() {
            model
                .entry(key)
                .or_default()
                .insert((file.clone(), row as u64));
        }
        builder.add_batch(&file, 0, &column(&keys)).expect("batch");
        written += batch;
        file_no += 1;
    }
    let run = builder.finish().expect("run");
    assert_eq!(run.len(), rows);
    for key in -1..5_001 {
        let mut found = BTreeSet::new();
        run.lookup(word(key), |file, position| {
            assert!(
                found.insert((file.to_string(), position)),
                "key {key} repeated a row"
            );
        });
        assert_eq!(found, model.remove(&key).unwrap_or_default(), "key {key}");
    }
}

/// Keys that share a word get each other's rows as candidates, never
/// fewer than their own: with 2-bit words, 100 keys share 4 words.
#[test]
fn keys_sharing_a_word_get_a_superset_of_their_rows() {
    let colliding = || encoder().with_word_bits(2);
    let keys: Vec<i64> = (0..100).flat_map(|k| [k, k]).collect();
    let index = TieredIndex::new(colliding());
    let mut builder = RunBuilder::new(colliding());
    builder.add_batch("f", 0, &column(&keys)).expect("add");
    index.publish(vec![builder.finish().expect("finish")], &[]);
    let mut extra = 0;
    for key in 0..100_i64 {
        let own: BTreeSet<u64> = keys
            .iter()
            .enumerate()
            .filter(|&(_, &k)| k == key)
            .map(|(position, _)| position as u64)
            .collect();
        let mut got = BTreeSet::new();
        index.candidates(&encoded(key), |Candidate { position, .. }| {
            got.insert(position);
        });
        assert!(own.is_subset(&got), "key {key} lost its own rows");
        extra += got.len() - own.len();
    }
    assert!(extra > 0, "2-bit words must collide");
}

/// A key's only row, at a position whose posting does not fit a slot's
/// 31 bits, is kept in the postings instead, and found exactly.
#[test]
fn a_lone_posting_too_wide_for_its_slot_is_found() {
    let wide = 1_u64 << 33;
    for position in [0, (1 << 31) - 1, 1 << 31, wide] {
        let mut builder = RunBuilder::new(encoder());
        builder
            .add_batch_at("f", &[position], &column(&[42]))
            .expect("add");
        let run = builder.finish().expect("finish");
        let mut found = Vec::new();
        run.lookup(word(42), |_, position| found.push(position));
        assert_eq!(found, vec![position]);
        let restored = IndexRun::from_bytes(&run.to_bytes()).expect("round trip");
        let mut found = Vec::new();
        restored.lookup(word(42), |_, position| found.push(position));
        assert_eq!(found, vec![position], "after a round trip");
    }
}

/// A run over `keys`, all in `file`.
/// The filter counts distinct keys, not inserts: runs over keys it holds
/// already (a rewrite's run over the rows it compacted, a batch of
/// updates) never make it due for a rebuild, while new keys past twice
/// its size and keys mostly retired do. A publish never rebuilds it.
#[test]
fn the_filter_is_rebuilt_for_new_or_retired_keys_not_repeated_ones() {
    let keys: Vec<i64> = (0..1_000).collect();
    let index = TieredIndex::new(encoder());
    index.publish(vec![run_of("a", &keys)], &[]);
    for file in ["b", "c", "d"] {
        index.publish(vec![run_of(file, &keys)], &[]);
    }
    assert!(
        !index.filter_overfull(),
        "republishing the same keys must not make the filter overfull"
    );

    let grown = TieredIndex::new(encoder());
    grown.publish(vec![run_of("a", &keys)], &[]);
    let more: Vec<i64> = (1_000..3_500).collect();
    grown.publish(vec![run_of("b", &more)], &[]);
    assert!(grown.filter_overfull(), "3.5x the keys it was sized for");
    let before = Arc::as_ptr(&grown.layers.load().filter);
    grown.publish(vec![run_of("c", &[10_000])], &[]);
    assert_eq!(
        Arc::as_ptr(&grown.layers.load().filter),
        before,
        "a publish must not rebuild the filter"
    );
    assert!(grown.rebuild_overfull_filter());
    assert!(!grown.filter_overfull());

    // With no other run left, retiring every file hands the next publish
    // a fresh filter; here a small run stays live alongside.
    let retired = TieredIndex::new(encoder());
    retired.publish(vec![run_of("a", &keys), run_of("c", &[7_000])], &[]);
    retired.publish(vec![run_of("b", &[5_000, 5_001])], &["a"]);
    assert!(
        retired.filter_overfull(),
        "a filter of mostly retired keys is due for a rebuild"
    );
    assert!(retired.rebuild_overfull_filter());
    assert!(!retired.filter_overfull());
    // Still answers for the live keys after the rebuild.
    let mut found = 0;
    retired.view().candidates(&encoded(5_001), |_| found += 1);
    assert_eq!(found, 1);
}

/// Merging runs of hashed (string) keys, some keys in several runs, from
/// single- and multi-file runs with retired files, keeps exactly the live
/// rows, under each row's word.
#[test]
fn merging_runs_keeps_every_live_row() {
    let string_encoder =
        || KeyEncoder::new(vec![KeyField::new(DataType::Utf8, false)]).expect("utf8 key");
    let key_of = |k: u64| format!("service-{k:04}");
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let index = TieredIndex::new(string_encoder());
    let mut model: Vec<(u64, String, u64)> = Vec::new();
    let mut runs = Vec::new();
    for run_no in 0..6 {
        let mut builder = RunBuilder::new(string_encoder());
        // Runs 0 and 1 span two files each; the rest one file.
        let files = if run_no < 2 { 2 } else { 1 };
        for file_no in 0..files {
            let file = format!("r{run_no}f{file_no}");
            let keys: Vec<String> = (0..200).map(|_| key_of(next() % 300)).collect();
            let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(keys.clone()))];
            builder.add_batch(&file, 0, &columns).expect("add");
            let bound_columns = columns.clone();
            let encoder = string_encoder();
            let bound = encoder.bind(&bound_columns).expect("bind");
            for (position, _) in keys.iter().enumerate() {
                let mut key_bytes = Vec::new();
                bound.encode_row(position, &mut key_bytes);
                model.push((encoder.key_word(&key_bytes), file.clone(), position as u64));
            }
        }
        runs.push(builder.finish().expect("finish"));
    }
    index.publish(runs, &[]);
    // Retire one file of a two-file run, and all of a one-file run.
    index.publish(vec![], &["r0f1", "r3f0"]);
    model.retain(|(_, file, _)| file != "r0f1" && file != "r3f0");
    model.sort();
    assert!(index.merge_all().expect("merge"));
    let runs = index.view().run_list();
    assert_eq!(runs.len(), 1);
    let mut got: Vec<(u64, String, u64)> = Vec::new();
    // The merge drops the rows of retired files, so every row is live.
    runs[0].for_each_row(|word, file, position| {
        got.push((word, file.to_string(), position));
    });
    got.sort();
    assert_eq!(got, model);
}

/// `publish` keeps every run it is given, one over no files included, while
/// `publish_visible` drops a run none of whose files is live.
#[test]
fn publish_keeps_every_run_and_publish_visible_only_live_ones() {
    let index = TieredIndex::new(encoder());
    index.publish(
        vec![RunBuilder::new(encoder()).finish().expect("empty run")],
        &[],
    );
    assert_eq!(index.view().runs(), 1, "a run over no files is kept");
    let live: HashSet<&str> = HashSet::new();
    index.publish_visible(
        vec![
            RunBuilder::new(encoder()).finish().expect("empty run"),
            run_of("gone", &[1]),
        ],
        &live,
    );
    assert_eq!(index.view().runs(), 1, "runs with no live file are dropped");
}

/// Rows at explicit positions (a read-back) index exactly like the same
/// rows at contiguous ones (a write); a file declared with no indexed row
/// is still covered; retiring a file ends its coverage.
#[test]
fn explicit_positions_and_coverage() {
    let keys: Vec<i64> = (0..100).map(|k| k % 17).collect();
    let mut written = RunBuilder::new(encoder());
    written
        .add_batch("a", 0, &column(&keys[..60]))
        .expect("add");
    written
        .add_batch("a", 60, &column(&keys[60..]))
        .expect("add");
    let mut read_back = RunBuilder::new(encoder());
    // A read-back returns the same rows in another order.
    let order: Vec<usize> = (0..100).rev().collect();
    let shuffled: Vec<i64> = order.iter().map(|&i| keys[i]).collect();
    let positions: Vec<u64> = order.iter().map(|&i| i as u64).collect();
    read_back
        .add_batch_at("a", &positions, &column(&shuffled))
        .expect("add");
    read_back.add_file("empty").expect("declare");
    assert!(matches!(
        read_back.add_batch_at("a", &positions[..3], &column(&shuffled)),
        Err(Error::PositionCount {
            rows: 100,
            positions: 3
        })
    ));
    let (written, read_back) = (TieredIndex::new(encoder()), {
        let index = TieredIndex::new(encoder());
        index.publish(vec![read_back.finish().expect("run")], &[]);
        index
    });
    written.publish(vec![run_of("a", &keys)], &[]);
    for k in 0..20 {
        let collect = |index: &TieredIndex| {
            let mut rows = Vec::new();
            index.candidates(&encoded(k), |c| rows.push(format!("{c:?}")));
            rows.sort();
            rows
        };
        assert_eq!(collect(&written), collect(&read_back), "key {k}");
    }
    let view = read_back.view();
    assert!(view.covers("a") && view.covers("empty") && !view.covers("b"));
    read_back.publish(Vec::new(), &["empty"]);
    assert!(read_back.view().covers("a") && !read_back.view().covers("empty"));
    assert!(
        view.covers("empty"),
        "a view keeps the coverage it was taken with"
    );
}

/// `finish` indexes exactly the rows added, in word order, for shuffled,
/// clustered, descending and duplicate-heavy input.
#[test]
fn a_run_holds_every_row_in_word_order() {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let shuffled: Vec<i64> = (0..3_000)
        .map(|_| (next() % 100_000).cast_signed())
        .collect();
    let duplicates: Vec<i64> = (0..3_000).map(|_| (next() % 7).cast_signed()).collect();
    for (name, keys) in [
        ("shuffled", shuffled),
        ("clustered", (0..3_000).collect::<Vec<i64>>()),
        ("descending", (0..3_000).rev().collect()),
        ("duplicates", duplicates),
    ] {
        let mut builder = RunBuilder::new(encoder());
        for (batch, chunk) in keys.chunks(97).enumerate() {
            builder
                .add_batch("f", (batch * 97) as u64, &column(chunk))
                .expect("add");
        }
        let run = builder.finish().expect("finish");
        let mut got: Vec<(u64, u64)> = Vec::new();
        run.for_each_row(|word, _, position| got.push((word, position)));
        let mut expected: Vec<(u64, u64)> = keys
            .iter()
            .enumerate()
            .map(|(position, &key)| (word(key), position as u64))
            .collect();
        expected.sort_unstable();
        assert_eq!(got, expected, "{name}");
    }
}

#[test]
fn writes_and_compactions_match_a_model() {
    const KEYS: i64 = 50;
    for seed in 0..20 {
        let mut rng = StdRng::seed_from_u64(seed);
        let index = TieredIndex::new(encoder());
        let mut model = Model::default();
        let mut live: BTreeSet<String> = BTreeSet::new();
        let mut next_file = 0;
        for _ in 0..60 {
            match rng.random_range(0..7) {
                // A write produces files and a run.
                0..=3 | 6 => {
                    let mut builder = RunBuilder::new(encoder());
                    let mut keys: Vec<i64> = (0..rng.random_range(0..40))
                        .map(|_| rng.random_range(0..KEYS))
                        .collect();
                    for _ in 0..rng.random_range(1..4) {
                        let file = format!("f{next_file}");
                        next_file += 1;
                        let take = keys.len().min(rng.random_range(0..30));
                        let batch: Vec<i64> = keys.drain(..take).collect();
                        builder
                            .add_batch(&file, 0, &column(&batch))
                            .expect("add batch");
                        for (position, &k) in (0_u64..).zip(&batch) {
                            model
                                .rows
                                .entry(k)
                                .or_default()
                                .insert((file.clone(), position));
                        }
                        live.insert(file);
                    }
                    // Anything left over goes into one last file.
                    let file = format!("f{next_file}");
                    next_file += 1;
                    builder
                        .add_batch(&file, 0, &column(&keys))
                        .expect("add batch");
                    for (position, &k) in (0_u64..).zip(&keys) {
                        model
                            .rows
                            .entry(k)
                            .or_default()
                            .insert((file.clone(), position));
                    }
                    live.insert(file);
                    index.publish(vec![builder.finish().expect("run")], &[]);
                }
                // Background run maintenance.
                4 => while index.merge_step().expect("merge") {},
                // A compaction rewrites some live files into one new file.
                _ => {
                    let victims: Vec<String> = live
                        .iter()
                        .filter(|_| rng.random_bool(0.4))
                        .cloned()
                        .collect();
                    // The victims' rows leave the live set; the rewritten
                    // file below holds new rows of its own.
                    for rows in model.rows.values_mut() {
                        rows.retain(|(file, _)| !victims.contains(file));
                    }
                    for victim in &victims {
                        live.remove(victim);
                    }
                    let file = format!("f{next_file}");
                    next_file += 1;
                    let rewritten: Vec<i64> = (0..rng.random_range(0..30))
                        .map(|_| rng.random_range(0..KEYS))
                        .collect();
                    let mut builder = RunBuilder::new(encoder());
                    builder
                        .add_batch(&file, 0, &column(&rewritten))
                        .expect("add batch");
                    for (position, &k) in (0_u64..).zip(&rewritten) {
                        model
                            .rows
                            .entry(k)
                            .or_default()
                            .insert((file.clone(), position));
                    }
                    live.insert(file);
                    let retired: Vec<&str> = victims.iter().map(String::as_str).collect();
                    index.publish(vec![builder.finish().expect("run")], &retired);
                }
            }
            check(&index, &model, &live, KEYS);
        }
    }
}

#[test]
fn a_run_is_retired_only_when_all_its_files_are() {
    let index = TieredIndex::new(encoder());
    let mut builder = RunBuilder::new(encoder());
    builder.add_batch("a", 0, &column(&[1, 2])).expect("a");
    builder.add_batch("b", 0, &column(&[1])).expect("b");
    index.publish(vec![builder.finish().expect("run")], &[]);
    index.publish(Vec::new(), &["a"]);
    assert_eq!(index.view().runs(), 1, "b is still live");
    index.publish(Vec::new(), &["b"]);
    assert_eq!(index.view().runs(), 0);
}

#[test]
fn reconcile_retires_files_seen_live_and_then_gone() {
    let index = TieredIndex::new(encoder());
    index.publish(vec![run_of("a", &[1]), run_of("b", &[2])], &[]);
    let live = |names: &[&'static str]| {
        names
            .iter()
            .copied()
            .collect::<std::collections::HashSet<&str>>()
    };
    // `b` is written but not visible yet: never seen, so kept.
    assert_eq!(index.reconcile(&live(&["a"])), 0);
    assert_eq!(index.view().runs(), 2);
    // Both visible, then a refresh replaces them with `c`.
    assert_eq!(index.reconcile(&live(&["a", "b"])), 0);
    index.publish(vec![run_of("c", &[3])], &[]);
    assert_eq!(index.reconcile(&live(&["c"])), 2);
    let view = index.view();
    assert_eq!(view.runs(), 1);
    let mut found = Vec::new();
    for k in 1..=3 {
        view.candidates(&encoded(k), |c| found.push((k, format!("{c:?}"))));
    }
    assert_eq!(found.len(), 1, "only c's key remains: {found:?}");
    // Unchanged file sets change nothing.
    assert_eq!(index.reconcile(&live(&["c"])), 0);
}

#[test]
fn publish_visible_retires_files_already_gone() {
    let index = TieredIndex::new(encoder());
    let mut builder = RunBuilder::new(encoder());
    builder.add_batch("a", 0, &column(&[1])).expect("batch");
    builder.add_batch("b", 0, &column(&[2])).expect("batch");
    let live = |names: &[&'static str]| names.iter().copied().collect::<HashSet<&str>>();
    // `b` was compacted away before the run was published.
    index.publish_visible(vec![builder.finish().expect("run")], &live(&["a", "c"]));
    let view = index.view();
    assert!(view.covers("a"));
    assert!(
        !view.covers("b"),
        "a file gone before publish is never covered"
    );
    // `a` was seen at publish, so leaving the set retires it and drops the run.
    assert_eq!(index.reconcile(&live(&["c"])), 1);
    assert_eq!(index.view().runs(), 0);
    // A run with no live file is not published at all.
    index.publish_visible(vec![run_of("d", &[4])], &live(&["c"]));
    assert_eq!(index.view().runs(), 0);
}

#[test]
fn unseen_files_are_retired_after_the_grace() {
    let index = TieredIndex::new(encoder());
    let mut builder = RunBuilder::new(encoder());
    builder.add_batch("never", 0, &column(&[1])).expect("batch");
    index.publish(vec![builder.finish().expect("run")], &[]);
    let live: HashSet<&str> = std::iter::once("other").collect();
    for _ in 1..UNSEEN_GRACE {
        assert_eq!(index.reconcile(&live), 0);
        assert!(index.view().covers("never"), "kept within the grace");
    }
    assert_eq!(index.reconcile(&live), 1);
    assert_eq!(index.view().runs(), 0);
}

#[test]
fn rejects_positions_beyond_forty_bits() {
    let mut builder = RunBuilder::new(encoder());
    assert!(matches!(
        builder.add_batch("a", POSITION_MASK, &column(&[1, 2])),
        Err(Error::Position { .. })
    ));
    builder
        .add_batch("a", POSITION_MASK, &column(&[1]))
        .expect("the largest position is accepted");
    // A first position so large that adding the row count overflows is
    // refused, not wrapped into range.
    assert!(matches!(
        builder.add_batch("a", u64::MAX, &column(&[1, 2])),
        Err(Error::Position {
            position: u64::MAX,
            ..
        })
    ));
    // A refused batch adds no file, so a run cannot claim to cover a
    // file whose rows it does not hold.
    let mut refused = RunBuilder::new(encoder());
    assert!(
        refused
            .add_batch("b", POSITION_MASK, &column(&[1, 2]))
            .is_err()
    );
    assert!(
        refused.files().is_empty(),
        "a refused batch's file is registered: {:?}",
        refused.files()
    );
}

mod merge {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow_array::Int64Array;
    use parking_lot::Mutex;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::super::*;
    use crate::test_support::{encoded, encoder};

    fn visible(view: &IndexView, k: i64) -> bool {
        let mut found = false;
        view.candidates(&encoded(k), |_| found = true);
        found
    }

    /// A merger loops while a writer publishes one-key runs and retires some
    /// of their files. A key whose file was never retired stays visible in
    /// every view, and at the end exactly those keys are.
    #[test]
    fn merges_racing_writes_and_retirements_keep_live_rows_only() {
        const RUNS: usize = 1_500;
        let index = TieredIndex::new(encoder());
        let committed = AtomicUsize::new(0);
        let retired: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
        let stop = AtomicBool::new(false);
        let merges = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    if index.merge_step().expect("merge") {
                        merges.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            for reader in 0..3 {
                let (index, committed, retired, stop) = (&index, &committed, &retired, &stop);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(reader);
                    while !stop.load(Ordering::Relaxed) {
                        let upto = committed.load(Ordering::Acquire);
                        if upto == 0 {
                            continue;
                        }
                        let k = rng.random_range(0..upto);
                        // Retirements only happen to keys below `upto`; check
                        // the set before taking the view, so a key seen as
                        // unretired was unretired when the view was taken.
                        if retired.lock().contains(&k) {
                            continue;
                        }
                        let view = index.view();
                        if !retired.lock().contains(&k) {
                            assert!(
                                visible(&view, i64::try_from(k).expect("key")),
                                "live key {k} lost"
                            );
                        }
                    }
                });
            }
            let mut rng = StdRng::seed_from_u64(99);
            for r in 0..RUNS {
                let mut builder = RunBuilder::new(encoder());
                let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
                    i64::try_from(r).expect("key"),
                ]))];
                builder
                    .add_batch(&format!("f{r}"), 0, &columns)
                    .expect("batch");
                index.publish(vec![builder.finish().expect("run")], &[]);
                committed.store(r + 1, Ordering::Release);
                if r > 0 && rng.random_bool(0.3) {
                    let victim = rng.random_range(0..r);
                    let newly = retired.lock().insert(victim);
                    if newly {
                        index.publish(Vec::new(), &[&format!("f{victim}")]);
                    }
                }
            }
            stop.store(true, Ordering::Relaxed);
        });
        assert!(merges.load(Ordering::Relaxed) > 20, "the merger barely ran");
        while index.merge_step().expect("merge") {}
        let view = index.view();
        let retired = retired.into_inner();
        for r in 0..RUNS {
            assert_eq!(
                visible(&view, i64::try_from(r).expect("key")),
                !retired.contains(&r),
                "key {r}"
            );
        }
        assert!(
            view.runs() < 40,
            "{} runs remain after merging",
            view.runs()
        );
    }
}

/// A run holds the words of the encoder that built it. A nullable field encodes
/// each value behind a validity byte, so the same string has another word under
/// a `NOT NULL` field: a run built while the column was `NOT NULL` and
/// published into an index of the column made nullable would cover its files
/// while matching none of their keys, and a lookup would miss their rows. Such
/// a run, persisted and read back, is refused, its files are left uncovered so
/// a reader scans them, and a run of the index's own encoding is published.
#[test]
fn a_run_of_another_encoding_is_not_published() {
    let utf8 = |nullable| {
        KeyEncoder::new(vec![KeyField::new(DataType::Utf8, nullable)]).expect("utf8 key")
    };
    let (strict, nullable) = (utf8(false), utf8(true));
    assert_eq!(strict.word_identity(), utf8(false).word_identity());
    assert_ne!(strict.word_identity(), nullable.word_identity());
    let int64 = KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key");
    let exact = int64.word_identity();
    assert_ne!(
        exact,
        int64.with_word_bits(64).word_identity(),
        "an encoder giving keys their own bytes and one hashing them must differ"
    );
    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["a", "b"]))];
    let run = |encoder: &KeyEncoder, file: &str| {
        let mut builder = RunBuilder::new(encoder.clone());
        builder.add_batch(file, 0, &columns).expect("add");
        IndexRun::from_bytes(&builder.finish().expect("finish").to_bytes()).expect("round trip")
    };
    let stale = run(&strict, "stale");
    assert_eq!(stale.encoding(), strict.word_identity());

    let index = TieredIndex::new(nullable.clone());
    index.publish(vec![stale], &[]);
    assert!(
        !index.view().covers("stale"),
        "a run of another encoding must not cover its files"
    );
    index.publish(vec![run(&nullable, "own")], &[]);
    assert!(index.view().covers("own"));
    let bound = nullable.bind(&columns).expect("bind");
    let mut key = Vec::new();
    bound.encode_row(0, &mut key);
    let mut found = Vec::new();
    index.candidates(&key, |candidate| {
        found.push((candidate.file.to_string(), candidate.position));
    });
    assert_eq!(found, vec![("own".to_string(), 0)]);
}

/// Each live file is covered by one run, so a lookup returns each of its rows
/// once. A run over a file another live run already covers (a write's run and
/// a read-back of the same file, both finishing) is not published, whether
/// that run was published before or alongside it; a run with any such file is
/// refused whole, so its other files stay uncovered and are read in full.
#[test]
fn a_file_is_covered_by_one_run_only() {
    let candidates = |index: &TieredIndex, key: i64| {
        let mut found = Vec::new();
        index.candidates(&encoded(key), |candidate| {
            found.push((candidate.file.to_string(), candidate.position));
        });
        found
    };
    let one = vec![("same".to_string(), 0)];

    let index = TieredIndex::new(encoder());
    index.publish(vec![run_of("same", &[11])], &[]);
    index.publish(vec![run_of("same", &[11])], &[]);
    assert_eq!(candidates(&index, 11), one, "published one after the other");
    assert_eq!(index.view().runs(), 1);

    let index = TieredIndex::new(encoder());
    index.publish(vec![run_of("same", &[11]), run_of("same", &[11])], &[]);
    assert_eq!(candidates(&index, 11), one, "published together");

    let index = TieredIndex::new(encoder());
    index.publish(vec![run_of("same", &[11])], &[]);
    let mut overlapping = RunBuilder::new(encoder());
    overlapping
        .add_batch("same", 0, &column(&[11]))
        .expect("add");
    overlapping
        .add_batch("other", 0, &column(&[12]))
        .expect("add");
    index.publish(vec![overlapping.finish().expect("finish")], &[]);
    assert_eq!(candidates(&index, 11), one, "a partly overlapping run");
    assert!(
        !index.view().covers("other"),
        "a refused run's other files stay uncovered"
    );
}

/// A merge keeps only the files still live when it starts. A file retired
/// from one run can be covered again by a later run (a write that becomes
/// visible after its grace ran out, then indexed by a read-back), and a merged
/// run naming it twice would be one its own reader rejects.
#[test]
fn a_merge_drops_retired_files_from_the_merged_run() {
    let index = TieredIndex::new(encoder());
    let mut builder = RunBuilder::new(encoder());
    builder.add_batch("same", 0, &column(&[11])).expect("add");
    builder.add_batch("keep", 0, &column(&[12])).expect("add");
    index.publish(vec![builder.finish().expect("finish")], &[]);
    index.publish(vec![], &["same"]);
    index.publish(vec![run_of("same", &[13])], &[]);
    assert!(index.merge_all().expect("merge"));
    let runs = index.view().run_list();
    assert_eq!(runs.len(), 1);
    let merged = &runs[0];
    let mut names: Vec<&str> = merged.files().iter().map(AsRef::as_ref).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["keep", "same"], "the retired file is not kept");
    let restored = IndexRun::from_bytes(&merged.to_bytes()).expect("the merged run reads back");
    assert_eq!(restored.len(), merged.len());
    let found = |key: i64| {
        let mut found = Vec::new();
        index.candidates(&encoded(key), |candidate| {
            found.push((candidate.file.to_string(), candidate.position));
        });
        found
    };
    assert_eq!(
        found(11),
        Vec::<(String, u64)>::new(),
        "a retired file's row"
    );
    assert_eq!(found(12), vec![("keep".to_string(), 0)]);
    assert_eq!(found(13), vec![("same".to_string(), 0)]);
}

mod persist {
    use std::collections::BTreeMap;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::DataType;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::super::*;
    use crate::KeyField;
    use crate::persist::Error;

    fn run(rng: &mut StdRng, rows: usize) -> IndexRun {
        let encoder = KeyEncoder::new(vec![
            KeyField::new(DataType::Int64, true),
            KeyField::new(DataType::Utf8, false),
        ])
        .expect("key types");
        let mut builder = RunBuilder::new(encoder);
        for file in 0..3 {
            let ids: Int64Array = (0..rows)
                .map(|_| rng.random_bool(0.9).then(|| rng.random_range(0..200_i64)))
                .collect();
            let names: StringArray = (0..rows)
                .map(|_| Some(["", "a", "a\0", "ab"][rng.random_range(0..4)]))
                .collect();
            let columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(names)];
            builder
                .add_batch(&format!("f{file}.vortex"), 0, &columns)
                .expect("add");
        }
        builder.finish().expect("finish")
    }

    fn contents(run: &IndexRun) -> BTreeMap<u64, Vec<(String, u64)>> {
        let mut out: BTreeMap<u64, Vec<(String, u64)>> = BTreeMap::new();
        run.for_each_row(|word, file, position| {
            out.entry(word)
                .or_default()
                .push((file.to_string(), position));
        });
        out
    }

    #[test]
    fn runs_round_trip_through_bytes() {
        let mut rng = StdRng::seed_from_u64(7);
        for rows in [0, 1, 50, 5_000] {
            let original = run(&mut rng, rows);
            let bytes = original.to_bytes();
            let restored = IndexRun::from_bytes(&bytes).expect("round trip");
            assert_eq!(contents(&original), contents(&restored), "{rows} rows");
            assert_eq!(restored.files(), original.files());
            // Lookups (filter included) answer the same.
            for (word, rows) in contents(&original) {
                let mut got = Vec::new();
                restored.lookup(word, |file, position| {
                    got.push((file.to_string(), position));
                });
                assert_eq!(got, rows);
            }
        }
    }

    #[test]
    fn corrupt_or_truncated_bytes_are_rejected() {
        let mut rng = StdRng::seed_from_u64(8);
        let bytes = run(&mut rng, 2_000).to_bytes();
        for at in [0, 5, 13, bytes.len() / 2, bytes.len() - 9, bytes.len() - 1] {
            let mut flipped = bytes.clone();
            flipped[at] ^= 0x10;
            assert!(
                IndexRun::from_bytes(&flipped).is_err(),
                "flip at {at} accepted"
            );
        }
        for len in [0, 10, 19, bytes.len() / 3, bytes.len() - 1] {
            assert!(
                IndexRun::from_bytes(&bytes[..len]).is_err(),
                "truncation to {len} accepted"
            );
        }
        // A run of another format version is rejected, not misread.
        let mut other = run(&mut rng, 10).to_bytes();
        other[4..8].copy_from_slice(&(crate::persist::VERSION + 1).to_le_bytes());
        assert_eq!(
            IndexRun::from_bytes(&other).err(),
            Some(Error::Format {
                kind: crate::persist::KIND_RUN
            })
        );
    }

    /// The bytes of a run over `files` built from these parts, sealed with a
    /// valid checksum, as a writer bug or a crafted file could leave them.
    fn sealed(
        files: &[&str],
        words: Vec<u64>,
        slots: Vec<u32>,
        postings: Vec<u8>,
        rows: usize,
    ) -> Vec<u8> {
        let files: Vec<Arc<str>> = files.iter().map(|&file| Arc::from(file)).collect();
        IndexRun::from_parts(
            0,
            files.into(),
            words.into(),
            slots.into(),
            postings.into(),
            rows,
        )
        .to_bytes()
    }

    /// A run of file `a` whose only word has `postings` as its posting stream.
    fn sealed_with_postings(postings: Vec<u8>, rows: usize) -> Vec<u8> {
        sealed(
            &["a"],
            vec![7],
            vec![word_proof::offset_slot(0)],
            postings,
            rows,
        )
    }

    fn varints(values: &[u64]) -> Vec<u8> {
        let mut out = Vec::new();
        for &value in values {
            varint::put(&mut out, value);
        }
        out
    }

    /// A posting stream that passes the checksum but would overflow, end
    /// early, repeat a posting or disagree with the row count is rejected: a
    /// lookup over it would panic, or skip rows after the damage.
    #[test]
    fn a_run_whose_postings_do_not_decode_in_full_is_rejected() {
        let intact = sealed_with_postings(varints(&[2, 0, 5]), 2);
        let run = IndexRun::from_bytes(&intact).expect("an intact stream loads");
        let mut got = Vec::new();
        run.lookup(7, |file, position| got.push((file.to_string(), position)));
        assert_eq!(got, vec![("a".to_string(), 0), ("a".to_string(), 5)]);

        for (what, postings, rows) in [
            ("overflowing deltas", varints(&[2, u64::MAX, 5]), 2),
            ("a count past the bytes", varints(&[3, 0, 5]), 3),
            ("a repeated posting", varints(&[2, 4, 0]), 2),
            ("a zero count", varints(&[0]), 0),
            ("a wrong row count", varints(&[2, 0, 5]), 3),
            ("a position past 40 bits", varints(&[2, 0, 1 << 41]), 2),
        ] {
            assert_eq!(
                IndexRun::from_bytes(&sealed_with_postings(postings, rows)).err(),
                Some(Error::Corrupt),
                "{what} accepted"
            );
        }
    }

    /// A run with rows but no files is rejected: its postings name a file it
    /// does not have, so it would report rows that no lookup can return.
    #[test]
    fn a_run_with_rows_but_no_files_is_rejected() {
        for slot in [word_proof::lone_slot(0), word_proof::offset_slot(0)] {
            let postings = if word_proof::slot_is_lone(slot) {
                Vec::new()
            } else {
                varints(&[1, 0])
            };
            let bytes = sealed(&[], vec![7], vec![slot], postings, 1);
            assert_eq!(IndexRun::from_bytes(&bytes).err(), Some(Error::Corrupt));
        }
        assert!(
            IndexRun::from_bytes(&sealed(&[], Vec::new(), Vec::new(), Vec::new(), 0)).is_ok(),
            "a run of no rows and no files loads"
        );
    }

    /// A run that names one file twice is rejected: its postings could reach
    /// one row through both names, and a lookup would return the row twice.
    #[test]
    fn a_run_naming_a_file_twice_is_rejected() {
        // Postings 0 and 1 are row 0 of file 0 and row 0 of file 1, both
        // `same`.
        let bytes = sealed(
            &["same", "same"],
            vec![7],
            vec![word_proof::offset_slot(0)],
            varints(&[2, 0, 1]),
            2,
        );
        assert_eq!(IndexRun::from_bytes(&bytes).err(), Some(Error::Corrupt));
        let distinct = sealed(
            &["a", "b"],
            vec![7],
            vec![word_proof::offset_slot(0)],
            varints(&[2, 0, 1]),
            2,
        );
        assert!(
            IndexRun::from_bytes(&distinct).is_ok(),
            "distinct names load"
        );
    }

    /// A run naming more files than [`MAX_RUN_FILES`] is rejected: no builder
    /// writes one, and a merge involving it could never be built.
    #[test]
    fn a_run_naming_too_many_files_is_rejected() {
        // A run of no rows over `files` distinct files, written as `to_bytes`
        // writes one.
        let run_over = |files: usize| {
            let mut out = Vec::new();
            crate::persist::header(&mut out, crate::persist::KIND_RUN);
            out.extend_from_slice(&0_u64.to_le_bytes());
            out.extend_from_slice(&u32::try_from(files).expect("fits").to_le_bytes());
            for file in 0..files {
                let name = file.to_string();
                out.extend_from_slice(&u32::try_from(name.len()).expect("fits").to_le_bytes());
                out.extend_from_slice(name.as_bytes());
            }
            out.extend_from_slice(&0_u64.to_le_bytes()); // rows
            out.extend_from_slice(&0_u64.to_le_bytes()); // words
            out.extend_from_slice(&0_u64.to_le_bytes()); // postings
            crate::persist::seal(&mut out);
            out
        };
        assert_eq!(
            IndexRun::from_bytes(&run_over(MAX_RUN_FILES + 1)).err(),
            Some(Error::Corrupt)
        );
        assert!(
            IndexRun::from_bytes(&run_over(2)).is_ok(),
            "a run within the limit loads"
        );
    }

    /// Two words whose slots share one posting stream, leaving the stream of
    /// the second unreferenced, are rejected even when the row count adds
    /// up: the second word's lookups would read the first's rows and miss
    /// its own.
    #[test]
    fn a_run_whose_words_share_a_posting_stream_is_rejected() {
        let mut postings = varints(&[2, 0, 5]);
        let second = u32::try_from(postings.len()).expect("small");
        postings.extend(varints(&[2, 1, 2]));
        let sealed = |slots: Vec<u32>, rows: usize| {
            sealed(&["a"], vec![7, 9], slots, postings.clone(), rows)
        };
        let intact = IndexRun::from_bytes(&sealed(
            vec![word_proof::offset_slot(0), word_proof::offset_slot(second)],
            4,
        ))
        .expect("streams laid out in word order load");
        let mut got = Vec::new();
        intact.lookup(9, |file, position| got.push((file.to_string(), position)));
        assert_eq!(got, vec![("a".to_string(), 1), ("a".to_string(), 3)]);

        let shared = sealed(
            vec![word_proof::offset_slot(0), word_proof::offset_slot(0)],
            4,
        );
        assert_eq!(IndexRun::from_bytes(&shared).err(), Some(Error::Corrupt));
    }

    /// The persisted bytes of runs a fixed workload builds and merges, as
    /// `(length, digest)`. A change to how runs are built, merged or written
    /// that alters a single byte fails here; the format itself is versioned
    /// by [`crate::persist::VERSION`], which this test does not replace.
    #[test]
    fn run_bytes_are_pinned() {
        let digest = |run: &IndexRun| {
            let bytes = run.to_bytes();
            (bytes.len(), hash_index::hash_key_bytes_oneshot(&bytes))
        };
        let mut rng = StdRng::seed_from_u64(0xB17E5);
        // Three files of a nullable compound key with escaped strings.
        let compound = run(&mut rng, 5_000);
        let empty = run(&mut rng, 0);

        // Exact words, repeated keys across files, explicit positions with a
        // repeated row and one too wide for a slot, and a file with no row.
        let int = KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64");
        let int_column =
            |keys: Vec<i64>| -> Vec<ArrayRef> { vec![Arc::new(Int64Array::from(keys))] };
        let mut builder = RunBuilder::new(int);
        for file in 0..4 {
            let keys: Vec<i64> = (0..3_000).map(|_| rng.random_range(0..2_000)).collect();
            builder
                .add_batch(
                    &format!("i{file}"),
                    rng.random_range(0..1_000),
                    &int_column(keys),
                )
                .expect("add");
        }
        builder
            .add_batch_at("g", &[5, 5, 1 << 33, 9], &int_column(vec![1, 1, 2, 3]))
            .expect("add at");
        builder.add_file("empty").expect("file");
        let exact = builder.finish().expect("finish");

        // Hashed words merged from single- and multi-file runs, one with a
        // retired file and one with every file live.
        let utf8 = KeyEncoder::new(vec![KeyField::new(DataType::Utf8, false)]).expect("utf8");
        let index = TieredIndex::new(utf8.clone());
        let mut runs = Vec::new();
        for run_no in 0..7 {
            let mut builder = RunBuilder::new(utf8.clone());
            for file_no in 0..if run_no < 2 { 2 } else { 1 } {
                let keys: Vec<String> = (0..400)
                    .map(|_| format!("svc\0{}", rng.random_range(0..600)))
                    .collect();
                let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(keys))];
                builder
                    .add_batch(&format!("r{run_no}f{file_no}"), 0, &columns)
                    .expect("add");
            }
            runs.push(builder.finish().expect("finish"));
        }
        index.publish(runs, &[]);
        index.publish(vec![], &["r0f1", "r4f0"]);
        assert!(index.merge_all().expect("merge"));
        let merged = index.view().run_list();
        assert_eq!(merged.len(), 1);

        assert_eq!(
            [
                digest(&compound),
                digest(&empty),
                digest(&exact),
                digest(&merged[0])
            ],
            [
                (35_592, 0xB56C_576B_EAC6_1324),
                (95, 0xCBA3_9660_1670_7D8D),
                (49_423, 0xF5F8_EEA9_AF2A_AA0E),
                (12_673, 0xA17C_D5CB_4945_D384),
            ]
        );
    }

    /// Words out of order, or offsets that do not span the postings, are
    /// rejected even under a valid checksum: a run read out of order would
    /// miss rows.
    #[test]
    fn a_run_with_unordered_words_is_rejected() {
        let mut rng = StdRng::seed_from_u64(9);
        let mut original = run(&mut rng, 500);
        original.words.swap(0, 1);
        assert_eq!(
            IndexRun::from_bytes(&original.to_bytes()).err(),
            Some(Error::Corrupt)
        );
    }
}
