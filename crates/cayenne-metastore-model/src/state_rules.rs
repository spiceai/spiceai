// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Focused finite compositions for state naming, table sequences and retention.
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, s: &str) -> bool {
        self.mode.contains(s)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Physical {
    files: Vec<usize>,
    tombstones: Vec<usize>,
    schema: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct File {
    id: usize,
    key: usize,
    seq: usize,
    min_seq: usize,
    applied: Option<usize>,
    cold: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    step: usize,
    high: usize,
    physical: Physical,
    named: Vec<(usize, Physical)>,
    checkpoints: Vec<(usize, Physical)>,
    table_high: [usize; 2],
    rows: Vec<(usize, usize, usize, usize)>,
    deletes: Vec<(usize, usize, usize)>,
    oracle: [[Option<usize>; 2]; 2],
    refused: Option<Vec<(usize, usize, bool, usize)>>,
    ranges: Vec<[Option<(usize, usize)>; 2]>,
    files: Vec<File>,
    tombstones: Vec<(usize, usize)>,
    expected: [bool; 2],
    // Version-indexed file sets are symbolic immutable checkpoint collections.
    versions: Vec<Vec<usize>>,
    objects: Vec<usize>,
    floor: usize,
    queries: Vec<usize>,
    candidate: Option<usize>,
    pin: Option<usize>,
    validated: bool,
    moved: bool,
    scan: Option<Vec<usize>>,
    recovered: Option<[bool; 2]>,
    keys: [bool; 2],
    index: Option<(usize, [bool; 2])>,
    checkpoint: Option<(usize, [bool; 2])>,
    records: Vec<(usize, bool)>,
    collected: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Mutation,
    Maintenance,
    Metadata,
    Checkpoint,
    TableRecord(usize, usize, bool),
    Move,
    Both,
    RetryRefused,
    UpsertKey(usize),
    DeleteKey(usize),
    Compact(usize),
    Promote,
    Clear,
    Advance,
    Choose,
    Pin,
    Recheck,
    SecondQuery,
    Renew,
    Floor,
    Scan,
    Collect,
    SetKey(usize, bool),
    Index,
    Recover,
}
impl Protocol {
    fn state_name(&self, s: &State) -> usize {
        if self.has("adopted") && !self.has("no-number") {
            s.step
        } else {
            s.high
        }
    }
    fn clear_covered(&self, n: &mut State, cutoff: usize) {
        let files = &n.files;
        n.tombstones.retain(|&(_, d)| {
            d > cutoff
                || (self.has("adopted")
                    && files.iter().any(|f| {
                        if self.has("no-cold") && f.cold {
                            return false;
                        }
                        f.min_seq <= d
                            && if self.has("iceberg") {
                                f.seq < d
                            } else {
                                f.applied.is_none_or(|c| c < d)
                            }
                    }))
        });
    }
    fn identity_actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.step < self.bound {
            a.extend([Action::Mutation, Action::Maintenance, Action::Metadata]);
        }
        if !s
            .checkpoints
            .contains(&(self.state_name(s), s.physical.clone()))
        {
            a.push(Action::Checkpoint);
        }
    }
    fn table_record(&self, s: &State, n: &mut State, operations: &[(usize, usize, bool)]) {
        let mut allocated = s.table_high;
        let mut record = vec![];
        for &(table, key, insert) in operations {
            allocated[table] += 1;
            record.push((table, key, false, allocated[table]));
            if insert {
                allocated[table] += 1;
                record.push((table, key, true, allocated[table]));
            }
        }
        // Compare the complete allocated range against the publication base.
        if record.iter().any(|r| {
            r.3 <= if self.has("adopted") && !self.has("no-table-scope") {
                s.table_high[r.0]
            } else {
                s.high
            }
        }) {
            n.refused = Some(record);
            return;
        }
        for &(table, key, insert, seq) in &record {
            if insert {
                n.rows.push((table, key, seq, s.step + 1));
            } else {
                n.deletes.push((table, key, seq));
            }
            n.oracle[table][key] = insert.then_some(s.step + 1);
        }
        n.ranges.push(std::array::from_fn(|table| {
            let mut seqs = record.iter().filter(|r| r.0 == table).map(|r| r.3);
            let lo = seqs.next()?;
            Some((lo, allocated[table]))
        }));
        n.table_high = allocated;
        n.high = *allocated.iter().max().unwrap_or(&0);
        n.step += 1;
    }
    fn coverage_visible(s: &State) -> [bool; 2] {
        std::array::from_fn(|key| {
            s.files
                .iter()
                .any(|f| f.key == key && !s.tombstones.iter().any(|&(k, d)| k == key && d >= f.seq))
        })
    }
    fn rewrite(&self, s: &State, n: &mut State, mask: usize, cold: bool) {
        for (i, file) in s.files.iter().enumerate() {
            if file.cold == cold && mask & (1 << i) != 0 {
                n.files.retain(|f| f.id != file.id);
                if !s
                    .tombstones
                    .iter()
                    .any(|&(k, d)| k == file.key && d >= file.seq)
                {
                    let mut output = file.clone();
                    output.id = 1000 + s.step * 32 + i;
                    output.applied = Some(s.high);
                    if self.has("iceberg") {
                        output.seq = s.high;
                        output.min_seq = s.high;
                    }
                    n.files.push(output);
                }
            }
        }
        n.files.sort_by_key(|f| f.id);
    }
    fn retained(s: &State, low: usize) -> Vec<usize> {
        let mut keep: Vec<_> = s.versions[low..].iter().flatten().copied().collect();
        keep.sort_unstable();
        keep.dedup();
        keep
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut s = State::default();
        if self.has("identity") {
            s.physical.files = vec![0];
            s.named.push((0, s.physical.clone()));
        } else if self.has("tables") {
            s.high = 4;
            s.table_high = [4, 0];
            s.rows.push((0, 0, 4, 1));
            s.oracle[0][0] = Some(1);
        } else if self.has("coverage") {
            s.high = 2;
            s.expected = [true; 2];
            s.files = vec![
                File {
                    id: 0,
                    key: 0,
                    seq: 1,
                    min_seq: 1,
                    applied: None,
                    cold: false,
                },
                File {
                    id: 1,
                    key: 1,
                    seq: 2,
                    min_seq: 2,
                    applied: None,
                    cold: false,
                },
                File {
                    id: 2,
                    key: 0,
                    seq: 1,
                    min_seq: 1,
                    applied: None,
                    cold: true,
                },
            ];
        } else if self.has("pins") {
            s.versions = vec![vec![0]];
            s.objects = vec![0];
        }
        if self.has("coverage") {
            let mut warm_key = s.clone();
            s.files.retain(|f| f.id != 0);
            warm_key.files.retain(|f| f.id != 2);
            warm_key.files[1].cold = true;
            vec![s, warm_key]
        } else {
            vec![s]
        }
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if self.has("identity") {
            self.identity_actions(s, a);
        } else if self.has("tables") {
            if s.step < self.bound && s.refused.is_none() {
                for t in 0..2 {
                    for k in 0..2 {
                        a.extend([
                            Action::TableRecord(t, k, false),
                            Action::TableRecord(t, k, true),
                        ]);
                    }
                }
                a.extend([Action::Move, Action::Both]);
            }
            if s.refused.is_some() {
                a.push(Action::RetryRefused);
            }
        } else if self.has("coverage") {
            if s.step < self.bound {
                for k in 0..2 {
                    if s.expected[k] {
                        a.push(Action::DeleteKey(k));
                    }
                    a.push(Action::UpsertKey(k));
                }
                for mask in 1..1 << s.files.len() {
                    if s.files
                        .iter()
                        .enumerate()
                        .any(|(i, f)| !f.cold && mask & (1 << i) != 0)
                    {
                        a.push(Action::Compact(mask));
                    }
                }
                if s.files.iter().any(|f| f.cold) {
                    a.push(Action::Promote);
                }
                if self.has("adopted") && !s.tombstones.is_empty() {
                    a.push(Action::Clear);
                }
            }
        } else if self.has("pins") {
            if s.step < self.bound {
                a.push(Action::Advance);
            }
            if s.candidate.is_none() && s.queries.is_empty() {
                a.push(Action::Choose);
            }
            if s.candidate.is_some() && s.pin.is_none() {
                a.push(Action::Pin);
            }
            if s.pin.is_some() && !s.validated {
                a.push(Action::Recheck);
            }
            if s.validated && s.queries.len() == 1 && s.step > s.queries[0] {
                a.push(Action::SecondQuery);
            }
            if s.queries.len() == 2 {
                a.push(Action::Renew);
            }
            if s.floor < s.step {
                a.push(Action::Floor);
            }
            if s.scan.is_none() && s.floor > 0 {
                a.push(Action::Scan);
            }
            if s.scan.is_some() {
                a.push(Action::Collect);
            }
        } else if self.has("index") {
            if s.step < self.bound {
                for k in 0..2 {
                    a.push(Action::SetKey(k, !s.keys[k]));
                }
            }
            if s.index.is_none() {
                a.push(Action::Index);
            }
            if s.step > s.floor {
                a.push(Action::Checkpoint);
            }
            if s.floor > s.collected {
                a.push(Action::Collect);
            }
            a.push(Action::Recover);
        }
        a.retain(|a| self.next_state(s, a.clone()).is_some());
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Mutation => {
                next.high += 2;
                next.physical.files.push(next.step + 1);
                next.physical.tombstones.push(next.high - 1);
                next.step += 1;
            }
            Action::Maintenance => {
                next.physical.files = vec![10 + next.step];
                next.step += 1;
            }
            Action::Metadata => {
                next.physical.schema += 1;
                next.step += 1;
            }
            Action::Checkpoint => {
                if self.has("identity") {
                    next.checkpoints
                        .push((self.state_name(state), state.physical.clone()));
                } else {
                    next.floor = state.step;
                    next.checkpoint = Some((state.step, state.keys));
                    if self.has("adopted") && !self.has("no-refresh") {
                        next.index = if self.has("drop") {
                            None
                        } else {
                            Some((state.step, state.keys))
                        };
                    }
                }
            }
            Action::TableRecord(t, k, v) => self.table_record(state, &mut next, &[(t, k, v)]),
            Action::Move => {
                self.table_record(state, &mut next, &[(0, 0, false), (1, 0, true)]);
                next.moved |= next.step > state.step;
            }
            Action::Both => self.table_record(state, &mut next, &[(0, 1, true), (1, 1, true)]),
            Action::RetryRefused => {
                return None;
            }
            Action::UpsertKey(k) => {
                next.high += 1;
                next.tombstones.push((k, next.high));
                next.high += 1;
                next.files.push(File {
                    id: 100 + state.step,
                    key: k,
                    seq: next.high,
                    min_seq: next.high,
                    applied: None,
                    cold: false,
                });
                next.expected[k] = true;
                next.step += 1;
            }
            Action::DeleteKey(k) => {
                next.high += 1;
                next.tombstones.push((k, next.high));
                next.expected[k] = false;
                next.step += 1;
            }
            Action::Compact(mask) => {
                self.rewrite(state, &mut next, mask, false);
                self.clear_covered(&mut next, state.high);
                next.step += 1;
            }
            Action::Promote => {
                self.rewrite(state, &mut next, usize::MAX, true);
                next.step += 1;
            }
            Action::Clear => {
                self.clear_covered(&mut next, state.high);
                next.step += 1;
            }
            Action::Advance => {
                next.step += 1;
                // File 1 exists only at version 1, between the two outer states.
                let files = if next.step == 1 { vec![0, 1] } else { vec![0] };
                for f in &files {
                    if !next.objects.contains(f) {
                        next.objects.push(*f);
                    }
                }
                next.versions.push(files);
            }
            Action::Choose => next.candidate = Some(state.step),
            Action::Pin => next.pin = state.candidate,
            Action::Recheck => {
                next.validated = true;
                if crate::core::pin_admitted(state.pin?, state.floor) || self.has("no-recheck") {
                    next.queries.push(state.candidate?);
                }
            }
            Action::SecondQuery => next.queries.push(state.step),
            Action::Renew => {
                next.pin = if self.has("no-active-low") {
                    Some(state.step)
                } else {
                    state.queries.iter().min().copied()
                };
            }
            Action::Floor => next.floor = state.step,
            Action::Scan => {
                let mut keep = Self::retained(state, state.floor);
                if let Some(v) = state.pin {
                    if self.has("adopted") {
                        keep = Self::retained(state, v.min(state.floor));
                    } else {
                        keep.extend(&state.versions[v]);
                    }
                }
                next.scan = Some(
                    state
                        .objects
                        .iter()
                        .filter(|f| !keep.contains(f))
                        .copied()
                        .collect(),
                );
            }
            Action::Collect => {
                if self.has("pins") {
                    next.objects
                        .retain(|f| !state.scan.as_ref().is_some_and(|v| v.contains(f)));
                } else {
                    next.records.drain(..state.floor - state.collected);
                    next.collected = state.floor;
                }
            }
            Action::SetKey(k, v) => {
                next.records.push((k, v));
                next.keys[k] = v;
                next.step += 1;
            }
            Action::Index => {
                next.index = Some(if self.has("adopted") {
                    state.checkpoint.unwrap_or((0, [false; 2]))
                } else {
                    (state.step, state.keys)
                });
            }
            Action::Recover => {
                let (base, mut keys) = state.index.or(state.checkpoint).unwrap_or((0, [false; 2]));
                for (i, &(k, v)) in state.records.iter().enumerate() {
                    if i + state.collected >= base {
                        keys[k] = v;
                    }
                }
                next.recovered = Some(keys);
            }
        }
        if self.has("identity") && next.step > state.step {
            next.named
                .push((self.state_name(&next), next.physical.clone()));
        }
        // Recovery is a fresh observation; subsequent mutations invalidate only this observation.
        if !matches!(action, Action::Recover) {
            next.recovered = None;
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        if self.has("identity") {
            vec![
                Property::always("state name determines physical state", |_, s: &State| {
                    s.named
                        .iter()
                        .all(|(v, p)| s.named.iter().all(|(w, q)| v != w || p == q))
                }),
                Property::always(
                    "state name determines live files and tombstones",
                    |_, s: &State| {
                        s.named.iter().all(|(v, p)| {
                            s.named.iter().all(|(w, q)| {
                                v != w || (p.files == q.files && p.tombstones == q.tombstones)
                            })
                        })
                    },
                ),
                Property::always("I8 checkpoint name determines contents", |_, s: &State| {
                    s.checkpoints
                        .iter()
                        .all(|(v, p)| s.checkpoints.iter().all(|(w, q)| v != w || p == q))
                }),
            ]
        } else if self.has("tables") {
            vec![
                Property::always("at most one live row per table key", |_, s: &State| {
                    (0..2).all(|t| {
                        (0..2).all(|k| {
                            s.rows
                                .iter()
                                .filter(|&&(a, b, q, _)| {
                                    a == t
                                        && b == k
                                        && !s
                                            .deletes
                                            .iter()
                                            .any(|&(c, d, r)| c == t && d == k && r >= q)
                                })
                                .count()
                                <= 1
                        })
                    })
                }),
                Property::always(
                    "per-table allocated record never refused",
                    |_, s: &State| s.refused.is_none(),
                ),
                Property::always("I9 tables equal serial oracle", |_, s: &State| {
                    (0..2).all(|t| {
                        (0..2).all(|k| {
                            let visible = s
                                .rows
                                .iter()
                                .filter(|&&(a, b, q, _)| {
                                    a == t
                                        && b == k
                                        && !s
                                            .deletes
                                            .iter()
                                            .any(|&(c, d, r)| c == t && d == k && r >= q)
                                })
                                .max_by_key(|r| r.2)
                                .map(|r| r.3);
                            visible == s.oracle[t][k]
                        })
                    })
                }),
                Property::always("per-table record ranges increase", |_, s: &State| {
                    let mut high = [4, 0];
                    for ranges in &s.ranges {
                        for t in 0..2 {
                            if let Some((lo, hi)) = ranges[t] {
                                if lo <= high[t] || lo > hi {
                                    return false;
                                }
                                high[t] = hi;
                            }
                        }
                    }
                    high == s.table_high
                }),
                Property::sometimes("key move committed", |_, s: &State| {
                    s.moved && s.oracle[0][0].is_none() && s.oracle[1][0].is_some()
                }),
                Property::sometimes("both tables touched", |_, s: &State| {
                    s.oracle[0][1].is_some() && s.oracle[1][1].is_some()
                }),
            ]
        } else if self.has("coverage") {
            vec![
                Property::always("at most one live row per table key", |_, s: &State| {
                    (0..2).all(|key| {
                        s.files
                            .iter()
                            .filter(|f| {
                                f.key == key
                                    && !s.tombstones.iter().any(|&(k, d)| k == key && d >= f.seq)
                            })
                            .count()
                            <= 1
                    })
                }),
                Property::always("I9 coverage equals serial oracle", |_, s: &State| {
                    Self::coverage_visible(s) == s.expected
                }),
                Property::always("I9 cold file obeys logical deletion", |_, s: &State| {
                    s.expected[0]
                        || !s.files.iter().any(|f| {
                            f.cold
                                && f.key == 0
                                && !s
                                    .tombstones
                                    .iter()
                                    .any(|&(key, seq)| key == 0 && seq >= f.seq)
                        })
                }),
                Property::sometimes("cold promotion completes", |_, s: &State| {
                    !s.files.iter().any(|f| f.cold)
                }),
            ]
        } else if self.has("pins") {
            vec![
                Property::always("I7 every active query readable", |_, s: &State| {
                    s.queries
                        .iter()
                        .all(|&v| s.versions[v].iter().all(|f| s.objects.contains(f)))
                }),
                Property::sometimes("two queries separated by removed file", |_, s: &State| {
                    s.queries.len() == 2 && s.queries[0] == 0 && s.queries[1] == 1 && s.floor > 1
                }),
            ]
        } else {
            vec![
                Property::always(
                    "checkpoint index reference aligned",
                    |model, state: &State| {
                        !model.has("adopted")
                            || model.has("no-refresh")
                            || state.index.is_none_or(|(base, _)| base == state.floor)
                    },
                ),
                Property::always("I8 recovered index equals full key set", |_, s: &State| {
                    s.recovered.is_none_or(|k| k == s.keys)
                }),
            ]
        }
    }
}
