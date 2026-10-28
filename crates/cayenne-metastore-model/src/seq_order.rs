// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::Head;
use stateright::{Model, Property};

type Values = [Option<usize>; 2];
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Op {
    key: usize,
    value: Option<usize>,
    delete: i64,
    insert: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Row {
    key: usize,
    value: usize,
    seq: i64,
    range: (i64, i64),
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Data {
    rows: Vec<Row>,
    deletes: Vec<(usize, i64)>,
}
impl Data {
    fn apply(&mut self, ops: &[Op]) {
        let lo = ops
            .iter()
            .map(|o| o.delete.min(o.insert))
            .min()
            .unwrap_or(0);
        let hi = ops
            .iter()
            .map(|o| o.delete.max(o.insert))
            .max()
            .unwrap_or(0);
        for op in ops {
            self.deletes.push((op.key, op.delete));
            if let Some(value) = op.value {
                self.rows.push(Row {
                    key: op.key,
                    value,
                    seq: op.insert,
                    range: (lo, hi),
                });
            }
        }
    }
    fn unique_keys(&self) -> bool {
        (0..2).all(|key| {
            self.rows
                .iter()
                .filter(|r| {
                    r.key == key && !self.deletes.iter().any(|&(k, d)| k == key && d >= r.seq)
                })
                .count()
                <= 1
        })
    }
    fn visible(&self) -> Values {
        std::array::from_fn(|key| {
            self.rows
                .iter()
                .filter(|r| {
                    r.key == key && !self.deletes.iter().any(|&(k, d)| k == key && d >= r.seq)
                })
                .max_by_key(|r| (r.seq, r.value))
                .map(|r| r.value)
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Writer {
    allocator: i64,
    base: usize,
    buffer: Vec<Op>,
    drawn: usize,
    sealed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Maintenance {
    base: usize,
    cutoff: i64,
    fresh: i64,
    inputs: Vec<Row>,
    outputs: Vec<Row>,
    ranges: Vec<(i64, i64)>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Record {
    Write { lo: i64, hi: i64, ops: Vec<Op> },
    Compact(Maintenance),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct State {
    writers: Vec<Writer>,
    log: Vec<Record>,
    high: i64,
    data: Data,
    // All immutable committed-prefix readers remain available. No buffer is published.
    readers: Vec<(Data, Values)>,
    reference: Values,
    maintenance: Option<Maintenance>,
    maintained: usize,
    shifted: bool,
    replanned: bool,
    interleaved: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Buffer(usize, usize, bool),
    Seal(usize),
    Commit(usize),
    Retry(usize),
    StartMaintenance,
    CommitMaintenance,
}
fn compact(data: &mut Data, m: &Maintenance) {
    data.rows.retain(|r| !m.inputs.contains(r));
    data.rows.extend(m.outputs.clone());
    data.rows.sort_by_key(|r| (r.key, r.seq, r.value));
    data.rows.dedup();
    data.deletes.retain(|&(_, seq)| seq > m.cutoff);
}
fn replay(log: &[Record]) -> Data {
    let mut data = initial_data();
    for record in log {
        match record {
            Record::Write { ops, .. } => data.apply(ops),
            Record::Compact(m) => compact(&mut data, m),
        }
    }
    data
}
fn initial_data() -> Data {
    Data {
        rows: vec![
            Row {
                key: 0,
                value: 1,
                seq: 2,
                range: (2, 2),
            },
            Row {
                key: 1,
                value: 2,
                seq: 4,
                range: (4, 4),
            },
        ],
        deletes: vec![],
    }
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, text: &str) -> bool {
        self.mode.contains(text)
    }
    fn head(state: &State) -> Head {
        Head {
            pos: state.log.len(),
            ..Head::default()
        }
    }
    fn save_reader(state: &mut State) {
        state.readers.push((state.data.clone(), state.reference));
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            writers: vec![
                Writer {
                    allocator: 4,
                    base: 0,
                    buffer: vec![],
                    drawn: 0,
                    sealed: false
                };
                if self.has("two-writers") { 2 } else { 1 }
            ],
            log: vec![],
            high: 4,
            data: initial_data(),
            readers: vec![],
            reference: [Some(1), Some(2)],
            maintenance: None,
            maintained: 0,
            shifted: false,
            replanned: false,
            interleaved: false,
        }]
    }
    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        for (i, w) in state.writers.iter().enumerate() {
            if !w.sealed && w.drawn < self.bound {
                for key in 0..2 {
                    actions.extend([Action::Buffer(i, key, false), Action::Buffer(i, key, true)]);
                }
            }
            if !w.sealed && !w.buffer.is_empty() {
                actions.push(Action::Seal(i));
            }
            if w.sealed && !w.buffer.is_empty() {
                if w.base == state.log.len() {
                    actions.push(Action::Commit(i));
                } else {
                    actions.push(Action::Retry(i));
                }
            }
        }
        if state.maintenance.is_none() && state.maintained < 2 {
            actions.push(Action::StartMaintenance);
        }
        if state.maintenance.is_some() {
            actions.push(Action::CommitMaintenance);
        }
        actions.retain(|a| self.next_state(state, a.clone()).is_some());
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Buffer(i, key, upsert) => {
                let w = &mut next.writers[i];
                // A new window starts at an observed head; an existing one keeps its CAS base.
                if w.buffer.is_empty() {
                    w.base = state.log.len();
                    w.allocator = w.allocator.max(state.high);
                }
                w.allocator += 1;
                let delete = w.allocator;
                if upsert {
                    w.allocator += 1;
                }
                w.buffer.push(Op {
                    key,
                    value: upsert.then_some(10 + i * 10 + w.drawn),
                    delete,
                    insert: w.allocator,
                });
                w.drawn += 1;
            }
            Action::Seal(i) => next.writers[i].sealed = true,
            Action::Commit(i) => {
                let w = &mut next.writers[i];
                if !Self::head(state).accepts(Head {
                    pos: w.base,
                    ..Head::default()
                }) {
                    return None;
                }
                let lo = w.buffer.iter().map(|o| o.delete.min(o.insert)).min()?;
                let hi = w.buffer.iter().map(|o| o.delete.max(o.insert)).max()?;
                if lo <= state.high && !self.has("no-shift") && !self.has("maintenance-draw") {
                    return None;
                }
                next.data.apply(&w.buffer);
                // Oracle uses logical operations, never sequence comparisons or physical rows.
                for op in &w.buffer {
                    next.reference[op.key] = op.value;
                }
                next.log.push(Record::Write {
                    lo,
                    hi,
                    ops: w.buffer.clone(),
                });
                next.high = next.high.max(hi);
                w.buffer.clear();
                w.sealed = false;
                Self::save_reader(&mut next);
            }
            Action::Retry(i) => {
                let w = &mut next.writers[i];
                let conflict = state.log[w.base..].iter().any(|r| match r {
                    Record::Write { ops, .. } => {
                        ops.iter().any(|o| w.buffer.iter().any(|b| b.key == o.key))
                    }
                    Record::Compact(_) => false,
                });
                if conflict {
                    // Re-plan the same absolute-value logical operations against the current head.
                    // No relative updates or predicate reads are modeled.
                    let mut seq = w.allocator.max(state.high);
                    for op in &mut w.buffer {
                        seq += 1;
                        op.delete = seq;
                        if op.value.is_some() {
                            seq += 1;
                        }
                        op.insert = seq;
                    }
                    w.allocator = seq;
                    next.replanned = true;
                } else if !self.has("no-shift") && !self.has("maintenance-draw") {
                    let lo = w.buffer.iter().map(|o| o.delete).min()?;
                    let delta = (state.high + 1 - lo).max(0);
                    if self.has("non-uniform")
                        && state.log[w.base..]
                            .iter()
                            .any(|r| matches!(r, Record::Write { .. }))
                    {
                        // Independent operation blocks, assigned in reverse buffer order.
                        // Each upsert still has delete < insert, but cross-operation order is lost.
                        let mut seq = w.allocator.max(state.high);
                        for op in w.buffer.iter_mut().rev() {
                            seq += 1;
                            op.delete = seq;
                            if op.value.is_some() {
                                seq += 1;
                            }
                            op.insert = seq;
                        }
                        w.allocator = seq;
                    } else {
                        for op in &mut w.buffer {
                            op.delete += delta;
                            op.insert += delta;
                        }
                        w.allocator += delta;
                    }
                    next.shifted |= delta > 0;
                }
                w.base = state.log.len();
            }
            Action::StartMaintenance => {
                let fresh = if self.has("maintenance-draw") || self.has("drawn-cutoff") {
                    next.writers[0].allocator = next.writers[0].allocator.max(state.high) + 1;
                    next.writers[0].allocator
                } else {
                    0
                };
                let cutoff = if self.has("drawn-cutoff") {
                    fresh
                } else {
                    state.high
                };
                let values = state.data.visible();
                let outputs = (0..2)
                    .filter_map(|key| {
                        let value = values[key]?;
                        state
                            .data
                            .rows
                            .iter()
                            .filter(|r| r.key == key && r.value == value)
                            .max_by_key(|r| r.seq)
                            .cloned()
                    })
                    .collect();
                let mut ranges: Vec<_> = state.data.rows.iter().map(|r| r.range).collect();
                ranges.sort_unstable();
                ranges.dedup();
                next.maintenance = Some(Maintenance {
                    base: state.log.len(),
                    cutoff,
                    fresh,
                    inputs: state.data.rows.clone(),
                    outputs,
                    ranges,
                });
                next.interleaved |= state.writers.iter().any(|w| !w.buffer.is_empty());
            }
            Action::CommitMaintenance => {
                let m = next.maintenance.take()?;
                // This step abstracts successful atomic rebase, guard validation and CAS.
                // Rebase physical rewrite only if every captured input is still live.
                // Tail writes are independent immutable additions and stay in place.
                if !m.inputs.iter().all(|r| state.data.rows.contains(r)) {
                    return None;
                }
                compact(&mut next.data, &m);
                if self.has("maintenance-draw") {
                    next.high = next.high.max(m.fresh);
                }
                next.log.push(Record::Compact(m));
                next.maintained += 1;
                Self::save_reader(&mut next);
            }
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one live row per table key", |_, s: &State| {
                s.data.unique_keys() && s.readers.iter().all(|(d, _)| d.unique_keys())
            }),
            Property::always(
                "I9 per-key visibility equals commit-order reference",
                |_, s: &State| {
                    s.data.visible() == s.reference
                        && s.readers.iter().all(|(d, r)| d.visible() == *r)
                },
            ),
            Property::always("I8 seq replay equality", |_, s: &State| {
                let d = replay(&s.log);
                d == s.data && (0..s.log.len()).all(|i| replay(&s.log[..=i]) == s.readers[i].0)
            }),
            Property::always("inherited file sequence ranges", |_, s: &State| {
                s.data
                    .rows
                    .iter()
                    .all(|r| r.range.0 <= r.seq && r.seq <= r.range.1)
                    && s.log.iter().all(|record| match record {
                        Record::Write { lo, hi, ops } => {
                            ops.iter().all(|o| *lo <= o.delete && o.insert <= *hi)
                        }
                        Record::Compact(m) => {
                            let mut ranges: Vec<_> = m.inputs.iter().map(|r| r.range).collect();
                            ranges.sort_unstable();
                            ranges.dedup();
                            m.ranges == ranges
                                && m.outputs
                                    .iter()
                                    .all(|r| m.inputs.contains(r) && m.ranges.contains(&r.range))
                        }
                    })
            }),
            Property::sometimes("maintenance interleaves buffered window", |_, s: &State| {
                s.interleaved && s.maintained > 0
            }),
            Property::sometimes("independent loser shifted", |_, s: &State| s.shifted),
            Property::sometimes("conflicting loser replanned", |_, s: &State| s.replanned),
            Property::sometimes("two operations committed in one record", |_, s: &State| {
                s.log
                    .iter()
                    .any(|r| matches!(r, Record::Write { ops, .. } if ops.len() == 2))
            }),
        ]
    }
}
