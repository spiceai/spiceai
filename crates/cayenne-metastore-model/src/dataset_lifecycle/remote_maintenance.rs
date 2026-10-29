// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Independent maintenance snapshots and exact-base publication beside one ingest buffer.
use super::{Data, File, Keys, Op, Protocol as Local, Record, Rows};
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Pending {
    base: usize,
    generation: usize,
    record: Record,
    checkpoint: Option<(usize, Data, Keys)>,
    retried: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol witnesses"
)]
pub(crate) struct State {
    initial: Data,
    data: Data,
    versions: Vec<Data>,
    expected: Vec<Rows>,
    log: Vec<Record>,
    objects: Vec<usize>,
    allocator: [usize; 2],
    buffer: Vec<Op>,
    draws: Vec<Op>,
    retried_ops: Vec<Op>,
    writer: Option<(usize, Vec<Op>)>,
    drawn: usize,
    pending: Vec<Option<Pending>>,
    plans: Vec<usize>,
    completed: Vec<usize>,
    generation: Vec<usize>,
    checkpoints: Vec<(usize, Data, Keys)>,
    decision: Option<bool>,
    prepared: bool,
    refused: bool,
    original_retry: bool,
    maintenance_retry: bool,
    clear_retry: bool,
    stale_publication: bool,
    replanned: bool,
    fenced: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Buffer(usize, usize, bool),
    Acknowledge,
    PublishWriter,
    RetryWriter,
    Prepare,
    Resolve(bool),
    Plan(usize, usize, bool),
    Promote(usize, usize),
    PlanClear(usize),
    PlanCheckpoint(usize),
    PublishMaintenance(usize),
    RetryMaintenance(usize),
    ExpireLease(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, word: &str) -> bool {
        self.mode.contains(word)
    }
    fn local() -> Local {
        Local::new("dataset-lifecycle", 0)
    }
    fn cutoff(&self, state: &State, table: usize) -> usize {
        let high = if self.has("no-committed-cutoff") {
            state.allocator[table]
        } else {
            state.data.high[table]
        };
        state
            .data
            .prepared
            .iter()
            .filter(|o| o.table == table)
            .map(|o| o.delete - 1)
            .min()
            .map_or(high, |cap| high.min(cap))
    }
    fn empty(state: &State) -> Record {
        Record {
            add: vec![],
            remove: vec![],
            deletes: vec![],
            clear: vec![],
            high: state.data.high,
            page: state.data.page,
            prepare: None,
            resolve: false,
            decision: None,
            ops: vec![],
        }
    }
    fn mutation(state: &State, ops: &[Op]) -> Record {
        let mut r = Self::empty(state);
        r.ops = ops.to_vec();
        for o in ops {
            r.deletes.push((o.table, o.key, o.delete));
            r.high[o.table] = r.high[o.table].max(o.insert);
            if let Some(value) = o.value {
                r.add.push(File {
                    id: 1000 + value,
                    table: o.table,
                    key: o.key,
                    value,
                    seq: o.insert,
                    min: o.insert,
                    applied: None,
                    cold: false,
                });
            }
        }
        r
    }
    fn accept(state: &mut State, r: Record) {
        let mut oracle = *state.expected.last().unwrap_or(&state.initial.rows());
        for o in &r.ops {
            oracle[o.table][o.key] = o.value;
        }
        state.objects.extend(r.add.iter().map(|f| f.id));
        if let Some(ops) = &r.prepare {
            state
                .objects
                .extend(ops.iter().filter_map(|o| o.value.map(|v| 1000 + v)));
        }
        state.objects.sort_unstable();
        state.objects.dedup();
        state.data.apply(&r);
        state.log.push(r);
        state.versions.push(state.data.clone());
        state.expected.push(oracle);
    }
    fn guarded(&self, state: &State, node: usize, p: &Pending) -> bool {
        if p.generation != state.generation[node] {
            return false;
        }
        if !p
            .record
            .remove
            .iter()
            .all(|id| state.data.files.iter().any(|f| f.id == *id))
        {
            return false;
        }
        if p.record
            .add
            .iter()
            .any(|f| f.applied.is_some_and(|d| d > self.cutoff(state, f.table)))
        {
            return false;
        }
        let mut after = state.data.clone();
        let mut replacement = p.record.clone();
        replacement.clear.clear();
        after.apply(&replacement);
        let covered = Self::local().clear(&after);
        p.record
            .clear
            .iter()
            .all(|d| covered.contains(d) && d.2 <= self.cutoff(state, d.0))
    }
    fn available(&self, state: &State, node: usize, r: &Record) -> bool {
        self.has("no-file-set-lease")
            || !state.pending.iter().enumerate().any(|(i, p)| {
                i != node
                    && p.as_ref().is_some_and(|p| {
                        p.generation == state.generation[i]
                            && p.record.remove.iter().any(|id| r.remove.contains(id))
                    })
            })
    }
    fn stage(
        &self,
        state: &mut State,
        node: usize,
        mut r: Record,
        checkpoint: Option<(usize, Data, Keys)>,
    ) {
        if self.has("no-stable-high-water") {
            for (t, high) in r.high.iter_mut().enumerate() {
                *high = (*high).max(state.allocator[t]) + 1;
            }
        }
        state.pending[node] = Some(Pending {
            base: state.log.len(),
            generation: state.generation[node],
            record: r,
            checkpoint,
            retried: false,
        });
        state.plans[node] += 1;
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        Self::local()
            .init_states()
            .into_iter()
            .take(if self.has("minimal") || self.has("clear-retry") {
                1
            } else {
                2
            })
            .map(|d| {
                let nodes = if self.has("two-maintainers") || self.has("no-file-set-lease") {
                    2
                } else {
                    1
                };
                State {
                    initial: d.data.clone(),
                    data: d.data.clone(),
                    versions: vec![d.data.clone()],
                    expected: vec![d.data.rows()],
                    log: vec![],
                    objects: d.present,
                    allocator: d.allocator,
                    buffer: vec![],
                    draws: vec![],
                    retried_ops: vec![],
                    writer: None,
                    drawn: 0,
                    pending: vec![None; nodes],
                    plans: vec![0; nodes],
                    completed: vec![0; nodes],
                    generation: vec![0; nodes],
                    checkpoints: vec![],
                    decision: None,
                    prepared: false,
                    refused: false,
                    original_retry: false,
                    maintenance_retry: false,
                    clear_retry: false,
                    stale_publication: false,
                    replanned: false,
                    fenced: false,
                }
            })
            .collect()
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        if state.log.len() < self.bound {
            if state.writer.is_none() && state.drawn < if self.has("minimal") { 1 } else { 2 } {
                for t in 0..if self.has("minimal") { 1 } else { 2 } {
                    for k in 0..2 {
                        action.extend([Action::Buffer(t, k, false), Action::Buffer(t, k, true)]);
                    }
                }
            }
            if state.writer.is_none() && !state.buffer.is_empty() {
                action.push(Action::Acknowledge);
                if self.has("prepare") && !state.prepared {
                    action.push(Action::Prepare);
                }
            }
            if let Some((base, _)) = &state.writer {
                action.push(if *base == state.log.len() {
                    Action::PublishWriter
                } else {
                    Action::RetryWriter
                });
            }
            if !state.data.prepared.is_empty() && state.writer.is_none() {
                action.extend([Action::Resolve(false), Action::Resolve(true)]);
            }
            for node in 0..state.pending.len() {
                if let Some(p) = &state.pending[node] {
                    action.push(if p.base == state.log.len() {
                        Action::PublishMaintenance(node)
                    } else {
                        Action::RetryMaintenance(node)
                    });
                    if state.generation[node] == 0
                        && (!self.has("minimal") || self.has("publication-fence"))
                    {
                        action.push(Action::ExpireLease(node));
                    }
                } else if state.plans[node] < 2
                    && state.completed[node] < if state.pending.len() == 1 { 2 } else { 1 }
                {
                    for cold in [false, true] {
                        for mask in 1..1 << state.data.files.len() {
                            if state
                                .data
                                .files
                                .iter()
                                .enumerate()
                                .all(|(i, f)| mask & (1 << i) == 0 || f.cold == cold)
                            {
                                action.push(Action::Plan(node, mask, cold));
                                if !cold {
                                    action.push(Action::Promote(node, mask));
                                }
                            }
                        }
                    }
                    action.extend([Action::PlanClear(node), Action::PlanCheckpoint(node)]);
                }
            }
        }
        if self.has("clear-retry") {
            action.retain(|action| match action {
                Action::Buffer(table, key, upsert) => {
                    *key == 0
                        && (state.drawn == 0 && *table == 0 && !upsert
                            || state.drawn == 1 && *table == 1 && *upsert)
                }
                Action::Plan(node, _, cold) => (*node == 0) == *cold,
                Action::Promote(node, _) => *node == 1,
                Action::PlanClear(_) | Action::PlanCheckpoint(_) | Action::ExpireLease(_) => false,
                _ => true,
            });
        }
        crate::prune_for_simulation(self, state, action);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Buffer(t, k, upsert) => {
                if state
                    .data
                    .prepared
                    .iter()
                    .any(|o| o.table == t && o.key == k)
                {
                    return None;
                }
                if state.buffer.is_empty() {
                    next.allocator[t] = next.allocator[t].max(state.data.high[t]);
                }
                next.allocator[t] += 1;
                let delete = next.allocator[t];
                next.allocator[t] += usize::from(upsert);
                next.buffer.push(Op {
                    table: t,
                    key: k,
                    value: upsert.then_some(10 + state.drawn),
                    delete,
                    insert: next.allocator[t],
                });
                next.draws.push(next.buffer.last()?.clone());
                next.drawn += 1;
            }
            Action::Acknowledge => {
                next.writer = Some((state.log.len(), state.buffer.clone()));
                next.buffer.clear();
            }
            Action::RetryWriter => {
                next.writer.as_mut()?.0 = state.log.len();
                // Maintenance changes neither logical keyed dependencies nor allocated stamps.
                next.original_retry = true;
                next.retried_ops.clone_from(&state.writer.as_ref()?.1);
            }
            Action::PublishWriter => {
                let (_, ops) = next.writer.take()?;
                if ops.iter().any(|o| o.delete <= state.data.high[o.table]) {
                    next.refused = true;
                } else {
                    let r = Self::mutation(state, &ops);
                    Self::accept(&mut next, r);
                }
            }
            Action::Prepare => {
                let mut r = Self::empty(state);
                r.prepare = Some(state.buffer.clone());
                for o in &state.buffer {
                    r.high[o.table] = r.high[o.table].max(o.insert);
                }
                next.buffer.clear();
                next.prepared = true;
                Self::accept(&mut next, r);
            }
            Action::Resolve(commit) => {
                let mut r = if commit {
                    Self::mutation(state, &state.data.prepared)
                } else {
                    Self::empty(state)
                };
                r.resolve = true;
                r.decision = Some(commit);
                next.decision = Some(commit);
                Self::accept(&mut next, r);
            }
            Action::Plan(node, mask, cold) => {
                let mut r = Self::empty(state);
                for (i, f) in state.data.files.iter().enumerate() {
                    if mask & (1 << i) == 0 || f.cold != cold {
                        continue;
                    }
                    r.remove.push(f.id);
                    if !state.data.deletes.iter().any(|&(t, k, d)| {
                        t == f.table && k == f.key && d >= f.seq && d <= self.cutoff(state, t)
                    }) {
                        let mut out = f.clone();
                        out.id = 100 + node * 100 + state.plans[node] * 16 + i;
                        out.applied = Some(self.cutoff(state, f.table));
                        r.add.push(out);
                    }
                }
                if r.remove.is_empty() || !self.available(state, node, &r) {
                    return None;
                }
                let mut after = state.data.clone();
                after.apply(&r);
                r.clear = Self::local()
                    .clear(&after)
                    .into_iter()
                    .filter(|d| d.2 <= self.cutoff(state, d.0))
                    .collect();
                self.stage(&mut next, node, r, None);
            }
            Action::Promote(node, mask) => {
                next = self.next_state(state, Action::Plan(node, mask, false))?;
                for output in &mut next.pending[node].as_mut()?.record.add {
                    output.cold = true;
                }
            }
            Action::PlanClear(node) => {
                let mut r = Self::empty(state);
                r.clear = Self::local()
                    .clear(&state.data)
                    .into_iter()
                    .filter(|d| d.2 <= self.cutoff(state, d.0))
                    .collect();
                if r.clear.is_empty() {
                    return None;
                }
                self.stage(&mut next, node, r, None);
            }
            Action::PlanCheckpoint(node) => {
                if !state.checkpoints.is_empty() {
                    return None;
                }
                self.stage(
                    &mut next,
                    node,
                    Self::empty(state),
                    Some((state.log.len(), state.data.clone(), state.data.keys())),
                );
            }
            Action::RetryMaintenance(node) => {
                let p = state.pending[node].as_ref()?;
                if self.has("no-guard-revalidation") || self.guarded(state, node, p) {
                    let p = next.pending[node].as_mut()?;
                    p.base = state.log.len();
                    for t in 0..2 {
                        p.record.high[t] = p.record.high[t].max(state.data.high[t]);
                    }
                    p.retried = true;
                } else {
                    next.pending[node] = None;
                    next.replanned = true;
                    next.fenced |= p.generation != state.generation[node];
                }
            }
            Action::PublishMaintenance(node) => {
                let p = next.pending[node].take()?;
                if p.generation != state.generation[node] {
                    next.fenced = true;
                    if !self.has("no-publication-fence") {
                        return Some(next);
                    }
                    next.stale_publication = true;
                }
                next.maintenance_retry |= p.retried;
                next.clear_retry |= p.retried && !p.record.clear.is_empty();
                if let Some(checkpoint) = p.checkpoint {
                    next.checkpoints.push(checkpoint);
                }
                Self::accept(&mut next, p.record);
                next.completed[node] += 1;
            }
            Action::ExpireLease(node) => {
                next.generation[node] += 1;
            }
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "I3 remote maintenance publication fenced",
                |_, state: &State| !state.stale_publication,
            ),
            Property::always("I9 remote maintenance table rows", |_, state: &State| {
                state
                    .versions
                    .iter()
                    .zip(&state.expected)
                    .all(|(d, r)| d.rows() == *r)
            }),
            Property::always("at most one live row per table key", |_, state: &State| {
                state.versions.iter().all(Data::unique_keys)
            }),
            Property::always("writer original sequences accepted", |_, state: &State| {
                !state.refused
                    && state.log.iter().all(|record| {
                        record
                            .ops
                            .iter()
                            .chain(record.prepare.iter().flatten())
                            .all(|operation| state.draws.contains(operation))
                    })
            }),
            Property::always("I4 remote maintenance objects", |_, state: &State| {
                state
                    .versions
                    .iter()
                    .all(|d| d.refs().iter().all(|id| state.objects.contains(id)))
            }),
            Property::always(
                "I8 remote maintenance replay and checkpoint",
                |_, state: &State| {
                    let mut data = state.initial.clone();
                    for (i, r) in state.log.iter().enumerate() {
                        data.apply(r);
                        if data != state.versions[i + 1] {
                            return false;
                        }
                    }
                    state.checkpoints.iter().all(|(base, d, k)| {
                        let mut recovered = d.clone();
                        for r in &state.log[*base..] {
                            recovered.apply(r);
                        }
                        *d == state.versions[*base] && *k == d.keys() && recovered == state.data
                    })
                },
            ),
            Property::always("I6 remote Prepare prefix visibility", |_, state: &State| {
                let mut logical = state.initial.rows();
                let mut pending = vec![];
                state.log.iter().enumerate().all(|(i, r)| {
                    if let Some(ops) = &r.prepare {
                        pending.clone_from(ops);
                    }
                    let ops = if r.resolve && r.decision == Some(true) {
                        &pending
                    } else {
                        &r.ops
                    };
                    for o in ops {
                        logical[o.table][o.key] = o.value;
                    }
                    if r.resolve {
                        pending.clear();
                    }
                    logical == state.versions[i + 1].rows()
                })
            }),
            Property::sometimes(
                "writer retries original stamps then commits",
                |_, state: &State| {
                    state.original_retry
                        && state
                            .log
                            .iter()
                            .any(|r| !r.ops.is_empty() && !r.resolve && r.ops == state.retried_ops)
                },
            ),
            Property::sometimes("covered clear retries and publishes", |_, state: &State| {
                state.clear_retry
            }),
            Property::sometimes("maintenance retries guards", |_, state: &State| {
                state.maintenance_retry && state.completed.iter().sum::<usize>() > 0
            }),
            Property::sometimes("maintenance replans invalid guards", |_, state: &State| {
                state.replanned
            }),
            Property::sometimes("maintenance lease fenced", |_, state: &State| state.fenced),
            Property::sometimes("two tables acknowledged together", |_, state: &State| {
                state
                    .log
                    .iter()
                    .any(|r| (0..2).all(|t| r.ops.iter().any(|o| o.table == t)))
            }),
            Property::sometimes("cold promotion publishes", |_, state: &State| {
                state.log.iter().enumerate().any(|(i, record)| {
                    record.remove.iter().any(|id| {
                        state.versions[i]
                            .files
                            .iter()
                            .any(|f| f.id == *id && !f.cold)
                    }) && record.add.iter().any(|f| f.cold)
                })
            }),
            Property::sometimes("overlapping maintenance planned", |_, state: &State| {
                state
                    .pending
                    .first()
                    .and_then(Option::as_ref)
                    .is_some_and(|first| {
                        state
                            .pending
                            .get(1)
                            .and_then(Option::as_ref)
                            .is_some_and(|second| {
                                first
                                    .record
                                    .remove
                                    .iter()
                                    .any(|id| second.record.remove.contains(id))
                            })
                    })
            }),
            Property::sometimes("checkpoint published", |_, state: &State| {
                !state.checkpoints.is_empty()
            }),
            Property::sometimes(
                "maintenance while Prepare unresolved",
                |_, state: &State| {
                    state.log.iter().enumerate().any(|(i, r)| {
                        !r.remove.is_empty() && !state.versions[i].prepared.is_empty()
                    })
                },
            ),
            Property::sometimes("Prepare committed", |_, state: &State| {
                state.decision == Some(true)
            }),
            Property::sometimes("Prepare aborted", |_, state: &State| {
                state.decision == Some(false)
            }),
        ]
    }
}
