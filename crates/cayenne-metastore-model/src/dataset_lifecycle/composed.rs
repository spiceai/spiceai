// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared file lifecycle with prepared mutations, reads bounded by retention, or byte-batch recovery.
use super::{
    Action as LocalAction, Data, File, Keys, Op, Protocol as Local, Record, Rows, State as Dataset,
};
use crate::core::{Store, record};
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent bounded protocol facts and reachability witnesses"
)]
pub(crate) struct State {
    dataset: Dataset,
    decision: Option<bool>,
    resolved: bool,
    rewrites: usize,
    promoted: bool,
    cleared: bool,
    clock: usize,
    expires: usize,
    cursor: usize,
    pending: Option<usize>,
    rows: Vec<File>,
    result: Option<Result<Rows, ()>>,
    store: Store,
    captured: Option<Store>,
    pages: Vec<(usize, Data, Keys)>,
    journal: Vec<Record>,
    window_collected: bool,
    maintained_pending: bool,
    gc_while_prepared: bool,
    higher_pending: bool,
    expired_completion: bool,
    missing_read_error: bool,
    deleted_pending: bool,
    collected_pages: usize,
}
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("actual_rows", &self.dataset.data.rows())
            .field("expected_rows", &self.dataset.oracle)
            .field("dataset", &self.dataset)
            .field("decision", &self.decision)
            .field("result", &self.result)
            .field("clock", &self.clock)
            .field("expires", &self.expires)
            .field("store", &self.store)
            .field("captured", &self.captured)
            .field("pages", &self.pages)
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Local(LocalAction),
    RebaseBuffer,
    Prepare,
    Decide(bool),
    Resolve,
    CollectPrepared,
    SelectRows,
    Tick,
    Dispatch,
    Complete,
    Observe,
    Seal(usize),
    PublishFloor(usize),
    CollectStorage,
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
    fn local(&self) -> Local {
        Local::new(
            if self.has("no-index-checkpoint") {
                "dataset-lifecycle-no-index-checkpoint"
            } else {
                "dataset-lifecycle"
            },
            self.bound,
        )
    }
    fn txn(&self) -> bool {
        self.has("prepare")
    }
    fn bytes(&self) -> bool {
        self.has("byte-window")
    }
    /// Whether the query is still within its retention.
    fn retained(&self, s: &State) -> bool {
        !self.has("expiry") || s.clock < s.expires
    }
    fn live(&self, s: &State) -> bool {
        self.retained(s) && s.result != Some(Err(()))
    }
    fn cutoff(&self, s: &State, table: usize) -> usize {
        let high = s.dataset.data.high[table];
        if self.has("cutoff-cap") && !self.has("no-cutoff-cap") {
            s.dataset
                .data
                .prepared
                .iter()
                .filter(|o| o.table == table)
                .map(|o| o.delete - 1)
                .min()
                .map_or(high, |d| high.min(d))
        } else {
            high
        }
    }
    fn rewrite(&self, s: &State, mask: usize, cold: bool) -> Record {
        let mut source = s.dataset.clone();
        source
            .data
            .deletes
            .retain(|&(table, _, seq)| seq <= self.cutoff(s, table));
        let mut r = self.local().rewrite(&source, mask, cold);
        for file in &mut r.add {
            file.applied = Some(self.cutoff(s, file.table));
        }
        r.clear.clear();
        let mut current = s.dataset.data.clone();
        current.apply(&r);
        r.clear = self
            .local()
            .clear(&current)
            .into_iter()
            .filter(|&(table, _, seq)| seq <= self.cutoff(s, table))
            .collect();
        r
    }
    fn mutation(s: &Dataset, ops: &[Op]) -> Record {
        let mut r = Local::empty_record(s);
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
    fn token(r: &Record, position: usize) -> crate::core::Record {
        let large = !r.add.is_empty()
            || !r.remove.is_empty()
            || !r.deletes.is_empty()
            || !r.clear.is_empty();
        record(2, 0, 2 * position + usize::from(large))
    }
    fn accept(&self, n: &mut State, r: Record) -> Option<()> {
        if self.bytes() {
            // The token's identity denotes the entire immutable canonical action payload.
            if !n.store.append(Self::token(&r, n.journal.len()), "") {
                return None;
            }
        }
        n.journal.push(r.clone());
        for o in &r.ops {
            n.dataset.oracle[o.table][o.key] = o.value;
        }
        n.dataset.present.extend(r.add.iter().map(|f| f.id));
        if let Some(ops) = &r.prepare {
            n.dataset
                .present
                .extend(ops.iter().filter_map(|o| o.value.map(|v| 1000 + v)));
        }
        n.dataset.present.sort_unstable();
        n.dataset.present.dedup();
        self.local().accept(&mut n.dataset, r);
        Some(())
    }
    fn scan_files(data: &Data) -> Vec<File> {
        let mut files = data.files.clone();
        files.sort_by_key(|f| (f.key, f.id));
        files
    }
    fn reconstruct(s: &State, view: &Store) -> Option<Data> {
        let mut view = view.clone();
        view.batches.clone_from(&s.store.batches);
        let records = view.decode()?;
        let mut data = if view.floor == 0 {
            s.dataset.initial.clone()
        } else {
            {
                let (_, data, index) = s.pages.iter().find(|(c, _, _)| *c == view.floor)?;
                if *index != data.keys() {
                    return None;
                }
                data.clone()
            }
        };
        for r in &records[view.floor..] {
            data.apply(s.journal.get(r.2 / 2)?);
        }
        Some(data)
    }
    fn recover_retained(s: &State, target: usize) -> Option<Data> {
        let (base, mut data, mut index) = s
            .pages
            .iter()
            .filter(|(c, _, _)| *c <= target)
            .max_by_key(|(c, _, _)| *c)
            .cloned()
            .unwrap_or((0, s.dataset.initial.clone(), s.dataset.initial.keys()));
        for pos in base..target {
            let token = if pos >= s.store.first {
                s.store.window.get(pos - s.store.first)
            } else {
                s.store
                    .batches
                    .iter()
                    .find_map(|(a, b)| pos.checked_sub(*a).and_then(|offset| b.get(offset)))
            }?;
            data.apply(s.journal.get(token.2 / 2)?);
            index = data.keys();
        }
        (index == data.keys()).then_some(data)
    }
    fn reader_rows(&self, s: &State, version: usize) -> Rows {
        let mut rows = s.dataset.versions[version].rows();
        if self.has("no-reader-record") && s.decision == Some(true) {
            for o in &s.dataset.versions[version].prepared {
                rows[o.table][o.key] = o.value;
            }
        }
        rows
    }
    fn logical_prefix(s: &State, end: usize) -> Rows {
        let mut rows = s.dataset.initial.rows();
        let mut pending = vec![];
        for r in &s.journal[..end] {
            if let Some(ops) = &r.prepare {
                pending.clone_from(ops);
            }
            let ops = if r.resolve {
                if r.decision == Some(true) {
                    pending.as_slice()
                } else {
                    &[]
                }
            } else {
                r.ops.as_slice()
            };
            for o in ops {
                rows[o.table][o.key] = o.value;
            }
            if r.resolve {
                pending.clear();
            }
        }
        rows
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        self.local()
            .init_states()
            .into_iter()
            .filter_map(|dataset| {
                let mut state = State {
                    dataset,
                    decision: None,
                    resolved: false,
                    rewrites: 0,
                    promoted: false,
                    cleared: false,
                    clock: 0,
                    expires: 0,
                    cursor: 0,
                    pending: None,
                    rows: vec![],
                    result: None,
                    store: Store::default(),
                    captured: None,
                    pages: vec![],
                    journal: vec![],
                    window_collected: false,
                    maintained_pending: false,
                    gc_while_prepared: false,
                    higher_pending: false,
                    expired_completion: false,
                    missing_read_error: false,
                    deleted_pending: false,
                    collected_pages: 0,
                };
                if self.has("metadata-prefix") {
                    for _ in 0..2 {
                        let r = Local::empty_record(&state.dataset);
                        self.accept(&mut state, r)?;
                    }
                }
                Some(state)
            })
            .collect()
    }
    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        let d = &s.dataset;
        let mut local = vec![];
        let mut observed = d.clone();
        if self.has("expiry") && !self.retained(s) {
            observed.query = None;
        }
        self.local().actions(&observed, &mut local);
        for a in local {
            let keep = match &a {
                LocalAction::Buffer(t, k, _) => {
                    // Two operations; a prepared window may contain either or both.
                    *t == 0
                        && d.buffer.len() < 2
                        && (!self.txn()
                            || self.has("no-write-reservation")
                            || !d.data.prepared.iter().any(|o| o.table == *t && o.key == *k))
                }
                LocalAction::Acknowledge => !d.buffer.is_empty(),
                LocalAction::Compact(_) => s.rewrites < if self.bytes() { 2 } else { 1 },
                LocalAction::Promote => !s.promoted,
                LocalAction::Clear => !s.cleared,
                LocalAction::Checkpoint
                | LocalAction::Floor
                | LocalAction::CollectLog
                | LocalAction::Recover => !self.bytes(),
                LocalAction::Choose => !self.txn() && !self.has("without-readers"),
                LocalAction::RemovePage => self.bytes(),
                LocalAction::SelectRemoved => !self.has("expiry"),
                LocalAction::Delete => true,
                _ => false,
            };
            if keep {
                actions.push(Action::Local(a));
            }
        }
        if d.buffer.iter().any(|o| o.delete <= d.data.high[o.table]) {
            actions.push(Action::RebaseBuffer);
        }
        if self.txn()
            && d.head.pos < self.bound
            && !d.buffer.is_empty()
            && d.data.prepared.is_empty()
            && !s.resolved
        {
            actions.push(Action::Prepare);
        }
        if self.has("no-prepare-roots")
            && d.data
                .prepared
                .iter()
                .any(|o| o.value.is_some_and(|v| d.present.contains(&(1000 + v))))
        {
            actions.push(Action::CollectPrepared);
        }
        if !d.data.prepared.is_empty() {
            if s.decision.is_none() {
                actions.extend([Action::Decide(false), Action::Decide(true)]);
            }
            if s.decision.is_some() && d.head.pos < self.bound {
                actions.push(Action::Resolve);
            }
        }
        if self.has("expiry") {
            if d.head.floor > 0 && d.selected.is_none() {
                actions.push(Action::SelectRows);
            }
            if s.clock < 2 {
                actions.push(Action::Tick);
            }
            if !d.queries.is_empty() && s.result.is_none() && s.pending.is_none() {
                actions.push(Action::Dispatch);
            }
            if s.pending.is_some() && s.result.is_none() {
                actions.push(Action::Complete);
            }
        }
        if self.bytes() {
            actions.push(Action::Observe);
            for j in 0..s.store.pending.len() {
                actions.push(Action::Seal(j));
            }
            if s.pages.len() < 2 {
                for c in 1..=s.store.first {
                    actions.push(Action::PublishFloor(c));
                }
                if self.has("no-sealed-floor") && !s.store.window.is_empty() {
                    let (cut, final_batch) = s.store.cut();
                    if final_batch {
                        actions.push(Action::PublishFloor(s.store.first + cut));
                    }
                }
            }
            if !s.window_collected && !s.pages.is_empty() {
                actions.push(Action::CollectStorage);
            }
        }
        actions.retain(|a| self.next_state(s, a.clone()).is_some());
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::RebaseBuffer => {
                for t in 0..2 {
                    if let Some(lo) = s
                        .dataset
                        .buffer
                        .iter()
                        .filter(|o| o.table == t)
                        .map(|o| o.delete)
                        .min()
                    {
                        let delta = (s.dataset.data.high[t] + 1).saturating_sub(lo);
                        for o in &mut n.dataset.buffer {
                            if o.table == t {
                                o.delete += delta;
                                o.insert += delta;
                            }
                        }
                        n.dataset.allocator[t] += delta;
                    }
                }
            }
            Action::Prepare => {
                let mut r = Local::empty_record(&s.dataset);
                r.prepare = Some(s.dataset.buffer.clone());
                for o in &s.dataset.buffer {
                    r.high[o.table] = r.high[o.table].max(o.insert);
                }
                n.dataset.buffer.clear();
                self.accept(&mut n, r)?;
            }
            Action::Decide(commit) => n.decision = Some(commit),
            Action::Resolve => {
                let ops = &s.dataset.data.prepared;
                let mut r = if s.decision? {
                    Self::mutation(&s.dataset, ops)
                } else {
                    Local::empty_record(&s.dataset)
                };
                r.resolve = true;
                r.decision = s.decision;
                self.accept(&mut n, r)?;
                n.resolved = true;
                for t in 0..2 {
                    n.dataset.allocator[t] = n.dataset.allocator[t].max(n.dataset.data.high[t]);
                }
            }
            Action::Local(a) => {
                let mut base = s.dataset.clone();
                if self.has("expiry") && !self.retained(s) {
                    base.query = None;
                }
                if matches!(a, LocalAction::Acknowledge) {
                    if s.dataset
                        .buffer
                        .iter()
                        .any(|o| o.delete <= s.dataset.data.high[o.table])
                    {
                        return None;
                    }
                    let r = Self::mutation(&s.dataset, &s.dataset.buffer);
                    n.dataset.buffer.clear();
                    self.accept(&mut n, r)?;
                } else if let LocalAction::Compact(mask) = a {
                    let r = self.rewrite(s, mask, false);
                    self.accept(&mut n, r)?;
                    n.rewrites += 1;
                    n.maintained_pending |= !s.dataset.data.prepared.is_empty();
                    n.higher_pending |= s.dataset.data.prepared.len() == 1
                        && s.dataset
                            .data
                            .prepared
                            .iter()
                            .any(|o| o.value.is_none() && self.cutoff(s, o.table) > o.delete);
                } else if matches!(a, LocalAction::Promote) {
                    let r = self.rewrite(s, usize::MAX, true);
                    self.accept(&mut n, r)?;
                    n.promoted = true;
                    n.maintained_pending |= !s.dataset.data.prepared.is_empty();
                    n.higher_pending |= s.dataset.data.prepared.len() == 1
                        && s.dataset
                            .data
                            .prepared
                            .iter()
                            .any(|o| o.value.is_none() && self.cutoff(s, o.table) > o.delete);
                } else if matches!(a, LocalAction::RemovePage) {
                    let mut r = Local::empty_record(&s.dataset);
                    r.page = None;
                    self.accept(&mut n, r)?;
                } else if matches!(a, LocalAction::Clear) {
                    let mut r = Local::empty_record(&s.dataset);
                    r.clear = self
                        .local()
                        .clear(&s.dataset.data)
                        .into_iter()
                        .filter(|&(t, _, d)| d <= self.cutoff(s, t))
                        .collect();
                    if r.clear.is_empty() {
                        return None;
                    }
                    self.accept(&mut n, r)?;
                    n.cleared = true;
                } else {
                    if self.has("no-log-expiry") && matches!(a, LocalAction::CollectLog) {
                        base.query = s.dataset.query;
                    }
                    if self.has("no-delete-selection")
                        && matches!(a, LocalAction::Delete)
                        && !self.retained(s)
                    {
                        base.selected = Some(
                            base.removed
                                .iter()
                                .filter(|&&id| !Local::referenced(&base, id))
                                .copied()
                                .collect(),
                        );
                    }
                    n.dataset = self.local().next_state(&base, a.clone())?;
                    if matches!(a, LocalAction::Delete) {
                        n.gc_while_prepared |= !s.dataset.data.prepared.is_empty()
                            && s.dataset
                                .present
                                .iter()
                                .any(|id| !n.dataset.present.contains(id));
                        n.deleted_pending |=
                            s.pending.is_some_and(|id| !n.dataset.present.contains(&id));
                    }
                    if matches!(a, LocalAction::Choose) {
                        n.expires = s.clock + 1;
                    }
                    if matches!(a, LocalAction::Choose) && self.bytes() {
                        let mut view = s.store.clone();
                        view.batches.clear();
                        view.pending.clear();
                        n.captured = Some(view);
                    }
                    // Expiry releases retention, not the query's immutable snapshot identity.
                    n.dataset.query = if matches!(a, LocalAction::Choose) {
                        n.dataset.query
                    } else {
                        s.dataset.query
                    };
                }
            }
            Action::CollectPrepared => {
                let ids: Vec<_> = s
                    .dataset
                    .data
                    .prepared
                    .iter()
                    .filter_map(|o| o.value.map(|v| 1000 + v))
                    .collect();
                n.dataset.present.retain(|id| !ids.contains(id));
            }
            Action::SelectRows => {
                let mut observed = s.dataset.clone();
                if !self.retained(s) && !self.has("no-delete-selection") {
                    observed.query = None;
                }
                n.dataset.selected = Some(
                    observed
                        .removed
                        .iter()
                        .filter(|&&id| !Local::referenced(&observed, id))
                        .copied()
                        .collect(),
                );
            }
            Action::Tick => {
                n.clock += 1;
            }
            Action::Dispatch => {
                let v = *s.dataset.queries.first()?;
                if !self.live(s) {
                    if s.dataset.head.floor > v {
                        n.result = Some(Err(()));
                    } else {
                        return None;
                    }
                } else if let Some(f) = Self::scan_files(&s.dataset.versions[v]).get(s.cursor) {
                    n.pending = Some(f.id);
                } else {
                    n.result = Some(Ok(s.dataset.versions[v].rows()));
                }
            }
            Action::Complete => {
                if s.result.is_some() {
                    return None;
                }
                n.expired_completion |= !self.retained(s);
                let id = n.pending.take()?;
                let v = *s.dataset.queries.first()?;
                if !s.dataset.present.contains(&id) && !self.has("missing-as-empty") {
                    n.missing_read_error = true;
                    n.result = Some(Err(()));
                } else {
                    if s.dataset.present.contains(&id) {
                        n.rows
                            .push(Self::scan_files(&s.dataset.versions[v])[s.cursor].clone());
                    }
                    n.cursor += 1;
                    if n.cursor == s.dataset.versions[v].files.len() {
                        let mut data = s.dataset.versions[v].clone();
                        data.files.clone_from(&n.rows);
                        n.result = Some(Ok(data.rows()));
                    }
                }
            }
            Action::Observe => n.store.observe(""),
            Action::Seal(j) => n.store.create(j),
            Action::PublishFloor(c) => {
                let mut data = s.dataset.initial.clone();
                for r in &s.journal[..c] {
                    data.apply(r);
                }
                if self.has("no-sealed-floor") && c > s.store.first {
                    // The complete checkpoint is durable; older intermediate states still need a batch.
                    n.store.floor = c;
                    n.store.root = s.journal[..c]
                        .iter()
                        .enumerate()
                        .map(|(i, r)| Self::token(r, i))
                        .collect();
                    n.store.first = c;
                    n.store.window.drain(..c - s.store.first);
                    n.store.index.retain(|&(_, end)| end > c);
                } else if !n.store.publish(c, "") {
                    return None;
                }
                let index = if self.has("no-index-checkpoint") {
                    s.dataset.initial.keys()
                } else {
                    data.keys()
                };
                n.pages.push((c, data, index));
                n.dataset.head.floor = c;
            }
            Action::CollectStorage => {
                let low = s.dataset.query.unwrap_or(s.store.floor).min(s.store.floor);
                let keep = s
                    .pages
                    .iter()
                    .filter(|(c, _, _)| *c <= low)
                    .map(|(c, _, _)| *c)
                    .max()
                    .unwrap_or(0);
                n.store.batches.retain(|(a, b)| {
                    a + b.len()
                        > if self.has("no-query-batch") {
                            s.store.floor
                        } else {
                            keep
                        }
                });
                n.pages.retain(|(c, _, _)| *c >= keep);
                n.collected_pages += s.pages.len() - n.pages.len();
                n.window_collected = true;
            }
        }
        (n != *s).then_some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one live row per table key", |_, s: &State| {
                s.dataset.data.unique_keys() && s.dataset.versions.iter().all(Data::unique_keys)
            }),
            Property::always("I9 lifecycle logical rows", |_, s: &State| {
                s.dataset.data.rows() == s.dataset.oracle
                    && (0..=s.dataset.head.pos)
                        .all(|v| s.dataset.versions[v].rows() == Self::logical_prefix(s, v))
            }),
            Property::always("I9 delayed delete below prior coverage", |_, s: &State| {
                !s.higher_pending || s.dataset.data.rows() == s.dataset.oracle
            }),
            Property::always("I6 lifecycle prefix visibility", |m, s: &State| {
                let mut data = s.dataset.initial.clone();
                for v in 0..=s.dataset.head.pos {
                    if m.reader_rows(s, v) != data.rows() {
                        return false;
                    }
                    if let Some(r) = s.journal.get(v) {
                        data.apply(r);
                    }
                }
                true
            }),
            Property::always("I8 lifecycle recovered state and PK", |m, s: &State| {
                m.bytes()
                    || Local::recover(&s.dataset, s.dataset.head.pos)
                        == Some((s.dataset.data.clone(), s.dataset.data.keys()))
            }),
            Property::always("I8 lifecycle replay", |_, s: &State| {
                let mut data = s.dataset.initial.clone();
                for (i, r) in s.journal.iter().enumerate() {
                    data.apply(r);
                    if data != s.dataset.versions[i + 1] {
                        return false;
                    }
                }
                data == s.dataset.data
            }),
            Property::always("I4 lifecycle objects", |m, s: &State| {
                let low = if m.live(s) && !s.dataset.queries.is_empty() {
                    Local::low(&s.dataset)
                } else {
                    s.dataset.head.floor
                };
                s.dataset.versions[low..]
                    .iter()
                    .all(|d| d.refs().iter().all(|id| s.dataset.present.contains(id)))
            }),
            Property::always("snapshot rows or error", |_, s: &State| {
                s.result.as_ref().is_none_or(|r| {
                    r.as_ref().map_or(true, |rows| {
                        Some(*rows)
                            == s.dataset
                                .queries
                                .first()
                                .map(|&v| s.dataset.versions[v].rows())
                    })
                })
            }),
            Property::always("I7 lifecycle unexpired query recovery", |m, s: &State| {
                !m.live(s)
                    || s.dataset.queries.is_empty()
                    || m.bytes()
                    || (s.dataset.queries[0]..=s.dataset.head.pos).all(|v| {
                        Local::recover(&s.dataset, v)
                            .is_some_and(|(d, _)| d == s.dataset.versions[v])
                    })
            }),
            Property::always("I4 byte head dependencies", |m, s: &State| {
                !m.bytes()
                    || (s.store.floor == 0 || s.pages.iter().any(|(c, _, _)| *c == s.store.floor))
                        && s.store.index.iter().all(|&(a, b)| {
                            s.store
                                .batches
                                .iter()
                                .any(|(start, records)| *start == a && records.len() == b - a)
                        })
            }),
            Property::always("I8 byte checkpoint PK collection", |_, s: &State| {
                s.pages
                    .iter()
                    .all(|(c, d, index)| *d == s.dataset.versions[*c] && *index == d.keys())
            }),
            Property::always("I8 byte lifecycle recovery", |m, s: &State| {
                !m.bytes() || Self::reconstruct(s, &s.store) == Some(s.dataset.data.clone())
            }),
            Property::always("I7 byte lifecycle query recovery", |m, s: &State| {
                !m.bytes()
                    || s.dataset.queries.is_empty()
                    || s.captured.as_ref().is_some_and(|p| {
                        Some(p.first + p.window.len()) == s.dataset.queries.first().copied()
                            && (s.dataset.queries[0]..=s.dataset.head.pos).all(|v| {
                                Self::recover_retained(s, v) == Some(s.dataset.versions[v].clone())
                            })
                    })
            }),
            Property::sometimes("warm subset rewrite", |_, s: &State| {
                s.journal
                    .iter()
                    .any(|r| r.remove.contains(&2) && !r.remove.contains(&3))
            }),
            Property::sometimes("cold promotion", |_, s: &State| {
                s.journal.iter().any(|r| r.remove.contains(&4))
            }),
            Property::sometimes("row object collected", |_, s: &State| {
                s.dataset.removed.contains(&2) && !s.dataset.present.contains(&2)
            }),
            Property::sometimes("GC while prepared", |_, s: &State| s.gc_while_prepared),
            Property::sometimes("checkpoint retains prepared actions", |_, s: &State| {
                s.dataset.collected > 0
                    && !s.dataset.data.prepared.is_empty()
                    && s.dataset
                        .checkpoint
                        .as_ref()
                        .is_some_and(|(_, d, _)| !d.prepared.is_empty())
            }),
            Property::sometimes("ordinary write while prepared", |_, s: &State| {
                s.journal.iter().enumerate().any(|(i, r)| {
                    !r.resolve && !r.ops.is_empty() && !s.dataset.versions[i].prepared.is_empty()
                })
            }),
            Property::sometimes("prepared delete commits", |_, s: &State| {
                s.resolved
                    && s.decision == Some(true)
                    && s.journal.iter().any(|r| {
                        r.prepare
                            .as_ref()
                            .is_some_and(|ops| ops.iter().any(|o| o.value.is_none()))
                    })
            }),
            Property::sometimes("prepared upsert commits", |_, s: &State| {
                s.resolved
                    && s.decision == Some(true)
                    && s.journal.iter().any(|r| {
                        r.prepare
                            .as_ref()
                            .is_some_and(|ops| ops.iter().any(|o| o.value.is_some()))
                    })
            }),
            Property::sometimes("two actions prepared together", |_, s: &State| {
                s.journal
                    .iter()
                    .any(|r| r.prepare.as_ref().is_some_and(|ops| ops.len() == 2))
            }),
            Property::sometimes("abort after maintenance", |_, s: &State| {
                s.resolved && s.decision == Some(false) && s.maintained_pending
            }),
            Property::sometimes("prepared commit visible", |_, s: &State| {
                s.resolved && s.decision == Some(true)
            }),
            Property::sometimes("prepared abort discarded", |_, s: &State| {
                s.resolved && s.decision == Some(false)
            }),
            Property::always("abort changes no coverage or rows", |_, s: &State| {
                s.journal.iter().enumerate().all(|(i, r)| {
                    r.decision != Some(false) || {
                        let mut before = s.dataset.versions[i].clone();
                        before.prepared.clear();
                        before == s.dataset.versions[i + 1]
                    }
                })
            }),
            Property::sometimes("higher cutoff while prepared", |_, s: &State| {
                s.dataset.data.prepared.len() == 1
                    && s.dataset.data.prepared.iter().any(|o| {
                        o.value.is_none()
                            && s.dataset.data.files.iter().any(|f| {
                                f.table == o.table && f.applied.is_some_and(|d| d > o.delete)
                            })
                    })
            }),
            Property::sometimes("maintenance while prepared", |_, s: &State| {
                s.maintained_pending
            }),
            Property::sometimes("expired delayed read completes", |_, s: &State| {
                s.expired_completion && s.result.is_some()
            }),
            Property::sometimes("missing delayed object errors", |_, s: &State| {
                s.missing_read_error
            }),
            Property::sometimes("object deleted during dispatched read", |_, s: &State| {
                s.deleted_pending
            }),
            Property::sometimes("checkpoint page collected", |_, s: &State| {
                s.collected_pages > 0
            }),
            Property::sometimes("byte budget evicts records", |_, s: &State| {
                s.store.byte_pressure && s.store.byte_cut
            }),
            Property::sometimes("count budget evicts records", |_, s: &State| {
                s.store.count_pressure && s.store.count_cut
            }),
            Property::sometimes("query replay batch retained", |_, s: &State| {
                let low = s.dataset.query.unwrap_or(s.store.floor).min(s.store.floor);
                let base = s
                    .pages
                    .iter()
                    .filter(|(c, _, _)| *c <= low)
                    .map(|(c, _, _)| *c)
                    .max()
                    .unwrap_or(0);
                s.window_collected
                    && !s.dataset.queries.is_empty()
                    && low < s.store.floor
                    && s.store
                        .batches
                        .iter()
                        .any(|(a, b)| *a >= base && *a + b.len() <= s.store.floor)
            }),
        ]
    }
}
