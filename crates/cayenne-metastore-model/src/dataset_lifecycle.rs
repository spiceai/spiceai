// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! One dataset head joins row visibility, checkpoint recovery and object retirement.
use crate::core::Head;
use stateright::{Model, Property};
pub(crate) mod composed;
pub(crate) mod promotion;
pub(crate) mod remote_maintenance;

type Rows = [[Option<usize>; 2]; 2];
type Keys = [[bool; 2]; 2];
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct File {
    id: usize,
    table: usize,
    key: usize,
    value: usize,
    seq: usize,
    min: usize,
    applied: Option<usize>,
    cold: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Data {
    high: [usize; 2],
    files: Vec<File>,
    deletes: Vec<(usize, usize, usize)>,
    page: Option<usize>,
    prepared: Vec<Op>,
}
impl Data {
    fn unique_keys(&self) -> bool {
        (0..2).all(|table| {
            (0..2).all(|key| {
                self.files
                    .iter()
                    .filter(|f| {
                        f.table == table
                            && f.key == key
                            && !self
                                .deletes
                                .iter()
                                .any(|&(t, k, d)| t == table && k == key && d >= f.seq)
                    })
                    .count()
                    <= 1
            })
        })
    }
    fn rows(&self) -> Rows {
        std::array::from_fn(|table| {
            std::array::from_fn(|key| {
                self.files
                    .iter()
                    .filter(|f| {
                        f.table == table
                            && f.key == key
                            && !self
                                .deletes
                                .iter()
                                .any(|&(t, k, d)| t == table && k == key && d >= f.seq)
                    })
                    .max_by_key(|f| f.seq)
                    .map(|f| f.value)
            })
        })
    }
    fn keys(&self) -> Keys {
        self.rows().map(|table| table.map(|value| value.is_some()))
    }
    fn refs(&self) -> Vec<usize> {
        self.files
            .iter()
            .map(|f| f.id)
            .chain(self.page)
            .chain(
                self.prepared
                    .iter()
                    .filter_map(|o| o.value.map(|v| 1000 + v)),
            )
            .collect()
    }
    fn apply(&mut self, record: &Record) {
        self.files.retain(|f| !record.remove.contains(&f.id));
        self.files.extend(record.add.clone());
        self.files.sort_by_key(|f| f.id);
        self.deletes.extend(&record.deletes);
        self.deletes.retain(|d| !record.clear.contains(d));
        self.high = record.high;
        self.page = record.page;
        if let Some(ops) = &record.prepare {
            self.prepared.clone_from(ops);
        }
        if record.resolve {
            self.prepared.clear();
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Op {
    table: usize,
    key: usize,
    value: Option<usize>,
    delete: usize,
    insert: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Record {
    add: Vec<File>,
    remove: Vec<usize>,
    deletes: Vec<(usize, usize, usize)>,
    clear: Vec<(usize, usize, usize)>,
    high: [usize; 2],
    page: Option<usize>,
    prepare: Option<Vec<Op>>,
    resolve: bool,
    decision: Option<bool>,
    ops: Vec<Op>,
}
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    head: Head,
    initial: Data,
    data: Data,
    versions: Vec<Data>,
    names: Vec<usize>,
    oracle: Rows,
    log: Vec<Record>,
    collected: usize,
    checkpoint: Option<(usize, Data, Keys)>,
    present: Vec<usize>,
    removed: Vec<usize>,
    selected: Option<Vec<usize>>,
    buffer: Vec<Op>,
    allocator: [usize; 2],
    drawn: usize,
    candidate: Option<usize>,
    /// The oldest running query's commit; retention keeps every state from it.
    query: Option<usize>,
    queries: Vec<usize>,
    upload: Option<usize>,
    started: bool,
    staged: Option<(Head, usize)>,
    swept: bool,
    /// Uploads past the age limit: the writer can no longer reference them.
    aged: Vec<usize>,
    late: bool,
    refused: bool,
    recovered: Option<(Data, Keys)>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Buffer(usize, usize, bool),
    Acknowledge,
    Compact(usize),
    Promote,
    Clear,
    Checkpoint,
    Floor,
    CollectLog,
    Recover,
    Choose,
    SecondQuery,
    StagePage,
    PublishPage,
    RemovePage,
    StartUpload,
    FinishUpload,
    Takeover,
    Age(usize),
    Sweep,
    SelectRemoved,
    Delete,
}
/// Fresh names for uploads, one per lease generation.
const UPLOAD: usize = 5;
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
    fn low(s: &State) -> usize {
        s.query.unwrap_or(s.head.floor).min(s.head.floor)
    }
    fn referenced(s: &State, id: usize) -> bool {
        s.versions[Self::low(s)..]
            .iter()
            .any(|v| v.refs().contains(&id))
    }
    fn collect_through(&self, s: &State) -> usize {
        if self.has("no-query-log-retention") {
            return s.head.floor;
        }
        s.checkpoint
            .as_ref()
            .filter(|(base, _, _)| *base <= Self::low(s))
            .map_or(0, |(base, _, _)| *base)
    }
    fn recover(s: &State, target: usize) -> Option<(Data, Keys)> {
        let (base, mut data, mut index) = s
            .checkpoint
            .clone()
            .filter(|(base, _, _)| *base <= s.head.floor && *base <= target)
            .unwrap_or((0, s.initial.clone(), s.initial.keys()));
        if base < s.collected || target > s.head.pos {
            return None;
        }
        for commit in base..target {
            data.apply(s.log.get(commit - s.collected)?);
            index = data.keys();
        }
        Some((data, index))
    }
    fn empty_record(s: &State) -> Record {
        Record {
            add: vec![],
            remove: vec![],
            deletes: vec![],
            clear: vec![],
            high: s.data.high,
            page: s.data.page,
            prepare: None,
            resolve: false,
            decision: None,
            ops: vec![],
        }
    }
    fn accept(&self, n: &mut State, r: Record) {
        let old = n.data.refs();
        n.data.apply(&r);
        for id in old {
            if !n.data.refs().contains(&id) && !n.removed.contains(&id) {
                n.removed.push(id);
            }
        }
        n.removed.sort_unstable();
        n.head.pos += 1;
        n.names.push(if self.has("no-commit-number") {
            *n.data.high.iter().max().unwrap_or(&0)
        } else {
            n.head.pos
        });
        n.versions.push(n.data.clone());
        n.log.push(r);
    }
    fn clear(&self, d: &Data) -> Vec<(usize, usize, usize)> {
        d.deletes
            .iter()
            .filter(|&&(table, _, seq)| {
                self.has("no-coverage")
                    || d.files
                        .iter()
                        .filter(|f| f.table == table && f.min <= seq)
                        .all(|f| f.applied.is_some_and(|c| c >= seq))
            })
            .copied()
            .collect()
    }
    fn rewrite(&self, s: &State, mask: usize, cold: bool) -> Record {
        let mut r = Self::empty_record(s);
        for (i, f) in s.data.files.iter().enumerate() {
            if f.cold == cold && mask & (1 << i) != 0 {
                if !(cold && self.has("no-cold-input-removal")
                    || !cold && self.has("no-warm-input-removal"))
                {
                    r.remove.push(f.id);
                }
                if !s
                    .data
                    .deletes
                    .iter()
                    .any(|&(t, k, d)| t == f.table && k == f.key && d >= f.seq)
                {
                    let mut out = f.clone();
                    out.id = 10 + s.head.pos * 16 + i;
                    out.applied = Some(s.data.high[f.table]);
                    // Promotion keeps the cold tier; warm compaction cannot select it.
                    r.add.push(out);
                }
            }
        }
        let mut d = s.data.clone();
        d.apply(&r);
        r.clear = self.clear(&d);
        r
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let data = Data {
            high: [4, 0],
            files: vec![
                File {
                    id: 2,
                    table: 0,
                    key: 0,
                    value: 1,
                    seq: 2,
                    min: 2,
                    applied: None,
                    cold: false,
                },
                File {
                    id: 3,
                    table: 0,
                    key: 1,
                    value: 2,
                    seq: 4,
                    min: 4,
                    applied: None,
                    cold: false,
                },
                File {
                    id: 4,
                    table: 0,
                    key: 0,
                    value: 1,
                    seq: 2,
                    min: 2,
                    applied: None,
                    cold: true,
                },
            ],
            deletes: vec![],
            page: Some(0),
            prepared: vec![],
        };
        let mut starts = Vec::new();
        for cold_key in 0..2 {
            let mut data = data.clone();
            data.files
                .retain(|f| f.id != if cold_key == 0 { 2 } else { 4 });
            if cold_key == 1 {
                data.files[1].cold = true;
            }
            starts.push(State {
                head: Head::default(),
                initial: data.clone(),
                versions: vec![data.clone()],
                names: vec![if self.has("no-commit-number") { 4 } else { 0 }],
                oracle: data.rows(),
                data,
                log: vec![],
                collected: 0,
                checkpoint: None,
                present: vec![0, 2, 3, 4],
                removed: vec![],
                selected: None,
                buffer: vec![],
                allocator: [4, 0],
                drawn: 0,
                candidate: None,
                query: None,
                queries: vec![],
                upload: None,
                started: false,
                staged: None,
                swept: false,
                aged: vec![],
                late: false,
                refused: false,
                recovered: None,
            });
        }
        starts
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.head.pos < self.bound && !s.refused {
            if s.drawn < 2 {
                for t in 0..2 {
                    for k in 0..2 {
                        a.extend([Action::Buffer(t, k, false), Action::Buffer(t, k, true)]);
                    }
                }
            }
            if !s.buffer.is_empty() {
                a.push(Action::Acknowledge);
            }
            for mask in 1..1 << s.data.files.len() {
                if s.data
                    .files
                    .iter()
                    .enumerate()
                    .all(|(i, f)| !f.cold || mask & (1 << i) == 0)
                {
                    a.push(Action::Compact(mask));
                }
            }
            if s.data.files.iter().any(|f| f.cold) {
                a.push(Action::Promote);
            }
            if !self.clear(&s.data).is_empty() {
                a.push(Action::Clear);
            }
            if s.data.page.is_some() {
                a.push(Action::RemovePage);
            }
            // Every object has a fresh name: a page is staged only from the
            // writer's own upload, never by reusing an object found present.
            let page = UPLOAD + s.head.lease;
            if s.staged.is_none()
                && s.present.contains(&page)
                && s.data.page != Some(page)
                && !s.removed.contains(&page)
                && !s.aged.contains(&page)
            {
                a.push(Action::StagePage);
            }
            if s.staged.is_some() {
                a.push(Action::PublishPage);
            }
            if s.checkpoint.is_none() && s.head.pos > 0 {
                a.push(Action::Checkpoint);
            }
        }
        if s.checkpoint
            .as_ref()
            .is_some_and(|(v, _, _)| *v > s.head.floor)
        {
            a.push(Action::Floor);
        }
        if s.collected < self.collect_through(s) {
            a.push(Action::CollectLog);
        }
        a.push(Action::Recover);
        if s.candidate.is_none() {
            a.push(Action::Choose);
        }
        if s.queries.len() == 1 && s.queries[0] < s.head.pos {
            a.push(Action::SecondQuery);
        }
        if s.head.lease == 0 {
            a.push(Action::Takeover);
        }
        if !s.started {
            a.push(Action::StartUpload);
        }
        if s.upload.is_some() {
            a.push(Action::FinishUpload);
        }
        // Only uploads can be unreferenced without having been removed.
        for id in [UPLOAD, UPLOAD + 1] {
            if s.present.contains(&id)
                && !s.aged.contains(&id)
                && !s.removed.contains(&id)
                && !Self::referenced(s, id)
                && s.staged.is_none_or(|(_, p)| p != id)
            {
                a.push(Action::Age(id));
            }
        }
        if !s.swept {
            a.push(Action::Sweep);
        }
        if s.selected.is_none() {
            a.push(Action::SelectRemoved);
        }
        if s.selected.is_some() {
            a.push(Action::Delete);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        next.recovered = None;
        match action {
            Action::Buffer(t, k, insert) => {
                next.allocator[t] += 1;
                let delete = next.allocator[t];
                if insert {
                    next.allocator[t] += 1;
                }
                next.buffer.push(Op {
                    table: t,
                    key: k,
                    value: insert.then_some(10 + state.drawn),
                    delete,
                    insert: next.allocator[t],
                });
                next.drawn += 1;
            }
            Action::Acknowledge => {
                if state.buffer.iter().any(|o| {
                    o.delete
                        <= if self.has("no-table-scope") {
                            state.data.high[0].max(state.data.high[1])
                        } else {
                            state.data.high[o.table]
                        }
                }) {
                    next.refused = true;
                } else {
                    let mut r = Self::empty_record(state);
                    for (i, o) in state.buffer.iter().enumerate() {
                        r.deletes.push((o.table, o.key, o.delete));
                        r.high[o.table] = o.insert;
                        if let Some(value) = o.value {
                            r.add.push(File {
                                id: 10 + state.head.pos * 16 + i,
                                table: o.table,
                                key: o.key,
                                value,
                                seq: o.insert,
                                min: o.insert,
                                applied: None,
                                cold: false,
                            });
                        }
                        next.oracle[o.table][o.key] = o.value;
                    }
                    next.present.extend(r.add.iter().map(|f| f.id));
                    next.present.sort_unstable();
                    next.present.dedup();
                    next.buffer.clear();
                    self.accept(&mut next, r);
                }
            }
            Action::Compact(mask) => {
                let r = self.rewrite(state, mask, false);
                next.present.extend(r.add.iter().map(|f| f.id));
                next.present.sort_unstable();
                next.present.dedup();
                self.accept(&mut next, r);
            }
            Action::Promote => {
                let r = self.rewrite(state, usize::MAX, true);
                next.present.extend(r.add.iter().map(|f| f.id));
                next.present.sort_unstable();
                next.present.dedup();
                self.accept(&mut next, r);
            }
            Action::Clear => {
                let mut r = Self::empty_record(state);
                r.clear = self.clear(&state.data);
                self.accept(&mut next, r);
            }
            Action::RemovePage => {
                let mut r = Self::empty_record(state);
                r.page = None;
                self.accept(&mut next, r);
            }
            Action::StagePage => next.staged = Some((state.head, UPLOAD + state.head.lease)),
            Action::PublishPage => {
                let (base, id) = next.staged.take()?;
                if state.head.accepts(base) {
                    let mut r = Self::empty_record(state);
                    r.page = Some(id);
                    self.accept(&mut next, r);
                }
            }
            Action::StartUpload => {
                next.started = true;
                next.upload = Some(UPLOAD + state.head.lease);
            }
            Action::FinishUpload => {
                let id = next.upload.take()?;
                if !next.present.contains(&id) {
                    next.present.push(id);
                    next.present.sort_unstable();
                }
                next.late |= id < UPLOAD + state.head.lease && state.swept;
            }
            Action::Takeover => {
                next.head.lease = 1;
                next.buffer.clear();
                next.allocator = state.data.high;
            }
            Action::Checkpoint => {
                next.checkpoint = Some((
                    state.head.pos,
                    state.data.clone(),
                    if self.has("no-index-checkpoint") {
                        state.initial.keys()
                    } else {
                        state.data.keys()
                    },
                ));
            }
            Action::Floor => next.head.floor = state.checkpoint.as_ref()?.0,
            Action::CollectLog => {
                let through = self.collect_through(state);
                next.log.drain(..through - state.collected);
                next.collected = through;
            }
            Action::Recover => next.recovered = Self::recover(state, state.head.pos),
            // A query starts on the current head; retention, not a reader
            // write, keeps its state until it expires.
            Action::Choose => {
                next.candidate = Some(state.head.pos);
                next.query = Some(state.head.pos);
                next.queries.push(state.head.pos);
            }
            Action::SecondQuery => next.queries.push(state.head.pos),
            Action::Age(id) => {
                next.aged.push(id);
                next.aged.sort_unstable();
            }
            Action::Sweep | Action::SelectRemoved => {
                let sweep = matches!(action, Action::Sweep);
                let ids: Vec<_> = state
                    .present
                    .iter()
                    .filter(|&&id| {
                        // The orphan sweep takes only aged uploads. The
                        // retention model's writer age check and head move are
                        // what make an aged upload unreferenceable.
                        (sweep != state.removed.contains(&id))
                            && !Self::referenced(state, id)
                            && (!sweep || self.has("no-upload-age") || state.aged.contains(&id))
                    })
                    .copied()
                    .collect();
                if sweep {
                    next.swept = true;
                }
                if !ids.is_empty() {
                    next.selected = Some(ids);
                }
            }
            Action::Delete => {
                let ids = next.selected.take()?;
                next.present.retain(|id| !ids.contains(id));
            }
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one live row per table key", |_, s: &State| {
                s.data.unique_keys() && s.versions.iter().all(Data::unique_keys)
            }),
            Property::always("I4 composed retained objects", |_, s: &State| {
                (s.head.floor..=s.head.pos).all(|v| {
                    Self::recover(s, v).is_some_and(|(data, _)| {
                        data == s.versions[v] && data.refs().iter().all(|id| s.present.contains(id))
                    })
                })
            }),
            Property::always("I7 composed query snapshots", |_, s: &State| {
                s.queries.is_empty()
                    || s.query.is_some_and(|low| {
                        (low..=s.head.pos).all(|v| {
                            Self::recover(s, v).is_some_and(|(data, index)| {
                                data == s.versions[v]
                                    && index == s.versions[v].keys()
                                    && data.refs().iter().all(|id| s.present.contains(id))
                            })
                        })
                    })
            }),
            Property::always("I8 composed commit names one state", |_, s: &State| {
                s.names.iter().enumerate().all(|(i, v)| {
                    s.names
                        .iter()
                        .enumerate()
                        .all(|(j, w)| v != w || s.versions[i] == s.versions[j])
                })
            }),
            Property::always("I8 composed checkpoint and PK recovery", |_, s: &State| {
                s.recovered.as_ref().is_none_or(|(d, index)| {
                    *d == s.data
                        && *index == s.oracle.map(|table| table.map(|value| value.is_some()))
                })
            }),
            Property::always("I9 composed table rows", |_, s: &State| {
                s.data.rows() == s.oracle
            }),
            Property::always("per-table composed allocation admitted", |_, s: &State| {
                !s.refused
            }),
            Property::sometimes("composed cold promotion", |_, s: &State| {
                s.log.iter().any(|r| r.remove.contains(&4))
            }),
            Property::sometimes("composed warm subset rewrite", |_, s: &State| {
                s.log
                    .iter()
                    .any(|r| r.remove.contains(&2) && !r.remove.contains(&3))
            }),
            Property::sometimes("composed retired object deleted", |_, s: &State| {
                s.removed.contains(&0) && !s.present.contains(&0)
            }),
            Property::sometimes("composed buffered maintenance", |_, s: &State| {
                !s.buffer.is_empty() && s.log.iter().any(|r| !r.remove.is_empty())
            }),
            Property::sometimes("composed parent and child record", |_, s: &State| {
                s.log.iter().any(|r| {
                    r.deletes.iter().any(|d| d.0 == 0) && r.deletes.iter().any(|d| d.0 == 1)
                })
            }),
            Property::sometimes("composed standalone covered clearing", |_, s: &State| {
                s.log
                    .iter()
                    .any(|r| !r.clear.is_empty() && r.remove.is_empty() && r.add.is_empty())
            }),
            Property::sometimes("composed two query snapshots", |_, s: &State| {
                s.queries.len() == 2
            }),
            Property::sometimes("composed late predecessor upload", |_, s: &State| s.late),
            Property::sometimes("composed checkpoint log collection", |_, s: &State| {
                s.collected > 0 && s.recovered.is_some()
            }),
            Property::sometimes("composed two-operation acknowledgement", |_, s: &State| {
                s.log.iter().any(|r| r.deletes.len() == 2)
            }),
        ]
    }
}
