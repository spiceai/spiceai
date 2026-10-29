// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::Head;
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub struct Protocol {
    name: String,
    bound: usize,
    k: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Request {
    base: Head,
    etag: usize,
    pos: usize,
    lease: usize,
    id: usize,
    window: Vec<usize>,
    floor: usize,
    sent: u8,
    done: u8,
    answer: u8,
    success: bool,
    unreadable: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub struct State {
    pos: usize,
    etag: usize,
    lease: usize,
    window: Vec<usize>,
    batches: Vec<Vec<usize>>,
    // Pending creates retain actual observed bytes, including after deletion.
    pending: Vec<(usize, Vec<usize>)>,
    history: Vec<usize>,
    requests: Vec<Vec<Request>>,
    ack: u8,
    persisted: [bool; 4],
    memory: [bool; 4],
    restored: [bool; 4],
    buffers: [bool; 2],
    restarted: bool,
    floor: usize,
    parts: Vec<usize>,
    root: Vec<usize>,
    stage: u8,
    gc: bool,
    listed: bool,
    plan: bool,
    deleted: bool,
    gc_lease: usize,
    query: bool,
    query_base: usize,
    query_target: usize,
    snapshot: bool,
    read: bool,
    bad_read: bool,
    bad_replay: bool,
    bad_query: bool,
    bad_recovery: bool,
    bad_fence: bool,
    bad_i1: bool,
    revived: bool,
    stale: bool,
    window_resolution: bool,
    batch_resolution: bool,
    lagged: bool,
    folded: bool,
    conflict: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Issue(usize),
    Buffer(usize),
    Persist(usize),
    Send(usize),
    Restart,
    Restore,
    Deliver(usize, usize, usize),
    Resolve(usize),
    Unreadable(usize),
    Response(usize),
    Ack(usize),
    Copy(usize),
    Observe,
    Fence,
    Poll,
    Parts,
    Root,
    Floor,
    Scan,
    Delete,
    Query,
    Snapshot,
    Recover,
    Lagging,
    Advance,
}
impl Protocol {
    fn head(state: &State) -> Head {
        Head {
            pos: state.pos,
            lease: state.lease,
            floor: state.floor,
        }
    }
    pub fn new(name: &str, bound: usize) -> Self {
        Self {
            name: name.into(),
            bound,
            k: if bound > 2 { 3 } else { 2 },
        }
    }
    fn has(&self, t: &str) -> bool {
        self.name.contains(t)
    }
    fn focused(&self) -> bool {
        self.has("guards") || self.has("retry") || self.has("etag") || self.has("restart")
    }
    fn storage(&self) -> bool {
        self.has("storage")
    }
    fn get(&self, state: &State, p: usize) -> Option<usize> {
        let start = state.pos + 1 - state.window.len();
        if p >= start && p <= state.pos {
            return Some(state.window[p - start]);
        }
        state.batches[(p - 1) / self.k]
            .get((p - 1) % self.k)
            .copied()
    }
    fn decode(&self, state: &State, base: usize, target: usize) -> Option<Vec<usize>> {
        let mut v = if base == 0 {
            vec![]
        } else if state.root.len() == base {
            state.root.clone()
        } else {
            return None;
        };
        for p in base + 1..=target {
            v.push(self.get(state, p)?);
        }
        Some(v)
    }
    fn can_advance(&self, state: &State) -> bool {
        state.window.len() < self.k
            || !state.batches[(state.pos - self.k) / self.k].is_empty()
            || self.has("no-copy")
    }
    fn queue(state: &mut State, b: usize, v: Vec<usize>) {
        if !state.pending.contains(&(b, v.clone())) {
            state.pending.push((b, v));
            state.pending.sort();
        }
    }
    fn append(&self, state: &mut State, id: usize) {
        state.pos += 1;
        state.history.push(id);
        state.window.push(id);
        if state.window.len() > self.k {
            state.window.remove(0);
        }
        if state.pos.is_multiple_of(self.k) {
            Self::queue(state, state.pos / self.k - 1, state.window.clone());
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let pos = if self.storage() || self.has("observed") {
            self.k + 1
        } else {
            self.k
        };
        let history: Vec<_> = (10..10 + pos).collect();
        let mut state = State {
            pos,
            etag: 0,
            lease: 1,
            window: history[pos - self.k..].to_vec(),
            batches: vec![vec![]; self.k + self.bound + 4],
            pending: vec![],
            history: history.clone(),
            requests: vec![vec![], vec![]],
            ack: 0,
            persisted: [false; 4],
            memory: [false; 4],
            restored: [false; 4],
            buffers: [false; 2],
            restarted: false,
            floor: 0,
            parts: vec![],
            root: vec![],
            stage: 0,
            gc: false,
            listed: false,
            plan: false,
            deleted: false,
            gc_lease: 0,
            query: false,
            query_base: 0,
            query_target: 0,
            snapshot: false,
            read: false,
            bad_read: false,
            bad_replay: false,
            bad_query: false,
            bad_recovery: false,
            bad_fence: false,
            bad_i1: false,
            revived: false,
            stale: false,
            window_resolution: false,
            batch_resolution: false,
            lagged: false,
            folded: false,
            conflict: false,
        };
        let bytes = history[..self.k].to_vec();
        Self::queue(&mut state, 0, bytes.clone());
        if self.storage() || self.has("observed") {
            state.batches[0] = bytes;
        }
        vec![state]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        use Action::{
            Ack, Advance, Copy, Delete, Deliver, Fence, Floor, Issue, Lagging, Observe, Parts,
            Poll, Query, Recover, Resolve, Response, Root, Scan, Snapshot,
        };
        if self.storage() {
            if state.stage == 0 {
                action.push(Parts);
            }
            if state.stage == 1 {
                action.push(Root);
            }
            if state.stage == 2 && !state.folded && self.can_advance(state) {
                action.push(Floor);
            }
            if !state.listed && (state.folded || self.has("no-published") && state.stage == 2) {
                action.push(Scan);
            }
            if state.plan && !state.deleted {
                action.push(Delete);
            }
            if !state.query {
                action.push(Query);
            }
            if state.query && !state.snapshot {
                action.push(Snapshot);
            }
            if !state.lagged && state.deleted {
                action.push(Lagging);
            }
        } else {
            for w in 0..if self.focused() { 1 } else { 2 } {
                let rs = &state.requests[w];
                if rs.len()
                    < if self.has("copy")
                        || self.has("observed")
                        || self.has("guards")
                        || self.has("etag")
                    {
                        1
                    } else {
                        2
                    }
                    && state.pos < self.k + self.bound
                    && (rs.last().is_none_or(|r| r.answer == 2)
                        || self.has("restart")
                            && rs.last().is_some_and(|r| {
                                !state.memory[r.id - 1] && !state.persisted[r.id - 1]
                            }))
                    && (!self.has("restart") || state.buffers[w])
                    && self.can_advance(state)
                {
                    action.push(Issue(w));
                }
                for (i, r) in rs.iter().enumerate() {
                    for q in 0..2 {
                        if r.sent & (1 << q) != 0 && r.done & (1 << q) == 0 {
                            action.push(Deliver(w, i, q));
                        }
                    }
                }
                if self.has("restart") {
                    if !state.buffers[w] && rs.len() < 2 {
                        action.push(Action::Buffer(w));
                    }
                    if let Some(r) = rs.last() {
                        if state.memory[r.id - 1] && !state.persisted[r.id - 1] {
                            action.push(Action::Persist(w));
                        }
                        if r.sent == 0
                            && !state.restored[r.id - 1]
                            && state.memory[r.id - 1]
                            && (state.persisted[r.id - 1] || self.has("no-persist-before-send"))
                        {
                            action.push(Action::Send(w));
                        }
                    }
                }
                if let Some(r) = rs
                    .last()
                    .filter(|r| !self.has("restart") || state.memory[r.id - 1] && r.sent != 0)
                {
                    if r.answer == 0 {
                        action.push(Resolve(w));
                        if self.has("retry") && !r.unreadable {
                            action.push(Action::Unreadable(w));
                        }
                        if r.success && !state.restored[r.id - 1] {
                            action.push(Response(w));
                        }
                    }
                    if r.answer == 1 && state.ack & (1 << w) == 0 {
                        action.push(Ack(w));
                    }
                }
            }
            if state.stage == 0 && !self.has("copy") && !self.has("observed") && !self.focused() {
                action.push(Parts);
            }
            if state.stage == 1 {
                action.push(Root);
            }
            if !self.has("copy")
                && !self.focused()
                && (if self.has("observed") {
                    state.requests.iter().flatten().any(|r| r.success)
                } else {
                    state.requests.iter().all(|rs| rs.iter().any(|r| r.success))
                })
                && state.pos < 3 * self.k
                && self.can_advance(state)
            {
                action.push(Advance);
            }
        }
        if self.has("restart") {
            if !state.restarted {
                action.push(Action::Restart);
            }
            if state
                .persisted
                .iter()
                .zip(state.memory)
                .any(|(p, m)| *p && !m)
            {
                action.push(Action::Restore);
            }
        }
        for i in 0..state.pending.len() {
            if state.batches[state.pending[i].0] != state.pending[i].1 {
                action.push(Copy(i));
            }
        }
        if state.pos.is_multiple_of(self.k) {
            action.push(Observe);
        }
        if !state.read {
            action.push(Poll);
        }
        action.push(Recover);
        if state.lease
            < if self.has("copy") || self.has("observed") {
                1
            } else if self.storage() {
                2
            } else {
                3
            }
        {
            action.push(Fence);
        }
        action.retain(|x| self.next_state(state, x.clone()).is_some());
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        use Action::{
            Ack, Advance, Copy, Delete, Deliver, Fence, Floor, Issue, Lagging, Observe, Parts,
            Poll, Query, Recover, Resolve, Response, Root, Scan, Snapshot,
        };
        let mut next = state.clone();
        match action {
            Action::Buffer(w) => next.buffers[w] = true,
            Action::Persist(w) => next.persisted[state.requests[w].last()?.id - 1] = true,
            Action::Send(w) => next.requests[w].last_mut()?.sent = 1,
            Action::Restart => {
                next.restarted = true;
                next.memory = [false; 4];
                next.buffers = [false; 2];
                for request in next.requests.iter_mut().flatten() {
                    request.answer = 0;
                }
            }
            Action::Restore => {
                next.memory = state.persisted;
                next.restored = state.persisted;
            }
            Advance => self.append(&mut next, 90 + state.pos),
            Issue(w) => {
                let id = w * 2 + state.requests[w].len() + 1;
                next.buffers[w] = false;
                next.memory[id - 1] = true;
                let mut window = state.window.clone();
                window.push(id);
                if window.len() > self.k {
                    window.remove(0);
                }
                let r = Request {
                    base: Self::head(state),
                    etag: state.etag,
                    pos: state.pos + 1,
                    lease: state.lease,
                    id,
                    window: window.clone(),
                    floor: if state.stage == 2 {
                        self.k
                    } else {
                        state.floor
                    },
                    sent: u8::from(!self.has("restart")),
                    done: 0,
                    answer: 0,
                    success: false,
                    unreadable: false,
                };
                next.requests[w].push(r);
                if self.has("no-observed") && (state.pos + 1).is_multiple_of(self.k) {
                    Self::queue(&mut next, (state.pos + 1) / self.k - 1, window);
                }
            }
            Deliver(w, i, q) => {
                next.requests[w][i].done |= 1 << q;
                let r = &state.requests[w][i];
                if (if self.has("etag") {
                    state.etag == r.etag
                } else {
                    Self::head(state).accepts(r.base)
                }) || self.has("no-cas")
                    || (self.has("no-lease") && r.lease < state.lease && r.pos == state.pos + 1)
                {
                    next.bad_i1 |= r.pos != state.pos + 1;
                    next.bad_fence |= r.lease < state.lease;
                    self.append(
                        &mut next,
                        if q == 1 && self.has("no-identical") {
                            r.id + 20
                        } else {
                            r.id
                        },
                    );
                    if self.has("etag") {
                        next.etag = state.etag + 1;
                    }
                    next.floor = r.floor;
                    next.folded |= r.floor > 0;
                    next.requests[w][i].success = true;
                }
            }
            Action::Unreadable(w) => {
                // A failed head read supplies neither acceptance nor rejection evidence.
                let i = state.requests[w].len() - 1;
                next.requests[w][i].unreadable = true;
                next.requests[w][i].answer = if self.has("no-resolve-before-retry") {
                    2
                } else {
                    0
                };
            }
            Resolve(w) => {
                let i = state.requests[w].len() - 1;
                let r = &state.requests[w][i];
                // Restoring just an identity does not restore the CAS condition or bytes.
                // Look for acceptance in retained storage; absence remains unresolved.
                let answer = if self.has("restart") && state.restored[r.id - 1] {
                    u8::from(state.history.contains(&r.id))
                } else if state.pos >= r.pos {
                    match self.get(state, r.pos) {
                        Some(id) => {
                            if r.pos >= state.pos + 1 - state.window.len() {
                                next.window_resolution = true;
                            } else {
                                next.batch_resolution = true;
                            }
                            crate::core::resolve_attempt(
                                Some(id == r.id),
                                Self::head(state).accepts(r.base),
                            )
                        }
                        None => 0,
                    }
                } else {
                    crate::core::resolve_attempt(None, Self::head(state).accepts(r.base))
                };
                next.requests[w][i].answer = answer;
                if answer == 0 && !state.restored[r.id - 1] && Self::head(state).accepts(r.base) {
                    next.requests[w][i].sent |= 2;
                }
            }
            Response(w) => {
                let i = state.requests[w].len() - 1;
                next.requests[w][i].answer = 1;
            }
            Ack(w) => next.ack |= 1 << w,
            Copy(i) => {
                let (b, v) = &state.pending[i];
                if state.batches[*b].is_empty() {
                    next.batches[*b].clone_from(v);
                    next.revived |= state.deleted && *b == 0;
                } else {
                    next.conflict = true; /* create-if-absent preserves first bytes */
                }
            }
            Observe => Self::queue(&mut next, state.pos / self.k - 1, state.window.clone()),
            Fence => {
                next.lease += 1;
                // The faulty store assigns the captured initial tag to different head bytes.
                if self.has("etag") {
                    next.etag = if self.has("etag-repeat") {
                        0
                    } else {
                        state.etag + 1
                    };
                }
            }
            Poll => {
                next.read = true;
                match self.decode(state, state.floor, state.pos) {
                    Some(v) => {
                        next.bad_read |= v != state.history;
                        next.bad_replay |= v != state.history;
                    }
                    None => next.bad_read = true,
                }
            }
            Parts => {
                next.parts = self.decode(state, 0, self.k)?;
                next.stage = 1;
            }
            Root => {
                next.root.clone_from(&state.parts);
                next.stage = 2;
            }
            Floor => {
                self.append(&mut next, 90);
                next.floor = self.k;
                next.folded = true;
            }
            Scan => {
                next.listed = true;
                next.gc_lease = state.lease;
                let floor = if self.has("no-published") {
                    state.root.len()
                } else {
                    state.floor
                };
                // Retention keeps a running query's state; no reader writes.
                next.plan = floor >= self.k && !(state.query && state.query_base < self.k);
                next.gc = true;
            }
            Delete => {
                next.batches[0].clear();
                next.deleted = true;
                next.stale |= state.lease > state.gc_lease;
            }
            Query => {
                next.query = true;
                next.query_base = state.floor;
                next.query_target = state.pos;
            }
            Snapshot => {
                next.snapshot = true;
                next.bad_query = self
                    .decode(state, state.query_base, state.query_target)
                    .as_deref()
                    != Some(&state.history[..state.query_target]);
            }
            Recover => {
                next.bad_recovery |= self.decode(state, state.floor, state.pos).as_deref()
                    != Some(state.history.as_slice());
            }
            Lagging => {
                next.lagged = true; // Reader retained base 0 before it fell behind GC.
                let mut v = vec![];
                let mut missing = false;
                for p in 1..=state.pos {
                    match self.get(state, p) {
                        Some(id) => v.push(id),
                        None => missing = true,
                    }
                }
                if missing && !self.has("no-missing") {
                    v = self.decode(state, state.floor, state.pos)?;
                }
                next.bad_replay |= v != state.history;
            }
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut p = vec![
            Property::always("I1 unique accepted position", |_, state: &State| {
                !state.bad_i1
            }),
            Property::always(
                "I2 acknowledged logical write exactly once",
                |_, state: &State| {
                    (0..2).all(|w| {
                        state.ack & (1 << w) == 0
                            || state
                                .history
                                .iter()
                                .filter(|id| **id <= 4 && (**id - 1) / 2 == w)
                                .count()
                                == 1
                    })
                },
            ),
            Property::always("logical write never applied twice", |_, state: &State| {
                (0..2).all(|w| {
                    state
                        .history
                        .iter()
                        .filter(|id| **id <= 4 && (**id - 1) / 2 == w)
                        .count()
                        <= 1
                })
            }),
            Property::always("I3 CAS fencing", |_, state: &State| !state.bad_fence),
            Property::always(
                "I4 missing is error and retained references live",
                |_, state: &State| !state.bad_read,
            ),
            Property::always("I5 resolution sound", |_, state: &State| {
                state.requests.iter().flatten().all(|r| match r.answer {
                    1 => state.history.contains(&r.id),
                    2 => !state.history.contains(&r.id),
                    _ => true,
                })
            }),
            Property::always("I6 prefix determinism", |_, state: &State| {
                state.history.len() == state.pos
            }),
            Property::always("I8 replay equality", |_, state: &State| !state.bad_replay),
            Property::always("I7 query snapshot equals serial", |_, state: &State| {
                !state.bad_query
            }),
            Property::always("I8 recovery equals serial reference", |_, state: &State| {
                !state.bad_recovery
            }),
            Property::sometimes(
                "folded checkpoint published by commit",
                |_, state: &State| state.folded,
            ),
        ];
        if self.has("restart") {
            p.push(Property::sometimes(
                "restart and lease turnover",
                |_, s: &State| {
                    s.restarted && s.lease > 1 && s.requests.iter().flatten().any(|r| r.success)
                },
            ));
            p.push(Property::sometimes(
                "persisted attempt restored",
                |_, s: &State| {
                    s.restarted && s.persisted.iter().zip(s.memory).any(|(p, m)| *p && m)
                },
            ));
        }
        if self.has("retry") {
            p.push(Property::sometimes(
                "head read unavailable",
                |_, state: &State| state.requests.iter().flatten().any(|r| r.unreadable),
            ));
        }
        if self.storage() {
            p.extend([
                Property::sometimes("late batch recreated after GC", |_, state: &State| {
                    state.revived
                }),
                Property::sometimes("stale GC delete delivered", |_, state: &State| state.stale),
                Property::sometimes("reader more than K behind completed", |_, state: &State| {
                    state.lagged && !state.bad_replay
                }),
                Property::sometimes("query snapshot completed", |_, state: &State| {
                    state.snapshot && !state.bad_query
                }),
            ]);
        } else {
            p.extend([
                Property::sometimes("resolution from window", |_, state: &State| {
                    state.window_resolution
                }),
                Property::sometimes("resolution from batch", |_, state: &State| {
                    state.batch_resolution
                }),
                Property::sometimes("delayed identical retry accepted", |_, state: &State| {
                    state
                        .requests
                        .iter()
                        .flatten()
                        .any(|r| r.done & 2 != 0 && r.success)
                }),
                Property::sometimes("two writers each two attempts", |_, state: &State| {
                    state.requests.iter().all(|r| r.len() == 2)
                }),
                Property::sometimes("conflicting batch bytes raced", |_, state: &State| {
                    state.conflict
                }),
            ]);
        }
        p
    }
}
