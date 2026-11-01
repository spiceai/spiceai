// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use super::core::{Record, Store, payload, record};
use crate::core::Head;
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub struct Protocol {
    name: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Req {
    base: Head,
    pos: usize,
    lease: u8,
    kind: u8,
    out: u8,
    sent: u8,
    done: u8,
    answer: u8,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
struct View {
    pending: bool,
    value: u8,
    out: u8,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Local {
    storage: Store,
    lease: u8,
    log: Vec<Record>,
    root: Vec<Record>,
    floor: usize,
    req: Option<Req>,
    parked: Option<Req>,
    prepare_known: bool,
    issued: u8,
    object: bool,
    gc: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub struct State {
    ds: Vec<Local>,
    decision: u8,
    deadline: bool,
    deciders: u8,
    bad_reservation: bool,
    bad_fence: bool,
    bad_resolution: bool,
    nonowner: bool,
    /// The transaction's records (its intent and decision objects) exist.
    records: bool,
    /// The decision object's outcome while it exists: 0 none, 1 commit, 2
    /// abort. `decision` keeps the first outcome ever recorded.
    recorded: u8,
    /// The coordinator removed the records and stops: it prepares and decides
    /// nothing more.
    coordinator_done: bool,
    /// Who removed records: 1 the coordinator, 2 another process after every
    /// participant's lease moved past the coordinator's, 4 a decision object
    /// recreated after the records went.
    removed: u8,
    /// The outcome the coordinator reported to its client: 0 none yet, 1
    /// committed, 2 aborted.
    reported: u8,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Issue(usize, usize),
    Deliver(usize, usize),
    Check(usize),
    Fence(usize),
    Deadline,
    /// Create the decision object if absent: 1 the coordinator commits, 2
    /// another process aborts, 3 the coordinator aborts at its deadline.
    Decide(usize),
    Checkpoint(usize),
    Publish(usize),
    Gc(usize),
    Switch(usize),
    Observe(usize),
    Advance(usize),
    Create(usize, usize),
    /// The coordinator reports the outcome to its client: the decision object
    /// it reads while the intent exists.
    Report,
    /// Remove the transaction's records: 0 by the coordinator once every
    /// participant it prepared has resolved and no `Prepare` of its is in
    /// flight; 1 by any other process once every participant's lease has moved
    /// past the coordinator's and none holds the transaction prepared, or a
    /// decision object left with no intent.
    Forget(usize),
}
/// The lease the coordinator holds on every participant when the transaction
/// begins. A `Prepare` is written only under it.
const COORDINATOR_LEASE: u8 = 1;
fn reference(log: &[Record]) -> View {
    let mut accepted = vec![];
    let mut pending = false;
    for &r in log {
        if (r.0 == 2 && pending) || (r.0 == 3 && !pending) {
            continue;
        }
        if r.0 == 1 {
            pending = true;
        }
        if r.0 == 3 {
            pending = false;
        }
        accepted.push(r);
    }
    fold(&accepted)
}
fn fold(log: &[Record]) -> View {
    let mut v = View::default();
    for &(k, o, _, _, _) in log {
        match k {
            1 => v.pending = true,
            2 => v.value += 2,
            3 => {
                if o == 1 {
                    v.value += 1;
                }
                v.pending = false;
                v.out = o;
            }
            _ => (),
        }
    }
    v
}
impl Protocol {
    pub(crate) fn representative(s: &State) -> State {
        let mut n = s.clone();
        n.ds.sort_unstable();
        n
    }
    fn project(state: &State, pair: [usize; 2]) -> State {
        let mut projected = state.clone();
        projected.ds = pair.map(|i| state.ds[i].clone()).to_vec();
        projected
    }
    fn project_action(action: &Action, pair: [usize; 2]) -> Option<Action> {
        let index = |i| pair.iter().position(|&p| p == i);
        Some(match *action {
            Action::Issue(i, k) => Action::Issue(index(i)?, k),
            Action::Deliver(i, k) => Action::Deliver(index(i)?, k),
            Action::Check(i) => Action::Check(index(i)?),
            Action::Fence(i) => Action::Fence(index(i)?),
            Action::Checkpoint(i) => Action::Checkpoint(index(i)?),
            Action::Publish(i) => Action::Publish(index(i)?),
            Action::Gc(i) => Action::Gc(index(i)?),
            Action::Switch(i) => Action::Switch(index(i)?),
            Action::Observe(i) => Action::Observe(index(i)?),
            Action::Advance(i) => Action::Advance(index(i)?),
            Action::Create(i, k) => Action::Create(index(i)?, k),
            Action::Deadline => Action::Deadline,
            Action::Decide(o) => Action::Decide(o),
            Action::Report => Action::Report,
            Action::Forget(o) => Action::Forget(o),
        })
    }
    fn projection_step(&self, state: &State) -> bool {
        let pair_model = Self::new("resolution-txn", self.bound);
        let mut actions = vec![];
        self.actions(state, &mut actions);
        actions.iter().all(|action| {
            let Some(next) = self.next_state(state, action.clone()) else {
                return true;
            };
            [[0, 1], [0, 2], [1, 2]].into_iter().all(|pair| {
                let from = Self::project(state, pair);
                let to = Self::project(&next, pair);
                if from == to {
                    return true;
                }
                let Some(projected) = Self::project_action(action, pair) else {
                    return false;
                };
                let mut enabled = vec![];
                pair_model.actions(&from, &mut enabled);
                enabled.contains(&projected) && pair_model.next_state(&from, projected) == Some(to)
            })
        })
    }
    /// Every participant's lease has moved past the coordinator's.
    fn all_fenced(state: &State) -> bool {
        state
            .ds
            .iter()
            .all(|dataset| dataset.lease > COORDINATOR_LEASE)
    }
    /// The coordinator knows the decision, has no `Prepare` in flight, and every
    /// participant it prepared has resolved.
    fn coordinator_may_forget(&self, state: &State) -> bool {
        let in_flight = |r: &Option<Req>| r.as_ref().is_some_and(|r| r.kind == 1);
        !state.coordinator_done
            && state.records
            && state.recorded != 0
            && state.ds.iter().all(|dataset| {
                !fold(&dataset.log).pending
                    && (self.has("no-coordinator-wait")
                        || !in_flight(&dataset.req) && !in_flight(&dataset.parked))
            })
    }
    /// Another process, from storage alone: the decision is recorded, every
    /// participant's lease has moved past the coordinator's, and none holds the
    /// transaction prepared. A decision object with no intent goes too.
    fn other_may_forget(&self, state: &State) -> bool {
        if !state.records {
            return state.recorded != 0;
        }
        state.recorded != 0
            && (self.has("no-fence-check") || Self::all_fenced(state))
            && state
                .ds
                .iter()
                .all(|dataset| {
                    self.has("no-prepared-check")
                        || Self::decode(dataset).is_some_and(|log| !fold(&log).pending)
                })
    }
    fn head(dataset: &Local) -> Head {
        Head {
            pos: dataset.log.len(),
            lease: usize::from(dataset.lease),
            floor: dataset.floor,
        }
    }
    pub fn new(name: &str, bound: usize) -> Self {
        Self {
            name: name.into(),
            bound,
        }
    }
    fn has(&self, updated: &str) -> bool {
        self.name.contains(updated)
    }
    fn decode(dataset: &Local) -> Option<Vec<Record>> {
        dataset.storage.decode()
    }

    fn reader(&self, state: &State, i: usize) -> Option<View> {
        let decoded = Self::decode(&state.ds[i])?;
        let mut v = fold(&decoded);
        if self.has("no-reader-log") && v.pending && state.decision != 0 {
            if state.decision == 1 {
                v.value += 1;
            }
            v.pending = false;
            v.out = state.decision;
        }
        Some(v)
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let seed = if self.has("resolution-") {
            vec![]
        } else {
            vec![record(2, 0, 0), record(2, 0, 2), record(2, 0, 4)]
        };
        let mut storage = Store::default();
        for r in &seed {
            if !storage.append(*r, "") {
                return vec![];
            }
        }
        let dataset = Local {
            storage,
            lease: 1,
            log: seed,
            root: vec![],
            floor: 0,
            req: None,
            parked: None,
            prepare_known: false,
            issued: 0,
            object: true,
            gc: false,
        };
        vec![State {
            ds: vec![dataset; if self.has("three-participants") { 3 } else { 2 }],
            decision: 0,
            deadline: false,
            deciders: 0,
            bad_reservation: false,
            bad_fence: false,
            bad_resolution: false,
            nonowner: false,
            records: true,
            recorded: 0,
            coordinator_done: false,
            reported: 0,
            removed: 0,
        }]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        use Action::{
            Advance, Check, Checkpoint, Create, Deadline, Decide, Deliver, Fence, Forget, Gc,
            Issue, Observe, Publish, Report, Switch,
        };
        for i in 0..state.ds.len() {
            let dataset = &state.ds[i];
            if !self.has("resolution-") {
                action.push(Observe(i));
            }
            // Padding after the outcome lets Resolve leave the window; padding
            // while prepared (a write to another key) lets the Prepare reach a
            // checkpoint, which names only records that left the window.
            // One participant pads, which keeps the state space near the
            // unpadded one.
            let padded = |d: &Local| d.log.iter().filter(|r| r.0 == 0).count();
            let others_padded = (0..state.ds.len()).any(|j| j != i && padded(&state.ds[j]) > 0);
            if (!self.has("resolution-") && fold(&dataset.log).out != 0
                || fold(&dataset.log).pending
                    && padded(dataset) < 2
                    && !others_padded
                    && dataset.root.is_empty())
                && dataset.log.len() < 8
                && dataset.req.is_none()
                && dataset.parked.is_none()
            {
                action.push(Advance(i));
            }
            for j in 0..dataset.storage.pending.len() {
                if !self.has("resolution-") {
                    action.push(Create(i, j));
                }
            }
            let Some(decoded) = Self::decode(dataset) else {
                return;
            };
            let v = fold(&decoded);
            if dataset.req.is_none() || (self.bound > 1 && dataset.parked.is_none()) {
                for actor in 1..=4 {
                    let k = if actor == 4 { 3 } else { actor };
                    if (self.has("resolution-") || self.bound > 0 || k != 2)
                        && (dataset.issued & (1 << k) == 0
                            || (k == 3 && dataset.log.iter().filter(|r| r.0 == 3).count() < 2))
                        && (k != 1 || dataset.object)
                        // Only the coordinator prepares, under the lease it held
                        // when the transaction began; a takeover fences it.
                        && (k != 1
                            || dataset.lease == COORDINATOR_LEASE && !state.coordinator_done)
                        && (k != 3 || state.recorded != 0)
                        && (k == 3 || !v.pending || self.has("no-reservation"))
                        && (k != 3 || v.pending || self.has("no-matching"))
                    {
                        action.push(Issue(i, actor));
                    }
                }
            }
            if let Some(r) = &dataset.req {
                for q in 0..2 {
                    if r.sent & (1 << q) != 0 && r.done & (1 << q) == 0 {
                        action.push(Deliver(i, q));
                    }
                }
                if r.answer == 0 {
                    action.push(Check(i));
                }
            }
            if usize::from(dataset.lease) < self.bound + 1 {
                action.push(Fence(i));
            }
            if dataset.parked.is_some() {
                action.push(Switch(i));
            }
            if dataset.root.is_empty() && dataset.storage.first > dataset.floor {
                action.push(Checkpoint(i));
            }
            if dataset.root.len() > dataset.floor {
                action.push(Publish(i));
            }
            if dataset.floor > 0 && dataset.root.iter().any(|r| r.0 == 1) && !dataset.gc {
                action.push(Gc(i));
            }
        }
        if !state.deadline {
            action.push(Deadline);
        }
        if state.deciders & 1 == 0
            && !state.coordinator_done
            && state.ds.iter().all(|dataset| dataset.prepare_known)
        {
            action.push(Decide(1));
        }
        // Anyone may abort after the deadline, or once every participant's lease
        // has moved past the coordinator's, which can then prepare nothing more.
        if (state.deadline || Self::all_fenced(state)) && state.deciders & 2 == 0 {
            action.push(Decide(2));
        }
        // The coordinator stops waiting for its commit at the deadline and
        // aborts, so its client gets an answer and its tables are released.
        if state.deadline && !state.coordinator_done && state.deciders & 4 == 0 {
            action.push(Decide(3));
        }
        // A decision object read once the intent is gone may be one created
        // after the records were removed, so the coordinator reports only what
        // it reads while the intent exists.
        if !state.coordinator_done
            && state.reported == 0
            && state.recorded != 0
            && (state.records || self.has("no-intent-check"))
        {
            action.push(Report);
        }
        if self.coordinator_may_forget(state) {
            action.push(Forget(0));
        }
        if self.other_may_forget(state) {
            action.push(Forget(1));
        }
        crate::prune_for_simulation(self, state, action);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        use Action::{
            Advance, Check, Checkpoint, Create, Deadline, Decide, Deliver, Fence, Forget, Gc,
            Issue, Observe, Publish, Report, Switch,
        };
        let mut next = state.clone();
        match action {
            Issue(i, actor) => {
                let k = if actor == 4 { 3 } else { actor };
                let dataset = &mut next.ds[i];
                if self.has("resolution-") {
                    dataset.storage.observe("");
                    for j in 0..dataset.storage.pending.len() {
                        dataset.storage.create(j);
                    }
                }
                if dataset.req.is_some() {
                    dataset.parked = dataset.req.take();
                }
                dataset.issued |= 1 << k;
                dataset.req = Some(Req {
                    base: Self::head(dataset),
                    pos: dataset.log.len(),
                    lease: if actor == 3 { 0 } else { dataset.lease },
                    kind: u8::try_from(k).ok()?,
                    out: if k == 3 { state.recorded } else { 0 },
                    sent: 1,
                    done: 0,
                    answer: 0,
                });
            }
            Deliver(i, q) => {
                let dataset = &state.ds[i];
                let r = dataset.req.as_ref()?;
                let updated = &mut next.ds[i];
                updated.req.as_mut()?.done |= 1 << q;
                if Self::head(dataset).accepts(r.base)
                    && !(r.kind == 3 && self.has("no-owner-resolve") && r.lease != dataset.lease)
                {
                    let v = fold(&dataset.log);
                    next.nonowner |= r.kind == 3 && r.lease == 0 && dataset.lease > 1;
                    next.bad_reservation |= r.kind != 3 && v.pending;
                    next.bad_fence |= r.kind != 3 && r.lease != dataset.lease;
                    if !updated
                        .storage
                        .append(record(r.kind, r.out, r.base.identity()), "")
                    {
                        return None;
                    }
                    updated.log.push(record(r.kind, r.out, r.base.identity()));
                }
            }
            Check(i) => {
                let dataset = &state.ds[i];
                let r = dataset.req.as_ref()?;
                let evidence = if dataset.log.len() > r.pos {
                    Some(Self::decode(dataset)?[r.pos] == record(r.kind, r.out, r.base.identity()))
                } else {
                    None
                };
                let ans =
                    crate::core::resolve_attempt(evidence, Self::head(dataset).accepts(r.base));
                next.bad_resolution |= ans != 0
                    && ((ans == 1)
                        != (dataset.log.get(r.pos)
                            == Some(&record(r.kind, r.out, r.base.identity()))));
                let updated = &mut next.ds[i];
                updated.req.as_mut()?.answer = ans;
                if ans == 0 && Self::head(dataset).accepts(r.base) {
                    updated.req.as_mut()?.sent |= 2;
                }
                // A rejected operation may be re-planned against the new base; only
                // one successful logical record of each kind is admitted.
                if ans == 2 {
                    updated.issued &= !(1 << r.kind);
                    updated.req = None;
                } else if ans == 1 {
                    if r.kind == 1 {
                        updated.prepare_known = true;
                    }
                    updated.req = None;
                }
            }
            Advance(i) => {
                let dataset = &mut next.ds[i];
                let kind = if fold(&dataset.log).pending { 0 } else { 2 };
                let r = record(kind, 0, 100 + dataset.log.len());
                if self.has("resolution-") {
                    dataset.storage.observe("");
                    for j in 0..dataset.storage.pending.len() {
                        dataset.storage.create(j);
                    }
                }
                if !dataset.storage.append(r, "") {
                    return None;
                }
                dataset.log.push(r);
            }
            Observe(i) => next.ds[i].storage.observe(""),
            Create(i, j) => next.ds[i].storage.create(j),
            Switch(i) => {
                let dataset = &mut next.ds[i];
                std::mem::swap(&mut dataset.req, &mut dataset.parked);
            }
            Fence(i) => {
                next.ds[i].lease += 1;
            }
            Deadline => next.deadline = true,
            Decide(o) => {
                next.deciders |= 1 << (o - 1);
                let o = if o == 3 { 2 } else { u8::try_from(o).ok()? };
                // Create-if-absent: it lands when no decision object exists,
                // including after the records were removed.
                if state.recorded == 0 || self.has("no-create-once") {
                    next.recorded = o;
                    if state.decision == 0 || self.has("no-create-once") {
                        next.decision = o;
                    }
                    if !state.records {
                        next.removed |= 4;
                    }
                }
            }
            Report => next.reported = state.recorded,
            Forget(0) => {
                next.coordinator_done = true;
                next.records = false;
                next.recorded = 0;
                next.removed |= 1;
            }
            Forget(_) => {
                if state.records {
                    next.removed |= 2;
                }
                next.records = false;
                next.recorded = 0;
            }
            Checkpoint(i) => {
                // A checkpoint names the newest batch end, so the floor never
                // splits a batch; records still in the window stay above it.
                let first = state.ds[i].storage.first;
                next.ds[i].root = Self::decode(&state.ds[i])?.get(..first)?.to_vec();
            }
            Publish(i) => {
                next.ds[i].floor = state.ds[i].root.len();
                if !next.ds[i].storage.publish(state.ds[i].root.len(), "") {
                    return None;
                }
            }
            Gc(i) => {
                let v = fold(&state.ds[i].log);
                next.ds[i].gc = true;
                next.ds[i].storage.gc();
                if (!v.pending || self.has("no-roots")) && v.out != 1 {
                    next.ds[i].object = false;
                }
            }
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties: Vec<Property<Self>> = vec![
            Property::always(
                "I8 exact accepted storage and inline bytes",
                |_, state: &State| {
                    state.ds.iter().all(|dataset| {
                        dataset.storage.decode() == Some(dataset.log.clone())
                            && dataset
                                .storage
                                .decode()
                                .map(|v| v.into_iter().map(payload).collect::<Vec<_>>())
                                == Some(dataset.log.iter().copied().map(payload).collect())
                    })
                },
            ),
            Property::always(
                "I4 Prepare and visible objects survive",
                |_, state: &State| {
                    state.ds.iter().all(|dataset| {
                        let v = fold(&dataset.log);
                        (!v.pending && v.out != 1) || dataset.object
                    })
                },
            ),
            Property::always(
                "a prepared participant keeps its transaction's records",
                |_, state: &State| {
                    state.records || state.ds.iter().all(|dataset| !fold(&dataset.log).pending)
                },
            ),
            Property::always(
                "the coordinator reports the transaction's decision",
                |_, state: &State| state.reported == 0 || state.reported == state.decision,
            ),
            Property::always("I6 applied outcomes equal decision", |_, state: &State| {
                state.ds.iter().all(|dataset| {
                    let v = fold(&dataset.log);
                    v.out == 0 || v.out == state.decision
                })
            }),
            Property::always("I6 participants never disagree", |_, state: &State| {
                state.ds.iter().all(|a| {
                    state.ds.iter().all(|b| {
                        let first = fold(&a.log).out;
                        let second = fold(&b.log).out;
                        first == 0 || second == 0 || first == second
                    })
                })
            }),
            Property::always(
                "reservation excludes intersecting records",
                |_, state: &State| !state.bad_reservation,
            ),
            Property::always("well-formed transaction prefix", |_, state: &State| {
                state.ds.iter().all(|dataset| {
                    let mut p = false;
                    for &(k, _, _, _, _) in &dataset.log {
                        if k == 1 {
                            if p {
                                return false;
                            }
                            p = true;
                        }
                        if k == 3 {
                            if !p {
                                return false;
                            }
                            p = false;
                        }
                    }
                    true
                })
            }),
            Property::always("I6 prefix determinism", |model, state: &State| {
                (0..state.ds.len()).all(|i| model.reader(state, i) == Some(fold(&state.ds[i].log)))
            }),
            Property::always("I8 visible replay equality", |model, state: &State| {
                (0..state.ds.len()).all(|i| {
                    model
                        .reader(state, i)
                        .is_some_and(|v| v.value == reference(&state.ds[i].log).value)
                })
            }),
            Property::always("I8 reader replay equality", |model, state: &State| {
                (0..state.ds.len())
                    .all(|i| model.reader(state, i) == Some(reference(&state.ds[i].log)))
            }),
            Property::always(
                "I8 recovery replay equality and unresolved Prepares",
                |_model, state: &State| {
                    state.ds.iter().all(|dataset| {
                        Self::decode(dataset).is_some_and(|v| fold(&v) == reference(&dataset.log))
                    })
                },
            ),
            Property::always(
                "I5 Prepare and Resolve CAS resolution sound",
                |_, state: &State| !state.bad_resolution,
            ),
            Property::always("I3 fencing", |_, state: &State| !state.bad_fence),
            Property::sometimes(
                "Prepare and Resolve survive batch eviction",
                |_, state: &State| {
                    state.ds.iter().any(|dataset| {
                        dataset
                            .storage
                            .index
                            .iter()
                            .any(|&(action, c)| dataset.log[action..c].iter().any(|r| r.0 == 3))
                            && dataset.storage.decode() == Some(dataset.log.clone())
                    })
                },
            ),
            Property::sometimes("simultaneous competing requests", |_, state: &State| {
                state
                    .ds
                    .iter()
                    .any(|dataset| dataset.req.is_some() && dataset.parked.is_some())
            }),
            Property::sometimes(
                if self.has("three-participants") {
                    "all three participants commit"
                } else {
                    "both participants commit"
                },
                |_, state: &State| state.ds.iter().all(|dataset| fold(&dataset.log).out == 1),
            ),
            Property::sometimes(
                if self.has("three-participants") {
                    "all three participants abort"
                } else {
                    "both participants abort"
                },
                |_, state: &State| state.ds.iter().all(|dataset| fold(&dataset.log).out == 2),
            ),
            Property::sometimes("the coordinator removes the records", |_, state: &State| {
                state.removed & 1 != 0
            }),
            Property::sometimes(
                "another process removes the records after a takeover",
                |_, state: &State| state.removed & 2 != 0,
            ),
            Property::sometimes(
                "a decision object recreated after removal",
                |_, state: &State| state.removed & 4 != 0,
            ),
            Property::sometimes(
                "the coordinator aborts at its deadline and reports it",
                |_, state: &State| {
                    state.reported == 2 && state.deciders & 4 != 0 && state.deciders & 2 == 0
                },
            ),
            Property::sometimes("non-owner Resolve after turnover", |_, state: &State| {
                state.nonowner
            }),
            Property::sometimes(
                "checkpoint rebuilds unresolved Prepare",
                |_, state: &State| {
                    state
                        .ds
                        .iter()
                        .any(|dataset| dataset.floor > 0 && fold(&dataset.root).pending)
                },
            ),
            Property::sometimes("identical retry accepted", |_, state: &State| {
                state.ds.iter().any(|dataset| {
                    dataset.req.as_ref().is_some_and(|r| {
                        r.done & 2 != 0
                            && dataset.log.get(r.pos)
                                == Some(&record(r.kind, r.out, r.base.identity()))
                    })
                })
            }),
        ];
        if self.has("projection") {
            properties.push(Property::always(
                "every pair projection transitions or stutters",
                Self::projection_step,
            ));
        }
        properties
    }
}

/// What one action of a transactions trace does, as the model takes it: the
/// expectation a replay of the trace through real storage checks against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub action: String,
    /// A record that landed on a participant's log: participant, kind (0 a
    /// write to another key, 1 `Prepare`, 2 a write to the prepared key, 3
    /// `Resolve`) and outcome (1 commit, 2 abort, for a `Resolve`).
    pub landed: Option<(usize, u8, u8)>,
    /// A delivery that did not land: participant, kind, and whether a newer
    /// lease fenced it (otherwise another record moved the head first).
    pub refused: Option<(usize, u8, bool)>,
    /// A participant whose lease the action advanced.
    pub fenced: Option<usize>,
    /// The decision object after the action: 0 none, 1 commit, 2 abort.
    pub decision: u8,
    /// The outcome the coordinator has reported after the action: 0 none, 1
    /// committed, 2 aborted.
    pub reported: u8,
    /// Whether the transaction's records exist after the action.
    pub records: bool,
    /// Whether the coordinator has a `Prepare` in flight to each participant
    /// before the action: its own knowledge, which it removes the records by.
    pub prepare_in_flight: Vec<bool>,
    /// Each participant's log after the action, as (kind, outcome).
    pub logs: Vec<Vec<(u8, u8)>>,
}

/// Step `actions` through the model in `mode`, or `None` when one is not
/// enabled where it occurs.
#[must_use]
pub fn steps(mode: &str, bound: usize, actions: &[String]) -> Option<Vec<Step>> {
    let protocol = Protocol::new(mode, bound);
    let mut state = protocol.init_states().pop()?;
    let mut out = Vec::with_capacity(actions.len());
    for text in actions {
        let (name, arguments) = match text.split_once('(') {
            Some((name, rest)) => (
                name,
                rest.strip_suffix(')')?
                    .split(',')
                    .map(|part| part.trim().parse::<usize>().ok())
                    .collect::<Option<Vec<_>>>()?,
            ),
            None => (text.as_str(), Vec::new()),
        };
        let action = match (name, arguments.as_slice()) {
            ("Issue", &[i, k]) => Action::Issue(i, k),
            ("Deliver", &[i, q]) => Action::Deliver(i, q),
            ("Check", &[i]) => Action::Check(i),
            ("Fence", &[i]) => Action::Fence(i),
            ("Deadline", &[]) => Action::Deadline,
            ("Decide", &[o]) => Action::Decide(o),
            ("Checkpoint", &[i]) => Action::Checkpoint(i),
            ("Publish", &[i]) => Action::Publish(i),
            ("Gc", &[i]) => Action::Gc(i),
            ("Switch", &[i]) => Action::Switch(i),
            ("Observe", &[i]) => Action::Observe(i),
            ("Advance", &[i]) => Action::Advance(i),
            ("Create", &[i, j]) => Action::Create(i, j),
            ("Forget", &[o]) => Action::Forget(o),
            ("Report", &[]) => Action::Report,
            _ => return None,
        };
        let mut enabled = vec![];
        protocol.actions(&state, &mut enabled);
        if !enabled.contains(&action) {
            return None;
        }
        let next = protocol.next_state(&state, action.clone())?;
        let grew = |i: usize| next.ds[i].log.len() > state.ds[i].log.len();
        let (landed, refused) = match action {
            Action::Deliver(i, _) | Action::Advance(i) if grew(i) => {
                let &(kind, outcome, ..) = next.ds[i].log.last()?;
                (Some((i, kind, outcome)), None)
            }
            Action::Deliver(i, _) => {
                let request = state.ds[i].req.as_ref()?;
                (
                    None,
                    Some((i, request.kind, request.base.lease != usize::from(state.ds[i].lease))),
                )
            }
            _ => (None, None),
        };
        out.push(Step {
            action: text.clone(),
            landed,
            refused,
            fenced: match action {
                Action::Fence(i) => Some(i),
                _ => None,
            },
            decision: next.recorded,
            reported: next.reported,
            records: next.records,
            prepare_in_flight: state
                .ds
                .iter()
                .map(|dataset| {
                    [&dataset.req, &dataset.parked]
                        .into_iter()
                        .any(|r| r.as_ref().is_some_and(|r| r.kind == 1))
                })
                .collect(),
            logs: next
                .ds
                .iter()
                .map(|dataset| dataset.log.iter().map(|&(k, o, ..)| (k, o)).collect())
                .collect(),
        });
        state = next;
    }
    Some(out)
}
