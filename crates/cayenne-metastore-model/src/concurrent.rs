// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use super::core::{Record, Store, record};
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub struct Protocol {
    name: String,
    bound: usize,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct View {
    pending: [bool; 2],
    out: [u8; 2],
    value: [usize; 2],
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Local {
    store: Store,
    log: Vec<Record>,
    attempted: [bool; 2],
    rejected: [bool; 2],
    writer: usize,
    objects: [bool; 2],
    checkpoint: bool,
    gc: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    ds: [Local; 2],
    decision: [u8; 2],
    deadline: [bool; 2],
    bad_reservation: bool,
    late_prepare: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Prepare(usize, usize),
    Resolve(usize, usize),
    Writer(usize),
    Deadline(usize),
    Decide(usize, usize),
    Seal(usize),
    Checkpoint(usize),
    Gc(usize),
    Advance(usize),
}
impl Protocol {
    pub fn new(name: &str, bound: usize) -> Self {
        Self {
            name: name.into(),
            bound,
        }
    }
    fn fp(&self, t: usize) -> usize {
        if self.name.contains("disjoint") { t } else { 0 }
    }
    fn intersects(&self, action: usize, b: usize) -> bool {
        self.fp(action) == self.fp(b)
    }
    // Actual apply: identity in safe mode; the mutation consumes another Prepare by footprint.
    fn fold(&self, log: &[Record], strict: bool) -> View {
        let mut v = View::default();
        for &(k, o, t, _, _) in log {
            match k {
                1 => {
                    if !strict || !(0..2).any(|u| v.pending[u] && self.intersects(t, u)) {
                        v.pending[t] = true;
                    }
                }
                2 => {
                    if !strict || !(0..2).any(|u| v.pending[u] && self.fp(u) == 0) {
                        v.value[0] += 2;
                    }
                }
                3 => {
                    let matched = if !strict && self.name.contains("no-match") {
                        (0..2).find(|&u| v.pending[u] && self.intersects(t, u))
                    } else {
                        v.pending[t].then_some(t)
                    };
                    if let Some(u) = matched {
                        v.pending[u] = false;
                        v.out[u] = o;
                        if o == 1 {
                            v.value[self.fp(u)] += 1;
                        }
                    }
                }
                _ => (),
            }
        }
        v
    }
    fn view(&self, d: &Local) -> Option<View> {
        d.store.decode().map(|v| self.fold(&v, false))
    }
    fn abort_cleanup(&self, state: &State) -> bool {
        for t in 0..2 {
            if state.decision[t] == 2 && state.ds.iter().any(|d| d.rejected[t]) {
                for d in &state.ds {
                    let mut d = d.clone();
                    let Some(view) = self.view(&d) else {
                        return false;
                    };
                    if view.pending[t] {
                        let r = record(3, 2, t);
                        if !Self::append(&mut d, r) {
                            d.store.observe("");
                            for j in 0..d.store.pending.len() {
                                d.store.create(j);
                            }
                            if !Self::append(&mut d, r) {
                                return false;
                            }
                        }
                        let Some(v) = self.view(&d) else { return false };
                        if v.pending[t] || v.out[t] != 2 {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }
    fn append(d: &mut Local, r: Record) -> bool {
        if !d.store.append(r, "") {
            return false;
        }
        d.log.push(r);
        true
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let seed = vec![record(0, 0, 0), record(0, 0, 2), record(0, 0, 4)];
        let mut store = Store::default();
        for r in &seed {
            if !store.append(*r, "") {
                return vec![];
            }
        }
        let d = Local {
            store,
            log: seed,
            attempted: [false; 2],
            rejected: [false; 2],
            writer: 0,
            objects: [true; 2],
            checkpoint: false,
            gc: false,
        };
        vec![State {
            ds: [d.clone(), d],
            decision: [0; 2],
            deadline: [false; 2],
            bad_reservation: false,
            late_prepare: false,
        }]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        use Action::{Advance, Checkpoint, Deadline, Decide, Gc, Prepare, Resolve, Seal, Writer};
        for i in 0..2 {
            let d = &state.ds[i];
            let Some(v) = self.view(d) else { return };
            for t in 0..2 {
                if !d.attempted[t] && d.objects[t] {
                    action.push(Prepare(i, t));
                }
                if state.decision[t] != 0
                    && (v.pending[t]
                        || self.name.contains("no-match")
                            && (0..2).any(|u| v.pending[u] && self.intersects(t, u)))
                {
                    action.push(Resolve(i, t));
                }
            }
            if d.writer < 1 + self.bound {
                action.push(Writer(i));
            }
            action.push(Seal(i));
            if self.bound > 0 && !d.checkpoint && d.log.len() > 3 {
                action.push(Checkpoint(i));
            }
            if !d.gc && (d.checkpoint || (0..2).any(|t| v.pending[t] || v.out[t] != 0)) {
                action.push(Gc(i));
            }
            // Padding after both outcomes gives Resolve records a chance to leave the window.
            if self.bound > 0 && v.out.iter().all(|o| *o != 0) && d.log.len() < 11 + self.bound {
                action.push(Advance(i));
            }
        }
        for t in 0..2 {
            if !state.deadline[t] {
                action.push(Deadline(t));
            }
            if state.decision[t] == 0 {
                if state.ds.iter().all(|d| {
                    d.store
                        .decode()
                        .is_some_and(|r| r.iter().any(|r| r.0 == 1 && r.2 == t))
                }) {
                    action.push(Decide(t, 1));
                }
                if state.deadline[t] {
                    action.push(Decide(t, 2));
                }
            }
        }
        crate::prune_for_simulation(self, state, action);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        use Action::{Advance, Checkpoint, Deadline, Decide, Gc, Prepare, Resolve, Seal, Writer};
        let mut next = state.clone();
        match action {
            Prepare(i, t) => {
                let v = self.view(&state.ds[i])?;
                let conflict = (0..2).any(|u| v.pending[u] && self.intersects(t, u));
                next.ds[i].attempted[t] = true;
                if conflict && !self.name.contains("no-reservation") {
                    next.ds[i].rejected[t] = true;
                } else {
                    if !Self::append(&mut next.ds[i], record(1, 0, t)) {
                        return None;
                    }
                    next.bad_reservation |= conflict;
                    next.late_prepare |= state.decision[t] == 2;
                }
            }
            Resolve(i, t) => {
                if !Self::append(&mut next.ds[i], record(3, state.decision[t], t)) {
                    return None;
                }
            }
            Writer(i) => {
                let v = self.view(&state.ds[i])?;
                next.ds[i].writer += 1;
                if !(0..2).any(|u| v.pending[u] && self.fp(u) == 0)
                    && !Self::append(&mut next.ds[i], record(2, 0, 10 + state.ds[i].writer))
                {
                    return None;
                }
            }
            Deadline(t) => next.deadline[t] = true,
            Decide(t, o) => next.decision[t] = u8::try_from(o).ok()?,
            Seal(i) => {
                let x = &mut next.ds[i].store;
                x.observe("");
                for j in 0..x.pending.len() {
                    x.create(j);
                }
            }
            Checkpoint(i) => {
                // The writer checkpoints at its newest batch end.
                let first = next.ds[i].store.first;
                if !next.ds[i].store.checkpoint(first) {
                    return None;
                }
                next.ds[i].checkpoint = true;
            }
            Gc(i) => {
                let v = self.view(&state.ds[i])?;
                next.ds[i].store.gc();
                next.ds[i].gc = true;
                for t in 0..2 {
                    if !v.pending[t]
                        && v.out[t] != 1
                        && (state.ds[i].attempted[t] || state.decision[t] == 2)
                    {
                        next.ds[i].objects[t] = false;
                    }
                }
            }
            Advance(i) => {
                if !Self::append(&mut next.ds[i], record(0, 0, 20 + state.ds[i].log.len())) {
                    return None;
                }
            }
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "I4 both transaction roots retained",
                |model, state: &State| {
                    state.ds.iter().all(|d| {
                        let Some(v) = model.view(d) else { return false };
                        (0..2).all(|t| (!v.pending[t] && v.out[t] != 1) || d.objects[t])
                    })
                },
            ),
            Property::always(
                "I6 each applied outcome equals its own decision",
                |model, state: &State| {
                    state.ds.iter().all(|d| {
                        let Some(v) = model.view(d) else { return false };
                        (0..2).all(|t| v.out[t] == 0 || v.out[t] == state.decision[t])
                    })
                },
            ),
            Property::always(
                "I6 participants agree per transaction",
                |model, state: &State| {
                    let Some(action) = model.view(&state.ds[0]) else {
                        return false;
                    };
                    let Some(b) = model.view(&state.ds[1]) else {
                        return false;
                    };
                    (0..2).all(|t| action.out[t] == 0 || b.out[t] == 0 || action.out[t] == b.out[t])
                },
            ),
            Property::always(
                "reservation includes other transactions",
                |_, state: &State| !state.bad_reservation,
            ),
            Property::always("rejected Prepare can only abort", |_, state: &State| {
                (0..2).all(|t| !state.ds.iter().any(|d| d.rejected[t]) || state.decision[t] != 1)
            }),
            Property::always(
                "deadline abort cleanup remains possible",
                |model, state: &State| model.abort_cleanup(state),
            ),
            Property::always(
                "I8 visible effects equal serial admissible prefix",
                |model, state: &State| {
                    state.ds.iter().all(|d| {
                        model
                            .view(d)
                            .is_some_and(|v| v.value == model.fold(&d.log, true).value)
                    })
                },
            ),
            Property::always(
                "I6 prefix determinism and reader replay equality",
                |model, state: &State| {
                    state.ds.iter().all(|d| {
                        d.store.decode().is_some_and(|r| {
                            r == d.log && model.fold(&r, false) == model.fold(&d.log, true)
                        })
                    })
                },
            ),
            Property::always(
                "I8 recovery replay equality pending and visible",
                |model, state: &State| {
                    state.ds.iter().all(|d| {
                        d.store
                            .decode()
                            .is_some_and(|r| model.fold(&r, false) == model.fold(&d.log, true))
                    })
                },
            ),
            Property::sometimes("late Prepare after abort decision", |_, state: &State| {
                state.late_prepare
            }),
            Property::sometimes(
                "both transactions commit on both participants",
                |model, state: &State| {
                    state
                        .ds
                        .iter()
                        .all(|d| model.view(d).is_some_and(|v| v.out == [1, 1]))
                },
            ),
            Property::sometimes(
                "both transactions abort on both participants",
                |model, state: &State| {
                    state
                        .ds
                        .iter()
                        .all(|d| model.view(d).is_some_and(|v| v.out == [2, 2]))
                },
            ),
            Property::sometimes(
                "opposite reservations both abort cleanly",
                |model, state: &State| {
                    state.ds[0].rejected[1]
                        && state.ds[1].rejected[0]
                        && state.decision == [2, 2]
                        && state
                            .ds
                            .iter()
                            .all(|d| model.view(d).is_some_and(|v| !v.pending.iter().any(|p| *p)))
                },
            ),
            Property::sometimes(
                "both Prepares simultaneously pending",
                |model, state: &State| {
                    state
                        .ds
                        .iter()
                        .any(|d| model.view(d).is_some_and(|v| v.pending == [true, true]))
                },
            ),
            Property::sometimes("both transaction ids enter batches", |_, state: &State| {
                state.ds.iter().any(|d| {
                    (0..2).all(|t| {
                        d.store.index.iter().any(|&(action, b)| {
                            d.log[action..b].iter().any(|r| r.0 == 1 && r.2 == t)
                        })
                    })
                })
            }),
            Property::sometimes("Resolve enters a batch", |_, state: &State| {
                state.ds.iter().any(|d| {
                    d.store
                        .index
                        .iter()
                        .any(|&(action, b)| d.log[action..b].iter().any(|r| r.0 == 3))
                })
            }),
            Property::sometimes("both Resolves enter batches", |_, state: &State| {
                state.ds.iter().any(|d| {
                    (0..2).all(|t| {
                        d.store.index.iter().any(|&(action, b)| {
                            d.log[action..b].iter().any(|r| r.0 == 3 && r.2 == t)
                        })
                    })
                })
            }),
            Property::sometimes(
                "checkpoint contains both pending ids",
                |model, state: &State| {
                    state
                        .ds
                        .iter()
                        .any(|d| model.fold(&d.store.root, false).pending == [true, true])
                },
            ),
        ]
    }
}
