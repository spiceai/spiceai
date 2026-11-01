// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::{Record, Store, payload, record};
use stateright::{Model, Property};
// Positions are zero-based; floor is the exclusive checkpoint prefix length.
#[derive(Clone, Debug)]
pub struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub struct State {
    store: Store,
    log: Vec<Record>,
    captured: Option<Store>,
    /// A running query, started on the current state; retention keeps it.
    query: Option<Store>,
    published: bool,
    scan: bool,
    deleted: bool,
    queue: Vec<usize>,
    bad_read: bool,
    bad_query: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Append(usize),
    Observe,
    Create(usize),
    Floor(usize),
    Poll,
    Read,
    Query,
    Scan,
    Delete,
}
impl Protocol {
    pub fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn read(&self, p: &Store, live: &Store) -> Option<Vec<Record>> {
        let mut p = p.clone();
        p.batches.clone_from(&live.batches);
        p.read(&self.mode)
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            store: Store::default(),
            log: vec![],
            captured: None,
            query: None,
            published: false,
            scan: false,
            deleted: false,
            queue: vec![],
            bad_read: false,
            bad_query: false,
        }]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        use Action::{Append, Create, Delete, Floor, Observe, Poll, Query, Read, Scan};
        if state.log.len() < self.bound {
            action.extend([Append(0), Append(1)]);
        }
        action.push(Observe);
        for j in 0..state.store.pending.len() {
            action.push(Create(j));
        }
        if !state.published {
            for c in state.store.floor + 1..=state.store.first {
                if c == state.store.first || state.store.index.iter().any(|&(_, b)| b == c) {
                    action.push(Floor(c));
                }
            }
        }
        if state.captured.is_none() {
            action.push(Poll);
        }
        if state.captured.is_some() {
            action.push(Read);
        }
        if state.query.is_none() {
            action.push(Query);
        }
        if state.published && !state.scan {
            action.push(Scan);
        }
        if state.scan && !state.deleted {
            action.push(Delete);
        }
        crate::prune_for_simulation(self, state, action);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        use Action::{Append, Create, Delete, Floor, Observe, Poll, Query, Read, Scan};
        let mut next = state.clone();
        match action {
            Append(z) => {
                let r = record(2, 0, 2 * state.log.len() + z);
                if !next.store.append(r, "") {
                    return None;
                }
                next.log.push(r);
            }
            Observe => next.store.observe(""),
            Create(j) => next.store.create(j),
            Floor(c) => {
                if !next.store.publish(c, &self.mode) {
                    return None;
                }
                next.published = true;
            }
            Poll => {
                let mut p = state.store.clone();
                p.batches.clear();
                p.pending.clear();
                next.captured = Some(p);
            }
            Read => {
                let p = state.captured.as_ref()?;
                let p = if p.floor < state.store.floor {
                    &state.store
                } else {
                    p
                };
                let target = p.first + p.window.len();
                next.bad_read |= self.read(p, &state.store) != Some(state.log[..target].to_vec());
            }
            Query => {
                let mut p = state.store.clone();
                p.batches.clear();
                p.pending.clear();
                next.query = Some(p);
            }
            Scan => {
                next.scan = true;
                next.queue = state
                    .store
                    .batches
                    .iter()
                    .filter(|(action, b)| {
                        let eligible = if self.mode.contains("no-gc") {
                            *action < state.store.floor
                        } else {
                            *action + b.len() <= state.store.floor
                        };
                        eligible
                            && !state
                                .query
                                .as_ref()
                                .is_some_and(|p| p.index.iter().any(|(x, _)| x == action))
                    })
                    .map(|(action, _)| *action)
                    .collect();
            }
            Delete => {
                next.deleted = true;
                next.store
                    .batches
                    .retain(|(action, _)| !state.queue.contains(action));
                for action in &state.queue {
                    if !next.store.deleted.contains(action) {
                        next.store.deleted.push(*action);
                    }
                }
            }
        }
        if let Some(p) = next.query.as_ref() {
            let target = p.first + p.window.len();
            next.bad_query |= self.read(p, &next.store) != Some(next.log[..target].to_vec());
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("I4 cold open exact bytes", |model, state: &State| {
                state.store.read(&model.mode) == Some(state.log.clone())
            }),
            Property::always(
                "I8 reader replay equality after reopen",
                |_, state: &State| !state.bad_read,
            ),
            Property::always("I8 recovery replay equality", |model, state: &State| {
                state
                    .store
                    .read(&model.mode)
                    .map(|v| v.into_iter().map(payload).collect::<Vec<_>>())
                    == Some(state.log.iter().copied().map(payload).collect())
            }),
            Property::always("I7 query snapshot", |_, state: &State| !state.bad_query),
            Property::sometimes("floor inside listed batch", |_, state: &State| {
                state
                    .store
                    .index
                    .iter()
                    .any(|&(action, b)| action < state.store.floor && state.store.floor < b)
            }),
            Property::sometimes("floor at batch end", |_, state: &State| {
                state.published
                    && state
                        .store
                        .batches
                        .iter()
                        .any(|(action, b)| *action + b.len() == state.store.floor)
            }),
            Property::sometimes("floor at w_first minus one", |_, state: &State| {
                state.published && state.store.floor == state.store.first
            }),
            Property::sometimes("query before floor survives GC", |_, state: &State| {
                state.deleted
                    && state
                        .query
                        .as_ref()
                        .is_some_and(|p| p.floor < state.store.floor)
            }),
            Property::sometimes("delayed seal after GC", |_, state: &State| {
                state.store.revived
            }),
        ]
    }
}
