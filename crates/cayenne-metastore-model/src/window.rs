// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::{Record, Store, payload, record};
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    store: Store,
    log: Vec<Record>,
    /// A running query; retention keeps its state's batches.
    query: Option<Store>,
    checkpoint: bool,
    gc: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Append(usize),
    Observe,
    Create(usize),
    Checkpoint(usize),
    Gc,
    Query,
}
impl Protocol {
    pub fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            store: Store::default(),
            log: vec![],
            query: None,
            checkpoint: false,
            gc: false,
        }]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        if state.log.len() < self.bound {
            for z in 0..2 {
                action.push(Action::Append(z));
            }
        }
        action.push(Action::Observe);
        for j in 0..state.store.pending.len() {
            action.push(Action::Create(j));
        }
        if !state.checkpoint {
            for c in state.store.checkpoint_ends() {
                action.push(Action::Checkpoint(c));
            }
        }
        if state.store.floor > 0 && !state.gc {
            action.push(Action::Gc);
        }
        if state.query.is_none() {
            action.push(Action::Query);
        }
        crate::prune_for_simulation(self, state, action);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Append(z) => {
                let r = record(2, 0, 2 * state.log.len() + z);
                if !next.store.append(r, &self.mode) {
                    return None;
                }
                next.log.push(r);
            }
            Action::Observe => next.store.observe(&self.mode),
            Action::Create(j) => next.store.create(j),
            Action::Checkpoint(c) => {
                if !next.store.checkpoint(c) {
                    return None;
                }
                next.checkpoint = true;
            }
            Action::Gc => {
                let saved = next.store.batches.clone();
                next.store.gc();
                if let Some(query) = &next.query {
                    for (action, v) in saved {
                        if query.index.iter().any(|(b, _)| *b == action)
                            && !next.store.batches.iter().any(|(b, _)| *b == action)
                        {
                            next.store.batches.push((action, v));
                            next.store.deleted.retain(|b| *b != action);
                        }
                    }
                    next.store.batches.sort();
                }
                next.gc = true;
            }
            Action::Query => {
                let mut p = next.store.clone();
                p.batches.clear();
                p.pending.clear();
                next.query = Some(p);
            }
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "I4 reader inline payload replay equality",
                |_, state: &State| state.store.decode() == Some(state.log.clone()),
            ),
            Property::always(
                "I8 recovery replay equality including payload bytes",
                |_, state: &State| {
                    state
                        .store
                        .decode()
                        .map(|v| v.into_iter().map(payload).collect::<Vec<_>>())
                        == Some(state.log.iter().copied().map(payload).collect())
                },
            ),
            Property::always("I7 query snapshot equals serial", |_, state: &State| {
                state.query.as_ref().is_none_or(|p| {
                    let mut p = p.clone();
                    p.batches.clone_from(&state.store.batches);
                    p.decode().is_some_and(|v| state.log.starts_with(&v))
                })
            }),
            Property::sometimes("byte and count cuts", |_, state: &State| {
                state.store.byte_cut && state.store.count_cut
            }),
            Property::sometimes(
                "head byte and count budgets both force drops",
                |_, state: &State| state.store.byte_pressure && state.store.count_pressure,
            ),
            Property::sometimes("delayed seal after GC", |_, state: &State| {
                state.store.revived && state.gc
            }),
            Property::sometimes("conflicting seal bytes", |_, state: &State| {
                state.store.conflict
            }),
        ]
    }
}
