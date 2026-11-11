// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::{Record, Store, payload, record};
use stateright::{Model, Property};

/// The newest model time; [`Action::Tick`] advances the store clock by one
/// retention.
const LAST_TIME: u8 = 2;
const MAX_CHECKPOINTS: u8 = 2;
const MAX_GCS: u8 = 3;

/// Deletion below the floor, as the head log's collector does it. A floor the
/// collector has not seen before is stamped with the store time of the
/// collection that first sees it, which is no earlier than its publication.
/// Objects below a stamped floor are deleted once the store clock is one
/// retention past the stamp: a reader that read an older head read it before
/// the floor was published, so it has had a whole retention to finish. A
/// restarted collector knows no stamps and stamps the current floor afresh.
///
/// With `gc-early` the collector deletes as soon as it stamps a floor, without
/// waiting a retention: a reader of an older head can then find its batches or
/// its checkpoint gone.
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
    /// A reader that read the head at the given store time and reads the
    /// objects it lists later.
    query: Option<(Store, u8)>,
    checkpoints: u8,
    gcs: u8,
    /// The store clock, in retentions.
    time: u8,
    /// Checkpoint objects that exist, by the commit they are at.
    roots: Vec<usize>,
    /// Floors the collector has seen, oldest first, with their stamps.
    retired: Vec<(usize, Option<u8>)>,
    /// The floor the collector last deleted below.
    collected: usize,
    restarted: bool,
    root_collected: bool,
    collected_after_restart: bool,
    /// A collection ran while a reader of an older head was still within
    /// its retention.
    gc_during_query: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Append(usize),
    Observe,
    Create(usize),
    Checkpoint(usize),
    Gc,
    Query,
    Tick,
    /// The collector restarts and forgets its stamps.
    Restart,
}
impl Protocol {
    pub fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn retention(&self) -> u8 {
        u8::from(!self.mode.contains("gc-early"))
    }
}
impl State {
    /// What a cold open reads now: the checkpoint object at the floor, then the
    /// listed batches and the window. `None` when a needed object is gone.
    fn read(&self) -> Option<Vec<Record>> {
        Self::read_at(&self.store, &self.store, &self.roots)
    }
    /// What `query` reads from the objects that exist now.
    fn read_query(&self) -> Option<Vec<Record>> {
        let (query, _) = self.query.as_ref()?;
        Self::read_at(query, &self.store, &self.roots)
    }
    fn read_at(head: &Store, live: &Store, roots: &[usize]) -> Option<Vec<Record>> {
        if head.floor > 0 && !roots.contains(&head.floor) {
            return None;
        }
        let mut head = head.clone();
        head.batches.clone_from(&live.batches);
        head.decode()
    }
    /// The records a head lists, which a reader of it must return.
    fn snapshot(&self, head: &Store) -> Vec<Record> {
        self.log[..head.first + head.window.len()].to_vec()
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
            checkpoints: 0,
            gcs: 0,
            time: 0,
            roots: vec![],
            retired: vec![],
            collected: 0,
            restarted: false,
            root_collected: false,
            collected_after_restart: false,
            gc_during_query: false,
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
        if state.checkpoints < MAX_CHECKPOINTS && state.store.staged.is_none() {
            for c in state.store.checkpoint_ends() {
                action.push(Action::Checkpoint(c));
            }
        }
        if state.gcs < MAX_GCS {
            action.push(Action::Gc);
        }
        if state.query.is_none() {
            action.push(Action::Query);
        }
        if state.time < LAST_TIME {
            action.push(Action::Tick);
        }
        if !state.restarted {
            action.push(Action::Restart);
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
                if !next.roots.contains(&c) {
                    next.roots.push(c);
                    next.roots.sort_unstable();
                }
                next.checkpoints += 1;
            }
            Action::Gc => {
                self.collect(&mut next);
                next.gcs += 1;
            }
            Action::Query => {
                let mut p = next.store.clone();
                p.batches.clear();
                p.pending.clear();
                next.query = Some((p, state.time));
            }
            Action::Tick => next.time += 1,
            Action::Restart => {
                next.retired.clear();
                next.collected = 0;
                next.restarted = true;
            }
        }
        if next == *state { None } else { Some(next) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "I4 reader inline payload replay equality",
                |_, state: &State| state.read() == Some(state.log.clone()),
            ),
            Property::always(
                "I8 recovery replay equality including payload bytes",
                |_, state: &State| {
                    state
                        .read()
                        .map(|v| v.into_iter().map(payload).collect::<Vec<_>>())
                        == Some(state.log.iter().copied().map(payload).collect())
                },
            ),
            Property::always(
                "I7 query within retention reads its snapshot",
                |_, state: &State| {
                    state.query.as_ref().is_none_or(|(p, at)| {
                        state.time > *at || state.read_query() == Some(state.snapshot(p))
                    })
                },
            ),
            Property::always(
                "I7 expired query reads its snapshot or finds an object missing",
                |_, state: &State| {
                    state.query.as_ref().is_none_or(|(p, _)| {
                        state
                            .read_query()
                            .is_none_or(|v| v == state.snapshot(p))
                    })
                },
            ),
            Property::sometimes("byte and count cuts", |_, state: &State| {
                state.store.byte_cut && state.store.count_cut
            }),
            Property::sometimes(
                "head byte and count budgets both force drops",
                |_, state: &State| state.store.byte_pressure && state.store.count_pressure,
            ),
            Property::sometimes("delayed seal after GC", |_, state: &State| {
                state.store.revived && state.gcs > 0
            }),
            Property::sometimes("conflicting seal bytes", |_, state: &State| {
                state.store.conflict
            }),
            Property::sometimes("batches and a checkpoint collected", |_, state: &State| {
                state.root_collected && !state.store.deleted.is_empty()
            }),
            Property::sometimes(
                "collection while a reader of an older head is within retention",
                |_, state: &State| state.gc_during_query,
            ),
            Property::sometimes(
                "expired reader of an older head finds an object collected",
                |_, state: &State| {
                    state.query.as_ref().is_some_and(|(_, at)| state.time > *at)
                        && state.read_query().is_none()
                },
            ),
            Property::sometimes("restarted collector collects", |_, state: &State| {
                state.collected_after_restart
            }),
        ]
    }
}
impl Protocol {
    /// One collection against the published head: stamp the newly seen floor,
    /// then delete below the newest floor whose stamp is a retention old. The
    /// rule names objects the way the head log does: a batch by its first
    /// commit (`action + 1`), a checkpoint by its commit.
    fn collect(&self, next: &mut State) {
        let floor = next.store.floor;
        let newest = next.retired.last().map_or(next.collected, |&(f, _)| f);
        if floor > newest {
            next.retired.push((floor, None));
        }
        let now = next.time;
        for (_, stamp) in &mut next.retired {
            stamp.get_or_insert(now);
        }
        let due = next
            .retired
            .iter()
            .filter(|(_, stamp)| stamp.is_some_and(|s| s + self.retention() <= now))
            .map(|&(f, _)| f)
            .max();
        if next
            .query
            .as_ref()
            .is_some_and(|(p, at)| *at == now && p.floor < floor)
        {
            next.gc_during_query = true;
        }
        let Some(due) = due else { return };
        let doomed: Vec<usize> = next
            .store
            .batches
            .iter()
            .map(|(action, _)| *action)
            .filter(|action| action + 1 <= due)
            .collect();
        next.store
            .batches
            .retain(|(action, _)| !doomed.contains(action));
        for action in doomed {
            if !next.store.deleted.contains(&action) {
                next.store.deleted.push(action);
            }
        }
        let before = next.roots.len();
        next.roots.retain(|&c| c >= due);
        next.root_collected |= next.roots.len() < before;
        next.collected_after_restart |= next.restarted;
        next.collected = due;
        next.retired.retain(|&(f, _)| f > due);
    }
}

/// What a window trace leaves in storage, from the model alone, so a replay
/// takes its expectations from the model rather than from the code it checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Each listed batch's end: its last record's commit, counting from 1.
    pub batch_ends: Vec<usize>,
    /// Each inline record's symbolic size.
    pub window: Vec<usize>,
    /// The first commit of every batch object that exists.
    pub batch_objects: Vec<usize>,
    /// The commit of every checkpoint object that exists.
    pub checkpoint_objects: Vec<usize>,
}

/// Step the model through `actions`. `None` when an action does not parse or
/// is not enabled.
#[must_use]
pub fn outcome_after(mode: &str, bound: usize, actions: &[String]) -> Option<Outcome> {
    let protocol = Protocol::new(mode, bound);
    let mut state = protocol.init_states().pop()?;
    for action in actions {
        let (name, argument) = match action.split_once('(') {
            Some((name, rest)) => (name, Some(rest.strip_suffix(')')?.parse().ok()?)),
            None => (action.as_str(), None),
        };
        let action = match (name, argument) {
            ("Append", Some(z)) => Action::Append(z),
            ("Observe", None) => Action::Observe,
            ("Create", Some(j)) => Action::Create(j),
            ("Checkpoint", Some(c)) => Action::Checkpoint(c),
            ("Gc", None) => Action::Gc,
            ("Query", None) => Action::Query,
            ("Tick", None) => Action::Tick,
            ("Restart", None) => Action::Restart,
            _ => return None,
        };
        let mut enabled = vec![];
        protocol.actions(&state, &mut enabled);
        if !enabled.contains(&action) {
            return None;
        }
        state = protocol.next_state(&state, action)?;
    }
    Some(Outcome {
        batch_ends: state.store.index.iter().map(|&(_, end)| end).collect(),
        window: state
            .store
            .window
            .iter()
            .map(|r| crate::core::size(*r))
            .collect(),
        batch_objects: state
            .store
            .batches
            .iter()
            .map(|(action, _)| action + 1)
            .collect(),
        checkpoint_objects: state.roots.clone(),
    })
}
