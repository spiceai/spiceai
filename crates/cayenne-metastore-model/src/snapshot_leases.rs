// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! GC-observer monotonic lapse and delayed reads of immutable snapshots.
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    commit: usize,
    floor: usize,
    clock: usize,
    expires: usize,
    present: [bool; 3],
    query: Option<usize>,
    cursor: usize,
    rows: Vec<usize>,
    pending: Option<usize>,
    result: Option<Result<Vec<usize>, ()>>,
    transaction: Option<usize>,
    pin: Option<usize>,
    ended: bool,
    read_transaction: Option<usize>,
    paused: bool,
    unchecked: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Begin,
    Advance,
    Floor,
    Tick,
    Renew,
    Collect(usize),
    Read,
    Complete,
    Commit,
    Abort,
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    fn refs(v: usize) -> Vec<usize> {
        if v == 0 { vec![0, 1] } else { vec![2] }
    }
    fn low(&self, s: &State) -> usize {
        let live = self.has("transaction") || s.clock < s.expires;
        if live {
            s.pin.unwrap_or(s.floor).min(s.floor)
        } else {
            s.floor
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            present: [true, true, false],
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.query.is_none() && s.transaction.is_none() && !s.ended {
            a.push(Action::Begin);
        }
        if s.commit == 0 {
            a.push(Action::Advance);
        }
        if s.floor < s.commit {
            a.push(Action::Floor);
        }
        if !self.has("transaction") && s.clock < 2 {
            a.push(Action::Tick);
        }
        if s.pin.is_some() && s.clock < s.expires && s.expires < 3 {
            a.push(Action::Renew);
        }
        let keep: Vec<_> = (self.low(s)..=s.commit).flat_map(Self::refs).collect();
        for id in 0..3 {
            if s.present[id] && !keep.contains(&id) {
                a.push(Action::Collect(id));
            }
        }
        if self.has("transaction") {
            if s.transaction.is_some() {
                if s.read_transaction.is_none() {
                    a.push(Action::Read);
                }
                a.extend([Action::Commit, Action::Abort]);
            }
        } else if s.query.is_some() && s.result.is_none() && s.pending.is_none() {
            a.push(Action::Read);
        }
        if s.pending.is_some() {
            a.push(Action::Complete);
        }
        a.retain(|a| self.next_state(s, a.clone()).is_some());
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        match a {
            Action::Begin => {
                n.pin = Some(s.commit);
                n.expires = s.clock + 1;
                if self.has("transaction") {
                    n.transaction = Some(s.commit);
                } else {
                    n.query = Some(s.commit);
                }
            }
            Action::Advance => {
                n.commit = 1;
                n.present[2] = true;
                if self.has("ignore-transactions") {
                    n.pin = Some(1);
                }
            }
            Action::Floor => n.floor = s.commit,
            Action::Tick => {
                n.clock += 1;
                n.paused |= s.cursor > 0 && s.result.is_none();
            }
            Action::Renew => n.expires = s.clock + 1,
            Action::Collect(id) => n.present[id] = false,
            Action::Read => {
                if let Some(begin) = s.transaction {
                    n.read_transaction = Some(s.commit);
                    n.result = Some(if Self::refs(begin).iter().all(|&id| s.present[id]) {
                        Ok(Self::refs(begin))
                    } else {
                        Err(())
                    });
                } else {
                    let v = s.query?;
                    if s.clock >= s.expires && !self.has("no-lapse-recheck") && s.floor > v {
                        n.result = Some(Err(()));
                    } else {
                        n.unchecked |= s.clock >= s.expires && self.has("no-lapse-recheck");
                        n.pending = Self::refs(v).get(s.cursor).copied();
                    }
                }
            }
            Action::Complete => {
                let id = n.pending.take()?;
                if !s.present[id] && !self.has("missing-as-empty") {
                    n.result = Some(Err(()));
                } else {
                    if s.present[id] {
                        n.rows.push(id);
                    }
                    n.cursor += 1;
                    if n.cursor == Self::refs(s.query?).len() {
                        n.result = Some(Ok(n.rows.clone()));
                    }
                }
            }
            Action::Commit | Action::Abort => {
                n.transaction = None;
                n.pin = None;
                n.ended = true;
            }
        }
        (n != *s).then_some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        if self.has("transaction") {
            vec![
                Property::always("I7 transaction begin snapshot readable", |_, s: &State| {
                    s.transaction
                        .is_none_or(|v| Self::refs(v).iter().all(|&id| s.present[id]))
                }),
                Property::sometimes(
                    "transaction reads begin after head advances",
                    |_, s: &State| s.read_transaction == Some(1) && s.transaction == Some(0),
                ),
                Property::sometimes("transaction finishes", |_, s: &State| s.ended),
            ]
        } else {
            vec![
                Property::always("snapshot rows or error", |_, s: &State| {
                    s.result.as_ref().is_none_or(|r| {
                        r.as_ref()
                            .map_or(true, |rows| Some(rows.clone()) == s.query.map(Self::refs))
                    })
                }),
                Property::sometimes("lapsed query errors", |_, s: &State| {
                    s.clock >= s.expires && s.result == Some(Err(()))
                }),
                Property::sometimes("paused partial scan resumes", |_, s: &State| {
                    s.paused && s.cursor == 2 && s.result.as_ref().is_some_and(Result::is_ok)
                }),
                Property::sometimes("expired read without head recheck", |_, s: &State| {
                    s.unchecked
                }),
            ]
        }
    }
}
