// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Finite fair schedules: uploads complete, and cleanup work is eventually serviced.
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    single: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    finished: bool,
    fenced: bool,
    present: bool,
    swept: bool,
    selected: bool,
    collected: bool,
    // History distinguishes DAG joins for Stateright's eventually implementation.
    history: Vec<Action>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    FinishUpload,
    Takeover,
    Sweep,
    Delete,
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self {
            single: mode.contains("single-sweep"),
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if !s.finished {
            a.push(Action::FinishUpload);
        }
        if !s.fenced {
            a.push(Action::Takeover);
        }
        if s.fenced && (!s.swept || !self.single && s.present && !s.selected) {
            a.push(Action::Sweep);
        }
        if s.selected {
            a.push(Action::Delete);
        }
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        n.history.push(a.clone());
        match a {
            Action::FinishUpload => {
                n.finished = true;
                n.present = true;
            }
            Action::Takeover => n.fenced = true,
            Action::Sweep => {
                n.swept = true;
                n.selected = s.present;
            }
            Action::Delete => {
                n.selected = false;
                n.present = false;
                n.collected = true;
            }
        }
        Some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::eventually("orphan eventually collected", |_, s: &State| {
                s.finished && s.collected && !s.present
            }),
            Property::sometimes("upload finishes after first sweep", |_, s: &State| {
                s.history
                    .windows(2)
                    .any(|a| a == [Action::Sweep, Action::FinishUpload])
            }),
        ]
    }
}
