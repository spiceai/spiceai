// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! An older reader encountering an action understood by the serial oracle.
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    skip: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    log: Vec<bool>,
    read: Option<Result<Vec<usize>, ()>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Append(bool),
    Open,
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self {
            skip: mode.contains("skip"),
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
        if s.log.len() < 2 {
            a.extend([Action::Append(false), Action::Append(true)]);
        }
        a.push(Action::Open);
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        match a {
            Action::Append(unknown) => {
                n.log.push(unknown);
                n.read = None;
            }
            Action::Open => {
                n.read = Some(if s.log.contains(&true) && !self.skip {
                    Err(())
                } else {
                    Ok(s.log
                        .iter()
                        .enumerate()
                        .filter(|(_, u)| !**u)
                        .map(|(i, _)| i)
                        .collect())
                });
            }
        }
        (n != *s).then_some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("I11 compatible replay or refusal", |_, s: &State| {
                s.read.as_ref().is_none_or(|r| {
                    r.as_ref()
                        .map_or(true, |rows| *rows == (0..s.log.len()).collect::<Vec<_>>())
                })
            }),
            Property::sometimes("unknown action refuses dataset", |_, s: &State| {
                s.log.contains(&true) && s.read == Some(Err(()))
            }),
        ]
    }
}
