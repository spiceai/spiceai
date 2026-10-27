// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

use crate::core::Head;
use stateright::{Model, Property};

/// Object admission and delayed collection composed with a conditional head.
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    admission: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    head: Head,
    uploaded: bool,
    planned: bool,
    closed: bool,
    request: Option<Head>,
    selected: bool,
    deleted: bool,
    accepted: bool,
    bad: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Upload,
    Plan,
    Close,
    Select,
    Delete,
    Rebase,
    Publish,
}
impl Protocol {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            admission: !name.contains("no-admission"),
        }
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }
    fn actions(&self, state: &State, action: &mut Vec<Action>) {
        if !state.uploaded {
            action.push(Action::Upload);
        }
        if state.uploaded && !state.planned && !state.closed {
            action.push(Action::Plan);
        }
        if !state.closed {
            action.push(Action::Close);
        }
        if state.closed && !state.selected && !state.accepted {
            action.push(Action::Select);
        }
        if state.selected && !state.deleted {
            action.push(Action::Delete);
        }
        if state.planned && state.request.is_none() {
            action.push(Action::Rebase);
        }
        if state.request.is_some() && !state.accepted {
            action.push(Action::Publish);
        }
        action.retain(|action| self.next_state(state, action.clone()).is_some());
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Upload => next.uploaded = true,
            Action::Plan => next.planned = true,
            Action::Close => {
                next.closed = true;
                next.head.pos += 1;
            }
            Action::Select => next.selected = true,
            Action::Delete => {
                next.deleted = true;
                next.bad |= state.accepted;
            }
            Action::Rebase => {
                if crate::core::admits_reference(state.closed, state.accepted) || !self.admission {
                    next.request = Some(state.head);
                }
            }
            Action::Publish => {
                if state.request == Some(state.head) {
                    next.accepted = true;
                    next.head.pos += 1;
                    next.bad |= state.deleted;
                }
            }
        }
        (next != *state).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("I4 admitted object survives GC", |_, state: &State| {
                !state.bad
            }),
            Property::sometimes(
                "closed generation rejects delayed upload",
                |_, state: &State| state.closed && state.planned && state.request.is_none(),
            ),
            Property::sometimes("accepted object retained", |_, state: &State| {
                state.accepted && !state.deleted
            }),
        ]
    }
}
