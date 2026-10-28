// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Single-owner removal accounting and one predecessor-generation sweep.
use crate::core::Head;
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct State {
    head: Head,
    versions: Vec<Vec<usize>>,
    present: [bool; 4],
    started: [bool; 4],
    uploading: [bool; 4],
    ready: [bool; 4],
    plan: Option<(Head, usize)>,
    removed: [Option<usize>; 4],
    deleting: Vec<usize>,
    swept: [bool; 2],
    pin: Option<usize>,
    queries: Vec<usize>,
    reused: bool,
    late_upload: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    StartUpload(usize),
    FinishUpload(usize),
    Reuse(usize),
    Plan(usize),
    Publish,
    Remove(usize),
    Takeover,
    Floor,
    Pin,
    SecondQuery,
    SelectRemoved(usize),
    Sweep(usize),
    Delete(usize),
    DiscardFailed(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, mode: &str) -> bool {
        self.mode.contains(mode)
    }
    fn low(s: &State) -> usize {
        s.pin.unwrap_or(s.head.floor).min(s.head.floor)
    }
    fn removal_low(&self, s: &State) -> usize {
        if self.has("no-pins") {
            s.head.floor
        } else {
            Self::low(s)
        }
    }
    fn current(s: &State) -> &[usize] {
        &s.versions[s.head.pos]
    }
    fn referenced(s: &State, low: usize, id: usize) -> bool {
        s.versions[low..].iter().any(|v| v.contains(&id))
    }
    fn reusable(&self, s: &State, id: usize) -> bool {
        // Current-owner retirement is serialized with create/reuse, including delete delivery.
        (!self.has("serialized") || !s.deleting.contains(&id)) && id / 2 == s.head.lease
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            versions: vec![vec![]],
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for id in 0..4 {
            if s.head.pos < self.bound {
                if !s.started[id] && self.reusable(s, id) {
                    a.push(Action::StartUpload(id));
                }
                if s.present[id] && !s.ready[id] && self.reusable(s, id) {
                    a.push(Action::Reuse(id));
                }
                if s.ready[id]
                    && s.plan.is_none()
                    && !Self::current(s).contains(&id)
                    && self.reusable(s, id)
                {
                    a.push(Action::Plan(id));
                }
                if Self::current(s).contains(&id) {
                    a.push(Action::Remove(id));
                }
            }
            if s.uploading[id] {
                a.push(Action::FinishUpload(id));
            }
            if s.removed[id].is_some_and(|v| self.removal_low(s) >= v)
                && !Self::referenced(s, self.removal_low(s), id)
                && !s.deleting.contains(&id)
                && s.present[id]
                && (!self.has("serialized")
                    || (!s.ready[id] && !s.uploading[id] && s.plan.is_none_or(|(_, i)| i != id)))
            {
                a.push(Action::SelectRemoved(id));
            }
            if s.deleting.contains(&id) {
                a.push(Action::Delete(id));
            }
            if s.ready[id]
                && id / 2 == s.head.lease
                && !Self::referenced(s, Self::low(s), id)
                && s.plan.is_none_or(|(_, i)| i != id)
            {
                a.push(Action::DiscardFailed(id));
            }
        }
        if s.plan.is_some() {
            a.push(Action::Publish);
        }
        if s.head.lease == 0 {
            a.push(Action::Takeover);
        }
        if s.head.floor < s.head.pos {
            a.push(Action::Floor);
        }
        if s.pin.is_none() {
            a.push(Action::Pin);
        }
        if s.queries.len() == 1 && s.queries[0] < s.head.pos {
            a.push(Action::SecondQuery);
        }
        for generation in 0..=s.head.lease {
            if !s.swept[generation] && (generation < s.head.lease || self.has("own-sweep")) {
                a.push(Action::Sweep(generation));
            }
        }
        a.retain(|a| self.next_state(s, a.clone()).is_some());
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        match a {
            Action::StartUpload(id) => {
                n.started[id] = true;
                n.uploading[id] = true;
            }
            Action::FinishUpload(id) => {
                n.uploading[id] = false;
                n.present[id] = true;
                n.ready[id] = true;
                n.late_upload |= id / 2 < s.head.lease && s.swept[id / 2];
            }
            Action::Reuse(id) => {
                n.ready[id] = true;
                n.reused = true;
            }
            Action::Plan(id) => n.plan = Some((s.head, id)),
            Action::Publish => {
                let (base, id) = n.plan.take()?;
                if s.head.accepts(base) {
                    let mut live = Self::current(s).to_vec();
                    live.push(id);
                    live.sort_unstable();
                    n.head.pos += 1;
                    n.versions.push(live);
                    n.ready[id] = false;
                    n.removed[id] = None;
                }
            }
            Action::Remove(id) => {
                let mut live = Self::current(s).to_vec();
                live.retain(|i| *i != id);
                n.head.pos += 1;
                n.versions.push(live);
                n.removed[id] = Some(n.head.pos);
            }
            Action::Takeover => {
                n.head.lease += 1;
            }
            Action::Floor => n.head.floor = s.head.pos,
            Action::Pin => {
                n.pin = Some(s.head.pos);
                n.queries.push(s.head.pos);
            }
            Action::SecondQuery => n.queries.push(s.head.pos),
            Action::SelectRemoved(id) => {
                n.deleting.push(id);
                n.deleting.sort_unstable();
            }
            Action::Sweep(generation) => {
                n.swept[generation] = true;
                for id in generation * 2..generation * 2 + 2 {
                    if s.present[id]
                        && !Self::referenced(s, Self::low(s), id)
                        && !n.deleting.contains(&id)
                    {
                        n.deleting.push(id);
                    }
                }
                n.deleting.sort_unstable();
            }
            Action::Delete(id) => {
                n.present[id] = false;
                n.deleting.retain(|i| *i != id);
            }
            Action::DiscardFailed(id) => {
                n.ready[id] = false;
                n.present[id] = false;
            }
        }
        (n != *s).then_some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("I4 removal retains accepted objects", |_, s: &State| {
                s.versions[s.head.floor..]
                    .iter()
                    .flatten()
                    .all(|&id| s.present[id])
            }),
            Property::always("I7 removal retains active queries", |_, s: &State| {
                s.queries
                    .iter()
                    .all(|&v| s.versions[v].iter().all(|&id| s.present[id]))
            }),
            Property::sometimes("content addressed AlreadyExists reuse", |_, s: &State| {
                s.reused
            }),
            Property::sometimes(
                "fenced upload completes after predecessor sweep",
                |_, s: &State| s.late_upload,
            ),
            Property::sometimes("two active query versions", |_, s: &State| {
                s.queries.len() == 2
            }),
        ]
    }
}
