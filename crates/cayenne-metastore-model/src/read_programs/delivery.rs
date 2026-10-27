// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Immutable conditional requests, lost responses and retained acceptance evidence.
use super::{Action as LocalAction, Protocol as Local, State as LocalState, Writer};
use stateright::{Model, Property};

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    local: Local,
    mode: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Request {
    writer: usize,
    payload: Writer,
    sent: u8,
    done: u8,
    answer: u8,
    lost_accepted_response: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct State {
    inner: LocalState,
    requests: Vec<Request>,
    active: Vec<Option<usize>>,
    // Attempt identities are part of the authoritative accepted head records.
    accepted: Vec<usize>,
    unknown_used: bool,
    duplicate_delivered: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Local(LocalAction),
    Issue(usize),
    Deliver(usize, u8),
    Check(usize),
    RetryIdentical(usize),
    Unknown(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            local: Local::new(mode, bound),
            mode: mode.into(),
        }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        self.local
            .init_states()
            .into_iter()
            .map(|inner| State {
                active: vec![None; inner.writers.len()],
                inner,
                requests: vec![],
                accepted: vec![],
                unknown_used: false,
                duplicate_delivered: false,
            })
            .collect()
    }
    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        let mut local = vec![];
        self.local.actions(&s.inner, &mut local);
        for action in local {
            actions.push(match action {
                LocalAction::Cas(writer) => Action::Issue(writer),
                action => Action::Local(action),
            });
        }
        for (index, request) in s.requests.iter().enumerate() {
            for copy in 0..2 {
                if request.sent & (1 << copy) != 0 && request.done & (1 << copy) == 0 {
                    actions.push(Action::Deliver(index, copy));
                }
            }
            if request.sent == 1 {
                actions.push(Action::RetryIdentical(index));
            }
            if request.answer == 0 && s.active[request.writer] == Some(index) {
                actions.push(Action::Check(index));
                if !s.unknown_used {
                    actions.push(Action::Unknown(index));
                }
            }
        }
        actions.retain(|a| self.next_state(s, a.clone()).is_some());
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        match action {
            Action::Local(action) => n.inner = self.local.next_state(&s.inner, action)?,
            Action::Issue(writer) => {
                n.active[writer] = Some(s.requests.len());
                n.requests.push(Request {
                    writer,
                    payload: s.inner.writers[writer].clone(),
                    sent: 1,
                    done: 0,
                    answer: 0,
                    lost_accepted_response: false,
                });
                n.inner.writers[writer].phase = 5;
            }
            Action::Deliver(index, copy) => {
                let request = &s.requests[index];
                n.requests[index].done |= 1 << copy;
                if request.payload.base == s.inner.log.len() || self.has("no-exact-base") {
                    let mut delivery = s.inner.clone();
                    delivery.writers[request.writer] = request.payload.clone();
                    if self.has("no-exact-base") {
                        delivery.writers[request.writer].base = s.inner.log.len();
                    }
                    n.inner = self
                        .local
                        .next_state(&delivery, LocalAction::Cas(request.writer))?;
                    // Delivery changes durable state, not the caller's knowledge or current plan.
                    n.inner.writers.clone_from(&s.inner.writers);
                    n.accepted.push(index);
                }
                n.duplicate_delivered |= copy == 1
                    && s.accepted.contains(&index)
                    && request
                        .payload
                        .plan
                        .as_ref()
                        .is_some_and(|p| !p.actions.is_empty());
            }
            Action::RetryIdentical(index) => n.requests[index].sent |= 2,
            Action::Check(index) => {
                let r = &s.requests[index];
                let evidence = s.accepted.get(r.payload.base).map(|id| *id == index);
                let answer =
                    crate::core::resolve_attempt(evidence, r.payload.base == s.inner.log.len());
                if answer == 0 {
                    return None;
                }
                n.requests[index].answer = answer;
                n.active[r.writer] = None;
                n.inner.writers[r.writer].phase = if answer == 1 { 2 } else { 1 };
            }
            Action::Unknown(index) => {
                let r = &s.requests[index];
                n.unknown_used = true;
                n.requests[index].lost_accepted_response = s.accepted.contains(&index);
                if self.has("no-resolve-before-replan") {
                    n.requests[index].answer = 2;
                    n.active[r.writer] = None;
                    let w = &mut n.inner.writers[r.writer];
                    w.phase = 0;
                    w.plan = None;
                    w.stamped.clear();
                } else if self.has("no-acceptance-evidence") {
                    n.requests[index].answer = 1;
                    n.active[r.writer] = None;
                    n.inner.writers[r.writer].phase = 2;
                }
            }
        }
        (n != *s).then_some(n)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("commit-order program serializability", |_, s: &State| {
                let mut oracle = Local::initial();
                let mut seen = Vec::new();
                for (index, record) in s.inner.log.iter().enumerate() {
                    let identity = (record.writer, record.statement);
                    if !seen.contains(&identity) {
                        let Some(after) = super::execute(&record.program, &oracle) else {
                            return false;
                        };
                        oracle = after;
                        seen.push(identity);
                    }
                    if oracle != s.inner.versions[index + 1] {
                        return false;
                    }
                }
                true
            }),
            Property::always("I9 keyed actions follow commit order", |m, s: &State| {
                m.local
                    .properties()
                    .iter()
                    .filter(|p| p.name == "I9 keyed actions follow commit order")
                    .all(|p| (p.condition)(&m.local, &s.inner))
            }),
            Property::always("at most one live row per table key", |m, s: &State| {
                m.local
                    .properties()
                    .iter()
                    .filter(|p| p.name == "at most one live row per table key")
                    .all(|p| (p.condition)(&m.local, &s.inner))
            }),
            Property::always("I5 resolution sound", |_, s: &State| {
                s.requests.iter().enumerate().all(|(i, r)| match r.answer {
                    1 => s.accepted.contains(&i),
                    2 => !s.accepted.contains(&i),
                    _ => true,
                })
            }),
            Property::always(
                "I2 acknowledged program accepted exactly once",
                |_, s: &State| {
                    s.requests.iter().filter(|r| r.answer == 1).all(|r| {
                        s.inner
                            .log
                            .iter()
                            .filter(|record| {
                                record.writer == r.writer
                                    && record.statement == r.payload.statements
                            })
                            .count()
                            == 1
                    })
                },
            ),
            Property::sometimes("lost response resolved by head read", |_, s: &State| {
                s.requests.iter().any(|r| {
                    r.lost_accepted_response
                        && r.answer == 1
                        && r.payload
                            .plan
                            .as_ref()
                            .is_some_and(|p| !p.actions.is_empty())
                })
            }),
            Property::sometimes("delayed duplicate of accepted request", |_, s: &State| {
                s.duplicate_delivered
            }),
            Property::sometimes("unknown relative update applied twice", |_, s: &State| {
                s.inner.log.iter().enumerate().any(|(i, r)| {
                    !r.plan.actions.is_empty()
                        && matches!(r.program, super::Program::Update { .. })
                        && s.inner.log[..i].iter().any(|p| {
                            p.writer == r.writer
                                && p.statement == r.statement
                                && !p.plan.actions.is_empty()
                        })
                })
            }),
        ]
    }
}
