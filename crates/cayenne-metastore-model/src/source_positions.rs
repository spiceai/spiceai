// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Numbered append source, durable offsets, ambiguous delivery and replacement owners.
use crate::core::Head;
use stateright::{Model, Property};
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Attempt {
    base: Head,
    rows: Vec<usize>,
    position: usize,
    delivered: u8,
    retry: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    head: Head,
    rows: Vec<usize>,
    position: usize,
    acknowledged: usize,
    buffer: Vec<usize>,
    cursor: usize,
    attempts: Vec<Attempt>,
    active: Option<usize>,
    accepted: Vec<usize>,
    answer: u8,
    crashed: bool,
    skipped: bool,
    unknown: bool,
    half: bool,
    needs_lease: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Receive,
    Issue,
    Deliver(usize, usize),
    Resolve,
    Unknown,
    Acknowledge,
    Crash,
    Takeover,
    Acquire,
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound,
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            cursor: 1,
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if !s.needs_lease
            && !s.half
            && s.active.is_none()
            && s.cursor <= self.bound
            && s.buffer.len() < 2
        {
            a.push(Action::Receive);
        }
        if !s.needs_lease && s.active.is_none() && !s.buffer.is_empty() && s.attempts.len() < 4 {
            a.push(Action::Issue);
        }
        for (i, r) in s.attempts.iter().enumerate() {
            for q in 0..2 {
                if r.delivered & (1 << q) == 0 && (q == 0 || r.retry) {
                    a.push(Action::Deliver(i, q));
                }
            }
        }
        if s.active.is_some() {
            if s.answer == 0 {
                a.extend([Action::Resolve, Action::Unknown]);
            }
            if s.answer == 1 || self.has("ack-before-acceptance") {
                a.push(Action::Acknowledge);
            }
        }
        if s.needs_lease {
            a.push(Action::Acquire);
        }
        if !s.crashed {
            a.push(Action::Crash);
        }
        if s.head.lease == 0 {
            a.push(Action::Takeover);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Receive => {
                next.cursor += 1;
                if state.cursor <= state.position && !self.has("no-redelivery-skip") {
                    next.skipped = true;
                } else {
                    next.buffer.push(state.cursor);
                }
            }
            Action::Issue => {
                if !self.has("no-redelivery-skip") {
                    next.buffer.retain(|p| *p > state.position);
                    next.skipped |= next.buffer.len() != state.buffer.len();
                    if next.buffer.is_empty() {
                        return Some(next);
                    }
                }
                let end = *next.buffer.last()?;
                let (rows, position) = if self.has("position-before-data") && !state.half {
                    (vec![], end)
                } else if self.has("position-after-data") && !state.half {
                    (next.buffer.clone(), state.position)
                } else if self.has("position-after-data") {
                    (vec![], end)
                } else {
                    (next.buffer.clone(), end)
                };
                next.active = Some(state.attempts.len());
                next.answer = 0;
                next.attempts.push(Attempt {
                    base: state.head,
                    rows,
                    position,
                    delivered: 0,
                    retry: false,
                });
            }
            Action::Deliver(i, q) => {
                next.attempts[i].delivered |= 1 << q;
                let r = &state.attempts[i];
                if state.head.accepts(r.base) {
                    next.head.pos += 1;
                    next.rows.extend(&r.rows);
                    next.position = r.position;
                    next.accepted.push(i);
                }
            }
            Action::Resolve => {
                let i = state.active?;
                let r = &state.attempts[i];
                next.answer = crate::core::resolve_attempt(
                    if state.accepted.contains(&i) {
                        Some(true)
                    } else if !state.head.accepts(r.base) {
                        Some(false)
                    } else {
                        None
                    },
                    state.head.accepts(r.base),
                );
                if next.answer == 0 {
                    next.attempts[i].retry = true;
                }
                if next.answer == 2 {
                    next.active = None;
                    next.answer = 0;
                }
            }
            Action::Unknown => next.unknown = true,
            Action::Acknowledge => {
                let split = self.has("position-before-data") || self.has("position-after-data");
                if split && !state.half {
                    next.half = true;
                } else {
                    next.acknowledged = next.acknowledged.max(*state.buffer.last()?);
                    next.buffer.clear();
                    next.half = false;
                }
                next.active = None;
                next.answer = 0;
            }
            Action::Acquire => {
                next.head.lease += 1;
                next.needs_lease = false;
            }
            Action::Crash | Action::Takeover => {
                next.buffer.clear();
                next.half = false;
                next.active = None;
                next.answer = 0;
                next.cursor = state.acknowledged + 1;
                if matches!(action, Action::Crash) {
                    next.crashed = true;
                    next.needs_lease = true;
                } else {
                    next.head.lease += 1;
                    next.needs_lease = false;
                }
            }
        }
        (next != *state).then_some(next)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Receive => "the writer receives the next source change".into(),
            Action::Issue => "the writer sends a commit of its buffer and source position".into(),
            Action::Deliver(i, 0) => format!("attempt {i} arrives"),
            Action::Deliver(i, _) => format!("the resent attempt {i} arrives"),
            Action::Resolve => "the writer reads the head to resolve its attempt".into(),
            Action::Unknown => "the response is lost (outcome unknown)".into(),
            Action::Acknowledge => "the writer acknowledges the source".into(),
            Action::Crash => "the writer crashes".into(),
            Action::Takeover => "another writer takes over".into(),
            Action::Acquire => "the restarted writer reacquires the lease".into(),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!(
                "source: next change {}, acknowledged through {}",
                s.cursor, s.acknowledged
            ),
            format!("buffered: {:?}", s.buffer),
            crate::explorer::flags(&[
                ("redelivery skipped", s.skipped),
                ("outcome unknown", s.unknown),
                ("crashed", s.crashed),
                ("half-acknowledged", s.half),
                ("needs lease", s.needs_lease),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let last = path.last_state().clone();
        let mut boxes = vec![
            (
                "SOURCE".into(),
                vec![
                    format!("next change to deliver: {}", last.cursor),
                    format!("acknowledged through {}", last.acknowledged),
                ],
                Tone::Actor,
            ),
            (
                "WRITER".into(),
                vec![
                    format!("buffered changes {:?}", last.buffer),
                    format!("active attempt {:?}", last.active),
                ],
                if last.needs_lease {
                    Tone::Gone
                } else {
                    Tone::Actor
                },
            ),
            (
                format!("HEAD (commit {}, lease {})", last.head.pos, last.head.lease),
                vec![
                    format!("rows {:?}", last.rows),
                    format!("source position {}", last.position),
                ],
                Tone::Live,
            ),
        ];
        for (i, a) in last.attempts.iter().enumerate() {
            let accepted = last.accepted.contains(&i);
            boxes.push((
                format!("ATTEMPT {i}"),
                vec![
                    format!("rows {:?} position {}", a.rows, a.position),
                    format!("on (commit {}, lease {})", a.base.pos, a.base.lease),
                    format!(
                        "landed copies {:02b}{}",
                        a.delivered,
                        if accepted { "; accepted" } else { "" }
                    ),
                ],
                if accepted { Tone::Live } else { Tone::Staged },
            ));
        }
        let (source, writer, head) = (0, 1, 2);
        let lanes = ["source", "writer", "head"].map(String::from).to_vec();
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            events.push(match action {
                Action::Receive => {
                    let label = if after.buffer.len() > before.buffer.len() {
                        format!("deliver change {}", before.cursor)
                    } else {
                        format!(
                            "redeliver change {} (skipped: already positioned)",
                            before.cursor
                        )
                    };
                    Event::on(source, label).to(writer)
                }
                Action::Issue => Event::on(writer, "send commit").to(head).tone(Tone::Staged),
                Action::Deliver(i, q) => {
                    let ok = after.head.pos > before.head.pos;
                    let copy = if q == 0 { "attempt" } else { "resent attempt" };
                    Event::on(
                        writer,
                        if ok {
                            format!("{copy} {i} lands (position {})", after.position)
                        } else {
                            format!("{copy} {i} refused")
                        },
                    )
                    .to(head)
                    .tone(if ok { Tone::Live } else { Tone::Gone })
                }
                Action::Resolve => Event::on(writer, "read head to resolve")
                    .to(head)
                    .tone(Tone::Actor),
                Action::Unknown => Event::on(writer, "response lost").tone(Tone::Bad),
                Action::Acknowledge => Event::on(
                    writer,
                    format!("acknowledge through {}", after.acknowledged),
                )
                .to(source),
                Action::Crash => Event::on(writer, "CRASH").tone(Tone::Bad),
                Action::Takeover => Event::on(writer, "takeover (new lease)").to(head),
                Action::Acquire => Event::on(writer, "reacquire lease").to(head),
            });
        }
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("at most one live row per source key", |_, s: &State| {
                s.rows
                    .iter()
                    .enumerate()
                    .all(|(i, key)| !s.rows[..i].contains(key))
            }),
            Property::always("I10 source append prefix exactly once", |_, s: &State| {
                s.rows == (1..=s.rows.len()).collect::<Vec<_>>()
            }),
            Property::always("I10 logged source position has its rows", |_, s: &State| {
                s.position <= s.rows.len()
                    && s.rows[..s.position] == (1..=s.position).collect::<Vec<_>>()
            }),
            Property::always(
                "I10 complete source state matches logged prefix",
                |_, s: &State| s.rows == (1..=s.position).collect::<Vec<_>>(),
            ),
            Property::always("I2 source acknowledgement covered", |_, s: &State| {
                s.acknowledged <= s.position && s.acknowledged <= s.rows.len()
            }),
            Property::sometimes(
                "source redelivery skipped after turnover",
                |_, s: &State| s.skipped && s.head.lease > 0,
            ),
            Property::sometimes("source ambiguous accepted outcome", |_, s: &State| {
                s.unknown && s.active.is_some_and(|i| s.accepted.contains(&i))
            }),
            Property::sometimes("source grouped two changes", |_, s: &State| {
                s.accepted.iter().any(|&i| s.attempts[i].rows.len() == 2)
            }),
        ]
    }
}
