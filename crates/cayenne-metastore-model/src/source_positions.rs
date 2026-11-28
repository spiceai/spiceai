// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Numbered append source, durable offsets, ambiguous delivery and replacement owners.
//!
//! The `idle` modes add source positions with nothing to apply for the
//! dataset, as a shared replication slot carries WAL for tables the dataset
//! does not publish. The writer credits its applied floor past such a position
//! at once, but records it in the head only on its next record pass; the
//! source acknowledgement and each replication connection's start position
//! must stay at or below the recorded position.
//!
//! Reconnect is modeled only with no attempt in flight and clears the buffer;
//! reconnecting during an outstanding commit is not modeled. Position 0 is an
//! established durable prefix (the start of the source), not the implementation's
//! "no recorded position" sentinel (`AckSlot::recorded() == 0` in
//! `crates/data_components/src/postgres_replication/shared.rs`), for which the
//! code holds the slot at the committed floor.
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
    /// Idle modes: the position through which every change is applied or has
    /// nothing to apply (the pump's applied floor); volatile.
    applied: usize,
    /// Idle modes: the highest source position ever delivered, so a
    /// redelivered position keeps its kind.
    high: usize,
    /// Idle modes: the source positions that carry nothing for the dataset.
    idle: Vec<usize>,
    /// An idle position recorded by `Record`, with no subsequent data commit.
    recorded_idle: Option<usize>,
    /// An acknowledgement reached that recorded idle position before a data commit.
    idle_acknowledged: bool,
    /// Recorded position at reconnect, when the start was below it or the applied floor.
    reconnect_position: Option<usize>,
    /// A change redelivered by that reconnect was skipped rather than applied.
    reconnect_skipped: bool,
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
    /// The source delivers a position with nothing to apply.
    Idle,
    /// The record pass commits the applied floor as the source position.
    Record,
    /// The pump acknowledges the source at the writer's acknowledgement position.
    AcknowledgeFloor,
    /// The replication connection reconnects; its acknowledgement starts at
    /// the connection's start position.
    Reconnect,
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
    /// The position the source may be acknowledged through: the applied floor
    /// capped at the recorded position, or (in the control) the applied floor.
    fn acknowledgeable(&self, s: &State) -> usize {
        if self.has("ack-applied") {
            s.applied
        } else {
            s.applied.min(s.position)
        }
    }
    /// The position a new replication connection starts at: the acknowledged
    /// position, or (in the control) the applied floor.
    fn connection_start(&self, s: &State) -> usize {
        if self.has("reconnect-applied") {
            s.applied
        } else {
            s.acknowledged
        }
    }
}
/// The data positions through `upto`, in order.
fn data_through(s: &State, upto: usize) -> Vec<usize> {
    (1..=upto).filter(|p| !s.idle.contains(p)).collect()
}
/// The first `n` data positions, in order.
fn first_data(s: &State, n: usize) -> Vec<usize> {
    (1..).filter(|p| !s.idle.contains(p)).take(n).collect()
}
/// Whether the source position `p` carries a change for the dataset. A new
/// position may carry either; a redelivered one keeps its kind.
fn may_be(s: &State, p: usize, idle: bool) -> bool {
    p > s.high || s.idle.contains(&p) == idle
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
            && may_be(s, s.cursor, false)
        {
            a.push(Action::Receive);
        }
        if self.has("idle") && !s.needs_lease && !s.half && s.active.is_none() {
            // Nothing is pending, so the applied floor may pass an idle position.
            if s.buffer.is_empty() && s.cursor <= self.bound && may_be(s, s.cursor, true) {
                a.push(Action::Idle);
            }
            if s.buffer.is_empty() && s.applied > s.position {
                a.push(Action::Record);
            }
            if self.acknowledgeable(s) > s.acknowledged {
                a.push(Action::AcknowledgeFloor);
            }
            a.push(Action::Reconnect);
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
                if self.has("idle") {
                    next.high = state.high.max(state.cursor);
                }
                if state.cursor <= state.position && !self.has("no-redelivery-skip") {
                    next.skipped = true;
                    next.reconnect_skipped |= state
                        .reconnect_position
                        .is_some_and(|position| state.cursor <= position);
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
                    if !r.rows.is_empty() {
                        next.recorded_idle = None;
                    }
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
                    if self.has("idle") {
                        // The commit recorded its rows and position together.
                        next.applied = state.applied.max(state.position);
                    }
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
            Action::Idle => {
                next.cursor += 1;
                if state.cursor > state.high {
                    next.high = state.cursor;
                    next.idle.push(state.cursor);
                }
                // The pump credits the applied floor to the delivered position.
                next.applied = state.applied.max(state.cursor);
            }
            Action::Record => {
                // The writer is the only owner here, so its record CAS lands.
                next.head.pos += 1;
                next.position = state.applied;
                next.recorded_idle = state
                    .idle
                    .iter()
                    .copied()
                    .find(|p| *p > state.position && *p > state.acknowledged && *p <= state.applied)
                    .or(state.recorded_idle);
            }
            Action::AcknowledgeFloor => {
                let through = self.acknowledgeable(state);
                next.idle_acknowledged |= through <= state.position
                    && state
                        .recorded_idle
                        .is_some_and(|p| p > state.acknowledged && p <= through);
                next.acknowledged = state.acknowledged.max(through);
            }
            Action::Reconnect => {
                // The client seeds its acknowledgement from the start position
                // and never lowers it; the source redelivers after it, and
                // changes the writer already applied are skipped again.
                let start = self.connection_start(state);
                next.reconnect_position =
                    (start < state.position || start < state.applied).then_some(state.position);
                next.acknowledged = state.acknowledged.max(start);
                next.cursor = start + 1;
                next.buffer.clear();
            }
            Action::Crash | Action::Takeover => {
                next.reconnect_position = None;
                if self.has("idle") {
                    // The applied floor is volatile; a restart resumes from the head.
                    next.applied = state.position;
                }
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
            Action::Issue if self.has("position-before-data") => {
                "the writer sends its source position, then its buffer, as two commits".into()
            }
            Action::Issue if self.has("position-after-data") => {
                "the writer sends its buffer, then its source position, as two commits".into()
            }
            Action::Issue => {
                "the writer sends one commit of its buffer and its source position".into()
            }
            Action::Deliver(i, 0) => format!("attempt {i} arrives"),
            Action::Deliver(i, _) => format!("the resent attempt {i} arrives"),
            Action::Resolve => "the writer reads the head to resolve its attempt".into(),
            Action::Unknown => "the response is lost (outcome unknown)".into(),
            Action::Acknowledge => "the writer acknowledges the source".into(),
            Action::Crash => "the writer crashes".into(),
            Action::Takeover => "another writer takes over".into(),
            Action::Acquire => "the restarted writer reacquires the lease".into(),
            Action::Idle => {
                "the source delivers a position with nothing to apply (the applied floor moves)"
                    .into()
            }
            Action::Record => "the writer records its applied floor in the head".into(),
            Action::AcknowledgeFloor if self.has("ack-applied") => {
                "the pump acknowledges the source at the applied floor".into()
            }
            Action::AcknowledgeFloor => {
                "the pump acknowledges the source at the applied floor, capped at the recorded position"
                    .into()
            }
            Action::Reconnect if self.has("reconnect-applied") => {
                "the replication connection reconnects at the applied floor".into()
            }
            Action::Reconnect => {
                "the replication connection reconnects at the acknowledged position".into()
            }
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
            format!(
                "applied floor {}, recorded position {}, idle positions {:?}",
                s.applied, s.position, s.idle
            ),
            crate::explorer::flags(&[
                ("redelivery skipped", s.skipped),
                ("outcome unknown", s.unknown),
                ("crashed", s.crashed),
                ("half-acknowledged", s.half),
                ("needs lease", s.needs_lease),
                ("recorded idle position acknowledged", s.idle_acknowledged),
                ("reconnect replay skipped", s.reconnect_skipped),
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
                    format!("applied floor {}", last.applied),
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
            let event = match action {
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
                Action::Idle => Event::on(
                    source,
                    format!("position {} has nothing to apply", before.cursor),
                )
                .to(writer)
                .tone(Tone::Gone),
                Action::Record => Event::on(writer, format!("record position {}", after.position))
                    .to(head)
                    .tone(Tone::Live),
                Action::AcknowledgeFloor => Event::on(
                    writer,
                    format!("acknowledge floor through {}", after.acknowledged),
                )
                .to(source),
                Action::Reconnect => {
                    Event::on(writer, format!("reconnect at {}", after.cursor - 1))
                        .to(source)
                        .tone(Tone::Actor)
                }
            };
            events.push(event.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut p = vec![
            Property::always("at most one live row per source key", |_, s: &State| {
                s.rows
                    .iter()
                    .enumerate()
                    .all(|(i, key)| !s.rows[..i].contains(key))
            }),
            // Without idle positions every position carries a change, so
            // `data_through(s, p)` is `1..=p` and `first_data(s, n)` is `1..=n`.
            Property::always("I10 source append prefix exactly once", |_, s: &State| {
                s.rows == first_data(s, s.rows.len())
            }),
            Property::always("I10 logged source position has its rows", |_, s: &State| {
                let logged = data_through(s, s.position);
                logged.len() <= s.rows.len() && s.rows[..logged.len()] == logged
            }),
            Property::always(
                "I10 complete source state matches logged prefix",
                |_, s: &State| s.rows == data_through(s, s.position),
            ),
            Property::always("I2 source acknowledgement covered", |_, s: &State| {
                s.acknowledged <= s.position
                    && data_through(s, s.acknowledged).len() <= s.rows.len()
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
        ];
        if self.has("idle") {
            p.extend([
                Property::sometimes("recorded idle position acknowledged", |_, s: &State| {
                    s.idle_acknowledged
                }),
                Property::sometimes(
                    "idle position recorded by the record step, then acknowledged",
                    |_, s: &State| s.idle_acknowledged,
                ),
                Property::sometimes(
                    "reconnect below the recorded position replays and skips",
                    |_, s: &State| s.reconnect_skipped,
                ),
            ]);
        }
        p
    }
}

#[cfg(test)]
mod explorer_tests {
    use super::{Action, Protocol};
    use stateright::Model;

    /// The commit label says what each mode publishes: data and position in
    /// one commit, or (in the split controls) one of them per commit.
    #[test]
    fn commit_label_follows_the_mode() {
        assert_eq!(
            Protocol::new("source-positions", 2).format_action(&Action::Issue),
            "the writer sends one commit of its buffer and its source position"
        );
        assert_eq!(
            Protocol::new("source-position-before-data", 2).format_action(&Action::Issue),
            "the writer sends its source position, then its buffer, as two commits"
        );
        assert_eq!(
            Protocol::new("source-position-after-data", 2).format_action(&Action::Issue),
            "the writer sends its buffer, then its source position, as two commits"
        );
    }
}
