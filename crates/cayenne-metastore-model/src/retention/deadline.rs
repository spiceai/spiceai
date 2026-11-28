// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! A replica's scan deadline against the writer's reclamation of removed
//! objects.
//!
//! A replica query confirms the head with one GET and stamps the state it
//! reads with the time that GET was sent, `sent`, on the reader's own clock.
//! Before each read the query checks `now >= sent + T` and, once it holds,
//! stops with "query outlived its files" instead of reading. A read is two
//! steps: the query issues it, and the store serves it arbitrarily later
//! (only the horizon bounds it), finding the object present or already
//! deleted. A deleted object is a read error, and an input error that arrives
//! at or after the deadline is reported as "outlived" too, so a read in flight
//! across the deadline that meets a delete fails that way. The GET must be
//! answered within S of `sent`, and a query starts only from a confirmed head.
//! The store serves the head GET when it is sent, the earliest it can, so the
//! removal that follows may commit right after `sent`; its response arrives up
//! to S later. GC records when it first observes a removal and deletes the
//! object once `T + MARGIN` has passed since, on its own clock.
//!
//! Time is GC's clock. The two processes measure their intervals on their own
//! monotonic clocks, so only the difference between those measurements
//! matters, and GC's clock serves as the time axis without loss. The reader's
//! clock reads `offset` ticks ahead of GC's, with `offset` in `0..=SKEW`
//! changing at most once, so every interval the reader measures is within
//! SKEW ticks of the same interval on GC's clock. The reader's clock never
//! runs backwards: it gains on GC's by jumping ahead and loses by holding
//! while GC's ticks. `DRIFT`, the store-timestamp error of the other retention
//! graphs, compares no process clocks and does not apply.
//!
//! What a user can see is checked: no query returns rows from a state missing
//! an object it read, and a query fails only past `sent + T`, as outlived.
//! The margin is SKEW, the smallest for which that holds. The removal commits
//! no earlier than `sent`, since the head the GET returned still listed the
//! object, and GC observes it no earlier than that, so a delete lands no
//! earlier than `sent + T + SKEW` on GC's clock, when the reader's clock, slow
//! by at most SKEW, has reached `sent + T`: any read that meets the delete is
//! reported as outlived. However long a read stays in flight does not matter.
//! That every read issued before the deadline finds its object is a stronger
//! property, which no margin gives while a read may stay in flight unboundedly.
use stateright::{Model, Property};

const N: usize = 2;
/// Data-file retention: how long a query may read after its head was sent.
const T: u8 = super::T;
/// Head confirmation bound.
const S: u8 = super::S;
/// How far the reader's measure of an interval may differ from GC's.
const SKEW: u8 = 1;
/// GC's wait past the retention before it deletes a removed object.
const MARGIN: u8 = SKEW;
const HORIZON: u8 = 7;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Outcome {
    Rows,
    /// Stopped at the deadline: "query outlived its files".
    Outlived,
    /// A read found its object deleted before the deadline.
    Missing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Query {
    /// When the head GET was sent, on the reader's clock.
    sent: u8,
    /// When the query stops reading, on the reader's clock.
    deadline: u8,
    /// The objects the confirmed head lists.
    set: [bool; N],
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    /// GC's clock.
    now: u8,
    /// How far the reader's clock reads ahead of GC's.
    offset: u8,
    drifted: bool,
    live: [bool; N],
    present: [bool; N],
    /// When GC first observed each removal, on its clock.
    observed: [Option<u8>; N],
    removed: [bool; N],
    /// A delete issued and not yet applied.
    collecting: [bool; N],
    /// Removed while the query was running.
    removed_under_query: [bool; N],
    /// The head GET in flight: when it was sent, on the reader's clock, and
    /// the objects the head it read lists.
    confirming: Option<(u8, [bool; N])>,
    /// The confirmed head: when its GET was sent and answered, on the
    /// reader's clock, and the objects it lists.
    confirmed: Option<(u8, u8, [bool; N])>,
    query: Option<Query>,
    /// Each read in flight, with when it was issued on GC's clock.
    issued: [Option<u8>; N],
    done: [bool; N],
    result: Option<Outcome>,
    found_deleted: bool,
    failed_early: bool,
    bad_rows: bool,
    stopped: bool,
    collected_under_query: bool,
    read_after_other_delete: bool,
    read_past_deadline: bool,
    /// A read in flight across the deadline met a delete and was reported as
    /// outlived.
    late_miss_outlived: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Tick,
    /// The reader's clock moves to read this many ticks ahead of GC's: ahead
    /// at once, or behind by holding while GC's clock ticks.
    Drift(u8),
    Remove(usize),
    Observe(usize),
    Expire(usize),
    Collect(usize),
    Confirm,
    Respond,
    Start,
    Issue(usize),
    Serve(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
    fn margin(&self) -> u8 {
        if self.has("no-margin") { 0 } else { MARGIN }
    }
    /// Advances GC's clock, unless the horizon is reached. A read in flight
    /// does not hold it back: the store may serve it arbitrarily late.
    fn tick(s: &State, n: &mut State) -> bool {
        if s.now >= HORIZON {
            return false;
        }
        n.now += 1;
        true
    }
    fn reader_clock(s: &State) -> u8 {
        s.now + s.offset
    }
    fn running(s: &State) -> bool {
        s.query.is_some() && s.result.is_none()
    }
    /// A delete of `o` has been issued, whether or not it has landed.
    fn delete_issued(s: &State, o: usize) -> bool {
        s.collecting[o] || (s.removed[o] && !s.present[o])
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        (0..=SKEW)
            .map(|offset| State {
                now: 0,
                offset,
                drifted: false,
                live: [true; N],
                present: [true; N],
                observed: [None; N],
                removed: [false; N],
                collecting: [false; N],
                removed_under_query: [false; N],
                confirming: None,
                confirmed: None,
                query: None,
                issued: [None; N],
                done: [false; N],
                result: None,
                found_deleted: false,
                failed_early: false,
                bad_rows: false,
                stopped: false,
                collected_under_query: false,
                read_after_other_delete: false,
                read_past_deadline: false,
                late_miss_outlived: false,
            })
            .collect()
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        // The graph ends once the query has an outcome.
        if s.result.is_some() {
            return;
        }
        a.push(Action::Tick);
        if !s.drifted {
            a.extend((0..=SKEW).filter(|v| *v != s.offset).map(Action::Drift));
        }
        for o in 0..N {
            a.extend([
                Action::Remove(o),
                Action::Observe(o),
                Action::Expire(o),
                Action::Collect(o),
                Action::Issue(o),
                Action::Serve(o),
            ]);
        }
        a.extend([Action::Confirm, Action::Respond, Action::Start]);
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        let clock = Self::reader_clock(s);
        match action {
            Action::Tick => {
                if !Self::tick(s, &mut n) {
                    return None;
                }
            }
            Action::Drift(offset) => {
                // Falling behind takes a GC tick the reader's clock does not see.
                if offset < s.offset && (s.offset - offset > 1 || !Self::tick(s, &mut n)) {
                    return None;
                }
                n.offset = offset;
                n.drifted = true;
            }
            Action::Remove(o) => {
                if !s.live[o] {
                    return None;
                }
                n.live[o] = false;
                n.removed[o] = true;
                n.removed_under_query[o] = Self::running(s);
            }
            Action::Observe(o) => {
                if !s.removed[o] || s.observed[o].is_some() {
                    return None;
                }
                n.observed[o] = Some(s.now);
            }
            Action::Expire(o) => {
                let at = s.observed[o]?;
                if !s.present[o] || s.collecting[o] || s.now - at < T + self.margin() {
                    return None;
                }
                n.collecting[o] = true;
            }
            Action::Collect(o) => {
                if !s.collecting[o] {
                    return None;
                }
                n.collecting[o] = false;
                n.present[o] = false;
                n.collected_under_query |= s.removed_under_query[o] && Self::running(s);
            }
            Action::Confirm => {
                if s.confirming.is_some() || s.confirmed.is_some() || !s.live.contains(&true) {
                    return None;
                }
                n.confirming = Some((clock, s.live));
            }
            Action::Respond => {
                let (sent, listed) = s.confirming?;
                // A head not confirmed within S fails the query instead.
                if clock.saturating_sub(sent) > S {
                    return None;
                }
                n.confirming = None;
                n.confirmed = Some((sent, clock, listed));
            }
            Action::Start => {
                let (sent, answered, listed) = s.confirmed?;
                if s.query.is_some() || clock.saturating_sub(sent) > S || !listed.contains(&true) {
                    return None;
                }
                let from = if self.has("from-start") {
                    clock
                } else if self.has("response-time") {
                    answered
                } else {
                    sent
                };
                n.query = Some(Query {
                    sent,
                    deadline: from + T,
                    set: listed,
                });
            }
            Action::Issue(o) => {
                let q = s.query?;
                if !Self::running(s) || !q.set[o] || s.issued[o].is_some() || s.done[o] {
                    return None;
                }
                if !self.has("ignores-deadline") && clock >= q.deadline {
                    n.result = Some(Outcome::Outlived);
                    n.stopped = true;
                    n.failed_early |= clock < q.sent + T;
                    n.issued = [None; N];
                } else {
                    n.issued[o] = Some(s.now);
                }
            }
            Action::Serve(o) => {
                let q = s.query?;
                s.issued[o]?;
                n.issued[o] = None;
                if s.present[o] {
                    n.done[o] = true;
                    n.read_past_deadline |= clock >= q.deadline;
                    n.read_after_other_delete |=
                        (0..N).any(|p| p != o && Self::delete_issued(s, p));
                    if (0..N).all(|p| !q.set[p] || n.done[p]) {
                        n.result = Some(Outcome::Rows);
                    }
                } else if self.has("missing-as-empty") {
                    // The read returns no rows instead of an error.
                    n.found_deleted = true;
                    n.done[o] = true;
                    n.bad_rows = true;
                    if (0..N).all(|p| !q.set[p] || n.done[p]) {
                        n.result = Some(Outcome::Rows);
                    }
                } else {
                    n.found_deleted = true;
                    let outlived = !self.has("ignores-deadline") && clock >= q.deadline;
                    n.result = Some(if outlived {
                        Outcome::Outlived
                    } else {
                        Outcome::Missing
                    });
                    n.late_miss_outlived |= outlived;
                    n.failed_early |= clock < q.sent + T;
                    n.issued = [None; N];
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Tick => "GC's clock ticks".into(),
            Action::Drift(offset) => {
                format!("the reader's clock now reads {offset} ahead of GC's")
            }
            Action::Remove(o) => format!("a commit removes object {o} from the head"),
            Action::Observe(o) => format!("GC observes object {o}'s removal"),
            Action::Expire(o) => {
                format!("object {o}'s removal passes T plus the margin; GC issues its delete")
            }
            Action::Collect(o) => format!("the delete of object {o} lands"),
            Action::Confirm => "the reader sends a head GET".into(),
            Action::Respond => "the head GET responds".into(),
            Action::Start => "a query starts from the confirmed head".into(),
            Action::Issue(o) => {
                format!("the query checks its deadline and issues a read of object {o}")
            }
            Action::Serve(o) => format!("the store serves the read of object {o}"),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!(
                "retention T = {T}, staleness S = {S}, GC margin {}, reader clock skew {SKEW}",
                self.margin()
            ),
            format!(
                "GC clock {}; reader clock {} ({} ahead)",
                s.now,
                Self::reader_clock(&s),
                s.offset
            ),
            match s.query {
                Some(q) => format!(
                    "query: head GET sent at {}, deadline {} (reader clock)",
                    q.sent, q.deadline
                ),
                None => "no query running".into(),
            },
            crate::explorer::flags(&[
                ("query stopped at its deadline", s.stopped),
                (
                    "removed object collected under a running query",
                    s.collected_under_query,
                ),
                (
                    "read served after another object's delete",
                    s.read_after_other_delete,
                ),
                ("read served past the deadline", s.read_past_deadline),
                (
                    "read across the deadline met a delete: outlived",
                    s.late_miss_outlived,
                ),
                ("read found a deleted object", s.found_deleted),
                ("BAD query failed before its deadline", s.failed_early),
                (
                    "BAD missing-object error reported",
                    s.result == Some(Outcome::Missing),
                ),
                ("BAD rows", s.bad_rows),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let last = path.last_state().clone();
        let mut boxes = vec![(
            "HEAD".to_string(),
            vec![format!(
                "live: {}",
                (0..N)
                    .filter(|o| last.live[*o])
                    .map(|o| format!("object {o}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )],
            Tone::Live,
        )];
        for o in 0..N {
            let mut lines = vec![if last.present[o] { "stored" } else { "deleted" }.to_string()];
            if let Some(at) = last.observed[o] {
                lines.push(format!(
                    "removal observed at {at}; deletable at {}",
                    at + T + self.margin()
                ));
            }
            if last.collecting[o] {
                lines.push("delete issued, not landed".into());
            }
            if let Some(at) = last.issued[o] {
                lines.push(format!("read issued at {at}, not served"));
            }
            let tone = if !last.present[o] {
                Tone::Gone
            } else if last.live[o] {
                Tone::Live
            } else {
                Tone::Staged
            };
            boxes.push((format!("OBJECT {o}"), lines, tone));
        }
        if let Some(q) = last.query {
            let (outcome, tone) = match last.result {
                None => ("running", Tone::Actor),
                Some(Outcome::Rows) => ("rows", Tone::Actor),
                Some(Outcome::Outlived) if last.failed_early => {
                    ("FAILED before its deadline", Tone::Bad)
                }
                Some(Outcome::Outlived) => ("outlived its files (allowed)", Tone::Gone),
                Some(Outcome::Missing) => ("FAILED: read a deleted object", Tone::Bad),
            };
            boxes.push((
                format!("QUERY (GET sent {}, deadline {})", q.sent, q.deadline),
                vec![outcome.into()],
                tone,
            ));
        }
        let (gc, head, store, reader) = (0, 1, 2, 3);
        let lanes = ["GC", "head", "object store", "reader"]
            .map(String::from)
            .to_vec();
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let label = self.format_action(&action);
            let event = match action {
                Action::Tick => Event::on(gc, format!("clock -> {}", after.now)).tone(Tone::Gone),
                Action::Drift(_) => Event::on(reader, label).tone(Tone::Gone),
                Action::Remove(_) => Event::on(head, label),
                Action::Observe(_) => Event::on(gc, label).to(head),
                Action::Expire(_) => Event::on(gc, label).to(store).tone(Tone::Staged),
                Action::Collect(_) => Event::on(store, label).tone(Tone::Gone),
                Action::Confirm => Event::on(reader, label).to(head).tone(Tone::Staged),
                Action::Respond => Event::on(head, label).to(reader),
                Action::Start => Event::on(reader, label).tone(Tone::Actor),
                Action::Issue(o) if after.stopped && !before.stopped => Event::on(
                    reader,
                    format!("deadline passed: query stops before reading object {o}"),
                )
                .tone(Tone::Gone),
                Action::Issue(_) => Event::on(reader, label).to(store).tone(Tone::Staged),
                Action::Serve(o) => {
                    if before.present[o] {
                        Event::on(store, format!("object {o} served")).to(reader)
                    } else {
                        Event::on(store, format!("object {o} is missing"))
                            .to(reader)
                            .tone(Tone::Bad)
                    }
                }
            };
            events.push(event.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always(
                "a query fails only past confirm_sent + T, as outlived",
                |_, s: &State| !s.failed_early && s.result != Some(Outcome::Missing),
            ),
            Property::always("rows or an error", |_, s: &State| {
                s.result != Some(Outcome::Rows) || !s.bad_rows
            }),
            Property::sometimes("a query stops at its deadline", |_, s: &State| s.stopped),
            Property::sometimes(
                "a removed object is collected while a query that started before its removal runs",
                |_, s: &State| s.collected_under_query,
            ),
            Property::sometimes(
                "a read is served after a delete of another object was issued",
                |_, s: &State| s.read_after_other_delete,
            ),
            Property::sometimes(
                "a read issued before the deadline is served after it",
                |_, s: &State| s.read_past_deadline,
            ),
            Property::sometimes(
                "a read in flight across the deadline meets a delete and fails as outlived",
                |_, s: &State| s.late_miss_outlived,
            ),
        ];
        if self.has("all-present") {
            properties.push(Property::always(
                "a replica read never finds a deleted object",
                |_, s: &State| !s.found_deleted,
            ));
        }
        properties
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, Outcome, Protocol};
    use stateright::Model;

    /// The trace up to a read of object 0 issued one tick before the
    /// deadline on a reader clock running slow, with its removal observed at
    /// GC time 0: the GET was sent at reader time 1, the deadline is 4.
    fn issued_before_the_deadline(model: &Protocol) -> super::State {
        let mut s = model.init_states().remove(1);
        let steps = [
            Action::Confirm,
            Action::Respond,
            Action::Start,
            Action::Remove(0),
            Action::Observe(0),
            Action::Drift(0),
            Action::Tick,
            Action::Tick,
            Action::Issue(0),
        ];
        for a in steps {
            s = model
                .next_state(&s, a.clone())
                .unwrap_or_else(|| panic!("{a:?} is enabled"));
        }
        s
    }

    fn after(model: &Protocol, mut s: super::State, steps: &[Action]) -> super::State {
        for a in steps {
            s = model
                .next_state(&s, a.clone())
                .unwrap_or_else(|| panic!("{a:?} is enabled"));
        }
        s
    }

    /// With the margin, the delete lands only once the reader's clock has
    /// reached the deadline, so the read that meets it fails as outlived;
    /// without it, the delete lands first and the failure is a missing object
    /// before the deadline.
    #[test]
    fn a_read_meeting_a_delete_fails_as_outlived_only_with_the_margin() {
        let model = Protocol::new("retention-deadline");
        let s = issued_before_the_deadline(&model);
        assert_eq!(model.next_state(&s, Action::Expire(0)), None);
        let s = after(
            &model,
            s,
            &[
                Action::Tick,
                Action::Expire(0),
                Action::Collect(0),
                Action::Serve(0),
            ],
        );
        assert_eq!(s.result, Some(Outcome::Outlived));
        assert!(s.found_deleted && !s.failed_early && s.late_miss_outlived);

        let model = Protocol::new("retention-deadline-no-margin");
        let s = issued_before_the_deadline(&model);
        let s = after(
            &model,
            s,
            &[Action::Expire(0), Action::Collect(0), Action::Serve(0)],
        );
        assert_eq!(s.result, Some(Outcome::Missing));
        assert!(s.failed_early);
    }
}
