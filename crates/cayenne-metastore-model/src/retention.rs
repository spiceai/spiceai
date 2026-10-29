// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Time-bounded retention: readers never write, and GC needs no generations.
//!
//! Removed objects are deleted a fixed retention `T` after GC observes their
//! removal. Uploads that never committed are deleted once they are `T_O` old by
//! the store's clock and the head has moved since GC selected them. A writer
//! references only objects live in its CAS base or its own uploads younger than
//! `T_O - M` by its own clock, which may run up to `DRIFT` slow. Every object
//! has a fresh name. Time is one discrete clock; the writer's measurement error
//! is the only skew modelled.
use stateright::{Model, Property};

const N: usize = 2;
/// Reader retention.
const T: u8 = 3;
/// Reader staleness bound: a query starts from a state confirmed this recently.
const S: u8 = 1;
/// Age at which an unreferenced upload may be collected.
const T_O: u8 = 4;
/// Writer clock error.
const DRIFT: u8 = 1;
const HORIZON: u8 = 9;
const VERSIONS: u8 = 6;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    now: u8,
    version: u8,
    leases: u8,
    used: [bool; N],
    present: [bool; N],
    /// Store `LastModified`.
    modified: [u8; N],
    /// The current writer's own uploads and when it started them.
    started: [Option<u8>; N],
    live: [bool; N],
    /// When GC observed each removal.
    removed: [Option<u8>; N],
    inflight: Option<(usize, u8)>,
    candidates: [bool; N],
    listed_at: Option<u8>,
    rechecked: bool,
    cycles: u8,
    confirmed: Option<(u8, [bool; N])>,
    query: Option<(u8, [bool; N])>,
    read: [bool; N],
    result: Option<bool>,
    bad_rows: bool,
    bad_availability: bool,
    orphan_collected: bool,
    removed_collected: bool,
    fenced_by_gc: bool,
    kept_by_recheck: bool,
    refused: bool,
    late_error: bool,
    restarted_timers: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Tick,
    Upload(usize),
    Dispatch(usize),
    Refuse(usize),
    Land,
    Remove(usize),
    Bump,
    Takeover,
    Collect(usize),
    List,
    Recheck,
    Delete(usize),
    Confirm,
    Start,
    Get(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
    fn retention(&self) -> u8 {
        if self.has("no-retention") { 0 } else { T }
    }
    /// The writer may reference its upload only while its own clock, which may
    /// run `DRIFT` slow, reads below `T_O - M`.
    fn young(&self, s: &State, started: u8) -> bool {
        let margin = if self.has("no-margin") { 0 } else { DRIFT };
        self.has("no-age-check") || (s.now - started).saturating_sub(DRIFT) < T_O - margin
    }
    fn referenceable(&self, s: &State, o: usize) -> Option<u8> {
        if s.live[o] || s.removed[o].is_some() || !s.present[o] {
            return None;
        }
        if self.has("reuse-found") && s.started[o].is_none() {
            // Deduplicate by finding an object already at the address.
            return Some(s.now);
        }
        s.started[o]
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.now < HORIZON {
            a.push(Action::Tick);
        }
        for o in 0..N {
            // Names are interchangeable, so uploads take them in order.
            if !s.used[o] && (o == 0 || s.used[o - 1]) {
                a.push(Action::Upload(o));
            }
            if s.inflight.is_none() && s.version < VERSIONS {
                if let Some(started) = self.referenceable(s, o) {
                    if self.young(s, started) {
                        a.push(Action::Dispatch(o));
                    } else if s.started[o].is_some() {
                        a.push(Action::Refuse(o));
                    }
                }
                if s.live[o] {
                    a.push(Action::Remove(o));
                }
            }
            if s.removed[o].is_some() && s.present[o] {
                a.push(Action::Collect(o));
            }
            if s.rechecked && s.candidates[o] {
                a.push(Action::Delete(o));
            }
            if s.query.is_some()
                && s.result.is_none()
                && s.query.is_some_and(|q| q.1[o])
                && !s.read[o]
            {
                a.push(Action::Get(o));
            }
        }
        if s.inflight.is_some() {
            a.push(Action::Land);
        }
        // GC's own head move; commits move it otherwise.
        if s.listed_at.is_some() && !s.rechecked && s.version < VERSIONS {
            a.push(Action::Bump);
        }
        if s.leases == 0 && s.version < VERSIONS {
            a.push(Action::Takeover);
        }
        if s.listed_at.is_none() && s.cycles < 2 {
            a.push(Action::List);
        }
        if s.listed_at.is_some() && !s.rechecked {
            a.push(Action::Recheck);
        }
        if s.confirmed.is_none() {
            a.push(Action::Confirm);
        }
        if s.query.is_none() && s.confirmed.is_some() {
            a.push(Action::Start);
        }
        a.retain(|action| self.next_state(s, action.clone()).is_some());
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        match a {
            Action::Tick => n.now += 1,
            Action::Upload(o) => {
                n.used[o] = true;
                n.present[o] = true;
                n.modified[o] = s.now;
                n.started[o] = Some(s.now);
            }
            Action::Dispatch(o) => n.inflight = Some((o, s.version)),
            Action::Refuse(o) => {
                n.started[o] = None;
                n.refused = true;
            }
            Action::Land => {
                let (o, base) = s.inflight?;
                n.inflight = None;
                if base == s.version {
                    n.version += 1;
                    n.live[o] = true;
                } else if s.listed_at.is_some_and(|v| base <= v) && s.candidates[o] {
                    n.fenced_by_gc = true;
                }
            }
            Action::Remove(o) => {
                n.version += 1;
                n.live[o] = false;
                n.removed[o] = Some(s.now);
            }
            Action::Bump => n.version += 1,
            Action::Takeover => {
                n.leases += 1;
                n.version += 1;
                n.started = [None; N];
                for r in n.removed.iter_mut().flatten() {
                    if self.has("keep-timestamps") {
                        // The predecessor's clock read one tick ahead.
                        *r = r.saturating_sub(1);
                    } else {
                        n.restarted_timers |= *r < s.now;
                        *r = s.now;
                    }
                }
            }
            Action::Collect(o) => {
                if s.now < s.removed[o]? + self.retention() {
                    return None;
                }
                n.present[o] = false;
                n.removed_collected = true;
            }
            Action::List => {
                for o in 0..N {
                    n.candidates[o] = s.present[o]
                        && s.now - s.modified[o] >= T_O
                        && !s.live[o]
                        && s.removed[o].is_none();
                }
                if !n.candidates.contains(&true) {
                    return None;
                }
                n.listed_at = Some(s.version);
                n.cycles += 1;
            }
            Action::Recheck => {
                if !self.has("no-head-move") && s.version <= s.listed_at? {
                    return None;
                }
                n.rechecked = true;
                if !self.has("no-recheck") {
                    for o in 0..N {
                        if n.candidates[o] && (s.live[o] || s.removed[o].is_some()) {
                            n.candidates[o] = false;
                            n.kept_by_recheck = true;
                        }
                    }
                }
                if !n.candidates.contains(&true) {
                    n.listed_at = None;
                    n.rechecked = false;
                }
            }
            Action::Delete(o) => {
                n.candidates[o] = false;
                n.present[o] = false;
                n.orphan_collected = true;
                if !n.candidates.contains(&true) {
                    n.listed_at = None;
                    n.rechecked = false;
                }
            }
            Action::Confirm => n.confirmed = Some((s.now, s.live)),
            Action::Start => {
                let (at, set) = s.confirmed?;
                if !self.has("stale-start") && s.now - at > S {
                    return None;
                }
                if !set.contains(&true) {
                    return None;
                }
                n.query = Some((s.now, set));
            }
            Action::Get(o) => {
                let (start, set) = s.query?;
                n.read[o] = true;
                if !s.present[o] {
                    n.bad_availability |= s.now - start < T - S;
                    if self.has("missing-as-empty") {
                        n.bad_rows = true;
                    } else {
                        n.result = Some(false);
                        n.late_error = true;
                        return Some(n);
                    }
                }
                if (0..N).all(|p| !set[p] || n.read[p]) {
                    n.result = Some(true);
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "committed state references only present objects",
                |_, s: &State| (0..N).all(|o| !s.live[o] || s.present[o]),
            ),
            Property::always("rows or an error", |_, s: &State| {
                s.result != Some(true) || !s.bad_rows
            }),
            Property::always(
                "a query within the retention never fails",
                |_, s: &State| !s.bad_availability,
            ),
            Property::sometimes("never-committed upload collected by age", |_, s: &State| {
                s.orphan_collected
            }),
            Property::sometimes(
                "removed object collected after retention",
                |_, s: &State| s.removed_collected,
            ),
            Property::sometimes("delayed commit fenced by GC's head move", |_, s: &State| {
                s.fenced_by_gc
            }),
            Property::sometimes(
                "recheck keeps an object a delayed commit referenced",
                |_, s: &State| s.kept_by_recheck,
            ),
            Property::sometimes("old upload refused at commit", |_, s: &State| s.refused),
            Property::sometimes("query past the retention errors", |_, s: &State| {
                s.late_error
            }),
            Property::sometimes("takeover restarts removal timers", |_, s: &State| {
                s.restarted_timers
            }),
        ]
    }
}
