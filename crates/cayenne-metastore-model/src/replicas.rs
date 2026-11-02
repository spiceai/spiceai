// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Regional read-only replicas: readers in a replica location never write.
//!
//! Writers commit only to the source location. A one-way replication process
//! copies objects and head versions to the replica asynchronously, one at a
//! time, in any order; source deletes are not replicated. A replica reader
//! adopts a replicated head only if it is newer than the one it holds and every
//! object it newly references is present in the replica, confirms freshness
//! with a conditional GET on the source head (timed from when the request was
//! sent), starts a query from a head confirmed within `S`, and reads objects
//! from the replica only. The source GC follows the retention family: a
//! removed object is deleted `T` after its removal, and a never-committed
//! upload once the head's store time is `T_O` past it, after which the writer
//! can no longer commit it. The replica GC deletes a copy `T` after the
//! replicated records show it removed, and a copy no replicated record
//! mentions only once the source no longer has it. Every object has a fresh
//! name and is never referenced again after its removal.
use stateright::{Model, Property};

const N: usize = 2;
/// Head versions, including the initial empty head.
const V: usize = 5;
/// Reader retention.
const T: u8 = 3;
/// Reader staleness bound: a query starts from a head confirmed this recently.
const S: u8 = 1;
/// Age at which an unreferenced upload may be collected.
const T_O: u8 = 4;
const HORIZON: u8 = 8;

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
    // Source location.
    /// The source head's commit number.
    version: u8,
    /// The source head's `LastModified`.
    head_time: u8,
    /// Each head version's live objects, as a bit set.
    live: [u8; V],
    /// Each head version's recorded removals not yet deleted at the source.
    removals: [u8; V],
    /// The uploads each head version's `LastModified` is `T_O` past, recorded
    /// only where the replica GC uses it.
    aged: [u8; V],
    used: u8,
    /// Ever referenced by a source head.
    published: u8,
    source: u8,
    /// Each object's store `LastModified`, which replication preserves.
    modified: [u8; N],
    /// When the source GC observed each recorded removal.
    removed: [Option<u8>; N],
    // Replica location.
    /// Head versions that have reached the replica.
    arrived: u8,
    /// The replica head: whichever version arrived last.
    replica_head: u8,
    /// When the replica head was last written.
    replica_time: u8,
    copied: u8,
    replica: u8,
    /// Versions that reached the replica before one of their objects.
    early: u8,
    /// When the replica GC first read each removal in a replicated head.
    timers: [Option<u8>; N],
    // Replica reader.
    held: u8,
    /// A conditional source head GET: when it was sent, whether the held head
    /// was still current, and which head it confirms.
    confirming: Option<(u8, bool, u8)>,
    confirmed: Option<(u8, u8)>,
    query: Option<(u8, u8)>,
    read: u8,
    result: Option<bool>,
    lagging: bool,
    // Violation and witness flags.
    bad_adopt: bool,
    bad_confirm: bool,
    lower_adopt: bool,
    bad_availability: bool,
    bad_rows: bool,
    waited: bool,
    late_error: bool,
    removed_collected: bool,
    orphan_collected: bool,
    deleted_collected: bool,
    committed_deleted: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Tick,
    Upload(usize),
    Publish(usize),
    Remove(usize),
    /// The source GC's empty commit, so an idle head's time advances.
    Bump,
    SourceExpire(usize),
    SourceSweep(usize),
    CopyObject(usize),
    CopyHead(u8),
    Adopt,
    Confirm,
    Respond,
    Start,
    Get(usize),
    /// The replica GC reads the replica head's recorded removals.
    ReplicaRead,
    ReplicaExpire(usize),
    ReplicaSweep(usize),
}
const fn bit(o: usize) -> u8 {
    1 << o
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
    /// The reader graph adds one replica reader and one object; the GC graph
    /// has two objects and no reader.
    fn readers(&self) -> bool {
        self.has("readers")
    }
    /// The head-age sweep records an aged set per version, so its graph keeps
    /// one version fewer to stay tractable.
    fn versions(&self) -> usize {
        if self.has("orphan-head-age") {
            V - 1
        } else {
            V
        }
    }
    fn objects(&self) -> usize {
        if self.readers() { 1 } else { N }
    }
    fn commit(&self, s: &mut State, live: u8) {
        let v = usize::from(s.version) + 1;
        s.version += 1;
        s.head_time = s.now;
        if self.has("orphan-head-age") {
            // Uploads after this commit are younger than it, so the set is final.
            s.aged[v] = (0..N)
                .filter(|&o| s.used & bit(o) != 0 && s.now.saturating_sub(s.modified[o]) >= T_O)
                .fold(0, |m, o| m | bit(o));
        }
        s.live[v] = live;
        s.removals[v] = (0..N)
            .filter(|&o| s.removed[o].is_some())
            .fold(0, |m, o| m | bit(o));
    }
    /// The replica GC may delete a copy no replicated head mentions.
    fn sweepable(&self, s: &State, o: usize) -> bool {
        let head = usize::from(s.replica_head);
        if self.has("source-404-only") {
            return s.source & bit(o) == 0;
        }
        let mentioned = (s.live[head] | s.removals[head]) & bit(o) != 0;
        if mentioned || s.timers[o].is_some() {
            return false;
        }
        if self.has("orphan-no-source-check") {
            // Aged by the replica's own clock instead of a source HEAD.
            return s.now.saturating_sub(s.modified[o]) >= T_O;
        }
        if self.has("orphan-head-age") {
            // Aged by the replica head's `LastModified`, as the source sweep ages
            // uploads by the source head's.
            return s.aged[head] & bit(o) != 0;
        }
        // HEAD on the source returns 404.
        s.source & bit(o) == 0
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            arrived: 1,
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.result.is_some() {
            return;
        }
        if s.now < HORIZON {
            a.push(Action::Tick);
        }
        let commits = usize::from(s.version) + 1 < self.versions();
        if commits {
            a.push(Action::Bump);
        }
        for o in 0..self.objects() {
            // Names are interchangeable, so uploads take them in order.
            if s.used & bit(o) == 0 && (o == 0 || s.used & bit(o - 1) != 0) {
                a.push(Action::Upload(o));
            }
            if commits {
                a.push(Action::Publish(o));
                a.push(Action::Remove(o));
            }
            a.push(Action::SourceExpire(o));
            a.push(Action::SourceSweep(o));
            a.push(Action::CopyObject(o));
            a.push(Action::ReplicaExpire(o));
            a.push(Action::ReplicaSweep(o));
            if self.readers() {
                a.push(Action::Get(o));
            }
        }
        for v in 1..=s.version {
            a.push(Action::CopyHead(v));
        }
        a.push(Action::ReplicaRead);
        if self.readers() {
            a.extend([
                Action::Adopt,
                Action::Confirm,
                Action::Respond,
                Action::Start,
            ]);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        let head = usize::from(s.version);
        if matches!(a, Action::Publish(_) | Action::Remove(_) | Action::Bump)
            && head + 1 >= self.versions()
        {
            return None;
        }
        match a {
            Action::Tick => n.now += 1,
            Action::Upload(o) => {
                n.used |= bit(o);
                n.source |= bit(o);
                n.modified[o] = s.now;
            }
            Action::Publish(o) => {
                // The writer refuses its upload once its base is T_O past it.
                if s.source & bit(o) == 0
                    || s.published & bit(o) != 0
                    || s.head_time.saturating_sub(s.modified[o]) >= T_O
                {
                    return None;
                }
                n.published |= bit(o);
                n.committed_deleted |= s.copied & !s.replica & bit(o) != 0;
                self.commit(&mut n, s.live[head] | bit(o));
            }
            Action::Remove(o) => {
                if s.live[head] & bit(o) == 0 {
                    return None;
                }
                n.removed[o] = Some(s.now);
                self.commit(&mut n, s.live[head] & !bit(o));
            }
            Action::Bump => self.commit(&mut n, s.live[head]),
            Action::SourceExpire(o) => {
                if s.now < s.removed[o]? + T {
                    return None;
                }
                n.removed[o] = None;
                n.source &= !bit(o);
            }
            Action::SourceSweep(o) => {
                if s.source & bit(o) == 0
                    || s.published & bit(o) != 0
                    || s.head_time.saturating_sub(s.modified[o]) < T_O
                {
                    return None;
                }
                n.source &= !bit(o);
            }
            Action::CopyObject(o) => {
                // Replication reads the source copy, so a deleted object never arrives.
                if s.source & bit(o) == 0 || s.copied & bit(o) != 0 {
                    return None;
                }
                n.copied |= bit(o);
                n.replica |= bit(o);
            }
            Action::CopyHead(v) => {
                if s.arrived & bit(usize::from(v)) != 0 {
                    return None;
                }
                n.arrived |= bit(usize::from(v));
                n.replica_head = v;
                if self.has("replica-time") {
                    n.replica_time = s.now;
                }
                if s.live[usize::from(v)] & !s.copied != 0 {
                    n.early |= bit(usize::from(v));
                }
            }
            Action::Adopt => {
                let c = usize::from(s.replica_head);
                let holding = usize::from(s.held);
                if s.query.is_some() || c == holding || (c < holding && !self.has("adopt-lower")) {
                    return None;
                }
                // HEAD requests on the replica for every newly referenced object.
                if !self.has("no-frontier") && s.live[c] & !s.live[holding] & !s.replica != 0 {
                    return None;
                }
                n.bad_adopt |= s.live[c] & !s.replica != 0;
                n.lower_adopt |= c < holding;
                n.waited |= s.early & bit(c) != 0;
                n.held = s.replica_head;
                n.confirmed = None;
            }
            Action::Confirm => {
                let fresh = s
                    .confirmed
                    .is_some_and(|(at, v)| v == s.held && s.now - at <= S);
                if s.query.is_some()
                    || s.confirming.is_some()
                    || fresh
                    || s.live[usize::from(s.held)] == 0
                {
                    return None;
                }
                if self.has("replica-time") {
                    // The replica head's own `LastModified`, read from the replica.
                    if s.replica_head != s.held {
                        return None;
                    }
                    n.confirmed = Some((s.replica_time, s.held));
                } else {
                    // If-None-Match on the source head, evaluated when sent.
                    let current = s.version == s.held;
                    n.bad_confirm |= current && s.live[usize::from(s.held)] & !s.replica != 0;
                    n.confirming = Some((s.now, current, s.held));
                }
            }
            Action::Respond => {
                let (sent, current, v) = s.confirming?;
                n.confirming = None;
                if current && v == s.held {
                    n.confirmed = Some((sent, v));
                }
            }
            Action::Start => {
                let (at, v) = s.confirmed?;
                if s.query.is_some() || v != s.held || s.now - at > S {
                    return None;
                }
                n.query = Some((s.now, s.live[usize::from(v)]));
                n.confirmed = None;
                n.lagging = (usize::from(v) + 1..=head).any(|w| s.arrived & bit(w) == 0);
            }
            Action::Get(o) => {
                let (start, set) = s.query?;
                if set & bit(o) == 0 || s.read & bit(o) != 0 {
                    return None;
                }
                n.read |= bit(o);
                if s.replica & bit(o) == 0 {
                    n.bad_availability |= s.now - start < T - S;
                    if self.has("missing-as-empty") {
                        n.bad_rows = true;
                    } else {
                        n.result = Some(false);
                        n.late_error = true;
                        return Some(n);
                    }
                }
                if set & !n.read == 0 {
                    n.result = Some(true);
                }
            }
            Action::ReplicaRead => {
                if self.has("source-404-only") {
                    return None;
                }
                let recorded = s.removals[usize::from(s.replica_head)] & s.replica;
                for o in 0..N {
                    if recorded & bit(o) != 0 && s.timers[o].is_none() {
                        n.timers[o] = Some(s.now);
                    }
                }
            }
            Action::ReplicaExpire(o) => {
                if s.replica & bit(o) == 0 {
                    return None;
                }
                let aged = if self.has("eu-age-only") {
                    // A lifecycle rule on the copy's age alone.
                    s.now.saturating_sub(s.modified[o]) >= T
                } else {
                    s.timers[o].is_some_and(|at| s.now >= at + T)
                };
                if !aged {
                    return None;
                }
                n.replica &= !bit(o);
                n.timers[o] = None;
                n.removed_collected |= s.published & bit(o) != 0;
            }
            Action::ReplicaSweep(o) => {
                if s.replica & bit(o) == 0 || !self.sweepable(s, o) {
                    return None;
                }
                n.replica &= !bit(o);
                n.timers[o] = None;
                if s.published & bit(o) == 0 {
                    n.orphan_collected = true;
                } else {
                    n.deleted_collected |= s.source & bit(o) == 0;
                }
            }
        }
        if n == *s { None } else { Some(n) }
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![Property::always(
            "replica GC never deletes an object the source head references",
            |_, s: &State| s.live[usize::from(s.version)] & s.copied & !s.replica == 0,
        )];
        if !self.readers() {
            properties.extend([
                Property::sometimes(
                    "replica GC reclaims a removed object from its replicated removal",
                    |_, s: &State| s.removed_collected,
                ),
                Property::sometimes(
                    "replica GC reclaims a removed object the source deleted",
                    |_, s: &State| s.deleted_collected,
                ),
                Property::sometimes("replica GC reclaims an orphan", |_, s: &State| {
                    s.orphan_collected
                }),
                Property::sometimes(
                    "replica keeps no copy the source deleted",
                    |_, s: &State| {
                        let all = bit(N) - 1;
                        s.used == all
                            && s.copied == all
                            && s.source == 0
                            && s.replica == 0
                            && s.published != 0
                            && s.published != all
                    },
                ),
            ]);
            if self.has("orphan-no-source-check") {
                properties.push(Property::sometimes(
                    "writer commits an upload the replica already deleted",
                    |_, s: &State| s.committed_deleted,
                ));
            }
            return properties;
        }
        if self.has("adoption-all-present") {
            properties.push(Property::always(
                "an adopted head references only objects in the replica",
                |_, s: &State| !s.bad_adopt,
            ));
        }
        properties.extend([
            Property::always(
                "a head confirmed current references only objects in the replica",
                |_, s: &State| !s.bad_confirm,
            ),
            Property::always("a reader never adopts a lower head", |_, s: &State| {
                !s.lower_adopt
            }),
            Property::always(
                "a query within the protection window never fails",
                |_, s: &State| !s.bad_availability,
            ),
            Property::always("rows or an error", |_, s: &State| {
                s.result != Some(true) || !s.bad_rows
            }),
            Property::sometimes(
                "head arrives before its objects and the reader waits",
                |_, s: &State| s.waited,
            ),
            Property::sometimes(
                "query returns rows while replication lags",
                |_, s: &State| s.lagging && s.result == Some(true),
            ),
            Property::sometimes("query past the protection window errors", |_, s: &State| {
                s.late_error
            }),
        ]);
        properties
    }
}
