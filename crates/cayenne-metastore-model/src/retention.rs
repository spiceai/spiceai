// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Time-bounded retention: readers never write, and GC needs no generations.
//!
//! Removed objects are deleted a fixed retention `T` after GC observes their
//! removal, and the removal stays recorded in durable state until then. Uploads
//! that never committed are deleted once the head's `LastModified` is `T_O`
//! past theirs. A writer references only objects live in its CAS base or its
//! own uploads it has never published, while its base's store time is less
//! than `T_O - M` past a lower bound on the upload's. Every age is a
//! difference of two store timestamps; object timestamps may lag that lower
//! bound by up to `DRIFT`. Every object has a fresh name. A query starts from
//! a head confirmed within `S` of when its request was sent.
//!
//! With `remote-gc`, GC runs on another node under its own lease: its empty
//! commit is a CAS that races the writer's, a second GC node can take the
//! lease over, and deletes the first node already issued land afterwards.
//! With `checkpoint`, removals are durable only as records after the floor
//! and as the checkpoint root's pending-removals list; a checkpoint moves the
//! floor and drops the records below it, and a GC restart rebuilds its
//! pending set from the root and the records after the floor.
use stateright::{Model, Property};

const N: usize = 2;
/// Reader retention.
const T: u8 = 3;
/// Reader staleness bound: a query starts from a state confirmed this recently.
const S: u8 = 1;
/// Age at which an unreferenced upload may be collected.
const T_O: u8 = 4;
/// Store timestamp error between servers.
const DRIFT: u8 = 1;
const HORIZON: u8 = 9;
const SHORT_HORIZON: u8 = 7;
const VERSIONS: u8 = 6;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum Sweep {
    #[default]
    Idle,
    Listed,
    Selected,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    /// The store's clock, which GC and readers also use for their timers.
    now: u8,
    /// How far the store's clock ran ahead of the writer's own clock.
    lag: u8,
    version: u8,
    /// The head's `LastModified`.
    head_time: u8,
    leases: u8,
    used: [bool; N],
    present: [bool; N],
    /// Store `LastModified`.
    modified: [u8; N],
    /// The current writer's own unpublished uploads, each with a lower bound
    /// on its store time.
    started: [Option<u8>; N],
    live: [bool; N],
    /// Durable record of each removal not yet deleted, with when GC observed it.
    removed: [Option<u8>; N],
    /// A delete of a removed object, issued and not yet applied.
    collecting: [bool; N],
    inflight: Option<(usize, u8)>,
    sweep: Sweep,
    candidates: [bool; N],
    sweeps: u8,
    bumped: bool,
    /// A head GET: when its request was sent and what it read.
    confirming: Option<(u8, [bool; N])>,
    confirmed: Option<(u8, [bool; N])>,
    query: Option<(u8, [bool; N])>,
    read: [bool; N],
    result: Option<bool>,
    bad_rows: bool,
    bad_availability: bool,
    orphan_collected: bool,
    idle_orphan_collected: bool,
    removed_collected: bool,
    collected_young_upload: bool,
    refused: bool,
    late_error: bool,
    late_response: bool,
    restarted_timers: bool,
    /// GC's empty commit in flight: whether the head is still the version it
    /// read (its CAS condition), and the live set it read.
    gc_inflight: Option<(bool, [bool; N])>,
    gc_lost: bool,
    gc_retried: bool,
    gc_leases: u8,
    /// A delete issued by a GC node that has since lost its lease.
    stale: [bool; N],
    /// The new GC node has listed since it took over.
    gc_moved: bool,
    stale_landed: bool,
    /// A removal recorded in a record after the floor.
    logged: [bool; N],
    /// A removal in the checkpoint root's pending-removals list.
    rooted: [bool; N],
    checkpoints: u8,
    rebuilt_from_root: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Tick,
    /// The store's clock jumps ahead of the writer's own clock.
    Jump,
    /// An upload whose `LastModified` lags the writer's lower bound by the skew.
    Upload(usize, u8),
    Dispatch(usize),
    Refuse(usize),
    Land,
    Remove(usize),
    Bump,
    /// A remote GC node sends its empty commit, conditioned on the head it read.
    GcDispatch,
    GcLand,
    /// Another GC node takes over the GC lease.
    GcTakeover,
    /// The writer moves the floor to a new checkpoint root.
    Checkpoint,
    Takeover,
    Expire(usize),
    Collect(usize),
    List,
    Select,
    Delete(usize),
    Confirm,
    Respond,
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
    /// The reader graph adds one query; the writer graph adds store
    /// timestamp skew and checks only references.
    fn readers(&self) -> bool {
        self.has("readers")
    }
    /// The reader graph ends once its query has a result or can no longer
    /// start; references are checked in the writer graph.
    fn settled(&self, s: &State) -> bool {
        s.result.is_some()
            || (s.query.is_none()
                && !self.has("stale-start")
                && s.confirmed.is_some_and(|(at, _)| s.now - at > S))
    }
    fn retention(&self) -> u8 {
        if self.has("no-retention") { 0 } else { T }
    }
    /// The reader graphs with remote GC or checkpoints stop the clock earlier
    /// to stay tractable, and the one with both earlier still; an aged upload,
    /// a removal, a checkpoint and a takeover still fit before the horizon.
    fn horizon(&self) -> u8 {
        match (self.readers(), self.remote_gc(), self.checkpoint()) {
            (true, true, true) => SHORT_HORIZON - 1,
            (true, true, false) | (true, false, true) => SHORT_HORIZON,
            _ => HORIZON,
        }
    }
    /// The writer may reference its upload only while its base is less than
    /// `T_O - M` past the upload by the store's clock.
    fn young(&self, s: &State, started: u8) -> bool {
        let margin = if self.has("no-margin") { 0 } else { DRIFT };
        if self.has("no-age-check") {
            return true;
        }
        if self.has("writer-clock") {
            // Its own monotonic clock, which a jump of the store's clock does not move.
            return (s.now - s.lag).saturating_sub(started) < T_O - margin;
        }
        s.head_time.saturating_sub(started) < T_O - margin
    }
    fn referenceable(&self, s: &State, o: usize) -> Option<u8> {
        if s.live[o] || !s.present[o] {
            return None;
        }
        if self.has("reuse-found") && s.started[o].is_none() && s.removed[o].is_none() {
            // Deduplicate by finding an object already at the address.
            return Some(s.now);
        }
        s.started[o]
    }
    /// GC's age of an object: the head's `LastModified` minus the object's.
    fn old(&self, s: &State, o: usize) -> bool {
        let at = if self.has("list-date") {
            s.now
        } else {
            s.head_time
        };
        at.saturating_sub(s.modified[o]) >= T_O
    }
    fn head_write(s: &mut State) {
        s.version += 1;
        s.head_time = s.now;
        if let Some((current, _)) = &mut s.gc_inflight {
            // GC's pending empty commit is now conditioned on a stale version.
            *current = false;
        }
    }
    fn remote_gc(&self) -> bool {
        self.has("remote-gc")
    }
    fn checkpoint(&self) -> bool {
        self.has("checkpoint")
    }
    /// Whether durable state still lists a removal as pending: the root's
    /// list with the records after the floor applied.
    fn durable_removal(&self, s: &State, o: usize) -> bool {
        let root = s.rooted[o] && !s.live[o] && !self.has("tail-only");
        root || s.logged[o]
    }
    /// A new GC instance: its pending set comes from durable state, and every
    /// timer restarts on its own clock.
    fn restart_gc(&self, s: &State, n: &mut State) {
        if self.has("volatile-removals") {
            // The removals lived only in the predecessor's memory.
            n.removed = [None; N];
        }
        if self.checkpoint() {
            for o in 0..N {
                let durable = self.durable_removal(s, o);
                n.rebuilt_from_root |=
                    durable && s.rooted[o] && !s.logged[o] && s.present[o] && !s.collecting[o];
                n.removed[o] = durable.then(|| s.removed[o].unwrap_or(s.now));
            }
        }
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
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if self.readers() && self.settled(s) {
            return;
        }
        if s.now < self.horizon() {
            a.push(Action::Tick);
        }
        if self.has("writer-clock") && s.lag == 0 && s.now + T_O <= HORIZON {
            a.push(Action::Jump);
        }
        for o in 0..N {
            // Names are interchangeable, so uploads take them in order.
            if !s.used[o] && (o == 0 || s.used[o - 1]) {
                a.push(Action::Upload(o, 0));
                if !self.readers() && s.now >= DRIFT {
                    a.push(Action::Upload(o, DRIFT));
                }
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
            if s.removed[o].is_some() && s.present[o] && !s.collecting[o] {
                a.push(Action::Expire(o));
            }
            if s.collecting[o] {
                a.push(Action::Collect(o));
            }
            if s.sweep == Sweep::Selected && s.candidates[o] {
                a.push(Action::Delete(o));
            }
            if self.readers()
                && s.query.is_some()
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
        // With remote GC, a writer takeover leaves GC's timers alone and is
        // otherwise checked in the in-process graph; the GC takeover replaces it.
        if s.leases == 0 && s.version < VERSIONS && !self.remote_gc() {
            a.push(Action::Takeover);
        }
        match s.sweep {
            Sweep::Idle if s.sweeps < 2 => a.push(Action::List),
            Sweep::Listed => {
                a.push(Action::Select);
                // GC's empty commit, so an idle head's time advances.
                if !s.bumped && s.version < VERSIONS {
                    if !self.remote_gc() {
                        a.push(Action::Bump);
                    } else if s.gc_inflight.is_none() {
                        a.push(Action::GcDispatch);
                    }
                }
            }
            _ => {}
        }
        if s.gc_inflight.is_some() {
            a.push(Action::GcLand);
        }
        if self.remote_gc() && s.gc_leases == 0 {
            a.push(Action::GcTakeover);
        }
        if self.checkpoint()
            && s.checkpoints == 0
            && s.inflight.is_none()
            && s.version < VERSIONS
            && s.logged.contains(&true)
        {
            a.push(Action::Checkpoint);
        }
        // A head with nothing live cannot start a query.
        if self.readers()
            && s.confirmed.is_none()
            && (s.confirming.is_some() || s.live.contains(&true))
        {
            a.push(if s.confirming.is_none() {
                Action::Confirm
            } else {
                Action::Respond
            });
        }
        if s.query.is_none() && s.confirmed.is_some() {
            a.push(Action::Start);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut n = s.clone();
        match a {
            Action::Tick => n.now += 1,
            Action::Jump => {
                n.now += T_O;
                n.lag = T_O;
            }
            Action::Upload(o, skew) => {
                n.used[o] = true;
                n.present[o] = true;
                n.modified[o] = s.now - skew;
                n.started[o] = Some(if self.has("writer-clock") {
                    s.now - s.lag
                } else {
                    s.now
                });
            }
            Action::Dispatch(o) => n.inflight = Some((o, s.version)),
            Action::Refuse(o) => {
                n.started[o] = None;
                n.refused = !self.readers();
            }
            Action::Land => {
                let (o, base) = s.inflight?;
                n.inflight = None;
                if base == s.version {
                    Self::head_write(&mut n);
                    n.live[o] = true;
                    n.removed[o] = None;
                    n.logged[o] = false;
                    if !self.has("reref-published") {
                        // Published: from now on it is referenced only while live.
                        n.started[o] = None;
                    }
                }
            }
            Action::Remove(o) => {
                Self::head_write(&mut n);
                n.live[o] = false;
                n.removed[o] = Some(s.now);
                n.logged[o] = self.checkpoint();
            }
            Action::Bump => {
                Self::head_write(&mut n);
                n.bumped = true;
            }
            Action::GcDispatch => {
                // Only the blind write needs the live set it read.
                let read = if self.has("blind-commit") {
                    s.live
                } else {
                    [false; N]
                };
                n.gc_inflight = Some((true, read));
            }
            Action::GcLand => {
                let (current, read) = s.gc_inflight?;
                n.gc_inflight = None;
                if self.has("blind-commit") {
                    // An unconditional PUT of the head GC read.
                    Self::head_write(&mut n);
                    n.live = read;
                    n.bumped = true;
                } else if current {
                    Self::head_write(&mut n);
                    n.bumped = true;
                    n.gc_retried |= s.gc_lost;
                } else {
                    // Lost to the writer's commit; GC may read the head again.
                    n.gc_lost = !self.readers();
                }
            }
            Action::GcTakeover => {
                n.gc_leases += 1;
                // The old node's selected deletes are already issued and land
                // whenever the store applies them; its listing is gone.
                for o in 0..N {
                    if s.sweep == Sweep::Selected && s.candidates[o] {
                        n.collecting[o] = true;
                    }
                    // Tracked for the witness, in the writer graph only.
                    n.stale[o] = n.collecting[o] && !self.readers();
                }
                n.sweep = Sweep::Idle;
                n.candidates = [false; N];
                self.restart_gc(s, &mut n);
            }
            Action::Checkpoint => {
                Self::head_write(&mut n);
                n.checkpoints += 1;
                for o in 0..N {
                    // The root lists every removal not yet deleted, and the
                    // records below the new floor are dropped.
                    n.rooted[o] =
                        !self.has("root-omits") && self.durable_removal(s, o) && s.present[o];
                }
                n.logged = [false; N];
            }
            Action::Takeover => {
                Self::head_write(&mut n);
                n.leases += 1;
                n.started = [None; N];
                if !self.remote_gc() {
                    // GC runs in the writer's process.
                    self.restart_gc(s, &mut n);
                }
            }
            Action::Expire(o) => {
                if s.now < s.removed[o]? + self.retention() {
                    return None;
                }
                n.collecting[o] = true;
                // Without the publication rule, its writer could still reference it.
                n.collected_young_upload |=
                    !self.readers() && s.head_time.saturating_sub(s.modified[o]) < T_O - DRIFT;
            }
            Action::Collect(o) => {
                n.collecting[o] = false;
                n.present[o] = false;
                n.removed_collected |= s.removed[o].is_some();
                n.stale[o] = false;
                n.stale_landed |= s.stale[o] && s.gc_moved;
            }
            Action::List => {
                n.candidates = s.present;
                if !n.candidates.contains(&true) {
                    return None;
                }
                n.sweep = Sweep::Listed;
                n.sweeps += 1;
                n.bumped = false;
                n.gc_moved |= s.gc_leases > 0 && !self.readers();
            }
            Action::Select => {
                for o in 0..N {
                    n.candidates[o] = s.candidates[o]
                        && s.present[o]
                        && !s.live[o]
                        && (s.removed[o].is_none() || self.has("sweep-removed"))
                        && self.old(s, o);
                }
                n.sweep = if n.candidates.contains(&true) {
                    Sweep::Selected
                } else {
                    Sweep::Idle
                };
            }
            Action::Delete(o) => {
                n.candidates[o] = false;
                n.present[o] = false;
                n.orphan_collected = true;
                n.idle_orphan_collected |= !self.readers() && s.bumped;
                if !n.candidates.contains(&true) {
                    n.sweep = Sweep::Idle;
                }
            }
            Action::Confirm => n.confirming = Some((s.now, s.live)),
            Action::Respond => {
                let (requested, set) = s.confirming?;
                n.confirming = None;
                n.late_response |= s.now > requested;
                let at = if self.has("response-time") {
                    s.now
                } else {
                    requested
                };
                n.confirmed = Some((at, set));
            }
            Action::Start => {
                let (at, set) = s.confirmed?;
                if !self.has("stale-start") && s.now - at > S {
                    return None;
                }
                if !set.contains(&true) {
                    return None;
                }
                n.query = Some((s.now, set));
                // Its time no longer matters once the query has started.
                n.confirmed = Some((0, set));
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
        let mut properties = vec![
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
            Property::sometimes("takeover restarts removal timers", |_, s: &State| {
                s.restarted_timers
            }),
        ];
        if self.readers() {
            properties.extend([
                Property::sometimes("query past the retention errors", |_, s: &State| {
                    s.late_error
                }),
                Property::sometimes("head response arrives after its request", |_, s: &State| {
                    s.late_response && s.query.is_some()
                }),
            ]);
        } else {
            properties.extend([
                Property::sometimes(
                    "upload collected after GC's empty commit",
                    |_, s: &State| s.idle_orphan_collected,
                ),
                Property::sometimes(
                    "removed object collected while its upload is still young",
                    |_, s: &State| s.collected_young_upload,
                ),
                Property::sometimes("old upload refused at commit", |_, s: &State| s.refused),
            ]);
        }
        if self.remote_gc() && !self.readers() {
            properties.extend([
                Property::sometimes(
                    "GC empty commit loses a race and retries",
                    |_, s: &State| s.gc_retried,
                ),
                Property::sometimes(
                    "stale GC node's delete lands after takeover",
                    |_, s: &State| s.stale_landed,
                ),
            ]);
        }
        if self.checkpoint() {
            properties.push(Property::sometimes(
                "takeover rebuilds a removal from the checkpoint root",
                |_, s: &State| s.rebuilt_from_root,
            ));
        }
        properties
    }
}
