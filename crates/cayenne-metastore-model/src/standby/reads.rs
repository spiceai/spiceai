// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Queries on instances of one Spicepod sharing a location: the writer
//! answers from its local committed state while its data-write gate is open,
//! and every other instance, or a writer whose gate has closed, answers only
//! after confirming the head.
//!
//! The writer's gate closes at `sent + D` on its own clock, where `sent` is
//! when its latest successful lease renewal (or acquisition) was sent. A
//! renewal is three steps: the request is sent, the store applies it as a
//! conditional write on the lease version the writer last wrote, and the
//! response arrives, arbitrarily late. Another instance watches the lease
//! object and takes it over only after one version has stayed unchanged for
//! D on its own clock, counted from when it first received that version; the
//! takeover is a conditional write on that version, after which the new
//! writer raises the generation in the head and only then commits. A commit
//! is acknowledged when it lands. A pause freezes an instance: it takes no
//! step, but real time, and with it the deadline of its gate, runs on. A
//! confirming query reads the head no earlier than its own start, which is
//! what the conditional GET timed from its send guarantees.
//!
//! The instances' monotonic clocks run at one rate, so a single clock stands
//! for all of them; `clock-rate` removes that assumption. A lease read is one
//! step that returns the version current at that moment, which is the
//! earliest any instance can first receive a version; a slower read only
//! moves that receipt, and with it the takeover, later.
//!
//! With `boot-clock`, each instance's clocks drift in rate as in
//! `clock-rate`, and its host may suspend: a suspended instance takes no step
//! and its monotonic process clock stops, while real time and the other
//! instances' clocks run on. The gate is timed on a boot clock that keeps
//! counting through a suspend, and closes at `sent + D - RATE_MARGIN` on it;
//! the lease lapse, timed by the other instance, stays on its process clock.
//! `RATE_MARGIN` is `2 * RATE`, the smallest that holds: the writer's boot
//! clock may close the gate RATE late and the taker's clock may end its D RATE
//! early. In production terms the margin is D/12, which covers clocks whose
//! rates differ by up to a twelfth over one lease duration.
//!
//! With `reclaim`, data files and their reclamation join the graph. Each
//! commit adds a file, a writer may rewrite the live files into one, and the
//! writer that retired a file deletes it once `RECLAIM_AFTER` has passed on
//! its clock since it observed the retirement; a new writer restarts those
//! timers when it raises the head's generation. A query on the writer then
//! has two steps, as in the code: it decides at its start to answer from
//! local state while the gate is open, and later reads the files that local
//! state names, with no deadline. A pause between the two can span a
//! takeover, the new writer's rewrite and the delete, and a read that meets a
//! deleted file fails the query. Any wait before the delete is as good as
//! another here, since the pause is unbounded.
use stateright::{Model, Property};

/// Lease duration.
const D: u8 = 3;
/// Source changes to commit.
const CHANGES: u8 = 2;
const HORIZON: u8 = 7;
/// How far a lease version's store timestamp may lag its write when read
/// against another instance's clock.
const DRIFT: u8 = 1;
/// How far an instance's clock may move against the others' in `clock-rate`.
const RATE: u8 = 1;
const INSTANCES: usize = 2;
/// Data files with `reclaim`: one per change, and one a rewrite writes.
const FILES: usize = CHANGES as usize + 1;
/// How long the writer waits after observing a file retired before it
/// deletes it.
const RECLAIM_AFTER: u8 = 2;
/// How early the gate closes on the boot clock: each of two instances'
/// clocks may misjudge a lease duration by RATE, in opposite directions.
const RATE_MARGIN: u8 = 2 * RATE;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum Role {
    /// Serves reads by confirming the head.
    #[default]
    Standby,
    /// Holds the lease at this generation and has not yet raised the head's.
    Acquired(u8),
    /// Writes the head at this generation.
    Writer(u8),
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts"
)]
struct Instance {
    paused: bool,
    resumed: bool,
    role: Role,
    /// The lease version it last wrote, which its renewal is conditioned on.
    version: u8,
    /// When its data-write gate closes, on its own clock.
    gate: u8,
    /// The change its local committed state reaches.
    local: u8,
    /// A renewal: when it was sent on its own clock, and its outcome once
    /// the store applied it.
    renewal: Option<(u8, Option<bool>)>,
    /// The lease version it watches and when its lapse count starts.
    watched: Option<(u8, u8)>,
    /// A query waiting for its head read: the acknowledged change when it
    /// started, and whether it started after this instance resumed.
    query: Option<(u8, bool)>,
    /// How far its clock reads ahead of the shared clock.
    offset: u8,
    drifted: bool,
    /// It paused while it was the writer, before any takeover.
    paused_as_writer: bool,
    /// The data files its local state names.
    local_files: [bool; FILES],
    /// A query that decided to answer from local state and has not yet read
    /// its files: the acknowledged change when it started.
    local_query: Option<u8>,
    /// Suspended with its host: no step, and its process clock stops.
    suspended: bool,
    /// How many ticks its process clock has lost to suspends.
    suspended_for: u8,
    /// It was suspended while it was the writer, before any takeover.
    suspended_as_writer: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Lease {
    version: u8,
    /// The version's store timestamp.
    written: u8,
    generation: u8,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Head {
    generation: u8,
    pos: u8,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    now: u8,
    lease: Lease,
    head: Head,
    acknowledged: u8,
    instances: [Instance; INSTANCES],
    takeovers: u8,
    pauses: u8,
    /// A query missed a commit acknowledged before it started.
    stale_read: bool,
    /// Two instances' gates were open at once.
    gates_overlap: bool,
    local_answer: bool,
    resumed_confirms: bool,
    resumed_sees_new_commit: bool,
    new_commit_after_pause: bool,
    late_renewal_response: bool,
    /// Data files the head names, and those still stored.
    files_live: [bool; FILES],
    files_present: [bool; FILES],
    /// Each retired file not yet deleted, with when its sweeper observed the
    /// retirement on its clock.
    retired: [Option<u8>; FILES],
    /// The instance whose timers decide the deletes.
    sweeper: usize,
    rewrites: u8,
    /// A query returned rows with a file it read missing.
    bad_rows: bool,
    /// A writer's local query met a deleted file after a takeover and failed.
    reclaimed_error: bool,
    suspends: u8,
    /// A writer suspended before a takeover woke and confirmed the head.
    woken_confirms: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    Tick,
    /// Instance `.0`'s clock moves to read `.1` ahead of the shared clock:
    /// ahead at once, or behind by holding while the shared clock ticks.
    Drift(usize, u8),
    Renew(usize),
    RenewLand(usize),
    RenewRespond(usize),
    Watch(usize),
    TakeLease(usize),
    TakeHead(usize),
    Commit(usize),
    Query(usize),
    Respond(usize),
    Pause(usize),
    Resume(usize),
    /// The writer rewrites the live files into one, retiring them.
    Rewrite(usize),
    /// The sweeper deletes a retired file.
    Delete(usize),
    /// A query that decided to answer locally reads its files.
    ReadLocal(usize),
    /// The instance's host suspends: its process clock stops.
    Suspend(usize),
    Wake(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self { mode: mode.into() }
    }
    fn has(&self, flag: &str) -> bool {
        self.mode.contains(flag)
    }
    /// The reclamation and boot-clock graphs stop one tick earlier to stay
    /// tractable; a takeover, a rewrite and the delete, or a suspend through a
    /// takeover, still fit.
    fn horizon(&self) -> u8 {
        if self.has("reclaim") || self.has("boot-clock") {
            HORIZON - 1
        } else {
            HORIZON
        }
    }
    /// Pauses and suspends taken so far. The boot-clock graph has one of
    /// either, to stay tractable; the others have one pause.
    fn freezes(&self, s: &State) -> u8 {
        if self.has("boot-clock") {
            s.pauses + s.suspends
        } else {
            s.pauses
        }
    }
    /// Instance clocks drift in rate.
    fn drifts(&self) -> bool {
        self.has("clock-rate") || self.has("boot-clock")
    }
    /// The instance's boot clock, which keeps counting through a suspend.
    fn boot(s: &State, i: usize) -> u8 {
        s.now + s.instances[i].offset
    }
    /// The instance's monotonic process clock, which stops while it is
    /// suspended.
    fn mono(s: &State, i: usize) -> u8 {
        Self::boot(s, i) - s.instances[i].suspended_for
    }
    /// The clock the data-write gate is timed on.
    fn gate_clock(&self, s: &State, i: usize) -> u8 {
        if self.has("suspend-monotonic") {
            Self::mono(s, i)
        } else {
            Self::boot(s, i)
        }
    }
    /// How early the gate closes before the lease duration has passed.
    fn rate_margin(&self) -> u8 {
        if !self.has("boot-clock") || self.has("no-rate-margin") {
            0
        } else if self.has("short-rate-margin") {
            RATE_MARGIN - 1
        } else {
            RATE_MARGIN
        }
    }
    /// How long a renewal keeps the gate open.
    fn gate_span(&self) -> u8 {
        D - self.rate_margin()
    }
    fn gate_open(&self, s: &State, i: usize) -> bool {
        matches!(s.instances[i].role, Role::Acquired(_) | Role::Writer(_))
            && self.gate_clock(s, i) < s.instances[i].gate
    }
    /// Advances real time; a suspended instance's process clock stands still.
    fn advance(n: &mut State) {
        n.now += 1;
        for m in &mut n.instances {
            if m.suspended {
                m.suspended_for += 1;
            }
        }
    }
    fn step_down(n: &mut Instance) {
        n.role = Role::Standby;
        n.watched = None;
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let offsets: &[u8] = if self.drifts() { &[0, RATE] } else { &[0] };
        let mut states = vec![];
        for &a in offsets {
            for &b in offsets {
                let mut instances = [Instance::default(); INSTANCES];
                instances[0] = Instance {
                    role: Role::Writer(1),
                    version: 1,
                    gate: a + self.gate_span(),
                    ..Instance::default()
                };
                instances[0].offset = a;
                instances[1].offset = b;
                states.push(State {
                    now: 0,
                    lease: Lease {
                        version: 1,
                        written: 0,
                        generation: 1,
                    },
                    head: Head {
                        generation: 1,
                        pos: 0,
                    },
                    acknowledged: 0,
                    instances,
                    takeovers: 0,
                    pauses: 0,
                    stale_read: false,
                    gates_overlap: false,
                    local_answer: false,
                    resumed_confirms: false,
                    resumed_sees_new_commit: false,
                    new_commit_after_pause: false,
                    late_renewal_response: false,
                    files_live: [false; FILES],
                    files_present: [false; FILES],
                    retired: [None; FILES],
                    sweeper: 0,
                    rewrites: 0,
                    bad_rows: false,
                    reclaimed_error: false,
                    suspends: 0,
                    woken_confirms: false,
                });
            }
        }
        states
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if s.now < self.horizon() {
            a.push(Action::Tick);
        }
        for i in 0..INSTANCES {
            if self.drifts() && !s.instances[i].drifted {
                a.extend(
                    (0..=RATE)
                        .filter(|v| *v != s.instances[i].offset)
                        .map(|v| Action::Drift(i, v)),
                );
            }
            a.extend([
                Action::Renew(i),
                Action::RenewLand(i),
                Action::RenewRespond(i),
                Action::Watch(i),
                Action::TakeLease(i),
                Action::TakeHead(i),
                Action::Commit(i),
                Action::Query(i),
                Action::Respond(i),
                Action::Pause(i),
                Action::Resume(i),
            ]);
            if self.has("boot-clock") {
                a.extend([Action::Suspend(i), Action::Wake(i)]);
            }
            if self.has("reclaim") {
                a.extend([Action::Rewrite(i), Action::ReadLocal(i)]);
            }
        }
        if self.has("reclaim") {
            a.extend((0..FILES).map(Action::Delete));
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut n = s.clone();
        let running = |i: usize| !s.instances[i].paused && !s.instances[i].suspended;
        match action {
            Action::Tick => Self::advance(&mut n),
            Action::Drift(i, offset) => {
                // A clock never runs backwards: falling behind takes a tick it
                // does not see.
                if offset < s.instances[i].offset {
                    if s.now >= self.horizon() || s.instances[i].offset - offset > 1 {
                        return None;
                    }
                    Self::advance(&mut n);
                }
                n.instances[i].offset = offset;
                n.instances[i].drifted = true;
            }
            Action::Renew(i) => {
                let m = &s.instances[i];
                // Renewing only extends the gate, so at most one renewal per tick.
                if !running(i)
                    || m.role == Role::Standby
                    || m.renewal.is_some()
                    || self.gate_clock(s, i) + self.gate_span() <= m.gate
                {
                    return None;
                }
                n.instances[i].renewal = Some((self.gate_clock(s, i), None));
            }
            Action::RenewLand(i) => {
                // The store applies the conditional write, paused sender or not.
                let (sent, None) = s.instances[i].renewal? else {
                    return None;
                };
                let won = s.lease.version == s.instances[i].version;
                if won {
                    n.lease.version += 1;
                    n.lease.written = s.now;
                    n.instances[i].version = n.lease.version;
                }
                n.instances[i].renewal = Some((sent, Some(won)));
            }
            Action::RenewRespond(i) => {
                let (sent, Some(won)) = s.instances[i].renewal? else {
                    return None;
                };
                if !running(i) {
                    return None;
                }
                let clock = self.gate_clock(s, i);
                n.late_renewal_response |= clock > sent;
                let m = &mut n.instances[i];
                m.renewal = None;
                if won {
                    let from = if self.has("response-time") {
                        clock
                    } else {
                        sent
                    };
                    m.gate = m.gate.max(from + self.gate_span());
                } else {
                    // Another instance wrote the lease: this one is a replica now.
                    Self::step_down(m);
                }
            }
            Action::Watch(i) => {
                let m = &s.instances[i];
                if !running(i)
                    || m.role != Role::Standby
                    || m.watched.is_some_and(|(v, _)| v == s.lease.version)
                {
                    return None;
                }
                let from = if self.has("from-written") {
                    // The version's store timestamp, read against this clock.
                    s.lease.written.saturating_sub(DRIFT)
                } else {
                    Self::mono(s, i)
                };
                n.instances[i].watched = Some((s.lease.version, from));
            }
            Action::TakeLease(i) => {
                let m = &s.instances[i];
                let (version, from) = m.watched?;
                // The lapse is timed on the process clock.
                if !running(i)
                    || m.role != Role::Standby
                    || s.takeovers >= 1
                    || Self::mono(s, i).saturating_sub(from) < D
                    || version != s.lease.version
                {
                    return None;
                }
                n.takeovers += 1;
                n.lease = Lease {
                    version: s.lease.version + 1,
                    written: s.now,
                    generation: s.lease.generation + 1,
                };
                let m = &mut n.instances[i];
                m.role = Role::Acquired(n.lease.generation);
                m.version = n.lease.version;
                m.gate = self.gate_clock(s, i) + self.gate_span();
                m.watched = None;
            }
            Action::TakeHead(i) => {
                let Role::Acquired(generation) = s.instances[i].role else {
                    return None;
                };
                if !running(i) || !self.gate_open(s, i) || s.head.generation >= generation {
                    return None;
                }
                n.head.generation = generation;
                let clock = Self::mono(s, i);
                // The new writer reads the retired files from committed state
                // and times them on its own clock.
                for at in n.retired.iter_mut().flatten() {
                    *at = clock;
                }
                n.sweeper = i;
                let m = &mut n.instances[i];
                m.role = Role::Writer(generation);
                m.local = s.head.pos;
                m.local_files = s.files_live;
            }
            Action::Commit(i) => {
                let Role::Writer(generation) = s.instances[i].role else {
                    return None;
                };
                if !running(i)
                    || !self.gate_open(s, i)
                    || s.head.generation != generation
                    || s.head.pos >= CHANGES
                {
                    return None;
                }
                n.head.pos += 1;
                n.acknowledged = n.head.pos;
                n.instances[i].local = n.head.pos;
                if self.has("reclaim") {
                    let file = usize::from(s.head.pos);
                    n.files_live[file] = true;
                    n.files_present[file] = true;
                    n.instances[i].local_files = n.files_live;
                }
                n.new_commit_after_pause |=
                    (0..INSTANCES).any(|j| j != i && s.instances[j].paused_as_writer);
            }
            Action::Query(i) => {
                let m = &s.instances[i];
                if !running(i) || m.query.is_some() || m.local_query.is_some() {
                    return None;
                }
                let writer = matches!(m.role, Role::Writer(_));
                let local = writer && (self.has("no-gate") || self.gate_open(s, i));
                if local && self.has("reclaim") {
                    // Decided now; the files are read later.
                    n.instances[i].local_query = Some(s.acknowledged);
                } else if local {
                    // Answered from local state, as of the query's start.
                    n.stale_read |= m.local < s.acknowledged;
                    n.local_answer |= s.acknowledged > 0;
                } else {
                    n.instances[i].query = Some((s.acknowledged, m.resumed));
                    n.resumed_confirms |=
                        writer && m.paused_as_writer && m.resumed && s.takeovers > 0;
                    n.woken_confirms |= writer && m.suspended_as_writer && s.takeovers > 0;
                }
            }
            Action::Respond(i) => {
                let (required, after_resume) = s.instances[i].query?;
                if !running(i) {
                    return None;
                }
                n.instances[i].query = None;
                n.stale_read |= s.head.pos < required;
                n.resumed_sees_new_commit |= after_resume
                    && s.instances[i].paused_as_writer
                    && s.head.pos > s.instances[i].local;
            }
            Action::Pause(i) => {
                if !running(i) || self.freezes(s) >= 1 {
                    return None;
                }
                n.pauses += 1;
                let m = &mut n.instances[i];
                m.paused = true;
                m.paused_as_writer = matches!(m.role, Role::Writer(_)) && s.takeovers == 0;
            }
            Action::Resume(i) => {
                if !s.instances[i].paused {
                    return None;
                }
                n.instances[i].paused = false;
                n.instances[i].resumed = true;
            }
            Action::Suspend(i) => {
                if !running(i) || self.freezes(s) >= 1 {
                    return None;
                }
                n.suspends += 1;
                n.instances[i].suspended = true;
                n.instances[i].suspended_as_writer =
                    matches!(s.instances[i].role, Role::Writer(_)) && s.takeovers == 0;
            }
            Action::Wake(i) => {
                if !s.instances[i].suspended {
                    return None;
                }
                n.instances[i].suspended = false;
            }
            Action::Rewrite(i) => {
                let Role::Writer(generation) = s.instances[i].role else {
                    return None;
                };
                let rewritten = FILES - 1;
                if !running(i)
                    || !self.gate_open(s, i)
                    || s.head.generation != generation
                    || s.rewrites >= 1
                    || !s.files_live[..rewritten].contains(&true)
                {
                    return None;
                }
                n.rewrites += 1;
                let clock = Self::mono(s, i);
                for f in 0..rewritten {
                    if s.files_live[f] {
                        n.files_live[f] = false;
                        n.retired[f] = Some(clock);
                    }
                }
                n.files_live[rewritten] = true;
                n.files_present[rewritten] = true;
                n.sweeper = i;
                n.instances[i].local_files = n.files_live;
            }
            Action::Delete(f) => {
                let at = s.retired[f]?;
                if !running(s.sweeper)
                    || Self::mono(s, s.sweeper).saturating_sub(at) < RECLAIM_AFTER
                {
                    return None;
                }
                n.retired[f] = None;
                n.files_present[f] = false;
            }
            Action::ReadLocal(i) => {
                let required = s.instances[i].local_query?;
                if !running(i) {
                    return None;
                }
                let m = &s.instances[i];
                n.instances[i].local_query = None;
                // The view is built from local state now and its files read.
                n.stale_read |= m.local < required;
                let missing = (0..FILES).any(|f| m.local_files[f] && !s.files_present[f]);
                if !missing {
                    n.local_answer |= required > 0;
                } else if self.has("missing-as-empty") {
                    n.bad_rows = true;
                } else {
                    n.reclaimed_error |= s.takeovers > 0;
                }
            }
        }
        n.gates_overlap |= (0..INSTANCES).all(|i| self.gate_open(&n, i));
        (n != *s).then_some(n)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Tick => "the clock ticks".into(),
            Action::Drift(i, offset) => {
                format!("instance {i}'s clock now reads {offset} ahead")
            }
            Action::Renew(i) => format!("instance {i} sends a lease renewal"),
            Action::RenewLand(i) => format!("the store applies instance {i}'s renewal"),
            Action::RenewRespond(i) => format!("instance {i}'s renewal response arrives"),
            Action::Watch(i) => format!("instance {i} reads the lease object"),
            Action::TakeLease(i) => {
                format!("instance {i} takes over the lease it watched lapse")
            }
            Action::TakeHead(i) => format!("instance {i} raises the head's generation"),
            Action::Commit(i) => format!("instance {i} commits and acknowledges a change"),
            Action::Query(i) => format!("a query starts on instance {i}"),
            Action::Respond(i) => format!("instance {i}'s head read responds"),
            Action::Pause(i) => format!("instance {i} pauses"),
            Action::Resume(i) => format!("instance {i} resumes"),
            Action::Rewrite(i) => format!("instance {i} rewrites the live files into one"),
            Action::Delete(f) => format!("the sweeper deletes retired file {f}"),
            Action::ReadLocal(i) => {
                format!("instance {i}'s local query builds its view and reads its files")
            }
            Action::Suspend(i) => format!("instance {i}'s host suspends (its process clock stops)"),
            Action::Wake(i) => format!("instance {i}'s host wakes"),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!("lease duration D = {D}; clock {}", s.now),
            format!(
                "lease version {} (generation {}), head generation {} at change {}, acknowledged {}",
                s.lease.version, s.lease.generation, s.head.generation, s.head.pos, s.acknowledged
            ),
            crate::explorer::flags(&[
                ("writer answered locally", s.local_answer),
                ("resumed writer confirmed the head", s.resumed_confirms),
                (
                    "resumed writer saw the new writer's commit",
                    s.resumed_sees_new_commit,
                ),
                (
                    "new writer committed after the old one paused",
                    s.new_commit_after_pause,
                ),
                ("late renewal response", s.late_renewal_response),
                ("BAD stale read", s.stale_read),
                ("BAD gates overlap", s.gates_overlap),
                ("local query failed on a deleted file", s.reclaimed_error),
                (
                    "suspended writer woke and confirmed the head",
                    s.woken_confirms,
                ),
                ("BAD rows with a file missing", s.bad_rows),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let last = path.last_state().clone();
        let (lease, head) = (INSTANCES, INSTANCES + 1);
        let mut lanes: Vec<String> = (0..INSTANCES).map(|i| format!("instance {i}")).collect();
        lanes.push("lease object".into());
        lanes.push("head".into());
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let label = self.format_action(&action);
            let event = match action {
                Action::Tick => {
                    Event::on(lease, format!("clock -> {}", after.now)).tone(Tone::Gone)
                }
                Action::Drift(i, _) => Event::on(i, label).tone(Tone::Gone),
                Action::Renew(i) => Event::on(i, "send renewal").to(lease).tone(Tone::Staged),
                Action::RenewLand(i) => {
                    let won = after.lease.version > before.lease.version;
                    Event::on(
                        lease,
                        if won {
                            "renewal applied"
                        } else {
                            "renewal refused"
                        },
                    )
                    .to(i)
                    .tone(if won { Tone::Live } else { Tone::Gone })
                }
                Action::RenewRespond(i) => Event::on(
                    i,
                    format!(
                        "renewal response; gate closes at {}",
                        after.instances[i].gate
                    ),
                ),
                Action::Watch(i) => Event::on(lease, label).to(i),
                Action::TakeLease(i) => Event::on(i, label).to(lease).tone(Tone::Actor),
                Action::TakeHead(i) | Action::Commit(i) | Action::Rewrite(i) => {
                    Event::on(i, label).to(head)
                }
                Action::Query(i) if after.instances[i].query.is_some() => {
                    Event::on(i, "query confirms the head")
                        .to(head)
                        .tone(Tone::Staged)
                }
                Action::Query(i) => {
                    Event::on(i, "query answers from local state").tone(Tone::Actor)
                }
                Action::Respond(i) => Event::on(head, label).to(i),
                Action::Pause(i) => Event::on(i, "PAUSE").tone(Tone::Bad),
                Action::Resume(i) => Event::on(i, "resume"),
                Action::Delete(_) => Event::on(after.sweeper, label).tone(Tone::Gone),
                Action::ReadLocal(i) => Event::on(i, label).tone(Tone::Actor),
                Action::Suspend(i) => Event::on(i, "SUSPEND").tone(Tone::Bad),
                Action::Wake(i) => Event::on(i, "wake"),
            };
            events.push(event.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        let mut boxes: Vec<(String, Vec<String>, Tone)> = last
            .instances
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let role = match m.role {
                    Role::Standby => "replica: confirms the head".to_string(),
                    Role::Acquired(g) => format!("took the lease at generation {g}"),
                    Role::Writer(g) => format!("writer at generation {g}"),
                };
                let facts = vec![
                    if m.paused { "PAUSED" } else { "running" }.to_string(),
                    role,
                    format!(
                        "gate closes at {} (its clock {})",
                        m.gate,
                        self.gate_clock(&last, i)
                    ),
                    format!("local state at change {}", m.local),
                ];
                (
                    format!("INSTANCE {i}"),
                    facts,
                    if m.paused { Tone::Gone } else { Tone::Actor },
                )
            })
            .collect();
        boxes.push((
            "LEASE OBJECT".into(),
            vec![
                format!("version {}", last.lease.version),
                format!("generation {}", last.lease.generation),
            ],
            Tone::Live,
        ));
        boxes.push((
            "HEAD".into(),
            vec![
                format!("generation {}", last.head.generation),
                format!("changes committed: {}", last.head.pos),
            ],
            Tone::Live,
        ));
        Some(state_and_trace(&boxes, &Timeline { lanes, events }))
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always(
                "a query sees every commit acknowledged before it started",
                |_, s: &State| !s.stale_read,
            ),
            Property::always(
                "no two instances' gates are open at once",
                |_, s: &State| !s.gates_overlap,
            ),
            Property::sometimes(
                "a writer answers a query locally while its gate is open",
                |_, s: &State| s.local_answer,
            ),
            Property::sometimes(
                "a paused writer resumes after a takeover and confirms the head",
                |_, s: &State| s.resumed_confirms,
            ),
            Property::sometimes(
                "the resumed writer's query sees the new writer's commit",
                |_, s: &State| s.resumed_sees_new_commit,
            ),
            Property::sometimes(
                "the new writer commits after the old writer paused",
                |_, s: &State| s.new_commit_after_pause,
            ),
            Property::sometimes(
                "a renewal response arrives after its request",
                |_, s: &State| s.late_renewal_response,
            ),
        ];
        if self.has("boot-clock") {
            properties.push(Property::sometimes(
                "a writer suspended before a takeover wakes and confirms the head",
                |_, s: &State| s.woken_confirms,
            ));
        }
        if self.has("reclaim") {
            properties.extend([
                Property::always("rows or an error", |_, s: &State| !s.bad_rows),
                Property::sometimes(
                    "a writer's local query meets a file deleted after a takeover and fails",
                    |_, s: &State| s.reclaimed_error,
                ),
            ]);
        }
        properties
    }
}
