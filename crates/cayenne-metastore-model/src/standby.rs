// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Instances running the same Spicepod on one location: a lease object with an
//! expiry on a discrete clock, renewal by the holder, crash, pause and resume,
//! and takeover by a head commit that raises the writer generation. Only the
//! generation in the head fences writers; lease timing decides availability.
//!
//! The lease grants the generation the head checks: every head takeover is at
//! the generation of the lease its taker holds. In the `handoff` runs a holder
//! hands its lease to the other instance at a commit boundary: it stops its
//! data writes, then names that instance in the lease, which takes it over at
//! once, without waiting for a lapse. A data write outside a commit (an upload,
//! a delete) is made only while the writer's gate is open: it holds a
//! generation and its own lease timer has not run out. The `release-first`
//! control names the target before stopping its data writes; the `unnamed`
//! control stops renewing without naming anyone, so the target waits for the
//! lease to lapse.
//!
//! In the `retake` runs a holder
//! also takes its head over again (after a commit whose outcome it could not
//! learn, or a takeover that failed): it first moves its own lease to the next
//! generation, then takes the head over at it. The `split` control keeps two
//! generations instead, as a takeover at the head's next generation whatever
//! the lease holds, and the head's generation then drifts from the lease's.
use stateright::{Model, Property};

/// Ticks a lease lasts after acquisition or renewal.
const DURATION: usize = 3;
/// Source changes available to ingest.
const CHANGES: usize = 2;
/// Lease acquisitions are bounded so churning controls stay finite. Safe runs
/// never reach it: each acquisition there follows a crash or a pause.
const EPOCHS: usize = 4;
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    instances: usize,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Head {
    generation: usize,
    pos: usize,
}
/// A prepared ingest CAS: the base it was read against and the generation the
/// writer believed it held when it read the change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Commit {
    base: Head,
    generation: usize,
    change: usize,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum Status {
    #[default]
    Running,
    Paused,
    Crashed,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Instance {
    status: Status,
    /// Lease epoch this instance believes it holds; 0 means none.
    epoch: usize,
    /// Ticks left on its own lease by its local timer.
    left: usize,
    /// Generation it took over with; 0 means it serves reads only.
    generation: usize,
    /// The head after its takeover or its last accepted commit.
    base: Option<Head>,
    pending: Option<Commit>,
    to_ack: Option<usize>,
    /// Paused and resumed since it last acquired the lease.
    resumed: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Lease {
    holder: usize,
    epoch: usize,
    /// Ticks until expiry; 0 means expired.
    left: usize,
    /// How the holder failed since it acquired or renewed: 0 none, 1 crash, 2
    /// pause, 3 let go on purpose (a handoff).
    lost: u8,
    /// `lost` of the previous holder when this lease was acquired.
    cause: u8,
    /// The instance the holder handed the lease to, which may take it over
    /// before it lapses.
    handoff: Option<usize>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    head: Head,
    rows: Vec<usize>,
    acknowledged: usize,
    lease: Lease,
    /// Generation of the latest completed takeover.
    fenced: usize,
    instances: Vec<Instance>,
    /// (head generation, instance) of each accepted ingest commit.
    committers: Vec<(usize, usize)>,
    /// Ticks since the latest holder failure during which some instance ran
    /// and none was ingesting.
    waiting: usize,
    crashes: u8,
    pauses: u8,
    stale: bool,
    churn: bool,
    crash_takeover: bool,
    fenced_resume: bool,
    standby_read: bool,
    bad_read: bool,
    /// (change, instance) of each committed data file left on the committing
    /// instance's local disk; only the local-files control records any.
    local_files: Vec<(usize, usize)>,
    /// The lease generation its taker held when the head was last taken over.
    head_epoch: usize,
    /// Retakes so far; at most one, to keep the runs finite.
    retakes: u8,
    /// Handoffs so far; at most one.
    handoffs: u8,
    /// Ticks since a handoff whose running target has not taken the lease.
    handoff_wait: Option<usize>,
    /// A data write was made under a generation older than the head's.
    stale_data: bool,
    /// A handed-over lease was taken by the instance it names.
    handed_over: bool,
    /// Data writes so far; at most two, so each is a step of the trace.
    data_writes: u8,
    /// The instance a lease was handed to wrote data under it.
    handed_write: bool,
    /// A standby read or a takeover needed a file on another instance's disk.
    unreadable_file: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Tick,
    Acquire(usize),
    Renew(usize),
    Takeover(usize),
    /// A holder that writes the head takes it over again: under the next
    /// generation of its own lease, or, in the `split` control, at the head's
    /// next generation with the lease left as it is.
    Retake(usize),
    /// The holder hands its lease to the next instance.
    Handoff(usize),
    /// In the `release-first` control: the holder, which named the target
    /// while still writing, stops writing.
    StopWriting(usize),
    /// A data write outside a commit (an upload, a delete).
    DataWrite(usize),
    Read(usize),
    Commit(usize),
    Ack(usize),
    Crash(usize),
    Restart(usize),
    Pause(usize),
    Resume(usize),
    Serve(usize),
}
impl Protocol {
    pub(crate) fn new(mode: &str) -> Self {
        Self {
            mode: mode.into(),
            instances: if mode.contains("three") { 3 } else { 2 },
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// Lease exclusivity, churn and availability depend on lease timing; the
    /// early-acquisition safety run checks that safety does not.
    fn timed(&self) -> bool {
        !self.has("early-safety")
    }
    fn ingester(s: &State, i: usize) -> bool {
        let n = &s.instances[i];
        n.status == Status::Running && n.generation != 0 && n.generation == s.fenced && n.left > 0
    }
    fn ingesting(s: &State) -> bool {
        (0..s.instances.len()).any(|i| Self::ingester(s, i))
    }
    /// An action no tick may overtake: a due renewal, an acquisition of an
    /// expired lease, or a takeover commit after acquisition.
    fn urgent(&self, s: &State) -> bool {
        s.instances.iter().enumerate().any(|(i, n)| {
            n.status == Status::Running
                && (n.epoch > 0 && n.left <= 1 && !self.has("no-renew")
                    || n.epoch > 0 && n.generation == 0
                    || s.lease.handoff == Some(i) && n.epoch != s.lease.epoch
                    || s.lease.left == 0 && n.epoch != s.lease.epoch && s.lease.epoch < EPOCHS)
        })
    }
    fn step_down(n: &mut Instance) {
        n.epoch = 0;
        n.left = 0;
        n.generation = 0;
        n.base = None;
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let head = Head {
            generation: 1,
            pos: 0,
        };
        let mut instances = vec![Instance::default(); self.instances];
        instances[0] = Instance {
            epoch: 1,
            left: DURATION,
            generation: 1,
            base: Some(head),
            ..Instance::default()
        };
        vec![State {
            head,
            rows: vec![],
            acknowledged: 0,
            lease: Lease {
                holder: 0,
                epoch: 1,
                left: DURATION,
                lost: 0,
                cause: 0,
                handoff: None,
            },
            fenced: 1,
            instances,
            committers: vec![],
            waiting: 0,
            crashes: 0,
            pauses: 0,
            stale: false,
            churn: false,
            crash_takeover: false,
            fenced_resume: false,
            standby_read: false,
            bad_read: false,
            local_files: vec![],
            unreadable_file: false,
            head_epoch: 1,
            retakes: 0,
            handoffs: 0,
            handoff_wait: None,
            stale_data: false,
            handed_over: false,
            data_writes: 0,
            handed_write: false,
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        a.push(Action::Tick);
        for i in 0..self.instances {
            a.extend([
                Action::Acquire(i),
                Action::Renew(i),
                Action::Takeover(i),
                Action::Retake(i),
                Action::Handoff(i),
                Action::StopWriting(i),
                Action::DataWrite(i),
                Action::Read(i),
                Action::Commit(i),
                Action::Ack(i),
                Action::Crash(i),
                Action::Restart(i),
                Action::Pause(i),
                Action::Resume(i),
                Action::Serve(i),
            ]);
        }
        crate::prune_for_simulation(self, s, a);
    }
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut next = s.clone();
        let running = |i: usize| s.instances[i].status == Status::Running;
        match action {
            Action::Tick => {
                if self.urgent(s) {
                    return None;
                }
                if !Self::ingesting(s) && (0..self.instances).any(running) {
                    next.waiting = (s.waiting + 1).min(DURATION + 1);
                }
                next.lease.left = s.lease.left.saturating_sub(1);
                for n in &mut next.instances {
                    n.left = n.left.saturating_sub(1);
                }
                // The wait counts only while the target runs: a handoff to an
                // instance that stopped falls back to a lapse, as designed.
                let target_runs = s
                    .lease
                    .handoff
                    .is_some_and(|to| s.instances[to].status == Status::Running);
                next.handoff_wait = s
                    .handoff_wait
                    .filter(|_| target_runs || self.has("unnamed"))
                    .map(|w| (w + 1).min(DURATION + 1));
            }
            Action::Acquire(i) => {
                let n = &s.instances[i];
                // The lease object's conditional write admits one acquirer per
                // epoch, and only after expiry.
                let handed = s.lease.handoff == Some(i);
                if !running(i)
                    || n.epoch == s.lease.epoch
                    || s.lease.epoch >= EPOCHS
                    || s.lease.left > 0 && !self.has("early") && !handed
                {
                    return None;
                }
                // A lease handed over, or let go on purpose, is not churn.
                next.churn |= s.lease.lost == 0 && !handed;
                next.handed_over |= handed;
                next.handoff_wait = None;
                next.lease = Lease {
                    holder: i,
                    epoch: s.lease.epoch + 1,
                    left: DURATION,
                    lost: 0,
                    cause: s.lease.lost,
                    handoff: None,
                };
                let m = &mut next.instances[i];
                Self::step_down(m);
                m.epoch = s.lease.epoch + 1;
                m.left = DURATION;
                m.resumed = false;
            }
            Action::Renew(i) => {
                let n = &s.instances[i];
                if !running(i) || n.epoch == 0 || n.left >= DURATION || self.has("no-renew") {
                    return None;
                }
                if s.lease.epoch == n.epoch && s.lease.handoff.is_none_or(|to| to == i) {
                    next.lease.left = DURATION;
                    next.lease.lost = 0;
                    next.instances[i].left = DURATION;
                } else {
                    // The conditional renewal fails: another instance holds the lease.
                    Self::step_down(&mut next.instances[i]);
                }
            }
            Action::Takeover(i) => {
                let n = &s.instances[i];
                if !running(i) || n.epoch == 0 || n.generation != 0 {
                    return None;
                }
                let generation = if self.has("no-generation") {
                    // Fencing by the lease object only: the head is read, not written.
                    n.epoch
                } else if self.has("split") {
                    // Two generations: the head's next, whatever the lease holds.
                    next.head.generation = s.head.generation + 1;
                    next.head.generation
                } else if s.head.generation >= n.epoch {
                    // The takeover CAS requires a lower generation in the head.
                    Self::step_down(&mut next.instances[i]);
                    return Some(next);
                } else {
                    next.head.generation = n.epoch;
                    n.epoch
                };
                if !self.has("no-generation") {
                    next.head_epoch = n.epoch;
                }
                next.crash_takeover |= s.lease.epoch == n.epoch && s.lease.cause == 1;
                next.unreadable_file |= s.local_files.iter().any(|&(_, o)| o != i);
                next.fenced = generation;
                let m = &mut next.instances[i];
                m.generation = generation;
                m.base = Some(next.head);
            }
            Action::Retake(i) => {
                let n = &s.instances[i];
                if !self.has("retake")
                    || !running(i)
                    || s.retakes >= 1
                    || n.generation == 0
                    || n.left == 0
                    || s.lease.epoch != n.epoch
                {
                    return None;
                }
                next.retakes += 1;
                if !self.has("split") {
                    // The holder moves its own lease to the next generation:
                    // a conditional write on the version it wrote.
                    if s.lease.epoch >= EPOCHS {
                        return None;
                    }
                    next.lease.epoch = s.lease.epoch + 1;
                    next.lease.left = DURATION;
                    next.lease.lost = 0;
                    next.instances[i].epoch = next.lease.epoch;
                    next.instances[i].left = DURATION;
                }
                // It takes the head over next; a commit it sent before stays in
                // flight.
                let m = &mut next.instances[i];
                m.generation = 0;
                m.base = None;
            }
            Action::Handoff(i) => {
                let n = &s.instances[i];
                let to = (i + 1) % self.instances;
                // At a commit boundary: nothing prepared, nothing to acknowledge.
                if !self.has("handoff")
                    || !running(i)
                    || !running(to)
                    || s.handoffs >= 1
                    || n.generation == 0
                    || n.left == 0
                    || s.lease.epoch != n.epoch
                    || n.pending.is_some()
                    || n.to_ack.is_some()
                {
                    return None;
                }
                next.handoffs += 1;
                next.handoff_wait = Some(0);
                if self.has("unnamed") {
                    // Stops writing and renewing, naming no one: the lease is
                    // let go (not lost) and lapses after its duration.
                    next.lease.lost = 3;
                    Self::step_down(&mut next.instances[i]);
                } else if self.has("release-first") {
                    // Names the target while its own writes are still open.
                    next.lease.handoff = Some(to);
                    next.lease.lost = 3;
                } else {
                    // Stops its data writes, then names the target. The lease
                    // is let go, not lost: if the target stops before taking
                    // it, it lapses and anyone may take it.
                    Self::step_down(&mut next.instances[i]);
                    next.lease.handoff = Some(to);
                    next.lease.lost = 3;
                }
            }
            Action::StopWriting(i) => {
                let n = &s.instances[i];
                if !self.has("release-first")
                    || !running(i)
                    || n.generation == 0
                    || s.lease.handoff.is_none_or(|to| to == i)
                {
                    return None;
                }
                Self::step_down(&mut next.instances[i]);
            }
            Action::DataWrite(i) => {
                let n = &s.instances[i];
                // The gate: a generation held and the instance's own lease
                // timer still running.
                if !self.has("handoff")
                    || !running(i)
                    || n.generation == 0
                    || n.left == 0
                    || s.data_writes >= 2
                {
                    return None;
                }
                next.data_writes += 1;
                next.stale_data |= n.generation < s.head.generation;
                next.handed_write |= s.handed_over && n.generation >= 2;
            }
            Action::Read(i) => {
                let n = &s.instances[i];
                let base = n.base?;
                if !running(i)
                    || n.generation == 0
                    || n.left == 0
                    || n.pending.is_some()
                    || n.to_ack.is_some()
                    || base.pos >= CHANGES
                {
                    return None;
                }
                next.instances[i].pending = Some(Commit {
                    base,
                    generation: n.generation,
                    change: base.pos + 1,
                });
            }
            Action::Commit(i) => {
                let c = s.instances[i].pending?;
                if !running(i) {
                    return None;
                }
                next.instances[i].pending = None;
                if s.head == c.base {
                    next.head.pos += 1;
                    next.rows.push(c.change);
                    if self.has("local-files") {
                        // Data files stay on the committing instance's disk.
                        next.local_files.push((c.change, i));
                    }
                    if !next.committers.contains(&(s.head.generation, i)) {
                        next.committers.push((s.head.generation, i));
                        next.committers.sort_unstable();
                    }
                    next.stale |= c.generation < s.fenced;
                    let m = &mut next.instances[i];
                    m.base = Some(next.head);
                    m.to_ack = Some(c.change);
                } else if s.head.generation == c.generation
                    && s.instances[i].generation == c.generation
                {
                    // Same generation: rebase on the current head.
                    next.instances[i].base = Some(s.head);
                } else if s.instances[i].generation == c.generation {
                    // A newer generation is in the head: this writer is fenced.
                    next.fenced_resume |= s.instances[i].resumed;
                    Self::step_down(&mut next.instances[i]);
                }
                // Otherwise the request predates the instance's current lease;
                // its rejection says nothing about that lease.
            }
            Action::Ack(i) => {
                let c = s.instances[i].to_ack?;
                if !running(i) {
                    return None;
                }
                next.acknowledged = s.acknowledged.max(c);
                next.instances[i].to_ack = None;
            }
            Action::Crash(i) => {
                if s.instances[i].status == Status::Crashed || s.crashes >= 1 {
                    return None;
                }
                if s.lease.holder == i && s.lease.epoch == s.instances[i].epoch {
                    next.lease.lost = 1;
                    next.waiting = 0;
                }
                next.crashes += 1;
                next.instances[i] = Instance {
                    status: Status::Crashed,
                    ..Instance::default()
                };
            }
            Action::Restart(i) => {
                if s.instances[i].status != Status::Crashed {
                    return None;
                }
                next.instances[i].status = Status::Running;
            }
            Action::Pause(i) => {
                if !running(i) || s.pauses >= 1 {
                    return None;
                }
                if s.lease.holder == i && s.lease.epoch == s.instances[i].epoch {
                    next.lease.lost = 2;
                    next.waiting = 0;
                }
                next.pauses += 1;
                next.instances[i].status = Status::Paused;
            }
            Action::Resume(i) => {
                if s.instances[i].status != Status::Paused {
                    return None;
                }
                next.instances[i].status = Status::Running;
                next.instances[i].resumed = true;
            }
            Action::Serve(i) => {
                if !running(i) || Self::ingester(s, i) || !Self::ingesting(s) || s.standby_read {
                    return None;
                }
                // A standby reads the committed head without writing.
                next.standby_read = true;
                next.bad_read |= s.rows != (1..=s.head.pos).collect::<Vec<_>>();
                next.unreadable_file |= s.local_files.iter().any(|&(_, o)| o != i);
            }
        }
        if Self::ingesting(&next) {
            next.waiting = 0;
        }
        (next != *s).then_some(next)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Tick => "the clock ticks".into(),
            Action::Acquire(i) => format!("instance {i} acquires the expired lease"),
            Action::Renew(i) => format!("instance {i} renews its lease"),
            Action::Takeover(i) if self.has("no-generation") => {
                format!(
                    "instance {i} takes over (lease object only; the head generation is not raised)"
                )
            }
            Action::Takeover(i) if self.has("split") => {
                format!("instance {i} takes over the head at its next generation")
            }
            Action::Takeover(i) => {
                format!("instance {i} takes over the head (raises its generation)")
            }
            Action::Retake(i) if self.has("split") => {
                format!("instance {i} takes its head over again (lease unchanged)")
            }
            Action::Retake(i) => {
                format!(
                    "instance {i} moves its lease to the next generation to take its head over again"
                )
            }
            Action::Handoff(i) if self.has("unnamed") => {
                format!("instance {i} stops writing and renewing, naming no successor")
            }
            Action::Handoff(i) if self.has("release-first") => format!(
                "instance {i} names instance {} in the lease while still writing",
                (i + 1) % self.instances
            ),
            Action::Handoff(i) => format!(
                "instance {i} stops writing, then hands its lease to instance {}",
                (i + 1) % self.instances
            ),
            Action::StopWriting(i) => format!("instance {i} stops writing"),
            Action::DataWrite(i) => format!("instance {i} writes a data object"),
            Action::Read(i) => format!("instance {i} reads a source change and prepares a commit"),
            Action::Commit(i) => format!("instance {i}'s commit CAS arrives at the head"),
            Action::Ack(i) => format!("instance {i} acknowledges the source"),
            Action::Crash(i) => format!("instance {i} crashes"),
            Action::Restart(i) => format!("instance {i} restarts"),
            Action::Pause(i) => format!("instance {i} pauses (GC pause, VM freeze)"),
            Action::Resume(i) => format!("instance {i} resumes"),
            Action::Serve(i) => format!("instance {i} serves a read from the head"),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        let committers: Vec<String> = s
            .committers
            .iter()
            .map(|(g, i)| format!("i{i}@gen{g}"))
            .collect();
        Some(crate::explorer::summary(&[
            format!(
                "head: generation {}, {} changes committed {:?}; acknowledged {}",
                s.head.generation, s.head.pos, s.rows, s.acknowledged
            ),
            format!(
                "commits by (instance, generation): {}",
                committers.join(" ")
            ),
            format!(
                "ticks with no ingester since the last holder failure: {}",
                s.waiting
            ),
            format!(
                "lease generation {}; head taken over under lease generation {}",
                s.lease.epoch, s.head_epoch
            ),
            crate::explorer::flags(&[
                ("stale commit attempted", s.stale),
                ("lease churn", s.churn),
                ("takeover after crash", s.crash_takeover),
                ("paused writer resumed and was fenced", s.fenced_resume),
                ("standby served a read", s.standby_read),
                ("BAD read", s.bad_read),
                ("file unreadable from another instance", s.unreadable_file),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Timeline, Tone, state_and_trace, steps};
        let last = path.last_state().clone();
        let n = last.instances.len();
        let (lease, head) = (n, n + 1);
        let mut lane_names: Vec<String> = (0..n).map(|i| format!("instance {i}")).collect();
        lane_names.push("lease object".into());
        lane_names.push("head".into());
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            let ev = match action {
                Action::Tick => Event::on(lease, format!("tick (lease {} left)", after.lease.left))
                    .tone(Tone::Gone),
                Action::Acquire(i) => {
                    Event::on(i, format!("acquire lease epoch {}", after.lease.epoch)).to(lease)
                }
                Action::Renew(i) => Event::on(i, "renew").to(lease),
                Action::Takeover(i) => {
                    // A takeover succeeds when the instance becomes the ingester; with
                    // fencing by the lease object alone the head generation stays put.
                    let ok = after.instances[i].generation != 0;
                    Event::on(
                        i,
                        if ok {
                            format!(
                                "takeover: ingester at generation {}",
                                after.instances[i].generation
                            )
                        } else {
                            "takeover rejected".into()
                        },
                    )
                    .to(head)
                    .tone(if ok { Tone::Live } else { Tone::Gone })
                }
                Action::Retake(i) => {
                    Event::on(i, format!("retake (lease epoch {})", after.lease.epoch)).to(lease)
                }
                Action::Handoff(i) => Event::on(i, "hand over").to(lease),
                Action::StopWriting(i) => Event::on(i, "stop writing"),
                Action::DataWrite(i) => Event::on(i, "data write").tone(if after.stale_data {
                    Tone::Bad
                } else {
                    Tone::Staged
                }),
                Action::Read(i) => Event::on(i, "read a change, prepare commit").tone(Tone::Staged),
                Action::Commit(i) => {
                    let ok = after.head.pos > before.head.pos;
                    Event::on(
                        i,
                        if ok {
                            format!("commit lands: change {}", after.head.pos)
                        } else {
                            "commit rejected (head moved or fenced)".into()
                        },
                    )
                    .to(head)
                    .tone(if ok { Tone::Live } else { Tone::Gone })
                }
                Action::Ack(i) => Event::on(i, "acknowledge source").tone(Tone::Actor),
                Action::Crash(i) => Event::on(i, "CRASH").tone(Tone::Bad),
                Action::Restart(i) => Event::on(i, "restart"),
                Action::Pause(i) => Event::on(i, "PAUSE").tone(Tone::Bad),
                Action::Resume(i) => Event::on(i, "resume"),
                Action::Serve(i) => Event::on(head, format!("instance {i} reads the head"))
                    .to(i)
                    .tone(Tone::Actor),
            };
            events.push(ev.breaking(&crate::explorer::broken(self, &before, &after)));
        }
        let mut boxes: Vec<(String, Vec<String>, Tone)> = last
            .instances
            .iter()
            .enumerate()
            .map(|(i, inst)| {
                let status = match inst.status {
                    Status::Running => "running",
                    Status::Paused => "PAUSED",
                    Status::Crashed => "CRASHED",
                };
                let mut lines = vec![
                    status.to_string(),
                    format!(
                        "believes lease epoch {} ({} ticks left)",
                        inst.epoch, inst.left
                    ),
                    if inst.generation == 0 {
                        "standby: serves reads".into()
                    } else {
                        format!("ingester at generation {}", inst.generation)
                    },
                ];
                if let Some(c) = inst.pending {
                    lines.push(format!(
                        "prepared commit of change {} on (gen {}, pos {})",
                        c.change, c.base.generation, c.base.pos
                    ));
                }
                let tone = match inst.status {
                    Status::Running => Tone::Actor,
                    _ => Tone::Gone,
                };
                (format!("INSTANCE {i}"), lines, tone)
            })
            .collect();
        boxes.push((
            "LEASE OBJECT".into(),
            vec![
                format!("holder instance {}", last.lease.holder),
                format!("epoch {}", last.lease.epoch),
                format!("{} ticks to expiry", last.lease.left),
            ],
            if last.lease.left > 0 {
                Tone::Live
            } else {
                Tone::Gone
            },
        ));
        boxes.push((
            "HEAD".into(),
            vec![
                format!("generation {}", last.head.generation),
                format!("changes committed: {}", last.head.pos),
                format!("acknowledged: {}", last.acknowledged),
            ],
            Tone::Live,
        ));
        Some(state_and_trace(
            &boxes,
            &Timeline {
                lanes: lane_names,
                events,
            },
        ))
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut p = vec![
            Property::always("source change applied at most once", |_, s: &State| {
                s.rows
                    .iter()
                    .enumerate()
                    .all(|(i, c)| !s.rows[..i].contains(c))
            }),
            Property::always("head rows are the source prefix", |_, s: &State| {
                s.rows.len() == s.head.pos && s.rows == (1..=s.rows.len()).collect::<Vec<_>>()
            }),
            Property::always("acknowledged source changes committed", |_, s: &State| {
                s.acknowledged <= s.head.pos
            }),
            Property::always("no stale-generation commit accepted", |_, s: &State| {
                !s.stale
            }),
            Property::always("one ingesting instance per generation", |_, s: &State| {
                s.committers.windows(2).all(|w| w[0].0 != w[1].0)
            }),
            Property::always("standby reads a committed prefix", |_, s: &State| {
                !s.bad_read
            }),
            Property::always(
                "standbys and new owners read every committed file",
                |_, s: &State| !s.unreadable_file,
            ),
            Property::sometimes("takeover after holder crash", |_, s: &State| {
                s.crash_takeover
            }),
            // The lease grants the generation the head checks.
            Property::always(
                "a head takeover carries its lease's generation",
                |_, s: &State| s.head_epoch == 0 || s.head.generation == s.head_epoch,
            ),
            Property::sometimes("paused writer resumes and is fenced", |_, s: &State| {
                s.fenced_resume
            }),
            Property::sometimes(
                "standby serves reads while another ingests",
                |_, s: &State| s.standby_read,
            ),
            Property::sometimes(
                "all source changes acknowledged across a takeover",
                |_, s: &State| s.acknowledged == CHANGES && s.fenced >= 2,
            ),
        ];
        if self.has("handoff") {
            p.extend([
                Property::always(
                    "no data write below the head's generation",
                    |_, s: &State| !s.stale_data,
                ),
                Property::always(
                    "a handed-over lease is taken without a lapse",
                    |_, s: &State| s.handoff_wait.is_none_or(|wait| wait == 0),
                ),
                Property::sometimes(
                    "the instance handed the lease writes data under it",
                    |_, s: &State| s.handed_write,
                ),
                Property::sometimes(
                    "a handoff completes and the new holder commits",
                    |_, s: &State| {
                        (s.handed_over || s.handoffs > 0 && s.fenced >= 2)
                            && s.committers.iter().any(|&(g, _)| g >= 2)
                    },
                ),
            ]);
        }
        if self.has("retake") {
            p.push(Property::sometimes(
                "a holder takes its head over again and commits",
                |_, s: &State| {
                    s.retakes > 0
                        && s.committers
                            .iter()
                            .any(|&(g, _)| g == s.head.generation && g >= 2)
                },
            ));
        }
        if self.timed() {
            p.extend([
                Property::always("lease exclusive until expiry", |_, s: &State| {
                    s.instances
                        .iter()
                        .filter(|n| n.status != Status::Crashed && n.epoch > 0 && n.left > 0)
                        .count()
                        <= 1
                }),
                Property::always("healthy holder keeps its lease", |_, s: &State| !s.churn),
                Property::always(
                    "ingesting again within the lease duration",
                    |_, s: &State| s.waiting <= DURATION,
                ),
                Property::sometimes("outage lasts the full lease duration", |_, s: &State| {
                    s.waiting == DURATION
                }),
            ]);
        } else {
            p.push(Property::sometimes(
                "two instances ingest on unexpired leases",
                |_, s: &State| {
                    s.instances
                        .iter()
                        .filter(|n| n.status == Status::Running && n.generation > 0 && n.left > 0)
                        .count()
                        >= 2
                },
            ));
        }
        p
    }
}

/// What one action of a standby trace did, as a replay checks it against the
/// implementation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepKind {
    Tick,
    /// The instance acquired the lease at this generation.
    Acquire {
        instance: usize,
        epoch: usize,
    },
    /// The instance renewed its lease, or (`renewed: false`) found another
    /// instance holds it and stepped down.
    Renew {
        instance: usize,
        renewed: bool,
    },
    /// The instance took the head over at `generation`, or was refused.
    Takeover {
        instance: usize,
        accepted: bool,
        generation: usize,
    },
    /// The instance takes its head over again: under lease generation
    /// `epoch` (`None` in the `split` control, which leaves the lease as is).
    Retake {
        instance: usize,
        epoch: Option<usize>,
    },
    /// The holder handed its lease over: to `to`, or (`None`, the `unnamed`
    /// control) to no one.
    Handoff {
        instance: usize,
        to: Option<usize>,
    },
    /// The holder stopped writing (the `release-first` control).
    StopWriting {
        instance: usize,
    },
    /// The instance wrote a data object outside a commit.
    DataWrite {
        instance: usize,
    },
    /// The instance prepared a commit of source change `change`.
    Read {
        instance: usize,
        change: usize,
    },
    /// The instance's prepared commit reached the head.
    Commit {
        instance: usize,
        outcome: CommitOutcome,
    },
    Ack {
        instance: usize,
    },
    Crash {
        instance: usize,
    },
    Restart {
        instance: usize,
    },
    Pause {
        instance: usize,
    },
    Resume {
        instance: usize,
    },
    Serve {
        instance: usize,
    },
}

/// How a prepared commit fared at the head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Accepted,
    /// Refused because the head moved under the same generation; the writer
    /// reads the head again.
    Rebase,
    /// Refused by a newer generation; the writer steps down.
    Fenced,
    /// Refused after the instance had already moved on from the generation
    /// that prepared it.
    Stale,
}

/// One step of a standby trace: what the action did and the state after it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub kind: StepKind,
    pub lease_epoch: usize,
    pub head_generation: usize,
    pub rows: Vec<usize>,
}

fn parse(text: &str) -> Option<Action> {
    let (name, arg) = match text.split_once('(') {
        Some((name, rest)) => (name, Some(rest.strip_suffix(')')?.trim().parse().ok()?)),
        None => (text, None),
    };
    Some(match (name, arg) {
        ("Tick", None) => Action::Tick,
        ("Acquire", Some(i)) => Action::Acquire(i),
        ("Renew", Some(i)) => Action::Renew(i),
        ("Takeover", Some(i)) => Action::Takeover(i),
        ("Retake", Some(i)) => Action::Retake(i),
        ("Handoff", Some(i)) => Action::Handoff(i),
        ("StopWriting", Some(i)) => Action::StopWriting(i),
        ("DataWrite", Some(i)) => Action::DataWrite(i),
        ("Read", Some(i)) => Action::Read(i),
        ("Commit", Some(i)) => Action::Commit(i),
        ("Ack", Some(i)) => Action::Ack(i),
        ("Crash", Some(i)) => Action::Crash(i),
        ("Restart", Some(i)) => Action::Restart(i),
        ("Pause", Some(i)) => Action::Pause(i),
        ("Resume", Some(i)) => Action::Resume(i),
        ("Serve", Some(i)) => Action::Serve(i),
        _ => return None,
    })
}

/// Step the model through `actions` (as a trace prints them) and say what
/// each did. `None` when an action does not parse or is not enabled.
#[must_use]
pub fn steps(mode: &str, actions: &[String]) -> Option<Vec<Step>> {
    let protocol = Protocol::new(mode);
    let mut state = protocol.init_states().pop()?;
    let mut out = Vec::with_capacity(actions.len());
    for text in actions {
        let action = parse(text)?;
        let next = protocol.next_state(&state, action.clone())?;
        let kind = match action {
            Action::Tick => StepKind::Tick,
            Action::Acquire(i) => StepKind::Acquire {
                instance: i,
                epoch: next.lease.epoch,
            },
            Action::Renew(i) => StepKind::Renew {
                instance: i,
                renewed: next.instances[i].epoch != 0,
            },
            Action::Takeover(i) => StepKind::Takeover {
                instance: i,
                accepted: next.instances[i].generation != 0,
                generation: next.instances[i].generation,
            },
            Action::Retake(i) => StepKind::Retake {
                instance: i,
                epoch: (!protocol.has("split")).then_some(next.lease.epoch),
            },
            Action::Handoff(i) => StepKind::Handoff {
                instance: i,
                to: next.lease.handoff,
            },
            Action::StopWriting(i) => StepKind::StopWriting { instance: i },
            Action::DataWrite(i) => StepKind::DataWrite { instance: i },
            Action::Read(i) => StepKind::Read {
                instance: i,
                change: next.instances[i].pending?.change,
            },
            Action::Commit(i) => StepKind::Commit {
                instance: i,
                outcome: if next.head.pos > state.head.pos {
                    CommitOutcome::Accepted
                } else if next.instances[i].generation == 0 && state.instances[i].generation != 0 {
                    CommitOutcome::Fenced
                } else if next.instances[i].base != state.instances[i].base {
                    CommitOutcome::Rebase
                } else {
                    CommitOutcome::Stale
                },
            },
            Action::Ack(i) => StepKind::Ack { instance: i },
            Action::Crash(i) => StepKind::Crash { instance: i },
            Action::Restart(i) => StepKind::Restart { instance: i },
            Action::Pause(i) => StepKind::Pause { instance: i },
            Action::Resume(i) => StepKind::Resume { instance: i },
            Action::Serve(i) => StepKind::Serve { instance: i },
        };
        out.push(Step {
            kind,
            lease_epoch: next.lease.epoch,
            head_generation: next.head.generation,
            rows: next.rows.clone(),
        });
        state = next;
    }
    Some(out)
}

#[cfg(test)]
mod explorer_tests {
    use super::{Action, Protocol, State};
    use stateright::{Model, Path};
    use std::collections::{HashMap, HashSet, VecDeque};

    /// The takeover label follows the mode: only with head fencing does it
    /// raise the generation.
    #[test]
    fn takeover_label_follows_the_mode() {
        assert_eq!(
            Protocol::new("standby").format_action(&Action::Takeover(1)),
            "instance 1 takes over the head (raises its generation)"
        );
        assert_eq!(
            Protocol::new("standby-no-generation").format_action(&Action::Takeover(1)),
            "instance 1 takes over (lease object only; the head generation is not raised)"
        );
    }

    /// With fencing by the lease object alone, a takeover succeeds without
    /// raising the head's generation; the timeline must still call it a
    /// takeover.
    #[test]
    fn lease_only_takeover_is_drawn_as_a_takeover() {
        let model = Protocol::new("standby-no-generation");
        let init = model.init_states().remove(0);
        let mut parent: HashMap<State, (State, Action)> = HashMap::new();
        let mut seen = HashSet::from([init.clone()]);
        let mut queue = VecDeque::from([init.clone()]);
        let mut found = None;
        'search: while let Some(state) = queue.pop_front() {
            let mut candidates = vec![];
            model.actions(&state, &mut candidates);
            for action in candidates {
                let Some(next) = model.next_state(&state, action.clone()) else {
                    continue;
                };
                if let Action::Takeover(i) = action
                    && next.instances[i].generation != 0
                {
                    let mut actions = vec![action];
                    let mut at = state;
                    while let Some((before, a)) = parent.get(&at) {
                        actions.push(a.clone());
                        at = before.clone();
                    }
                    actions.reverse();
                    found = Some(actions);
                    break 'search;
                }
                if seen.insert(next.clone()) {
                    parent.insert(next.clone(), (state.clone(), action));
                    queue.push_back(next);
                }
            }
        }
        let actions = found.expect("a successful takeover");
        let path = Path::from_actions(&model, init, &actions).expect("the path replays");
        let svg = model.as_svg(path).expect("standby draws");
        assert!(!svg.contains("takeover rejected"), "{actions:?}");
        assert!(
            svg.contains("takeover: ingester at generation"),
            "{actions:?}"
        );
    }
}
