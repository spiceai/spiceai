// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Instances running the same Spicepod on one location: a lease object with an
//! expiry on a discrete clock, renewal by the holder, crash, pause and resume,
//! and takeover by a head commit that raises the writer generation. Only the
//! generation in the head fences writers; lease timing decides availability.
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
    /// How the holder failed since it acquired or renewed: 0 none, 1 crash, 2 pause.
    lost: u8,
    /// `lost` of the previous holder when this lease was acquired.
    cause: u8,
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
    /// A standby read or a takeover needed a file on another instance's disk.
    unreadable_file: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Tick,
    Acquire(usize),
    Renew(usize),
    Takeover(usize),
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
        s.instances.iter().any(|n| {
            n.status == Status::Running
                && (n.epoch > 0 && n.left <= 1 && !self.has("no-renew")
                    || n.epoch > 0 && n.generation == 0
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
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        a.push(Action::Tick);
        for i in 0..self.instances {
            a.extend([
                Action::Acquire(i),
                Action::Renew(i),
                Action::Takeover(i),
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
            }
            Action::Acquire(i) => {
                let n = &s.instances[i];
                // The lease object's conditional write admits one acquirer per
                // epoch, and only after expiry.
                if !running(i)
                    || n.epoch == s.lease.epoch
                    || s.lease.epoch >= EPOCHS
                    || s.lease.left > 0 && !self.has("early")
                {
                    return None;
                }
                next.churn |= s.lease.lost == 0;
                next.lease = Lease {
                    holder: i,
                    epoch: s.lease.epoch + 1,
                    left: DURATION,
                    lost: 0,
                    cause: s.lease.lost,
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
                if s.lease.epoch == n.epoch {
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
                if self.has("no-generation") {
                    // Fencing by the lease object only: the head is read, not written.
                } else if s.head.generation >= n.epoch {
                    // The takeover CAS requires a lower generation in the head.
                    Self::step_down(&mut next.instances[i]);
                    return Some(next);
                } else {
                    next.head.generation = n.epoch;
                }
                next.crash_takeover |= s.lease.epoch == n.epoch && s.lease.cause == 1;
                next.unreadable_file |= s.local_files.iter().any(|&(_, o)| o != i);
                next.fenced = n.epoch;
                let m = &mut next.instances[i];
                m.generation = n.epoch;
                m.base = Some(next.head);
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
