// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Instances running the same Spicepod on one location: a lease object with an
//! expiry on a discrete clock, renewal by the holder, crash, pause and resume,
//! and takeover by a head commit that raises the writer generation. Only the
//! generation in the head fences writers; lease timing decides availability.
//!
//! Every instance serves reads: a writer from its local table, which includes
//! the changes it applied but has not committed, and any other instance from
//! the committed head. A writer discards that uncommitted buffer the moment it
//! is demoted, so a former writer serves only committed rows.
//!
//! The `partition` modes cut one instance off from the location but not from
//! the source: it cannot renew or commit, and it keeps the source's single
//! exclusive consumer (a replication slot is active for one connection), which
//! an instance needs to ingest. An instance releases the consumer when it
//! crashes, when contact with the location tells it it was fenced, and, under
//! the design rule, when its own lease has expired by its own clock while it
//! cannot renew (self-fencing).
//!
//! The consumer property checks that the new lease holder is not blocked from
//! the consumer, not that it makes ingestion progress. A paused (frozen)
//! partitioned holder is excluded: source-side consumer timeout is not modeled.
use stateright::{Model, Property};

/// Ticks a lease lasts after acquisition or renewal.
const DURATION: usize = 3;
/// Source changes available to ingest.
const CHANGES: usize = 2;
/// Lease acquisitions are bounded so churning controls stay finite. Safe runs
/// never reach it: each acquisition there follows a crash or a pause.
const EPOCHS: usize = 4;
/// Ticks a new lease holder may wait for the source consumer once it is the
/// ingester. The model's clocks do not drift, so the design never waits a tick;
/// the grace stands for the clock skew a real deployment allows.
const CONSUMER_GRACE: usize = 1;
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
    /// Changes it applied to its local table but has not committed.
    buffer: Vec<usize>,
    /// It discarded a nonempty uncommitted buffer when it was demoted.
    discarded: bool,
    /// Cut off from the location (head and lease object) but not the source.
    partitioned: bool,
    /// Generation at partition time, only if it held the source consumer.
    partitioned_consumer_generation: Option<usize>,
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
    /// A read showed a row twice or rows that are not a source prefix.
    duplicate_read: bool,
    /// An instance without a writer generation read rows not in the head.
    uncommitted_read: bool,
    /// A writer read its own uncommitted change.
    writer_read: bool,
    /// An instance that discarded its buffer at demotion served a read.
    demoted_read: bool,
    /// Partition modes: the instance holding the source's exclusive consumer.
    consumer: Option<usize>,
    /// Ticks during which the ingester could not attach the source consumer.
    blocked: usize,
    partitions: u8,
    /// A partitioned writer released the consumer at its own lease expiry.
    self_fenced: bool,
    /// A new writer committed while the old one was partitioned.
    partition_commit: bool,
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
    Partition(usize),
    Heal(usize),
    SelfFence(usize),
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
    /// An ingester that can commit and can attach the source consumer.
    /// Outside the partition modes no instance is partitioned and no consumer
    /// is held, so this is any ingester.
    fn ingesting(s: &State) -> bool {
        (0..s.instances.len()).any(|i| {
            Self::ingester(s, i) && !s.instances[i].partitioned && s.consumer.is_none_or(|c| c == i)
        })
    }
    /// Whether instance `i` releases the consumer at its own lease expiry.
    fn self_fences(&self, s: &State, i: usize) -> bool {
        let n = &s.instances[i];
        !self.has("no-self-fence")
            && n.status == Status::Running
            && n.partitioned
            && n.epoch > 0
            && n.left == 0
    }
    /// What instance `i` serves: a writer its local table (its committed
    /// rows and its uncommitted buffer); any other instance the committed head,
    /// plus whatever buffer it kept.
    fn view(s: &State, i: usize) -> Vec<usize> {
        let n = &s.instances[i];
        let mut v = match n.base {
            Some(b) if n.generation != 0 => s.rows.get(..b.pos).unwrap_or(&s.rows).to_vec(),
            _ => s.rows.clone(),
        };
        v.extend(&n.buffer);
        v
    }
    /// An action no tick may overtake: a due renewal, an acquisition of an
    /// expired lease, or a takeover commit after acquisition.
    /// A partitioned instance can do none of these; under the design rule its
    /// self-fencing at expiry is urgent instead.
    fn urgent(&self, s: &State) -> bool {
        s.instances.iter().enumerate().any(|(i, n)| {
            self.self_fences(s, i)
                || n.status == Status::Running
                    && !n.partitioned
                    && (n.epoch > 0 && n.left <= 1 && !self.has("no-renew")
                        || n.epoch > 0 && n.generation == 0
                        || s.lease.left == 0 && n.epoch != s.lease.epoch && s.lease.epoch < EPOCHS)
        })
    }
    /// Instance `i` stops writing: it forgets its lease and generation,
    /// discards its uncommitted buffer (the current-code control keeps it),
    /// and releases the source consumer.
    fn step_down(&self, s: &mut State, i: usize) {
        let n = &mut s.instances[i];
        n.epoch = 0;
        n.left = 0;
        n.generation = 0;
        n.base = None;
        if !self.has("keeps-buffer") && !n.buffer.is_empty() {
            n.buffer.clear();
            n.discarded = true;
        }
        Self::release(s, i);
    }
    /// Instance `i` detaches the source consumer; an acknowledgement it still
    /// owed is lost with it, and the next consumer resumes from the source's
    /// acknowledged position.
    fn release(s: &mut State, i: usize) {
        if s.consumer == Some(i) {
            s.consumer = None;
            s.instances[i].to_ack = None;
        }
    }
    /// Reads, resumption and the partition steps of `next_state`, applied to
    /// `next`; `None` when `action` is disabled in `s`.
    fn serve_or_partition(&self, s: &State, next: &mut State, action: &Action) -> Option<()> {
        match *action {
            Action::Resume(i) => {
                if s.instances[i].status != Status::Paused {
                    return None;
                }
                next.instances[i].status = Status::Running;
                next.instances[i].resumed = true;
            }
            Action::Serve(i) => {
                let n = &s.instances[i];
                // A writer serves its local table; any other instance reads the head.
                let from_head = n.generation == 0 || n.base.is_none();
                if s.instances[i].status != Status::Running || from_head && n.partitioned {
                    return None;
                }
                let v = Self::view(s, i);
                if !Self::ingester(s, i) && Self::ingesting(s) {
                    next.standby_read = true;
                }
                next.bad_read |= s.rows != (1..=s.head.pos).collect::<Vec<_>>();
                next.duplicate_read |= v != (1..=v.len()).collect::<Vec<_>>();
                next.uncommitted_read |= from_head && v.len() > s.head.pos;
                next.writer_read |= !from_head && !n.buffer.is_empty();
                next.demoted_read |= from_head && n.discarded;
                if from_head {
                    next.unreadable_file |= s.local_files.iter().any(|&(_, o)| o != i);
                }
            }
            Action::Partition(i) => {
                if s.instances[i].status != Status::Running
                    || s.instances[i].partitioned
                    || s.partitions >= 1
                {
                    return None;
                }
                if s.lease.holder == i && s.lease.epoch == s.instances[i].epoch {
                    next.lease.lost = 3;
                    next.waiting = 0;
                }
                next.partitions += 1;
                next.instances[i].partitioned = true;
                next.instances[i].partitioned_consumer_generation =
                    (s.consumer == Some(i)).then_some(s.instances[i].generation);
            }
            Action::Heal(i) => {
                let n = &s.instances[i];
                if !n.partitioned {
                    return None;
                }
                next.instances[i].partitioned = false;
                // Contact with the location: a lease or head generation that
                // moved on tells the instance it was fenced.
                if n.epoch != 0
                    && (s.lease.epoch != n.epoch
                        || n.generation != 0 && s.head.generation > n.generation)
                {
                    self.step_down(next, i);
                }
            }
            Action::SelfFence(i) => {
                if !self.self_fences(s, i) {
                    return None;
                }
                next.self_fenced |= s.consumer == Some(i);
                self.step_down(next, i);
            }
            _ => return None,
        }
        Some(())
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
            duplicate_read: false,
            uncommitted_read: false,
            writer_read: false,
            demoted_read: false,
            consumer: None,
            blocked: 0,
            partitions: 0,
            self_fenced: false,
            partition_commit: false,
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
            if self.has("partition") {
                a.extend([Action::Partition(i), Action::Heal(i), Action::SelfFence(i)]);
            }
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
                // Availability needs an instance that runs and reaches the location.
                if !Self::ingesting(s)
                    && (0..self.instances).any(|i| running(i) && !s.instances[i].partitioned)
                {
                    next.waiting = (s.waiting + 1).min(DURATION + 1);
                }
                let blocked = (0..self.instances).any(|i| {
                    Self::ingester(s, i)
                        && !s.instances[i].partitioned
                        && s.consumer.is_some_and(|c| c != i)
                });
                next.blocked = if blocked {
                    (s.blocked + 1).min(CONSUMER_GRACE + 1)
                } else {
                    0
                };
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
                    || n.partitioned
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
                self.step_down(&mut next, i);
                let m = &mut next.instances[i];
                m.epoch = s.lease.epoch + 1;
                m.left = DURATION;
                m.resumed = false;
            }
            Action::Renew(i) => {
                let n = &s.instances[i];
                if !running(i)
                    || n.partitioned
                    || n.epoch == 0
                    || n.left >= DURATION
                    || self.has("no-renew")
                {
                    return None;
                }
                if s.lease.epoch == n.epoch {
                    next.lease.left = DURATION;
                    next.lease.lost = 0;
                    next.instances[i].left = DURATION;
                } else {
                    // The conditional renewal fails: another instance holds the lease.
                    self.step_down(&mut next, i);
                }
            }
            Action::Takeover(i) => {
                let n = &s.instances[i];
                if !running(i) || n.partitioned || n.epoch == 0 || n.generation != 0 {
                    return None;
                }
                if self.has("no-generation") {
                    // Fencing by the lease object only: the head is read, not written.
                } else if s.head.generation >= n.epoch {
                    // The takeover CAS requires a lower generation in the head.
                    self.step_down(&mut next, i);
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
                if self.has("partition") {
                    // Ingesting needs the source's single exclusive consumer.
                    if s.consumer.is_some_and(|c| c != i) {
                        return None;
                    }
                    next.consumer = Some(i);
                }
                next.instances[i].pending = Some(Commit {
                    base,
                    generation: n.generation,
                    change: base.pos + 1,
                });
                // The change is applied to the local table before it commits.
                next.instances[i].buffer.push(base.pos + 1);
            }
            Action::Commit(i) => {
                let c = s.instances[i].pending?;
                if !running(i) || s.instances[i].partitioned {
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
                    next.partition_commit |= s.instances.iter().any(|n| {
                        n.partitioned
                            && n.partitioned_consumer_generation
                                .is_some_and(|generation| c.generation > generation)
                    });
                    // An instance acknowledges only through the consumer it reads with.
                    let owes = !self.has("partition") || s.consumer == Some(i);
                    let m = &mut next.instances[i];
                    m.base = Some(next.head);
                    m.to_ack = owes.then_some(c.change);
                    m.buffer.retain(|&b| b != c.change);
                } else if s.head.generation == c.generation
                    && s.instances[i].generation == c.generation
                {
                    // Same generation: rebase on the current head and re-read.
                    next.instances[i].base = Some(s.head);
                    next.instances[i].buffer.clear();
                } else if s.instances[i].generation == c.generation {
                    // A newer generation is in the head: this writer is fenced.
                    next.fenced_resume |= s.instances[i].resumed;
                    self.step_down(&mut next, i);
                }
                // Otherwise the request predates the instance's current lease;
                // its rejection says nothing about that lease.
            }
            Action::Ack(i) => {
                let c = s.instances[i].to_ack?;
                if !running(i) || self.has("partition") && s.consumer != Some(i) {
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
                Self::release(&mut next, i);
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
                // A paused process cannot release the consumer; the partition
                // modes leave the source's own timeout of a silent consumer out.
                if !running(i) || s.pauses >= 1 || self.has("partition") {
                    return None;
                }
                if s.lease.holder == i && s.lease.epoch == s.instances[i].epoch {
                    next.lease.lost = 2;
                    next.waiting = 0;
                }
                next.pauses += 1;
                next.instances[i].status = Status::Paused;
            }
            Action::Resume(_)
            | Action::Serve(_)
            | Action::Partition(_)
            | Action::Heal(_)
            | Action::SelfFence(_) => {
                self.serve_or_partition(s, &mut next, &action)?;
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
            Action::Takeover(i) => {
                format!("instance {i} takes over the head (raises its generation)")
            }
            Action::Read(i) => format!("instance {i} reads a source change and prepares a commit"),
            Action::Commit(i) => format!("instance {i}'s commit CAS arrives at the head"),
            Action::Ack(i) => format!("instance {i} acknowledges the source"),
            Action::Crash(i) => format!("instance {i} crashes"),
            Action::Restart(i) => format!("instance {i} restarts"),
            Action::Pause(i) => format!("instance {i} pauses (GC pause, VM freeze)"),
            Action::Resume(i) => format!("instance {i} resumes"),
            Action::Serve(i) => format!("instance {i} serves a read"),
            Action::Partition(i) => {
                format!("instance {i} is cut off from the location (it still reaches the source)")
            }
            Action::Heal(i) => format!("instance {i} reaches the location again"),
            Action::SelfFence(i) => format!(
                "instance {i}'s own lease expired while cut off: it releases the source consumer"
            ),
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
                "uncommitted buffers {:?}; source consumer {:?} (ingester blocked {} ticks)",
                s.instances
                    .iter()
                    .map(|n| n.buffer.clone())
                    .collect::<Vec<_>>(),
                s.consumer,
                s.blocked
            ),
            crate::explorer::flags(&[
                ("stale commit attempted", s.stale),
                ("lease churn", s.churn),
                ("takeover after crash", s.crash_takeover),
                ("paused writer resumed and was fenced", s.fenced_resume),
                ("standby served a read", s.standby_read),
                ("BAD read", s.bad_read),
                ("BAD read: duplicate or non-prefix rows", s.duplicate_read),
                (
                    "BAD read: uncommitted rows off the head",
                    s.uncommitted_read,
                ),
                ("writer read its uncommitted change", s.writer_read),
                ("demoted writer served committed rows", s.demoted_read),
                ("partitioned writer self-fenced", s.self_fenced),
                (
                    "committed while the old writer was partitioned",
                    s.partition_commit,
                ),
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
                Action::Serve(i) if before.instances[i].generation != 0 => {
                    Event::on(i, format!("serve local rows {:?}", Self::view(&before, i)))
                        .tone(Tone::Actor)
                }
                Action::Serve(i) => Event::on(
                    head,
                    format!("instance {i} serves {:?}", Self::view(&before, i)),
                )
                .to(i)
                .tone(Tone::Actor),
                Action::Partition(i) => Event::on(i, "CUT OFF from the location").tone(Tone::Bad),
                Action::Heal(i) => {
                    let fenced = after.instances[i].epoch == 0 && before.instances[i].epoch != 0;
                    Event::on(
                        i,
                        if fenced {
                            "reaches the location: fenced, steps down"
                        } else {
                            "reaches the location"
                        },
                    )
                    .to(lease)
                }
                Action::SelfFence(i) => {
                    Event::on(i, "own lease expired: release source consumer").tone(Tone::Gone)
                }
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
                if inst.partitioned {
                    lines.push("CUT OFF from the location".into());
                }
                if last.consumer == Some(i) {
                    lines.push("holds the source consumer".into());
                }
                if !inst.buffer.is_empty() {
                    lines.push(format!("uncommitted local rows {:?}", inst.buffer));
                }
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
                "every instance reads a source prefix, with no row twice",
                |_, s: &State| !s.duplicate_read,
            ),
            Property::always(
                "only a writer reads its uncommitted rows",
                |_, s: &State| !s.uncommitted_read,
            ),
            Property::always(
                "standbys and new owners read every committed file",
                |_, s: &State| !s.unreadable_file,
            ),
            Property::sometimes("takeover after holder crash", |_, s: &State| {
                s.crash_takeover
            }),
            Property::sometimes(
                "standby serves reads while another ingests",
                |_, s: &State| s.standby_read,
            ),
            Property::sometimes(
                "all source changes acknowledged across a takeover",
                |_, s: &State| s.acknowledged == CHANGES && s.fenced >= 2,
            ),
            Property::sometimes("writer reads its uncommitted change", |_, s: &State| {
                s.writer_read
            }),
            Property::sometimes(
                "demoted writer discards its buffer and serves committed rows",
                |_, s: &State| s.demoted_read,
            ),
        ];
        if self.has("partition") {
            p.extend([
                Property::always(
                    "new lease holder not blocked from the source consumer",
                    |_, s: &State| s.blocked <= CONSUMER_GRACE,
                ),
                Property::sometimes(
                    "partitioned writer releases the consumer at its lease expiry",
                    |_, s: &State| s.self_fenced,
                ),
                Property::sometimes(
                    "new writer commits while the old one is partitioned",
                    |_, s: &State| s.partition_commit,
                ),
            ]);
        } else {
            // Pausing is outside the partition graphs.
            p.push(Property::sometimes(
                "paused writer resumes and is fenced",
                |_, s: &State| s.fenced_resume,
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
