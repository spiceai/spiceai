// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! A source transaction over datasets A and B committed without two-phase
//! commit by the one writer that holds both writer leases: A's record carries
//! A's actions, the source position and a pending intent with B's actions; B's
//! record applies the intent under the transaction id; an optional marker in A
//! records the intent done. Whoever next holds both leases rolls a pending
//! intent forward. Maintenance moves either head; requests are delayed,
//! duplicated and lose their responses; writers crash, pause and are fenced.
use stateright::{Model, Property};

/// The value a second writer stores in a key.
const OTHER: u8 = 9;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Head {
    pos: u8,
    lease: u8,
}
/// A log record. `epoch` and `at` (the base position) are the attempt id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Rec {
    /// A's actions for `tx`, the source position `tx` and, unless the control
    /// removes it, the pending intent planned against B position `intent`.
    Txn {
        tx: u8,
        epoch: u8,
        at: u8,
        intent: Option<u8>,
    },
    /// B's actions for `tx`, applied under the transaction id.
    Apply { tx: u8, epoch: u8, at: u8 },
    /// The intent for `tx` is applied to B.
    Done { tx: u8, epoch: u8, at: u8 },
    /// Control only: recovery undoes A's actions and position for `tx`.
    Abort { tx: u8, epoch: u8, at: u8 },
    /// Compaction or another maintenance commit: no keyed change.
    Maint,
    /// A keyed write by a second writer on B.
    Other { key: u8 },
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Dataset {
    log: Vec<Rec>,
    lease: u8,
}
impl Dataset {
    fn head(&self) -> Head {
        Head {
            pos: u8::try_from(self.log.len()).unwrap_or(u8::MAX),
            lease: self.lease,
        }
    }
    fn len(&self) -> u8 {
        self.head().pos
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    /// Acquire A's lease (0), then B's (1).
    Acquire(u8),
    /// Scan A for pending intents and roll each forward.
    Recover,
    Ready,
    ApplyB {
        tx: u8,
        plan: u8,
        recovering: bool,
    },
    /// Optionally record the intent done in A.
    MarkDone {
        tx: u8,
        recovering: bool,
    },
    /// B's lost race was a keyed conflict; the intent cannot be rebased.
    Blocked,
    /// The writer saw a newer lease and stopped.
    Fenced,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Writer {
    epoch: u8,
    alive: bool,
    phase: Phase,
    active: Option<u8>,
    /// Why B's last CAS lost: bit 0 maintenance, bit 1 a disjoint writer.
    lost: u8,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Req {
    ds: u8,
    base: Head,
    rec: Rec,
    /// B's position the transaction was planned against.
    plan: u8,
    sent: u8,
    done: u8,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    ds: [Dataset; 2],
    writers: Vec<Writer>,
    reqs: Vec<Req>,
    acknowledged: u8,
    clock: u8,
    /// Commit points: A's acceptance of each transaction, then the other write.
    stamps: [u8; 3],
    crashes: u8,
    maintained: [bool; 2],
    other: bool,
    seen_in_a: u8,
    retracted: bool,
    bad_resolution: bool,
    crashed_between: bool,
    recovered_apply: bool,
    rebased: bool,
    disjoint_rebased: bool,
    resolved_by_id: bool,
    paused_fenced: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Ingest(usize),
    IssueApply(usize),
    IssueDone(usize),
    SkipDone(usize),
    Recover(usize),
    Acquire(usize),
    Check(usize),
    Deliver(usize, usize),
    Crash(usize),
    Takeover,
    Maintain(usize),
    OtherWrite,
}
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bound: u8,
    /// Lease turnovers: each new owner pauses, fences or replaces the last.
    takeovers: usize,
}
fn in_a(log: &[Rec], tx: u8) -> bool {
    let mut present = false;
    for r in log {
        match *r {
            Rec::Txn { tx: t, .. } if t == tx => present = true,
            Rec::Abort { tx: t, .. } if t == tx => present = false,
            _ => (),
        }
    }
    present
}
fn applies(log: &[Rec], tx: u8) -> usize {
    log.iter()
        .filter(|r| matches!(r, Rec::Apply { tx: t, .. } if *t == tx))
        .count()
}
/// The source position recorded in A: the last transaction record, rolled
/// back by an abort.
fn position(log: &[Rec]) -> u8 {
    let mut p = 0;
    for r in log {
        match *r {
            Rec::Txn { tx, .. } => p = tx,
            Rec::Abort { tx, .. } => p = tx - 1,
            _ => (),
        }
    }
    p
}
/// Intents in A with no done marker, with the B position each was planned at.
fn pending(log: &[Rec]) -> Vec<(u8, u8)> {
    log.iter()
        .filter_map(|r| match *r {
            Rec::Txn {
                tx,
                intent: Some(plan),
                ..
            } if in_a(log, tx)
                && !log
                    .iter()
                    .any(|d| matches!(d, Rec::Done { tx: t, .. } if *t == tx)) =>
            {
                Some((tx, plan))
            }
            _ => None,
        })
        .collect()
}
/// B's value for key 0, which every source transaction writes.
fn b_value(log: &[Rec]) -> Option<u8> {
    let mut v = None;
    for r in log {
        match *r {
            Rec::Apply { tx, .. } => v = Some(tx),
            Rec::Other { key: 0 } => v = Some(OTHER),
            _ => (),
        }
    }
    v
}
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bound: u8::try_from(bound).unwrap_or(u8::MAX),
            takeovers: if mode.contains("two-takeovers") { 2 } else { 1 },
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    fn other_key(&self) -> u8 {
        u8::from(self.has("disjoint-writer"))
    }
    fn holds(s: &State, w: Writer, ds: usize) -> bool {
        s.ds[ds].lease == w.epoch
    }
    fn issue(next: &mut State, w: usize, ds: usize, rec: Rec, plan: u8) -> Option<()> {
        let base = next.ds[ds].head();
        next.reqs.push(Req {
            ds: u8::try_from(ds).ok()?,
            base,
            rec,
            plan,
            sent: 1,
            done: 0,
        });
        next.writers[w].active = Some(u8::try_from(next.reqs.len() - 1).ok()?);
        Some(())
    }
    /// Drops requests no writer is resolving that can never be accepted: a
    /// head's position and lease never decrease, so once it differs from a
    /// request's base, every later delivery of that request is a no-op.
    fn forget_dead_requests(next: &mut State) -> Option<()> {
        let mut keep = Vec::with_capacity(next.reqs.len());
        let mut remap = vec![None; next.reqs.len()];
        for (i, r) in next.reqs.iter().enumerate() {
            let index = u8::try_from(i).ok()?;
            let active = next.writers.iter().any(|w| w.active == Some(index));
            let live = r.sent & !r.done != 0 && next.ds[usize::from(r.ds)].head() == r.base;
            if active || live {
                remap[i] = Some(u8::try_from(keep.len()).ok()?);
                keep.push(*r);
            }
        }
        for w in &mut next.writers {
            w.active = w.active.and_then(|i| remap[usize::from(i)]);
        }
        next.reqs = keep;
        Some(())
    }
    /// Whether B records after the intent's plan intersect its keys.
    fn conflicts(log: &[Rec], tx: u8, plan: u8) -> bool {
        log[usize::from(plan)..].iter().any(|r| match *r {
            Rec::Apply { tx: t, .. } => t != tx,
            Rec::Other { key } => key == 0,
            _ => false,
        })
    }
    /// Recovery has finished and no protocol step remains: the current owner
    /// is alive with both leases, and only environment faults are enabled.
    fn quiescent(&self, s: &State) -> bool {
        let Some(owner) = s.writers.last() else {
            return false;
        };
        if !owner.alive || !Self::holds(s, *owner, 0) || !Self::holds(s, *owner, 1) {
            return false;
        }
        let mut listed = vec![];
        self.actions(s, &mut listed);
        !listed.into_iter().any(|a| {
            !matches!(
                a,
                Action::Crash(_) | Action::Takeover | Action::Maintain(_) | Action::OtherWrite
            ) && self.next_state(s, a).is_some()
        })
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        vec![State {
            ds: [
                Dataset {
                    log: vec![],
                    lease: 1,
                },
                Dataset {
                    log: vec![],
                    lease: 1,
                },
            ],
            writers: vec![Writer {
                epoch: 1,
                alive: true,
                phase: Phase::Ready,
                active: None,
                lost: 0,
            }],
            ..State::default()
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for (w, writer) in s.writers.iter().enumerate() {
            if !writer.alive {
                continue;
            }
            if writer.active.is_some() {
                a.push(Action::Check(w));
                continue;
            }
            match writer.phase {
                Phase::Ready => {
                    a.push(Action::Ingest(w));
                    if self.has("ingest-before-recovery") {
                        a.push(Action::Recover(w));
                    }
                }
                Phase::Recover => a.push(Action::Recover(w)),
                Phase::Acquire(_) => a.push(Action::Acquire(w)),
                Phase::ApplyB { .. } => a.push(Action::IssueApply(w)),
                Phase::MarkDone { .. } => a.extend([Action::IssueDone(w), Action::SkipDone(w)]),
                Phase::Blocked | Phase::Fenced => (),
            }
            if usize::from(s.crashes) < self.takeovers {
                a.push(Action::Crash(w));
            }
        }
        for (i, r) in s.reqs.iter().enumerate() {
            for q in 0..2 {
                if r.sent & (1 << q) != 0 && r.done & (1 << q) == 0 {
                    a.push(Action::Deliver(i, q));
                }
            }
        }
        if s.writers.len() <= self.takeovers {
            a.push(Action::Takeover);
        }
        for ds in 0..2 {
            if !s.maintained[ds] {
                a.push(Action::Maintain(ds));
            }
        }
        if (self.has("second-writer") || self.has("disjoint-writer")) && !s.other {
            a.push(Action::OtherWrite);
        }
        crate::prune_for_simulation(self, s, a);
    }
    #[expect(
        clippy::too_many_lines,
        reason = "One match arm per protocol step keeps the transitions in one place"
    )]
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        let mut next = s.clone();
        match action {
            Action::Ingest(w) => {
                let writer = s.writers[w];
                if !writer.alive || writer.active.is_some() || writer.phase != Phase::Ready {
                    return None;
                }
                if !Self::holds(s, writer, 0) || !Self::holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                // Redelivery resumes after the position in A; the source keeps
                // only what it has not acknowledged.
                let tx = position(&s.ds[0].log) + 1;
                if tx > self.bound || tx <= s.acknowledged {
                    return None;
                }
                let plan = s.ds[1].len();
                let rec = Rec::Txn {
                    tx,
                    epoch: writer.epoch,
                    at: s.ds[0].len(),
                    intent: (!self.has("no-intent")).then_some(plan),
                };
                Self::issue(&mut next, w, 0, rec, plan)?;
            }
            Action::IssueApply(w) => {
                let writer = s.writers[w];
                let Phase::ApplyB {
                    tx,
                    plan,
                    recovering,
                } = writer.phase
                else {
                    return None;
                };
                if !writer.alive || writer.active.is_some() {
                    return None;
                }
                if !Self::holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                let log = &s.ds[1].log;
                if !self.has("no-idempotency") && applies(log, tx) > 0 {
                    next.writers[w].phase = Phase::MarkDone { tx, recovering };
                    return Some(next);
                }
                if Self::conflicts(log, tx, plan) && !self.has("blind-rebase") {
                    next.writers[w].phase = Phase::Blocked;
                    return Some(next);
                }
                let rec = Rec::Apply {
                    tx,
                    epoch: writer.epoch,
                    at: s.ds[1].len(),
                };
                Self::issue(&mut next, w, 1, rec, plan)?;
            }
            Action::IssueDone(w) => {
                let writer = s.writers[w];
                let Phase::MarkDone { tx, .. } = writer.phase else {
                    return None;
                };
                if !writer.alive || writer.active.is_some() {
                    return None;
                }
                if !Self::holds(s, writer, 0) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                let rec = Rec::Done {
                    tx,
                    epoch: writer.epoch,
                    at: s.ds[0].len(),
                };
                Self::issue(&mut next, w, 0, rec, 0)?;
            }
            Action::SkipDone(w) => {
                let writer = s.writers[w];
                let Phase::MarkDone { recovering, .. } = writer.phase else {
                    return None;
                };
                if !writer.alive || writer.active.is_some() {
                    return None;
                }
                next.writers[w].phase = if recovering {
                    Phase::Recover
                } else {
                    Phase::Ready
                };
            }
            Action::Recover(w) => {
                let writer = s.writers[w];
                if !writer.alive
                    || writer.active.is_some()
                    || !matches!(writer.phase, Phase::Recover | Phase::Ready)
                {
                    return None;
                }
                if !Self::holds(s, writer, 0) || !Self::holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                let found = pending(&s.ds[0].log)
                    .into_iter()
                    .find(|&(tx, _)| self.has("no-idempotency") || applies(&s.ds[1].log, tx) == 0);
                match found {
                    Some((tx, _)) if self.has("abort-recovery") => {
                        let rec = Rec::Abort {
                            tx,
                            epoch: writer.epoch,
                            at: s.ds[0].len(),
                        };
                        next.writers[w].phase = Phase::Recover;
                        Self::issue(&mut next, w, 0, rec, 0)?;
                    }
                    Some((tx, plan)) => {
                        next.writers[w].phase = Phase::ApplyB {
                            tx,
                            plan,
                            recovering: true,
                        };
                    }
                    None if writer.phase == Phase::Recover => {
                        next.writers[w].phase = Phase::Ready;
                    }
                    None => return None,
                }
            }
            Action::Acquire(w) => {
                let writer = s.writers[w];
                let Phase::Acquire(k) = writer.phase else {
                    return None;
                };
                if !writer.alive {
                    return None;
                }
                let ds = &mut next.ds[usize::from(k)];
                if ds.lease >= writer.epoch {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                ds.lease = writer.epoch;
                next.writers[w].phase = if k == 0 {
                    Phase::Acquire(1)
                } else if self.has("ingest-before-recovery") {
                    Phase::Ready
                } else {
                    Phase::Recover
                };
            }
            Action::Check(w) => {
                let writer = s.writers[w];
                let i = usize::from(writer.active?);
                if !writer.alive {
                    return None;
                }
                let r = s.reqs[i];
                let ds = &s.ds[usize::from(r.ds)];
                let head = ds.head();
                let evidence = ds.log.get(usize::from(r.base.pos)).map(|x| *x == r.rec);
                let answer = crate::core::resolve_attempt(evidence, head == r.base);
                let accepted = ds.log.get(usize::from(r.base.pos)) == Some(&r.rec);
                next.bad_resolution |= answer == 1 && !accepted || answer == 2 && accepted;
                match answer {
                    0 => {
                        // Unknown: resend the identical request.
                        next.reqs[i].sent |= 2;
                        return (next != *s).then_some(next);
                    }
                    1 => {
                        next.writers[w].active = None;
                        // Another commit followed this one, so the head alone
                        // no longer says whose record landed at the base.
                        next.resolved_by_id |=
                            head.pos > r.base.pos + 1 || head.lease != r.base.lease;
                        next.writers[w].phase = match (r.rec, writer.phase) {
                            (Rec::Txn { tx, .. }, _) => {
                                // The position is durable in A: acknowledge.
                                next.acknowledged = next.acknowledged.max(tx);
                                Phase::ApplyB {
                                    tx,
                                    plan: r.plan,
                                    recovering: false,
                                }
                            }
                            (Rec::Apply { tx, .. }, Phase::ApplyB { recovering, .. }) => {
                                next.recovered_apply |= recovering && s.crashed_between;
                                next.rebased |= writer.lost & 1 != 0;
                                next.disjoint_rebased |= writer.lost & 2 != 0;
                                next.writers[w].lost = 0;
                                Phase::MarkDone { tx, recovering }
                            }
                            (
                                Rec::Done { .. },
                                Phase::MarkDone {
                                    recovering: true, ..
                                },
                            )
                            | (Rec::Abort { .. }, _) => Phase::Recover,
                            (Rec::Done { .. }, _) => Phase::Ready,
                            _ => return None,
                        };
                    }
                    _ => {
                        next.writers[w].active = None;
                        if ds.lease != writer.epoch {
                            next.writers[w].phase = Phase::Fenced;
                            next.paused_fenced = true;
                        } else if matches!(r.rec, Rec::Apply { .. }) {
                            for x in &ds.log[usize::from(r.base.pos)..] {
                                match x {
                                    Rec::Maint => next.writers[w].lost |= 1,
                                    Rec::Other { .. } => next.writers[w].lost |= 2,
                                    _ => (),
                                }
                            }
                        }
                        // Otherwise the phase re-plans against the new head.
                    }
                }
            }
            Action::Deliver(i, q) => {
                let r = s.reqs[i];
                if r.sent & (1 << q) == 0 || r.done & (1 << q) != 0 {
                    return None;
                }
                next.reqs[i].done |= 1 << q;
                let ds = &mut next.ds[usize::from(r.ds)];
                if ds.head() == r.base {
                    ds.log.push(r.rec);
                    match r.rec {
                        Rec::Txn { tx, .. } => {
                            next.clock += 1;
                            next.stamps[usize::from(tx - 1)] = next.clock;
                            next.seen_in_a |= 1 << tx;
                        }
                        Rec::Abort { tx, .. } => {
                            next.retracted |= s.seen_in_a & (1 << tx) != 0;
                        }
                        _ => (),
                    }
                }
            }
            Action::Crash(w) => {
                if usize::from(s.crashes) >= self.takeovers || !s.writers[w].alive {
                    return None;
                }
                next.crashes += 1;
                next.writers[w].alive = false;
                next.crashed_between |= (1..=self.bound)
                    .any(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 0);
            }
            Action::Takeover => {
                if s.writers.len() > self.takeovers {
                    return None;
                }
                next.writers.push(Writer {
                    epoch: u8::try_from(s.writers.len() + 1).ok()?,
                    alive: true,
                    phase: Phase::Acquire(0),
                    active: None,
                    lost: 0,
                });
            }
            Action::Maintain(ds) => {
                if s.maintained[ds] {
                    return None;
                }
                next.maintained[ds] = true;
                next.ds[ds].log.push(Rec::Maint);
            }
            Action::OtherWrite => {
                if s.other || !(self.has("second-writer") || self.has("disjoint-writer")) {
                    return None;
                }
                next.other = true;
                next.clock += 1;
                next.stamps[2] = next.clock;
                next.ds[1].log.push(Rec::Other {
                    key: self.other_key(),
                });
            }
        }
        Self::forget_dead_requests(&mut next)?;
        (next != *s).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "eventual atomicity at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || (1..=model.bound)
                            .all(|tx| in_a(&s.ds[0].log, tx) == (applies(&s.ds[1].log, tx) > 0))
                },
            ),
            Property::always(
                "B applies each transaction at most once",
                |model: &Self, s: &State| {
                    (1..=model.bound).all(|tx| applies(&s.ds[1].log, tx) <= 1)
                },
            ),
            Property::always(
                "acknowledged transactions in both datasets at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || (1..=s.acknowledged)
                            .all(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) > 0)
                },
            ),
            Property::always(
                "source position durable in both datasets at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || (1..=position(&s.ds[0].log))
                            .all(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) > 0)
                },
            ),
            Property::always(
                "I2 source acknowledgement covered by A's position",
                |_, s: &State| s.acknowledged <= position(&s.ds[0].log),
            ),
            Property::always(
                "A's committed transactions never retracted",
                |_, s: &State| !s.retracted,
            ),
            Property::always(
                "B keyed values follow commit order",
                |model: &Self, s: &State| {
                    // The oracle orders B's writers by their commit points: A's
                    // acceptance for a source transaction, its own CAS for the
                    // second writer. B's value must be the latest applied one.
                    let log = &s.ds[1].log;
                    let mut latest: Option<(u8, u8)> = None;
                    for tx in 1..=model.bound {
                        if applies(log, tx) > 0 {
                            let stamp = s.stamps[usize::from(tx - 1)];
                            if latest.is_none_or(|(l, _)| stamp > l) {
                                latest = Some((stamp, tx));
                            }
                        }
                    }
                    if log.contains(&Rec::Other { key: 0 })
                        && latest.is_none_or(|(l, _)| s.stamps[2] > l)
                    {
                        latest = Some((s.stamps[2], OTHER));
                    }
                    b_value(log) == latest.map(|(_, v)| v)
                },
            ),
            Property::always("I5 attempt-id resolution sound", |_, s: &State| {
                !s.bad_resolution
            }),
            Property::sometimes(
                "crash between A and B then recovery completes B",
                |_, s: &State| s.recovered_apply,
            ),
            Property::sometimes("B CAS lost to maintenance and retried", |_, s: &State| {
                s.rebased
            }),
            Property::sometimes(
                "B CAS lost to disjoint writer and rebased",
                |_, s: &State| s.disjoint_rebased,
            ),
            Property::sometimes("lost response resolved by attempt id", |_, s: &State| {
                s.resolved_by_id
            }),
            Property::sometimes("paused writer's request fenced", |_, s: &State| {
                s.paused_fenced
            }),
            Property::sometimes("reader sees A before B", |model: &Self, s: &State| {
                (1..=model.bound).any(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 0)
            }),
            Property::sometimes(
                "two transactions applied to both",
                |model: &Self, s: &State| {
                    model.bound >= 2
                        && (1..=2)
                            .all(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 1)
                },
            ),
        ]
    }
}
