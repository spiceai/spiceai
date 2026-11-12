// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! The single-writer cross-dataset fast path of `intent_roll_forward` run by
//! one exclusive writer per source partition. Datasets A and B each have one
//! head shared by every partition; the head records each partition's writer
//! generation, and a partition turnover is a head commit on A and then on B.
//! Each source transaction writes only its partition's key in both datasets.
//! Each partition writer commits A's actions, its partition's source position
//! and a pending intent in one A record, then applies the intent to B under the
//! transaction id. Both writers race on both heads: a lost CAS is rebased after
//! checking that the winners' keys are disjoint from the intent's. Crashes,
//! pauses, turnovers and handoffs are independent per partition; a partition's
//! next owner rolls that partition's pending intents forward before ingesting
//! more for it, while the other partition keeps running.
use stateright::{Model, Property};

const PARTS: usize = 2;
/// Source transactions per partition the state arrays have room for.
const MAX_SEQ: usize = 2;

/// A transaction id: partition in the high bits, sequence (from 1) in the low.
fn tx_id(part: usize, seq: u8) -> u8 {
    (u8::try_from(part).unwrap_or(u8::MAX) << 2) | seq
}
fn part_of(tx: u8) -> usize {
    usize::from(tx >> 2)
}
fn seq_of(tx: u8) -> u8 {
    tx & 3
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Head {
    pos: u8,
    generation: [u8; PARTS],
}
/// A log record. `epoch` (with the transaction's partition) and `at` (the base
/// position) are the attempt id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Rec {
    /// A's actions for `tx`, its partition's source position and the pending
    /// intent planned against B position `plan`.
    Txn { tx: u8, epoch: u8, at: u8, plan: u8 },
    /// B's actions for `tx`, applied under the transaction id.
    Apply { tx: u8, epoch: u8, at: u8 },
    /// The intent for `tx` is applied to B.
    Done { tx: u8, epoch: u8, at: u8 },
}
impl Rec {
    fn tx(self) -> u8 {
        match self {
            Rec::Txn { tx, .. } | Rec::Apply { tx, .. } | Rec::Done { tx, .. } => tx,
        }
    }
    fn epoch(self) -> u8 {
        match self {
            Rec::Txn { epoch, .. } | Rec::Apply { epoch, .. } | Rec::Done { epoch, .. } => epoch,
        }
    }
}
fn rec_label(r: Rec) -> String {
    let tx = |tx: u8| format!("p{}.t{}", part_of(tx), seq_of(tx));
    match r {
        Rec::Txn {
            tx: t,
            epoch,
            at,
            plan,
        } => {
            format!("Txn {} + intent for B@{plan} [epoch {epoch} @{at}]", tx(t))
        }
        Rec::Apply { tx: t, epoch, at } => format!("Apply {} [epoch {epoch} @{at}]", tx(t)),
        Rec::Done { tx: t, epoch, at } => format!("Done {} [epoch {epoch} @{at}]", tx(t)),
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Dataset {
    log: Vec<Rec>,
    generation: [u8; PARTS],
}
impl Dataset {
    fn head(&self) -> Head {
        Head {
            pos: self.len(),
            generation: self.generation,
        }
    }
    fn len(&self) -> u8 {
        u8::try_from(self.log.len()).unwrap_or(u8::MAX)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    /// Record the partition's new generation in A (0), then in B (1).
    Acquire(u8),
    /// Scan A for the partition's pending intents and roll each forward.
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
    /// The writer saw a newer generation for its partition and stopped.
    Fenced,
    /// The writer handed its partition to the next owner and stopped.
    Handed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Writer {
    part: u8,
    epoch: u8,
    alive: bool,
    phase: Phase,
    active: Option<u8>,
    /// Why B's last CAS lost: bit 0 a turnover, bit 1 the other partition.
    lost: u8,
    /// Took the partition over at a handoff rather than after a failure.
    handed: bool,
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
    acknowledged: [u8; PARTS],
    clock: u8,
    /// A's acceptance of each transaction, by partition and sequence.
    stamps: [[u8; MAX_SEQ]; PARTS],
    crashes: [u8; PARTS],
    /// The generation of each partition's newest owner that finished acquiring.
    owner: [u8; PARTS],
    stale_accepted: bool,
    bad_resolution: bool,
    /// A crash left one of the partition's intents in A but not in B.
    crashed_pending: [bool; PARTS],
    /// The other partition committed while this one was down or recovering.
    other_committed: [bool; PARTS],
    concurrent: bool,
    recovered_while_other_committed: bool,
    other_rebased: bool,
    fenced_on_b: bool,
    resolved_by_id: bool,
    handoff_drained: bool,
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
    Takeover(usize),
    Handoff(usize),
}
#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
    bounds: [u8; PARTS],
    /// Turnovers (takeovers and handoffs) and crashes per partition.
    takeovers: u8,
    /// Turnovers over all partitions.
    turnovers: u8,
    /// Crashes over all partitions.
    crashes: u8,
}
fn in_a(log: &[Rec], tx: u8) -> bool {
    log.iter()
        .any(|r| matches!(r, Rec::Txn { tx: t, .. } if *t == tx))
}
fn applies(log: &[Rec], tx: u8) -> usize {
    log.iter()
        .filter(|r| matches!(r, Rec::Apply { tx: t, .. } if *t == tx))
        .count()
}
/// The partition's source position recorded in A: its last transaction record.
fn position(log: &[Rec], part: usize) -> u8 {
    log.iter()
        .rev()
        .find_map(|r| match *r {
            Rec::Txn { tx, .. } if part_of(tx) == part => Some(seq_of(tx)),
            _ => None,
        })
        .unwrap_or(0)
}
/// The partition's intents in A with no done marker, with each one's plan.
fn pending(log: &[Rec], part: usize) -> Vec<(u8, u8)> {
    log.iter()
        .filter_map(|r| match *r {
            Rec::Txn { tx, plan, .. }
                if part_of(tx) == part
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
impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            bounds: [u8::try_from(bound).unwrap_or(u8::MAX), 1],
            takeovers: 1,
            turnovers: if mode.contains("each-partition") {
                2
            } else {
                1
            },
            crashes: if mode.contains("each-partition") {
                2
            } else {
                1
            },
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    /// The keys a transaction writes in both datasets: its partition's key,
    /// unless the control routes partition 1's transactions across both.
    fn footprint(&self, tx: u8) -> u8 {
        if self.has("cross-partition") && part_of(tx) == 1 {
            0b11
        } else {
            1 << part_of(tx)
        }
    }
    /// Whether the partition's generation is recorded in dataset `ds`.
    fn recorded(&self, ds: usize) -> bool {
        ds == 0 || !self.has("a-only-generation")
    }
    /// The dataset whose head a writer checks before writing to `ds`.
    fn fencing_head(&self, ds: usize) -> usize {
        if self.recorded(ds) { ds } else { 0 }
    }
    fn holds(&self, s: &State, w: Writer, ds: usize) -> bool {
        s.ds[self.fencing_head(ds)].generation[usize::from(w.part)] == w.epoch
    }
    fn transactions(&self) -> impl Iterator<Item = u8> + '_ {
        (0..PARTS).flat_map(move |p| (1..=self.bounds[p]).map(move |q| tx_id(p, q)))
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
    /// head's position and generations never decrease, so once it differs from
    /// a request's base, every later delivery of that request is a no-op.
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
    /// Whether B records after the intent's plan write any of its keys.
    fn conflicts(&self, log: &[Rec], tx: u8, plan: u8) -> bool {
        log[usize::from(plan)..].iter().any(|r| match *r {
            Rec::Apply { tx: t, .. } => t != tx && self.footprint(t) & self.footprint(tx) != 0,
            _ => false,
        })
    }
    fn latest(s: &State, part: usize) -> Option<Writer> {
        s.writers
            .iter()
            .rev()
            .find(|w| usize::from(w.part) == part)
            .copied()
    }
    fn crash_allowed(&self, s: &State, part: usize) -> bool {
        s.crashes[part] < self.takeovers && s.crashes.iter().sum::<u8>() < self.crashes
    }
    fn turnover_allowed(&self, s: &State, part: usize) -> bool {
        Self::generations(s, part) <= self.takeovers
            && usize::from(self.turnovers) + PARTS > s.writers.len()
    }
    fn generations(s: &State, part: usize) -> u8 {
        u8::try_from(
            s.writers
                .iter()
                .filter(|w| usize::from(w.part) == part)
                .count(),
        )
        .unwrap_or(u8::MAX)
    }
    /// The partition's owner is down, acquiring or rolling intents forward.
    fn recovering(s: &State, part: usize) -> bool {
        Self::latest(s, part).is_some_and(|w| {
            !w.alive
                || matches!(
                    w.phase,
                    Phase::Acquire(_)
                        | Phase::Recover
                        | Phase::ApplyB {
                            recovering: true,
                            ..
                        }
                        | Phase::MarkDone {
                            recovering: true,
                            ..
                        }
                )
        })
    }
    /// Recovery has finished and no protocol step remains: every partition's
    /// newest owner is alive with its generation in both heads, and only
    /// environment faults are enabled.
    fn quiescent(&self, s: &State) -> bool {
        for p in 0..PARTS {
            let Some(owner) = Self::latest(s, p) else {
                return false;
            };
            if !owner.alive || !self.holds(s, owner, 0) || !self.holds(s, owner, 1) {
                return false;
            }
        }
        let mut listed = vec![];
        self.actions(s, &mut listed);
        !listed.into_iter().any(|a| {
            !matches!(
                a,
                Action::Crash(_) | Action::Takeover(_) | Action::Handoff(_)
            ) && self.next_state(s, a).is_some()
        })
    }
    fn new_owner(next: &mut State, part: usize, handed: bool) -> Option<()> {
        let epoch = Self::generations(next, part) + 1;
        next.writers.push(Writer {
            part: u8::try_from(part).ok()?,
            epoch,
            alive: true,
            phase: Phase::Acquire(0),
            active: None,
            lost: 0,
            handed,
        });
        Some(())
    }
}
impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let writers = (0..PARTS)
            .map(|p| Writer {
                part: u8::try_from(p).unwrap_or(u8::MAX),
                epoch: 1,
                alive: true,
                phase: Phase::Ready,
                active: None,
                lost: 0,
                handed: false,
            })
            .collect();
        let generation = [1; PARTS];
        vec![State {
            ds: [
                Dataset {
                    log: vec![],
                    generation,
                },
                Dataset {
                    log: vec![],
                    generation,
                },
            ],
            writers,
            owner: generation,
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
            } else {
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
                    Phase::MarkDone { .. } => {
                        if !self.has("without-done") {
                            a.push(Action::IssueDone(w));
                        }
                        a.push(Action::SkipDone(w));
                    }
                    Phase::Blocked | Phase::Fenced | Phase::Handed => (),
                }
            }
            if self.crash_allowed(s, usize::from(writer.part)) {
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
        for p in 0..PARTS {
            if self.turnover_allowed(s, p) {
                a.push(Action::Takeover(p));
                if self.has("handoff") {
                    a.push(Action::Handoff(p));
                }
            }
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
                if !self.holds(s, writer, 0) || !self.holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                // Redelivery resumes after the partition's position in A; the
                // source keeps only what it has not acknowledged.
                let p = usize::from(writer.part);
                let seq = position(&s.ds[0].log, p) + 1;
                if seq > self.bounds[p] || seq <= s.acknowledged[p] {
                    return None;
                }
                let plan = s.ds[1].len();
                let rec = Rec::Txn {
                    tx: tx_id(p, seq),
                    epoch: writer.epoch,
                    at: s.ds[0].len(),
                    plan,
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
                if !self.holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    next.fenced_on_b |= self.recorded(1);
                    return Some(next);
                }
                let log = &s.ds[1].log;
                if applies(log, tx) > 0 {
                    next.writers[w].phase = Phase::MarkDone { tx, recovering };
                    return Some(next);
                }
                // The footprint check: rebase only past winners whose keys are
                // disjoint from the intent's.
                if self.conflicts(log, tx, plan) && !self.has("blind-rebase") {
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
                if !writer.alive || writer.active.is_some() || self.has("without-done") {
                    return None;
                }
                if !self.holds(s, writer, 0) {
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
                if !self.holds(s, writer, 0) || !self.holds(s, writer, 1) {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                let found = pending(&s.ds[0].log, usize::from(writer.part))
                    .into_iter()
                    .find(|&(tx, _)| applies(&s.ds[1].log, tx) == 0);
                match found {
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
                let p = usize::from(writer.part);
                let ds = &mut next.ds[usize::from(k)];
                if ds.generation[p] >= writer.epoch {
                    next.writers[w].phase = Phase::Fenced;
                    return Some(next);
                }
                ds.generation[p] = writer.epoch;
                let acquired = k == 1 || !self.recorded(1);
                next.writers[w].phase = if !acquired {
                    Phase::Acquire(1)
                } else if self.has("ingest-before-recovery")
                    || writer.handed && self.has("handoff-no-drain")
                {
                    Phase::Ready
                } else {
                    Phase::Recover
                };
                if acquired {
                    next.owner[p] = next.owner[p].max(writer.epoch);
                }
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
                let owned = usize::from(writer.part);
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
                            head.pos > r.base.pos + 1 || head.generation != r.base.generation;
                        next.writers[w].phase = match (r.rec, writer.phase) {
                            (Rec::Txn { tx, .. }, _) => {
                                // The position is durable in A: acknowledge.
                                next.acknowledged[owned] = next.acknowledged[owned].max(seq_of(tx));
                                Phase::ApplyB {
                                    tx,
                                    plan: r.plan,
                                    recovering: false,
                                }
                            }
                            (Rec::Apply { tx, .. }, Phase::ApplyB { recovering, .. }) => {
                                if recovering
                                    && s.crashed_pending[owned]
                                    && s.other_committed[owned]
                                {
                                    next.recovered_while_other_committed = true;
                                }
                                next.handoff_drained |= recovering && writer.handed;
                                next.other_rebased |= writer.lost & 2 != 0;
                                next.writers[w].lost = 0;
                                Phase::MarkDone { tx, recovering }
                            }
                            (
                                Rec::Done { .. },
                                Phase::MarkDone {
                                    recovering: true, ..
                                },
                            ) => Phase::Recover,
                            (Rec::Done { .. }, _) => Phase::Ready,
                            _ => return None,
                        };
                    }
                    _ => {
                        next.writers[w].active = None;
                        let fencing = usize::from(r.ds);
                        if !self.holds(s, writer, fencing) {
                            next.writers[w].phase = Phase::Fenced;
                            next.fenced_on_b |= fencing == 1 && self.recorded(1);
                        } else if matches!(r.rec, Rec::Apply { .. }) {
                            if head.generation != r.base.generation {
                                next.writers[w].lost |= 1;
                            }
                            if ds.log[usize::from(r.base.pos)..]
                                .iter()
                                .any(|x| part_of(x.tx()) != owned)
                            {
                                next.writers[w].lost |= 2;
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
                let target = usize::from(r.ds);
                if next.ds[target].head() == r.base {
                    next.ds[target].log.push(r.rec);
                    let owned = part_of(r.rec.tx());
                    next.stale_accepted |= r.rec.epoch() < s.owner[owned];
                    if let Rec::Txn { tx, .. } = r.rec {
                        next.clock += 1;
                        next.stamps[owned][usize::from(seq_of(tx) - 1)] = next.clock;
                    }
                    if matches!(r.rec, Rec::Txn { .. } | Rec::Apply { .. }) {
                        for other in (0..PARTS).filter(|&o| o != owned) {
                            next.other_committed[other] |= Self::recovering(s, other);
                        }
                    }
                    let (log_a, log_b) = (&next.ds[0].log, &next.ds[1].log);
                    next.concurrent |= (0..PARTS).all(|part| {
                        (1..=self.bounds[part]).any(|seq| {
                            let tx = tx_id(part, seq);
                            in_a(log_a, tx) && applies(log_b, tx) == 0
                        })
                    });
                }
            }
            Action::Crash(w) => {
                let writer = s.writers[w];
                let p = usize::from(writer.part);
                if !self.crash_allowed(s, p) || !writer.alive {
                    return None;
                }
                next.crashes[p] += 1;
                next.writers[w].alive = false;
                if (1..=self.bounds[p]).any(|seq| {
                    let tx = tx_id(p, seq);
                    in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 0
                }) {
                    next.crashed_pending[p] = true;
                    next.other_committed[p] = false;
                }
            }
            Action::Takeover(p) => {
                if !self.turnover_allowed(s, p) {
                    return None;
                }
                Self::new_owner(&mut next, p, false)?;
            }
            Action::Handoff(p) => {
                if !self.has("handoff") || !self.turnover_allowed(s, p) {
                    return None;
                }
                // The owner stops between protocol steps, at any point of its
                // fast path, and the next owner takes the partition over.
                let w = s.writers.iter().rposition(|w| usize::from(w.part) == p)?;
                let writer = s.writers[w];
                if !writer.alive
                    || writer.active.is_some()
                    || matches!(
                        writer.phase,
                        Phase::Acquire(_) | Phase::Fenced | Phase::Handed
                    )
                {
                    return None;
                }
                next.writers[w].phase = Phase::Handed;
                Self::new_owner(&mut next, p, true)?;
            }
        }
        Self::forget_dead_requests(&mut next)?;
        (next != *s).then_some(next)
    }
    fn format_action(&self, action: &Action) -> String {
        match action {
            Action::Ingest(w) => {
                format!("writer {w} sends A: its partition's actions, position and intent")
            }
            Action::IssueApply(w) => {
                format!("writer {w} sends B: apply the intent under its transaction id")
            }
            Action::IssueDone(w) => format!("writer {w} sends A: mark the intent done"),
            Action::SkipDone(w) => format!("writer {w} skips the done marker"),
            Action::Recover(w) => {
                format!("writer {w} rolls its partition's pending intents forward")
            }
            Action::Acquire(w) => format!("writer {w} records its generation in the next head"),
            Action::Check(w) => format!("writer {w} resolves its lost response by attempt id"),
            Action::Deliver(i, 0) => format!("request {i} arrives"),
            Action::Deliver(i, _) => format!("a duplicate of request {i} arrives"),
            Action::Crash(w) => format!("writer {w} crashes"),
            Action::Takeover(p) => format!("a new owner takes partition {p} over"),
            Action::Handoff(p) => format!("partition {p}'s live owner hands it off"),
        }
    }
    fn format_step(&self, last_state: &State, action: Action) -> Option<String> {
        let s = self.next_state(last_state, action)?;
        Some(crate::explorer::summary(&[
            format!(
                "acknowledged per partition: {:?}; owner writer per partition: {:?}; crashes {:?}",
                s.acknowledged, s.owner, s.crashes
            ),
            crate::explorer::flags(&[
                ("partitions committed concurrently", s.concurrent),
                (
                    "recovered while the other partition committed",
                    s.recovered_while_other_committed,
                ),
                ("other partition's write rebased past", s.other_rebased),
                ("fenced on B", s.fenced_on_b),
                ("outcome resolved by attempt id", s.resolved_by_id),
                ("handoff drained", s.handoff_drained),
                ("BAD stale-generation commit accepted", s.stale_accepted),
                ("BAD resolution", s.bad_resolution),
            ]),
        ]))
    }
    fn as_svg(&self, path: stateright::Path<State, Action>) -> Option<String> {
        use crate::explorer::{Event, Svg, Timeline, Tone, row, steps};
        let last = path.last_state().clone();
        let mut svg = Svg::new();
        let lines = |d: &Dataset| -> Vec<String> {
            let mut out = vec![format!("partition generations {:?}", d.generation)];
            out.extend(
                d.log
                    .iter()
                    .enumerate()
                    .map(|(i, r)| format!("{}. {}", i + 1, rec_label(*r))),
            );
            out
        };
        svg.heading(0, 0, "DATASET LOGS (intent in A -> apply in B)");
        let log_a = svg.node(0, 22, 0, "A", &lines(&last.ds[0]), Tone::Live);
        let log_b = svg.node(
            log_a.right() + 120,
            22,
            0,
            "B",
            &lines(&last.ds[1]),
            Tone::Live,
        );
        for (i, r) in last.ds[0].log.iter().enumerate() {
            if let Rec::Txn { tx, .. } = r {
                let found: Vec<usize> = last.ds[1]
                    .log
                    .iter()
                    .enumerate()
                    .filter(|(_, x)| matches!(x, Rec::Apply { tx: t, .. } if t == tx))
                    .map(|(j, _)| j + 1)
                    .collect();
                if found.is_empty() {
                    svg.arrow(
                        (log_a.right(), log_a.line_y(i + 1)),
                        (log_b.x, log_b.bottom() - 8),
                        Tone::Staged,
                        Some("pending"),
                    );
                }
                let tone = if found.len() > 1 {
                    Tone::Bad
                } else {
                    Tone::Live
                };
                for j in found {
                    svg.arrow(
                        (log_a.right(), log_a.line_y(i + 1)),
                        (log_b.x, log_b.line_y(j)),
                        tone,
                        Some("applied"),
                    );
                }
            }
        }
        let mut boxes: Vec<(String, Vec<String>, Tone)> = last
            .writers
            .iter()
            .enumerate()
            .map(|(w, wr)| {
                (
                    format!("WRITER {w} (partition {}, epoch {})", wr.part, wr.epoch),
                    vec![
                        if wr.alive {
                            "alive".into()
                        } else {
                            "crashed".into()
                        },
                        format!("{:?}", wr.phase),
                    ],
                    if wr.alive && !wr.handed {
                        Tone::Actor
                    } else {
                        Tone::Gone
                    },
                )
            })
            .collect();
        for (i, r) in last.reqs.iter().enumerate() {
            boxes.push((
                format!("REQUEST {i} to {}", if r.ds == 0 { "A" } else { "B" }),
                vec![
                    rec_label(r.rec),
                    format!(
                        "on (pos {}, generations {:?})",
                        r.base.pos, r.base.generation
                    ),
                    format!("copies sent {:02b} landed {:02b}", r.sent, r.done),
                ],
                Tone::Staged,
            ));
        }
        let trace_y = row(&mut svg, log_a.bottom().max(log_b.bottom()) + 24, &boxes) + 24;
        let writer_count = last.writers.len();
        let (lane_a, lane_b) = (writer_count, writer_count + 1);
        let mut lane_names: Vec<String> = last
            .writers
            .iter()
            .enumerate()
            .map(|(w, wr)| format!("w{w} (p{})", wr.part))
            .collect();
        lane_names.extend(["A".to_string(), "B".to_string()]);
        let mut events = vec![];
        for (before, action, after) in steps(path) {
            events.push(match action {
                Action::Ingest(w) => Event::on(w, "send Txn + intent")
                    .to(lane_a)
                    .tone(Tone::Staged),
                Action::IssueApply(w) => Event::on(w, "send Apply").to(lane_b).tone(Tone::Staged),
                Action::IssueDone(w) => Event::on(w, "send Done").to(lane_a).tone(Tone::Staged),
                Action::SkipDone(w) => Event::on(w, "skip Done"),
                Action::Recover(w) => Event::on(w, "roll pending intents forward")
                    .to(lane_a)
                    .tone(Tone::Actor),
                Action::Acquire(w) => {
                    let to = if matches!(before.writers[w].phase, Phase::Acquire(1)) {
                        lane_b
                    } else {
                        lane_a
                    };
                    Event::on(w, "record generation in head").to(to)
                }
                Action::Check(w) => Event::on(w, "resolve by attempt id").tone(Tone::Actor),
                Action::Deliver(i, q) => {
                    let r = before.reqs[i];
                    let ds = usize::from(r.ds);
                    let landed = after.ds[ds].log.len() > before.ds[ds].log.len();
                    let w = before
                        .writers
                        .iter()
                        .position(|w| {
                            w.epoch == r.rec.epoch() && usize::from(w.part) == part_of(r.rec.tx())
                        })
                        .unwrap_or(0);
                    let copy = if q == 0 { "" } else { "duplicate " };
                    Event::on(
                        w,
                        if landed {
                            format!("{copy}{} lands", rec_label(r.rec))
                        } else {
                            format!("{copy}{} refused", rec_label(r.rec))
                        },
                    )
                    .to(if ds == 0 { lane_a } else { lane_b })
                    .tone(if landed { Tone::Live } else { Tone::Gone })
                }
                Action::Crash(w) => Event::on(w, "CRASH").tone(Tone::Bad),
                Action::Takeover(p) => {
                    Event::on(after.writers.len() - 1, format!("takes partition {p} over"))
                        .tone(Tone::Actor)
                }
                Action::Handoff(p) => Event::on(
                    after.writers.len() - 1,
                    format!("receives partition {p} by handoff"),
                )
                .tone(Tone::Actor),
            });
        }
        Timeline {
            lanes: lane_names,
            events,
        }
        .draw(&mut svg, 0, trace_y);
        Some(svg.finish())
    }
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "eventual atomicity at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || model
                            .transactions()
                            .all(|tx| in_a(&s.ds[0].log, tx) == (applies(&s.ds[1].log, tx) > 0))
                },
            ),
            Property::always(
                "B applies each transaction at most once",
                |model: &Self, s: &State| {
                    model
                        .transactions()
                        .all(|tx| applies(&s.ds[1].log, tx) <= 1)
                },
            ),
            Property::always(
                "acknowledged transactions in both datasets at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || (0..PARTS).all(|p| {
                            (1..=s.acknowledged[p]).all(|seq| {
                                let tx = tx_id(p, seq);
                                in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) > 0
                            })
                        })
                },
            ),
            Property::always(
                "source position per partition durable in both datasets at quiescence",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || (0..PARTS).all(|p| {
                            (1..=position(&s.ds[0].log, p)).all(|seq| {
                                let tx = tx_id(p, seq);
                                in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) > 0
                            })
                        })
                },
            ),
            Property::always(
                "I2 source acknowledgement covered by A's position per partition",
                |_, s: &State| (0..PARTS).all(|p| s.acknowledged[p] <= position(&s.ds[0].log, p)),
            ),
            Property::always(
                "B keyed values follow commit order",
                |model: &Self, s: &State| {
                    // For each key, B's value must be the applied transaction
                    // that writes it with the latest commit point in A.
                    let log = &s.ds[1].log;
                    (0..PARTS).all(|key| {
                        let bit = 1 << key;
                        let value = log
                            .iter()
                            .rev()
                            .find(|r| {
                                matches!(r, Rec::Apply { .. }) && model.footprint(r.tx()) & bit != 0
                            })
                            .map(|r| r.tx());
                        let expected = model
                            .transactions()
                            .filter(|&tx| model.footprint(tx) & bit != 0 && applies(log, tx) > 0)
                            .max_by_key(|&tx| s.stamps[part_of(tx)][usize::from(seq_of(tx) - 1)]);
                        value == expected
                    })
                },
            ),
            Property::always(
                "no stale-generation commit accepted per partition",
                |_, s: &State| !s.stale_accepted,
            ),
            Property::always("I5 attempt-id resolution sound", |_, s: &State| {
                !s.bad_resolution
            }),
            Property::sometimes(
                "both partitions commit concurrently and both complete",
                |model: &Self, s: &State| {
                    s.concurrent
                        && (0..PARTS).all(|p| applies(&s.ds[1].log, tx_id(p, 1)) == 1)
                        && model
                            .transactions()
                            .all(|tx| in_a(&s.ds[0].log, tx) == (applies(&s.ds[1].log, tx) > 0))
                },
            ),
            Property::sometimes(
                "partition crash recovered while the other commits",
                |_, s: &State| s.recovered_while_other_committed,
            ),
            Property::sometimes(
                "B CAS lost to the other partition and rebased",
                |_, s: &State| s.other_rebased,
            ),
            Property::sometimes(
                "partition turnover fences its old writer on B",
                |_, s: &State| s.fenced_on_b,
            ),
            Property::sometimes("lost response resolved by attempt id", |_, s: &State| {
                s.resolved_by_id
            }),
            Property::sometimes("reader sees A before B", |model: &Self, s: &State| {
                model
                    .transactions()
                    .any(|tx| in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 0)
            }),
            Property::sometimes(
                "handoff drained before the new owner ingests",
                |_, s: &State| s.handoff_drained,
            ),
            Property::sometimes(
                "two transactions of one partition applied to both",
                |model: &Self, s: &State| {
                    model.bounds[0] >= 2
                        && (1..=2).all(|seq| {
                            let tx = tx_id(0, seq);
                            in_a(&s.ds[0].log, tx) && applies(&s.ds[1].log, tx) == 1
                        })
                },
            ),
        ]
    }
}
