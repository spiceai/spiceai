// Copyright 2024-2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Data files uploaded before the commit that names them. A writer holding the
//! dataset's lease uploads each data file to a fresh final key under the
//! dataset prefix, `<generation>-<id>`, and only after every upload it names has
//! returned does it send a commit: a conditional publication of the head, on
//! the exact head version it read and its lease generation, that adds the
//! files to the committed set. Readers read the head and fetch the files it
//! names; they never list the store. A commit's response can be lost: the
//! writer then holds an unknown outcome, may resend the identical request, and
//! resolves it from the head alone: committed when the head names its files,
//! aborted once the head has moved past the request's base (the exact-base
//! condition can then never pass), and otherwise still unknown. A takeover is
//! a head commit that raises the lease generation, so it fences the previous
//! holder, whose uploads may still be in flight and land afterwards. A writer
//! can crash, losing what it knew; its requests and uploads already sent can
//! still land. An upload no committed head names is an orphan: readers never
//! see it, and once its generation is fenced it can be deleted, because no
//! commit naming it can pass the lease condition. (The `retention` runs model
//! age-based collection; this graph uses the generation alone.)
//!
//! In the `intent` runs the sweep is the lease holder's, and it uses no
//! generation in the key: it deletes an unnamed file unless the holder's own
//! intent protects it. A writer records the key it is about to upload before
//! it sends the upload, and keeps it through the upload and its commit until
//! the outcome resolves; the record lives in the writer's process, so a crash
//! or a takeover loses it, and then the lease condition is what keeps the file
//! out of every commit. (Cayenne's orphan sweep adds an age floor on the store's
//! clock as a backstop; the `retention` runs model age.)
//!
//! In the `retire` runs (on top of `intent`) data files sit flat per table and
//! retire per file. The head's live set is a set of files, and a record
//! changes it by name: an append adds its uploads (`AddFiles`); a removal
//! (`RemoveFiles`) takes the oldest live file out; a compaction
//! (`ReplaceFiles`) removes its inputs and adds its outputs, and
//! `CompactCarry` carries the oldest live file in place (it names it and does
//! not remove it). So several commits can name one file. The record that
//! removes a file retires it in the same record: the head's retired set gains
//! the file with that record's version, and a carried file, never removed, is
//! never retired. The head records each accepted attempt, so an unknown
//! outcome resolves by attempt. Time is a logical clock: the retention T is
//! `RETENTION` ticks and a remote reader's staleness bound S is `STALENESS`.
//! Reclamation runs in the lease holder's process. A sweep pass reads the
//! committed retired set and starts an in-memory timer for each file it sees
//! for the first time (a crash or takeover loses the timers, and the next
//! holder restarts them, which only delays a delete). It deletes a retired
//! file by name, never by listing, once T has passed on its timer. It declines
//! while the holder has a commit whose outcome it does not know, while a
//! prepared transaction reserves the table, and while a restore point holds
//! deletions. Only after every delete has returned does it commit one record,
//! `ForgetRetired`, that takes those files out of the retired set. Deletes can
//! land late (after a crash or a takeover of the sweeping process) or fail; a
//! sweep with a failed delete or a crash before its forget leaves the files
//! retired, and a later pass deletes again (a delete of a missing key is a
//! no-op) and forgets them. A local scan captures the head in one step
//! (S = 0), then fetches its files with protection only for T; a restore point reads the head
//! and copies the live files while it holds deletions. A remote reader on
//! another instance writes nothing: it confirms the head with a
//! GET, starts a query within S of the confirmation, and fetches the files; the
//! retention guarantees it its files for T − S after it starts. In the
//! `prepared` runs a commit can instead record a prepared transaction, a
//! compaction of every live file applied only by a later resolve commit
//! (`CommitTx`) or discarded (`AbortTx`); while it is unresolved the table
//! refuses other changes and both sweeps decline. The orphan sweep treats a
//! retired file as named until its forget. `lean` runs have no scans, restore
//! points or remote readers, so a larger upload bound stays exhaustive.
//!
//! The clock advances only while a holder timer or a local or remote query
//! is running: every time comparison is between two ticks of one of those, so
//! a tick with neither only shifts every later time and reaches no new outcome.
//!
//! Controls: `no-upload-before-commit` lets a commit name a file whose upload
//! has not returned; `no-committed-read` makes the reader list the store
//! instead of reading the head; `no-resolve-by-head` resolves an unknown
//! outcome as aborted when the head lacks its files, without waiting for the
//! head to move past the request's base; `sweep-ignores-intent` lets the
//! holder's sweep delete a file its own write in flight still holds (reclaiming
//! too early); `adopt-by-listing` lets a commit name every unnamed file the
//! store lists, as a maintenance pass that derives its file set from a listing
//! would. In the `retire` runs: `whole-snapshot` retires every file live at a
//! removing record's base, as a snapshot-level rule would, and its sweep keeps
//! the files the live set names; `retires-carried` also retires the file a
//! compaction carries; `local-ignores-retention` skips T for a local scan; `ignores-retention` deletes as soon as the sweep observes the
//! removal; `forget-early` forgets the files before their deletes return;
//! `ignores-restore-hold` sweeps while a restore point copies;
//! `sweep-uncommitted` plans from the holder's local view, with its removal
//! applied before the record commits; `orphan-ignores-reservation` lets the
//! orphan sweep run while a prepared transaction reserves the table, and
//! `retire-ignores-reservation` lets the retirement sweep do so.
use stateright::{Model, Property};

/// Writer processes.
const WRITERS: usize = 2;
/// Lease takeovers.
const TAKES: u8 = 2;
/// Writer crashes.
const CRASHES: u8 = 1;
/// Commit attempts across writers.
const ATTEMPTS: u8 = 3;
/// Identical resends of an attempt with an unknown outcome.
const RETRIES: u8 = 1;
/// Head reads by the reader.
const READS: u8 = 2;
/// In the `retire` runs: lease takeovers, local scans, restore points, remote
/// queries and failed deletes, smaller so the added reclamation stays
/// exhaustive.
const RETIRE_TAKES: u8 = 1;
const RETIRE_READS: u8 = 1;
const RESTORES: u8 = 1;
const REMOTE_READS: u8 = 1;
const DELETE_FAILURES: u8 = 1;
/// The retention T and the remote reader's staleness bound S, in clock ticks.
const RETENTION: u8 = 2;
const STALENESS: u8 = 1;
/// Clock ticks, reduced at three uploads. The three-upload `remote` runs
/// also omit crashes; a takeover still restarts the sweep's timers.
const TICKS: u8 = 3;
const TICKS_3: u8 = 2;

/// Independent controls selected once from the model run's mode.
#[derive(Clone, Copy, Debug)]
enum ModeFlag {
    AdoptByListing,
    ForgetEarly,
    LocalIgnoresRetention,
    IgnoresReservation,
    IgnoresRestoreHold,
    IgnoresRetention,
    Intent,
    Lean,
    NoCommittedRead,
    NoResolveByHead,
    NoUploadBeforeCommit,
    OrphanIgnoresReservation,
    Prepared,
    Remote,
    Retire,
    RetireIgnoresReservation,
    RetiresCarried,
    SweepIgnoresIntent,
    SweepUncommitted,
    WholeSnapshot,
}

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    flags: u32,
    /// Uploads across writers.
    uploads: u8,
}

/// A data file's final key: the lease generation it was uploaded under and a
/// unique id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub generation: u8,
    pub id: u8,
}
/// A key and the bytes stored under it, as the uploading writer's index: keys
/// are unique, so the bytes are fixed by the upload that created the key.
pub type File = (Key, u8);
/// The record that retired a file: its head version, and the clock tick at
/// which the file actually left the live set, `None` while it is still live
/// (only a control retires a live file). The tick is a ghost for the I15
/// timing check; the sweep times the retention from when it observes the
/// removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Removal {
    pub version: u8,
    pub tick: Option<u8>,
}
/// A retired file and the record that removed it.
pub type Retired = (File, Removal);

/// What a commit request publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CommitKind {
    /// `AddFiles`: the uploaded files join the live set.
    Append,
    /// `ReplaceFiles` of every live file by the uploaded files.
    Compact,
    /// `ReplaceFiles` of every live file but the oldest, which it carries in
    /// place, by the uploaded files.
    CompactCarry,
    /// `RemoveFiles` of the oldest live file; the rest stay live.
    Remove,
    /// A prepared transaction: a compaction of every live file, applied only
    /// when a later resolve commits it.
    Prepare,
}

/// An unresolved prepared transaction the head records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Prepared {
    pub id: u8,
    pub files: Vec<File>,
    pub remove: Vec<File>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Request {
    attempt: u8,
    writer: usize,
    generation: u8,
    base: u8,
    /// New uploads the request names.
    files: Vec<File>,
    kind: CommitKind,
    /// Live files a compaction carries in place.
    carry: Vec<File>,
    /// Live files it removes, and so retires.
    remove: Vec<File>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Writer {
    /// The lease generation this writer believes it holds.
    generation: Option<u8>,
    /// Uploads sent whose response has not arrived.
    started: Vec<File>,
    /// Uploads whose response arrived and that no commit has named yet.
    finished: Vec<File>,
    /// The outstanding commit attempt, and whether its response was lost.
    pending: Option<(Request, bool)>,
    /// In the `retire` runs: each retired file this process's sweep has seen,
    /// with the tick it first saw it (its retention timer).
    timers: Vec<(File, u8)>,
}
/// A reclamation pass in one process: the files it will forget, the
/// generation it runs under, and whether one of its deletes failed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Sweep {
    writer: usize,
    generation: u8,
    files: Vec<File>,
    failed: bool,
}
/// A delete request in flight: the file, the sweeping process's generation, and
/// whether the sweep that sent it still waits for it.
type Delete = (File, u8, bool);
/// A query on another instance: the head version and files its confirming GET
/// read, the tick the GET was sent, the tick the query started, and the next
/// file to fetch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Remote {
    version: u8,
    keys: Vec<Key>,
    confirmed: u8,
    started: Option<u8>,
    next: usize,
}
/// What a sweep pass would do now: its process, the retired files it sees for
/// the first time, and the deletes it sends.
struct Pass {
    writer: usize,
    observe: Vec<File>,
    delete: Vec<File>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    version: u8,
    lease: u8,
    /// The committed file set the head names (in the `retire` runs: the live
    /// set).
    head: Vec<File>,
    /// The committed file set at every head version (a ghost for the checks).
    history: Vec<Vec<File>>,
    /// The object store's data files.
    store: Vec<File>,
    /// Upload requests in flight, by writer.
    puts: Vec<(usize, File)>,
    /// Commit requests in flight.
    net: Vec<Request>,
    writers: Vec<Writer>,
    /// The reader's head version and the keys it will fetch, and the next one.
    snapshot: Option<(u8, Vec<Key>)>,
    fetched: usize,
    /// Every fetch: snapshot version, key and the bytes returned, if any.
    observed: Vec<(u8, Key, Option<u8>)>,
    /// Keys of attempts refused or resolved as aborted (a ghost).
    aborted: Vec<Key>,
    /// Keys a writer dropped unnamed at a crash, takeover or fence (a ghost).
    dropped: Vec<Key>,
    /// Each resolved unknown outcome: its files and whether it resolved committed.
    resolutions: Vec<(Vec<File>, bool)>,
    uploads: u8,
    attempts: u8,
    takes: u8,
    crashes: u8,
    retries: u8,
    reads: u8,
    accepted: bool,
    fenced_refused: bool,
    /// A fenced writer, still running, had an upload land after the takeover.
    fenced_landed: bool,
    reclaimed: bool,
    // The `retire` runs; empty, zero or false otherwise.
    /// The logical clock, in ticks.
    now: u8,
    /// Removed files whose forget has not committed, by file, with the record
    /// that retired each.
    retired: Vec<Retired>,
    /// Attempts the head records as accepted.
    applied: Vec<u8>,
    /// The unresolved prepared transaction the head records.
    prepared: Option<Prepared>,
    sweep: Option<Sweep>,
    deletes: Vec<Delete>,
    /// The local scan start tick (S = 0); it never holds back deletion.
    local_started: Option<u8>,
    /// A restore point in progress: head version, the keys it copies, the next.
    restore: Option<(u8, Vec<Key>, usize)>,
    /// Every copy: key and whether the store returned it.
    copied: Vec<(Key, bool)>,
    remote: Option<Remote>,
    remote_reads: u8,
    /// Files that stayed live through a record that removed other files: a
    /// compaction's carried file, or a file a removal kept (a ghost).
    shared: Vec<File>,
    /// Files whose forget committed, with the record that had retired each
    /// (a ghost).
    forgotten: Vec<Retired>,
    /// Files a sweep planned and ended without forgetting (a ghost).
    interrupted: Vec<File>,
    /// Each resolved unknown outcome, by attempt, and whether it resolved committed.
    attempt_resolutions: Vec<(u8, bool)>,
    restores: u8,
    delete_failures: u8,
    /// A committed file was deleted before T had passed since the record that
    /// removed it, or with no such record (a ghost).
    early_delete: bool,
    /// A file was deleted while the live set named it (a ghost).
    deleted_live: bool,
    /// A remote query, within T − S of its start, found a file missing (a ghost).
    remote_missed: bool,
    local_missed: bool,
    local_expired_missing: bool,
    remote_read_removed: bool,
    remote_late_missed: bool,
    fetched_retired: bool,
    /// Files a compaction carried in place (a ghost).
    carried: Vec<File>,
    forgot_shared: bool,
    forgot_carried: bool,
    landed_after_takeover: bool,
    resumed: bool,
    copied_retired: bool,
    tx_committed: bool,
    tx_aborted: bool,
    planned_while_reserved: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    StartUpload(usize),
    FinishUpload(usize),
    Commit(usize),
    Deliver(usize),
    Drop(usize),
    Timeout(usize),
    Retry(usize),
    Resolve(usize),
    Takeover(usize),
    Crash(usize),
    Read,
    Fetch,
    Sweep(usize),
    Compact(usize),
    CompactCarry(usize),
    Remove(usize),
    Prepare(usize),
    CommitTx(usize),
    AbortTx(usize),
    Plan,
    DeleteLand(usize),
    DeleteFail(usize),
    Forget,
    EndScan,
    RestoreStart,
    RestoreCopy,
    RestoreEnd,
    Tick,
    RemoteConfirm,
    RemoteStart,
    RemoteFetch,
}

fn keys(files: &[File]) -> impl Iterator<Item = Key> + '_ {
    files.iter().map(|(k, _)| *k)
}
fn insert_sorted<T: Ord>(set: &mut Vec<T>, items: impl IntoIterator<Item = T>) {
    for item in items {
        if let Err(at) = set.binary_search(&item) {
            set.insert(at, item);
        }
    }
}
/// Add `file` to a set keyed by file, keeping an entry already there.
fn insert_first<T: Ord + Copy>(set: &mut Vec<(File, T)>, file: File, value: T) {
    if let Err(at) = set.binary_search_by(|(f, _)| f.cmp(&file)) {
        set.insert(at, (file, value));
    }
}
fn entry<T: Copy>(set: &[(File, T)], file: File) -> Option<T> {
    set.binary_search_by(|(f, _)| f.cmp(&file))
        .ok()
        .map(|at| set[at].1)
}

impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            flags: [
                (ModeFlag::AdoptByListing, "adopt-by-listing"),
                (ModeFlag::ForgetEarly, "forget-early"),
                (ModeFlag::LocalIgnoresRetention, "local-ignores-retention"),
                (ModeFlag::IgnoresReservation, "ignores-reservation"),
                (ModeFlag::IgnoresRestoreHold, "ignores-restore-hold"),
                (ModeFlag::IgnoresRetention, "ignores-retention"),
                (ModeFlag::Intent, "intent"),
                (ModeFlag::Lean, "lean"),
                (ModeFlag::NoCommittedRead, "no-committed-read"),
                (ModeFlag::NoResolveByHead, "no-resolve-by-head"),
                (ModeFlag::NoUploadBeforeCommit, "no-upload-before-commit"),
                (
                    ModeFlag::OrphanIgnoresReservation,
                    "orphan-ignores-reservation",
                ),
                (ModeFlag::Prepared, "prepared"),
                (ModeFlag::Remote, "remote"),
                (ModeFlag::Retire, "retire"),
                (
                    ModeFlag::RetireIgnoresReservation,
                    "retire-ignores-reservation",
                ),
                (ModeFlag::RetiresCarried, "retires-carried"),
                (ModeFlag::SweepIgnoresIntent, "sweep-ignores-intent"),
                (ModeFlag::SweepUncommitted, "sweep-uncommitted"),
                (ModeFlag::WholeSnapshot, "whole-snapshot"),
            ]
            .into_iter()
            .filter(|(_, name)| mode.contains(*name))
            .fold(0, |flags, (flag, _)| flags | (1 << flag as u32)),
            uploads: u8::try_from(bound.max(1)).unwrap_or(1),
        }
    }
    fn has(&self, flag: ModeFlag) -> bool {
        self.flags & (1 << flag as u32) != 0
    }
    fn retiring(&self) -> bool {
        self.has(ModeFlag::Retire)
    }
    fn takes(&self) -> u8 {
        if self.retiring() { RETIRE_TAKES } else { TAKES }
    }
    /// Whether this retirement graph has a reader on another instance.
    fn remote(&self) -> bool {
        self.retiring() && self.has(ModeFlag::Remote)
    }
    fn retention(&self) -> u8 {
        if self.retiring() { RETENTION } else { 0 }
    }
    fn ticks(&self) -> u8 {
        if self.uploads >= 3 { TICKS_3 } else { TICKS }
    }
    fn crashes(&self) -> u8 {
        if self.remote() && self.uploads >= 3 {
            0
        } else {
            CRASHES
        }
    }
    fn reads(&self) -> u8 {
        if self.has(ModeFlag::Lean) || self.remote() {
            0
        } else if self.retiring() {
            RETIRE_READS
        } else {
            READS
        }
    }
    /// Restore points are covered by the base local retirement graph and its
    /// restore-hold control; prepared and three-upload graphs omit them to
    /// stay within the exhaustive checker's time limit.
    fn restores(&self) -> u8 {
        if self.has(ModeFlag::Prepared)
            || self.has(ModeFlag::Lean)
            || self.remote()
            || self.uploads >= 3
        {
            0
        } else {
            RESTORES
        }
    }
    fn remote_reads(&self) -> u8 {
        if self.remote() { REMOTE_READS } else { 0 }
    }
    /// The store's files the committed head does not name, sorted.
    fn unnamed(s: &State) -> Vec<File> {
        s.store
            .iter()
            .filter(|f| !s.head.contains(f))
            .copied()
            .collect()
    }
    /// The writer holding the current lease, if one runs.
    fn holder(s: &State) -> Option<&Writer> {
        s.writers.iter().find(|w| w.generation == Some(s.lease))
    }
    fn holder_index(s: &State) -> Option<usize> {
        s.writers.iter().position(|w| w.generation == Some(s.lease))
    }
    /// Whether `writer` recorded `file` before uploading it and has not seen the
    /// outcome of the commit that names it yet.
    fn intends(writer: &Writer, file: File) -> bool {
        writer.started.contains(&file)
            || writer.finished.contains(&file)
            || writer
                .pending
                .as_ref()
                .is_some_and(|(request, _)| request.files.contains(&file))
    }
    /// How many files of `files` the committed head names.
    fn named(s: &State, files: &[File]) -> usize {
        files.iter().filter(|f| s.head.contains(f)).count()
    }
    /// Whether the outcome of `request` is committed, as the head shows it.
    fn landed(&self, s: &State, request: &Request) -> bool {
        if self.retiring() {
            s.applied.contains(&request.attempt)
        } else {
            Self::named(s, &request.files) == request.files.len()
        }
    }
    /// Whether the orphan sweep counts `file` as named: by the live set, by
    /// the retired set until its forget, or, while a prepared transaction
    /// reserves the table, by anything at all (the sweep declines).
    fn named_for_orphans(&self, s: &State, file: File) -> bool {
        s.head.contains(&file)
            || entry(&s.retired, file).is_some()
            || s.prepared.is_some() && !self.has(ModeFlag::OrphanIgnoresReservation)
    }
    /// The retired set the sweeping process reads: the committed one, and in
    /// `sweep-uncommitted` its own removal in flight applied as if it had
    /// landed.
    fn local_view(&self, s: &State, holder: &Writer) -> Vec<Retired> {
        let mut retired = s.retired.clone();
        if self.has(ModeFlag::SweepUncommitted)
            && let Some((request, _)) = &holder.pending
            && request.kind != CommitKind::Prepare
            && !s.applied.contains(&request.attempt)
        {
            for f in &request.remove {
                let removal = Removal {
                    version: request.base + 1,
                    tick: Some(s.now),
                };
                insert_first(&mut retired, *f, removal);
            }
        }
        retired
    }
    /// The sweep pass the lease holder would run now, if it would change
    /// anything.
    fn pass(&self, s: &State) -> Option<Pass> {
        let h = Self::holder_index(s)?;
        let holder = &s.writers[h];
        if s.sweep.is_some()
            || s.prepared.is_some() && !self.has(ModeFlag::RetireIgnoresReservation)
            || s.restore.is_some() && !self.has(ModeFlag::IgnoresRestoreHold)
            || holder.pending.is_some() && !self.has(ModeFlag::SweepUncommitted)
        {
            return None;
        }
        let mut observe = Vec::new();
        let mut delete = Vec::new();
        for (file, _) in self.local_view(s, holder) {
            let waited = self.waited(s, holder, file);
            if self.retention() > 0 && entry(&holder.timers, file).is_none() {
                observe.push(file);
            }
            // A snapshot-level rule keeps the files the live set names.
            let live = self.has(ModeFlag::WholeSnapshot) && s.head.contains(&file);
            if waited && !live {
                delete.push(file);
            }
        }
        (!observe.is_empty() || !delete.is_empty()).then_some(Pass {
            writer: h,
            observe,
            delete,
        })
    }
    /// Whether `holder`'s sweep may delete the retired `file` now: T has
    /// passed on its timer.
    fn waited(&self, s: &State, holder: &Writer, file: File) -> bool {
        self.retention() == 0
            || self.has(ModeFlag::IgnoresRetention)
            || self.has(ModeFlag::LocalIgnoresRetention)
            || entry(&holder.timers, file).is_some_and(|since| s.now >= since + self.retention())
    }
    /// Deletes in flight the running sweep still waits for.
    fn outstanding(s: &State) -> usize {
        s.deletes.iter().filter(|d| d.2).count()
    }
    /// End the running sweep without a forget: its deletes no longer report,
    /// and its files stay retired for a later pass.
    fn end_sweep(next: &mut State) {
        if let Some(sweep) = next.sweep.take() {
            for delete in &mut next.deletes {
                delete.2 = false;
            }
            next.deletes.sort_unstable();
            insert_sorted(&mut next.interrupted, sweep.files);
        }
    }
    /// After a delete of the running sweep returned: a sweep with a failed
    /// delete ends once its last delete returns.
    fn settle_sweep(next: &mut State) {
        if next.sweep.as_ref().is_some_and(|sweep| sweep.failed) && Self::outstanding(next) == 0 {
            Self::end_sweep(next);
        }
    }
    /// Whether time can matter: a holder timer is running, or the remote reader
    /// is in a query.
    fn timed(&self, s: &State) -> bool {
        Self::holder(s).is_some_and(|holder| {
            holder
                .timers
                .iter()
                .any(|(_, since)| s.now < since + self.retention())
        }) || s.remote.is_some()
            || s.local_started.is_some()
    }
    /// Record a delete of `file` landing in the store (for the I15 checks).
    fn deleting(&self, s: &State, next: &mut State, file: File) {
        if !self.retiring() || !s.store.contains(&file) {
            return;
        }
        next.deleted_live |= s.head.contains(&file);
        if s.history.iter().any(|set| set.contains(&file)) {
            // Only a control lets a delete land after the forget.
            next.early_delete |= entry(&s.retired, file)
                .or_else(|| entry(&s.forgotten, file))
                .and_then(|by| by.tick)
                .is_none_or(|at| s.now < at + self.retention());
        }
    }
    fn commit_enabled(&self, s: &State, w: usize, kind: CommitKind) -> bool {
        let writer = &s.writers[w];
        let ready = writer.generation.is_some()
            && writer.pending.is_none()
            && s.attempts < ATTEMPTS
            && s.prepared.is_none();
        ready
            && match kind {
                CommitKind::Append => {
                    !writer.finished.is_empty()
                        || self.has(ModeFlag::NoUploadBeforeCommit) && !writer.started.is_empty()
                        || self.has(ModeFlag::AdoptByListing) && !Self::unnamed(s).is_empty()
                }
                CommitKind::Compact => {
                    self.retiring() && !writer.finished.is_empty() && !s.head.is_empty()
                }
                CommitKind::CompactCarry => {
                    self.retiring() && !writer.finished.is_empty() && s.head.len() >= 2
                }
                CommitKind::Remove => self.retiring() && !s.head.is_empty(),
                CommitKind::Prepare => self.has(ModeFlag::Prepared) && !writer.finished.is_empty(),
            }
    }
    /// Whether `action` is enabled in `s`. The model lists only enabled
    /// actions, so a seeded walk (`WALK`) runs to its length rather than
    /// stopping at the first disabled pick.
    fn enabled(&self, s: &State, action: &Action) -> bool {
        let writer = |w: usize| &s.writers[w];
        match *action {
            Action::StartUpload(w) => {
                writer(w).generation.is_some()
                    && writer(w).pending.is_none()
                    && s.uploads < self.uploads
            }
            Action::FinishUpload(i) => i < s.puts.len(),
            Action::Commit(w) => self.commit_enabled(s, w, CommitKind::Append),
            Action::Compact(w) => self.commit_enabled(s, w, CommitKind::Compact),
            Action::CompactCarry(w) => self.commit_enabled(s, w, CommitKind::CompactCarry),
            Action::Remove(w) => self.commit_enabled(s, w, CommitKind::Remove),
            Action::Prepare(w) => self.commit_enabled(s, w, CommitKind::Prepare),
            Action::Deliver(i) | Action::Drop(i) => i < s.net.len(),
            Action::Timeout(w) => matches!(writer(w).pending, Some((_, false))),
            Action::Retry(w) => match &writer(w).pending {
                Some((request, true)) => {
                    s.retries < RETRIES && !s.net.iter().any(|r| r.attempt == request.attempt)
                }
                _ => false,
            },
            // The request could still land while the head is at its base, so
            // the outcome stays unknown unless the head shows it landed.
            Action::Resolve(w) => match &writer(w).pending {
                Some((request, true)) => {
                    self.landed(s, request)
                        || s.version != request.base
                        || self.has(ModeFlag::NoResolveByHead)
                }
                _ => false,
            },
            Action::Takeover(w) => s.takes < self.takes() && writer(w).generation != Some(s.lease),
            Action::Crash(w) => s.crashes < self.crashes() && *writer(w) != Writer::default(),
            Action::Read => {
                s.reads < self.reads()
                    && (!self.retiring() || s.local_started.is_none() && !s.head.is_empty())
            }
            Action::Fetch => s
                .snapshot
                .as_ref()
                .is_some_and(|(_, listed)| s.fetched < listed.len()),
            // An unnamed file of a fenced generation: no commit naming it can
            // pass the lease condition. In the `intent` runs: an unnamed file
            // the holder's own intent does not protect.
            Action::Sweep(i) => s.store.get(i).is_some_and(|f| {
                !self.named_for_orphans(s, *f)
                    && if self.has(ModeFlag::Intent) {
                        Self::holder(s).is_some_and(|holder| {
                            self.has(ModeFlag::SweepIgnoresIntent) || !Self::intends(holder, *f)
                        })
                    } else {
                        f.0.generation < s.lease
                    }
            }),
            Action::CommitTx(w) | Action::AbortTx(w) => {
                s.prepared.is_some()
                    && Self::holder_index(s) == Some(w)
                    && writer(w).pending.is_none()
            }
            Action::Plan => self.retiring() && self.pass(s).is_some(),
            Action::DeleteLand(i) => i < s.deletes.len(),
            Action::DeleteFail(i) => i < s.deletes.len() && s.delete_failures < DELETE_FAILURES,
            Action::Forget => s.sweep.as_ref().is_some_and(|sweep| {
                self.has(ModeFlag::ForgetEarly) || !sweep.failed && Self::outstanding(s) == 0
            }),
            Action::EndScan => s.local_started.is_some(),
            Action::RestoreStart => {
                self.retiring()
                    && s.restores < self.restores()
                    && s.restore.is_none()
                    && !s.head.is_empty()
                    && (s.sweep.is_none() || self.has(ModeFlag::IgnoresRestoreHold))
            }
            Action::RestoreCopy => s
                .restore
                .as_ref()
                .is_some_and(|(_, listed, next)| *next < listed.len()),
            Action::RestoreEnd => s.restore.is_some(),
            Action::Tick => self.retiring() && s.now < self.ticks() && self.timed(s),
            Action::RemoteConfirm => {
                s.remote_reads < self.remote_reads() && s.remote.is_none() && !s.head.is_empty()
            }
            Action::RemoteStart => s
                .remote
                .as_ref()
                .is_some_and(|r| r.started.is_none() && s.now <= r.confirmed + STALENESS),
            Action::RemoteFetch => s
                .remote
                .as_ref()
                .is_some_and(|r| r.started.is_some() && r.next < r.keys.len()),
        }
    }
    /// Quiescence for the reclamation check: a lease holder runs with no
    /// commit in flight, no scan, restore point or remote query holds
    /// anything, no transaction is prepared, and only new work, faults or
    /// the clock remain enabled.
    fn quiescent(&self, s: &State) -> bool {
        let Some(holder) = Self::holder(s) else {
            return false;
        };
        if holder.pending.is_some()
            || s.local_started.is_some()
            || s.restore.is_some()
            || s.remote.is_some()
            || s.prepared.is_some()
        {
            return false;
        }
        let mut listed = vec![];
        self.actions(s, &mut listed);
        !listed.into_iter().any(|a| {
            !matches!(
                a,
                Action::StartUpload(_)
                    | Action::Commit(_)
                    | Action::Compact(_)
                    | Action::CompactCarry(_)
                    | Action::Remove(_)
                    | Action::Prepare(_)
                    | Action::Read
                    | Action::RestoreStart
                    | Action::RemoteConfirm
                    | Action::Crash(_)
                    | Action::Takeover(_)
                    | Action::Timeout(_)
                    | Action::Retry(_)
                    | Action::Drop(_)
                    | Action::DeleteFail(_)
                    | Action::Tick
            )
        })
    }
    fn send(&self, s: &State, next: &mut State, w: usize, kind: CommitKind) -> Option<()> {
        let writer = &s.writers[w];
        let generation = writer.generation?;
        // A removal names no uploads; the writer keeps them for a later commit.
        let uploads = kind != CommitKind::Remove;
        let mut files = if uploads {
            writer.finished.clone()
        } else {
            vec![]
        };
        if self.has(ModeFlag::NoUploadBeforeCommit) {
            files.extend(writer.started.iter().copied());
        }
        if self.has(ModeFlag::AdoptByListing) {
            let adopted: Vec<File> = Self::unnamed(s)
                .into_iter()
                .filter(|f| !files.contains(f))
                .collect();
            files.extend(adopted);
        }
        files.sort_unstable();
        // The writer reads the head it commits on; the head is sorted, so the
        // oldest file is first.
        let (carry, remove): (Vec<File>, Vec<File>) = match kind {
            CommitKind::Append => (vec![], vec![]),
            CommitKind::Compact | CommitKind::Prepare => (vec![], s.head.clone()),
            CommitKind::CompactCarry => (s.head[..1].to_vec(), s.head[1..].to_vec()),
            CommitKind::Remove => (vec![], s.head[..1].to_vec()),
        };
        let request = Request {
            attempt: s.attempts,
            writer: w,
            generation,
            base: s.version,
            files,
            kind,
            carry,
            remove,
        };
        next.attempts += 1;
        let writer = &mut next.writers[w];
        if uploads {
            writer.finished.clear();
        }
        if self.has(ModeFlag::NoUploadBeforeCommit) {
            writer.started.clear();
        }
        writer.pending = Some((request.clone(), false));
        next.net.push(request);
        Some(())
    }
    /// Apply a record at the new head version that removes `remove` from the
    /// live set, carries `carry` and adds `add`; it retires what it removes.
    fn replace(&self, next: &mut State, remove: &[File], carry: &[File], add: &[File]) {
        let base = next.head.clone();
        next.head.retain(|f| !remove.contains(f));
        insert_sorted(&mut next.head, add.iter().copied());
        if !remove.is_empty() {
            insert_sorted(
                &mut next.shared,
                base.iter().filter(|f| !remove.contains(f)).copied(),
            );
        }
        insert_sorted(&mut next.carried, carry.iter().copied());
        let retiring: Vec<File> = if self.has(ModeFlag::WholeSnapshot) && !remove.is_empty() {
            base
        } else if self.has(ModeFlag::RetiresCarried) {
            remove.iter().chain(carry).copied().collect()
        } else {
            remove.to_vec()
        };
        for f in retiring {
            let removal = Removal {
                version: next.version,
                tick: None,
            };
            insert_first(&mut next.retired, f, removal);
        }
        // The ghost tick records when each file actually left the live set,
        // whichever record retired it.
        for (f, by) in &mut next.retired {
            if by.tick.is_none() && !next.head.contains(f) {
                by.tick = Some(next.now);
            }
        }
    }
    /// Apply an accepted request to the `retire` head.
    fn publish(&self, next: &mut State, request: &Request) {
        insert_sorted(&mut next.applied, [request.attempt]);
        if request.kind == CommitKind::Prepare {
            next.prepared = Some(Prepared {
                id: request.attempt,
                files: request.files.clone(),
                remove: request.remove.clone(),
            });
        } else {
            self.replace(next, &request.remove, &request.carry, &request.files);
        }
    }
}

impl Model for Protocol {
    type State = State;
    type Action = Action;
    fn init_states(&self) -> Vec<State> {
        let mut writers = vec![Writer::default(); WRITERS];
        writers[0].generation = Some(1);
        vec![State {
            version: 0,
            lease: 1,
            head: vec![],
            history: vec![vec![]],
            store: vec![],
            puts: vec![],
            net: vec![],
            writers,
            snapshot: None,
            fetched: 0,
            observed: vec![],
            aborted: vec![],
            dropped: vec![],
            resolutions: vec![],
            uploads: 0,
            attempts: 0,
            takes: 0,
            crashes: 0,
            retries: 0,
            reads: 0,
            accepted: false,
            fenced_refused: false,
            fenced_landed: false,
            reclaimed: false,
            now: 0,
            retired: vec![],
            applied: vec![],
            prepared: None,
            sweep: None,
            deletes: vec![],
            local_started: None,
            restore: None,
            copied: vec![],
            remote: None,
            remote_reads: 0,
            shared: vec![],
            forgotten: vec![],
            interrupted: vec![],
            attempt_resolutions: vec![],
            restores: 0,
            delete_failures: 0,
            early_delete: false,
            deleted_live: false,
            remote_missed: false,
            local_missed: false,
            local_expired_missing: false,
            remote_read_removed: false,
            remote_late_missed: false,
            fetched_retired: false,
            carried: vec![],
            forgot_shared: false,
            forgot_carried: false,
            landed_after_takeover: false,
            resumed: false,
            copied_retired: false,
            tx_committed: false,
            tx_aborted: false,
            planned_while_reserved: false,
        }]
    }
    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        for w in 0..WRITERS {
            a.extend([
                Action::StartUpload(w),
                Action::Commit(w),
                Action::Timeout(w),
                Action::Retry(w),
                Action::Resolve(w),
                Action::Takeover(w),
                Action::Crash(w),
            ]);
            if self.retiring() {
                a.extend([
                    Action::Compact(w),
                    Action::CompactCarry(w),
                    Action::Remove(w),
                    Action::Prepare(w),
                    Action::CommitTx(w),
                    Action::AbortTx(w),
                ]);
            }
        }
        a.extend((0..s.puts.len()).map(Action::FinishUpload));
        for i in 0..s.net.len() {
            a.extend([Action::Deliver(i), Action::Drop(i)]);
        }
        a.extend([Action::Read, Action::Fetch]);
        a.extend((0..s.store.len()).map(Action::Sweep));
        if self.retiring() {
            a.extend([
                Action::Plan,
                Action::Forget,
                Action::EndScan,
                Action::RestoreStart,
                Action::RestoreCopy,
                Action::RestoreEnd,
                Action::Tick,
                Action::RemoteConfirm,
                Action::RemoteStart,
                Action::RemoteFetch,
            ]);
            for i in 0..s.deletes.len() {
                a.extend([Action::DeleteLand(i), Action::DeleteFail(i)]);
            }
        }
        a.retain(|action| self.enabled(s, action));
    }
    #[expect(clippy::too_many_lines, reason = "one arm per action")]
    fn next_state(&self, s: &State, action: Action) -> Option<State> {
        if !self.enabled(s, &action) {
            return None;
        }
        let mut next = s.clone();
        match action {
            Action::StartUpload(w) => {
                // A writer that believes it holds the lease uploads to a fresh
                // final key, fenced or not.
                let generation = s.writers[w].generation?;
                let file = (
                    Key {
                        generation,
                        id: s.uploads,
                    },
                    u8::try_from(w).ok()?,
                );
                next.uploads += 1;
                next.puts.push((w, file));
                next.writers[w].started.push(file);
            }
            Action::FinishUpload(i) => {
                let (w, file) = *s.puts.get(i)?;
                next.puts.remove(i);
                insert_sorted(&mut next.store, [file]);
                // The response reaches the writer only if it still knows the upload.
                let writer = &mut next.writers[w];
                if let Some(at) = writer.started.iter().position(|f| *f == file) {
                    writer.started.remove(at);
                    writer.finished.push(file);
                    // A running writer, fenced by another's takeover, is told
                    // its upload landed.
                    next.fenced_landed |= file.0.generation < s.lease;
                }
            }
            Action::Commit(w) => self.send(s, &mut next, w, CommitKind::Append)?,
            Action::Compact(w) => self.send(s, &mut next, w, CommitKind::Compact)?,
            Action::CompactCarry(w) => self.send(s, &mut next, w, CommitKind::CompactCarry)?,
            Action::Remove(w) => self.send(s, &mut next, w, CommitKind::Remove)?,
            Action::Prepare(w) => self.send(s, &mut next, w, CommitKind::Prepare)?,
            Action::Deliver(i) => {
                // The store evaluates the conditional publication: the exact
                // base version and the lease generation.
                let request = s.net.get(i)?.clone();
                next.net.remove(i);
                let accepted = s.version == request.base && s.lease == request.generation;
                if accepted {
                    next.version += 1;
                    if self.retiring() {
                        self.publish(&mut next, &request);
                    } else {
                        insert_sorted(&mut next.head, request.files.iter().copied());
                    }
                    next.history.push(next.head.clone());
                    next.accepted = true;
                }
                let writer = &mut next.writers[request.writer];
                let waiting = writer
                    .pending
                    .as_ref()
                    .is_some_and(|(p, lost)| p.attempt == request.attempt && !lost);
                if waiting {
                    writer.pending = None;
                    if !accepted {
                        insert_sorted(&mut next.aborted, keys(&request.files));
                        if s.lease != request.generation {
                            // The refusal tells a fenced writer it lost the lease.
                            next.fenced_refused = true;
                            let unnamed: Vec<Key> = keys(&writer.started)
                                .chain(keys(&writer.finished))
                                .collect();
                            *writer = Writer::default();
                            insert_sorted(&mut next.dropped, unnamed);
                            if next
                                .sweep
                                .as_ref()
                                .is_some_and(|sweep| sweep.writer == request.writer)
                            {
                                Self::end_sweep(&mut next);
                            }
                        }
                    }
                }
            }
            Action::Drop(i) => {
                s.net.get(i)?;
                next.net.remove(i);
            }
            Action::Timeout(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                next.writers[w].pending = Some((request.clone(), true));
            }
            Action::Retry(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                next.retries += 1;
                next.net.push(request.clone());
            }
            Action::Resolve(w) => {
                let (request, _) = s.writers[w].pending.as_ref()?;
                let committed = self.landed(s, request);
                if !committed {
                    insert_sorted(&mut next.aborted, keys(&request.files));
                }
                if self.retiring() {
                    next.attempt_resolutions.push((request.attempt, committed));
                } else {
                    next.resolutions.push((request.files.clone(), committed));
                }
                next.writers[w].pending = None;
            }
            Action::Takeover(w) => {
                // A head commit raising the generation; the new holder starts
                // with nothing of its own (no timers either), and its earlier
                // sends stay in flight.
                next.takes += 1;
                next.lease += 1;
                next.version += 1;
                next.history.push(next.head.clone());
                let old = &s.writers[w];
                insert_sorted(
                    &mut next.dropped,
                    keys(&old.started).chain(keys(&old.finished)),
                );
                next.writers[w] = Writer {
                    generation: Some(next.lease),
                    ..Writer::default()
                };
                if s.sweep.as_ref().is_some_and(|sweep| sweep.writer == w) {
                    Self::end_sweep(&mut next);
                }
            }
            Action::Crash(w) => {
                let old = &s.writers[w];
                next.crashes += 1;
                insert_sorted(
                    &mut next.dropped,
                    keys(&old.started).chain(keys(&old.finished)),
                );
                next.writers[w] = Writer::default();
                if s.sweep.as_ref().is_some_and(|sweep| sweep.writer == w) {
                    Self::end_sweep(&mut next);
                }
            }
            Action::Read => {
                next.reads += 1;
                let listed = if self.has(ModeFlag::NoCommittedRead) {
                    keys(&s.store).collect()
                } else {
                    keys(&s.head).collect()
                };
                next.snapshot = Some((s.version, listed));
                next.fetched = 0;
                if self.retiring() {
                    // Local head capture has no staleness: S = 0.
                    next.local_started = Some(s.now);
                }
            }
            Action::Fetch => {
                let (version, listed) = s.snapshot.as_ref()?;
                let key = *listed.get(s.fetched)?;
                let bytes = s.store.iter().find(|(k, _)| *k == key).map(|(_, b)| *b);
                next.local_missed |= s
                    .local_started
                    .is_some_and(|start| s.now < start + self.retention())
                    && bytes.is_none();
                next.local_expired_missing |= s
                    .local_started
                    .is_some_and(|start| s.now >= start + self.retention())
                    && bytes.is_none();
                next.observed.push((*version, key, bytes));
                next.fetched += 1;
                next.fetched_retired |= bytes.is_some() && !s.head.iter().any(|(k, _)| *k == key);
            }
            Action::EndScan => {
                next.local_started = None;
                next.snapshot = None;
                next.fetched = 0;
            }
            Action::Sweep(i) => {
                let file = *s.store.get(i)?;
                self.deleting(s, &mut next, file);
                next.store.remove(i);
                next.reclaimed = true;
            }
            Action::CommitTx(_) | Action::AbortTx(_) => {
                let tx = s.prepared.clone()?;
                next.prepared = None;
                next.version += 1;
                if matches!(action, Action::CommitTx(_)) {
                    self.replace(&mut next, &tx.remove, &[], &tx.files);
                    next.tx_committed = true;
                } else {
                    insert_sorted(&mut next.aborted, keys(&tx.files));
                    next.tx_aborted = true;
                }
                next.history.push(next.head.clone());
            }
            Action::Plan => {
                let pass = self.pass(s)?;
                let generation = s.writers[pass.writer].generation?;
                let now = s.now;
                let writer = &mut next.writers[pass.writer];
                for f in pass.observe {
                    insert_first(&mut writer.timers, f, now);
                }
                if !pass.delete.is_empty() {
                    insert_sorted(
                        &mut next.deletes,
                        pass.delete.iter().map(|f| (*f, generation, true)),
                    );
                    next.sweep = Some(Sweep {
                        writer: pass.writer,
                        generation,
                        files: pass.delete,
                        failed: false,
                    });
                    next.planned_while_reserved |= s.prepared.is_some();
                }
            }
            Action::DeleteLand(i) => {
                let (file, generation, _) = *s.deletes.get(i)?;
                next.deletes.remove(i);
                self.deleting(s, &mut next, file);
                if let Ok(at) = next.store.binary_search(&file) {
                    next.store.remove(at);
                }
                next.landed_after_takeover |= generation < s.lease;
                Self::settle_sweep(&mut next);
            }
            Action::DeleteFail(i) => {
                let (_, _, owned) = *s.deletes.get(i)?;
                next.deletes.remove(i);
                next.delete_failures += 1;
                if owned && let Some(sweep) = &mut next.sweep {
                    sweep.failed = true;
                }
                Self::settle_sweep(&mut next);
            }
            Action::Forget => {
                let sweep = s.sweep.clone()?;
                next.sweep = None;
                for delete in &mut next.deletes {
                    delete.2 = false;
                }
                next.deletes.sort_unstable();
                // One `ForgetRetired` record, through the sweeping process's
                // serial writer: a fenced process's commit is refused, and a
                // reserved table refuses it before any request.
                let committed = s.writers[sweep.writer].generation == Some(sweep.generation)
                    && sweep.generation == s.lease
                    && s.prepared.is_none();
                if committed {
                    next.version += 1;
                    let (gone, kept): (Vec<Retired>, Vec<Retired>) = next
                        .retired
                        .drain(..)
                        .partition(|(f, _)| sweep.files.contains(f));
                    next.retired = kept;
                    next.writers[sweep.writer]
                        .timers
                        .retain(|(f, _)| !sweep.files.contains(f));
                    next.resumed |= sweep
                        .files
                        .iter()
                        .any(|f| s.interrupted.binary_search(f).is_ok());
                    next.forgot_shared |= sweep
                        .files
                        .iter()
                        .any(|f| s.shared.binary_search(f).is_ok());
                    next.forgot_carried |= sweep
                        .files
                        .iter()
                        .any(|f| s.carried.binary_search(f).is_ok());
                    insert_sorted(&mut next.forgotten, gone);
                    next.history.push(next.head.clone());
                } else {
                    insert_sorted(&mut next.interrupted, sweep.files);
                }
            }
            Action::RestoreStart => {
                next.restores += 1;
                next.restore = Some((s.version, keys(&s.head).collect(), 0));
            }
            Action::RestoreCopy => {
                let (_, listed, at) = s.restore.as_ref()?;
                let key = *listed.get(*at)?;
                let found = s.store.iter().any(|(k, _)| *k == key);
                next.copied.push((key, found));
                next.copied_retired |= found && !s.head.iter().any(|(k, _)| *k == key);
                if let Some(restore) = &mut next.restore {
                    restore.2 += 1;
                }
            }
            Action::RestoreEnd => next.restore = None,
            Action::Tick => next.now += 1,
            Action::RemoteConfirm => {
                next.remote_reads += 1;
                next.remote = Some(Remote {
                    version: s.version,
                    keys: keys(&s.head).collect(),
                    confirmed: s.now,
                    started: None,
                    next: 0,
                });
            }
            Action::RemoteStart => {
                if let Some(remote) = &mut next.remote {
                    remote.started = Some(s.now);
                }
            }
            Action::RemoteFetch => {
                let remote = s.remote.as_ref()?;
                let key = *remote.keys.get(remote.next)?;
                let found = s.store.iter().any(|(k, _)| *k == key);
                let within = s.now < remote.started? + RETENTION - STALENESS;
                next.remote_missed |= within && !found;
                next.remote_read_removed |= found && !s.head.iter().any(|(k, _)| *k == key);
                next.remote_late_missed |= !within && !found;
                if remote.next + 1 == remote.keys.len() {
                    // The query ends.
                    next.remote = None;
                } else if let Some(remote) = &mut next.remote {
                    remote.next += 1;
                }
            }
        }
        (next != *s).then_some(next)
    }
    #[expect(clippy::too_many_lines, reason = "one entry per property")]
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("committed files exist", |_, s: &State| {
                s.head.iter().all(|f| s.store.contains(f))
                    && s.prepared
                        .as_ref()
                        .is_none_or(|tx| tx.files.iter().all(|f| s.store.contains(f)))
            }),
            Property::always(
                "reader sees only committed files",
                |model: &Self, s: &State| {
                    s.observed.iter().all(|&(version, key, bytes)| {
                        model.retiring() && bytes.is_none()
                            || bytes.is_some_and(|b| {
                                s.history
                                    .get(usize::from(version))
                                    .is_some_and(|set| set.contains(&(key, b)))
                            })
                    })
                },
            ),
            Property::always(
                "aborted or abandoned upload never visible to a reader",
                |_, s: &State| {
                    s.observed.iter().all(|(_, key, _)| {
                        s.aborted.binary_search(key).is_err()
                            && s.dropped.binary_search(key).is_err()
                    })
                },
            ),
            Property::always(
                "unknown outcome resolves all-or-nothing",
                |model: &Self, s: &State| {
                    if model.retiring() {
                        return s
                            .attempt_resolutions
                            .iter()
                            .all(|(attempt, committed)| s.applied.contains(attempt) == *committed);
                    }
                    s.resolutions.iter().all(|(files, committed)| {
                        let named = Self::named(s, files);
                        if *committed {
                            named == files.len()
                        } else {
                            named == 0
                        }
                    })
                },
            ),
            Property::sometimes("commit accepted", |_, s: &State| s.accepted),
            Property::sometimes("orphan upload after an aborted commit", |_, s: &State| {
                s.store
                    .iter()
                    .any(|f| s.aborted.binary_search(&f.0).is_ok() && !s.head.contains(f))
            }),
            Property::sometimes("fenced commit refused", |_, s: &State| s.fenced_refused),
            Property::sometimes(
                "fenced writer's upload lands after a takeover",
                |_, s: &State| s.fenced_landed,
            ),
            Property::sometimes(
                "unknown outcome resolved as committed",
                |model: &Self, s: &State| {
                    if model.retiring() {
                        s.attempt_resolutions
                            .iter()
                            .any(|(_, committed)| *committed)
                    } else {
                        s.resolutions.iter().any(|(_, committed)| *committed)
                    }
                },
            ),
            Property::sometimes(
                "unknown outcome resolved as aborted",
                |model: &Self, s: &State| {
                    if model.retiring() {
                        s.attempt_resolutions
                            .iter()
                            .any(|(_, committed)| !*committed)
                    } else {
                        s.resolutions.iter().any(|(_, committed)| !*committed)
                    }
                },
            ),
            Property::sometimes("reader fetches a committed file", |_, s: &State| {
                s.observed.iter().any(|(_, _, bytes)| bytes.is_some())
            }),
            Property::sometimes("orphan reclaimed", |_, s: &State| s.reclaimed),
        ];
        if !self.retiring() {
            return properties;
        }
        properties.extend([
            // I15, per file.
            Property::always(
                "data file deleted only T after the record that removed it",
                |_, s: &State| !s.early_delete,
            ),
            Property::always(
                "carried or shared file never deleted while live",
                |_, s: &State| !s.deleted_live,
            ),
            Property::always("local scan within T finds every file", |_, s: &State| {
                !s.local_missed
            }),
            Property::always(
                "remote reader within T - S finds every file",
                |_, s: &State| !s.remote_missed,
            ),
            Property::always("restore point copies every file", |_, s: &State| {
                s.copied.iter().all(|(_, found)| *found)
            }),
            Property::always("forgotten file leaves nothing behind", |_, s: &State| {
                s.forgotten.iter().all(|(f, _)| !s.store.contains(f))
            }),
            // Bounded: at quiescence each retired file waits out T on the
            // holder's timer, so nothing reclaimable is left behind.
            Property::always(
                "retired files reclaimed at quiescence once T has passed",
                |model: &Self, s: &State| {
                    !model.quiescent(s)
                        || Self::holder(s).is_some_and(|holder| {
                            s.retired.iter().all(|(f, _)| {
                                model.retention() > 0
                                    && entry(&holder.timers, *f)
                                        .is_some_and(|since| s.now < since + model.retention())
                            })
                        })
                },
            ),
            Property::sometimes(
                "file retired by the record that removed it",
                |_, s: &State| !s.retired.is_empty() || !s.forgotten.is_empty(),
            ),
            Property::sometimes("retired file deleted and forgotten", |_, s: &State| {
                !s.forgotten.is_empty()
            }),
            Property::sometimes(
                "file shared across a removal later deleted and forgotten",
                |_, s: &State| s.forgot_shared,
            ),
            Property::sometimes(
                "sweep waits for T while a local scan runs",
                |model: &Self, s: &State| {
                    s.local_started.is_some()
                        && Self::holder(s).is_some_and(|holder| {
                            s.retired.iter().any(|(f, _)| !model.waited(s, holder, *f))
                        })
                },
            ),
            Property::sometimes("local scan past T finds a file deleted", |_, s: &State| {
                s.local_expired_missing
            }),
            Property::sometimes(
                "scan reads a file of its commit after the file was removed",
                |_, s: &State| s.fetched_retired,
            ),
            Property::sometimes(
                "remote reader reads a file removed after its confirmation",
                |_, s: &State| s.remote_read_removed,
            ),
            Property::sometimes(
                "remote reader past T - S finds a file deleted",
                |_, s: &State| s.remote_late_missed,
            ),
            Property::sometimes("delete lands after a takeover", |_, s: &State| {
                s.landed_after_takeover
            }),
            Property::sometimes(
                "interrupted sweep's files forgotten by a later sweep",
                |_, s: &State| s.resumed,
            ),
            Property::sometimes(
                "restore point copies a file removed after it started",
                |_, s: &State| s.copied_retired,
            ),
        ]);
        if self.uploads >= 3 {
            properties.push(Property::sometimes(
                "file a compaction carried in place later deleted and forgotten",
                |_, s: &State| s.forgot_carried,
            ));
        }
        if self.has(ModeFlag::Prepared) {
            properties.extend([
                Property::sometimes("prepared transaction committed", |_, s: &State| {
                    s.tx_committed
                }),
                Property::sometimes("prepared transaction aborted", |_, s: &State| s.tx_aborted),
            ]);
        }
        if self.has(ModeFlag::IgnoresReservation) {
            properties.push(Property::sometimes(
                "sweep plans while a transaction is prepared",
                |_, s: &State| s.planned_while_reserved,
            ));
        }
        // A graph checks only the readers it has: the others would hold
        // vacuously, and their witnesses cannot be reached.
        let local = self.reads() > 0;
        let restore = self.restores() > 0;
        let remote = self.remote_reads() > 0;
        properties.retain(|p| match p.name {
            "reader fetches a committed file"
            | "local scan within T finds every file"
            | "sweep waits for T while a local scan runs"
            | "local scan past T finds a file deleted"
            | "scan reads a file of its commit after the file was removed" => local,
            "restore point copies every file"
            | "restore point copies a file removed after it started" => restore,
            "remote reader within T - S finds every file"
            | "remote reader reads a file removed after its confirmation"
            | "remote reader past T - S finds a file deleted" => remote,
            _ => true,
        });
        properties
    }
}

/// The real operation one action of a `data-files` trace stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// `StartUpload(w)`: writer `w` sends a PUT of a fresh data file to its
    /// final key under the dataset prefix (the replay names `key` uniquely).
    PutSent { writer: usize, key: Key },
    /// `FinishUpload(i)`: that PUT lands in the store; `acknowledged` when the
    /// writer still knows the upload and records it as uploaded.
    PutLanded {
        writer: usize,
        key: Key,
        acknowledged: bool,
    },
    /// `Commit(w)`, `Compact(w)`, `CompactCarry(w)`, `Remove(w)`,
    /// `Prepare(w)`: writer `w` sends a conditional head publication on
    /// `base_version` and `generation` that adds `files` (new uploads),
    /// carries `carry` in place and removes (so retires) `remove`, as attempt
    /// `attempt`.
    CommitSent {
        writer: usize,
        attempt: u8,
        generation: u8,
        base_version: u8,
        files: Vec<Key>,
        kind: CommitKind,
        carry: Vec<Key>,
        remove: Vec<Key>,
    },
    /// `Deliver(i)`: the store evaluates a commit request; `accepted` when it
    /// published, `fenced` when the lease generation refused it, `answered`
    /// when the writer was waiting for the response (otherwise it was lost).
    CommitApplied {
        writer: usize,
        attempt: u8,
        accepted: bool,
        fenced: bool,
        answered: bool,
    },
    /// `Drop(i)`: a commit request is lost before reaching the store.
    CommitDropped { writer: usize, attempt: u8 },
    /// `Timeout(w)`: the writer gives up on the response: an unknown outcome.
    ResponseLost { writer: usize, attempt: u8 },
    /// `Retry(w)`: the identical request is sent again.
    CommitResent { writer: usize, attempt: u8 },
    /// `Resolve(w)`: the writer reads the head and resolves its unknown outcome.
    Resolved {
        writer: usize,
        attempt: u8,
        committed: bool,
    },
    /// `Takeover(w)`: writer `w` takes the lease by a head commit raising the
    /// generation to `generation`.
    Takeover { writer: usize, generation: u8 },
    /// `Crash(w)`: the writer process restarts and forgets its state.
    Crash { writer: usize },
    /// `Read`: the reader reads the head (`version`, `files`), and in the
    /// `retire` runs starts a local scan with S = 0 (`local`).
    ReadHead {
        version: u8,
        files: Vec<Key>,
        local: bool,
    },
    /// `Fetch`: the reader GETs the next file of its snapshot; `found` when the
    /// store returned it.
    Fetch { key: Key, found: bool },
    /// `EndScan`: the local scan ends.
    ScanEnded,
    /// `Sweep(i)`: the orphan sweep deletes an unnamed file.
    Swept { key: Key },
    /// `CommitTx(w)` / `AbortTx(w)`: the lease holder commits the resolve
    /// record of the prepared transaction `id`.
    TxResolved { writer: usize, id: u8, commit: bool },
    /// `Plan`: the holder's sweep pass reads the committed retired set, starts
    /// a retention timer for each file in `observed`, and sends the deletes of
    /// `deletes` (none when no file is due).
    SweepPlanned {
        writer: usize,
        generation: u8,
        observed: Vec<Key>,
        deletes: Vec<Key>,
    },
    /// `DeleteLand(i)`: a delete request lands; `acknowledged` when the sweep
    /// that sent it still waits for it.
    DeleteLanded { key: Key, acknowledged: bool },
    /// `DeleteFail(i)`: a delete request fails and the object stays.
    DeleteFailed { key: Key, acknowledged: bool },
    /// `Forget`: the sweep commits the `ForgetRetired` record for `files`;
    /// `committed` when the head accepted it.
    Forgotten {
        writer: usize,
        generation: u8,
        files: Vec<Key>,
        committed: bool,
    },
    /// `RestoreStart`: a restore point reads the head and holds deletions.
    RestoreStarted { version: u8, files: Vec<Key> },
    /// `RestoreCopy`: the restore point GETs its next file.
    RestoreCopied { key: Key, found: bool },
    /// `RestoreEnd`: the restore point releases its hold.
    RestoreEnded,
    /// `Tick`: the logical clock advances to `now`.
    Ticked { now: u8 },
    /// `RemoteConfirm`: a reader on another instance confirms the head
    /// (`version`, `files`) with a GET sent at tick `at`.
    RemoteConfirmed {
        version: u8,
        files: Vec<Key>,
        at: u8,
    },
    /// `RemoteStart`: its query starts at tick `at`, within S of the GET.
    RemoteStarted { at: u8 },
    /// `RemoteFetch`: it GETs its next file; `within_bound` when less than
    /// T − S has passed since the query started.
    RemoteFetched {
        key: Key,
        found: bool,
        within_bound: bool,
    },
}

/// What one action of a `data-files` trace does, as the model takes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub action: String,
    pub op: Op,
    /// The head after the action: version, lease generation and file set
    /// (in the `retire` runs, the live set).
    pub version: u8,
    pub lease: u8,
    pub head: Vec<File>,
    /// The store's data files after the action.
    pub store: Vec<File>,
    /// In the `retire` runs: the retired files with the record that removed
    /// each, the prepared transaction, and the clock
    /// after the action.
    pub retired: Vec<Retired>,
    pub prepared: Option<Prepared>,
    pub now: u8,
}

fn parse(text: &str) -> Option<Action> {
    let (name, arguments) = match text.split_once('(') {
        Some((name, rest)) => (
            name,
            rest.strip_suffix(')')?
                .split(',')
                .map(|part| part.trim().parse::<usize>().ok())
                .collect::<Option<Vec<_>>>()?,
        ),
        None => (text, Vec::new()),
    };
    Some(match (name, arguments.as_slice()) {
        ("StartUpload", &[w]) => Action::StartUpload(w),
        ("FinishUpload", &[i]) => Action::FinishUpload(i),
        ("Commit", &[w]) => Action::Commit(w),
        ("Compact", &[w]) => Action::Compact(w),
        ("CompactCarry", &[w]) => Action::CompactCarry(w),
        ("Remove", &[w]) => Action::Remove(w),
        ("Prepare", &[w]) => Action::Prepare(w),
        ("CommitTx", &[w]) => Action::CommitTx(w),
        ("AbortTx", &[w]) => Action::AbortTx(w),
        ("Deliver", &[i]) => Action::Deliver(i),
        ("Drop", &[i]) => Action::Drop(i),
        ("Timeout", &[w]) => Action::Timeout(w),
        ("Retry", &[w]) => Action::Retry(w),
        ("Resolve", &[w]) => Action::Resolve(w),
        ("Takeover", &[w]) => Action::Takeover(w),
        ("Crash", &[w]) => Action::Crash(w),
        ("Read", &[]) => Action::Read,
        ("Fetch", &[]) => Action::Fetch,
        ("EndScan", &[]) => Action::EndScan,
        ("Sweep", &[i]) => Action::Sweep(i),
        ("Plan", &[]) => Action::Plan,
        ("DeleteLand", &[i]) => Action::DeleteLand(i),
        ("DeleteFail", &[i]) => Action::DeleteFail(i),
        ("Forget", &[]) => Action::Forget,
        ("RestoreStart", &[]) => Action::RestoreStart,
        ("RestoreCopy", &[]) => Action::RestoreCopy,
        ("RestoreEnd", &[]) => Action::RestoreEnd,
        ("Tick", &[]) => Action::Tick,
        ("RemoteConfirm", &[]) => Action::RemoteConfirm,
        ("RemoteStart", &[]) => Action::RemoteStart,
        ("RemoteFetch", &[]) => Action::RemoteFetch,
        _ => return None,
    })
}

/// Step `actions` through the model in `mode`, or `None` when one is not
/// enabled where it occurs.
#[must_use]
pub fn steps(mode: &str, bound: usize, actions: &[String]) -> Option<Vec<Step>> {
    let protocol = Protocol::new(mode, bound);
    let mut state = protocol.init_states().pop()?;
    let mut out = Vec::with_capacity(actions.len());
    for text in actions {
        let action = parse(text)?;
        let mut enabled = vec![];
        protocol.actions(&state, &mut enabled);
        if !enabled.contains(&action) {
            return None;
        }
        let next = protocol.next_state(&state, action.clone())?;
        let pending = |w: usize| state.writers[w].pending.as_ref().map(|(p, _)| p.attempt);
        let op = match action {
            Action::StartUpload(w) => Op::PutSent {
                writer: w,
                key: next.writers[w].started.last()?.0,
            },
            Action::FinishUpload(i) => {
                let (w, (key, _)) = state.puts[i];
                Op::PutLanded {
                    writer: w,
                    key,
                    acknowledged: next.writers[w].finished.iter().any(|f| f.0 == key),
                }
            }
            Action::Commit(w)
            | Action::Compact(w)
            | Action::CompactCarry(w)
            | Action::Remove(w)
            | Action::Prepare(w) => {
                let request = next.net.last()?;
                Op::CommitSent {
                    writer: w,
                    attempt: request.attempt,
                    generation: request.generation,
                    base_version: request.base,
                    files: keys(&request.files).collect(),
                    kind: request.kind,
                    carry: keys(&request.carry).collect(),
                    remove: keys(&request.remove).collect(),
                }
            }
            Action::Deliver(i) => {
                let request = &state.net[i];
                Op::CommitApplied {
                    writer: request.writer,
                    attempt: request.attempt,
                    accepted: next.version > state.version,
                    fenced: request.generation != state.lease,
                    answered: state.writers[request.writer]
                        .pending
                        .as_ref()
                        .is_some_and(|(p, lost)| p.attempt == request.attempt && !lost),
                }
            }
            Action::Drop(i) => Op::CommitDropped {
                writer: state.net[i].writer,
                attempt: state.net[i].attempt,
            },
            Action::Timeout(w) => Op::ResponseLost {
                writer: w,
                attempt: pending(w)?,
            },
            Action::Retry(w) => Op::CommitResent {
                writer: w,
                attempt: pending(w)?,
            },
            Action::Resolve(w) => Op::Resolved {
                writer: w,
                attempt: pending(w)?,
                committed: if protocol.retiring() {
                    next.attempt_resolutions.last()?.1
                } else {
                    next.resolutions.last()?.1
                },
            },
            Action::Takeover(w) => Op::Takeover {
                writer: w,
                generation: next.lease,
            },
            Action::Crash(w) => Op::Crash { writer: w },
            Action::Read => {
                let (version, files) = next.snapshot.clone()?;
                Op::ReadHead {
                    version,
                    files,
                    local: next.local_started.is_some(),
                }
            }
            Action::Fetch => {
                let &(_, key, bytes) = next.observed.last()?;
                Op::Fetch {
                    key,
                    found: bytes.is_some(),
                }
            }
            Action::EndScan => Op::ScanEnded,
            Action::Sweep(i) => Op::Swept {
                key: state.store[i].0,
            },
            Action::CommitTx(w) | Action::AbortTx(w) => Op::TxResolved {
                writer: w,
                id: state.prepared.as_ref()?.id,
                commit: matches!(action, Action::CommitTx(_)),
            },
            Action::Plan => {
                let pass = protocol.pass(&state)?;
                Op::SweepPlanned {
                    writer: pass.writer,
                    generation: state.writers[pass.writer].generation?,
                    observed: keys(&pass.observe).collect(),
                    deletes: keys(&pass.delete).collect(),
                }
            }
            Action::DeleteLand(i) => Op::DeleteLanded {
                key: state.deletes[i].0.0,
                acknowledged: state.deletes[i].2,
            },
            Action::DeleteFail(i) => Op::DeleteFailed {
                key: state.deletes[i].0.0,
                acknowledged: state.deletes[i].2,
            },
            Action::Forget => {
                let sweep = state.sweep.as_ref()?;
                Op::Forgotten {
                    writer: sweep.writer,
                    generation: sweep.generation,
                    files: keys(&sweep.files).collect(),
                    committed: next.version > state.version,
                }
            }
            Action::RestoreStart => {
                let (version, files, _) = next.restore.clone()?;
                Op::RestoreStarted { version, files }
            }
            Action::RestoreCopy => {
                let &(key, found) = next.copied.last()?;
                Op::RestoreCopied { key, found }
            }
            Action::RestoreEnd => Op::RestoreEnded,
            Action::Tick => Op::Ticked { now: next.now },
            Action::RemoteConfirm => {
                let remote = next.remote.as_ref()?;
                Op::RemoteConfirmed {
                    version: remote.version,
                    files: remote.keys.clone(),
                    at: remote.confirmed,
                }
            }
            Action::RemoteStart => Op::RemoteStarted { at: state.now },
            Action::RemoteFetch => {
                let remote = state.remote.as_ref()?;
                let key = *remote.keys.get(remote.next)?;
                Op::RemoteFetched {
                    key,
                    found: state.store.iter().any(|(k, _)| *k == key),
                    within_bound: state.now < remote.started? + RETENTION - STALENESS,
                }
            }
        };
        out.push(Step {
            action: text.clone(),
            op,
            version: next.version,
            lease: next.lease,
            head: next.head.clone(),
            store: next.store.clone(),
            retired: next.retired.clone(),
            prepared: next.prepared.clone(),
            now: next.now,
        });
        state = next;
    }
    Some(out)
}
