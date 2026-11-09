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
//! In the `retire` runs (on top of `intent`) the head holds snapshots: each
//! accepted commit is a snapshot named by its attempt, and the live set is the
//! snapshots no commit has retired. A compaction is a commit that, in the same
//! record, adds a new snapshot and retires every snapshot live at its base; it
//! may keep a file of a retired snapshot in place (the new snapshot references
//! it). The head records each accepted attempt, so an unknown outcome resolves
//! by attempt, as the head log does, rather than by files a later compaction
//! may retire. Reclamation runs in the lease holder's process: a sweep reads
//! the committed retired set, and declines while the holder has a commit whose
//! outcome it does not know (its local view would include a change that may not
//! land), while a prepared transaction reserves the table, and while a restore
//! point holds deletions. It skips each retired snapshot a scan pins, plans the
//! deletes of the files of the rest that no live or pinned snapshot references
//! (a compaction can keep a retired snapshot's file in place), and, only
//! after every delete has returned, commits one record that forgets those
//! snapshots (the retired entries and their files' rows together). Deletes are
//! requests that can land late (after a crash or a takeover of the sweeping
//! process) or fail; a sweep with a failed delete or a crash before its forget
//! leaves the snapshots retired, and a later sweep deletes again (a delete of a
//! missing key is a no-op) and forgets them. A scan reads the head and pins its
//! live snapshots in one step and fetches their files; a restore point reads
//! the head and copies the live files while it holds deletions. In the
//! `prepared` runs a commit can instead record a prepared transaction: a new
//! snapshot and the snapshots it retires, applied only by a later resolve
//! commit (`CommitTx`) or discarded (`AbortTx`), and while it is unresolved the
//! table refuses other changes and both sweeps decline. The orphan sweep treats
//! a retired snapshot's files as named until its forget. `lean` runs have no
//! scans or restore points, so a larger upload bound stays exhaustive.
//!
//! Controls: `no-upload-before-commit` lets a commit name a file whose upload
//! has not returned; `no-committed-read` makes the reader list the store
//! instead of reading the head; `no-resolve-by-head` resolves an unknown
//! outcome as aborted when the head lacks its files, without waiting for the
//! head to move past the request's base; `sweep-ignores-intent` lets the
//! holder's sweep delete a file its own write in flight still holds (reclaiming
//! too early); `adopt-by-listing` lets a commit name every unnamed file the
//! store lists, as a maintenance pass that derives its file set from a listing
//! would. In the `retire` runs: `sweep-uncommitted` plans from the holder's
//! local view, with its compaction applied before the record commits;
//! `ignores-pins` deletes a snapshot a scan pins; `forget-early` forgets the
//! snapshots before their deletes return; `ignores-live-references` deletes a
//! retired file a live snapshot keeps in place; `ignores-pinned-references`
//! keeps only files live snapshots reference, so a file a pinned retired
//! snapshot references in place goes with another retired snapshot's sweep;
//! `ignores-restore-hold` sweeps
//! while a restore point copies; `orphan-ignores-reservation` lets the orphan
//! sweep run while a prepared transaction reserves the table, and
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
/// In the `retire` runs: lease takeovers, scans, restore points and failed
/// deletes, smaller so the added reclamation stays exhaustive.
const RETIRE_TAKES: u8 = 1;
const RETIRE_READS: u8 = 1;
const RESTORES: u8 = 1;
const DELETE_FAILURES: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct Protocol {
    mode: String,
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
/// A committed snapshot: the attempt that committed it and its files.
pub type Snapshot = (u8, Vec<File>);

/// What a commit request publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CommitKind {
    /// A new snapshot of the uploaded files.
    Append,
    /// A new snapshot of the uploaded files that retires every snapshot live
    /// at the base, in the same record.
    Compact,
    /// As `Compact`, and the new snapshot keeps the first file of the first
    /// retired snapshot in place.
    CompactKeep,
    /// A prepared transaction: a new snapshot that retires every snapshot live
    /// at the base, applied only when a later resolve commits it.
    Prepare,
}

/// An unresolved prepared transaction the head records.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Prepared {
    pub id: u8,
    pub files: Vec<File>,
    pub retire: Vec<u8>,
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
    /// Committed files the new snapshot keeps in place.
    keep: Vec<File>,
    /// Snapshots it retires.
    retire: Vec<u8>,
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
}
/// A reclamation pass in one process: the snapshots it will forget, the
/// generation it runs under, and whether one of its deletes failed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Sweep {
    writer: usize,
    generation: u8,
    snapshots: Vec<u8>,
    failed: bool,
}
/// A delete request in flight: the file, the sweeping process's generation, and
/// whether the sweep that sent it still waits for it.
type Delete = (File, u8, bool);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent finite-state protocol facts and witness flags"
)]
pub(crate) struct State {
    version: u8,
    lease: u8,
    /// The committed file set the head names (in the `retire` runs: the files
    /// of the live snapshots).
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
    // The `retire` runs; empty or false otherwise.
    /// Committed live snapshots, by id.
    live: Vec<Snapshot>,
    /// Committed retired snapshots whose forget has not committed.
    retired: Vec<Snapshot>,
    /// Attempts the head records as accepted.
    applied: Vec<u8>,
    /// The unresolved prepared transaction the head records.
    prepared: Option<Prepared>,
    sweep: Option<Sweep>,
    deletes: Vec<Delete>,
    /// Snapshots the scan pins.
    pins: Vec<u8>,
    /// A restore point in progress: head version, the keys it copies, the next.
    restore: Option<(u8, Vec<Key>, usize)>,
    /// Every copy: key and whether the store returned it.
    copied: Vec<(Key, bool)>,
    /// Snapshots whose forget committed, with their files (a ghost).
    forgotten: Vec<Snapshot>,
    /// Snapshots a sweep planned and ended without forgetting (a ghost).
    interrupted: Vec<u8>,
    /// Each resolved unknown outcome, by attempt, and whether it resolved committed.
    attempt_resolutions: Vec<(u8, bool)>,
    restores: u8,
    delete_failures: u8,
    fetched_retired: bool,
    kept_in_place: bool,
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
    CompactKeep(usize),
    Prepare(usize),
    CommitTx(usize),
    AbortTx(usize),
    Plan,
    DeleteLand(usize),
    DeleteFail(usize),
    Forget,
    Unpin,
    RestoreStart,
    RestoreCopy,
    RestoreEnd,
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
/// The union of the snapshots' files, sorted.
fn files_of(snapshots: &[Snapshot]) -> Vec<File> {
    let mut files = Vec::new();
    for (_, named) in snapshots {
        insert_sorted(&mut files, named.iter().copied());
    }
    files
}
/// Move the snapshots `ids` from `live` to `retired`.
fn retire(live: &mut Vec<Snapshot>, retired: &mut Vec<Snapshot>, ids: &[u8]) {
    let (gone, kept): (Vec<Snapshot>, Vec<Snapshot>) =
        live.drain(..).partition(|(id, _)| ids.contains(id));
    *live = kept;
    insert_sorted(retired, gone);
}

impl Protocol {
    pub(crate) fn new(mode: &str, bound: usize) -> Self {
        Self {
            mode: mode.into(),
            uploads: u8::try_from(bound.max(1)).unwrap_or(1),
        }
    }
    fn has(&self, t: &str) -> bool {
        self.mode.contains(t)
    }
    fn retiring(&self) -> bool {
        self.has("retire")
    }
    fn takes(&self) -> u8 {
        if self.retiring() { RETIRE_TAKES } else { TAKES }
    }
    fn reads(&self) -> u8 {
        if self.has("lean") {
            0
        } else if self.retiring() {
            RETIRE_READS
        } else {
            READS
        }
    }
    fn restores(&self) -> u8 {
        if self.has("lean") { 0 } else { RESTORES }
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
    /// Whether the orphan sweep counts `file` as named: by a live snapshot, by
    /// a retired snapshot until its forget, or, while a prepared transaction
    /// reserves the table, by anything at all (the sweep declines).
    fn named_for_orphans(&self, s: &State, file: File) -> bool {
        s.head.contains(&file)
            || s.retired.iter().any(|(_, files)| files.contains(&file))
            || s.prepared.is_some() && !self.has("orphan-ignores-reservation")
    }
    /// The live and retired snapshots the sweeping process reads: the committed
    /// head, and in `sweep-uncommitted` its own compaction in flight applied
    /// as if it had landed.
    fn local_view(&self, s: &State, holder: &Writer) -> (Vec<Snapshot>, Vec<Snapshot>) {
        let (mut live, mut retired) = (s.live.clone(), s.retired.clone());
        if self.has("sweep-uncommitted")
            && let Some((request, _)) = &holder.pending
            && matches!(request.kind, CommitKind::Compact | CommitKind::CompactKeep)
            && !s.applied.contains(&request.attempt)
        {
            let mut files = request.files.clone();
            insert_sorted(&mut files, request.keep.iter().copied());
            retire(&mut live, &mut retired, &request.retire);
            insert_sorted(&mut live, [(request.attempt, files)]);
        }
        (live, retired)
    }
    /// The sweep the lease holder would plan now: its process, the snapshots
    /// it will forget, the deletes it sends, and whether a live snapshot kept
    /// a file in place.
    fn plan(&self, s: &State) -> Option<(usize, Vec<u8>, Vec<File>, bool)> {
        let h = Self::holder_index(s)?;
        let holder = &s.writers[h];
        if s.sweep.is_some()
            || s.prepared.is_some() && !self.has("retire-ignores-reservation")
            || s.restore.is_some() && !self.has("ignores-restore-hold")
            || holder.pending.is_some() && !self.has("sweep-uncommitted")
        {
            return None;
        }
        let (live, retired) = self.local_view(s, holder);
        let candidates: Vec<&Snapshot> = retired
            .iter()
            .filter(|(id, _)| self.has("ignores-pins") || !s.pins.contains(id))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        // Files a live snapshot, or a snapshot a scan pins, references (in
        // place, from a retired snapshot's files) stay.
        let mut referenced = files_of(&live);
        if !self.has("ignores-pinned-references") && !self.has("ignores-pins") {
            for (_, files) in retired.iter().filter(|(id, _)| s.pins.contains(id)) {
                insert_sorted(&mut referenced, files.iter().copied());
            }
        }
        let mut doomed = Vec::new();
        let mut kept = false;
        for (_, files) in &candidates {
            for f in files {
                if self.has("ignores-live-references") || !referenced.contains(f) {
                    insert_sorted(&mut doomed, [*f]);
                } else {
                    kept = true;
                }
            }
        }
        let ids = candidates.iter().map(|(id, _)| *id).collect();
        Some((h, ids, doomed, kept))
    }
    /// Deletes in flight the running sweep still waits for.
    fn outstanding(s: &State) -> usize {
        s.deletes.iter().filter(|d| d.2).count()
    }
    /// End the running sweep without a forget: its deletes no longer report,
    /// and its snapshots stay retired for a later sweep.
    fn end_sweep(next: &mut State) {
        if let Some(sweep) = next.sweep.take() {
            for delete in &mut next.deletes {
                delete.2 = false;
            }
            next.deletes.sort_unstable();
            insert_sorted(&mut next.interrupted, sweep.snapshots);
        }
    }
    /// After a delete of the running sweep returned: a sweep with a failed
    /// delete ends once its last delete returns.
    fn settle_sweep(next: &mut State) {
        if next.sweep.as_ref().is_some_and(|sweep| sweep.failed) && Self::outstanding(next) == 0 {
            Self::end_sweep(next);
        }
    }
    fn refresh_head(next: &mut State) {
        next.head = files_of(&next.live);
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
                        || self.has("no-upload-before-commit") && !writer.started.is_empty()
                        || self.has("adopt-by-listing") && !Self::unnamed(s).is_empty()
                }
                CommitKind::Compact | CommitKind::CompactKeep => {
                    self.retiring() && !writer.finished.is_empty() && !s.live.is_empty()
                }
                CommitKind::Prepare => self.has("prepared") && !writer.finished.is_empty(),
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
            Action::CompactKeep(w) => self.commit_enabled(s, w, CommitKind::CompactKeep),
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
                        || self.has("no-resolve-by-head")
                }
                _ => false,
            },
            Action::Takeover(w) => s.takes < self.takes() && writer(w).generation != Some(s.lease),
            Action::Crash(w) => s.crashes < CRASHES && *writer(w) != Writer::default(),
            Action::Read => s.reads < self.reads() && (!self.retiring() || s.pins.is_empty()),
            Action::Fetch => s
                .snapshot
                .as_ref()
                .is_some_and(|(_, listed)| s.fetched < listed.len()),
            // An unnamed file of a fenced generation: no commit naming it can
            // pass the lease condition. In the `intent` runs: an unnamed file
            // the holder's own intent does not protect.
            Action::Sweep(i) => s.store.get(i).is_some_and(|f| {
                !self.named_for_orphans(s, *f)
                    && if self.has("intent") {
                        Self::holder(s).is_some_and(|holder| {
                            self.has("sweep-ignores-intent") || !Self::intends(holder, *f)
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
            Action::Plan => self.retiring() && self.plan(s).is_some(),
            Action::DeleteLand(i) => i < s.deletes.len(),
            Action::DeleteFail(i) => i < s.deletes.len() && s.delete_failures < DELETE_FAILURES,
            Action::Forget => s.sweep.as_ref().is_some_and(|sweep| {
                self.has("forget-early") || !sweep.failed && Self::outstanding(s) == 0
            }),
            Action::Unpin => !s.pins.is_empty(),
            Action::RestoreStart => {
                self.retiring()
                    && s.restores < self.restores()
                    && s.restore.is_none()
                    && (s.sweep.is_none() || self.has("ignores-restore-hold"))
            }
            Action::RestoreCopy => s
                .restore
                .as_ref()
                .is_some_and(|(_, listed, next)| *next < listed.len()),
            Action::RestoreEnd => s.restore.is_some(),
        }
    }
    /// Quiescence for the reclamation check: a lease holder runs with no
    /// commit in flight, no scan or restore point holds anything, no
    /// transaction is prepared, and only new work or faults remain enabled.
    fn quiescent(&self, s: &State) -> bool {
        let Some(holder) = Self::holder(s) else {
            return false;
        };
        if holder.pending.is_some()
            || !s.pins.is_empty()
            || s.restore.is_some()
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
                    | Action::CompactKeep(_)
                    | Action::Prepare(_)
                    | Action::Read
                    | Action::RestoreStart
                    | Action::Crash(_)
                    | Action::Takeover(_)
                    | Action::Timeout(_)
                    | Action::Retry(_)
                    | Action::Drop(_)
                    | Action::DeleteFail(_)
            )
        })
    }
    fn send(&self, s: &State, next: &mut State, w: usize, kind: CommitKind) -> Option<()> {
        let writer = &s.writers[w];
        let generation = writer.generation?;
        let mut files = writer.finished.clone();
        if self.has("no-upload-before-commit") {
            files.extend(writer.started.iter().copied());
        }
        if self.has("adopt-by-listing") {
            let adopted: Vec<File> = Self::unnamed(s)
                .into_iter()
                .filter(|f| !files.contains(f))
                .collect();
            files.extend(adopted);
        }
        files.sort_unstable();
        // The writer reads the head it commits on.
        let retire: Vec<u8> = match kind {
            CommitKind::Append => vec![],
            _ => s.live.iter().map(|(id, _)| *id).collect(),
        };
        let keep: Vec<File> = match kind {
            CommitKind::CompactKeep => s
                .live
                .first()
                .and_then(|(_, files)| files.first())
                .copied()
                .into_iter()
                .collect(),
            _ => vec![],
        };
        let request = Request {
            attempt: s.attempts,
            writer: w,
            generation,
            base: s.version,
            files,
            kind,
            keep,
            retire,
        };
        next.attempts += 1;
        let writer = &mut next.writers[w];
        writer.finished.clear();
        if self.has("no-upload-before-commit") {
            writer.started.clear();
        }
        writer.pending = Some((request.clone(), false));
        next.net.push(request);
        Some(())
    }
    /// Apply an accepted request to the `retire` head.
    fn publish(next: &mut State, request: &Request) {
        insert_sorted(&mut next.applied, [request.attempt]);
        match request.kind {
            CommitKind::Append => {
                insert_sorted(&mut next.live, [(request.attempt, request.files.clone())]);
            }
            CommitKind::Compact | CommitKind::CompactKeep => {
                let mut files = request.files.clone();
                insert_sorted(&mut files, request.keep.iter().copied());
                retire(&mut next.live, &mut next.retired, &request.retire);
                insert_sorted(&mut next.live, [(request.attempt, files)]);
            }
            CommitKind::Prepare => {
                next.prepared = Some(Prepared {
                    id: request.attempt,
                    files: request.files.clone(),
                    retire: request.retire.clone(),
                });
            }
        }
        Self::refresh_head(next);
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
            live: vec![],
            retired: vec![],
            applied: vec![],
            prepared: None,
            sweep: None,
            deletes: vec![],
            pins: vec![],
            restore: None,
            copied: vec![],
            forgotten: vec![],
            interrupted: vec![],
            attempt_resolutions: vec![],
            restores: 0,
            delete_failures: 0,
            fetched_retired: false,
            kept_in_place: false,
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
                    Action::CompactKeep(w),
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
                Action::Unpin,
                Action::RestoreStart,
                Action::RestoreCopy,
                Action::RestoreEnd,
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
            Action::CompactKeep(w) => self.send(s, &mut next, w, CommitKind::CompactKeep)?,
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
                        Self::publish(&mut next, &request);
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
                // with nothing of its own, and its earlier sends stay in flight.
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
                let listed = if self.has("no-committed-read") {
                    keys(&s.store).collect()
                } else {
                    keys(&s.head).collect()
                };
                next.snapshot = Some((s.version, listed));
                next.fetched = 0;
                if self.retiring() {
                    // The scan pins the snapshots it reads, in the same step.
                    next.pins = s.live.iter().map(|(id, _)| *id).collect();
                }
            }
            Action::Fetch => {
                let (version, listed) = s.snapshot.as_ref()?;
                let key = *listed.get(s.fetched)?;
                let bytes = s.store.iter().find(|(k, _)| *k == key).map(|(_, b)| *b);
                next.observed.push((*version, key, bytes));
                next.fetched += 1;
                next.fetched_retired |= bytes.is_some() && !s.head.iter().any(|(k, _)| *k == key);
            }
            Action::Unpin => {
                next.pins.clear();
                next.snapshot = None;
                next.fetched = 0;
            }
            Action::Sweep(i) => {
                next.store.remove(i);
                next.reclaimed = true;
            }
            Action::CommitTx(_) | Action::AbortTx(_) => {
                let tx = s.prepared.clone()?;
                next.prepared = None;
                next.version += 1;
                if matches!(action, Action::CommitTx(_)) {
                    retire(&mut next.live, &mut next.retired, &tx.retire);
                    insert_sorted(&mut next.live, [(tx.id, tx.files)]);
                    Self::refresh_head(&mut next);
                    next.tx_committed = true;
                } else {
                    insert_sorted(&mut next.aborted, keys(&tx.files));
                    next.tx_aborted = true;
                }
                next.history.push(next.head.clone());
            }
            Action::Plan => {
                let (writer, snapshots, doomed, kept) = self.plan(s)?;
                let generation = s.writers[writer].generation?;
                insert_sorted(
                    &mut next.deletes,
                    doomed.into_iter().map(|f| (f, generation, true)),
                );
                next.sweep = Some(Sweep {
                    writer,
                    generation,
                    snapshots,
                    failed: false,
                });
                next.kept_in_place |= kept;
                next.planned_while_reserved |= s.prepared.is_some();
            }
            Action::DeleteLand(i) => {
                let (file, generation, _) = *s.deletes.get(i)?;
                next.deletes.remove(i);
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
                // One record, through the sweeping process's serial writer: a
                // fenced process's commit is refused, and a reserved table
                // refuses it before any request.
                let committed = s.writers[sweep.writer].generation == Some(sweep.generation)
                    && sweep.generation == s.lease
                    && s.prepared.is_none();
                if committed {
                    next.version += 1;
                    let (gone, kept): (Vec<Snapshot>, Vec<Snapshot>) = next
                        .retired
                        .drain(..)
                        .partition(|(id, _)| sweep.snapshots.contains(id));
                    next.retired = kept;
                    next.resumed |= gone
                        .iter()
                        .any(|(id, _)| s.interrupted.binary_search(id).is_ok());
                    insert_sorted(&mut next.forgotten, gone);
                    next.history.push(next.head.clone());
                } else {
                    insert_sorted(&mut next.interrupted, sweep.snapshots);
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
        }
        (next != *s).then_some(next)
    }
    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("committed files exist", |_, s: &State| {
                s.head.iter().all(|f| s.store.contains(f))
                    && s.prepared
                        .as_ref()
                        .is_none_or(|tx| tx.files.iter().all(|f| s.store.contains(f)))
            }),
            Property::always("reader sees only committed files", |_, s: &State| {
                s.observed.iter().all(|&(version, key, bytes)| {
                    bytes.is_some_and(|b| {
                        s.history
                            .get(usize::from(version))
                            .is_some_and(|set| set.contains(&(key, b)))
                    })
                })
            }),
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
            Property::always("pinned reader finds every file", |_, s: &State| {
                s.observed.iter().all(|(_, _, bytes)| bytes.is_some())
            }),
            Property::always("restore point copies every file", |_, s: &State| {
                s.copied.iter().all(|(_, found)| *found)
            }),
            // Unless a live snapshot or another retired one (whose own sweep
            // deletes it) still names it.
            Property::always("forgotten snapshot leaves no file", |_, s: &State| {
                s.forgotten.iter().all(|(_, files)| {
                    files.iter().all(|f| {
                        s.head.contains(f)
                            || s.retired.iter().any(|(_, named)| named.contains(f))
                            || !s.store.contains(f)
                    })
                })
            }),
            Property::always(
                "retired snapshots reclaimed at quiescence",
                |model: &Self, s: &State| !model.quiescent(s) || s.retired.is_empty(),
            ),
            Property::sometimes("snapshot retired by a commit", |_, s: &State| {
                !s.retired.is_empty() || !s.forgotten.is_empty()
            }),
            Property::sometimes("retired snapshot deleted and forgotten", |_, s: &State| {
                s.forgotten
                    .iter()
                    .any(|(_, files)| files.iter().any(|f| !s.head.contains(f)))
            }),
            // The holder could sweep but for a scan's pin on a retired snapshot.
            Property::sometimes("sweep waits for a snapshot a scan pins", |_, s: &State| {
                s.retired.iter().any(|(id, _)| s.pins.contains(id))
                    && s.sweep.is_none()
                    && s.restore.is_none()
                    && s.prepared.is_none()
                    && Self::holder(s).is_some_and(|holder| holder.pending.is_none())
            }),
            Property::sometimes(
                "scan reads a file of its snapshot after the snapshot retired",
                |_, s: &State| s.fetched_retired,
            ),
            Property::sometimes(
                "live snapshot keeps a retired snapshot's file in place",
                |_, s: &State| s.kept_in_place && !s.forgotten.is_empty(),
            ),
            Property::sometimes("delete lands after a takeover", |_, s: &State| {
                s.landed_after_takeover
            }),
            Property::sometimes(
                "interrupted sweep's snapshots forgotten by a later sweep",
                |_, s: &State| s.resumed,
            ),
            Property::sometimes(
                "restore point copies a file retired after it started",
                |_, s: &State| s.copied_retired,
            ),
        ]);
        if self.has("prepared") {
            properties.extend([
                Property::sometimes("prepared transaction committed", |_, s: &State| {
                    s.tx_committed
                }),
                Property::sometimes("prepared transaction aborted", |_, s: &State| s.tx_aborted),
            ]);
        }
        if self.has("ignores-reservation") {
            properties.push(Property::sometimes(
                "sweep plans while a transaction is prepared",
                |_, s: &State| s.planned_while_reserved,
            ));
        }
        if self.has("lean") {
            // No scans or restore points: their witnesses cannot be reached.
            properties.retain(|p| {
                !matches!(
                    p.name,
                    "reader fetches a committed file"
                        | "sweep waits for a snapshot a scan pins"
                        | "scan reads a file of its snapshot after the snapshot retired"
                        | "restore point copies a file retired after it started"
                )
            });
        }
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
    /// `Commit(w)`, `Compact(w)`, `CompactKeep(w)`, `Prepare(w)`: writer `w`
    /// sends a conditional head publication on `base_version` and
    /// `generation` that adds `files` (new uploads) and `keep` (committed files
    /// kept in place) as snapshot `attempt` and retires `retire`.
    CommitSent {
        writer: usize,
        attempt: u8,
        generation: u8,
        base_version: u8,
        files: Vec<Key>,
        kind: CommitKind,
        keep: Vec<Key>,
        retire: Vec<u8>,
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
    /// `retire` runs pins the live snapshots `pins` in the same step.
    ReadHead {
        version: u8,
        files: Vec<Key>,
        pins: Vec<u8>,
    },
    /// `Fetch`: the reader GETs the next file of its snapshot; `found` when the
    /// store returned it.
    Fetch { key: Key, found: bool },
    /// `Unpin`: the scan ends and releases its pins.
    Unpinned,
    /// `Sweep(i)`: the orphan sweep deletes an unnamed file.
    Swept { key: Key },
    /// `CommitTx(w)` / `AbortTx(w)`: the lease holder commits the resolve
    /// record of the prepared transaction `id`.
    TxResolved { writer: usize, id: u8, commit: bool },
    /// `Plan`: the holder's sweep reads the committed retired set and sends
    /// the deletes of `deletes` for the snapshots `snapshots`.
    SweepPlanned {
        writer: usize,
        generation: u8,
        snapshots: Vec<u8>,
        deletes: Vec<Key>,
    },
    /// `DeleteLand(i)`: a delete request lands; `acknowledged` when the sweep
    /// that sent it still waits for it.
    DeleteLanded { key: Key, acknowledged: bool },
    /// `DeleteFail(i)`: a delete request fails and the object stays.
    DeleteFailed { key: Key, acknowledged: bool },
    /// `Forget`: the sweep commits the record forgetting `snapshots`;
    /// `committed` when the head accepted it.
    Forgotten {
        writer: usize,
        generation: u8,
        snapshots: Vec<u8>,
        committed: bool,
    },
    /// `RestoreStart`: a restore point reads the head and holds deletions.
    RestoreStarted { version: u8, files: Vec<Key> },
    /// `RestoreCopy`: the restore point GETs its next file.
    RestoreCopied { key: Key, found: bool },
    /// `RestoreEnd`: the restore point releases its hold.
    RestoreEnded,
}

/// What one action of a `data-files` trace does, as the model takes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub action: String,
    pub op: Op,
    /// The head after the action: version, lease generation and file set.
    pub version: u8,
    pub lease: u8,
    pub head: Vec<File>,
    /// The store's data files after the action.
    pub store: Vec<File>,
    /// In the `retire` runs: the live and retired snapshots and the prepared
    /// transaction after the action.
    pub live: Vec<Snapshot>,
    pub retired: Vec<Snapshot>,
    pub prepared: Option<Prepared>,
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
        ("CompactKeep", &[w]) => Action::CompactKeep(w),
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
        ("Unpin", &[]) => Action::Unpin,
        ("Sweep", &[i]) => Action::Sweep(i),
        ("Plan", &[]) => Action::Plan,
        ("DeleteLand", &[i]) => Action::DeleteLand(i),
        ("DeleteFail", &[i]) => Action::DeleteFail(i),
        ("Forget", &[]) => Action::Forget,
        ("RestoreStart", &[]) => Action::RestoreStart,
        ("RestoreCopy", &[]) => Action::RestoreCopy,
        ("RestoreEnd", &[]) => Action::RestoreEnd,
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
            | Action::CompactKeep(w)
            | Action::Prepare(w) => {
                let request = next.net.last()?;
                Op::CommitSent {
                    writer: w,
                    attempt: request.attempt,
                    generation: request.generation,
                    base_version: request.base,
                    files: keys(&request.files).collect(),
                    kind: request.kind,
                    keep: keys(&request.keep).collect(),
                    retire: request.retire.clone(),
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
                    pins: next.pins.clone(),
                }
            }
            Action::Fetch => {
                let &(_, key, bytes) = next.observed.last()?;
                Op::Fetch {
                    key,
                    found: bytes.is_some(),
                }
            }
            Action::Unpin => Op::Unpinned,
            Action::Sweep(i) => Op::Swept {
                key: state.store[i].0,
            },
            Action::CommitTx(w) | Action::AbortTx(w) => Op::TxResolved {
                writer: w,
                id: state.prepared.as_ref()?.id,
                commit: matches!(action, Action::CommitTx(_)),
            },
            Action::Plan => {
                let sweep = next.sweep.as_ref()?;
                Op::SweepPlanned {
                    writer: sweep.writer,
                    generation: sweep.generation,
                    snapshots: sweep.snapshots.clone(),
                    deletes: next.deletes.iter().filter(|d| d.2).map(|d| d.0.0).collect(),
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
                    snapshots: sweep.snapshots.clone(),
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
        };
        out.push(Step {
            action: text.clone(),
            op,
            version: next.version,
            lease: next.lease,
            head: next.head.clone(),
            store: next.store.clone(),
            live: next.live.clone(),
            retired: next.retired.clone(),
            prepared: next.prepared.clone(),
        });
        state = next;
    }
    Some(out)
}
