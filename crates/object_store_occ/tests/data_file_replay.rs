// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! Replays `data-files` model traces through real storage: data files PUT to
//! unique final keys under a dataset prefix, and commits published through the
//! real head log, whose records carry the committed file set. After every
//! action the real head and the stored data files must equal the model's, and
//! independent invariants (committed files exist; a read returns exactly the
//! committed set) must hold.
//!
//! The `data-files-retire` traces add snapshot retirement: each record is a
//! snapshot (which may retire the snapshots live at its base), a prepared
//! transaction, its resolve, or a forget of retired snapshots, and the replay
//! folds the real records into the live and retired sets. The sweep's rules are
//! checked against that real state when the trace plans, deletes, and forgets:
//! a sweep plans only snapshots the committed head retired, by the lease holder
//! with no commit of its own outstanding, while no transaction is prepared and
//! no restore point holds deletions, skipping snapshots a scan pins and files a
//! live snapshot keeps; it forgets only after its deletes returned. Deletes go
//! through `delete_stream` on the fault store, and a failed delete is a `Fail`
//! fault on that path. Added invariants: a pinned scan's files, a restore
//! point's files and a prepared transaction's files exist, and a forgotten
//! snapshot's files are gone unless a live or another retired snapshot names
//! them. The
//! retirement records here are this test's own encoding; the Cayenne state
//! machine that applies the engine's `RetireSnapshots`/`ForgetRetired` actions
//! is not driven.
#![expect(clippy::expect_used, reason = "integration setup and trace assertions")]

use bytes::Bytes;
use cayenne_metastore_model::{
    DataFile, DataFileCommitKind, DataFileKey, DataFileOp, DataFileStep, data_file_steps,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use object_store_occ::{
    LocalConditionalPut,
    headlog::{HeadLog, HeadLogConfig, Outcome, PreparedAttempt, Reader, Writer},
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, String>;
fn result<T>(value: Result<T, impl std::fmt::Display>) -> TestResult<T> {
    value.map_err(|e| e.to_string())
}
fn require(condition: bool, message: impl FnOnce() -> String) -> TestResult {
    if condition { Ok(()) } else { Err(message()) }
}

const LOG: &str = "dataset/log";
const DATA: &str = "dataset/data";
const WAIT: Duration = Duration::from_secs(10);

/// A data file as (generation, id, the byte stored under its key).
type File = (u8, u8, u8);
fn file(f: DataFile) -> File {
    (f.0.generation, f.0.id, f.1)
}
fn key_of(f: File) -> (u8, u8) {
    (f.0, f.1)
}
fn model_key(k: DataFileKey) -> (u8, u8) {
    (k.generation, k.id)
}
fn data_path(generation: u8, id: u8) -> Path {
    Path::from(format!("{DATA}/{generation}-{id}"))
}
fn is_head(path: &Path) -> bool {
    path.as_ref() == "dataset/log/head"
}

/// A head-log record's payload: the committed file set it adds (`data-files`
/// traces) or one retirement record (`data-files-retire` traces).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum Payload {
    Files(Vec<File>),
    Record(Record),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum Record {
    /// A new snapshot that retires the snapshots `retire` in the same record.
    Snapshot {
        id: u8,
        files: Vec<File>,
        retire: Vec<u8>,
    },
    /// A prepared transaction: applied as a snapshot only by a committing resolve.
    Prepare {
        id: u8,
        files: Vec<File>,
        retire: Vec<u8>,
    },
    Resolve {
        commit: bool,
    },
    /// Retired snapshots whose files are deleted.
    Forget {
        snapshots: Vec<u8>,
    },
}

/// The committed state folded from the real head log's records.
#[derive(Debug, Default, Clone)]
struct Head {
    /// Model numbering: one per accepted record and per takeover.
    version: u64,
    lease: u64,
    /// The committed file set: every named file (`data-files`), or the live
    /// snapshots' files (`data-files-retire`).
    files: Vec<File>,
    live: BTreeMap<u8, Vec<File>>,
    retired: BTreeMap<u8, Vec<File>>,
    prepared: Option<(u8, Vec<File>, Vec<u8>)>,
}
impl Head {
    fn apply_snapshot(&mut self, id: u8, files: Vec<File>, retire: &[u8]) {
        for snapshot in retire {
            if let Some(files) = self.live.remove(snapshot) {
                self.retired.insert(*snapshot, files);
            }
        }
        self.live.insert(id, files);
    }
    fn apply(&mut self, payload: Payload) {
        match payload {
            Payload::Files(named) => self.files.extend(named),
            Payload::Record(Record::Snapshot { id, files, retire }) => {
                self.apply_snapshot(id, files, &retire);
            }
            Payload::Record(Record::Prepare { id, files, retire }) => {
                self.prepared = Some((id, files, retire));
            }
            Payload::Record(Record::Resolve { commit }) => {
                if let Some((id, files, retire)) = self.prepared.take()
                    && commit
                {
                    self.apply_snapshot(id, files, &retire);
                }
            }
            Payload::Record(Record::Forget { snapshots }) => {
                for snapshot in snapshots {
                    self.retired.remove(&snapshot);
                }
            }
        }
    }
    fn live_files(&self) -> BTreeSet<File> {
        self.live.values().flatten().copied().collect()
    }
}

#[derive(Debug, Deserialize)]
struct Trace {
    kind: String,
    run: String,
    #[serde(default)]
    property: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    walk: Option<usize>,
    initial_index: usize,
    actions: Vec<String>,
}
fn traces(text: &str) -> Vec<Trace> {
    text.lines()
        .map(|line| serde_json::from_str(line).expect("driver JSON"))
        .collect()
}
fn all_traces() -> Vec<Trace> {
    let mut all = traces(include_str!("traces/data-files.jsonl"));
    all.extend(traces(include_str!("traces/data-files-walk.jsonl")));
    all
}
fn retire_traces() -> Vec<Trace> {
    let mut all = traces(include_str!("traces/data-files-retire.jsonl"));
    all.extend(traces(include_str!("traces/data-files-retire-walk.jsonl")));
    all
}

/// The implementation the replay drives; each control breaks one rule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Control {
    Safe,
    /// A commit also names files whose PUT has not returned.
    CommitBeforeUpload,
    /// A reader lists the data prefix instead of reading the head.
    ListingReader,
    /// The sweep reads its process's view, with its own compaction in flight
    /// applied before the record commits.
    SweepUncommitted,
    /// The sweep deletes snapshots a scan pins.
    IgnoresPins,
    /// The sweep forgets before its deletes return.
    ForgetEarly,
    /// The sweep deletes retired files a live snapshot keeps in place.
    IgnoresLiveReferences,
    /// The sweep runs while a restore point holds deletions.
    IgnoresRestoreHold,
    /// The orphan sweep runs while a prepared transaction reserves the table.
    OrphanIgnoresReservation,
}

/// The client side of one writer process, as the protocol keeps it.
#[derive(Default)]
struct Client {
    generation: Option<u8>,
    started: Vec<File>,
    finished: Vec<File>,
    /// The outstanding attempt and whether its response was given up on.
    pending: Option<(usize, bool)>,
    call: Option<JoinHandle<object_store_occ::headlog::Result<Outcome>>>,
}
impl Client {
    async fn stop(&mut self) {
        if let Some(call) = self.call.take() {
            call.abort();
            let _ = call.await;
        }
    }
}

struct Attempt {
    generation: u8,
    /// The new uploads the attempt names.
    files: Vec<File>,
    /// The record it publishes (`data-files-retire` traces).
    record: Option<Record>,
    /// `None` when the head log refused to prepare because the writer is fenced:
    /// no request reaches the store, so every delivery is a refusal.
    receipt: Option<PreparedAttempt>,
}

/// A commit request in flight: the captured CAS, or a locally refused one.
struct Wire {
    attempt: usize,
    held: Option<u64>,
}

struct Upload {
    writer: usize,
    file: File,
    held: u64,
    call: JoinHandle<object_store::Result<object_store::PutResult>>,
}

/// A running reclamation pass, as its process keeps it.
struct Sweep {
    writer: usize,
    generation: u8,
    snapshots: Vec<u8>,
    /// Deletes sent and not yet returned.
    outstanding: Vec<File>,
    failed: bool,
}

struct Replay {
    inner: Arc<dyn ObjectStore>,
    store: Arc<FaultStore>,
    config: HeadLogConfig,
    control: Control,
    clients: Vec<Client>,
    attempts: Vec<Attempt>,
    net: Vec<Wire>,
    puts: Vec<Upload>,
    snapshot: Vec<(u8, u8)>,
    fetched: usize,
    /// Snapshots the scan pins, and the files it read.
    pins: Vec<u8>,
    pinned_files: Vec<File>,
    /// A restore point holding deletions: the files it copies, and the next.
    restore: Option<(Vec<File>, usize)>,
    sweep: Option<Sweep>,
    /// Deletes in flight: file, the sweeping process's generation, and whether
    /// the running sweep waits for it.
    deletes: Vec<(File, u8, bool)>,
    /// Files of forgotten snapshots.
    forgotten: BTreeSet<File>,
    /// Whether the trace is a `data-files-retire` one: its records are
    /// retirement records rather than file sets.
    retire: bool,
    /// How often each real path ran, so a replay cannot pass vacuously.
    coverage: BTreeMap<&'static str, usize>,
}

impl Replay {
    async fn new(inner: Arc<dyn ObjectStore>, control: Control) -> TestResult<Self> {
        let store = Arc::new(FaultStore::new(Arc::clone(&inner)));
        let object: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let config = HeadLogConfig::default();
        result(HeadLog::create(Arc::clone(&object), Path::from(LOG), config).await)?;
        result(Writer::take_over(object, Path::from(LOG), config, 1).await)?;
        let mut clients: Vec<Client> = (0..2).map(|_| Client::default()).collect();
        clients[0].generation = Some(1);
        Ok(Self {
            inner,
            store,
            config,
            control,
            clients,
            attempts: vec![],
            net: vec![],
            puts: vec![],
            snapshot: vec![],
            fetched: 0,
            pins: vec![],
            pinned_files: vec![],
            restore: None,
            sweep: None,
            deletes: vec![],
            forgotten: BTreeSet::new(),
            retire: false,
            coverage: BTreeMap::new(),
        })
    }

    fn cover(&mut self, path: &'static str) {
        *self.coverage.entry(path).or_default() += 1;
    }

    fn object(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }

    async fn writer(&self, generation: u8) -> TestResult<Writer> {
        result(
            Writer::resume(
                self.object(),
                Path::from(LOG),
                self.config,
                u64::from(generation),
            )
            .await,
        )
    }

    /// The committed state read from storage: version (model numbering), lease
    /// and the fold of every accepted record.
    async fn state(&self) -> TestResult<Head> {
        let (reader, opened) =
            result(Reader::open(Arc::clone(&self.inner), Path::from(LOG)).await)?;
        let mut state = Head::default();
        for record in opened.records {
            state.apply(result(serde_json::from_slice::<Payload>(
                &record.record.payload,
            ))?);
        }
        if !state.live.is_empty() || !state.retired.is_empty() || state.prepared.is_some() {
            state.files = state.live_files().into_iter().collect();
        }
        state.files.sort_unstable();
        state.files.dedup();
        let head = &reader.state().head;
        state.version = head.commit + head.lease - 1;
        state.lease = head.lease;
        Ok(state)
    }

    /// The data files actually stored under the dataset prefix.
    async fn stored(&self) -> TestResult<Vec<File>> {
        let listed: Vec<_> = result(
            self.inner
                .list(Some(&Path::from(DATA)))
                .try_collect::<Vec<_>>()
                .await,
        )?;
        let mut files = vec![];
        for meta in listed {
            let name = meta.location.filename().ok_or("unnamed data object")?;
            let (generation, id) = name.split_once('-').ok_or("malformed data key")?;
            let bytes = result(result(self.inner.get(&meta.location).await)?.bytes().await)?;
            require(bytes.len() == 1, || {
                format!("data object {name} is {bytes:?}")
            })?;
            files.push((result(generation.parse())?, result(id.parse())?, bytes[0]));
        }
        files.sort_unstable();
        Ok(files)
    }

    /// Independent invariants first, then equality with the model's state.
    async fn check(&self, step: &DataFileStep) -> TestResult {
        let head = self.state().await?;
        let stored = self.stored().await?;
        let prepared = head.prepared.iter().flat_map(|(_, files, _)| files);
        for f in head.files.iter().chain(prepared) {
            require(stored.contains(f), || {
                format!("committed file missing from the store: {f:?}; stored {stored:?}")
            })?;
        }
        for f in &self.pinned_files {
            require(stored.contains(f), || {
                format!("file of a pinned snapshot missing from the store: {f:?}")
            })?;
        }
        if let Some((files, next)) = &self.restore {
            for f in &files[*next..] {
                require(stored.contains(f), || {
                    format!("restore point's file missing from the store: {f:?}")
                })?;
            }
        }
        // Unless a live snapshot or another retired one (whose own sweep
        // deletes it) still names it.
        let named: BTreeSet<File> = head
            .live
            .values()
            .chain(head.retired.values())
            .flatten()
            .copied()
            .collect();
        for f in &self.forgotten {
            require(named.contains(f) || !stored.contains(f), || {
                format!("forgotten snapshot's file still stored: {f:?}")
            })?;
        }
        let model_head: Vec<File> = step.head.iter().copied().map(file).collect();
        let model_store: Vec<File> = step.store.iter().copied().map(file).collect();
        require(head.files == model_head, || {
            format!("head files: model {model_head:?}; real {:?}", head.files)
        })?;
        require(
            (head.version, head.lease) == (u64::from(step.version), u64::from(step.lease)),
            || {
                format!(
                    "head version/lease: model {}/{}; real {}/{}",
                    step.version, step.lease, head.version, head.lease
                )
            },
        )?;
        require(stored == model_store, || {
            format!("stored files: model {model_store:?}; real {stored:?}")
        })?;
        let snapshots = |set: &[(u8, Vec<DataFile>)]| -> BTreeMap<u8, Vec<File>> {
            set.iter()
                .map(|(id, files)| (*id, files.iter().copied().map(file).collect()))
                .collect()
        };
        require(head.live == snapshots(&step.live), || {
            format!(
                "live snapshots: model {:?}; real {:?}",
                step.live, head.live
            )
        })?;
        require(head.retired == snapshots(&step.retired), || {
            format!(
                "retired snapshots: model {:?}; real {:?}",
                step.retired, head.retired
            )
        })?;
        let model_prepared = step.prepared.as_ref().map(|tx| {
            (
                tx.id,
                tx.files.iter().copied().map(file).collect::<Vec<_>>(),
                tx.retire.clone(),
            )
        });
        require(head.prepared == model_prepared, || {
            format!(
                "prepared: model {model_prepared:?}; real {:?}",
                head.prepared
            )
        })
    }

    /// End the running sweep without a forget, as a crash, a takeover of its
    /// process or a fenced refusal does: its deletes in flight no longer report.
    fn end_sweep(&mut self, writer: usize) {
        if self
            .sweep
            .as_ref()
            .is_some_and(|sweep| sweep.writer == writer)
        {
            self.sweep = None;
            for delete in &mut self.deletes {
                delete.2 = false;
            }
        }
    }

    /// Whether the lease holder's own writes in flight hold `key`.
    fn intends(&self, holder: &Client, key: (u8, u8)) -> bool {
        holder
            .started
            .iter()
            .chain(&holder.finished)
            .any(|f| key_of(*f) == key)
            || holder.pending.is_some_and(|(index, _)| {
                self.attempts[index].files.iter().any(|f| key_of(*f) == key)
            })
    }

    /// Send one delete through the fault store's `delete_stream`, failing it
    /// with a `Fail` fault when `fail`. An absent key is reported deleted, as a
    /// sweep treats it.
    async fn delete(&self, f: File, fail: bool) -> TestResult<bool> {
        let path = data_path(f.0, f.1);
        if fail {
            let target = path.clone();
            self.store
                .arm(Op::Delete, move |p| p == &target, Fault::Fail);
        }
        let answer = self
            .store
            .delete_stream(futures::stream::iter([Ok(path)]).boxed())
            .next()
            .await
            .ok_or("delete returned no result")?;
        self.store.clear_rules();
        Ok(matches!(
            answer,
            Ok(_) | Err(object_store::Error::NotFound { .. })
        ))
    }

    #[expect(clippy::too_many_lines, reason = "one arm per operation")]
    async fn apply(&mut self, step: &DataFileStep) -> TestResult {
        match step.op.clone() {
            DataFileOp::PutSent { writer, key } => {
                let generation = self.clients[writer]
                    .generation
                    .ok_or("upload without lease")?;
                require(key.generation == generation, || "upload generation".into())?;
                let file = (generation, key.id, u8::try_from(writer).expect("writer"));
                let path = data_path(generation, key.id);
                let after = self.store.held_ids().last().copied();
                let target = path.clone();
                self.store
                    .arm(Op::Create, move |p| p == &target, Fault::Hold);
                let store = Arc::clone(&self.store);
                let call = tokio::spawn(async move {
                    store
                        .put_opts(
                            &path,
                            Bytes::from(vec![file.2]).into(),
                            PutOptions {
                                mode: PutMode::Create,
                                ..PutOptions::default()
                            },
                        )
                        .await
                });
                let held = result(tokio::time::timeout(WAIT, self.store.wait_held(after)).await)?;
                self.clients[writer].started.push(file);
                self.puts.push(Upload {
                    writer,
                    file,
                    held,
                    call,
                });
            }
            DataFileOp::PutLanded {
                writer,
                key,
                acknowledged,
            } => {
                let index = self
                    .puts
                    .iter()
                    .position(|u| (u.file.0, u.file.1) == (key.generation, key.id))
                    .ok_or("unknown upload")?;
                let upload = self.puts.remove(index);
                require(upload.writer == writer, || "upload writer".into())?;
                result(self.store.deliver(upload.held).await)?;
                let client = &mut self.clients[writer];
                let known = client.started.iter().position(|f| *f == upload.file);
                require(known.is_some() == acknowledged, || {
                    format!("acknowledgement: model {acknowledged}; client {known:?}")
                })?;
                if let Some(at) = known {
                    result(self.store.respond(upload.held))?;
                    result(result(tokio::time::timeout(WAIT, upload.call).await)?)?
                        .map_err(|e| format!("acknowledged upload failed: {e}"))?;
                    client.started.remove(at);
                    client.finished.push(upload.file);
                    self.cover("upload landed, acknowledged");
                } else {
                    upload.call.abort();
                    let _ = upload.call.await;
                    self.cover("upload landed, writer gone");
                }
            }
            DataFileOp::CommitSent {
                writer,
                attempt,
                generation,
                files: model_files,
                kind,
                keep: model_keep,
                retire: model_retire,
                ..
            } => {
                require(usize::from(attempt) == self.attempts.len(), || {
                    "attempt numbering".into()
                })?;
                let client = &mut self.clients[writer];
                require(client.generation == Some(generation), || {
                    "commit generation".into()
                })?;
                let mut files = std::mem::take(&mut client.finished);
                if self.control == Control::CommitBeforeUpload {
                    files.extend(client.started.iter().copied());
                }
                files.sort_unstable();
                let (payload, record) = if self.retire {
                    // The writer reads the head it commits on: a compaction or
                    // a transaction retires every snapshot live there.
                    let head = self.state().await?;
                    let uploaded: Vec<(u8, u8)> = files.iter().map(|f| key_of(*f)).collect();
                    let expected: Vec<(u8, u8)> =
                        model_files.iter().map(|k| model_key(*k)).collect();
                    require(uploaded == expected, || {
                        format!("commit files: model {expected:?}; client {uploaded:?}")
                    })?;
                    let live: Vec<u8> = head.live.keys().copied().collect();
                    let (retire, keep): (Vec<u8>, Vec<File>) = match kind {
                        DataFileCommitKind::Append => (vec![], vec![]),
                        DataFileCommitKind::Compact | DataFileCommitKind::Prepare => (live, vec![]),
                        DataFileCommitKind::CompactKeep => (
                            live,
                            head.live
                                .values()
                                .next()
                                .and_then(|files| files.first())
                                .copied()
                                .into_iter()
                                .collect(),
                        ),
                    };
                    let kept: Vec<(u8, u8)> = keep.iter().map(|f| key_of(*f)).collect();
                    let model_kept: Vec<(u8, u8)> =
                        model_keep.iter().map(|k| model_key(*k)).collect();
                    require(retire == model_retire && kept == model_kept, || {
                        format!(
                            "commit retires {retire:?} keeps {kept:?}; model {model_retire:?} {model_kept:?}"
                        )
                    })?;
                    let record = if kind == DataFileCommitKind::Prepare {
                        Record::Prepare {
                            id: attempt,
                            files: files.clone(),
                            retire,
                        }
                    } else {
                        let mut named = files.clone();
                        named.extend(keep);
                        named.sort_unstable();
                        Record::Snapshot {
                            id: attempt,
                            files: named,
                            retire,
                        }
                    };
                    (
                        result(serde_json::to_vec(&Payload::Record(record.clone())))?,
                        Some(record),
                    )
                } else {
                    (result(serde_json::to_vec(&files))?, None)
                };
                // The writer reads the head it commits on, as the model does.
                let mut log = self.writer(generation).await?;
                let receipt = match log.prepare(Bytes::from(payload)).await {
                    Ok(receipt) => Some(receipt),
                    // A fenced writer's head log refuses before any request.
                    Err(_) if log.state().head.lease > u64::from(generation) => None,
                    Err(e) => return Err(format!("prepare: {e}")),
                };
                let index = self.attempts.len();
                self.attempts.push(Attempt {
                    generation,
                    files,
                    record,
                    receipt,
                });
                let held = self.send(index, Some(log)).await?;
                self.clients[writer].pending = Some((index, false));
                self.clients[writer].call = held.1;
                self.net.push(Wire {
                    attempt: index,
                    held: held.0,
                });
            }
            DataFileOp::CommitApplied {
                writer,
                attempt,
                accepted,
                fenced,
                answered,
            } => {
                let index = self
                    .net
                    .iter()
                    .position(|w| w.attempt == usize::from(attempt))
                    .ok_or("unknown commit request")?;
                let wire = self.net.remove(index);
                let landed = match wire.held {
                    Some(id) => self.store.deliver(id).await.is_ok(),
                    None => false,
                };
                require(landed == accepted, || {
                    format!("store acceptance: model {accepted}; real {landed}")
                })?;
                self.cover(match (wire.held, landed) {
                    (Some(_), true) => "commit CAS accepted",
                    (Some(_), false) => "commit CAS refused by the store",
                    (None, _) => "commit refused before I/O (fenced)",
                });
                if landed
                    && matches!(
                        self.attempts[wire.attempt].record,
                        Some(Record::Snapshot { ref retire, .. }) if !retire.is_empty()
                    )
                {
                    self.cover("snapshot retired by a record");
                }
                let client = &mut self.clients[writer];
                let waiting = client.pending == Some((wire.attempt, false));
                require(waiting == answered, || "commit response expected".into())?;
                if waiting {
                    let outcome = match (wire.held, client.call.take()) {
                        (Some(id), Some(call)) => {
                            result(self.store.respond(id))?;
                            result(result(tokio::time::timeout(WAIT, call).await)?)?
                                .map_err(|e| format!("commit failed: {e}"))?
                        }
                        (None, None) => Outcome::Fenced { head_lease: 0 },
                        _ => return Err("commit call does not match its request".into()),
                    };
                    let real_fenced = matches!(outcome, Outcome::Fenced { .. });
                    require(
                        matches!(outcome, Outcome::Accepted { .. }) == accepted
                            && real_fenced == (fenced && !accepted),
                        || {
                            format!(
                                "commit outcome {outcome:?}; model accepted={accepted} fenced={fenced}"
                            )
                        },
                    )?;
                    client.pending = None;
                    self.coverage
                        .entry(if real_fenced {
                            "answered fenced"
                        } else {
                            "answered"
                        })
                        .and_modify(|n| *n += 1)
                        .or_insert(1);
                    if real_fenced {
                        // The refusal tells the writer it lost the lease.
                        *client = Client::default();
                        self.end_sweep(writer);
                    }
                }
            }
            DataFileOp::CommitDropped { attempt, .. } => {
                // A lost request never reaches the store: its capture is never delivered.
                let index = self
                    .net
                    .iter()
                    .position(|w| w.attempt == usize::from(attempt))
                    .ok_or("unknown commit request")?;
                self.net.remove(index);
                self.cover("commit request lost");
            }
            DataFileOp::ResponseLost { writer, attempt } => {
                let client = &mut self.clients[writer];
                require(
                    client.pending == Some((usize::from(attempt), false)),
                    || "timeout without a pending attempt".into(),
                )?;
                client.pending = Some((usize::from(attempt), true));
                client.stop().await;
            }
            DataFileOp::CommitResent { attempt, .. } => {
                let index = usize::from(attempt);
                let (held, call) = self.send(index, None).await?;
                // The resend's response is never awaited; the writer resolves instead.
                if let Some(call) = call {
                    call.abort();
                    let _ = call.await;
                }
                self.net.push(Wire {
                    attempt: index,
                    held,
                });
                self.cover("identical commit resent");
            }
            DataFileOp::Resolved {
                writer,
                attempt,
                committed,
            } => {
                let index = usize::from(attempt);
                let outcome = match self.attempts[index].receipt.clone() {
                    Some(receipt) => {
                        let mut log = self.writer(self.attempts[index].generation).await?;
                        // Resolution reads the head; it must never send the CAS here.
                        self.store.fail_while(Op::Cas, is_head);
                        let outcome = log.resolve(&receipt).await;
                        self.store.clear_rules();
                        result(outcome)?
                    }
                    None => Outcome::Fenced { head_lease: 0 },
                };
                let real = matches!(outcome, Outcome::Accepted { .. });
                require(real == committed, || {
                    format!("resolution: model committed={committed}; real {outcome:?}")
                })?;
                self.clients[writer].pending = None;
                self.cover(if real {
                    "resolved committed"
                } else {
                    "resolved aborted"
                });
            }
            DataFileOp::Takeover { writer, generation } => {
                let lease = self.state().await?.lease;
                require(lease + 1 == u64::from(generation), || {
                    "takeover generation".into()
                })?;
                result(
                    Writer::take_over(
                        self.object(),
                        Path::from(LOG),
                        self.config,
                        u64::from(generation),
                    )
                    .await,
                )?;
                self.clients[writer].stop().await;
                self.clients[writer] = Client {
                    generation: Some(generation),
                    ..Client::default()
                };
                self.end_sweep(writer);
            }
            DataFileOp::Crash { writer } => {
                self.clients[writer].stop().await;
                self.clients[writer] = Client::default();
                self.end_sweep(writer);
            }
            DataFileOp::ReadHead {
                version,
                files,
                pins,
            } => {
                let head = self.state().await?;
                let committed: Vec<(u8, u8)> = head.files.iter().map(|f| key_of(*f)).collect();
                let read: Vec<(u8, u8)> = if self.control == Control::ListingReader {
                    self.stored().await?.iter().map(|f| key_of(*f)).collect()
                } else {
                    committed.clone()
                };
                require(read == committed, || {
                    format!("read returned {read:?}; committed set {committed:?}")
                })?;
                let model: Vec<(u8, u8)> = files.iter().map(|k| model_key(*k)).collect();
                require(read == model && head.version == u64::from(version), || {
                    format!(
                        "read: model v{version} {model:?}; real v{} {read:?}",
                        head.version
                    )
                })?;
                // A scan pins the snapshots it reads in the same step.
                let pinned: Vec<u8> = head.live.keys().copied().collect();
                require(pinned == pins, || {
                    format!("scan pins: model {pins:?}; real {pinned:?}")
                })?;
                self.pins = pinned;
                self.pinned_files = if self.pins.is_empty() {
                    vec![]
                } else {
                    head.files.clone()
                };
                self.snapshot = read;
                self.fetched = 0;
            }
            DataFileOp::Fetch { key, found } => {
                let (generation, id) = *self
                    .snapshot
                    .get(self.fetched)
                    .ok_or("fetch past snapshot")?;
                self.fetched += 1;
                require((generation, id) == model_key(key), || "fetch order".into())?;
                let got = match self.store.get(&data_path(generation, id)).await {
                    Ok(answer) => Some(result(answer.bytes().await)?),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(e) => return Err(e.to_string()),
                };
                require(got.is_some(), || {
                    format!("committed file {generation}-{id} not found")
                })?;
                require(got.is_some() == found, || "fetch found".into())?;
                self.cover("fetch of a committed file");
                if !self.pins.is_empty()
                    && !self
                        .state()
                        .await?
                        .files
                        .iter()
                        .any(|f| key_of(*f) == (generation, id))
                {
                    self.cover("scan read a retired snapshot's file");
                }
            }
            DataFileOp::Unpinned => {
                self.pins.clear();
                self.pinned_files.clear();
                self.snapshot.clear();
                self.fetched = 0;
            }
            DataFileOp::Swept { key } => {
                // The sweeper's own rule, from real state.
                let head = self.state().await?;
                let key = model_key(key);
                if self.retire {
                    // The lease holder's orphan sweep: unnamed by any live or
                    // retired snapshot, not held by its own writes, and never
                    // while a prepared transaction reserves the table.
                    let named = head
                        .live
                        .values()
                        .chain(head.retired.values())
                        .flatten()
                        .any(|f| key_of(*f) == key);
                    require(!named, || format!("orphan sweep of a named file {key:?}"))?;
                    if self.control != Control::OrphanIgnoresReservation {
                        require(head.prepared.is_none(), || {
                            format!("orphan sweep of {key:?} while a transaction is prepared")
                        })?;
                    }
                    let holder = self
                        .clients
                        .iter()
                        .find(|c| c.generation.map(u64::from) == Some(head.lease))
                        .ok_or("orphan sweep without a lease holder")?;
                    require(!self.intends(holder, key), || {
                        format!("orphan sweep of {key:?} that the holder's write holds")
                    })?;
                } else {
                    // Unnamed and fenced.
                    require(
                        u64::from(key.0) < head.lease
                            && !head.files.iter().any(|f| key_of(*f) == key),
                        || format!("sweep of {key:?} is not safe"),
                    )?;
                }
                result(self.store.delete(&data_path(key.0, key.1)).await)?;
                self.cover("orphan swept");
            }
            DataFileOp::TxResolved { writer, id, commit } => {
                let head = self.state().await?;
                require(head.prepared.as_ref().is_some_and(|tx| tx.0 == id), || {
                    format!("resolve of transaction {id}; real {:?}", head.prepared)
                })?;
                let generation = self.clients[writer]
                    .generation
                    .filter(|g| u64::from(*g) == head.lease)
                    .ok_or("resolve by a process that does not hold the lease")?;
                let payload = result(serde_json::to_vec(&Payload::Record(Record::Resolve {
                    commit,
                })))?;
                let mut log = self.writer(generation).await?;
                let receipt = result(log.prepare(Bytes::from(payload)).await)?;
                let outcome = result(log.commit(&receipt).await)?;
                require(matches!(outcome, Outcome::Accepted { .. }), || {
                    format!("resolve record {outcome:?}")
                })?;
                self.cover(if commit {
                    "transaction committed"
                } else {
                    "transaction aborted"
                });
            }
            DataFileOp::SweepPlanned {
                writer,
                generation,
                snapshots,
                deletes,
            } => {
                self.plan(writer, generation, &snapshots, &deletes).await?;
            }
            DataFileOp::DeleteLanded { key, acknowledged }
            | DataFileOp::DeleteFailed { key, acknowledged } => {
                let fail = matches!(step.op, DataFileOp::DeleteFailed { .. });
                let index = self
                    .deletes
                    .iter()
                    .position(|d| key_of(d.0) == model_key(key) && d.2 == acknowledged)
                    .ok_or("unknown delete")?;
                let (f, generation, owned) = self.deletes.remove(index);
                let deleted = self.delete(f, fail).await?;
                require(deleted != fail, || {
                    format!("delete of {f:?}: deleted={deleted}, injected failure={fail}")
                })?;
                let lease = self.state().await?.lease;
                if owned && let Some(sweep) = &mut self.sweep {
                    if let Some(at) = sweep.outstanding.iter().position(|o| *o == f) {
                        sweep.outstanding.remove(at);
                    }
                    sweep.failed |= fail;
                    if sweep.failed && sweep.outstanding.is_empty() {
                        let writer = sweep.writer;
                        self.end_sweep(writer);
                    }
                }
                self.cover(match (fail, owned) {
                    (true, _) => "delete failed",
                    (false, true) => "delete landed, acknowledged",
                    (false, false) => "delete landed after its sweep ended",
                });
                if !fail && u64::from(generation) < lease {
                    self.cover("delete landed after a takeover");
                }
            }
            DataFileOp::Forgotten {
                writer,
                generation,
                snapshots,
                committed,
            } => {
                let sweep = self.sweep.take().ok_or("forget without a sweep")?;
                require(
                    sweep.writer == writer
                        && sweep.generation == generation
                        && sweep.snapshots == snapshots,
                    || "forget of another sweep".into(),
                )?;
                if self.control != Control::ForgetEarly {
                    require(sweep.outstanding.is_empty() && !sweep.failed, || {
                        format!(
                            "forget of {snapshots:?} before its deletes returned: {:?} outstanding, failed={}",
                            sweep.outstanding, sweep.failed
                        )
                    })?;
                }
                for delete in &mut self.deletes {
                    delete.2 = false;
                }
                let head = self.state().await?;
                // A reserved table refuses the record before any request; a
                // fenced process's head log refuses it too.
                let real = if head.prepared.is_some() {
                    false
                } else {
                    let payload = result(serde_json::to_vec(&Payload::Record(Record::Forget {
                        snapshots: snapshots.clone(),
                    })))?;
                    let mut log = self.writer(generation).await?;
                    match log.prepare(Bytes::from(payload)).await {
                        Ok(receipt) => matches!(
                            result(log.commit(&receipt).await)?,
                            Outcome::Accepted { .. }
                        ),
                        Err(_) if log.state().head.lease > u64::from(generation) => false,
                        Err(e) => return Err(format!("prepare forget: {e}")),
                    }
                };
                require(real == committed, || {
                    format!("forget committed: model {committed}; real {real}")
                })?;
                if real {
                    for snapshot in &snapshots {
                        if let Some(files) = head.retired.get(snapshot) {
                            self.forgotten.extend(files.iter().copied());
                        }
                    }
                }
                self.cover(if real {
                    "forget committed"
                } else {
                    "forget refused"
                });
            }
            DataFileOp::RestoreStarted { version, files } => {
                if self.control != Control::IgnoresRestoreHold {
                    require(self.sweep.is_none(), || {
                        "restore point while a sweep holds deletions".into()
                    })?;
                }
                let head = self.state().await?;
                let listed: Vec<(u8, u8)> = head.files.iter().map(|f| key_of(*f)).collect();
                let model: Vec<(u8, u8)> = files.iter().map(|k| model_key(*k)).collect();
                require(
                    listed == model && head.version == u64::from(version),
                    || format!("restore point reads {listed:?}; model {model:?}"),
                )?;
                self.restore = Some((head.files, 0));
            }
            DataFileOp::RestoreCopied { key, found } => {
                let (files, next) = self.restore.as_mut().ok_or("copy without a restore")?;
                let f = *files.get(*next).ok_or("copy past the restore point")?;
                *next += 1;
                require(key_of(f) == model_key(key), || "copy order".into())?;
                let got = match self.store.get(&data_path(f.0, f.1)).await {
                    Ok(answer) => Some(result(answer.bytes().await)?),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(e) => return Err(e.to_string()),
                };
                require(got.is_some() && found, || {
                    format!("restore point copy of {f:?} not found")
                })?;
                self.cover("restore point copied a file");
            }
            DataFileOp::RestoreEnded => self.restore = None,
        }
        Ok(())
    }

    /// The sweep's rules, from real state, then its deletes sent.
    async fn plan(
        &mut self,
        writer: usize,
        generation: u8,
        snapshots: &[u8],
        model_deletes: &[DataFileKey],
    ) -> TestResult {
        let head = self.state().await?;
        let client = &self.clients[writer];
        require(
            client.generation == Some(generation) && u64::from(generation) == head.lease,
            || "sweep by a process that does not hold the lease".into(),
        )?;
        require(self.sweep.is_none(), || "sweep while another runs".into())?;
        if self.control != Control::SweepUncommitted {
            require(client.pending.is_none(), || {
                "sweep while the holder's own commit is outstanding".into()
            })?;
        }
        require(head.prepared.is_none(), || {
            "sweep while a transaction is prepared".into()
        })?;
        if self.control != Control::IgnoresRestoreHold {
            require(self.restore.is_none(), || {
                "sweep while a restore point holds deletions".into()
            })?;
        }
        let (mut live, mut retired) = (head.live, head.retired);
        if self.control == Control::SweepUncommitted
            && let Some((index, _)) = client.pending
            && let Some(Record::Snapshot { id, files, retire }) = &self.attempts[index].record
            && !live.contains_key(id)
        {
            for snapshot in retire {
                if let Some(files) = live.remove(snapshot) {
                    retired.insert(*snapshot, files);
                }
            }
            live.insert(*id, files.clone());
        }
        for snapshot in snapshots {
            require(retired.contains_key(snapshot), || {
                format!("sweep of snapshot {snapshot}, which the committed head does not retire")
            })?;
            if self.control != Control::IgnoresPins {
                require(!self.pins.contains(snapshot), || {
                    format!("sweep of snapshot {snapshot}, which a scan pins")
                })?;
            }
        }
        // Files a live snapshot or a pinned scan references stay.
        let mut referenced: BTreeSet<File> = live.values().flatten().copied().collect();
        if self.control != Control::IgnoresPins {
            referenced.extend(self.pinned_files.iter().copied());
        }
        let mut doomed: BTreeSet<File> = BTreeSet::new();
        for snapshot in snapshots {
            for f in &retired[snapshot] {
                if self.control == Control::IgnoresLiveReferences || !referenced.contains(f) {
                    doomed.insert(*f);
                }
            }
        }
        let planned: Vec<(u8, u8)> = doomed.iter().map(|f| key_of(*f)).collect();
        let model: Vec<(u8, u8)> = model_deletes.iter().map(|k| model_key(*k)).collect();
        if self.control != Control::IgnoresLiveReferences {
            let live_files: BTreeSet<File> = live.values().flatten().copied().collect();
            for key in &model {
                require(!live_files.iter().any(|f| key_of(*f) == *key), || {
                    format!("sweep deletes {key:?}, which a live snapshot keeps")
                })?;
            }
        }
        require(planned == model, || {
            format!("sweep deletes: model {model:?}; real {planned:?}")
        })?;
        for f in &doomed {
            self.deletes.push((*f, generation, true));
        }
        self.sweep = Some(Sweep {
            writer,
            generation,
            snapshots: snapshots.to_vec(),
            outstanding: doomed.into_iter().collect(),
            failed: false,
        });
        self.cover("sweep planned");
        Ok(())
    }

    /// Send attempt `index`'s exact CAS, held at the store. Returns the capture
    /// and the waiting call, or neither for a locally refused attempt.
    async fn send(
        &mut self,
        index: usize,
        log: Option<Writer>,
    ) -> TestResult<(
        Option<u64>,
        Option<JoinHandle<object_store_occ::headlog::Result<Outcome>>>,
    )> {
        let Some(receipt) = self.attempts[index].receipt.clone() else {
            return Ok((None, None));
        };
        let mut log = match log {
            Some(log) => log,
            None => self.writer(self.attempts[index].generation).await?,
        };
        let after = self.store.held_ids().last().copied();
        self.store.arm(Op::Cas, is_head, Fault::Hold);
        let call = tokio::spawn(async move { log.commit(&receipt).await });
        let held = result(tokio::time::timeout(WAIT, self.store.wait_held(after)).await)?;
        Ok((Some(held), Some(call)))
    }

    async fn stop(&mut self) {
        for client in &mut self.clients {
            client.stop().await;
        }
        for upload in self.puts.drain(..) {
            upload.call.abort();
            let _ = upload.call.await;
        }
    }
}

fn label(trace: &Trace) -> String {
    match trace.walk {
        Some(walk) => format!("{} walk {walk}", trace.run),
        None => format!("{} {} \"{}\"", trace.run, trace.kind, trace.property),
    }
}

/// Replay one trace; the error names the trace, step and action.
async fn replay(
    inner: Arc<dyn ObjectStore>,
    trace: &Trace,
    control: Control,
    coverage: &mut BTreeMap<&'static str, usize>,
) -> TestResult<usize> {
    require(trace.initial_index == 0, || "initial state".into())?;
    let steps = data_file_steps(&trace.run, &trace.actions)
        .ok_or_else(|| format!("{}: the model rejects the trace", label(trace)))?;
    let mut replay = Replay::new(inner, control).await?;
    replay.retire = trace.run.contains("retire");
    let mut outcome = Ok(steps.len());
    for (n, step) in steps.iter().enumerate() {
        let checked = match replay.apply(step).await {
            Ok(()) => replay.check(step).await,
            Err(e) => Err(e),
        };
        if let Err(e) = checked {
            outcome = Err(format!(
                "{} step={n} action={}: {e}",
                label(trace),
                step.action
            ));
            break;
        }
    }
    replay.stop().await;
    for (path, n) in replay.coverage {
        *coverage.entry(path).or_default() += n;
    }
    outcome
}

fn backends(directory: &std::path::Path) -> Vec<(&'static str, Arc<dyn ObjectStore>)> {
    vec![
        ("memory", Arc::new(object_store::memory::InMemory::new())),
        (
            "durable-local",
            Arc::new(LocalConditionalPut::new_durable(directory).expect("local")),
        ),
    ]
}

/// A fresh store of `backend` per trace: every trace starts from the model's
/// initial state.
async fn replay_on(
    backend: &str,
    trace: &Trace,
    control: Control,
    coverage: &mut BTreeMap<&'static str, usize>,
) -> TestResult<usize> {
    let directory = tempfile::tempdir().expect("directory");
    let inner = backends(directory.path())
        .into_iter()
        .find(|(name, _)| *name == backend)
        .expect("backend")
        .1;
    replay(inner, trace, control, coverage).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_file_traces_replay_on_real_storage() {
    let traces = all_traces();
    assert_eq!(traces.len(), 28, "8 witnesses and 20 walks");
    for backend in ["memory", "durable-local"] {
        let mut steps = 0;
        let mut coverage = BTreeMap::new();
        for trace in &traces {
            steps += replay_on(backend, trace, Control::Safe, &mut coverage)
                .await
                .unwrap_or_else(|e| panic!("{backend}: {e}"));
        }
        println!(
            "REPLAY {backend}: traces={} steps={steps} coverage={coverage:?}",
            traces.len()
        );
        for path in [
            "upload landed, acknowledged",
            "upload landed, writer gone",
            "commit CAS accepted",
            "commit CAS refused by the store",
            "commit refused before I/O (fenced)",
            "commit request lost",
            "identical commit resent",
            "resolved committed",
            "resolved aborted",
            "fetch of a committed file",
            "orphan swept",
        ] {
            assert!(
                coverage.contains_key(path),
                "{backend}: no trace reached {path}"
            );
        }
    }
}

/// Each broken implementation must be caught by its invariant on some trace,
/// while the safe implementation replays every trace (the test above).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_implementations_are_caught() {
    let traces = all_traces();
    for (control, invariant) in [
        (
            Control::CommitBeforeUpload,
            "committed file missing from the store",
        ),
        (Control::ListingReader, "read returned"),
    ] {
        for backend in ["memory", "durable-local"] {
            let mut caught = vec![];
            for trace in &traces {
                if let Err(e) = replay_on(backend, trace, control, &mut BTreeMap::new()).await {
                    caught.push(e);
                }
            }
            let by_invariant = caught.iter().filter(|e| e.contains(invariant)).count();
            assert!(
                by_invariant > 0,
                "{control:?} on {backend} was not caught by \"{invariant}\": {caught:#?}"
            );
            println!(
                "CONTROL {control:?} {backend}: caught on {}/{} traces ({by_invariant} by \"{invariant}\"); first: {}",
                caught.len(),
                traces.len(),
                caught
                    .iter()
                    .find(|e| e.contains(invariant))
                    .expect("caught")
            );
        }
    }
}

/// The `data-files-retire` witnesses and walks replay through real storage:
/// each sweep plans, deletes and forgets only as the rules allow from the real
/// head, and the added invariants hold after every action.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_traces_replay_on_real_storage() {
    let traces = retire_traces();
    let witnesses = traces.iter().filter(|t| t.kind == "witness").count();
    let walks = traces.iter().filter(|t| t.kind == "walk").count();
    assert_eq!(
        (witnesses, walks),
        (RETIRE_WITNESSES, RETIRE_WALKS),
        "retirement trace set"
    );
    for backend in ["memory", "durable-local"] {
        let mut steps = 0;
        let mut coverage = BTreeMap::new();
        for trace in &traces {
            steps += replay_on(backend, trace, Control::Safe, &mut coverage)
                .await
                .unwrap_or_else(|e| panic!("{backend}: {e}"));
        }
        println!(
            "REPLAY retire {backend}: traces={} steps={steps} coverage={coverage:?}",
            traces.len()
        );
        for path in [
            "snapshot retired by a record",
            "sweep planned",
            "delete landed, acknowledged",
            "delete landed after its sweep ended",
            "delete landed after a takeover",
            "delete failed",
            "forget committed",
            "scan read a retired snapshot's file",
            "restore point copied a file",
            "transaction committed",
            "transaction aborted",
            "orphan swept",
            "resolved committed",
            "resolved aborted",
            "commit refused before I/O (fenced)",
        ] {
            assert!(
                coverage.contains_key(path),
                "{backend}: no trace reached {path}"
            );
        }
    }
}

/// Witnesses in `data-files-retire.jsonl` and walks in
/// `data-files-retire-walk.jsonl` (see `traces/README.md`).
const RETIRE_WITNESSES: usize = 34;
const RETIRE_WALKS: usize = 40;

/// Each model control's counterexample, replayed on real storage: the safe
/// implementation refuses the unsafe step by its rule, and the implementation
/// with that one rule removed follows the trace into the state the model
/// reports, where an independent invariant fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retirement_counterexamples_are_refused_and_caught() {
    let traces = traces(include_str!("traces/data-files-retire-controls.jsonl"));
    for (run, control, refusal, invariant) in [
        (
            "data-files-retire-sweep-uncommitted",
            Control::SweepUncommitted,
            "sweep while the holder's own commit is outstanding",
            "committed file missing from the store",
        ),
        (
            "data-files-retire-ignores-pins",
            Control::IgnoresPins,
            "which a scan pins",
            "file of a pinned snapshot missing from the store",
        ),
        (
            "data-files-retire-forget-early",
            Control::ForgetEarly,
            "before its deletes returned",
            "forgotten snapshot's file still stored",
        ),
        (
            "data-files-retire-ignores-live-references",
            Control::IgnoresLiveReferences,
            "which a live snapshot keeps",
            "committed file missing from the store",
        ),
        (
            "data-files-retire-ignores-restore-hold",
            Control::IgnoresRestoreHold,
            "sweep while a restore point holds deletions",
            "restore point's file missing from the store",
        ),
        (
            "data-files-retire-prepared-orphan-ignores-reservation",
            Control::OrphanIgnoresReservation,
            "while a transaction is prepared",
            "committed file missing from the store",
        ),
    ] {
        let trace = traces
            .iter()
            .find(|t| t.run == run && t.status == "FAIL")
            .unwrap_or_else(|| panic!("no counterexample for {run}"));
        for backend in ["memory", "durable-local"] {
            let safe = replay_on(backend, trace, Control::Safe, &mut BTreeMap::new()).await;
            let refused = safe.expect_err("the safe implementation follows a counterexample");
            assert!(
                refused.contains(refusal),
                "{run} on {backend}: refused by \"{refused}\", expected \"{refusal}\""
            );
            let broken = replay_on(backend, trace, control, &mut BTreeMap::new()).await;
            let caught = broken.expect_err("the broken implementation is not caught");
            assert!(
                caught.contains(invariant),
                "{run} on {backend}: caught by \"{caught}\", expected \"{invariant}\""
            );
            println!(
                "COUNTEREXAMPLE {run} {backend}: safe refused: {refused}; {control:?} caught: {caught}"
            );
        }
    }
}
