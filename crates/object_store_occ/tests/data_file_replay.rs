// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! Replays `data-files` model traces through real storage: data files PUT to
//! unique final keys under a dataset prefix, and commits published through the
//! real head log, whose records carry the committed file set. After every
//! action the real head and the stored data files must equal the model's, and
//! independent invariants (committed files exist; a read returns exactly the
//! committed set) must hold.
#![expect(clippy::expect_used, reason = "integration setup and trace assertions")]

use bytes::Bytes;
use cayenne_metastore_model::{DataFile, DataFileOp, DataFileStep, data_file_steps};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use object_store_occ::{
    LocalConditionalPut,
    headlog::{HeadLog, HeadLogConfig, Outcome, PreparedAttempt, Reader, Writer},
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
};
use serde::Deserialize;
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
fn data_path(generation: u8, id: u8) -> Path {
    Path::from(format!("{DATA}/{generation}-{id}"))
}
fn is_head(path: &Path) -> bool {
    path.as_ref() == "dataset/log/head"
}

#[derive(Debug, Deserialize)]
struct Trace {
    kind: String,
    run: String,
    #[serde(default)]
    property: String,
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

/// The implementation the replay drives; each control breaks one rule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Control {
    Safe,
    /// A commit also names files whose PUT has not returned.
    CommitBeforeUpload,
    /// A reader lists the data prefix instead of reading the head.
    ListingReader,
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

    /// The committed head read from storage: version (model numbering), lease
    /// and the union of the file sets of every accepted record.
    async fn head(&self) -> TestResult<(u64, u64, Vec<File>)> {
        let (reader, opened) =
            result(Reader::open(Arc::clone(&self.inner), Path::from(LOG)).await)?;
        let mut files = BTreeSet::new();
        for record in opened.records {
            let named: Vec<File> = result(serde_json::from_slice(&record.record.payload))?;
            files.extend(named);
        }
        let head = &reader.state().head;
        Ok((
            head.commit + head.lease - 1,
            head.lease,
            files.into_iter().collect(),
        ))
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
        let (version, lease, head) = self.head().await?;
        let stored = self.stored().await?;
        for f in &head {
            require(stored.contains(f), || {
                format!("committed file missing from the store: {f:?}; stored {stored:?}")
            })?;
        }
        let model_head: Vec<File> = step.head.iter().copied().map(file).collect();
        let model_store: Vec<File> = step.store.iter().copied().map(file).collect();
        require(head == model_head, || {
            format!("head files: model {model_head:?}; real {head:?}")
        })?;
        require(
            (version, lease) == (u64::from(step.version), u64::from(step.lease)),
            || {
                format!(
                    "head version/lease: model {}/{}; real {version}/{lease}",
                    step.version, step.lease
                )
            },
        )?;
        require(stored == model_store, || {
            format!("stored files: model {model_store:?}; real {stored:?}")
        })
    }

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
                let payload = Bytes::from(result(serde_json::to_vec(&files))?);
                // The writer reads the head it commits on, as the model does.
                let mut log = self.writer(generation).await?;
                let receipt = match log.prepare(payload).await {
                    Ok(receipt) => Some(receipt),
                    // A fenced writer's head log refuses before any request.
                    Err(_) if log.state().head.lease > u64::from(generation) => None,
                    Err(e) => return Err(format!("prepare: {e}")),
                };
                let index = self.attempts.len();
                self.attempts.push(Attempt {
                    generation,
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
                let (_, lease, _) = self.head().await?;
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
            }
            DataFileOp::Crash { writer } => {
                self.clients[writer].stop().await;
                self.clients[writer] = Client::default();
            }
            DataFileOp::ReadHead { version, files } => {
                let (real_version, _, head) = self.head().await?;
                let committed: Vec<(u8, u8)> = head.iter().map(|f| (f.0, f.1)).collect();
                let read: Vec<(u8, u8)> = if self.control == Control::ListingReader {
                    self.stored().await?.iter().map(|f| (f.0, f.1)).collect()
                } else {
                    committed.clone()
                };
                require(read == committed, || {
                    format!("read returned {read:?}; committed set {committed:?}")
                })?;
                let model: Vec<(u8, u8)> = files.iter().map(|k| (k.generation, k.id)).collect();
                require(read == model && real_version == u64::from(version), || {
                    format!("read: model v{version} {model:?}; real v{real_version} {read:?}")
                })?;
                self.snapshot = read;
                self.fetched = 0;
            }
            DataFileOp::Fetch { key, found } => {
                let (generation, id) = *self
                    .snapshot
                    .get(self.fetched)
                    .ok_or("fetch past snapshot")?;
                self.fetched += 1;
                require((generation, id) == (key.generation, key.id), || {
                    "fetch order".into()
                })?;
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
            }
            DataFileOp::Swept { key } => {
                // The sweeper's own rule, from real state: unnamed and fenced.
                let (_, lease, head) = self.head().await?;
                require(
                    u64::from(key.generation) < lease
                        && !head.iter().any(|f| (f.0, f.1) == (key.generation, key.id)),
                    || format!("sweep of {key:?} is not safe"),
                )?;
                result(self.store.delete(&data_path(key.generation, key.id)).await)?;
                self.cover("orphan swept");
            }
        }
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
        Some(walk) => format!("walk {walk}"),
        None => format!("{} \"{}\"", trace.kind, trace.property),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_file_traces_replay_on_real_storage() {
    let traces = all_traces();
    assert_eq!(traces.len(), 28, "8 witnesses and 20 walks");
    for (backend, _) in backends(tempfile::tempdir().expect("directory").path()) {
        let mut steps = 0;
        let mut coverage = BTreeMap::new();
        for trace in &traces {
            // A fresh store per trace: every trace starts from the model's initial state.
            let directory = tempfile::tempdir().expect("directory");
            let inner = backends(directory.path())
                .into_iter()
                .find(|(name, _)| *name == backend)
                .expect("backend")
                .1;
            steps += replay(inner, trace, Control::Safe, &mut coverage)
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
                let directory = tempfile::tempdir().expect("directory");
                let inner = backends(directory.path())
                    .into_iter()
                    .find(|(name, _)| *name == backend)
                    .expect("backend")
                    .1;
                if let Err(e) = replay(inner, trace, control, &mut BTreeMap::new()).await {
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
