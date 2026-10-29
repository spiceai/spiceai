// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Best-effort shadow: storage and file I/O run only on the dedicated thread.
use super::counting::CountingStore;
use bytes::Bytes;
use cayenne_metastore_actions::{DatasetState, Record, TableState, differences, state_as_record};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store_occ::headlog::{Group, HeadLog, HeadLogConfig, Outcome, Reader, Writer};
use prost::Message as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

const CHECKPOINT_BATCHES: usize = 8;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Storage-only verification and cumulative shadow counters.
#[derive(Debug, Default, Clone)]
pub struct ShadowReport {
    /// Accepted group commits across all tables.
    pub commits: u64,
    /// Tables compared with SQL by this verification.
    pub verified_tables: u64,
    /// Immutable checkpoint roots staged successfully.
    pub checkpoints: u64,
    /// Checkpoint floors published by subsequent accepted commits.
    pub published_checkpoints: u64,
    /// Checkpoint roots read back and compared at their commit boundary.
    pub verified_checkpoints: u64,
    /// Checkpoint comparisons that differed from the corresponding tap state.
    pub checkpoint_mismatches: u64,
    /// Cumulative failures by kind, logged once per kind.
    pub failures: BTreeMap<String, u64>,
    /// Storage replay or checkpoint differences.
    pub differences: Vec<String>,
    /// Sum of original payload bytes accepted across all tables.
    pub committed_metadata_bytes: u64,
    /// Sum of checkpoint root bytes, excluding head-log envelopes.
    pub checkpoint_bytes_written: u64,
}

pub(super) struct Payload {
    pub table: String,
    pub bytes: Bytes,
    pub enqueued: Instant,
    /// The tap's baseline from SQL for a reopened metastore: a full table state,
    /// not a commit. A table whose log already exists checks it instead.
    pub seed: bool,
}
enum Command {
    Blob(Bytes),
    Payload(Box<Payload>),
    /// A barrier: every earlier payload is committed, then the tables are listed.
    Verify(oneshot::Sender<(ShadowReport, Vec<String>)>),
    /// Rebuild one table from storage alone.
    Replay(
        String,
        oneshot::Sender<std::result::Result<DatasetState, String>>,
    ),
}

pub(super) struct Shadow {
    tx: mpsc::UnboundedSender<Command>,
    // Includes startup/channel failures even if the worker cannot run.
    failures: Arc<parking_lot::Mutex<BTreeMap<String, u64>>>,
}
fn failure(
    counts: &parking_lot::Mutex<BTreeMap<String, u64>>,
    kind: &str,
    error: impl std::fmt::Display,
) {
    let mut counts = counts.lock();
    let n = counts.entry(kind.to_string()).or_default();
    if *n == 0 {
        tracing::warn!("Metastore shadow {kind} failed: {error}");
    }
    *n += 1;
}
impl Shadow {
    #[cfg(cayenne_metastore_tap)]
    pub fn failed(error: impl std::fmt::Display) -> Self {
        let (tx, _) = mpsc::unbounded_channel();
        let failures = Arc::new(parking_lot::Mutex::new(BTreeMap::new()));
        failure(&failures, "configuration", error);
        Self { tx, failures }
    }
    pub fn start(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: HeadLogConfig,
        out: Option<PathBuf>,
        drop_kind: Option<&'static str>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let failures = Arc::new(parking_lot::Mutex::new(BTreeMap::new()));
        let errors = Arc::clone(&failures);
        let spawn = std::thread::Builder::new()
            .name("metastore-shadow".into())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt.block_on(run(store, prefix, config, out, drop_kind, rx, errors)),
                    Err(e) => failure(&errors, "runtime", e),
                }
            });
        if let Err(e) = spawn {
            failure(&failures, "thread", e);
        }
        Self { tx, failures }
    }
    pub fn blob(&self, bytes: &[u8]) {
        if self
            .tx
            .send(Command::Blob(Bytes::copy_from_slice(bytes)))
            .is_err()
        {
            failure(&self.failures, "channel", "worker stopped");
        }
    }
    pub fn enqueue(&self, payload: Payload) {
        if self.tx.send(Command::Payload(Box::new(payload))).is_err() {
            failure(&self.failures, "channel", "worker stopped");
        }
    }
    /// Wait for every queued payload, then list the shadowed tables. Replay
    /// them one at a time with [`Self::replay`], so verification holds one
    /// table's state at once.
    pub async fn verify(&self) -> (ShadowReport, Vec<String>) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Command::Verify(tx)).is_ok()
            && let Ok((mut report, tables)) = rx.await
        {
            report.failures = self.failures.lock().clone();
            return (report, tables);
        }
        failure(&self.failures, "channel", "verification worker stopped");
        (
            ShadowReport {
                failures: self.failures.lock().clone(),
                ..Default::default()
            },
            Vec::new(),
        )
    }
    /// One table's state rebuilt from storage alone.
    pub async fn replay(&self, table: &str) -> std::result::Result<DatasetState, String> {
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(Command::Replay(table.to_string(), tx))
            .is_err()
        {
            return Err("verification worker stopped".into());
        }
        rx.await
            .unwrap_or_else(|_| Err("verification worker stopped".into()))
    }
}
#[derive(prost::Message)]
struct RootEnvelope {
    #[prost(bytes = "vec", tag = "1")]
    root: Vec<u8>,
}

struct Table {
    writer: Writer,
    store: Arc<CountingStore>,
    state: DatasetState,
    // Reconstruct the tap's state from every original successfully applied
    // record, before the negative control can drop any shadow payload. This
    // keeps table-state cloning off the caller's commit path.
    tap_state: DatasetState,
    // Retain group boundaries above the latest staged root, with both states
    // captured at exactly that boundary.
    ends: BTreeMap<u64, (DatasetState, Option<TableState>)>,
    staged: u64,
    disabled: bool,
    metadata_bytes: u64,
    checkpoint_bytes: u64,
    spilled_bytes: u64,
    config: HeadLogConfig,
    prefix: Path,
    /// The log existed when this session opened it: this session rebuilt its
    /// state from storage and took the head over from an earlier writer.
    reopened: bool,
}
/// Opens a table's log, creating it if absent. An existing log is rebuilt from
/// storage alone and taken over with the next generation, which fences every
/// earlier writer of it.
async fn table(store: Arc<dyn ObjectStore>, prefix: Path, config: HeadLogConfig) -> Result<Table> {
    let store = Arc::new(CountingStore::new(store));
    let dyn_store: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
    let existing = match dyn_store.head(&prefix.clone().join("head")).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => return Err(e.into()),
    };
    let (state, generation) = if existing {
        let (reader, _) = Reader::open(Arc::clone(&dyn_store), prefix.clone()).await?;
        let lease = reader.state().head.lease;
        let state = replay(Arc::clone(&dyn_store), prefix.clone()).await?;
        (state, lease + 1)
    } else {
        HeadLog::create(Arc::clone(&dyn_store), prefix.clone(), config).await?;
        (DatasetState::default(), 1)
    };
    let writer = Writer::take_over(dyn_store, prefix.clone(), config, generation).await?;
    let staged = writer.state().head.floor;
    Ok(Table {
        writer,
        store,
        tap_state: state.clone(),
        state,
        ends: BTreeMap::new(),
        staged,
        disabled: false,
        metadata_bytes: 0,
        checkpoint_bytes: 0,
        spilled_bytes: 0,
        config,
        prefix,
        reopened: existing,
    })
}
fn table_prefix(prefix: &Path, id: &str) -> Path {
    prefix.clone().join("datasets").join(id)
}

async fn run(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    config: HeadLogConfig,
    out: Option<PathBuf>,
    drop_kind: Option<&'static str>,
    mut rx: mpsc::UnboundedReceiver<Command>,
    failures: Arc<parking_lot::Mutex<BTreeMap<String, u64>>>,
) {
    let mut report = ShadowReport::default();
    let mut output = out.and_then(|p| match std::fs::File::create(p) {
        Ok(f) => Some(std::io::BufWriter::new(f)),
        Err(e) => {
            failure(&failures, "output", e);
            None
        }
    });
    let receipts =
        std::env::temp_dir().join(format!("cayenne-shadow-receipts-{}", uuid::Uuid::now_v7()));
    let mut tables = BTreeMap::new();
    let mut failed_tables = BTreeSet::new();
    let mut dropped = false;
    let mut blobs = BTreeSet::new();
    while let Some(first) = rx.recv().await {
        let mut commands = vec![first];
        // A barrier bounds this drain, including when producers keep writing.
        while !matches!(commands.last(), Some(Command::Verify(_))) {
            match rx.try_recv() {
                Ok(c) => commands.push(c),
                Err(_) => break,
            }
        }
        let mut queued: BTreeMap<String, Vec<Payload>> = BTreeMap::new();
        let mut barrier = None;
        let mut replays = Vec::new();
        for command in commands {
            match command {
                Command::Blob(bytes) => {
                    let digest = blake3::hash(&bytes).to_hex().to_string();
                    if !blobs.contains(&digest) {
                        let path = prefix
                            .clone()
                            .join("objects")
                            .join("blobs")
                            .join(digest.as_str());
                        match store.put(&path, bytes.into()).await {
                            Ok(_) => {
                                blobs.insert(digest);
                            }
                            Err(error) => failure(&failures, "blob", error),
                        }
                    }
                }
                Command::Payload(p) => queued.entry(p.table.clone()).or_default().push(*p),
                Command::Verify(tx) => barrier = Some(tx),
                Command::Replay(id, tx) => replays.push((id, tx)),
            }
        }
        for (id, payloads) in queued {
            if failed_tables.contains(&id) {
                failure(&failures, "disabled", &id);
                continue;
            }
            if !tables.contains_key(&id) {
                match table(Arc::clone(&store), table_prefix(&prefix, &id), config).await {
                    Ok(t) => {
                        tables.insert(id.clone(), t);
                    }
                    Err(e) => {
                        failure(&failures, "initialize", e);
                        failed_tables.insert(id);
                        continue;
                    }
                }
            }
            let Some(t) = tables.get_mut(&id) else {
                continue;
            };
            if t.disabled {
                failure(&failures, "disabled", &id);
                continue;
            }
            // A reopened log already holds this table: SQL's baseline must
            // equal what storage rebuilt, and is not committed again.
            let payloads = if t.reopened {
                let (seeds, payloads): (Vec<_>, Vec<_>) =
                    payloads.into_iter().partition(|p| p.seed);
                for seed in seeds {
                    report
                        .differences
                        .extend(seed_differences(&t.state, &id, &seed.bytes));
                }
                payloads
            } else {
                payloads
            };
            if payloads.is_empty() {
                continue;
            }
            let start = Instant::now();
            let oldest = payloads[0].enqueued;
            let count = payloads.len();
            let bytes: usize = payloads.iter().map(|p| p.bytes.len()).sum();
            let checkpoint_before = report.checkpoint_bytes_written;
            let commits_before = report.commits;
            let result = commit(
                t,
                &id,
                payloads,
                &receipts,
                &mut report,
                drop_kind,
                &mut dropped,
            )
            .await;
            if let Err(e) = &result {
                t.disabled = true;
                failure(&failures, "commit", e);
                report.differences.push(format!("{id}: shadow commit: {e}"));
            }
            let latency = t
                .store
                .accepted
                .lock()
                .map(|at| at.saturating_duration_since(oldest).as_secs_f64() * 1000.0);
            let outcome = match &result {
                Ok(()) if report.commits == commits_before => "dropped",
                Ok(()) => "accepted",
                Err(_) => "failed",
            };
            let line = serde_json::json!({"table_id": id, "commit_wall_ms": start.elapsed().as_secs_f64()*1000.0,
                "payloads": count, "record_bytes": bytes, "head_bytes": t.writer.state().head.encoded_len(),
                "requests": t.store.take_counts(), "checkpoint_bytes": report.checkpoint_bytes_written-checkpoint_before,
                "commit_latency_ms": latency, "outcome": outcome, "error": result.as_ref().err().map(ToString::to_string),
                "committed_metadata_bytes": t.metadata_bytes, "checkpoint_bytes_written": t.checkpoint_bytes, "spilled_bytes": t.spilled_bytes, "group_commits": report.commits - commits_before});
            if let Some(out) = output.as_mut()
                && let Err(e) = writeln!(out, "{line}").and_then(|()| out.flush())
            {
                failure(&failures, "output", e);
            }
        }
        for (id, tx) in replays {
            let result = replay(Arc::clone(&store), table_prefix(&prefix, &id))
                .await
                .map_err(|e| {
                    failure(&failures, "readback", &e);
                    e.to_string()
                });
            let _ = tx.send(result);
        }
        if let Some(tx) = barrier {
            let ids = tables.keys().chain(failed_tables.iter()).cloned().collect();
            let _ = tx.send((report.clone(), ids));
        }
    }
}

/// One record the tap committed: its original bytes, whether the shadow keeps
/// it (a negative control may drop one), and its payload as the head log will
/// carry it (inline, or a reference to a spilled object).
struct Entry {
    bytes: Bytes,
    keep: bool,
    payload: Option<Bytes>,
}

/// Bytes a group commit's envelope adds beyond its payloads (attempt id,
/// generation, framing); a group is packed to stay this far under the budget.
const GROUP_OVERHEAD: usize = 256;

async fn commit(
    t: &mut Table,
    id: &str,
    payloads: Vec<Payload>,
    receipts: &std::path::Path,
    report: &mut ShadowReport,
    drop_kind: Option<&str>,
    dropped: &mut bool,
) -> Result<()> {
    let config = t.config;
    let limit = config.max_record_bytes.saturating_sub(GROUP_OVERHEAD);
    // Pack the queued records into as few group commits as fit the record
    // budget, in order; each chunk is one head CAS.
    let mut chunks: Vec<Vec<Entry>> = vec![Vec::new()];
    let mut size = 0;
    for p in payloads {
        let record = Record::decode(p.bytes.clone())?;
        let keep = *dropped
            || !drop_kind.is_some_and(|kind| record.actions.iter().any(|a| a.name() == kind));
        if !keep {
            *dropped = true;
            chunks.last_mut().ok_or("no chunk")?.push(Entry {
                bytes: p.bytes,
                keep,
                payload: None,
            });
            continue;
        }
        let payload = encode_payload(&mut t.writer, config, &p.bytes, &mut t.spilled_bytes).await?;
        let len = payload.len() + 8;
        let current = chunks.last().ok_or("no chunk")?;
        if current.iter().any(|e| e.keep) && size + len > limit {
            chunks.push(Vec::new());
            size = 0;
        }
        size += len;
        chunks.last_mut().ok_or("no chunk")?.push(Entry {
            bytes: p.bytes,
            keep,
            payload: Some(payload),
        });
    }
    for chunk in chunks {
        commit_chunk(t, id, chunk, receipts, report).await?;
    }
    Ok(())
}

async fn commit_chunk(
    t: &mut Table,
    id: &str,
    chunk: Vec<Entry>,
    receipts: &std::path::Path,
    report: &mut ShadowReport,
) -> Result<()> {
    *t.store.accepted.lock() = None;
    let mut records = Vec::new();
    let mut encoded = Vec::new();
    for entry in chunk {
        t.tap_state.apply(&Record::decode(entry.bytes.clone())?)?;
        if entry.keep {
            records.push(entry.bytes);
            encoded.push(entry.payload.ok_or("kept entry without payload")?);
        }
    }
    if records.is_empty() {
        return Ok(());
    }
    let bytes: u64 = records.iter().map(|r| r.len() as u64).sum();
    std::fs::create_dir_all(receipts)?;
    if let Some(parent) = receipts.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    let receipt_path = receipts.join(format!("{}-{}", id, t.writer.state().head.commit + 1));
    let previous_floor = t.writer.state().head.floor;
    let outcome = t
        .writer
        .commit_group(encoded, |attempt| {
            let persist = || -> Result<()> {
                let data = serde_json::to_vec(attempt)?;
                let mut f = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&receipt_path)?;
                f.write_all(&data)?;
                f.sync_all()?;
                std::fs::File::open(receipts)?.sync_all()?;
                Ok(())
            };
            persist().map_err(|e| object_store_occ::headlog::Error::Invalid {
                reason: format!("shadow receipt: {e}"),
            })
        })
        .await?;
    let Outcome::Accepted { commit } = outcome else {
        return Err(format!("shadow outcome: {outcome:?}").into());
    };
    if t.writer.state().head.floor > previous_floor {
        report.published_checkpoints += 1;
    }
    for bytes in records {
        t.state.apply(&Record::decode(bytes)?)?;
    }
    t.metadata_bytes += bytes;
    report.committed_metadata_bytes += bytes;
    report.commits += 1;
    t.ends.insert(
        commit,
        (t.state.clone(), t.tap_state.tables.get(id).cloned()),
    );
    if let Some(e) = t.writer.sealing_error() {
        return Err(format!("batch sealing: {e}").into());
    }
    if !t.state.unknown.is_empty() {
        return Err("shadow contains unconverted actions".into());
    }
    let head = &t.writer.state().head;
    // A checkpoint lands on a batch end, and a batch end is cut from the
    // window, so only states at listed batch ends and at commits still in the
    // window can ever be needed.
    let window_start = (head.commit + 1).saturating_sub(head.window.len() as u64);
    t.ends.retain(|end, _| {
        *end > t.staged && (*end + 1 >= window_start || head.batches.contains(end))
    });
    if head.batches.len() >= CHECKPOINT_BATCHES
        && let Some(&at) = head.batches.last()
        && at > t.staged
    {
        let (state, expected) = t.ends.get(&at).ok_or("checkpoint boundary state missing")?;
        let root = state
            .tables
            .get(id)
            .map_or_else(Record::default, |s| state_as_record(s, id))
            .encode_to_vec();
        t.writer.publish_checkpoint(at, root.clone().into()).await?;
        // Read the actual immutable checkpoint envelope back from storage.
        let stored = t
            .store
            .get(
                &t.prefix
                    .clone()
                    .join("checkpoints")
                    .join(format!("{at:020}")),
            )
            .await?
            .bytes()
            .await?;
        let stored = RootEnvelope::decode(stored)?;
        let mut rebuilt = DatasetState::default();
        rebuilt.apply(&Record::decode(stored.root.as_slice())?)?;
        let diffs = match (rebuilt.tables.get(id), expected) {
            (Some(actual), Some(expected)) => differences(expected, actual),
            (None, None) => Vec::new(),
            (actual, expected) => vec![format!(
                "table presence: checkpoint {}, tap {}",
                actual.is_some(),
                expected.is_some()
            )],
        };
        let same = diffs.is_empty();
        report.verified_checkpoints += 1;
        if !same {
            report.checkpoint_mismatches += 1;
            report.differences.push(format!(
                "{id}: checkpoint {at} differs from tap at its commit"
            ));
            let direct = match (state.tables.get(id), expected) {
                (Some(actual), Some(expected)) => differences(expected, actual).len(),
                _ => usize::MAX,
            };
            return Err(format!(
                "checkpoint {at} differs from tap (head commit {}, saved ends {:?}, state-vs-tap diffs without the root round trip {direct}): {}",
                t.writer.state().head.commit,
                t.ends.keys().collect::<Vec<_>>(),
                diffs.join("; ")
            )
            .into());
        }
        report.checkpoints += 1;
        report.checkpoint_bytes_written += root.len() as u64;
        t.checkpoint_bytes += root.len() as u64;
        t.staged = at;
        t.ends.retain(|end, _| *end > at);
    }
    Ok(())
}
/// Group payloads are tagged: an inline record, or a reference to a record that
/// was too large to travel in the head and was written as an immutable object
/// named by its BLAKE3 digest (the design's "data out of the log").
const INLINE: u8 = 0;
const SPILLED: u8 = 1;

fn spill_threshold(config: HeadLogConfig) -> usize {
    config.max_record_bytes / 2
}

async fn encode_payload(
    writer: &mut Writer,
    config: HeadLogConfig,
    record: &Bytes,
    spilled_bytes: &mut u64,
) -> Result<Bytes> {
    if record.len() <= spill_threshold(config) {
        let mut out = Vec::with_capacity(record.len() + 1);
        out.push(INLINE);
        out.extend_from_slice(record);
        return Ok(out.into());
    }
    let name = format!("records/{}", blake3::hash(record).to_hex());
    let reference = writer.upload(&name, record.clone()).await?;
    *spilled_bytes += record.len() as u64;
    let mut out = Vec::with_capacity(reference.path.as_ref().len() + 1);
    out.push(SPILLED);
    out.extend_from_slice(reference.path.as_ref().as_bytes());
    Ok(out.into())
}

async fn decode_payload(store: &Arc<dyn ObjectStore>, payload: &[u8]) -> Result<Record> {
    match payload.split_first() {
        Some((&INLINE, record)) => Ok(Record::decode(record)?),
        Some((&SPILLED, path)) => {
            let path = Path::parse(std::str::from_utf8(path)?)?;
            let bytes = store.get(&path).await?.bytes().await?;
            let expected = path
                .filename()
                .ok_or("spilled record reference has no name")?;
            if blake3::hash(&bytes).to_hex().as_str() != expected {
                return Err(format!("spilled record {path} does not match its digest").into());
            }
            Ok(Record::decode(bytes)?)
        }
        _ => Err("unknown shadow payload tag".into()),
    }
}

pub(crate) async fn replay(store: Arc<dyn ObjectStore>, prefix: Path) -> Result<DatasetState> {
    let (_, opened) = Reader::open(Arc::clone(&store), prefix).await?;
    let mut state = DatasetState::default();
    if let Some(root) = opened.checkpoint_root {
        state.apply(&Record::decode(root)?)?;
    }
    for entry in opened.records {
        for payload in Group::decode(entry.record.payload)?.payloads {
            state.apply(&decode_payload(&store, &payload).await?)?;
        }
    }
    if !state.unknown.is_empty() {
        return Err("storage replay contains unconverted actions".into());
    }
    Ok(state)
}

/// Differences between SQL's baseline for a table and the state a reopened
/// shadow rebuilt from storage.
fn seed_differences(rebuilt: &DatasetState, id: &str, seed: &[u8]) -> Vec<String> {
    let mut expected = DatasetState::default();
    let applied = Record::decode(seed)
        .map_err(|e| e.to_string())
        .and_then(|record| expected.apply(&record).map_err(|e| e.to_string()));
    if let Err(e) = applied {
        return vec![format!("{id}: reopened baseline does not decode: {e}")];
    }
    match (expected.tables.get(id), rebuilt.tables.get(id)) {
        (Some(sql), Some(storage)) => differences(sql, storage)
            .into_iter()
            .map(|d| format!("{id}: reopened shadow differs from SQL: {d}"))
            .collect(),
        (None, None) => Vec::new(),
        _ => vec![format!(
            "{id}: reopened shadow and SQL disagree on table presence"
        )],
    }
}
