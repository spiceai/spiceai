// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0
//! Driver-generated histories through real conditional storage, with an independent
//! serial oracle derived exclusively from requests that actually landed.
#![expect(clippy::expect_used, reason = "integration setup and trace assertions")]

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutOptions, PutPayload, path::Path};
use object_store_occ::{
    Expected, LocalConditionalPut, conditional_put,
    headlog::{HeadLog, HeadLogConfig, Outcome, Poll, PreparedAttempt, Reader, Record, Writer},
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
};
use prost::Message;
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::task::JoinHandle;

type TestResult<T = ()> = Result<T, String>;
fn result<T>(value: Result<T, impl std::fmt::Display>) -> TestResult<T> {
    value.map_err(|e| e.to_string())
}
fn require(condition: bool, message: impl FnOnce() -> String) -> TestResult {
    if condition { Ok(()) } else { Err(message()) }
}
fn bytes(payload: &PutPayload) -> Vec<u8> {
    payload.iter().flat_map(|b| b.iter().copied()).collect()
}
fn head_path(path: &Path) -> bool {
    path.as_ref() == "replay/head"
}
fn wal_path(path: &Path) -> bool {
    path.as_ref().starts_with("replay/wal/")
}

#[derive(Debug, Deserialize)]
struct Trace {
    kind: String,
    run: String,
    family: String,
    initial_index: usize,
    actions: Vec<String>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    property: String,
}
fn traces(text: &str) -> Vec<Trace> {
    text.lines()
        .map(|line| serde_json::from_str(line).expect("driver JSON"))
        .collect()
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Control {
    Safe,
    NoPersist,
    AckEarly,
    NoSkip,
    NoExists,
}
type Call = JoinHandle<(Writer, object_store_occ::headlog::Result<Outcome>)>;
struct Attempt {
    receipt: Option<PreparedAttempt>,
    durable: Option<Vec<u8>>,
    generation: u64,
    wires: Vec<u64>,
    call: Option<Call>,
    outcome: Option<Outcome>,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if let Some(call) = self.call.take() {
            call.abort();
        }
    }
}
struct Proposal {
    payload: Bytes,
    generation: u64,
}
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent trace coverage, checkpoint, source and acknowledgement facts"
)]
struct Replay {
    store: Arc<FaultStore>,
    config: HeadLogConfig,
    staged_writer: Option<Writer>,
    strict_window: bool,
    reader: Reader,
    incremental: Vec<Bytes>,
    proposals: BTreeMap<Vec<u8>, Proposal>,
    attempts: Vec<Attempt>,
    active: Option<usize>,
    copies: Vec<(Path, PutPayload, PutOptions)>,
    control: Control,
    source: bool,
    source_slots: Vec<Option<usize>>,
    control_source_slots: bool,
    buffer: Vec<usize>,
    cursor: usize,
    acknowledged: usize,
    logical_ack: bool,
    skipped: BTreeMap<String, usize>,
}
impl Replay {
    fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }
    async fn new(
        inner: Arc<dyn ObjectStore>,
        source: bool,
        window: bool,
        control: Control,
    ) -> Self {
        let store = Arc::new(FaultStore::new(inner));
        let object: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let config = if window {
            HeadLogConfig {
                head_budget_bytes: 55_000,
                batch_budget_bytes: 30_000,
                max_record_bytes: 20_000,
                batch_max_records: 3,
                max_listed_batches: 64,
                // The model has no spill: keep every record inline.
                spill_above_bytes: 20_000,
            }
        } else {
            HeadLogConfig {
                head_budget_bytes: 4096,
                batch_budget_bytes: 180,
                max_record_bytes: 100,
                batch_max_records: 2,
                max_listed_batches: 64,
                spill_above_bytes: 100,
            }
        };
        HeadLog::create(Arc::clone(&object), Path::from("replay"), config)
            .await
            .expect("create");
        Writer::take_over(Arc::clone(&object), Path::from("replay"), config, 1)
            .await
            .expect("owner");
        let (reader, _) = Reader::open(object, Path::from("replay"))
            .await
            .expect("initial reader");
        store.fail_while(Op::Create, wal_path);
        Self {
            store,
            config,
            staged_writer: None,
            strict_window: true,
            reader,
            incremental: vec![],
            proposals: BTreeMap::new(),
            attempts: vec![],
            active: None,
            copies: vec![],
            control,
            source,
            source_slots: vec![],
            control_source_slots: false,
            buffer: vec![],
            cursor: 1,
            acknowledged: 0,
            logical_ack: false,
            skipped: BTreeMap::new(),
        }
    }
    fn skip(&mut self, reason: &str) {
        *self.skipped.entry(reason.into()).or_default() += 1;
    }
    /// No head-log decoder or writer code participates in this reference:
    /// successful CAS bytes identify a registered logical proposal. All other
    /// successful head CASes are lease transitions.
    fn serial(&self) -> TestResult<(Vec<Bytes>, u64)> {
        let mut records = vec![];
        let mut lease = 0;
        for entry in self.store.log() {
            if entry.op != Op::Cas || !head_path(&entry.path) || entry.result.is_err() {
                continue;
            }
            let encoded = bytes(entry.payload.as_ref().expect("CAS payload"));
            if let Some(proposal) = self.proposals.get(&encoded) {
                require(proposal.generation == lease, || {
                    format!(
                        "I3 expected generation {lease}, actual {}",
                        proposal.generation
                    )
                })?;
                records.push(proposal.payload.clone());
            } else {
                lease += 1;
            }
        }
        Ok((records, lease))
    }
    async fn writer(&self, generation: u64) -> TestResult<Writer> {
        result(
            Writer::resume(
                self.object_store(),
                Path::from("replay"),
                self.config,
                generation,
            )
            .await,
        )
    }
    fn register(&mut self, receipt: &PreparedAttempt, payload: Bytes, generation: u64) {
        let json = serde_json::to_value(receipt).expect("receipt");
        let encoded: Vec<u8> =
            serde_json::from_value(json["head_bytes"].clone()).expect("exact bytes");
        self.proposals.entry(encoded).or_insert(Proposal {
            payload,
            generation,
        });
    }
    async fn prepare(&mut self, payload: Bytes) -> TestResult<usize> {
        let (_, generation) = self.serial()?;
        let mut writer = if let Some(staged) = self.staged_writer.take() {
            staged
        } else {
            self.writer(generation).await?
        };
        let receipt = result(writer.prepare(payload.clone()).await)?;
        self.register(&receipt, payload, generation);
        let index = self.attempts.len();
        self.attempts.push(Attempt {
            receipt: Some(receipt),
            durable: None,
            generation,
            wires: vec![],
            call: None,
            outcome: None,
        });
        self.active = Some(index);
        Ok(index)
    }
    async fn send(&mut self, index: usize, resolve: bool) -> TestResult {
        if let Some(call) = self.attempts[index].call.take() {
            call.abort();
            let _ = call.await;
        }
        let receipt = self.attempts[index]
            .receipt
            .clone()
            .ok_or("no volatile receipt")?;
        let mut writer = self.writer(self.attempts[index].generation).await?;
        let last = self.store.held_ids().last().copied();
        self.store.arm(Op::Cas, head_path, Fault::Hold);
        let mut call = tokio::spawn(async move {
            let outcome = if resolve {
                writer.resolve(&receipt).await
            } else {
                writer.commit(&receipt).await
            };
            (writer, outcome)
        });
        tokio::select! {
            id = self.store.wait_held(last) => {
                self.attempts[index].wires.push(id);
                self.attempts[index].call = Some(call);
            }
            answer = &mut call => {
                let (_, outcome) = result(answer)?;
                self.attempts[index].outcome = Some(result(outcome)?);
                // No CAS was needed; remove its unused hold without removing the copy gate.
                self.store.clear_rules();
                self.store.fail_while(Op::Create, wal_path);
            }
        }
        Ok(())
    }
    fn persist(&mut self, index: usize) {
        self.attempts[index].durable = Some(
            serde_json::to_vec(self.attempts[index].receipt.as_ref().expect("receipt"))
                .expect("persist"),
        );
    }
    async fn append(&mut self, payload: Bytes) -> TestResult {
        let index = self.prepare(payload).await?;
        self.persist(index);
        self.send(index, false).await?;
        let wire = self.attempts[index].wires[0];
        result(self.store.deliver(wire).await)?;
        self.response(index).await?;
        Ok(())
    }
    async fn response(&mut self, index: usize) -> TestResult {
        let Some(call) = self.attempts[index].call.take() else {
            return Ok(());
        };
        let wire = *self.attempts[index]
            .wires
            .last()
            .ok_or("no response request")?;
        if self.store.respond(wire).is_err() {
            // A resolve call may be awaiting a retry while the response that
            // became available belongs to the original request.
            call.abort();
            let _ = call.await;
            let delivered = self.attempts[index]
                .wires
                .iter()
                .rev()
                .copied()
                .any(|id| self.store.respond(id).is_ok());
            require(delivered, || "Response without a delivered request".into())?;
            self.store.clear_rules();
            self.store.fail_while(Op::Create, wal_path);
            let mut writer = self.writer(self.attempts[index].generation).await?;
            let receipt = self.attempts[index]
                .receipt
                .as_ref()
                .ok_or("response receipt")?;
            self.attempts[index].outcome = Some(result(writer.resolve(receipt).await)?);
        } else {
            let (_, outcome) = result(result(
                tokio::time::timeout(Duration::from_secs(10), call).await,
            )?)?;
            self.attempts[index].outcome = Some(result(outcome)?);
        }
        Ok(())
    }
    fn observe(&mut self) {
        for (path, payload, options) in self.store.blocked() {
            if wal_path(&path)
                && !self
                    .copies
                    .iter()
                    .any(|(p, b, _)| p == &path && bytes(b) == bytes(&payload))
            {
                self.copies.push((path, payload, options));
            }
        }
        self.copies.sort_by_key(|(p, b, _)| (p.clone(), bytes(b)));
    }
    async fn copy(&mut self, index: usize) -> TestResult {
        self.observe();
        let (path, payload, options) = self
            .copies
            .get(index)
            .cloned()
            .ok_or_else(|| format!("copy index {index}, pending={}", self.copies.len()))?;
        self.store.clear_rules();
        // A duplicate create must preserve the original bytes.
        let answer = self.store.put_opts(&path, payload.clone(), options).await;
        if answer.is_err() {
            let actual = result(result(self.store.get(&path).await)?.bytes().await)?;
            require(actual.as_ref() == bytes(&payload), || {
                "conflicting immutable copy".into()
            })?;
        }
        self.store.fail_while(Op::Create, wal_path);
        Ok(())
    }
    async fn restart(&mut self, erase: bool) {
        for attempt in &mut self.attempts {
            if let Some(call) = attempt.call.take() {
                call.abort();
                let _ = call.await;
            }
            if erase {
                attempt.receipt = None;
                attempt.outcome = None;
            }
        }
        self.active = None;
    }
    async fn fence(&mut self) -> TestResult {
        let (_, lease) = self.serial()?;
        result(
            Writer::take_over(
                self.object_store(),
                Path::from("replay"),
                self.config,
                lease + 1,
            )
            .await,
        )?;
        Ok(())
    }
    fn source_state(serial: &[Bytes]) -> TestResult<(Vec<usize>, usize)> {
        let mut rows = vec![];
        let mut position = 0;
        for payload in serial {
            let (next, pos): (Vec<usize>, usize) = result(serde_json::from_slice(payload))?;
            rows.extend(next);
            position = pos;
        }
        Ok((rows, position))
    }
    async fn check(&mut self) -> TestResult {
        let (expected, _) = self.serial()?;
        let (_, opened) = result(Reader::open(self.object_store(), Path::from("replay")).await)
            .map_err(|e| format!("I4 expected readable committed prefix; actual {e}"))?;
        let mut actual: Vec<Bytes> = if let Some(root) = opened.checkpoint_root {
            result(serde_json::from_slice(&root))?
        } else {
            vec![]
        };
        actual.extend(opened.records.into_iter().map(|r| r.record.payload));
        require(actual == expected, || {
            format!("I1/I6 expected {expected:?}; actual {actual:?}")
        })?;
        match result(self.reader.poll().await)? {
            Poll::Unchanged => {}
            Poll::Records(records) => self
                .incremental
                .extend(records.into_iter().map(|r| r.record.payload)),
            Poll::Reopen => {
                let (reader, _) =
                    result(Reader::open(self.object_store(), Path::from("replay")).await)?;
                self.reader = reader;
                self.incremental.clone_from(&actual);
            }
        }
        require(self.incremental == actual, || {
            format!("I8 expected {actual:?}; actual {:?}", self.incremental)
        })?;
        // I5 is independently checked against successful exact bytes and the
        // current version: rejection is only safe once the old condition is dead.
        let log = self.store.log();
        for attempt in &self.attempts {
            let Some(receipt) = &attempt.receipt else {
                continue;
            };
            let json = serde_json::to_value(receipt).expect("receipt");
            let encoded: Vec<u8> =
                serde_json::from_value(json["head_bytes"].clone()).expect("bytes");
            let landed = log.iter().any(|e| {
                head_path(&e.path)
                    && e.result.is_ok()
                    && e.payload.as_ref().is_some_and(|p| bytes(p) == encoded)
            });
            match attempt.outcome {
                Some(Outcome::Accepted { .. }) => {
                    require(landed, || "I5 Accepted without landing".into())?;
                }
                Some(Outcome::Rejected | Outcome::Fenced { .. }) => {
                    require(!landed, || "I5 Rejected after landing".into())?;
                    require(
                        json["e_tag"].as_str() != self.reader.state().version.e_tag.as_deref(),
                        || "I5 Rejected while condition can still land".into(),
                    )?;
                }
                _ => {}
            }
        }
        if self.source {
            let (rows, position) = Self::source_state(&expected)?;
            let prefix: Vec<_> = (1..=position).collect();
            require(rows == prefix, || {
                format!("I10 expected rows {prefix:?} at position {position}; actual {rows:?}")
            })?;
            require(self.acknowledged <= position, || {
                format!(
                    "I2 expected acknowledgement <= {position}; actual {}",
                    self.acknowledged
                )
            })?;
        } else {
            let count = expected
                .iter()
                .filter(|p| p.as_ref() == b"logical-write-0")
                .count();
            require(count <= 1 && (!self.logical_ack || count == 1), || {
                format!(
                    "I2 expected logical-write-0 count <= 1 (exactly 1 after acknowledgement); actual {count}"
                )
            })?;
        }
        Ok(())
    }
    async fn commits(&mut self, action: &str, args: &[usize]) -> TestResult {
        match action {
            "Buffer" => {
                self.buffer = vec![0];
            }
            "Issue" => {
                // Resolve durable work before replacing it, even when a faulty
                // model trace asks to issue a new identity after restart.
                let reuse = if self.control == Control::NoPersist {
                    None
                } else {
                    self.attempts
                        .iter()
                        .rposition(|a| a.durable.is_some() && a.receipt.is_none())
                };
                if let Some(index) = reuse {
                    self.attempts[index].receipt = Some(result(serde_json::from_slice(
                        self.attempts[index].durable.as_ref().expect("durable"),
                    ))?);
                    let receipt = self.attempts[index].receipt.clone();
                    let saved = self.attempts[index].durable.clone();
                    let generation = self.attempts[index].generation;
                    self.attempts.push(Attempt {
                        receipt,
                        durable: saved,
                        generation,
                        wires: vec![],
                        call: None,
                        outcome: None,
                    });
                    self.active = Some(self.attempts.len() - 1);
                } else {
                    self.prepare(Bytes::from_static(b"logical-write-0")).await?;
                }
                self.buffer.clear();
            }
            "Persist" => self.persist(self.active.ok_or("no active attempt")?),
            "Send" => {
                let index = self.active.ok_or("no active attempt")?;
                if self.control != Control::NoPersist && self.attempts[index].durable.is_none() {
                    self.persist(index);
                }
                self.send(index, false).await?;
            }
            "Deliver" => {
                let index = args[1];
                let wires = &self.attempts[index].wires;
                let wire = if args[2] == 0 {
                    wires.first()
                } else {
                    wires.last()
                }
                .copied()
                .ok_or("no captured delivery")?;
                let _ = self.store.deliver(wire).await;
            }
            "Response" => {
                self.response(self.active.ok_or("no active attempt")?)
                    .await?;
            }
            "Resolve" => {
                let index = self.active.ok_or("no active attempt")?;
                self.send(index, true).await?;
            }
            "Unreadable" => {
                let index = self.active.ok_or("no active attempt")?;
                if let Some(call) = self.attempts[index].call.take() {
                    call.abort();
                    let _ = call.await;
                }
                let mut writer = self.writer(self.attempts[index].generation).await?;
                self.store.arm(Op::Get, head_path, Fault::Fail);
                let receipt = self.attempts[index].receipt.as_ref().ok_or("no receipt")?;
                require(writer.resolve(receipt).await.is_err(), || {
                    "unreadable head unexpectedly resolved".into()
                })?;
            }
            "Ack" => {
                self.logical_ack = true;
            }
            "Restart" => {
                self.restart(true).await;
                self.buffer.clear();
            }
            "Restore" => {
                for a in &mut self.attempts {
                    if let Some(saved) = &a.durable {
                        a.receipt = Some(result(serde_json::from_slice(saved))?);
                    }
                }
                self.active = self.attempts.iter().rposition(|a| a.receipt.is_some());
                if let Some(index) = self.active {
                    self.writer(self.attempts[index].generation).await?;
                }
            }
            "Fence" => self.fence().await?,
            "Copy" => self.copy(args[0]).await?,
            "Observe" => self.observe(),
            "Poll" => {
                result(self.reader.poll().await).map(|poll| {
                    if let Poll::Records(records) = poll {
                        self.incremental
                            .extend(records.into_iter().map(|r| r.record.payload));
                    }
                })?;
            }
            "Recover" => {
                result(Reader::open(self.object_store(), Path::from("replay")).await)?;
            }
            other => return Err(format!("unmapped commit action {other}")),
        }
        Ok(())
    }
    async fn source(&mut self, action: &str, args: &[usize]) -> TestResult {
        match action {
            "Receive" => {
                let (_, position) = Self::source_state(&self.serial()?.0)?;
                if self.control == Control::NoSkip || self.cursor > position {
                    self.buffer.push(self.cursor);
                }
                self.cursor += 1;
            }
            "Issue" => {
                let (_, position) = Self::source_state(&self.serial()?.0)?;
                if self.control != Control::NoSkip {
                    self.buffer.retain(|p| *p > position);
                }
                if let Some(&end) = self.buffer.last() {
                    let payload =
                        Bytes::from(result(serde_json::to_vec(&(self.buffer.clone(), end)))?);
                    let index = self.prepare(payload).await?;
                    self.persist(index);
                    self.send(index, false).await?;
                    self.source_slots.push(Some(index));
                } else if self.control_source_slots {
                    self.source_slots.push(None);
                    self.active = None;
                }
            }
            "Deliver" => {
                let Some(index) = self.source_slots.get(args[0]).copied().flatten() else {
                    require(self.control_source_slots, || {
                        format!("missing source slot {}", args[0])
                    })?;
                    return Ok(());
                };
                let wires = &self.attempts[index].wires;
                let wire = if args[1] == 0 {
                    wires.first()
                } else {
                    wires.last()
                }
                .copied()
                .ok_or("source missing delivery")?;
                let _ = self.store.deliver(wire).await;
            }
            "Resolve" => {
                if let Some(index) = self.active {
                    self.send(index, true).await?;
                }
            }
            "Unknown" => {
                // Finish the caller with an unreadable resolution head. The
                // captured request itself remains deliverable after this error.
                if let Some(index) = self.active
                    && let Some(wire) = self.attempts[index].wires.last().copied()
                {
                    self.store.arm(Op::Get, head_path, Fault::Fail);
                    result(self.store.drop_response(wire))?;
                    if let Some(call) = self.attempts[index].call.take() {
                        let (_, outcome) = result(call.await)?;
                        require(outcome.is_err(), || {
                            "Unknown must not supply a decision".into()
                        })?;
                    }
                }
            }
            "Acknowledge" => {
                let (_, position) = Self::source_state(&self.serial()?.0)?;
                let end = self.buffer.last().copied().unwrap_or(position);
                if self.control == Control::AckEarly || end <= position {
                    self.acknowledged = self.acknowledged.max(end);
                    self.buffer.clear();
                    self.active = None;
                }
            }
            "Crash" | "Takeover" => {
                self.restart(true).await;
                self.buffer.clear();
                self.cursor = self.acknowledged + 1;
                if action == "Takeover" {
                    self.fence().await?;
                }
            }
            "Acquire" => self.fence().await?,
            other => return Err(format!("unmapped source action {other}")),
        }
        Ok(())
    }
    async fn window(&mut self, action: &str, args: &[usize]) -> TestResult {
        match action {
            "Append" => {
                // Original encoded record sizes are exactly 10,000 / 20,000.
                // H reserves 5,000 bytes for the envelope/index. For these bounded
                // traces overhead is < 5,000, so integer symbolic cuts agree.
                let size = 10_000 * (args[0] + 1);
                let mut payload =
                    vec![u8::try_from(self.proposals.len()).expect("bounded trace"); size - 24];
                while (Record {
                    attempt_id: vec![0; 16],
                    generation: 1,
                    payload: Bytes::copy_from_slice(&payload),
                    spilled: None,
                })
                .encoded_len()
                    < size
                {
                    payload.push(0);
                }
                let payload = Bytes::from(payload);
                match self.append(payload.clone()).await {
                    Ok(()) => {}
                    Err(error) if self.control == Control::NoExists => {
                        // Remove precisely the exists-before-drop safeguard in
                        // the test harness; publish the same dropped window and
                        // batch index while its immutable batch is still absent.
                        let (_, lease) = self.serial()?;
                        let writer = self.writer(lease).await?;
                        let mut head = writer.state().head.clone();
                        let mut total = 0;
                        let count = head
                            .window
                            .iter()
                            .take(3)
                            .take_while(|r| {
                                total += r.encoded.len();
                                total <= self.config.batch_budget_bytes
                            })
                            .count();
                        require(count > 0, || format!("no cut for control: {error}"))?;
                        let end = head.commit - head.window.len() as u64 + count as u64;
                        head.window.drain(..count);
                        head.batches.push(end);
                        head.commit += 1;
                        let record = Record {
                            attempt_id: vec![42; 16],
                            generation: lease,
                            payload: payload.clone(),
                            spilled: None,
                        };
                        head.window.push(object_store_occ::headlog::WindowRecord {
                            encoded: record.encode_to_vec(),
                        });
                        let encoded = head.encode_to_vec();
                        self.proposals.insert(
                            encoded.clone(),
                            Proposal {
                                payload,
                                generation: lease,
                            },
                        );
                        result(
                            conditional_put(
                                self.store.as_ref(),
                                &Path::from("replay/head"),
                                encoded.into(),
                                &Expected::Version(writer.state().version.clone()),
                            )
                            .await,
                        )?;
                    }
                    Err(error) => {
                        // A control trace may request an unsafe append. The real
                        // writer refuses it; no publication is invented.
                        require(!self.strict_window, || {
                            format!("positive model append refused: {error}")
                        })?;
                        require(
                            error.contains("storage")
                                || error.contains("conditional")
                                || error.contains("head budget"),
                            || format!("unexpected append refusal: {error}"),
                        )?;
                        self.skip("Append refused: batch not yet created");
                    }
                }
            }
            "Observe" => self.observe(),
            "Create" => {
                self.observe();
                if args[0] < self.copies.len() {
                    self.copy(args[0]).await?;
                } else {
                    self.skip("Create: checkpoint-model pending copy has no head-log equivalent");
                }
            }
            "Checkpoint" => {
                // The model writes the root at batch end `c` (commit `c`, since
                // commits number from 1), and the next accepted commit publishes
                // the floor; the staged writer carries it to that commit.
                let c = u64::try_from(args[0]).expect("small boundary");
                let (serial, lease) = self.serial()?;
                let mut writer = match self.staged_writer.take() {
                    Some(writer) => writer,
                    None => self.writer(lease).await?,
                };
                let root = Bytes::from(result(serde_json::to_vec(
                    &serial[..usize::try_from(c).expect("small boundary")],
                ))?);
                result(writer.publish_checkpoint(c, root).await)
                    .map_err(|e| format!("model checkpoint at {c} refused: {e}"))?;
                self.staged_writer = Some(writer);
            }
            "Gc" | "Query" => self.skip(action),
            other => return Err(format!("unmapped window action {other}")),
        }
        Ok(())
    }
}

fn parse(action: &str) -> (&str, Vec<usize>) {
    let name = action.split('(').next().expect("action");
    let args = action
        .strip_prefix(name)
        .unwrap_or_default()
        .trim_matches(['(', ')']);
    (
        name,
        args.split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().parse().expect("numeric model action"))
            .collect(),
    )
}
async fn replay(
    trace: &Trace,
    local: bool,
    control: Control,
) -> TestResult<BTreeMap<String, usize>> {
    require(trace.initial_index == 0, || {
        format!("unsupported initial state {}", trace.initial_index)
    })?;
    let directory = tempfile::tempdir().expect("directory");
    let inner: Arc<dyn ObjectStore> = if local {
        Arc::new(LocalConditionalPut::new_durable(directory.path()).expect("local"))
    } else {
        Arc::new(object_store::memory::InMemory::new())
    };
    let source = trace.family == "source-positions";
    let window = trace.family == "window";
    let mut replay = Replay::new(inner, source, window, control).await;
    replay.control_source_slots = trace.run == "source-no-redelivery-skip";
    replay.strict_window = !trace.run.contains("no-exists");
    if !source && !window {
        replay.append(Bytes::from_static(b"filler-10")).await?;
        replay.append(Bytes::from_static(b"filler-11")).await?;
        replay.observe();
        replay.attempts.clear();
        replay.active = None;
    }
    replay.check().await?;
    for (step, action) in trace.actions.iter().enumerate() {
        let (name, args) = parse(action);
        let run = async {
            if source {
                replay.source(name, &args).await?;
            } else if window {
                replay.window(name, &args).await?;
            } else {
                replay.commits(name, &args).await?;
            }
            replay.check().await
        };
        result(tokio::time::timeout(Duration::from_secs(20), run).await).and_then(std::convert::identity).map_err(|e| {
            format!(
                "trace={} {:?} step={} action={} expected invariants hold; actual={} actions={:?}",
                trace.run,
                trace.property,
                step + 1,
                action,
                e,
                trace.actions
            )
        })?;
    }
    if window && trace.property == "byte and count cuts" {
        let head = &replay.reader.state().head;
        let sizes: Vec<_> = head.window.iter().map(|r| r.encoded.len()).collect();
        require(head.batches == [3, 4] && sizes == [20_000, 20_000], || {
            format!(
                "trace={} expected batch ends [3, 4], window bytes [20000, 20000]; actual {:?}, {sizes:?}",
                trace.run, head.batches
            )
        })?;
        println!(
            "CUTS backend={} batch_ends={:?} window_record_bytes={sizes:?} head_encoded_bytes={}",
            if local { "durable-local" } else { "memory" },
            head.batches,
            head.encoded_len()
        );
    }
    Ok(replay.skipped.clone())
}
async fn positive(text: &str) {
    let traces = traces(text);
    let mut skips = BTreeMap::<String, usize>::new();
    for local in [false, true] {
        for trace in &traces {
            for (reason, count) in replay(trace, local, Control::Safe)
                .await
                .expect("positive replay")
            {
                *skips.entry(reason).or_default() += count;
            }
        }
    }
    println!(
        "{}: witnesses={} walks={} backends=memory,durable-local skipped={skips:?}",
        traces[0].run,
        traces.iter().filter(|t| t.kind == "witness").count(),
        traces.iter().filter(|t| t.kind == "walk").count()
    );
}
async fn negative(text: &str, control: Control, invariant: &str) {
    let traces: Vec<_> = traces(text)
        .into_iter()
        .filter(|t| t.status == "FAIL" && t.property.contains(invariant))
        .collect();
    assert!(!traces.is_empty(), "control trace must exist");
    for local in [false, true] {
        for trace in &traces {
            replay(trace, local, Control::Safe)
                .await
                .expect("same control trace with safeguards");
            let failure = replay(trace, local, control)
                .await
                .expect_err("removed safeguard must be detected");
            assert!(failure.contains(invariant), "wrong failure: {failure}");
            println!(
                "CONTROL backend={} safe=holds removed=caught {failure}",
                if local { "durable-local" } else { "memory" }
            );
        }
    }
}
#[tokio::test]
async fn commits_restart_turnover() {
    positive(include_str!("traces/commits-restart-turnover.jsonl")).await;
}
#[tokio::test]
async fn window_storage() {
    positive(include_str!("traces/window-storage.jsonl")).await;
}
#[tokio::test]
async fn source_positions() {
    positive(include_str!("traces/source-positions.jsonl")).await;
}
#[tokio::test]
async fn control_persist_before_send() {
    negative(
        include_str!("traces/commits-no-persist.jsonl"),
        Control::NoPersist,
        "I2",
    )
    .await;
}
#[tokio::test]
async fn control_ack_before_acceptance() {
    negative(
        include_str!("traces/source-ack-early.jsonl"),
        Control::AckEarly,
        "I2",
    )
    .await;
}
#[tokio::test]
async fn control_redelivery_skip() {
    negative(
        include_str!("traces/source-no-skip.jsonl"),
        Control::NoSkip,
        "I10",
    )
    .await;
}
#[tokio::test]
async fn control_batch_exists() {
    negative(
        include_str!("traces/window-no-exists.jsonl"),
        Control::NoExists,
        "I4",
    )
    .await;
}

#[tokio::test]
async fn listed_missing_batch_is_error() {
    for local in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let inner: Arc<dyn ObjectStore> = if local {
            Arc::new(LocalConditionalPut::new_durable(directory.path()).expect("local"))
        } else {
            Arc::new(object_store::memory::InMemory::new())
        };
        let mut replay = Replay::new(inner, false, false, Control::Safe).await;
        let (mut lagging, _) = Reader::open(replay.object_store(), Path::from("replay"))
            .await
            .expect("lagging");
        replay
            .append(Bytes::from_static(b"one"))
            .await
            .expect("append");
        replay
            .append(Bytes::from_static(b"two"))
            .await
            .expect("append");
        replay.copy(0).await.expect("create");
        replay
            .append(Bytes::from_static(b"three"))
            .await
            .expect("append");
        replay
            .store
            .delete(&Path::from("replay/wal/00000000000000000001"))
            .await
            .expect("delete");
        let open_error = Reader::open(replay.object_store(), Path::from("replay"))
            .await
            .expect_err("missing listed object");
        let poll_error = lagging.poll().await.expect_err("missing listed object");
        assert_eq!(
            lagging.state().head.commit,
            0,
            "failed poll preserves cursor"
        );
        println!(
            "I4 backend={} open={open_error}; poll={poll_error}; cursor=0",
            if local { "durable-local" } else { "memory" }
        );
    }
}
