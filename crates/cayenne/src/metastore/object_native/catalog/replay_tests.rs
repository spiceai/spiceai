/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Replay of the `cayenne-metastore-model` `transactions` traces through two
//! real dataset logs, the transaction's records (intent and decision) and their
//! removal.
//!
//! Each record the model lands on a participant's log is the same operation on
//! that participant's log here: a write to another key (a source position), a
//! `Prepare`, a write to the prepared table, or a `Resolve` with the outcome
//! read back from the decision object. A delivery the model refuses because a
//! newer lease fenced it runs through the fenced handle and must be refused.
//! `Fence` opens the log again, which takes it over. `Decide` records a
//! decision create-if-absent: the coordinator's commit, another process's
//! abort, or the coordinator's abort at its deadline. `Report` is the
//! coordinator reading the outcome it reports to its client by
//! [`confirmed_outcome`], which must be the model's and the first recorded
//! decision. `Forget(1)` is another process removing the
//! records by [`remove_if_finished`], from storage alone; the prototype must
//! remove exactly when the model does. `Forget(0)` is the coordinator removing
//! them, which `commit_across` does only after every `Prepare` it sent has
//! returned: the replay refuses it while the model's coordinator has one in
//! flight, and otherwise removes the records. `Gc` collects a participant's
//! staged objects, which the head log's own GC does, so it is counted.
//!
//! Each `Check`, identical retry and lost response of one write is the head
//! log's own protocol, which the `commits` and `source-positions` replays in
//! `object_store_occ` cover; here a write is issued once, at the delivery that
//! lands it. Checkpoints are the logs' own (a checkpoint at every sealed batch of
//! two records), so `Checkpoint` and `Publish` are counted, not driven: every
//! check reads each log from storage alone, through its checkpoint.
//!
//! After every action, the replay compares what storage holds with the model
//! stepped through the same actions (`cayenne_metastore_model::transaction_steps`):
//! whether each participant holds the transaction prepared, whether its
//! publication is visible, how many writes and other-key writes landed, and the
//! recorded decision. It also checks, from storage alone and whatever the model
//! says, the guarantees the model's negative controls break:
//!
//! - reservation: no write to a prepared table lands;
//! - matching: no `Resolve` lands without its `Prepare`;
//! - create-once: a recorded decision never changes while the intent exists;
//! - agreement: every participant resolves to the first recorded decision;
//! - report: the coordinator reports the first recorded decision or nothing;
//! - records: a transaction's records survive while a log holds it prepared.

use super::{
    INTENT, confirmed_outcome, decide, forget_records, read_decision, remove_if_finished,
};
use crate::{
    catalog::{CatalogPublication, MetadataCatalog},
    metadata::{CreateTableOptions, VortexConfig},
    metastore::object_native::{Domain, NativeCatalogConfig, position, replay_state},
};
use cayenne_metastore_actions::{DatasetState, proto};
use prost::Message;
use cayenne_metastore_model::TransactionStep;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, path::Path};
use object_store_occ::headlog::{HeadLogConfig, Reader};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

const TXN: &str = "01a19500-0000-7000-8000-000000000000";

struct Trace {
    run: String,
    kind: String,
    status: String,
    property: String,
    actions: Vec<String>,
}

fn traces(text: &str) -> Vec<Trace> {
    text.lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("driver JSON");
            let field = |name: &str| value[name].as_str().unwrap_or_default().to_string();
            Trace {
                run: field("run"),
                kind: field("kind"),
                status: field("status"),
                property: field("property"),
                actions: value["actions"]
                    .as_array()
                    .expect("actions")
                    .iter()
                    .map(|action| action.as_str().expect("action").to_string())
                    .collect(),
            }
        })
        .collect()
}

fn config() -> NativeCatalogConfig {
    NativeCatalogConfig {
        log: HeadLogConfig {
            batch_max_records: 2,
            ..HeadLogConfig::default()
        },
        checkpoint_after_batches: 1,
        ..NativeCatalogConfig::default()
    }
}

fn publication(table_id: &str, snapshot: &str, sequence: i64) -> CatalogPublication {
    CatalogPublication {
        table_id: table_id.to_string(),
        snapshot_id: snapshot.to_string(),
        snapshot_sequence: sequence,
        insert_sequence: None,
        delete_files: Vec::new(),
        insert_pk_bytes: Vec::new(),
        inline_tombstone: None,
        pending_durable_flips: Vec::new(),
        dirty_pk_bytes: Vec::new(),
    }
}

/// What a participant holds of the transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct View {
    prepared: bool,
    committed: bool,
    writes: usize,
    others: usize,
}

impl View {
    /// The model's participant log, folded.
    fn of_model(log: &[(u8, u8)]) -> Self {
        let mut view = Self {
            prepared: false,
            committed: false,
            writes: 0,
            others: 0,
        };
        for &(kind, outcome) in log {
            match kind {
                0 => view.others += 1,
                1 => view.prepared = true,
                2 => view.writes += 1,
                3 => {
                    view.prepared = false;
                    view.committed = outcome == 1;
                }
                _ => {}
            }
        }
        view
    }

    fn of_state(state: &DatasetState, table: &str) -> Self {
        let groups = &state.tables[table].groups;
        Self {
            prepared: state.prepared.contains_key(TXN),
            committed: groups.contains_key("txn"),
            writes: groups.keys().filter(|id| id.starts_with("w-")).count(),
            others: state
                .positions
                .keys()
                .filter(|(source, _)| source == "other")
                .count(),
        }
    }
}

struct Participant {
    prefix: Path,
    table: String,
    /// The log's handles by model lease; the newest one owns it.
    handles: BTreeMap<usize, Arc<Domain>>,
    lease: usize,
    /// The lease the latest `Issue` was made under.
    issued: usize,
    footprint: Option<Vec<String>>,
    resolved: Option<bool>,
}

#[derive(Debug, Default)]
struct Outcome {
    /// Storage differed from the model, or refused what the model lands.
    divergence: Option<String>,
    /// A guarantee broke, from storage alone.
    violation: Option<String>,
    /// Actions replayed and actions counted without a call.
    replayed: BTreeMap<&'static str, usize>,
    counted: BTreeMap<String, usize>,
}

struct Replay {
    store: Arc<dyn ObjectStore>,
    transactions: Path,
    participants: Vec<Participant>,
    /// The first decision read back.
    decided: Option<bool>,
    sequence: i64,
}

impl Replay {
    async fn new(store: Arc<dyn ObjectStore>, count: usize) -> Self {
        let mut participants = Vec::with_capacity(count);
        for i in 0..count {
            let prefix = Path::from(format!("datasets/p{i}"));
            let domain = Domain::create_with_config(Arc::clone(&store), prefix.clone(), config(), false)
                .await
                .expect("create a participant's log");
            let table = domain
                .create_table(CreateTableOptions {
                    table_name: format!("t{i}"),
                    schema: Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        "id",
                        arrow_schema::DataType::Int64,
                        false,
                    )])),
                    primary_key: vec!["id".to_string()],
                    on_conflict: None,
                    base_path: format!("/tmp/unused/t{i}"),
                    partition_column: None,
                    vortex_config: VortexConfig::default(),
                })
                .await
                .expect("create a participant's table");
            participants.push(Participant {
                prefix,
                table,
                handles: BTreeMap::from([(1, Arc::new(domain))]),
                lease: 1,
                issued: 1,
                footprint: None,
                resolved: None,
            });
        }
        let transactions = Path::from("transactions");
        let intent = proto::Intent {
            participants: (0..count)
                .map(|i| proto::Participant {
                    folder: format!("p{i}"),
                    lease: participants[i].handles[&1].opened.lease,
                })
                .collect(),
            deadline_unix_millis: 0,
        };
        store
            .put_opts(
                &transactions.clone().join(TXN).join(INTENT),
                PutPayload::from(intent.encode_to_vec()),
                PutMode::Create.into(),
            )
            .await
            .expect("write the intent");
        Self {
            store,
            transactions,
            participants,
            decided: None,
            sequence: 0,
        }
    }

    async fn read(&self, i: usize) -> DatasetState {
        let (_, opened) = Reader::open(Arc::clone(&self.store), self.participants[i].prefix.clone())
            .await
            .expect("open a participant's log from storage");
        replay_state(opened).expect("replay a participant's log").state
    }

    async fn view(&self, i: usize) -> View {
        View::of_state(&self.read(i).await, &self.participants[i].table)
    }

    /// Whether the intent exists.
    async fn intent(&self) -> bool {
        match self
            .store
            .head(&self.transactions.clone().join(TXN).join(INTENT))
            .await
        {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(error) => panic!("read the intent: {error}"),
        }
    }

    /// The recorded decision: `Some(true)` to commit.
    async fn decision(&self) -> Option<bool> {
        read_decision(&self.store, &self.transactions, TXN)
            .await
            .expect("read the decision")
    }

    /// Run a record of `kind` on participant `i` through the handle of `lease`.
    async fn write(&mut self, i: usize, lease: usize, kind: u8) -> crate::catalog::CatalogResult<()> {
        self.sequence += 1;
        let sequence = self.sequence;
        let decision = self.decision().await;
        let participant = &self.participants[i];
        let handle = Arc::clone(&participant.handles[&lease]);
        let table = participant.table.clone();
        match kind {
            0 => {
                handle
                    .commit_actions(vec![position("other", &sequence.to_string(), Some(vec![1]))])
                    .await
            }
            1 => {
                let reservation = handle.inner.reserve().await;
                let footprint = handle
                    .prepare(TXN, &[publication(&table, "txn", sequence)])
                    .await;
                drop(reservation);
                self.participants[i].footprint = Some(footprint?);
                Ok(())
            }
            2 => {
                handle
                    .commit_publications(&[publication(&table, &format!("w-{sequence}"), sequence)])
                    .await?;
                handle.commit_pending().await
            }
            _ => {
                // A resolver commits what the decision object records.
                let commit = decision == Some(true);
                let footprint = participant
                    .footprint
                    .clone()
                    .unwrap_or_else(|| vec![table.clone()]);
                let reservation = handle.inner.reserve().await;
                let resolved = handle.resolve(TXN, footprint, commit).await;
                drop(reservation);
                resolved?;
                self.participants[i].resolved = Some(commit);
                Ok(())
            }
        }
    }

    /// The guarantees, from storage alone.
    async fn guarantees(&mut self) -> Option<String> {
        let decision = self.decision().await;
        if let Some(decided) = self.decided {
            if self.intent().await && decision.is_some_and(|now| now != decided) {
                return Some("create-once: the recorded decision changed".to_string());
            }
        } else {
            self.decided = decision;
        }
        let mut outcomes = HashSet::new();
        for participant in &self.participants {
            if let Some(commit) = participant.resolved {
                outcomes.insert(commit);
            }
        }
        if outcomes.len() > 1 || self.decided.is_some_and(|d| outcomes.iter().any(|&o| o != d)) {
            return Some(format!(
                "agreement: participants resolved {outcomes:?} with decision {:?}",
                self.decided
            ));
        }
        for i in 0..self.participants.len() {
            if self.view(i).await.prepared && !self.intent().await {
                return Some(format!(
                    "records: participant {i} holds the transaction prepared and its records are gone"
                ));
            }
        }
        None
    }

    async fn run(mut self, run: &str, actions: &[String]) -> Outcome {
        let steps = cayenne_metastore_model::transaction_steps(run, actions)
            .unwrap_or_else(|| panic!("{run}: the model does not take {actions:?}"));
        let mut outcome = Outcome::default();
        for (index, (text, step)) in actions.iter().zip(&steps).enumerate() {
            let at = |what: String| format!("step {index} {text}: {what}");
            let name = text.split('(').next().unwrap_or(text);
            let TransactionStep {
                landed,
                refused,
                fenced,
                ..
            } = step;
            if let Some(&(i, kind, model_outcome)) = landed.as_ref() {
                let lease = self.participants[i].lease;
                if kind == 1 && lease != 1 {
                    // Only the coordinator writes a `Prepare`, under the lease it
                    // held when the transaction began.
                    outcome.divergence = Some(at("the model prepares under a later lease".into()));
                    return outcome;
                }
                let before = self.view(i).await;
                if let Err(error) = self.write(i, lease, kind).await {
                    outcome.divergence = Some(at(format!("refused a record the model lands: {error}")));
                    return outcome;
                }
                if kind == 2 && before.prepared {
                    outcome.violation = Some(at("reservation: a write landed on a prepared table".into()));
                    return outcome;
                }
                if kind == 3 && !before.prepared {
                    outcome.violation = Some(at("matching: a Resolve landed without its Prepare".into()));
                    return outcome;
                }
                if kind == 3 && self.participants[i].resolved != Some(model_outcome == 1) {
                    outcome.divergence = Some(at(format!(
                        "resolved {:?} where the model resolves outcome {model_outcome}",
                        self.participants[i].resolved
                    )));
                    return outcome;
                }
                *outcome.replayed.entry("landed").or_default() += 1;
            } else if let Some(&(i, kind, true)) = refused.as_ref() {
                let issued = self.participants[i].issued;
                let before = self.read(i).await;
                if self.write(i, issued, kind).await.is_ok() {
                    outcome.divergence = Some(at("a fenced handle's record landed".into()));
                    return outcome;
                }
                if self.read(i).await != before {
                    outcome.divergence = Some(at("a refused record changed the log".into()));
                    return outcome;
                }
                *outcome.replayed.entry("fenced").or_default() += 1;
            } else if refused.is_some() {
                *outcome.counted.entry("Deliver after another record".into()).or_default() += 1;
            } else if let Some(i) = *fenced {
                let participant = &mut self.participants[i];
                let domain = Domain::open_with_config(
                    Arc::clone(&self.store),
                    participant.prefix.clone(),
                    config(),
                    false,
                )
                .await
                .expect("take a participant's log over");
                participant.lease += 1;
                participant.handles.insert(participant.lease, Arc::new(domain));
                *outcome.replayed.entry("fence").or_default() += 1;
            } else if name == "Decide" {
                let commit = text == "Decide(1)";
                let recorded = decide(&self.store, &self.transactions, TXN, commit)
                    .await
                    .expect("record a decision");
                if recorded != (step.decision == 1) {
                    outcome.divergence = Some(at(format!(
                        "recorded {} where the model records {}",
                        if recorded { "commit" } else { "abort" },
                        step.decision
                    )));
                    return outcome;
                }
                *outcome.replayed.entry("decide").or_default() += 1;
            } else if text == "Report" {
                let reported = confirmed_outcome(&self.store, &self.transactions, TXN)
                    .await
                    .expect("read the outcome");
                if let (Some(reported), Some(decided)) = (reported, self.decided)
                    && reported != decided
                {
                    outcome.violation = Some(at(format!(
                        "report: the coordinator reports {reported} where the decision is {decided}"
                    )));
                    return outcome;
                }
                let expected = match step.reported {
                    0 => None,
                    reported => Some(reported == 1),
                };
                if reported != expected {
                    outcome.divergence = Some(at(format!(
                        "reports {reported:?} where the model reports {}",
                        step.reported
                    )));
                    return outcome;
                }
                *outcome.replayed.entry("report").or_default() += 1;
            } else if text == "Forget(0)" {
                if step.prepare_in_flight.iter().any(|&in_flight| in_flight) {
                    outcome.divergence = Some(at(
                        "the coordinator removes the records only after every Prepare it sent has returned".into(),
                    ));
                    return outcome;
                }
                forget_records(&self.store, &self.transactions, TXN)
                    .await
                    .expect("remove the records");
                *outcome.replayed.entry("forget-coordinator").or_default() += 1;
            } else if text == "Forget(1)" {
                let log = |folder: &str| Path::from(format!("datasets/{folder}"));
                let removed = remove_if_finished(&self.store, &self.transactions, &log, TXN)
                    .await
                    .expect("remove finished records");
                if !removed {
                    outcome.divergence = Some(at("kept records the model removes".into()));
                    return outcome;
                }
                *outcome.replayed.entry("forget-other").or_default() += 1;
            } else {
                if let Some(i) = text
                    .strip_prefix("Issue(")
                    .and_then(|rest| rest.split(',').next())
                    .and_then(|i| i.parse::<usize>().ok())
                {
                    self.participants[i].issued = self.participants[i].lease;
                }
                *outcome.counted.entry(name.to_string()).or_default() += 1;
            }
            if let Some(violation) = self.guarantees().await {
                outcome.violation = Some(at(violation));
                return outcome;
            }
            for (i, log) in step.logs.iter().enumerate() {
                let expected = View::of_model(log);
                let actual = self.view(i).await;
                if actual != expected {
                    outcome.divergence = Some(at(format!(
                        "participant {i} holds {actual:?}; the model holds {expected:?}"
                    )));
                    return outcome;
                }
            }
            let expected = match step.decision {
                0 => None,
                decision => Some(decision == 1),
            };
            let intent = self.intent().await;
            if intent != step.records {
                outcome.divergence = Some(at(format!(
                    "the intent exists: {intent}; the model's records exist: {}",
                    step.records
                )));
                return outcome;
            }
            if self.decision().await != expected {
                outcome.divergence = Some(at(format!(
                    "decision {:?}; the model records {}",
                    self.decision().await,
                    step.decision
                )));
                return outcome;
            }
        }
        outcome
    }
}

async fn replay(trace: &Trace) -> Vec<(&'static str, Outcome)> {
    let participants = if trace.run.contains("three-participants") { 3 } else { 2 };
    let directory = tempfile::tempdir().expect("temporary directory");
    let stores: [(&'static str, Arc<dyn ObjectStore>); 2] = [
        ("memory", Arc::new(object_store::memory::InMemory::new())),
        (
            "durable-local",
            Arc::new(
                object_store_occ::LocalConditionalPut::new_durable(directory.path())
                    .expect("local store"),
            ),
        ),
    ];
    let mut outcomes = Vec::new();
    for (backend, store) in stores {
        let outcome = Replay::new(store, participants)
            .await
            .run(&trace.run, &trace.actions)
            .await;
        outcomes.push((backend, outcome));
    }
    outcomes
}

fn describe(trace: &Trace) -> String {
    format!(
        "{} {} '{}' {}: {}",
        trace.run,
        trace.kind,
        trace.property,
        trace.status,
        trace.actions.join(" ")
    )
}

/// Every witness and walk replays on both backends without a divergence or a
/// broken guarantee.
async fn positive(text: &str) {
    let mut replayed: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut counted: BTreeMap<String, usize> = BTreeMap::new();
    let mut count = 0;
    for trace in traces(text) {
        for (backend, outcome) in replay(&trace).await {
            assert!(
                outcome.divergence.is_none() && outcome.violation.is_none(),
                "{backend} {}: {outcome:?}",
                describe(&trace)
            );
            for (name, n) in outcome.replayed {
                *replayed.entry(name).or_default() += n;
            }
            for (name, n) in outcome.counted {
                *counted.entry(name).or_default() += n;
            }
            count += 1;
        }
    }
    eprintln!(
        "REPLAYED traces×backends={count} replayed={replayed:?} counted={counted:?}"
    );
}

/// Each witness that breaks `property` in the model: every guarantee holds on
/// both backends. Where the model's step that breaks it is a record or a
/// decision, the prototype refuses it, which shows as a divergence.
async fn negative(text: &str, property: &str, refuses: bool) {
    let mut checked = 0;
    for trace in traces(text) {
        if trace.status != "FAIL" || trace.property != property {
            continue;
        }
        for (backend, outcome) in replay(&trace).await {
            assert!(
                outcome.violation.is_none(),
                "{backend} {}: {outcome:?}",
                describe(&trace)
            );
            assert_eq!(
                outcome.divergence.is_some(),
                refuses,
                "{backend} {}: {outcome:?}",
                describe(&trace)
            );
            eprintln!(
                "CONTROL {backend} {}: {}",
                describe(&trace),
                outcome
                    .divergence
                    .unwrap_or_else(|| format!("followed, guarantees hold; replayed {:?}", outcome.replayed))
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no FAIL witness for '{property}'");
}

#[tokio::test]
async fn transactions_replay() {
    positive(include_str!("../../../../tests/traces/transactions.jsonl")).await;
}

#[tokio::test]
async fn control_reservation() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-reservation.jsonl"),
        "reservation excludes intersecting records",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_matching() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-matching.jsonl"),
        "well-formed transaction prefix",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_create_once() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-create-once.jsonl"),
        "I6 participants never disagree",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_fence_check() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-fence-check.jsonl"),
        "a prepared participant keeps its transaction's records",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_coordinator_wait() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-coordinator-wait.jsonl"),
        "a prepared participant keeps its transaction's records",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_prepared_check() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-prepared-check.jsonl"),
        "a prepared participant keeps its transaction's records",
        true,
    )
    .await;
}

#[tokio::test]
async fn control_intent_check() {
    negative(
        include_str!("../../../../tests/traces/transactions-no-intent-check.jsonl"),
        "the coordinator reports the transaction's decision",
        true,
    )
    .await;
}
