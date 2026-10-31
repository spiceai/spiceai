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

//! The protocol model's `source-positions` traces replayed through the
//! object-native catalog itself: rows staged as inline data and the source
//! position staged beside them in a deferred scope, published by the table's
//! acknowledgement point (`commit_pending_for`), with the head CAS held,
//! delivered, re-delivered and its response lost as each trace says.
//!
//! After every action the log is read back from storage alone and folded with
//! `DatasetState::apply`, never through the catalog: the logged rows must be
//! exactly the numbered stream up to the logged position (I10), and nothing is
//! acknowledged beyond it (I2). The traces are the ones `object_store_occ`'s
//! head-log replay uses (see its `tests/traces/README.md`).

use std::sync::Arc;
use std::time::Duration;

use cayenne::MetadataCatalog;
use cayenne::metadata::{CreateTableOptions, InlinedData, VortexConfig};
use cayenne::metastore::object_native::ObjectNativeCatalog;
use cayenne_metastore_actions::DatasetState;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_occ::{
    headlog::Reader,
    sim::{
        Op,
        fault::{Fault, FaultStore},
    },
};
use prost::Message;
use serde::Deserialize;
use tokio::task::JoinHandle;

const TABLE: &str = "orders";
const POSITION_KEY: &str = "spice_sys_applied_lsn/orders";
const STEP: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct Trace {
    run: String,
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

fn is_head(path: &Path) -> bool {
    path.as_ref() == "catalog/head"
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Control {
    Safe,
    /// The position is committed on its own before the rows it covers, as a
    /// position recorded outside the deferred scope would be.
    PositionFirst,
}

struct Attempt {
    task: Option<JoinHandle<Result<(), String>>>,
    wires: Vec<u64>,
}

struct Replay {
    faults: Arc<FaultStore>,
    store: Arc<dyn ObjectStore>,
    catalog: Option<Arc<ObjectNativeCatalog>>,
    table_id: String,
    control: Control,
    half: bool,
    attempts: Vec<Attempt>,
    active: Option<usize>,
    /// Bytes added to each staged row batch, to exceed the catalog's buffer limits.
    padding: usize,
    buffer: Vec<usize>,
    cursor: usize,
    acknowledged: usize,
    sequence: i64,
}

/// The logged rows and position, from storage alone.
async fn logged(store: &Arc<dyn ObjectStore>, table_id: &str) -> Result<(Vec<usize>, usize), String> {
    let (_, opened) = Reader::open(Arc::clone(store), Path::from("catalog"))
        .await
        .map_err(|e| format!("I4 expected readable log; actual {e}"))?;
    if opened.checkpoint_root.is_some() {
        return Err("these traces commit too few records to checkpoint".into());
    }
    let mut state = DatasetState::default();
    for record in opened.records {
        let group = object_store_occ::headlog::Group::decode(record.record.payload)
            .map_err(|e| e.to_string())?;
        for payload in group.payloads {
            let record =
                cayenne_metastore_actions::Record::decode(payload).map_err(|e| e.to_string())?;
            state.apply(&record).map_err(|e| e.to_string())?;
        }
    }
    let mut rows = vec![];
    if let Some(table) = state.tables.get(table_id) {
        let mut entries: Vec<_> = table.inlined_data.values().collect();
        entries.sort_by_key(|entry| entry.seq);
        for entry in entries {
            let (batch, _padding): (Vec<usize>, String) =
                serde_json::from_slice(&entry.raw_payload).map_err(|e| e.to_string())?;
            rows.extend(batch);
        }
    }
    let position = state
        .positions
        .get(&(POSITION_KEY.to_string(), String::new()))
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| format!("unreadable position {bytes:?}"))
        })
        .transpose()?
        .unwrap_or(0);
    Ok((rows, position))
}

impl Replay {
    async fn new(control: Control) -> Self {
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let store: Arc<dyn ObjectStore> = Arc::clone(&faults) as Arc<dyn ObjectStore>;
        let catalog = ObjectNativeCatalog::create(Arc::clone(&store), Path::from("catalog"))
            .await
            .expect("create the catalog");
        let table_id = catalog
            .create_table(CreateTableOptions {
                table_name: TABLE.to_string(),
                schema: Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                ])),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: format!("/tmp/unused/{TABLE}"),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await
            .expect("create table");
        Self {
            faults,
            store,
            catalog: Some(Arc::new(catalog)),
            table_id,
            control,
            half: false,
            padding: 0,
            attempts: vec![],
            active: None,
            buffer: vec![],
            cursor: 1,
            acknowledged: 0,
            sequence: 0,
        }
    }

    async fn position(&self) -> Result<usize, String> {
        Ok(logged(&self.store, &self.table_id).await?.1)
    }

    /// Wait until the attempt's task ends or it sends a new head CAS, which
    /// becomes that attempt's latest wire.
    async fn follow(&mut self, index: usize) -> Result<(), String> {
        let after = self.faults.held_ids().into_iter().max();
        let faults = Arc::clone(&self.faults);
        let Some(mut task) = self.attempts[index].task.take() else {
            return Ok(());
        };
        tokio::select! {
            id = faults.wait_held(after) => {
                self.attempts[index].wires.push(id);
                self.attempts[index].task = Some(task);
            }
            joined = &mut task => {
                // An error leaves the catalog needing a reopen, as the model's
                // caller does after an unknown outcome.
                if joined.map_err(|e| e.to_string())?.is_err() {
                    self.catalog = None;
                }
            }
        }
        Ok(())
    }

    async fn step(&mut self, action: &str, args: &[usize]) -> Result<(), String> {
        match action {
            "Receive" => {
                if self.cursor > self.position().await? {
                    self.buffer.push(self.cursor);
                }
                self.cursor += 1;
            }
            "Issue" => {
                let position = self.position().await?;
                self.buffer.retain(|p| *p > position);
                let Some(&end) = self.buffer.last() else {
                    return Ok(());
                };
                let catalog = Arc::clone(self.catalog.as_ref().ok_or("Issue without a lease")?);
                let rows = if self.control == Control::PositionFirst && !self.half {
                    vec![]
                } else {
                    self.buffer.clone()
                };
                self.sequence += 1;
                let data = InlinedData {
                    inlined_id: String::new(),
                    table_id: self.table_id.clone(),
                    partition_key: None,
                    data_ipc: serde_json::to_vec(&(&rows, " ".repeat(self.padding)))
                        .map_err(|e| e.to_string())?,
                    record_count: i64::try_from(rows.len()).map_err(|e| e.to_string())?,
                    sequence_number: self.sequence,
                    created_at: String::new(),
                };
                let table_id = self.table_id.clone();
                let after = self.faults.held_ids().into_iter().max();
                self.faults.arm(Op::Cas, is_head, Fault::Hold);
                let task = tokio::spawn(async move {
                    cayenne::catalog::with_deferred_commits(async {
                        if !rows.is_empty() {
                            catalog.add_inlined_data(data).await?;
                        }
                        catalog
                            .set_sidecar_value(POSITION_KEY, Some(end.to_string().into_bytes()))
                            .await
                    })
                    .await
                    .map_err(|e| e.to_string())?;
                    catalog
                        .commit_pending_for(&table_id)
                        .await
                        .map_err(|e| e.to_string())
                });
                let wire = tokio::time::timeout(STEP, self.faults.wait_held(after))
                    .await
                    .map_err(|_| "Issue sent no head CAS".to_string())?;
                self.attempts.push(Attempt {
                    task: Some(task),
                    wires: vec![wire],
                });
                self.active = Some(self.attempts.len() - 1);
            }
            "Deliver" => {
                let wires = &self.attempts[args[0]].wires;
                let wire = if args[1] == 0 { wires.first() } else { wires.last() };
                let wire = *wire.ok_or("missing delivery")?;
                let _ = self.faults.deliver(wire).await;
            }
            "Resolve" => {
                // Release the active attempt's response as lost: the catalog
                // resolves the outcome itself and either finishes or re-sends the
                // same bytes, which is held as this attempt's next wire.
                if let Some(index) = self.active
                    && self.attempts[index].task.is_some()
                {
                    let wire = *self.attempts[index].wires.last().ok_or("no wire")?;
                    self.faults.arm(Op::Cas, is_head, Fault::Hold);
                    if self.faults.drop_response(wire).is_ok() {
                        tokio::time::timeout(STEP, self.follow(index))
                            .await
                            .map_err(|_| "Resolve neither finished nor re-sent".to_string())??;
                    }
                    self.faults.clear_rules();
                }
            }
            "Unknown" => {
                // The response is lost and the next head read fails.
                if let Some(index) = self.active
                    && self.attempts[index].task.is_some()
                {
                    let wire = *self.attempts[index].wires.last().ok_or("no wire")?;
                    self.faults.arm(Op::Get, is_head, Fault::Fail);
                    self.faults.arm(Op::Cas, is_head, Fault::Hold);
                    if self.faults.drop_response(wire).is_ok() {
                        tokio::time::timeout(STEP, self.follow(index))
                            .await
                            .map_err(|_| "Unknown neither finished nor re-sent".to_string())??;
                    }
                    self.faults.clear_rules();
                }
            }
            "Acknowledge" => {
                if self.control == Control::PositionFirst && !self.half {
                    self.half = true;
                    self.active = None;
                    return Ok(());
                }
                self.half = false;
                let position = self.position().await?;
                let end = self.buffer.last().copied().unwrap_or(position);
                if end <= position {
                    self.acknowledged = self.acknowledged.max(end);
                    self.buffer.clear();
                    self.active = None;
                }
            }
            "Crash" | "Takeover" => {
                for attempt in &mut self.attempts {
                    if let Some(task) = attempt.task.take() {
                        task.abort();
                        let _ = task.await;
                    }
                }
                self.catalog = None;
                self.active = None;
                self.half = false;
                self.buffer.clear();
                self.cursor = self.acknowledged + 1;
                if action == "Takeover" {
                    self.acquire().await?;
                }
            }
            "Acquire" => self.acquire().await?,
            other => return Err(format!("unmapped source action {other}")),
        }
        Ok(())
    }

    async fn acquire(&mut self) -> Result<(), String> {
        let catalog = ObjectNativeCatalog::open(Arc::clone(&self.store), Path::from("catalog"))
            .await
            .map_err(|e| format!("take over: {e}"))?;
        self.catalog = Some(Arc::new(catalog));
        Ok(())
    }

    async fn check(&self) -> Result<(), String> {
        let (rows, position) = logged(&self.store, &self.table_id).await?;
        let prefix: Vec<_> = (1..=position).collect();
        if rows != prefix {
            return Err(format!(
                "I10 expected rows {prefix:?} at position {position}; actual {rows:?}"
            ));
        }
        if self.acknowledged > position {
            return Err(format!(
                "I2 expected acknowledgement <= {position}; actual {}",
                self.acknowledged
            ));
        }
        Ok(())
    }
}

fn parse(action: &str) -> (&str, Vec<usize>) {
    match action.split_once('(') {
        Some((name, rest)) => (
            name,
            rest.trim_end_matches(')')
                .split(',')
                .map(|arg| arg.trim().parse().expect("numeric argument"))
                .collect(),
        ),
        None => (action, vec![]),
    }
}

async fn replay(trace: &Trace, control: Control) -> Result<(), String> {
    replay_padded(trace, control, 0).await
}

async fn replay_padded(trace: &Trace, control: Control, padding: usize) -> Result<(), String> {
    let mut replay = Replay::new(control).await;
    replay.padding = padding;
    for (step, action) in trace.actions.iter().enumerate() {
        let (name, args) = parse(action);
        let outcome = match tokio::time::timeout(STEP, replay.step(name, &args)).await {
            Ok(result) => result,
            Err(_) => Err("step timed out".to_string()),
        };
        outcome.and(replay.check().await).map_err(|e| {
            format!(
                "trace={} {:?} step={} action={action}: {e}; actions={:?}",
                trace.run,
                trace.property,
                step + 1,
                trace.actions
            )
        })?;
    }
    Ok(())
}

#[tokio::test]
async fn source_positions_replay_through_the_catalog() {
    let traces = traces(include_str!(
        "../../object_store_occ/tests/traces/source-positions.jsonl"
    ));
    assert!(traces.len() > 1, "trace set must not be empty");
    for trace in &traces {
        replay(trace, Control::Safe).await.expect("positive trace");
    }
    println!(
        "source-positions through ObjectNativeCatalog: {} traces replayed",
        traces.len()
    );
}

/// The model's `source-position-before-data` witnesses: with the position
/// staged beside its rows the catalog holds I10, and with the position committed
/// on its own first the readback catches it.
#[tokio::test]
async fn a_position_committed_before_its_rows_is_caught() {
    let traces: Vec<_> = traces(include_str!(
        "../../object_store_occ/tests/traces/source-position-before-data.jsonl"
    ))
    .into_iter()
    .filter(|t| t.status == "FAIL" && t.property.contains("I10"))
    .collect();
    assert!(!traces.is_empty(), "control trace must exist");
    for trace in &traces {
        replay(trace, Control::Safe)
            .await
            .expect("same trace with the position staged beside its rows");
        let failure = replay(trace, Control::PositionFirst)
            .await
            .expect_err("a position committed before its rows must be caught");
        assert!(failure.contains("I10"), "wrong failure: {failure}");
        println!("CONTROL safe=holds removed=caught {failure}");
    }
}

/// A change staged in a deferred scope commits in its acknowledgement point's
/// record however large it is: rows larger than any buffer limit still share
/// their record with the position that covers them.
#[tokio::test]
async fn a_large_change_commits_with_its_position() {
    let traces = traces(include_str!(
        "../../object_store_occ/tests/traces/source-positions.jsonl"
    ));
    for trace in &traces {
        replay_padded(trace, Control::Safe, 300 * 1024)
            .await
            .expect("positive trace with 300 KiB row batches");
    }
}
