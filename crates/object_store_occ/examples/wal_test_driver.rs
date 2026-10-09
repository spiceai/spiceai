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

//! JSON-lines worker for the process-level WAL qualification harness.
//! Run via `test/object_store_occ/e2e.py`; this is not a runtime API.
#![expect(
    clippy::expect_used,
    reason = "qualification worker reports test preconditions"
)]

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store_occ::LocalConditionalPut;
use object_store_occ::store::{
    self, ExpectedRevision, ObjectStoreState, StateChange, StateRecord, StateStore, WriteId,
    WriteOutcome,
};
use object_store_occ::wal::{
    CommitOutcome, CommitReceipt, Limits, PreparedCommit, Resolution, Snapshot, Transaction,
    WalStateStore,
};
use serde::Deserialize;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Snapshot {
        name: String,
    },
    Read {
        snapshot: Option<String>,
        prefix: String,
    },
    Get {
        snapshot: Option<String>,
        transaction: Option<String>,
        key: String,
    },
    Begin {
        name: String,
        snapshot: Option<String>,
    },
    Change {
        name: String,
        changes: BTreeMap<String, Option<Bytes>>,
    },
    View {
        name: String,
        prefix: String,
    },
    Prepare {
        name: String,
    },
    Commit {
        name: String,
        barrier: Option<u8>,
    },
    Checkpoint {
        snapshot: Option<String>,
        barrier: Option<u8>,
    },
    Resolve {
        receipt: CommitReceipt,
    },
    Release {
        name: String,
    },
}

fn emit(value: &Value) {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value).expect("write protocol response");
    writeln!(stdout).expect("terminate protocol response");
    stdout.flush().expect("flush protocol response");
}

struct Barriers {
    inner: Arc<dyn StateStore>,
    armed: Arc<AtomicU8>,
}

impl Barriers {
    async fn stop(&self, point: u8) {
        if self.armed.load(Ordering::SeqCst) == point {
            emit(&json!({"paused": point}));
            std::future::pending::<()>().await;
        }
    }
}

#[async_trait]
impl StateStore for Barriers {
    async fn read(&self, key: &Path) -> store::Result<Option<StateRecord>> {
        self.inner.read(key).await
    }

    async fn compare_exchange(
        &self,
        key: &Path,
        expected: ExpectedRevision<'_>,
        change: StateChange,
        id: WriteId,
    ) -> store::Result<WriteOutcome> {
        if key.as_ref().ends_with("/head") {
            self.stop(1).await;
        }
        let outcome = self
            .inner
            .compare_exchange(key, expected, change, id)
            .await?;
        if matches!(outcome, WriteOutcome::Applied) {
            if key.as_ref().contains("/wal/") {
                self.stop(2).await;
            }
            if key.as_ref().ends_with("/head") {
                self.stop(3).await;
            }
            if key.as_ref().contains("/pages/") {
                self.stop(4).await;
            }
            if key.as_ref().contains("/checkpoints/") {
                self.stop(5).await;
            }
        }
        Ok(outcome)
    }
}

struct Driver {
    store: WalStateStore,
    armed: Arc<AtomicU8>,
    snapshots: BTreeMap<String, Snapshot>,
    transactions: BTreeMap<String, Transaction>,
    attempts: BTreeMap<String, PreparedCommit>,
}

impl Driver {
    async fn snapshot(&self, name: Option<&str>) -> Result<Snapshot> {
        match name {
            Some(name) => Ok(self.snapshots.get(name).ok_or("unknown snapshot")?.clone()),
            None => Ok(self.store.snapshot().await?),
        }
    }

    async fn execute(&mut self, command: Command) -> Result<Value> {
        match command {
            Command::Snapshot { name } => {
                let snapshot = self.store.snapshot().await?;
                let sequence = snapshot.sequence();
                self.snapshots.insert(name, snapshot);
                Ok(json!({"sequence": sequence}))
            }
            Command::Read { snapshot, prefix } => {
                let snapshot = self.snapshot(snapshot.as_deref()).await?;
                let rows: Vec<_> = snapshot.scan_prefix(&prefix).collect();
                Ok(json!({"sequence": snapshot.sequence(), "rows": rows}))
            }
            Command::Get {
                snapshot,
                transaction,
                key,
            } => {
                let value = if let Some(name) = transaction {
                    if snapshot.is_some() {
                        return Err("select either transaction or snapshot".into());
                    }
                    self.transactions
                        .get(&name)
                        .ok_or("unknown transaction")?
                        .get(&key)
                        .cloned()
                } else {
                    self.snapshot(snapshot.as_deref()).await?.get(&key).cloned()
                };
                Ok(json!({"value": value}))
            }
            Command::Begin { name, snapshot } => {
                let snapshot = self.snapshot(snapshot.as_deref()).await?;
                let sequence = snapshot.sequence();
                self.transactions.insert(name, snapshot.transaction());
                Ok(json!({"sequence": sequence}))
            }
            Command::Change { name, changes } => {
                let tx = self
                    .transactions
                    .get_mut(&name)
                    .ok_or("unknown transaction")?;
                for (key, value) in changes {
                    match value {
                        Some(value) => tx.put(key, value)?,
                        None => tx.delete(key)?,
                    }
                }
                Ok(json!({"ok": true}))
            }
            Command::View { name, prefix } => {
                let tx = self.transactions.get(&name).ok_or("unknown transaction")?;
                let rows: Vec<_> = tx.scan_prefix(&prefix).await?.into_iter().collect();
                Ok(json!({"rows": rows}))
            }
            Command::Prepare { name } => {
                let tx = self
                    .transactions
                    .remove(&name)
                    .ok_or("unknown transaction")?;
                let attempt = tx.prepare().await?;
                let receipt = serde_json::to_value(attempt.receipt())?;
                self.attempts.insert(name, attempt);
                Ok(json!({"receipt": receipt}))
            }
            Command::Commit { name, barrier } => {
                let attempt = self.attempts.get(&name).ok_or("unknown prepared commit")?;
                self.armed.store(barrier.unwrap_or(0), Ordering::SeqCst);
                let result = self.store.commit(attempt).await?;
                self.armed.store(0, Ordering::SeqCst);
                Ok(match result {
                    CommitOutcome::Committed { sequence } => {
                        json!({"outcome": "committed", "sequence": sequence})
                    }
                    CommitOutcome::Conflict => json!({"outcome": "conflict"}),
                    CommitOutcome::Unknown { source } => {
                        json!({"outcome": "unknown", "diagnostic": format!("{source:?}")})
                    }
                })
            }
            Command::Checkpoint { snapshot, barrier } => {
                let snapshot = self.snapshot(snapshot.as_deref()).await?;
                self.armed.store(barrier.unwrap_or(0), Ordering::SeqCst);
                let result = self.store.checkpoint(&snapshot).await?;
                self.armed.store(0, Ordering::SeqCst);
                Ok(json!({"outcome": match result {
                    WriteOutcome::Applied => "applied",
                    WriteOutcome::Conflict => "conflict",
                    WriteOutcome::Unknown { .. } => "unknown",
                }}))
            }
            Command::Resolve { receipt } => Ok(match self.store.resolve(&receipt).await? {
                Resolution::Committed { sequence } => {
                    json!({"outcome": "committed", "sequence": sequence})
                }
                Resolution::Rejected => json!({"outcome": "rejected"}),
                Resolution::Pending => json!({"outcome": "pending"}),
            }),
            Command::Release { name } => {
                self.snapshots.remove(&name);
                self.transactions.remove(&name);
                self.attempts.remove(&name);
                Ok(json!({"ok": true}))
            }
        }
    }
}

fn main() -> Result<()> {
    let root = std::env::args()
        .nth(1)
        .ok_or("expected storage directory")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let inner: Arc<dyn StateStore> = Arc::new(ObjectStoreState::new(Arc::new(
        LocalConditionalPut::new(root)?,
    )));
    let armed = Arc::new(AtomicU8::new(0));
    let state: Arc<dyn StateStore> = Arc::new(Barriers {
        inner,
        armed: Arc::clone(&armed),
    });
    let store = runtime.block_on(WalStateStore::open(
        state,
        Path::from("domain"),
        Limits::default(),
    ))?;
    let mut driver = Driver {
        store,
        armed,
        snapshots: BTreeMap::new(),
        transactions: BTreeMap::new(),
        attempts: BTreeMap::new(),
    };
    emit(&json!({"ready": true}));
    // Stdio is synchronous on the supervisor thread, outside the async runtime.
    for line in std::io::stdin().lock().lines() {
        let command = serde_json::from_str(&line?)?;
        let response = runtime.block_on(driver.execute(command));
        emit(&response.unwrap_or_else(|error| json!({"error": error.to_string()})));
    }
    Ok(())
}
