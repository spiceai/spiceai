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

//! A test-only tap that records, for every committed metastore transaction,
//! the actions the object-native metastore would commit for it.
//!
//! The SQL metastore stays authoritative. Each converted catalog method builds
//! its actions from its own arguments and pushes them into the transaction's
//! [`TxnTap`]; when the SQL transaction commits, the tap applies them to its
//! [`DatasetState`] and writes one line per touched table. A differential
//! (`CayenneCatalog::tap_verify`) reads the state back from SQL and compares.
//!
//! Commits reach the tap in SQL commit order: the tap holds one lock from the
//! start of each writing transaction (or autocommit statement) until its
//! commit has been recorded.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use cayenne_metastore_actions::proto::{Action, Record};
use cayenne_metastore_actions::size::{checkpoint_size, design_record, encoded_len};
use cayenne_metastore_actions::state::{DatasetState, TableState};
use parking_lot::Mutex;
use prost::Message as _;

mod counting;
pub mod reader;
mod shadow;
pub use shadow::ShadowReport;

use crate::catalog::CatalogResult;
use crate::metastore::{ExecuteParams, MetastoreTransaction, MetastoreValue, QueryRowParams};

/// The actions one transaction has pushed so far.
#[derive(Default)]
pub struct TxnTap {
    pushed: Mutex<Vec<(&'static str, Vec<Action>)>>,
    mutating_statements: Mutex<Vec<String>>,
}

impl TxnTap {
    /// Records the actions of one converted operation.
    pub fn push(&self, op: &'static str, actions: Vec<Action>) {
        self.pushed.lock().push((op, actions));
    }

    fn note_sql(&self, sql: &str) {
        let trimmed = sql.trim_start();
        let upper: String = trimmed
            .chars()
            .take(6)
            .collect::<String>()
            .to_ascii_uppercase();
        if upper.starts_with("INSERT") || upper.starts_with("UPDATE") || upper.starts_with("DELETE")
        {
            let prefix: String = trimmed
                .split_whitespace()
                .take(4)
                .collect::<Vec<_>>()
                .join(" ");
            self.mutating_statements.lock().push(prefix);
        }
    }
}

/// Counters and failures a test reads back.
#[derive(Debug, Default, Clone)]
pub struct TapReport {
    /// Committed transactions that pushed actions.
    pub commits: u64,
    /// Table comparisons with SQL run so far.
    pub verified_tables: u64,
    /// Every difference the comparisons found, one line each.
    pub differences: Vec<String>,
    /// Records whose guards failed in `apply`.
    pub apply_errors: Vec<String>,
    /// Transactions that ran mutating SQL but pushed no actions, by statement.
    pub unconverted: BTreeMap<String, u64>,
    /// Actions per operation.
    pub ops: BTreeMap<String, u64>,
}

struct TapInner {
    state: DatasetState,
    report: TapReport,
    last_commit: BTreeMap<String, Instant>,
    pending_verify: BTreeSet<String>,
    out: Option<std::io::BufWriter<std::fs::File>>,
    seeded: bool,
}

/// The tap one catalog writes through.
pub struct MetastoreTap {
    serial: Arc<tokio::sync::Mutex<()>>,
    inner: Mutex<TapInner>,
    verify_each_commit: bool,
    started: Instant,
    break_apply: Option<&'static str>,
    panic_on_divergence: bool,
    shadow: Option<shadow::Shadow>,
    shadow_out: Option<PathBuf>,
    shadow_drop: Option<&'static str>,
}

impl std::fmt::Debug for MetastoreTap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreTap")
            .field("verify_each_commit", &self.verify_each_commit)
            .finish_non_exhaustive()
    }
}

impl MetastoreTap {
    /// A tap that writes one JSON line per committed table change to `out`
    /// (if any), and, with `verify_each_commit`, compares its state with SQL
    /// after every commit.
    ///
    /// # Errors
    ///
    /// Returns an error if `out` cannot be created.
    pub fn new(out: Option<PathBuf>, verify_each_commit: bool) -> std::io::Result<Self> {
        let shadow_out = out.as_ref().map(|p| {
            let mut name = p.as_os_str().to_os_string();
            name.push(".shadow.jsonl");
            PathBuf::from(name)
        });
        let out = out
            .map(|path| std::fs::File::create(path).map(std::io::BufWriter::new))
            .transpose()?;
        Ok(Self {
            serial: Arc::new(tokio::sync::Mutex::new(())),
            inner: Mutex::new(TapInner {
                state: DatasetState::default(),
                report: TapReport::default(),
                last_commit: BTreeMap::new(),
                pending_verify: BTreeSet::new(),
                out,
                seeded: false,
            }),
            verify_each_commit,
            started: Instant::now(),
            break_apply: None,
            panic_on_divergence: false,
            shadow: None,
            shadow_out,
            shadow_drop: None,
        })
    }

    /// Attach an asynchronous, best-effort per-table head-log shadow.
    #[must_use]
    pub fn with_shadow(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: object_store::path::Path,
        config: object_store_occ::headlog::HeadLogConfig,
    ) -> Self {
        self.shadow = Some(shadow::Shadow::start(
            store,
            prefix,
            config,
            self.shadow_out.clone(),
            self.shadow_drop,
        ));
        self
    }

    /// Test-only negative control; set before attaching the shadow.
    #[doc(hidden)]
    #[must_use]
    pub fn breaking_shadow_of(mut self, kind: &'static str) -> Self {
        self.shadow_drop = Some(kind);
        self
    }

    pub(crate) fn blob(&self, bytes: &[u8]) {
        if let Some(shadow) = &self.shadow {
            shadow.blob(bytes);
        }
    }

    /// Wait for the shadow to commit everything queued, then list its tables.
    pub(crate) async fn shadow_tables(&self) -> (ShadowReport, Vec<String>) {
        match &self.shadow {
            Some(s) => s.verify().await,
            None => (ShadowReport::default(), Vec::new()),
        }
    }

    /// One shadowed table rebuilt from storage alone.
    pub(crate) async fn shadow_replay(
        &self,
        table: &str,
    ) -> std::result::Result<DatasetState, String> {
        match &self.shadow {
            Some(s) => s.replay(table).await,
            None => Err("no shadow is configured".into()),
        }
    }

    /// Panic on the first difference or apply error, so an existing test
    /// suite run with the tap attached fails at the commit that diverged.
    #[must_use]
    pub fn panicking_on_divergence(mut self) -> Self {
        self.panic_on_divergence = true;
        self
    }

    /// Whether the tap still needs its baseline from SQL.
    pub(crate) fn needs_seed(&self) -> bool {
        !self.inner.lock().seeded
    }

    /// Starts the tap from the state SQL already holds (a reopened metastore).
    pub(crate) fn seed(&self, tables: Vec<(String, TableState)>) {
        let mut inner = self.inner.lock();
        inner.seeded = true;
        for (table_id, table) in tables {
            if let Some(shadow) = &self.shadow {
                shadow.enqueue(shadow::Payload {
                    table: table_id.clone(),
                    bytes: cayenne_metastore_actions::state_as_record(&table, &table_id)
                        .encode_to_vec()
                        .into(),
                    enqueued: Instant::now(),
                    seed: true,
                });
            }
            inner.state.tables.insert(table_id, table);
        }
    }

    /// Negative control: drop every action named `action` before applying,
    /// so the differential must go red.
    #[must_use]
    pub fn breaking_apply_of(mut self, action: &'static str) -> Self {
        self.break_apply = Some(action);
        self
    }

    pub(crate) async fn enter(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.serial).lock_owned().await
    }

    /// The counters and failures so far.
    #[must_use]
    pub fn report(&self) -> TapReport {
        self.inner.lock().report.clone()
    }

    /// The tap's state of every table.
    #[must_use]
    pub fn state(&self) -> DatasetState {
        self.inner.lock().state.clone()
    }

    pub(crate) fn take_pending_verify(&self) -> BTreeSet<String> {
        std::mem::take(&mut self.inner.lock().pending_verify)
    }

    pub(crate) fn state_of(&self, table_id: &str) -> (bool, Option<TableState>) {
        let inner = self.inner.lock();
        (
            inner.state.unknown.contains(table_id),
            inner.state.tables.get(table_id).cloned(),
        )
    }

    pub(crate) fn record_verification(&self, table: &str, differences: Vec<String>) {
        let mut inner = self.inner.lock();
        inner.report.verified_tables += 1;
        assert!(
            !self.panic_on_divergence || differences.is_empty(),
            "metastore tap: table {table} differs from SQL: {differences:#?}\nentered from:\n{}",
            std::backtrace::Backtrace::force_capture()
        );
        for difference in differences {
            inner
                .report
                .differences
                .push(format!("{table}: {difference}"));
        }
    }

    /// Applies one committed transaction's actions and writes its lines.
    pub(crate) fn commit(&self, pushed: Vec<(&'static str, Vec<Action>)>, statements: Vec<String>) {
        let mut inner = self.inner.lock();
        let inner = &mut *inner;
        if pushed.is_empty() {
            for statement in statements {
                *inner.report.unconverted.entry(statement).or_default() += 1;
            }
            return;
        }
        inner.report.commits += 1;
        let commit = inner.report.commits;
        let ops: Vec<&'static str> = pushed.iter().map(|(op, _)| *op).collect();
        for op in &ops {
            *inner.report.ops.entry((*op).to_string()).or_default() += 1;
        }
        for (_, actions) in &pushed {
            for action in actions {
                if let Some(cayenne_metastore_actions::Kind::Unconverted(a)) = &action.kind {
                    *inner
                        .report
                        .unconverted
                        .entry(format!("{}: {}", a.method, a.table_id))
                        .or_default() += 1;
                }
            }
        }
        let touched: BTreeSet<String> = pushed
            .iter()
            .flat_map(|(_, actions)| actions)
            .filter_map(|a| a.table_id().map(str::to_string))
            .collect();
        let actions: Vec<Action> = pushed
            .into_iter()
            .flat_map(|(_, actions)| actions)
            .filter(|action| Some(action.name()) != self.break_apply)
            .collect();
        let record = Record { actions };
        let record_bytes = encoded_len(&record);
        let before: BTreeMap<String, Option<TableState>> = touched
            .iter()
            .map(|t| (t.clone(), inner.state.tables.get(t).cloned()))
            .collect();
        if let Err(err) = inner.state.apply(&record) {
            assert!(
                !self.panic_on_divergence,
                "metastore tap: commit {commit} ({ops:?}) failed to apply: {err}"
            );
            inner
                .report
                .apply_errors
                .push(format!("commit {commit} ({ops:?}): {err}"));
            return;
        }
        let now = Instant::now();
        let t_ms = now.duration_since(self.started).as_secs_f64() * 1e3;
        for table_id in &touched {
            if self.verify_each_commit {
                inner.pending_verify.insert(table_id.clone());
            }
            let after = inner.state.tables.get(table_id);
            let design = design_record(table_id, before[table_id].as_ref(), after);
            let since_prev_ms = inner
                .last_commit
                .insert(table_id.clone(), now)
                .map(|prev| now.duration_since(prev).as_secs_f64() * 1e3);
            let table_actions: Vec<&Action> = record
                .actions
                .iter()
                .filter(|a| a.table_id() == Some(table_id.as_str()))
                .collect();
            let names: Vec<&str> = table_actions.iter().map(|a| a.name()).collect();
            let table_record = Record {
                actions: table_actions.into_iter().cloned().collect(),
            };
            if let Some(shadow) = &self.shadow {
                shadow.enqueue(shadow::Payload {
                    table: table_id.clone(),
                    bytes: table_record.encode_to_vec().into(),
                    enqueued: Instant::now(),
                    seed: false,
                });
            }
            let size = after.map(checkpoint_size).unwrap_or_default();
            let (name, live_files, dvs) = after.map_or_else(
                || {
                    (
                        (*before[table_id].as_ref().map_or("", |t| t.name.as_str())).to_string(),
                        0,
                        0,
                    )
                },
                |t| (t.name.clone(), t.live_files().len(), t.delete_files.len()),
            );
            if let Some(out) = inner.out.as_mut() {
                let line = serde_json::json!({
                    "commit": commit,
                    "t_ms": t_ms,
                    "table": name,
                    "table_id": table_id,
                    "ops": ops,
                    "actions": names,
                    "record_bytes": encoded_len(&table_record),
                    "txn_record_bytes": record_bytes,
                    "design_bytes": encoded_len(&design),
                    "design_actions": design.actions.iter().map(Action::name).collect::<Vec<_>>(),
                    "since_prev_ms": since_prev_ms,
                    "live_files": live_files,
                    "delete_files": dvs,
                    "ckpt_bytes": size.without_pk_index,
                    "ckpt_pk_bytes": size.pk_index,
                    "ckpt_live_file_bytes": size.live_files,
                    "ckpt_dv_bytes": size.delete_files,
                    "ckpt_inline_bytes": size.inline_bytes,
                });
                let _ = writeln!(out, "{line}");
            }
        }
    }

    pub(crate) fn flush(&self) {
        if let Some(out) = self.inner.lock().out.as_mut() {
            let _ = out.flush();
        }
    }
}

/// A metastore transaction whose converted operations push actions into the
/// tap, recorded when it commits.
pub(crate) struct TappedTxn {
    inner: Box<dyn MetastoreTransaction>,
    tap: Arc<MetastoreTap>,
    txn: TxnTap,
    _serial: tokio::sync::OwnedMutexGuard<()>,
}

impl TappedTxn {
    pub(crate) fn new(
        inner: Box<dyn MetastoreTransaction>,
        tap: Arc<MetastoreTap>,
        serial: tokio::sync::OwnedMutexGuard<()>,
    ) -> Self {
        Self {
            inner,
            tap,
            txn: TxnTap::default(),
            _serial: serial,
        }
    }
}

#[async_trait]
impl MetastoreTransaction for TappedTxn {
    async fn execute(&self, params: ExecuteParams<'_>) -> CatalogResult<()> {
        self.txn.note_sql(params.sql);
        self.inner.execute(params).await
    }

    async fn execute_many(&self, sql: &str, params: Vec<Vec<MetastoreValue>>) -> CatalogResult<()> {
        self.txn.note_sql(sql);
        self.inner.execute_many(sql, params).await
    }

    async fn query_row_values(
        &self,
        params: QueryRowParams<'_>,
    ) -> CatalogResult<Vec<MetastoreValue>> {
        self.txn.note_sql(params.sql);
        self.inner.query_row_values(params).await
    }

    async fn execute_batch(&self, sql: &str) -> CatalogResult<()> {
        for statement in sql.split(';') {
            self.txn.note_sql(statement);
        }
        self.inner.execute_batch(sql).await
    }

    async fn commit(self: Box<Self>) -> CatalogResult<()> {
        let Self {
            inner,
            tap,
            txn,
            _serial,
        } = *self;
        let mut pushed = txn.pushed.into_inner();
        capture_inline_timestamps(&mut pushed, inner.as_ref()).await?;
        inner.commit().await?;
        tap.commit(pushed, txn.mutating_statements.into_inner());
        drop(_serial);
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> CatalogResult<()> {
        self.inner.rollback().await
    }

    fn tap(&self) -> Option<&TxnTap> {
        Some(&self.txn)
    }
}

/// Pushes the actions `build` returns into `txn`'s tap, if it has one; builds
/// nothing otherwise.
pub(crate) fn push(
    txn: &dyn MetastoreTransaction,
    op: &'static str,
    build: impl FnOnce() -> Vec<Action>,
) {
    if let Some(tap) = txn.tap() {
        tap.push(op, build());
    }
}

pub(crate) mod actions;

#[cfg(cayenne_metastore_tap)]
impl MetastoreTap {
    pub(crate) fn shadow_from_dev_file(mut self) -> Self {
        use object_store::ObjectStore;
        use object_store::aws::AmazonS3Builder;
        use object_store::path::Path;
        use object_store_occ::sim::latency::{Delay, LatencyProfile, LatencyStore};
        use std::time::Duration;
        // Only `--cfg cayenne_metastore_tap` developer builds read this, so a
        // release build gains no setting. The variable holds the shadow
        // configuration as JSON, or the path of a JSON file; when it is unset,
        // `<temp dir>/cayenne-metastore-tap/shadow.json` is read if present.
        let configured = std::env::var("SPICE_CAYENNE_METASTORE_TAP_SHADOW").ok();
        let data = match configured {
            Some(value) if value.trim_start().starts_with('{') => Ok(value.into_bytes()),
            Some(path) => std::fs::read(path),
            None => std::fs::read(std::env::temp_dir().join("cayenne-metastore-tap/shadow.json")),
        };
        let data = match data {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return self,
            Err(e) => {
                self.shadow = Some(shadow::Shadow::failed(e));
                return self;
            }
        };
        let configure = || -> Result<_, Box<dyn std::error::Error>> {
            let v: serde_json::Value = serde_json::from_slice(&data)?;
            let text = |key: &str| {
                v[key]
                    .as_str()
                    .ok_or_else(|| format!("shadow configuration missing {key}"))
            };
            let store: Arc<dyn ObjectStore> = match text("kind")? {
                "local" => Arc::new(object_store_occ::LocalConditionalPut::new_durable(text(
                    "path",
                )?)?),
                "s3" => Arc::new(
                    AmazonS3Builder::new()
                        .with_endpoint(text("endpoint")?)
                        .with_bucket_name(text("bucket")?)
                        .with_access_key_id(text("access_key")?)
                        .with_secret_access_key(text("secret")?)
                        .with_region("us-east-1")
                        .with_allow_http(true)
                        .build()?,
                ),
                kind => return Err(format!("unknown shadow kind {kind}").into()),
            };
            let median = v["latency_median_ms"].as_u64().unwrap_or(0);
            let p99 = v["latency_p99_ms"].as_u64().unwrap_or(median);
            let store: Arc<dyn ObjectStore> = if median == 0 {
                store
            } else {
                Arc::new(LatencyStore::new(
                    store,
                    LatencyProfile::uniform(Delay::new(
                        Duration::from_millis(median),
                        Duration::from_millis(p99),
                    )),
                    0,
                ))
            };
            Ok((store, Path::parse(v["prefix"].as_str().unwrap_or(""))?))
        };
        match configure() {
            Ok((store, prefix)) => self.with_shadow(
                store,
                prefix,
                // Records above max_record_bytes / 2 spill to objects; the head keeps
                // headroom over B + R for its own metadata and batch index.
                object_store_occ::headlog::HeadLogConfig {
                    head_budget_bytes: 256 * 1024,
                    batch_budget_bytes: 64 * 1024,
                    batch_max_records: 64,
                    max_record_bytes: 16 * 1024,
                    max_listed_batches: 1024,
                    // The shadow spills its own payloads above R/2 first.
                    spill_above_bytes: 16 * 1024,
                },
            ),
            Err(e) => {
                self.shadow = Some(shadow::Shadow::failed(e));
                self
            }
        }
    }
}

/// Read SQL-generated timestamps while the committing transaction still owns its rows.
pub(crate) async fn capture_inline_timestamps(
    pushed: &mut [(&'static str, Vec<Action>)],
    txn: &dyn MetastoreTransaction,
) -> CatalogResult<()> {
    for (_, actions) in pushed {
        for action in actions {
            let (table, id, created_at, sql) = match action.kind.as_mut() {
                Some(cayenne_metastore_actions::Kind::AddInlinedData(a)) => (
                    &a.table_id,
                    &a.id,
                    &mut a.created_at,
                    "SELECT COALESCE((SELECT created_at FROM cayenne_inlined_data WHERE table_id = ?1 AND inlined_id = ?2), '')",
                ),
                Some(cayenne_metastore_actions::Kind::AddInlinedDelete(a)) => (
                    &a.table_id,
                    &a.id,
                    &mut a.created_at,
                    "SELECT COALESCE((SELECT created_at FROM cayenne_inlined_delete WHERE table_id = ?1 AND inlined_id = ?2), '')",
                ),
                _ => continue,
            };
            let values = txn
                .query_row_values(QueryRowParams {
                    sql,
                    params: vec![
                        MetastoreValue::Text(table.clone()),
                        MetastoreValue::Text(id.clone()),
                    ],
                })
                .await?;
            let Some(MetastoreValue::Text(timestamp)) = values.into_iter().next() else {
                return Err(crate::catalog::CatalogError::InvalidOperationNoSource {
                    message: "inline timestamp is not text".to_string(),
                });
            };
            *created_at = timestamp;
        }
    }
    Ok(())
}
