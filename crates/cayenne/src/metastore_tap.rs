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
        let upper: String = trimmed.chars().take(6).collect::<String>().to_ascii_uppercase();
        if upper.starts_with("INSERT") || upper.starts_with("UPDATE") || upper.starts_with("DELETE")
        {
            let prefix: String = trimmed.split_whitespace().take(4).collect::<Vec<_>>().join(" ");
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
}

/// The tap one catalog writes through.
pub struct MetastoreTap {
    serial: Arc<tokio::sync::Mutex<()>>,
    inner: Mutex<TapInner>,
    verify_each_commit: bool,
    started: Instant,
    break_apply: Option<&'static str>,
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
            }),
            verify_each_commit,
            started: Instant::now(),
            break_apply: None,
        })
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
        for difference in differences {
            inner.report.differences.push(format!("{table}: {difference}"));
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
        let actions: Vec<Action> = pushed
            .into_iter()
            .flat_map(|(_, actions)| actions)
            .filter(|action| Some(action.name()) != self.break_apply)
            .collect();
        let record = Record { actions };
        let record_bytes = encoded_len(&record);
        let touched: BTreeSet<String> = record
            .actions
            .iter()
            .filter_map(|a| a.table_id().map(str::to_string))
            .collect();
        let before: BTreeMap<String, Option<TableState>> = touched
            .iter()
            .map(|t| (t.clone(), inner.state.tables.get(t).cloned()))
            .collect();
        if let Err(err) = inner.state.apply(&record) {
            inner.report.apply_errors.push(format!("commit {commit} ({ops:?}): {err}"));
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
            let size = after.map(checkpoint_size).unwrap_or_default();
            let (name, live_files, dvs) = after.map_or_else(
                || ((*before[table_id].as_ref().map_or("", |t| t.name.as_str())).to_string(), 0, 0),
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
        inner.commit().await?;
        tap.commit(txn.pushed.into_inner(), txn.mutating_statements.into_inner());
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
pub(crate) fn push(txn: &dyn MetastoreTransaction, op: &'static str, build: impl FnOnce() -> Vec<Action>) {
    if let Some(tap) = txn.tap() {
        tap.push(op, build());
    }
}

pub(crate) mod actions;
