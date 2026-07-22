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

//! Shared binlog-dump primitives used by the multiplexed pump in
//! [`super::shared`].
//!
//! `MySQL`'s `COM_BINLOG_DUMP` is server-wide with no server-side table filter,
//! so every `refresh_mode: changes` dataset on a connection is coalesced onto a
//! single dump ([`super::shared`]); this module holds the pieces that pump needs
//! and does not own a stream loop of its own:
//!
//!   - [`open_binlog_stream`] to start a dump at a resume position (requesting
//!     server heartbeats so a quiet stream still advances),
//!   - [`buffer_rows_event`] to decode a rows event into a [`TransactionBuffer`],
//!   - [`readiness_heartbeat`] / [`record_watermark`] / [`commit_ts_ms`] for
//!     lag-based readiness and freshness metrics,
//!   - [`adopt_current_layout`] + [`compute_pk_source_indexes`] for compatible
//!     mid-stream `ALTER` adoption,
//!   - [`classify_query`] / [`classify_statement`] for the transaction and DDL
//!     boundaries in the event stream.
//!
//! # Ack model
//!
//! Postgres replication acks by advancing a *server-side* slot cursor; `MySQL`
//! has no such cursor, so the ack is Spice's own persisted [`BinlogPosition`],
//! held per member and folded into the shared dump's resume position (the
//! minimum across members) by [`super::shared`]. A crash replays at most
//! `checkpoint_interval` of already-applied changes, which the accelerator's PK
//! upsert absorbs idempotently (at-least-once).

use std::time::{Duration, SystemTime};

use arrow::datatypes::SchemaRef;
use mysql_async::binlog::events::{RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Value};

use super::config::{BinlogPosition, ReplicationParams};
use super::metrics::MetricsCollector;
use super::rows::{TransactionBuffer, normalize_binlog_value};
use super::setup::TableLayout;
use super::{Error, Result};
use crate::cdc::{ChangeEnvelope, StreamError, build_heartbeat_envelope};

/// Binlog events start at offset 4 (after the magic header); positions below
/// that (fake rotates and heartbeats report 0) are not resumable.
pub(super) const MIN_VALID_EVENT_POS: u64 = 4;

pub(super) async fn open_binlog_stream(
    params: &ReplicationParams,
    resume: &BinlogPosition,
    dataset_name: &str,
) -> std::result::Result<BinlogStream, mysql_async::Error> {
    let mut conn = Conn::new(params.opts.clone()).await?;

    // Ask the source to send heartbeat events while idle so the stream can
    // detect dead connections and advance its checkpoint. Half the
    // checkpoint interval (min 500ms) keeps idle persists within ~1.5×
    // the interval. The session variable is in nanoseconds; MySQL 8.4
    // renamed the replica-facing vocabulary, so set both spellings (unknown
    // user variables are inert).
    let heartbeat_nanos = (params.checkpoint_interval / 2)
        .max(Duration::from_millis(500))
        .as_nanos()
        .min(u128::from(u64::MAX));
    // Two separate statements: if a server rejects one spelling, the other
    // must still take effect (a combined statement fails atomically).
    for var in ["master_heartbeat_period", "source_heartbeat_period"] {
        if let Err(e) = mysql_async::prelude::Queryable::query_drop(
            &mut conn,
            format!("SET @{var} = {heartbeat_nanos}"),
        )
        .await
        {
            tracing::debug!(dataset = %dataset_name, error = %e, "failed to set @{var}");
        }
    }

    let pos_u32 = u32::try_from(resume.pos).unwrap_or(u32::MAX);
    conn.get_binlog_stream(
        BinlogStreamRequest::new(params.server_id)
            .with_filename(resume.file.as_bytes())
            .with_pos(u64::from(pos_u32)),
    )
    .await
}

/// Decode a rows event for the subscribed table into the transaction buffer.
pub(super) fn buffer_rows_event(
    rows_data: &RowsEventData<'_>,
    tme: &TableMapEvent<'_>,
    layout: &TableLayout,
    pk_source_indexes: &[usize],
    buffer: &mut TransactionBuffer,
    metrics: &MetricsCollector,
) -> Result<()> {
    #[derive(Clone, Copy)]
    enum RowOp {
        Insert,
        Update,
        Delete,
    }
    let op = match rows_data {
        RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => RowOp::Insert,
        RowsEventData::UpdateRowsEvent(_) | RowsEventData::UpdateRowsEventV1(_) => RowOp::Update,
        RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => RowOp::Delete,
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(Error::Decode {
                message: "partial-JSON row images are not supported. Set \
                          `binlog_row_value_options = ''` on the source server."
                    .to_string(),
            });
        }
    };

    // `RowsEventData::rows` unifies the V1/V2 event variants.
    for row in rows_data.rows(tme) {
        let (before, after) = row.map_err(row_io_error)?;
        match op {
            RowOp::Insert => {
                let after = required_image(after, "write", "after")?;
                buffer.push_insert(binlog_row_to_values(after, layout)?);
                metrics.inc_insert();
            }
            RowOp::Update => {
                let before = required_image(before, "update", "before")?;
                let after = required_image(after, "update", "after")?;
                buffer.push_update(
                    pk_source_indexes,
                    binlog_row_to_values(before, layout)?,
                    binlog_row_to_values(after, layout)?,
                );
                metrics.inc_update();
            }
            RowOp::Delete => {
                let before = required_image(before, "delete", "before")?;
                buffer.push_delete(binlog_row_to_values(before, layout)?);
                metrics.inc_delete();
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a function pointer in map_err; taking by reference would require a closure at every call site"
)]
fn row_io_error(e: std::io::Error) -> Error {
    Error::Decode {
        message: format!("row image parse: {e}"),
    }
}

fn required_image(image: Option<BinlogRow>, op: &str, side: &str) -> Result<BinlogRow> {
    image.ok_or_else(|| Error::Decode {
        message: format!("{op} event is missing its {side} row image"),
    })
}

/// Convert a full binlog row image into per-source-column [`Value`]s.
fn binlog_row_to_values(mut row: BinlogRow, layout: &TableLayout) -> Result<Vec<Value>> {
    if row.len() != layout.columns.len() {
        return Err(Error::Decode {
            message: format!(
                "row image has {} columns but the validated layout has {} — the source \
                 table was altered. Restart the dataset to re-validate the schema.",
                row.len(),
                layout.columns.len()
            ),
        });
    }
    (0..row.len())
        .map(|idx| {
            let value = row.take(idx).ok_or_else(|| Error::Decode {
                message: format!(
                    "column #{idx} (`{}`) is absent from the row image. Spice requires \
                     `binlog_row_image = FULL` — a writer session overrode it.",
                    layout.columns[idx].name
                ),
            })?;
            normalize_binlog_value(&layout.columns[idx], value)
        })
        .collect()
}

/// source-attested clock (`source_now_ms`), flagged Ready when that clock is
/// within `ready_lag` of now. Emitted only when the stream has caught up to the
/// source head, so it never marks a still-behind dataset Ready.
pub(super) fn readiness_heartbeat(
    schema: &SchemaRef,
    source_now_ms: i64,
    ready_lag: Duration,
    dataset_name: &str,
) -> std::result::Result<ChangeEnvelope, StreamError> {
    let is_ready = crate::cdc::source_commit_within_ready_lag(Some(source_now_ms), ready_lag);
    // Log the idle heartbeat so lag-based readiness can be verified from the logs
    // (target spice_cdc::heartbeat). Covers both call sites of this helper.
    let lag_ms = crate::cdc::replication_lag_ms(Some(source_now_ms));
    tracing::debug!(
        target: "spice_cdc::heartbeat",
        connector = "mysql",
        dataset = %dataset_name,
        source_commit_ts_ms = source_now_ms,
        is_dataset_ready = is_ready,
        lag_ms = ?lag_ms,
        "CDC idle heartbeat emitted"
    );
    build_heartbeat_envelope(schema, Some(source_now_ms), is_ready).map_err(|e| {
        StreamError::External(format!(
            "heartbeat envelope build failed for {dataset_name}: {e}"
        ))
    })
}

pub(super) fn record_watermark(metrics: &MetricsCollector, event_timestamp: u32) {
    if event_timestamp > 0 {
        metrics.record_commit_watermark(
            SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(event_timestamp)),
        );
    }
}

pub(super) fn commit_ts_ms(event_timestamp: u32) -> Option<i64> {
    (event_timestamp > 0).then(|| i64::from(event_timestamp) * 1000)
}

/// Source row-image indexes of the declared primary keys, for PK-change
/// detection on UPDATE. `column_map` is dataset-field-indexed, so PK names
/// map through the dataset schema.
pub(super) fn compute_pk_source_indexes(
    schema: &SchemaRef,
    primary_keys: &[String],
    column_map: &[usize],
) -> Vec<usize> {
    primary_keys
        .iter()
        .filter_map(|pk| {
            schema
                .fields()
                .iter()
                .position(|f| f.name() == pk)
                .and_then(|field_idx| column_map.get(field_idx).copied())
        })
        .collect()
}

/// The re-validated positional mappings adopted after a source DDL.
#[derive(Clone)]
pub(super) struct AdoptedLayout {
    pub(super) layout: TableLayout,
    pub(super) column_map: Vec<usize>,
    pub(super) pk_source_indexes: Vec<usize>,
}

/// Re-fetch the source table's layout and reconcile it against the dataset
/// schema. Succeeds when every dataset column still exists on the source —
/// values keep decoding by name at their new positions. Source columns the
/// dataset doesn't declare (including ones a DDL just added) are not
/// replicated; newly-appeared ones are warned about, mirroring the Postgres
/// connector's block-mode behavior for compatible relation changes.
pub(super) async fn adopt_current_layout(
    params: &ReplicationParams,
    database: &str,
    table: &str,
    schema: &SchemaRef,
    old_layout: &TableLayout,
    primary_keys: &[String],
    dataset_name: &str,
) -> Result<AdoptedLayout> {
    let mut conn = super::setup::connect(params).await?;
    let layout = super::setup::fetch_table_layout(&mut conn, database, table).await?;
    if let Err(e) = conn.disconnect().await {
        tracing::debug!(dataset = %dataset_name, error = %e, "layout refetch disconnect");
    }

    let column_map = layout.column_map(schema, database, table)?;
    let pk_source_indexes = compute_pk_source_indexes(schema, primary_keys, &column_map);

    let old_names: std::collections::HashSet<&str> =
        old_layout.columns.iter().map(|c| c.name.as_str()).collect();
    let added: Vec<&str> = layout
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .filter(|name| !old_names.contains(name))
        .collect();
    if added.is_empty() {
        tracing::info!(
            dataset = %dataset_name,
            columns = layout.columns.len(),
            "adopted the source table's new layout; all dataset columns still map"
        );
    } else {
        tracing::warn!(
            dataset = %dataset_name,
            columns = ?added,
            "source table {database}.{table} gained columns whose values are not \
             replicated. Add them to the dataset schema (and restart) to capture them"
        );
    }

    Ok(AdoptedLayout {
        layout,
        column_map,
        pk_source_indexes,
    })
}
pub(super) fn purged_position_error(resume: &BinlogPosition, dataset_name: &str) -> StreamError {
    StreamError::External(format!(
        "mysql binlog for {dataset_name}: the source no longer has binlog position {resume} \
         (binary logs were purged). Restart the dataset with \
         `mysql_replication_invalid_checkpoint_behavior: restart` to drop the saved position \
         and re-snapshot the table, or increase `binlog_expire_logs_seconds` on the source."
    ))
}

/// See `postgres_replication::client` for the WARN→DEBUG demotion rationale:
/// the first failure of an outage is loud, the rest keep log volume sublinear.
pub(super) fn log_transient_reconnect(attempt: u32, dataset: &str, error: &str, retry_in_ms: u128) {
    if attempt <= 1 {
        tracing::warn!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "binlog connection lost; reconnecting"
        );
    } else {
        tracing::debug!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "binlog connection still down; reconnecting"
        );
    }
}

pub(super) enum QueryKind {
    Begin,
    Commit,
    /// `XA START|END|PREPARE|COMMIT|ROLLBACK ...` — two-phase transactions
    /// use a different commit protocol this stream does not implement.
    Xa,
    Statement,
}

pub(super) fn classify_query(statement: &str) -> QueryKind {
    let trimmed = statement.trim();
    if trimmed.eq_ignore_ascii_case("BEGIN") {
        QueryKind::Begin
    } else if trimmed.eq_ignore_ascii_case("COMMIT") {
        QueryKind::Commit
    } else if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("xa "))
    {
        QueryKind::Xa
    } else {
        QueryKind::Statement
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StatementKind {
    Truncate,
    SchemaChange(&'static str),
}

/// Detect statements that affect the subscribed table: TRUNCATE (applied as
/// a change) and schema-changing DDL (fatal — see the stream loop).
///
/// Deliberately a bounded hand-rolled tokenizer rather than sqlparser: the
/// classifier must be total over arbitrary server-wide statements, and a
/// full-parser failure on an exotic-but-relevant `ALTER` variant would
/// silently miss a schema change — worse than this prefix scan's confined
/// vocabulary.
///
/// `default_db` is the session database recorded in the query event, used
/// for unqualified table references. Identifier comparison is
/// case-insensitive (matching `MySQL`'s common `lower_case_table_names`
/// deployments; a false positive requires two tables differing only in case).
pub(super) fn classify_statement(
    statement: &str,
    default_db: &str,
    database: &str,
    table: &str,
) -> Option<StatementKind> {
    let tokens = tokenize_statement(statement);
    let mut idx = 0;
    let word = |i: usize| tokens.get(i).map(Token::normalized);

    let matches_target = |db: Option<&str>, name: &str| {
        let effective_db = db.unwrap_or(default_db);
        effective_db.eq_ignore_ascii_case(database) && name.eq_ignore_ascii_case(table)
    };

    match word(idx).as_deref() {
        Some("truncate") => {
            idx += 1;
            if word(idx).as_deref() == Some("table") {
                idx += 1;
            }
            let (db, name) = parse_table_ref(&tokens, &mut idx)?;
            matches_target(db.as_deref(), &name).then_some(StatementKind::Truncate)
        }
        Some("alter") => {
            idx += 1;
            if word(idx).as_deref() != Some("table") {
                return None;
            }
            idx += 1;
            let (db, name) = parse_table_ref(&tokens, &mut idx)?;
            matches_target(db.as_deref(), &name)
                .then_some(StatementKind::SchemaChange("ALTER TABLE"))
        }
        Some("drop") => {
            idx += 1;
            match word(idx).as_deref() {
                Some("table") => {
                    idx += 1;
                    if word(idx).as_deref() == Some("if")
                        && word(idx + 1).as_deref() == Some("exists")
                    {
                        idx += 2;
                    }
                    loop {
                        let (db, name) = parse_table_ref(&tokens, &mut idx)?;
                        if matches_target(db.as_deref(), &name) {
                            return Some(StatementKind::SchemaChange("DROP TABLE"));
                        }
                        if tokens.get(idx).map(Token::normalized).as_deref() == Some(",") {
                            idx += 1;
                        } else {
                            return None;
                        }
                    }
                }
                Some("database" | "schema") => {
                    idx += 1;
                    if word(idx).as_deref() == Some("if")
                        && word(idx + 1).as_deref() == Some("exists")
                    {
                        idx += 2;
                    }
                    let name = tokens.get(idx)?.identifier()?;
                    name.eq_ignore_ascii_case(database)
                        .then_some(StatementKind::SchemaChange("DROP DATABASE"))
                }
                _ => None,
            }
        }
        Some("rename") => {
            idx += 1;
            if word(idx).as_deref() != Some("table") {
                return None;
            }
            idx += 1;
            loop {
                let (db, name) = parse_table_ref(&tokens, &mut idx)?;
                if matches_target(db.as_deref(), &name) {
                    return Some(StatementKind::SchemaChange("RENAME TABLE"));
                }
                if tokens.get(idx).map(Token::normalized).as_deref() != Some("to") {
                    return None;
                }
                idx += 1;
                let (to_db, to_name) = parse_table_ref(&tokens, &mut idx)?;
                if matches_target(to_db.as_deref(), &to_name) {
                    return Some(StatementKind::SchemaChange("RENAME TABLE"));
                }
                if tokens.get(idx).map(Token::normalized).as_deref() == Some(",") {
                    idx += 1;
                } else {
                    return None;
                }
            }
        }
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    /// Bare word — keywords and unquoted identifiers.
    Word(String),
    /// Backtick-quoted identifier (already unescaped).
    Quoted(String),
    /// Single punctuation character (`.`, `,`, …).
    Punct(char),
}

impl Token {
    fn normalized(&self) -> String {
        match self {
            Token::Word(w) => w.to_ascii_lowercase(),
            Token::Quoted(q) => q.clone(),
            Token::Punct(c) => c.to_string(),
        }
    }

    fn identifier(&self) -> Option<String> {
        match self {
            Token::Word(w) => Some(w.clone()),
            Token::Quoted(q) => Some(q.clone()),
            Token::Punct(_) => None,
        }
    }
}

fn tokenize_statement(statement: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = statement.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '/' {
            // Possible /* ... */ comment (MySQL prepends version comments).
            chars.next();
            if chars.peek() == Some(&'*') {
                chars.next();
                let mut prev = ' ';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            } else {
                tokens.push(Token::Punct('/'));
            }
        } else if c == '`' {
            chars.next();
            let mut ident = String::new();
            while let Some(c) = chars.next() {
                if c == '`' {
                    if chars.peek() == Some(&'`') {
                        chars.next();
                        ident.push('`');
                    } else {
                        break;
                    }
                } else {
                    ident.push(c);
                }
            }
            tokens.push(Token::Quoted(ident));
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let mut word = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_alphanumeric() || c == '_' || c == '$' {
                    word.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            tokens.push(Token::Word(word));
        } else {
            tokens.push(Token::Punct(c));
            chars.next();
        }
    }
    tokens
}

/// Parse `ident` or `ident.ident` starting at `*idx`, advancing it past the
/// reference. Returns `(database, table)`.
fn parse_table_ref(tokens: &[Token], idx: &mut usize) -> Option<(Option<String>, String)> {
    let first = tokens.get(*idx)?.identifier()?;
    *idx += 1;
    if tokens.get(*idx) == Some(&Token::Punct('.')) {
        *idx += 1;
        let second = tokens.get(*idx)?.identifier()?;
        *idx += 1;
        Some((Some(first), second))
    } else {
        Some((None, first))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn pk_source_indexes_follow_a_remapped_layout() {
        use arrow::datatypes::{DataType, Field, Schema};
        // Dataset (name, id); PK is `id`.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let pks = vec!["id".to_string()];

        // Original source layout: (id, name) → id at source index 0.
        assert_eq!(compute_pk_source_indexes(&schema, &pks, &[1, 0]), vec![0]);
        // Post-ALTER layout with a column inserted first: (note, id, name)
        // → id shifted to source index 1. Adoption must re-route the PK.
        assert_eq!(compute_pk_source_indexes(&schema, &pks, &[2, 1]), vec![1]);
    }

    #[test]
    fn classifies_begin_and_commit() {
        assert!(matches!(classify_query(" BEGIN "), QueryKind::Begin));
        assert!(matches!(classify_query("commit"), QueryKind::Commit));
        assert!(matches!(
            classify_query("TRUNCATE TABLE t"),
            QueryKind::Statement
        ));
    }

    #[test]
    fn truncate_statement_matches_with_and_without_qualifier() {
        for stmt in [
            "TRUNCATE TABLE orders",
            "truncate orders",
            "TRUNCATE TABLE `orders`",
            "TRUNCATE TABLE mydb.orders",
            "TRUNCATE `mydb`.`orders`",
            "/* comment */ TRUNCATE TABLE ORDERS",
        ] {
            assert_eq!(
                classify_statement(stmt, "mydb", "mydb", "orders"),
                Some(StatementKind::Truncate),
                "statement: {stmt}"
            );
        }
    }

    #[test]
    fn truncate_of_other_table_or_db_is_ignored() {
        assert_eq!(
            classify_statement("TRUNCATE TABLE customers", "mydb", "mydb", "orders"),
            None
        );
        assert_eq!(
            classify_statement("TRUNCATE otherdb.orders", "mydb", "mydb", "orders"),
            None
        );
        // Unqualified reference resolved against a different session db.
        assert_eq!(
            classify_statement("TRUNCATE orders", "otherdb", "mydb", "orders"),
            None
        );
    }

    #[test]
    fn ddl_statements_are_flagged_as_schema_changes() {
        assert_eq!(
            classify_statement(
                "ALTER TABLE orders ADD COLUMN note TEXT",
                "mydb",
                "mydb",
                "orders"
            ),
            Some(StatementKind::SchemaChange("ALTER TABLE"))
        );
        assert_eq!(
            classify_statement("DROP TABLE IF EXISTS a, mydb.orders", "x", "mydb", "orders"),
            Some(StatementKind::SchemaChange("DROP TABLE"))
        );
        assert_eq!(
            classify_statement(
                "RENAME TABLE orders TO orders_old, orders_new TO orders2",
                "mydb",
                "mydb",
                "orders"
            ),
            Some(StatementKind::SchemaChange("RENAME TABLE"))
        );
        assert_eq!(
            classify_statement("DROP DATABASE mydb", "", "mydb", "orders"),
            Some(StatementKind::SchemaChange("DROP DATABASE"))
        );
    }

    #[test]
    fn unrelated_ddl_and_dml_are_ignored() {
        for stmt in [
            "ALTER TABLE customers ADD COLUMN x INT",
            "DROP TABLE customers",
            "RENAME TABLE a TO b",
            "DROP DATABASE otherdb",
            "CREATE TABLE orders_new (id INT)",
            "INSERT INTO orders VALUES (1)",
            "OPTIMIZE TABLE orders",
        ] {
            assert_eq!(
                classify_statement(stmt, "mydb", "mydb", "orders"),
                None,
                "statement: {stmt}"
            );
        }
    }

    #[test]
    fn tokenizer_handles_backticks_and_comments() {
        let tokens = tokenize_statement("/*!80000 hint*/ TRUNCATE `my``db`.`t`");
        assert_eq!(
            tokens,
            vec![
                Token::Word("TRUNCATE".to_string()),
                Token::Quoted("my`db".to_string()),
                Token::Punct('.'),
                Token::Quoted("t".to_string()),
            ]
        );
    }
}
