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

//! Pre-flight validation against the source server and table-layout
//! discovery.
//!
//! `MySQL` row-based binlog events carry column *positions*, not names (column
//! names ride along only with `binlog_row_metadata=FULL`, which is not the
//! default). The positional layout is therefore fetched from
//! `information_schema.COLUMNS` up front and used to map dataset schema
//! fields onto row-image indexes — the same approach Debezium takes.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row};

use super::config::{BinlogPosition, ReplicationParams};
use super::gtid::GtidSet;
use super::{
    BinaryLoggingDisabledSnafu, ColumnMissingSnafu, ConnectSnafu, Error, MissingPrivilegesSnafu,
    Result, SetupQuerySnafu, SourceTableNotFoundSnafu, UnsupportedBinlogFormatSnafu,
    UnsupportedBinlogRowImageSnafu, UnsupportedBinlogRowValueOptionsSnafu,
};
use snafu::prelude::*;

/// One column of the source table, in `ORDINAL_POSITION` order. The index in
/// [`TableLayout::columns`] is the binlog row-image index.
#[derive(Clone, Debug)]
pub struct SourceColumn {
    pub name: String,
    /// Raw `information_schema.COLUMNS.COLUMN_TYPE` (e.g. `int`,
    /// `varchar(255)`, `enum('a','b')`). Captured so a checkpoint can detect
    /// source-only layout changes (reorder / retype) that leave the dataset
    /// Arrow schema unchanged.
    pub column_type: String,
    /// For `ENUM` columns: the 1-based variant labels. Binlog row images
    /// carry only the variant *index*.
    pub enum_variants: Option<Arc<[String]>>,
    /// For `SET` columns: the member labels. Binlog row images carry only a
    /// member *bitmask*.
    pub set_variants: Option<Arc<[String]>>,
    /// Part of the source table's PRIMARY KEY.
    pub is_primary_key: bool,
}

/// The positional column layout of the source table.
#[derive(Clone, Debug)]
pub struct TableLayout {
    pub columns: Vec<SourceColumn>,
}

impl TableLayout {
    /// Map every dataset schema field to its source row-image index.
    ///
    /// Every dataset column must exist on the source — unlike Postgres
    /// (where pgoutput omits GENERATED columns) `MySQL` row images carry every
    /// table column, so an unmapped field means the dataset schema has
    /// drifted from the source table.
    pub fn column_map(
        &self,
        dataset_schema: &SchemaRef,
        database: &str,
        table: &str,
    ) -> Result<Vec<usize>> {
        dataset_schema
            .fields()
            .iter()
            .map(|field| {
                self.columns
                    .iter()
                    .position(|c| c.name == *field.name())
                    .context(ColumnMissingSnafu {
                        column: field.name().clone(),
                        database,
                        table,
                    })
            })
            .collect()
    }

    /// Names of the source table's PRIMARY KEY columns, in layout order.
    #[must_use]
    pub fn primary_key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Stable fingerprint of the source positional layout.
    ///
    /// Binlog row images are positional. Resuming from a persisted position
    /// against a *different* ordinal layout (reorder, same-count reshape,
    /// retype) silently maps values onto the wrong dataset columns whenever
    /// types still convert. This fingerprint is persisted with each
    /// checkpoint so resume can refuse that case.
    ///
    /// Format (one line per ordinal, `\n`-joined):
    /// `{ordinal}\t{name}\t{column_type}\t{PRI|}\n`
    #[must_use]
    pub fn fingerprint(&self) -> String {
        let mut out = String::with_capacity(self.columns.len().saturating_mul(48));
        for (ordinal, col) in self.columns.iter().enumerate() {
            use std::fmt::Write as _;
            let _ = writeln!(
                out,
                "{ordinal}\t{}\t{}\t{}",
                col.name,
                col.column_type,
                if col.is_primary_key { "PRI" } else { "" }
            );
        }
        out
    }
}

/// Open a plain (non-binlog) connection for setup/snapshot queries.
pub async fn connect(params: &ReplicationParams) -> Result<Conn> {
    Conn::new(params.opts.clone()).await.context(ConnectSnafu)
}

// Privileges a CDC subscription needs at global (`ON *.*`) scope.
//
// `REPLICATION SLAVE` streams the binlog and `REPLICATION CLIENT` reads its
// position (`SHOW BINARY LOG STATUS`). Both are global-only in MySQL — they
// cannot be granted per-database — so their absence from an account's `ON *.*`
// grants is conclusive.
//
// MariaDB 10.5 renamed the pair to `REPLICATION REPLICA` and `BINLOG MONITOR`
// and reports the new spelling from `SHOW GRANTS`, so both must count: treating
// only the MySQL names as valid would reject a MariaDB account that streams
// perfectly well today.
//
// `SELECT` is deliberately not audited: it is grantable at database, table and
// column scope, and this check runs before the dataset's table is known, so a
// perfectly valid per-table grant would read as missing. A missing `SELECT`
// still surfaces at snapshot time, named against the table that needs it.
const REPLICATION_SLAVE_ALIASES: [&str; 2] = ["REPLICATION SLAVE", "REPLICATION REPLICA"];
const REPLICATION_CLIENT_ALIASES: [&str; 2] = ["REPLICATION CLIENT", "BINLOG MONITOR"];

/// What [`audit_grants`] could conclude from a `SHOW GRANTS` result.
#[derive(Debug, PartialEq, Eq)]
enum PrivilegeAudit {
    /// Every required privilege is held (possibly via `ALL PRIVILEGES`).
    Satisfied,
    /// The grants were understood in full and these are definitively absent.
    Missing(Vec<&'static str>),
    /// The grants contain something this parser does not model — most often a
    /// role grant, whose constituent privileges `SHOW GRANTS` does not expand.
    /// No conclusion is drawn, so the check defers to the server rather than
    /// blocking a dataset that would in fact replicate.
    Inconclusive,
}

/// Split `haystack` on the first case-insensitive occurrence of `needle`.
///
/// `to_ascii_uppercase` is byte-length preserving, so offsets into the folded
/// copy index the original safely.
fn split_once_ignore_ascii_case<'a>(haystack: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let folded = haystack.to_ascii_uppercase();
    let at = folded.find(&needle.to_ascii_uppercase())?;
    Some((haystack.get(..at)?, haystack.get(at + needle.len()..)?))
}

/// Normalize one privilege token from a `SHOW GRANTS` privilege list.
fn normalize_privilege(token: &str) -> String {
    let mut out = String::with_capacity(token.len());
    for word in token.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(&word.to_ascii_uppercase());
    }
    out
}

/// Escape the characters that would end a `MySQL` single-quoted string early.
///
/// `MySQL` and `MariaDB` both permit `'` and `\` inside a user or host name, so
/// interpolating one unescaped would make the suggested `GRANT` unpasteable.
///
/// A quote is **doubled** rather than backslash-escaped because doubling is
/// accepted whatever the session's `NO_BACKSLASH_ESCAPES` `sql_mode` is, whereas
/// `\'` silently stops being an escape under that mode. A literal backslash has
/// no such mode-independent spelling — `\\` is correct under the default mode,
/// and an account name containing one is far rarer than one containing a quote.
fn escape_quoted(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out
}

/// Render `user@host` (as `CURRENT_USER()` reports it) in `MySQL` account
/// syntax, so the suggested `GRANT` can be pasted verbatim.
///
/// `CURRENT_USER()` flattens the two components into one string with no
/// escaping, so the last `@` is the only available boundary. That is right for
/// every host that contains no `@` — every DNS name, IPv4 and IPv6 literal, and
/// `%` — and misattributes only a proxied account whose host component itself
/// contains one.
fn quote_account(account: &str) -> String {
    match account.rsplit_once('@') {
        Some((user, host)) => {
            let (user, host) = (escape_quoted(user), escape_quoted(host));
            format!("'{user}'@'{host}'")
        }
        None => format!("'{}'", escape_quoted(account)),
    }
}

/// Whether the audited global grants include any spelling in `aliases`.
fn holds_any(global: &[String], aliases: &[&str]) -> bool {
    aliases
        .iter()
        .any(|alias| global.iter().any(|held| held.as_str() == *alias))
}

/// Audit a `SHOW GRANTS` result for the privileges CDC requires.
fn audit_grants(grants: &[String]) -> PrivilegeAudit {
    // Every account holds at least `GRANT USAGE ON *.*`, so an empty result is
    // not "no privileges" — it is a result this parser cannot reason about.
    if grants.is_empty() {
        return PrivilegeAudit::Inconclusive;
    }

    let mut global: Vec<String> = Vec::new();
    for line in grants {
        let line = line.trim();
        let is_grant = line
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("GRANT "));
        if !is_grant {
            return PrivilegeAudit::Inconclusive;
        }
        let rest = line.get(6..).unwrap_or("");
        // A role grant (`GRANT `reader`@`%` TO `spice`@`%``) carries no ` ON `
        // object and never lists the privileges the role confers.
        let Some((privileges, object)) = split_once_ignore_ascii_case(rest, " ON ") else {
            return PrivilegeAudit::Inconclusive;
        };
        let object = split_once_ignore_ascii_case(object, " TO ")
            .map_or(object, |(target, _)| target)
            .trim();
        // Only `ON *.*` can carry the global-only replication privileges. A
        // narrower object cannot, and cannot have column-scoped commas either.
        if object != "*.*" {
            continue;
        }
        for token in privileges.split(',') {
            global.push(normalize_privilege(token));
        }
    }

    let holds_everything = global
        .iter()
        .any(|held| held.as_str() == "ALL PRIVILEGES" || held.as_str() == "ALL");
    if holds_everything {
        return PrivilegeAudit::Satisfied;
    }

    let mut missing: Vec<&'static str> = Vec::new();
    if !holds_any(&global, &REPLICATION_SLAVE_ALIASES) {
        missing.push("REPLICATION SLAVE");
    }
    if !holds_any(&global, &REPLICATION_CLIENT_ALIASES) {
        missing.push("REPLICATION CLIENT");
    }

    if missing.is_empty() {
        PrivilegeAudit::Satisfied
    } else {
        PrivilegeAudit::Missing(missing)
    }
}

/// Pre-flight the connecting account's replication privileges, so a missing
/// `GRANT` is reported as one instead of as a bare `Access denied` from the
/// first replication command.
///
/// Only a definitive absence fails. If `SHOW GRANTS` cannot be read, or lists a
/// role whose privileges it does not expand, the check defers to the server.
pub async fn check_privileges(conn: &mut Conn) -> Result<()> {
    let grants: Vec<String> = match conn.query("SHOW GRANTS FOR CURRENT_USER()").await {
        Ok(grants) => grants,
        Err(e) => {
            tracing::debug!(
                error = %e,
                "SHOW GRANTS unavailable; skipping the CDC privilege pre-flight"
            );
            return Ok(());
        }
    };

    let PrivilegeAudit::Missing(missing) = audit_grants(&grants) else {
        return Ok(());
    };

    // Typed as `Option<String>`, not `String`, so the fallback below survives a
    // server that answers with SQL NULL: `query_first` converts rows with the
    // panicking `FromRow`, so a NULL decoded straight into `String` would panic
    // instead of falling through to the placeholder. Hence the double flatten —
    // one for the `Result`, one for the NULL.
    let current_user = conn
        .query_first::<Option<String>, _>("SELECT CURRENT_USER()")
        .await
        .ok()
        .flatten()
        .flatten();
    let (account, grant_target) = match current_user {
        Some(user) => {
            let target = quote_account(&user);
            (user, target)
        }
        None => (
            "the connecting account".to_string(),
            "'<user>'@'<host>'".to_string(),
        ),
    };

    MissingPrivilegesSnafu {
        account,
        grant_target,
        missing: missing.join(", "),
    }
    .fail()
}

/// Validate that the source server is configured for row-based binlog
/// replication. Each failure carries the exact server setting to change.
pub async fn validate_server(conn: &mut Conn) -> Result<()> {
    // Privileges first: an account without `REPLICATION CLIENT` can still read
    // the settings below, so checking them first would report a healthy server
    // and defer the real problem to an opaque `Access denied` at stream start.
    check_privileges(conn).await?;

    let settings: Option<(i64, String, String)> = conn
        .query_first("SELECT @@log_bin, @@binlog_format, @@binlog_row_image")
        .await
        .context(SetupQuerySnafu {
            context: "SELECT @@log_bin, @@binlog_format, @@binlog_row_image",
        })?;
    let (log_bin, format, image) = settings.unwrap_or_default();

    if log_bin != 1 {
        return BinaryLoggingDisabledSnafu.fail();
    }
    if !format.eq_ignore_ascii_case("ROW") {
        return UnsupportedBinlogFormatSnafu { format }.fail();
    }
    if !image.eq_ignore_ascii_case("FULL") {
        return UnsupportedBinlogRowImageSnafu { image }.fail();
    }

    // `binlog_row_value_options = PARTIAL_JSON` makes JSON row images partial,
    // which cannot be applied — fail fast here instead of at decode time.
    // Queried separately because the variable does not exist on every server
    // (e.g. MariaDB): an unknown-variable error means "never partial".
    match conn
        .query_first::<Option<String>, _>("SELECT @@binlog_row_value_options")
        .await
    {
        Ok(row) => {
            let options = row.flatten().unwrap_or_default();
            if !options.trim().is_empty() {
                return UnsupportedBinlogRowValueOptionsSnafu { options }.fail();
            }
        }
        // 1193: ER_UNKNOWN_SYSTEM_VARIABLE
        Err(mysql_async::Error::Server(ref e)) if e.code == 1193 => {}
        Err(e) => {
            return Err(e).context(SetupQuerySnafu {
                context: "SELECT @@binlog_row_value_options",
            });
        }
    }

    Ok(())
}

/// Fetch the positional column layout for `database.table` from
/// `information_schema.COLUMNS`.
pub async fn fetch_table_layout(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<TableLayout> {
    let rows: Vec<Row> = conn
        .exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .context(SetupQuerySnafu {
            context: "information_schema.COLUMNS layout query",
        })?;

    if rows.is_empty() {
        return SourceTableNotFoundSnafu { database, table }.fail();
    }

    let columns = rows
        .into_iter()
        .enumerate()
        .map(|(idx, row)| {
            let name: String = row.get("COLUMN_NAME").ok_or_else(|| Error::Decode {
                message: format!(
                    "information_schema.COLUMNS row {idx} for {database}.{table} is missing COLUMN_NAME"
                ),
            })?;
            // COLUMN_TYPE is part of the resume-safety fingerprint — fail
            // closed rather than recording an empty type that would weaken
            // drift detection.
            let column_type: String = row.get("COLUMN_TYPE").ok_or_else(|| Error::Decode {
                message: format!(
                    "information_schema.COLUMNS row for {database}.{table}.{name} is missing COLUMN_TYPE"
                ),
            })?;
            if name.is_empty() || column_type.is_empty() {
                return Err(Error::Decode {
                    message: format!(
                        "information_schema.COLUMNS row {idx} for {database}.{table} has empty COLUMN_NAME or COLUMN_TYPE"
                    ),
                });
            }
            let column_key: String = row.get("COLUMN_KEY").unwrap_or_default();
            let has_prefix = |prefix: &str| {
                column_type
                    .get(..prefix.len())
                    .is_some_and(|p| p.eq_ignore_ascii_case(prefix))
            };
            let enum_variants = has_prefix("enum(")
                .then(|| parse_quoted_variants(&column_type))
                .flatten()
                .map(Arc::from);
            let set_variants = has_prefix("set(")
                .then(|| parse_quoted_variants(&column_type))
                .flatten()
                .map(Arc::from);
            Ok(SourceColumn {
                name,
                column_type,
                enum_variants,
                set_variants,
                is_primary_key: column_key == "PRI",
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TableLayout { columns })
}

/// The current head of the binary log — where a fresh subscription starts.
///
/// Uses `SHOW BINARY LOG STATUS` (`MySQL` 8.2+; the only form on 8.4+) and
/// falls back to `SHOW MASTER STATUS` on older servers.
pub async fn fetch_head_position(conn: &mut Conn) -> Result<BinlogPosition> {
    let row = match conn.query_first::<Row, _>("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(e) if is_unknown_statement(&e) => conn
            .query_first::<Row, _>("SHOW MASTER STATUS")
            .await
            .context(SetupQuerySnafu {
                context: "SHOW MASTER STATUS",
            })?,
        Err(e) => {
            return Err(e).context(SetupQuerySnafu {
                context: "SHOW BINARY LOG STATUS",
            });
        }
    };

    let Some(row) = row else {
        // Only possible when binary logging is off, which `validate_server`
        // has already rejected — but surface it cleanly anyway.
        return BinaryLoggingDisabledSnafu.fail();
    };

    let file: Option<String> = row.get("File");
    let pos: Option<u64> = row.get("Position");
    match (file, pos) {
        (Some(file), Some(pos)) => Ok(BinlogPosition::new(file, pos)),
        _ => Err(Error::Decode {
            message: "binary log status row did not include File/Position".to_string(),
        }),
    }
}

/// The source's observed `@@GLOBAL.gtid_mode`, normalized (trimmed, uppercased)
/// so it can be reported verbatim in logs and compared for the on/off decision.
///
/// `None` means the server does not support `MySQL`-format GTIDs at all —
/// `MariaDB` and pre-GTID `MySQL` don't know the variable (server error `1193`,
/// `ER_UNKNOWN_SYSTEM_VARIABLE`), which is not a fatal error — so callers can
/// report "GTIDs unsupported" rather than a confusing literal value. Otherwise
/// the value is the reported mode: only the exact `Some("ON")` enables GTID
/// auto-positioning; `OFF` and the transitional `ON_PERMISSIVE`/`OFF_PERMISSIVE`
/// states (a mixed topology can still emit anonymous transactions, which GTID
/// auto-positioning cannot resume from) mean file+offset.
pub async fn detect_gtid_mode(conn: &mut Conn) -> Result<Option<String>> {
    match conn
        .query_first::<Option<String>, _>("SELECT @@GLOBAL.gtid_mode")
        .await
    {
        Ok(row) => Ok(row.flatten().map(|mode| mode.trim().to_ascii_uppercase())),
        // 1193: ER_UNKNOWN_SYSTEM_VARIABLE (MariaDB / pre-GTID MySQL) — the
        // variable does not exist, so GTIDs are unsupported here.
        Err(mysql_async::Error::Server(ref e)) if e.code == 1193 => Ok(None),
        Err(e) => Err(e).context(SetupQuerySnafu {
            context: "SELECT @@GLOBAL.gtid_mode",
        }),
    }
}

/// Whether an observed [`detect_gtid_mode`] value enables GTID auto-positioning
/// (exactly `ON`). `None` (GTIDs unsupported) is not on.
#[must_use]
pub fn gtid_mode_is_on(observed: Option<&str>) -> bool {
    matches!(observed, Some(mode) if mode.eq_ignore_ascii_case("ON"))
}

/// The current binlog head together with the source's executed GTID set,
/// captured atomically from a single `SHOW BINARY LOG STATUS` row (the
/// `Executed_Gtid_Set` column). This pairing is the cold-start seed for
/// GTID auto-positioning: file+offset drive the initial snapshot boundary while
/// the GTID set becomes the durable, failover-safe resume identity.
///
/// The returned set is empty when the server reports no executed GTIDs (a
/// freshly-reset GTID-enabled server), which is a valid starting point.
pub async fn fetch_head_and_gtid(conn: &mut Conn) -> Result<(BinlogPosition, GtidSet)> {
    let row = match conn.query_first::<Row, _>("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(e) if is_unknown_statement(&e) => conn
            .query_first::<Row, _>("SHOW MASTER STATUS")
            .await
            .context(SetupQuerySnafu {
                context: "SHOW MASTER STATUS",
            })?,
        Err(e) => {
            return Err(e).context(SetupQuerySnafu {
                context: "SHOW BINARY LOG STATUS",
            });
        }
    };

    let Some(row) = row else {
        return BinaryLoggingDisabledSnafu.fail();
    };

    let file: Option<String> = row.get("File");
    let pos: Option<u64> = row.get("Position");
    let executed: Option<String> = row.get("Executed_Gtid_Set");
    let gtid = match executed {
        Some(raw) => GtidSet::parse(&raw).map_err(|message| Error::GtidParse { message })?,
        None => GtidSet::new(),
    };
    match (file, pos) {
        (Some(file), Some(pos)) => Ok((BinlogPosition::new(file, pos), gtid)),
        _ => Err(Error::Decode {
            message: "binary log status row did not include File/Position".to_string(),
        }),
    }
}

/// The source's current executed GTID set (`@@GLOBAL.gtid_executed`).
///
/// Used on resume to validate a GTID checkpoint against the live source: the
/// persisted set must be a subset of this (see [`GtidSet::is_subset_of`]). A
/// reset/rebuilt source reports a set that no longer contains the checkpoint,
/// which is how a source reset is detected before blindly resuming. Only
/// called when the source reports `gtid_mode = ON`, so the variable always
/// exists; an empty value (a freshly-reset GTID server) parses to the empty
/// set.
pub async fn fetch_executed_gtid_set(conn: &mut Conn) -> Result<GtidSet> {
    let raw: Option<String> = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .context(SetupQuerySnafu {
            context: "SELECT @@GLOBAL.gtid_executed",
        })?;
    // `@@GLOBAL.gtid_executed` always returns exactly one row (an empty *string*
    // for a fresh GTID server — parsed to the empty set below). A *missing row*
    // is an unexpected server/driver anomaly, not a known-empty set: treating it
    // as empty would make every non-empty checkpoint fail the subset check and
    // force a spurious re-snapshot, masking the real problem. Surface it instead.
    match raw {
        Some(raw) => GtidSet::parse(&raw).map_err(|message| Error::GtidParse { message }),
        None => Err(Error::Decode {
            message: "SELECT @@GLOBAL.gtid_executed returned no row".to_string(),
        }),
    }
}

/// The source's current wall clock as Unix-epoch milliseconds, via `NOW(3)`
/// (millisecond precision). Used to stamp idle readiness heartbeats with a
/// source-attested time: a binlog HEARTBEAT event carries no usable clock, so
/// lag-based readiness on a quiet source reads the source's own clock here
/// rather than a local `now()`.
pub async fn fetch_source_now_ms(conn: &mut Conn) -> Result<i64> {
    let ms: Option<i64> = conn
        .query_first("SELECT CAST(ROUND(UNIX_TIMESTAMP(NOW(3)) * 1000) AS SIGNED)")
        .await
        .context(SetupQuerySnafu {
            context: "UNIX_TIMESTAMP(NOW(3))",
        })?;
    match ms {
        Some(ms) => Ok(ms),
        None => Err(Error::Decode {
            message: "source clock query (UNIX_TIMESTAMP(NOW(3))) returned no row".to_string(),
        }),
    }
}

/// The source's approximate row count for the table, from
/// `information_schema.TABLES` (an `InnoDB` statistics estimate — cheap and
/// possibly stale, which is fine for snapshot-progress reporting). `None`
/// when the server has no estimate.
pub async fn fetch_approx_row_count(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<Option<u64>> {
    let rows: Option<Option<u64>> = conn
        .exec_first(
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (database, table),
        )
        .await
        .context(SetupQuerySnafu {
            context: "information_schema.TABLES row-count estimate",
        })?;
    Ok(rows.flatten())
}

/// Whether `file` is still present in the server's binary log index.
/// A resumable persisted position requires its file to still exist.
pub async fn binlog_file_exists(conn: &mut Conn, file: &str) -> Result<bool> {
    let rows: Vec<Row> = conn
        .query("SHOW BINARY LOGS")
        .await
        .context(SetupQuerySnafu {
            context: "SHOW BINARY LOGS",
        })?;
    Ok(rows
        .iter()
        .any(|row| row.get::<String, _>("Log_name").as_deref() == Some(file)))
}

/// `true` when the error is the server rejecting the statement syntax —
/// i.e. `SHOW BINARY LOG STATUS` on a pre-8.2 server (`ER_PARSE_ERROR`).
fn is_unknown_statement(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1064)
}

/// Extract the single-quoted variant labels from an `enum(...)`/`set(...)`
/// `COLUMN_TYPE`. `MySQL` escapes an embedded quote by doubling it (`''`).
/// Returns `None` when the value doesn't parse as a quoted list (defensive —
/// `COLUMN_TYPE` is server-generated and should always parse).
fn parse_quoted_variants(column_type: &str) -> Option<Vec<String>> {
    let open = column_type.find('(')?;
    let close = column_type.rfind(')')?;
    let body = column_type.get(open + 1..close)?;

    let mut variants = Vec::new();
    let mut chars = body.chars().peekable();
    // Each iteration skips separators up to the next opening quote, then
    // consumes one quoted variant.
    while chars.find(|c| *c == '\'').is_some() {
        let mut current = String::new();
        loop {
            match chars.next() {
                Some('\'') => {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        current.push('\'');
                    } else {
                        break;
                    }
                }
                Some(c) => current.push(c),
                None => return None, // unterminated quote
            }
        }
        variants.push(current);
    }
    Some(variants)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn layout(names: &[&str]) -> TableLayout {
        TableLayout {
            columns: names
                .iter()
                .map(|n| SourceColumn {
                    name: (*n).to_string(),
                    column_type: "int".to_string(),
                    enum_variants: None,
                    set_variants: None,
                    is_primary_key: false,
                })
                .collect(),
        }
    }

    #[test]
    fn fingerprint_changes_on_column_reorder() {
        let a = layout(&["id", "name"]);
        let b = layout(&["name", "id"]);
        assert_ne!(
            a.fingerprint(),
            b.fingerprint(),
            "reordering columns must change the layout fingerprint"
        );
    }

    #[test]
    fn fingerprint_changes_on_retype() {
        let mut a = layout(&["id", "name"]);
        let mut b = layout(&["id", "name"]);
        b.columns[1].column_type = "varchar(255)".to_string();
        assert_ne!(a.fingerprint(), b.fingerprint());
        // Same layout fingerprints identically (stability).
        assert_eq!(a.fingerprint(), a.fingerprint());
        a.columns[0].is_primary_key = true;
        assert_ne!(
            a.fingerprint(),
            b.fingerprint(),
            "primary-key membership is part of the fingerprint"
        );
    }

    #[test]
    fn parses_enum_variants() {
        assert_eq!(
            parse_quoted_variants("enum('small','medium','large')"),
            Some(vec![
                "small".to_string(),
                "medium".to_string(),
                "large".to_string()
            ])
        );
    }

    #[test]
    fn variant_labels_preserve_case() {
        // Labels must replicate exactly as defined — lowercasing them would
        // make CDC values disagree with federated reads of the same column.
        assert_eq!(
            parse_quoted_variants("enum('Small','LARGE')"),
            Some(vec!["Small".to_string(), "LARGE".to_string()])
        );
    }

    #[test]
    fn parses_escaped_quote_in_variant() {
        assert_eq!(
            parse_quoted_variants("enum('it''s','plain')"),
            Some(vec!["it's".to_string(), "plain".to_string()])
        );
    }

    #[test]
    fn parses_variant_containing_comma_and_parens() {
        assert_eq!(
            parse_quoted_variants("set('a,b','c(d)')"),
            Some(vec!["a,b".to_string(), "c(d)".to_string()])
        );
    }

    #[test]
    fn unterminated_variant_returns_none() {
        assert_eq!(parse_quoted_variants("enum('oops"), None);
    }

    #[test]
    fn column_map_resolves_by_name_in_any_order() {
        let layout = layout(&["id", "name", "created_at"]);
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let map = layout
            .column_map(&schema, "db", "t")
            .expect("all fields resolve");
        assert_eq!(map, vec![1, 0]);
    }

    #[test]
    fn column_map_errors_on_missing_dataset_column() {
        let layout = layout(&["id"]);
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("ghost", DataType::Utf8, true)]));
        let err = layout
            .column_map(&schema, "db", "t")
            .expect_err("missing column must error");
        assert!(err.to_string().contains("ghost"), "got: {err}");
    }

    fn grants(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|l| (*l).to_string()).collect()
    }

    #[test]
    fn audit_accepts_the_documented_cdc_grant() {
        let held =
            grants(&["GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `spice`@`%`"]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Satisfied);
    }

    #[test]
    fn audit_reports_the_replication_privileges_a_select_only_account_lacks() {
        // The reported case: an account with SELECT but no replication grants
        // previously failed with a bare `Access denied`.
        let held = grants(&["GRANT SELECT ON *.* TO `spice`@`%`"]);
        assert_eq!(
            audit_grants(&held),
            PrivilegeAudit::Missing(vec!["REPLICATION SLAVE", "REPLICATION CLIENT"])
        );
    }

    #[test]
    fn audit_reports_only_the_privilege_actually_absent() {
        let held = grants(&["GRANT SELECT, REPLICATION CLIENT ON *.* TO `spice`@`%`"]);
        assert_eq!(
            audit_grants(&held),
            PrivilegeAudit::Missing(vec!["REPLICATION SLAVE"])
        );
    }

    #[test]
    fn audit_accepts_the_mariadb_spelling_of_the_same_privileges() {
        // MariaDB 10.5+ reports REPLICATION REPLICA / BINLOG MONITOR for the
        // grants MySQL calls REPLICATION SLAVE / REPLICATION CLIENT. Rejecting
        // those would break a MariaDB source that replicates today.
        let held =
            grants(&["GRANT SELECT, REPLICATION REPLICA, BINLOG MONITOR ON *.* TO `spice`@`%`"]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Satisfied);
    }

    #[test]
    fn audit_accepts_all_privileges() {
        let held = grants(&["GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost` WITH GRANT OPTION"]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Satisfied);
    }

    #[test]
    fn audit_ignores_narrower_objects_when_looking_for_global_privileges() {
        // Replication privileges are global-only, so a database-scoped grant
        // cannot supply them — but it must not confuse the parser either.
        let held = grants(&[
            "GRANT USAGE ON *.* TO `spice`@`%`",
            "GRANT SELECT ON `spice_demo`.`orders` TO `spice`@`%`",
            "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `spice`@`%`",
        ]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Satisfied);
    }

    #[test]
    fn audit_defers_when_privileges_arrive_through_a_role() {
        // `SHOW GRANTS` does not expand a role's privileges, so concluding
        // "missing" here would block an account that can in fact replicate.
        let held = grants(&[
            "GRANT USAGE ON *.* TO `spice`@`%`",
            "GRANT `cdc_reader`@`%` TO `spice`@`%`",
        ]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Inconclusive);
    }

    #[test]
    fn audit_defers_on_an_empty_or_unrecognized_result() {
        assert_eq!(audit_grants(&[]), PrivilegeAudit::Inconclusive);
        let held = grants(&["REVOKE SELECT ON *.* FROM `spice`@`%`"]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Inconclusive);
    }

    #[test]
    fn audit_tolerates_lowercase_keywords_and_loose_whitespace() {
        let held = grants(&["  grant  replication   slave ,replication client on *.* to `s`@`%`"]);
        assert_eq!(audit_grants(&held), PrivilegeAudit::Satisfied);
    }

    #[test]
    fn account_is_quoted_so_the_suggested_grant_can_be_pasted() {
        assert_eq!(quote_account("spice@%"), "'spice'@'%'");
        assert_eq!(quote_account("spice@10.0.0.1"), "'spice'@'10.0.0.1'");
        // A host-less value still yields valid single-quoted SQL.
        assert_eq!(quote_account("spice"), "'spice'");
    }

    #[test]
    fn account_quoting_escapes_characters_that_would_end_the_string_early() {
        // `MySQL` permits both characters in an account name, and an unescaped
        // one would close the literal and make the suggested GRANT unpasteable.
        // A quote is doubled so the result parses under `NO_BACKSLASH_ESCAPES`
        // too; a backslash has no spelling that is correct under both modes.
        assert_eq!(quote_account(r"o'brien@%"), r"'o''brien'@'%'");
        assert_eq!(quote_account(r"spice@ho'st"), r"'spice'@'ho''st'");
        assert_eq!(quote_account(r"back\slash@%"), r"'back\\slash'@'%'");
        // The host-less arm escapes on the same path.
        assert_eq!(quote_account(r"o'brien"), r"'o''brien'");
    }

    #[test]
    fn account_quoting_splits_on_the_last_at_sign() {
        // A username may contain `@`; the host component of a direct connection
        // never does, so the last `@` is the correct boundary.
        assert_eq!(quote_account("cdc@corp@10.0.0.1"), "'cdc@corp'@'10.0.0.1'");
        // An anonymous account reports an empty user part.
        assert_eq!(quote_account("@localhost"), "''@'localhost'");
        // An IPv6 host carries `:`, not `@`, so it survives the split intact.
        assert_eq!(quote_account("spice@::1"), "'spice'@'::1'");
    }
}
