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
use super::{
    BinaryLoggingDisabledSnafu, ColumnMissingSnafu, ConnectSnafu, Error, Result, SetupQuerySnafu,
    SourceTableNotFoundSnafu, UnsupportedBinlogFormatSnafu, UnsupportedBinlogRowImageSnafu,
    UnsupportedBinlogRowValueOptionsSnafu,
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

/// Validate that the source server is configured for row-based binlog
/// replication. Each failure carries the exact server setting to change.
pub async fn validate_server(conn: &mut Conn) -> Result<()> {
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
}
