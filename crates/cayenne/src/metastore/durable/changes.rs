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

//! Exact, row-level capture and replay of catalog transactions.
//!
//! A [`Capture`] installs a `TEMP` trigger on every catalog table of the
//! connection every catalog write runs on, and records each row a transaction
//! inserts, deletes or updates, in the order `SQLite` changes them. The resulting [`ChangeSet`]
//! holds the stored values themselves, so replaying it reproduces the
//! transaction exactly: a column filled by a SQL default such as
//! `strftime('now')`, a value computed by `UPDATE … RETURNING`, and the rows a
//! foreign-key cascade deletes are all recorded as the values they became,
//! never re-evaluated.
//!
//! A rowid table's row is addressed by its rowid, which `SQLite` reports for
//! every change, and a `WITHOUT ROWID` table's by its primary key, which it
//! requires to be non-NULL. Replay checks that every change affects exactly
//! one row, so a change set applied to any state other than the one it was
//! captured on fails instead of silently diverging.

use std::collections::HashMap;
use std::sync::Arc;

use rusqlite::types::{ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu, ensure};

/// The local bookkeeping table that records which durable version a catalog
/// file holds. It is never captured: every catalog file has its own.
pub(crate) const POSITION_TABLE: &str = "cayenne_durable_position";

const FORMAT: u32 = 1;

/// Why a catalog change set could not be captured or replayed.
#[derive(Debug, Snafu)]
#[expect(missing_docs, reason = "each variant's display message documents it")]
pub enum Error {
    #[snafu(display("Failed to read the catalog layout: {source}"))]
    Layout { source: rusqlite::Error },

    #[snafu(display(
        "Catalog table '{table}' changed without a known layout; its schema changed after durability was enabled"
    ))]
    UnknownTable { table: String },

    #[snafu(display("SQLite reported an unrecognized kind of change to catalog table '{table}'"))]
    UnrecognizedChange { table: String },

    #[snafu(display(
        "The catalog's schema changed while it is durable in object storage, so its changes cannot all be recorded; restart Spice to resume writing"
    ))]
    SchemaChanged,

    #[snafu(display("Failed to encode a catalog change set: {source}"))]
    Encode { source: postcard::Error },

    #[snafu(display("Failed to decode a catalog change set: {source}"))]
    Decode { source: postcard::Error },

    #[snafu(display("Unsupported catalog change set format {format}"))]
    UnsupportedFormat { format: u32 },

    #[snafu(display(
        "Catalog change set does not match the stored catalog: {operation} on table '{table}' changed {changed} rows instead of 1"
    ))]
    Diverged {
        operation: &'static str,
        table: String,
        changed: usize,
    },

    #[snafu(display("Failed to replay a change to catalog table '{table}': {source}"))]
    Replay {
        table: String,
        source: rusqlite::Error,
    },
}

/// The result of a catalog change set operation.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A stored `SQLite` value. Text keeps its raw bytes: `SQLite` does not
/// guarantee valid UTF-8, and replay must store exactly what was captured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl From<ValueRef<'_>> for Value {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(v) => Self::Integer(v),
            ValueRef::Real(v) => Self::Real(v),
            ValueRef::Text(v) => Self::Text(v.to_vec()),
            ValueRef::Blob(v) => Self::Blob(v.to_vec()),
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(v) => ValueRef::Integer(*v),
            Self::Real(v) => ValueRef::Real(*v),
            Self::Text(v) => ValueRef::Text(v),
            Self::Blob(v) => ValueRef::Blob(v),
        }))
    }
}

/// One row change, in the order `SQLite` made it. `table` indexes
/// [`ChangeSet::tables`]; `key` is the old primary key of a `WITHOUT ROWID`
/// row and empty for a rowid table, whose row the rowid addresses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum RowChange {
    Insert {
        table: u16,
        rowid: i64,
        values: Vec<Value>,
    },
    Delete {
        table: u16,
        rowid: i64,
        key: Vec<Value>,
    },
    Update {
        table: u16,
        old_rowid: i64,
        key: Vec<Value>,
        new_rowid: i64,
        values: Vec<Value>,
    },
}

/// Every row change of one catalog transaction.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct ChangeSet {
    format: u32,
    tables: Vec<String>,
    changes: Vec<RowChange>,
}

impl ChangeSet {
    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.changes.len()
    }

    /// # Errors
    ///
    /// Returns an error if the change set cannot be serialized.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        postcard::to_stdvec(self).context(EncodeSnafu)
    }

    /// # Errors
    ///
    /// Returns an error for malformed bytes or an unsupported format.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let set: Self = postcard::from_bytes(bytes).context(DecodeSnafu)?;
        ensure!(
            set.format == FORMAT,
            UnsupportedFormatSnafu { format: set.format }
        );
        Ok(set)
    }

    fn table_index(&mut self, table: &str) -> Result<u16> {
        if let Some(index) = self.tables.iter().position(|t| t == table) {
            return u16::try_from(index).map_err(|_| Error::UnknownTable {
                table: table.to_string(),
            });
        }
        let index = u16::try_from(self.tables.len()).map_err(|_| Error::UnknownTable {
            table: table.to_string(),
        })?;
        self.tables.push(table.to_string());
        Ok(index)
    }
}

/// How a catalog table addresses its rows.
#[derive(Debug, Clone)]
struct TableShape {
    columns: Vec<String>,
    /// Column indexes of the primary key, for a `WITHOUT ROWID` table only.
    key_columns: Option<Vec<usize>>,
}

/// The layout of every captured catalog table.
#[derive(Debug, Clone, Default)]
pub(crate) struct Layout(HashMap<String, TableShape>);

impl Layout {
    /// Read the layout of every `cayenne_*` table except the position table.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be read.
    pub(crate) fn load(conn: &rusqlite::Connection) -> Result<Self> {
        let tables: Vec<(String, bool)> = conn
            .prepare(
                "SELECT name, wr FROM pragma_table_list WHERE schema = 'main' AND type = 'table' AND name LIKE 'cayenne\\_%' ESCAPE '\\' AND name <> ?1",
            )
            .and_then(|mut stmt| {
                stmt.query_map([POSITION_TABLE], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0))
                })?
                .collect()
            })
            .context(LayoutSnafu)?;
        let mut layout = HashMap::with_capacity(tables.len());
        for (table, without_rowid) in tables {
            let mut columns: Vec<(String, i64)> = conn
                .prepare("SELECT name, pk FROM pragma_table_info(?1) ORDER BY cid")
                .and_then(|mut stmt| {
                    stmt.query_map([&table], |row| Ok((row.get(0)?, row.get(1)?)))?
                        .collect()
                })
                .context(LayoutSnafu)?;
            let key_columns = without_rowid.then(|| {
                let mut key: Vec<(i64, usize)> = columns
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, pk))| *pk > 0)
                    .map(|(i, (_, pk))| (*pk, i))
                    .collect();
                key.sort_unstable();
                key.into_iter().map(|(_, i)| i).collect()
            });
            layout.insert(
                table,
                TableShape {
                    columns: columns.drain(..).map(|(name, _)| name).collect(),
                    key_columns,
                },
            );
        }
        Ok(Self(layout))
    }

    fn shape(&self, table: &str) -> Result<&TableShape> {
        self.0.get(table).context(UnknownTableSnafu { table })
    }
}

#[derive(Default)]
struct Recording {
    changes: ChangeSet,
    error: Option<Error>,
}

/// The function every capture trigger calls with the row it saw.
const CAPTURE_FUNCTION: &str = "cayenne_capture_change";

const INSERT: i64 = 1;
const DELETE: i64 = 2;
const UPDATE: i64 = 3;

/// Records the row changes made on one connection.
///
/// Installs a `TEMP` trigger per catalog table, so only the connection that
/// installed it records anything and the stored schema is untouched. Each
/// trigger passes the row it saw to a function this connection registers.
/// `recursive_triggers` is turned on, without which `SQLite` deletes the rows a
/// `REPLACE` displaces without firing delete triggers.
///
/// The connection must run one transaction at a time, as the metastore's
/// writer connection does: [`Self::take`] returns everything recorded since
/// the last take or clear.
pub(crate) struct Capture {
    recording: Arc<parking_lot::Mutex<Recording>>,
    /// The last schema version seen to match `tables`.
    schema_version: std::sync::atomic::AtomicI64,
    /// The definitions of the catalog tables the triggers were installed on.
    tables: String,
}

impl Capture {
    /// Install a capture on `conn`, recording changes to the tables `layout`
    /// describes.
    ///
    /// # Errors
    ///
    /// Returns an error if the triggers or their function cannot be installed.
    pub(crate) fn install(conn: &rusqlite::Connection, layout: Layout) -> Result<Arc<Self>> {
        let layout = Arc::new(layout);
        let mut tables: Vec<&String> = layout.0.keys().collect();
        tables.sort();
        let names: Vec<String> = tables.iter().map(|t| (*t).clone()).collect();
        let recording = Arc::new(parking_lot::Mutex::new(Recording::default()));
        let sink = Arc::clone(&recording);
        let shapes = Arc::clone(&layout);
        conn.create_scalar_function(
            CAPTURE_FUNCTION,
            -1,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            move |ctx| {
                let mut recording = sink.lock();
                if recording.error.is_none()
                    && let Err(error) = record(&mut recording.changes, &names, &shapes, ctx)
                {
                    recording.error = Some(error);
                }
                Ok(rusqlite::types::Null)
            },
        )
        .context(LayoutSnafu)?;
        conn.pragma_update(None, "recursive_triggers", "ON")
            .context(LayoutSnafu)?;
        for (index, table) in tables.iter().enumerate() {
            let shape = layout.shape(table)?;
            conn.execute_batch(&triggers(index, table, shape))
                .context(LayoutSnafu)?;
        }
        Ok(Arc::new(Self {
            recording,
            schema_version: std::sync::atomic::AtomicI64::new(
                schema_version(conn).context(LayoutSnafu)?,
            ),
            tables: table_definitions(conn).context(LayoutSnafu)?,
        }))
    }

    /// Discard everything recorded so far.
    pub(crate) fn clear(&self) {
        *self.recording.lock() = Recording::default();
    }

    /// Whether the catalog tables on `conn` are still the ones the triggers
    /// were installed on, so that every change to them was recorded. Other
    /// schema changes, such as `ANALYZE` creating `sqlite_stat1`, do not count.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be read.
    pub(crate) fn covers(&self, conn: &rusqlite::Connection) -> rusqlite::Result<bool> {
        use std::sync::atomic::Ordering;
        let version = schema_version(conn)?;
        if version == self.schema_version.load(Ordering::Acquire) {
            return Ok(true);
        }
        let unchanged = table_definitions(conn)? == self.tables;
        if unchanged {
            self.schema_version.store(version, Ordering::Release);
        }
        Ok(unchanged)
    }

    /// Return the change set recorded since the last take or clear.
    ///
    /// # Errors
    ///
    /// Returns the first error recording a change hit; the transaction's
    /// changes are then incomplete and it must not commit.
    pub(crate) fn take(&self) -> Result<ChangeSet> {
        let recording = std::mem::take(&mut *self.recording.lock());
        if let Some(error) = recording.error {
            return Err(error);
        }
        let mut changes = recording.changes;
        changes.format = FORMAT;
        Ok(changes)
    }
}

/// The schema version `SQLite` bumps on every schema change.
fn schema_version(conn: &rusqlite::Connection) -> rusqlite::Result<i64> {
    conn.pragma_query_value(None, "schema_version", |row| row.get(0))
}

/// The stored definitions of every captured catalog table, in name order.
fn table_definitions(conn: &rusqlite::Connection) -> rusqlite::Result<String> {
    conn.query_row(
        "SELECT coalesce(group_concat(name || ':' || sql, ';'), '') FROM (SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name LIKE 'cayenne\\_%' ESCAPE '\\' AND name <> ?1 ORDER BY name)",
        [POSITION_TABLE],
        |row| row.get(0),
    )
}

/// The three capture triggers of one table. A `WITHOUT ROWID` table has no
/// rowid to pass, and is addressed by its key columns instead.
fn triggers(index: usize, table: &str, shape: &TableShape) -> String {
    let rowid = |row: &str| {
        if shape.key_columns.is_some() {
            "NULL".to_string()
        } else {
            format!("{row}.rowid")
        }
    };
    let columns = |row: &str| {
        shape
            .columns
            .iter()
            .map(|c| format!("{row}.{}", quote(c)))
            .collect::<Vec<_>>()
    };
    let key = |row: &str| {
        shape
            .key_columns
            .iter()
            .flatten()
            .map(|&i| format!("{row}.{}", quote(&shape.columns[i])))
            .collect::<Vec<_>>()
    };
    let call = |name: &str, event: &str, args: Vec<String>| {
        format!(
            "CREATE TEMP TRIGGER IF NOT EXISTS {} AFTER {event} ON main.{} BEGIN SELECT {CAPTURE_FUNCTION}({}); END;",
            quote(&format!("cayenne_capture_{index}_{name}")),
            quote(table),
            args.join(", ")
        )
    };
    let mut insert = vec![index.to_string(), INSERT.to_string(), rowid("NEW")];
    insert.extend(columns("NEW"));
    let mut delete = vec![index.to_string(), DELETE.to_string(), rowid("OLD")];
    delete.extend(key("OLD"));
    let mut update = vec![
        index.to_string(),
        UPDATE.to_string(),
        rowid("OLD"),
        rowid("NEW"),
    ];
    update.extend(key("OLD"));
    update.extend(columns("NEW"));
    [
        call("insert", "INSERT", insert),
        call("delete", "DELETE", delete),
        call("update", "UPDATE", update),
    ]
    .join("\n")
}

/// Record one trigger call: `(table, op, rowid…, key…, values…)`.
fn record(
    changes: &mut ChangeSet,
    tables: &[String],
    layout: &Layout,
    ctx: &rusqlite::functions::Context<'_>,
) -> Result<()> {
    let integer = |i: usize| match ctx.get_raw(i) {
        ValueRef::Integer(v) => Some(v),
        _ => None,
    };
    let table_name = integer(0)
        .and_then(|i| usize::try_from(i).ok())
        .and_then(|i| tables.get(i))
        .context(UnrecognizedChangeSnafu {
            table: "(unknown)".to_string(),
        })?;
    let shape = layout.shape(table_name)?;
    let table = changes.table_index(table_name)?;
    let rowid = |i: usize| integer(i).unwrap_or(0);
    let values = |from: usize, count: usize| -> Vec<Value> {
        (from..from + count)
            .map(|i| Value::from(ctx.get_raw(i)))
            .collect()
    };
    let width = shape.columns.len();
    let key_width = shape.key_columns.as_ref().map_or(0, Vec::len);
    let expect = |args: usize| {
        if ctx.len() == args {
            Ok(())
        } else {
            UnrecognizedChangeSnafu {
                table: table_name.clone(),
            }
            .fail()
        }
    };
    let change = match integer(1) {
        Some(INSERT) => {
            expect(3 + width)?;
            RowChange::Insert {
                table,
                rowid: rowid(2),
                values: values(3, width),
            }
        }
        Some(DELETE) => {
            expect(3 + key_width)?;
            RowChange::Delete {
                table,
                rowid: rowid(2),
                key: values(3, key_width),
            }
        }
        Some(UPDATE) => {
            expect(4 + key_width + width)?;
            RowChange::Update {
                table,
                old_rowid: rowid(2),
                key: values(4, key_width),
                new_rowid: rowid(3),
                values: values(4 + key_width, width),
            }
        }
        _ => {
            return UnrecognizedChangeSnafu {
                table: table_name.clone(),
            }
            .fail();
        }
    };
    changes.changes.push(change);
    Ok(())
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Apply `changes` to `conn`, inside the caller's transaction.
///
/// Foreign-key enforcement must be off on `conn`: the change set already
/// holds every row a cascade changed, so letting a cascade run again would
/// change rows twice.
///
/// # Errors
///
/// Returns [`Error::Diverged`] when a change does not affect exactly one row,
/// or the statement's error when one fails.
pub(crate) fn apply(
    conn: &rusqlite::Connection,
    layout: &Layout,
    changes: &ChangeSet,
) -> Result<()> {
    for change in &changes.changes {
        let (index, operation) = match change {
            RowChange::Insert { table, .. } => (*table, "insert"),
            RowChange::Delete { table, .. } => (*table, "delete"),
            RowChange::Update { table, .. } => (*table, "update"),
        };
        let table = changes
            .tables
            .get(usize::from(index))
            .context(UnknownTableSnafu {
                table: format!("#{index}"),
            })?;
        let shape = layout.shape(table)?;
        let replay = |source| Error::Replay {
            table: table.clone(),
            source,
        };
        let affected = apply_one(conn, table, shape, change).map_err(replay)?;
        ensure!(
            affected == 1,
            DivergedSnafu {
                operation,
                table: table.clone(),
                changed: affected,
            }
        );
    }
    Ok(())
}

fn key_predicate(shape: &TableShape, first_param: usize) -> String {
    match &shape.key_columns {
        None => format!("rowid = ?{first_param}"),
        Some(key) => key
            .iter()
            .enumerate()
            .map(|(n, &i)| format!("{} IS ?{}", quote(&shape.columns[i]), first_param + n))
            .collect::<Vec<_>>()
            .join(" AND "),
    }
}

fn apply_one(
    conn: &rusqlite::Connection,
    table: &str,
    shape: &TableShape,
    change: &RowChange,
) -> rusqlite::Result<usize> {
    let rowid_table = shape.key_columns.is_none();
    let columns = shape.columns.iter().map(|c| quote(c)).collect::<Vec<_>>();
    match change {
        RowChange::Insert { rowid, values, .. } => {
            check_width(shape, values)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(values.len() + 1);
            let (names, offset) = if rowid_table {
                params.push(rowid);
                (format!("rowid, {}", columns.join(", ")), 1)
            } else {
                (columns.join(", "), 0)
            };
            params.extend(values.iter().map(|v| v as &dyn rusqlite::ToSql));
            let placeholders = (1..=values.len() + offset)
                .map(|n| format!("?{n}"))
                .collect::<Vec<_>>()
                .join(", ");
            conn.prepare_cached(&format!(
                "INSERT INTO {} ({names}) VALUES ({placeholders})",
                quote(table)
            ))?
            .execute(params.as_slice())
        }
        RowChange::Delete { rowid, key, .. } => {
            let params: Vec<&dyn rusqlite::ToSql> = if rowid_table {
                vec![rowid]
            } else {
                key.iter().map(|v| v as &dyn rusqlite::ToSql).collect()
            };
            conn.prepare_cached(&format!(
                "DELETE FROM {} WHERE {}",
                quote(table),
                key_predicate(shape, 1)
            ))?
            .execute(params.as_slice())
        }
        RowChange::Update {
            old_rowid,
            key,
            new_rowid,
            values,
            ..
        } => {
            check_width(shape, values)?;
            let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(values.len() + 2);
            let mut assignments = Vec::with_capacity(columns.len() + 1);
            if rowid_table {
                params.push(new_rowid);
                assignments.push("rowid = ?1".to_string());
            }
            let offset = params.len();
            for (n, (column, value)) in columns.iter().zip(values).enumerate() {
                params.push(value);
                assignments.push(format!("{column} = ?{}", offset + n + 1));
            }
            let first_key_param = params.len() + 1;
            if rowid_table {
                params.push(old_rowid);
            } else {
                params.extend(key.iter().map(|v| v as &dyn rusqlite::ToSql));
            }
            conn.prepare_cached(&format!(
                "UPDATE {} SET {} WHERE {}",
                quote(table),
                assignments.join(", "),
                key_predicate(shape, first_key_param)
            ))?
            .execute(params.as_slice())
        }
    }
}

fn check_width(shape: &TableShape, values: &[Value]) -> rusqlite::Result<()> {
    if values.len() == shape.columns.len() {
        return Ok(());
    }
    Err(rusqlite::Error::InvalidColumnIndex(values.len()))
}
