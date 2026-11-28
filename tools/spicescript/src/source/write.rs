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

//! Row images and SQL shared by source-neutral writes.

use super::{Source, TextResult};
use crate::generate::Dialect;
use std::{collections::BTreeMap, fmt::Write};

use anyhow::{Context, Result, bail, ensure};
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::value::RawValue;
use sqlparser::{
    ast::{
        Assignment, AssignmentTarget, FromTable, ObjectName, OnConflictAction, OnInsert, Statement,
        TableFactor, TableObject,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};

/// Scalars keep numeric text until the container CLI decodes it. Routing
/// decimals through an f64 would change the oracle's row images.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    Bool(bool),
    Number(String),
    String(String),
}

impl Serialize for Cell {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Number(value) => RawValue::from_string(value.clone())
                .map_err(serde::ser::Error::custom)?
                .serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowChange {
    Upsert(BTreeMap<String, Cell>),
    Delete(Cell),
}

impl Serialize for RowChange {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::Upsert(doc) => map.serialize_entry("doc", doc)?,
            Self::Delete(key) => map.serialize_entry("delete", key)?,
        }
        map.end()
    }
}

#[derive(Debug)]
pub struct WriteStatement {
    pub table: String,
    pub sql: String,
    pub delete: bool,
}

fn relation(table: &TableFactor) -> Result<&ObjectName> {
    match table {
        TableFactor::Table { name, .. } => Ok(name),
        _ => bail!("write needs a named table"),
    }
}

/// Parse before executing any statement so unsupported SQL has no side effects.
pub fn statements(sql: &[String], key: &str) -> Result<Vec<WriteStatement>> {
    let mut out = Vec::new();
    for sql in sql {
        for statement in Parser::parse_sql(&PostgreSqlDialect {}, sql)? {
            let (table, returning, delete) = match &statement {
                Statement::Insert(insert) => {
                    if let Some(OnInsert::OnConflict(conflict)) = &insert.on
                        && let OnConflictAction::DoUpdate(update) = &conflict.action
                    {
                        check_assignments(&update.assignments, key)?;
                    }
                    let TableObject::TableName(name) = &insert.table else {
                        bail!("write needs a named table")
                    };
                    (name, &insert.returning, false)
                }
                Statement::Update(update) => {
                    check_assignments(&update.assignments, key)?;
                    ensure!(
                        update.table.joins.is_empty(),
                        "write cannot update joined tables"
                    );
                    (relation(&update.table.relation)?, &update.returning, false)
                }
                Statement::Delete(delete) => {
                    let (FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables)) =
                        &delete.from;
                    ensure!(
                        tables.len() == 1 && tables[0].joins.is_empty(),
                        "write needs exactly one target table"
                    );
                    (relation(&tables[0].relation)?, &delete.returning, true)
                }
                _ => bail!("write accepts only INSERT, UPDATE and DELETE statements"),
            };
            ensure!(
                returning.is_none(),
                "write adds RETURNING *; omit an explicit RETURNING clause"
            );
            out.push(WriteStatement {
                table: table.to_string(),
                sql: format!("{statement} RETURNING *"),
                delete,
            });
        }
    }
    ensure!(!out.is_empty(), "write needs at least one statement");
    Ok(out)
}

fn check_assignments(assignments: &[Assignment], key: &str) -> Result<()> {
    for assignment in assignments {
        let columns = match &assignment.target {
            AssignmentTarget::ColumnName(name) => std::slice::from_ref(name),
            AssignmentTarget::Tuple(names) => names.as_slice(),
        };
        ensure!(
            !columns.iter().any(|name| name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .is_some_and(|id| ident_value(id) == key)),
            "write cannot update key column '{key}'; delete then insert instead"
        );
    }
    Ok(())
}

fn ident_value(id: &sqlparser::ast::Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_lowercase()
    }
}

pub fn table_parts(table: &str) -> Result<Vec<String>> {
    let mut parser = Parser::new(&PostgreSqlDialect {}).try_with_sql(table)?;
    let name = parser.parse_object_name(false)?;
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(ident_value)
                .context("table name must contain identifiers")
        })
        .collect()
}

pub async fn column_types(
    source: &dyn Source,
    namespace: &str,
    table: &str,
    key: &str,
) -> Result<Vec<(String, String)>> {
    let relation = table.replace('\'', "''");
    let key_literal = key.replace('\'', "''");
    let unique = source.query(namespace, &format!("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] WHERE i.indrelid = '{relation}'::regclass AND i.indisunique AND i.indisvalid AND i.indnkeyatts = 1 AND i.indpred IS NULL AND i.indexprs IS NULL AND a.attname = '{key_literal}')")).await?;
    ensure!(
        unique.rows == vec![vec![Some("t".into())]],
        "write key '{key}' on table {table} needs a single-column PRIMARY KEY or UNIQUE constraint"
    );
    let result = source.query(namespace, &format!("SELECT a.attname, t.typname FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid WHERE a.attrelid = '{}'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum", table.replace('\'', "''"))).await?;
    result
        .rows
        .into_iter()
        .map(|row| {
            let mut row = row.into_iter();
            Ok((
                row.next().flatten().context("missing column name")?,
                row.next().flatten().context("missing column type")?,
            ))
        })
        .collect()
}

pub fn numeric(kind: &str) -> bool {
    matches!(
        kind,
        "int2"
            | "int4"
            | "int8"
            | "numeric"
            | "float4"
            | "float8"
            | "bigint"
            | "integer"
            | "double precision"
    )
}

pub fn json_value(text: Option<&str>, kind: &str) -> Result<Cell> {
    let Some(text) = text else {
        return Ok(Cell::Null);
    };
    if numeric(kind) {
        let number = RawValue::from_string(text.into())
            .with_context(|| format!("'{text}' of type {kind} is not a finite JSON number"))?;
        ensure!(
            text.starts_with('-') || text.as_bytes().first().is_some_and(u8::is_ascii_digit),
            "'{text}' of type {kind} is not a finite JSON number"
        );
        return Ok(Cell::Number(number.get().into()));
    }
    if matches!(kind, "bool" | "boolean") {
        return match text {
            "t" | "true" => Ok(Cell::Bool(true)),
            "f" | "false" => Ok(Cell::Bool(false)),
            _ => bail!("invalid boolean '{text}'"),
        };
    }
    Ok(Cell::String(text.into()))
}

pub fn row_changes(
    images: &TextResult,
    types: &[(String, String)],
    key: &str,
    delete: bool,
) -> Result<Vec<RowChange>> {
    let names = images
        .columns
        .iter()
        .collect::<std::collections::HashSet<_>>();
    ensure!(
        names.len() == images.columns.len(),
        "row image has duplicate column names"
    );
    images
        .rows
        .iter()
        .map(|row| {
            ensure!(
                row.len() == images.columns.len(),
                "row image width does not match columns"
            );
            let doc: BTreeMap<String, Cell> = images
                .columns
                .iter()
                .zip(row)
                .map(|(name, cell)| {
                    let kind = types
                        .iter()
                        .find(|(column, _)| column == name)
                        .map(|(_, kind)| kind)
                        .with_context(|| format!("no mirror type for column '{name}'"))?;
                    Ok((name.clone(), json_value(cell.as_deref(), kind)?))
                })
                .collect::<Result<_>>()?;
            let value = doc
                .get(key)
                .filter(|v| !matches!(v, Cell::Null))
                .with_context(|| format!("row image has no non-NULL key '{key}'"))?;
            if delete {
                Ok(RowChange::Delete(value.clone()))
            } else {
                Ok(RowChange::Upsert(doc))
            }
        })
        .collect()
}

fn identifier(text: &str, dialect: Dialect) -> String {
    let quote = if dialect == Dialect::Mysql { '`' } else { '"' };
    format!(
        "{quote}{}{quote}",
        text.replace(quote, &format!("{quote}{quote}"))
    )
}

fn literal(value: &Cell, dialect: Dialect) -> String {
    match value {
        Cell::Null => "NULL".into(),
        Cell::Bool(v) => {
            if dialect == Dialect::Mysql {
                u8::from(*v).to_string()
            } else {
                v.to_string()
            }
        }
        Cell::Number(v) => v.clone(),
        Cell::String(v) => {
            let escaped = v.replace('\'', "''");
            if dialect == Dialect::Mysql {
                format!("'{}'", escaped.replace('\\', "\\\\"))
            } else {
                format!("E'{}'", escaped.replace('\\', "\\\\"))
            }
        }
    }
}

pub fn sql_changes(
    table: &str,
    key: &str,
    changes: &[RowChange],
    dialect: Dialect,
) -> Result<String> {
    let table = table_parts(table)?
        .iter()
        .map(|p| identifier(p, dialect))
        .collect::<Vec<_>>()
        .join(".");
    let key = identifier(key, dialect);
    let mut sql = String::new();
    for change in changes {
        match change {
            RowChange::Delete(value) => writeln!(
                sql,
                "DELETE FROM {table} WHERE {key} = {};",
                literal(value, dialect)
            )?,
            RowChange::Upsert(doc) => {
                let columns = doc
                    .keys()
                    .map(|c| identifier(c, dialect))
                    .collect::<Vec<_>>();
                let values = doc
                    .values()
                    .map(|v| literal(v, dialect))
                    .collect::<Vec<_>>();
                let update = columns
                    .iter()
                    .map(|c| {
                        if dialect == Dialect::Mysql {
                            format!("{c} = VALUES({c})")
                        } else {
                            format!("{c} = EXCLUDED.{c}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let conflict = if dialect == Dialect::Mysql {
                    format!("ON DUPLICATE KEY UPDATE {update}")
                } else {
                    format!("ON CONFLICT ({key}) DO UPDATE SET {update}")
                };
                writeln!(
                    sql,
                    "INSERT INTO {table} ({}) VALUES ({}) {conflict};",
                    columns.join(", "),
                    values.join(", ")
                )?;
            }
        }
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn test_doc(value: &Value) -> BTreeMap<String, Cell> {
        value
            .as_object()
            .expect("document")
            .iter()
            .map(|(key, value)| {
                let cell = match value {
                    Value::Null => Cell::Null,
                    Value::Bool(v) => Cell::Bool(*v),
                    Value::Number(v) => Cell::Number(v.to_string()),
                    Value::String(v) => Cell::String(v.clone()),
                    _ => panic!("test documents contain only scalars"),
                };
                (key.clone(), cell)
            })
            .collect()
    }

    fn json_text(value: &Cell) -> String {
        serde_json::to_string(value).expect("JSON scalar")
    }

    #[test]
    fn row_image_json_keeps_numeric_text_and_distinguishes_strings() {
        let changes = vec![RowChange::Upsert(BTreeMap::from([
            (
                "decimal".into(),
                json_value(Some("12345678901234567890.123456789"), "numeric").expect("decimal"),
            ),
            (
                "integer".into(),
                json_value(Some("18446744073709551615"), "numeric").expect("integer"),
            ),
            ("text".into(), Cell::String("18446744073709551615".into())),
            ("null".into(), Cell::Null),
        ]))];
        assert_eq!(
            serde_json::to_string(&changes).expect("row images"),
            r#"[{"doc":{"decimal":12345678901234567890.123456789,"integer":18446744073709551615,"null":null,"text":"18446744073709551615"}}]"#
        );
    }

    #[test]
    fn returning_rewrite_and_statement_order() {
        let statements = statements(&["INSERT INTO t VALUES (1, 'a;b'); UPDATE t SET s = 'x' WHERE n <= 100; -- end\nDELETE FROM t WHERE id = 2;".into()], "id").expect("valid DML");
        assert_eq!(
            statements
                .iter()
                .map(|s| (s.table.as_str(), s.sql.as_str(), s.delete))
                .collect::<Vec<_>>(),
            vec![
                ("t", "INSERT INTO t VALUES (1, 'a;b') RETURNING *", false),
                (
                    "t",
                    "UPDATE t SET s = 'x' WHERE n <= 100 RETURNING *",
                    false
                ),
                ("t", "DELETE FROM t WHERE id = 2 RETURNING *", true),
            ]
        );
        let quoted = statements_for("INSERT INTO public.\"My Table\" VALUES (1)");
        assert_eq!(quoted[0].table, "public.\"My Table\"");
        assert_eq!(
            table_parts(&quoted[0].table).expect("identifiers"),
            vec!["public", "My Table"]
        );
        for (invalid, expected) in [
            (
                "SELECT * FROM t",
                "write accepts only INSERT, UPDATE and DELETE statements",
            ),
            (
                "UPDATE t SET id = id + 1",
                "write cannot update key column 'id'; delete then insert instead",
            ),
            (
                "UPDATE t SET (id, n) = (2, 3)",
                "write cannot update key column 'id'; delete then insert instead",
            ),
            (
                "DELETE FROM t RETURNING id",
                "write adds RETURNING *; omit an explicit RETURNING clause",
            ),
            (
                "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2",
                "write cannot update key column 'id'; delete then insert instead",
            ),
            ("", "write needs at least one statement"),
        ] {
            assert_eq!(
                super::statements(&[invalid.into()], "id")
                    .expect_err("unsupported DML")
                    .to_string(),
                expected
            );
        }
    }

    fn statements_for(sql: &str) -> Vec<WriteStatement> {
        statements(&[sql.into()], "id").expect("valid DML")
    }

    #[test]
    fn image_changes_keep_null_empty_and_key_types() {
        let images = TextResult {
            columns: vec!["id".into(), "s".into(), "b".into()],
            rows: vec![
                vec![Some("42".into()), None, Some("t".into())],
                vec![Some("43".into()), Some(String::new()), Some("f".into())],
            ],
        };
        let types = vec![
            ("id".into(), "int8".into()),
            ("s".into(), "text".into()),
            ("b".into(), "bool".into()),
        ];
        assert_eq!(
            row_changes(&images, &types, "id", false).expect("images"),
            vec![
                RowChange::Upsert(test_doc(&json!({"id":42,"s":null,"b":true}))),
                RowChange::Upsert(test_doc(&json!({"id":43,"s":"","b":false})))
            ]
        );
        assert_eq!(
            row_changes(&images, &types, "id", true).expect("deletes"),
            vec![
                RowChange::Delete(Cell::Number("42".into())),
                RowChange::Delete(Cell::Number("43".into()))
            ]
        );
        assert_eq!(
            row_changes(&images, &types, "s", false)
                .expect_err("NULL key")
                .to_string(),
            "row image has no non-NULL key 's'"
        );
        assert_eq!(
            row_changes(&images, &types, "absent", false)
                .expect_err("missing key")
                .to_string(),
            "row image has no non-NULL key 'absent'"
        );
        assert_eq!(
            row_changes(&images, &[], "id", false)
                .expect_err("missing type")
                .to_string(),
            "no mirror type for column 'id'"
        );
        let empty = TextResult {
            columns: images.columns,
            rows: vec![],
        };
        assert_eq!(
            row_changes(&empty, &types, "id", false).expect("empty images"),
            vec![]
        );
    }

    #[test]
    fn json_types_are_exact_and_invalid_numbers_fail() {
        for kind in ["int2", "int4", "int8", "numeric", "float4", "float8"] {
            assert_eq!(
                json_value(Some("123"), kind).expect("number"),
                Cell::Number("123".into())
            );
        }
        assert_eq!(
            json_text(&json_value(Some("9223372036854775807"), "int8").expect("integer")),
            "9223372036854775807"
        );
        assert_eq!(
            json_text(
                &json_value(Some("12345678901234567890.123456789"), "numeric").expect("decimal")
            ),
            "12345678901234567890.123456789"
        );
        assert_eq!(json_value(None, "numeric").expect("null"), Cell::Null);
        assert_eq!(
            json_value(Some(""), "text").expect("empty"),
            Cell::String(String::new())
        );
        assert_eq!(
            json_value(Some("123"), "text").expect("string"),
            Cell::String("123".into())
        );
        assert_eq!(
            json_value(Some("true"), "bool").expect("boolean"),
            Cell::Bool(true)
        );
        assert_eq!(
            json_value(Some("f"), "bool").expect("boolean"),
            Cell::Bool(false)
        );
        assert_eq!(
            json_value(Some("NaN"), "numeric")
                .expect_err("non-finite number")
                .to_string(),
            "'NaN' of type numeric is not a finite JSON number"
        );
        assert_eq!(
            json_value(Some("Infinity"), "float8")
                .expect_err("non-finite number")
                .to_string(),
            "'Infinity' of type float8 is not a finite JSON number"
        );
        assert_eq!(
            json_value(Some("yes"), "bool")
                .expect_err("invalid boolean")
                .to_string(),
            "invalid boolean 'yes'"
        );
    }

    #[test]
    fn dialect_upserts_and_deletes_are_exact() {
        let doc = test_doc(&json!({"id": 1, "s": "a'\\b", "v": null, "b": true}));
        let changes = vec![
            RowChange::Upsert(doc),
            RowChange::Delete(Cell::Number("2".into())),
        ];
        assert_eq!(
            sql_changes("t", "id", &changes, Dialect::Postgres).expect("SQL"),
            "INSERT INTO \"t\" (\"b\", \"id\", \"s\", \"v\") VALUES (true, 1, E'a''\\\\b', NULL) ON CONFLICT (\"id\") DO UPDATE SET \"b\" = EXCLUDED.\"b\", \"id\" = EXCLUDED.\"id\", \"s\" = EXCLUDED.\"s\", \"v\" = EXCLUDED.\"v\";\nDELETE FROM \"t\" WHERE \"id\" = 2;\n"
        );
        assert_eq!(
            sql_changes("t", "id", &changes, Dialect::Mysql).expect("SQL"),
            "INSERT INTO `t` (`b`, `id`, `s`, `v`) VALUES (1, 1, 'a''\\\\b', NULL) ON DUPLICATE KEY UPDATE `b` = VALUES(`b`), `id` = VALUES(`id`), `s` = VALUES(`s`), `v` = VALUES(`v`);\nDELETE FROM `t` WHERE `id` = 2;\n"
        );
        assert_eq!(
            sql_changes(
                "\"Odd.Table\"",
                "k",
                &[RowChange::Delete(Cell::String("x'y".into()))],
                Dialect::Postgres
            )
            .expect("quoted"),
            "DELETE FROM \"Odd.Table\" WHERE \"k\" = E'x''y';\n"
        );
    }
}
