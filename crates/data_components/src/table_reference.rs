/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    sql::{
        sqlparser::{ast::Ident, dialect::GenericDialect, parser::Parser},
        TableReference,
    },
};

const UNIT_SEPARATOR: &str = "\u{001F}";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiPartTableReference {
    TableReference(TableReference),
    Multi(Vec<Arc<str>>),
}

impl From<TableReference> for MultiPartTableReference {
    fn from(table_reference: TableReference) -> Self {
        // If the table reference is a Bare reference, split it on the unit separator.
        // If the number of parts after splitting is greater than 3, return a MultiPartTableReference, otherwise return a TableReference.
        match &table_reference {
            TableReference::Bare { table } => {
                // Check if the table name contains the unit separator first to optimize the common case.
                if table.contains(UNIT_SEPARATOR) {
                    let parts = table.split(UNIT_SEPARATOR).collect::<Vec<&str>>();
                    if parts.len() > 3 {
                        Self::Multi(parts.into_iter().map(Arc::from).collect())
                    } else {
                        Self::TableReference(table_reference)
                    }
                } else {
                    Self::TableReference(table_reference)
                }
            }
            _ => Self::TableReference(table_reference),
        }
    }
}

impl MultiPartTableReference {
    /// Encode a multi-part table reference into a `TableReference`.
    ///
    /// # Panics
    ///
    /// This function will panic if the number of parts is less than 4.
    #[must_use]
    pub fn encode_multi_part_table_reference(parts: &[Arc<str>]) -> TableReference {
        assert!(
            parts.len() > 3,
            "Multi-part table references must have at least 4 parts, got {}",
            parts.len()
        );
        TableReference::Bare {
            table: parts.join(UNIT_SEPARATOR).into(),
        }
    }

    /// Convert a `MultiPartTableReference` to a quoted string.
    ///
    /// Example:
    ///
    /// ```
    /// let parts = vec![Arc::from("a"), Arc::from("b"), Arc::from("c"), Arc::from("d")];
    /// let multi_part_table_reference = MultiPartTableReference::encode_multi_part_table_reference(&parts);
    /// assert_eq!(multi_part_table_reference.to_quoted_string(), r#""a"."b"."c"."d""#);
    /// ```
    #[must_use]
    pub fn to_quoted_string(&self) -> String {
        match self {
            MultiPartTableReference::TableReference(table_reference) => match table_reference {
                // The `TableReference` will sometimes not quote the table name, even if we ask it to because it detects that it would be safe (within DataFusion).
                // Unfortunately, some systems have reserved keywords that will error if we don't quote them.
                // Err on the safe side and always quote the table name.
                TableReference::Bare { table } => quote_identifier(table),
                TableReference::Partial { schema, table } => {
                    format!("{}.{}", quote_identifier(schema), quote_identifier(table))
                }
                TableReference::Full {
                    catalog,
                    schema,
                    table,
                } => format!(
                    "{}.{}.{}",
                    quote_identifier(catalog),
                    quote_identifier(schema),
                    quote_identifier(table)
                ),
            },
            MultiPartTableReference::Multi(parts) => parts
                .iter()
                .map(|p| quote_identifier(p))
                .collect::<Vec<_>>()
                .join("."),
        }
    }
}

impl std::fmt::Display for MultiPartTableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiPartTableReference::TableReference(table_reference) => {
                write!(f, "{table_reference}")
            }
            MultiPartTableReference::Multi(parts) => {
                write!(f, "{}", parts.join("."))
            }
        }
    }
}

impl TryFrom<MultiPartTableReference> for TableReference {
    type Error = DataFusionError;

    fn try_from(value: MultiPartTableReference) -> Result<Self, Self::Error> {
        match value {
            MultiPartTableReference::TableReference(table_reference) => Ok(table_reference),
            MultiPartTableReference::Multi(_) => Err(DataFusionError::External(
                "MultiPartTableReference cannot be converted to TableReference".into(),
            )),
        }
    }
}

/// Parses a dataset path string into a `TableReference`, handling quoted identifiers and multi-part paths.
/// The path can contain 1-3 parts separated by periods (e.g. "table", "schema.table", or "catalog.schema.table").
/// Parts can be quoted with double quotes to include periods or other special characters.
#[must_use]
pub fn parse_multi_part_table_reference(s: &str) -> TableReference {
    let mut parts = parse_identifiers_normalized(s, false);

    match parts.len() {
        1 => TableReference::Bare {
            table: parts.remove(0).into(),
        },
        2 => TableReference::Partial {
            schema: parts.remove(0).into(),
            table: parts.remove(0).into(),
        },
        3 => TableReference::Full {
            catalog: parts.remove(0).into(),
            schema: parts.remove(0).into(),
            table: parts.remove(0).into(),
        },
        _ => TableReference::Bare {
            table: parts.join(UNIT_SEPARATOR).into(),
        },
    }
}

/// Wraps identifier string in double quotes, escaping any double quotes in
/// the identifier by replacing it with two double quotes
///
/// e.g. identifier `tab.le"name` becomes `"tab.le""name"`
#[must_use]
pub fn quote_identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

pub(crate) fn parse_identifiers(s: &str) -> DataFusionResult<Vec<Ident>> {
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(s)?;
    let idents = parser.parse_multipart_identifier()?;
    Ok(idents)
}

pub(crate) fn parse_identifiers_normalized(s: &str, ignore_case: bool) -> Vec<String> {
    parse_identifiers(s)
        .unwrap_or_default()
        .into_iter()
        .map(|id| match id.quote_style {
            Some(_) => id.value,
            None if ignore_case => id.value,
            _ => id.value.to_ascii_lowercase(),
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_table_reference_bare() {
        let table_ref = TableReference::Bare {
            table: "simple".into(),
        };
        let multi = MultiPartTableReference::from(table_ref);

        match multi {
            MultiPartTableReference::TableReference(TableReference::Bare { table }) => {
                assert_eq!(table, "simple".into());
            }
            _ => panic!("Expected TableReference::Bare"),
        }
    }

    #[test]
    fn test_from_table_reference_multi() {
        let table_ref = TableReference::Bare {
            table: Arc::from(format!(
                "a{UNIT_SEPARATOR}b{UNIT_SEPARATOR}c{UNIT_SEPARATOR}d",
            )),
        };
        let multi = MultiPartTableReference::from(table_ref);

        match multi {
            MultiPartTableReference::Multi(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[0], Arc::from("a"));
                assert_eq!(parts[1], Arc::from("b"));
                assert_eq!(parts[2], Arc::from("c"));
                assert_eq!(parts[3], Arc::from("d"));
            }
            MultiPartTableReference::TableReference(_) => {
                panic!("Expected MultiPartTableReference::Multi")
            }
        }
    }

    #[test]
    fn test_encode_multi_part_table_reference() {
        let parts = vec![
            Arc::from("part1"),
            Arc::from("part2"),
            Arc::from("part3"),
            Arc::from("part4"),
        ];

        let table_ref = MultiPartTableReference::encode_multi_part_table_reference(&parts);

        match table_ref {
            TableReference::Bare { table } => {
                let expected = format!(
                    "part1{UNIT_SEPARATOR}part2{UNIT_SEPARATOR}part3{UNIT_SEPARATOR}part4",
                );
                assert_eq!(table, Arc::from(expected));
            }
            _ => panic!("Expected TableReference::Bare"),
        }
    }

    #[test]
    #[should_panic(expected = "Multi-part table references must have at least 4 parts")]
    fn test_encode_multi_part_table_reference_panic() {
        let parts = vec![Arc::from("part1"), Arc::from("part2"), Arc::from("part3")];
        let _ = MultiPartTableReference::encode_multi_part_table_reference(&parts);
    }

    #[test]
    fn test_to_quoted_string_table_reference() {
        let table_ref = TableReference::Bare {
            table: "simple".into(),
        };
        let multi = MultiPartTableReference::TableReference(table_ref);
        assert_eq!(multi.to_quoted_string(), r#""simple""#);
    }

    #[test]
    fn test_to_quoted_string_multi() {
        let parts = vec![
            Arc::from("a"),
            Arc::from("b"),
            Arc::from("c"),
            Arc::from("d"),
        ];
        let multi = MultiPartTableReference::Multi(parts);
        assert_eq!(multi.to_quoted_string(), r#""a"."b"."c"."d""#);
    }

    #[test]
    fn test_quote_identifier() {
        // Test basic identifier
        assert_eq!(quote_identifier("simple"), r#""simple""#);

        // Test identifier with dots
        assert_eq!(quote_identifier("table.name"), r#""table.name""#);

        // Test identifier with quotes
        assert_eq!(quote_identifier(r#"table"name"#), r#""table""name""#);

        // Test identifier with both dots and quotes
        assert_eq!(quote_identifier(r#"my.table"name"#), r#""my.table""name""#);
    }

    #[test]
    fn test_to_quoted_string_partial_reference() {
        let table_ref = TableReference::Partial {
            schema: "my.schema".into(),
            table: "table.name".into(),
        };
        let multi = MultiPartTableReference::TableReference(table_ref);
        assert_eq!(multi.to_quoted_string(), r#""my.schema"."table.name""#);
    }

    #[test]
    fn test_to_quoted_string_full_reference() {
        let table_ref = TableReference::Full {
            catalog: "my.catalog".into(),
            schema: "my.schema".into(),
            table: "table.name".into(),
        };
        let multi = MultiPartTableReference::TableReference(table_ref);
        assert_eq!(
            multi.to_quoted_string(),
            r#""my.catalog"."my.schema"."table.name""#
        );
    }

    #[test]
    fn test_to_quoted_string_with_quotes() {
        let table_ref = TableReference::Bare {
            table: r#"my"table"#.into(),
        };
        let multi = MultiPartTableReference::TableReference(table_ref);
        assert_eq!(multi.to_quoted_string(), r#""my""table""#);
    }

    #[test]
    fn test_to_quoted_string_multi_with_special_chars() {
        let parts = vec![
            Arc::from("my.catalog"),
            Arc::from(r#"special"schema"#),
            Arc::from("table.name"),
            Arc::from(r#"part"4"#),
        ];
        let multi = MultiPartTableReference::Multi(parts);
        assert_eq!(
            multi.to_quoted_string(),
            r#""my.catalog"."special""schema"."table.name"."part""4""#
        );
    }

    #[test]
    fn test_parse_identifiers() -> DataFusionResult<()> {
        // Test simple identifiers
        let idents = parse_identifiers("table")?;
        assert_eq!(idents.len(), 1);
        assert_eq!(idents[0].value, "table");
        assert!(idents[0].quote_style.is_none());

        // Test multi-part identifiers
        let idents = parse_identifiers("schema.table")?;
        assert_eq!(idents.len(), 2);
        assert_eq!(idents[0].value, "schema");
        assert_eq!(idents[1].value, "table");

        // Test quoted identifiers
        let idents = parse_identifiers(r#""My.Schema"."Table.Name""#)?;
        assert_eq!(idents.len(), 2);
        assert_eq!(idents[0].value, "My.Schema");
        assert_eq!(idents[1].value, "Table.Name");
        assert!(idents[0].quote_style.is_some());
        assert!(idents[1].quote_style.is_some());

        // Test mixed quoted and unquoted
        let idents = parse_identifiers(r#"catalog."schema.name".table"#)?;
        assert_eq!(idents.len(), 3);
        assert_eq!(idents[0].value, "catalog");
        assert_eq!(idents[1].value, "schema.name");
        assert_eq!(idents[2].value, "table");
        assert!(idents[0].quote_style.is_none());
        assert!(idents[1].quote_style.is_some());
        assert!(idents[2].quote_style.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_identifiers_normalized() {
        // Test case-sensitive (ignore_case = false)
        let parts = parse_identifiers_normalized("MyTable", false);
        assert_eq!(parts, vec!["mytable"]);

        let parts = parse_identifiers_normalized(r#""MyTable""#, false);
        assert_eq!(parts, vec!["MyTable"]);

        // Test case-insensitive (ignore_case = true)
        let parts = parse_identifiers_normalized("MyTable", true);
        assert_eq!(parts, vec!["MyTable"]);

        // Test multi-part identifiers
        let parts = parse_identifiers_normalized("Schema.MyTable", false);
        assert_eq!(parts, vec!["schema", "mytable"]);

        // Test quoted identifiers with special characters
        let parts = parse_identifiers_normalized(r#""My.Schema"."Table.Name""#, false);
        assert_eq!(parts, vec!["My.Schema", "Table.Name"]);

        // Test invalid SQL (should return empty vec)
        let parts = parse_identifiers_normalized("invalid..sql", false);
        assert!(parts.is_empty());
    }

    #[test]
    fn test_parse_multi_part_table_reference() {
        // Test single part
        let table_ref = parse_multi_part_table_reference("table");
        assert!(matches!(
            table_ref,
            TableReference::Bare { table } if table == "table".into()
        ));

        // Test two parts
        let table_ref = parse_multi_part_table_reference("schema.table");
        assert!(matches!(
            table_ref,
            TableReference::Partial { schema, table }
            if schema == "schema".into() && table == "table".into()
        ));

        // Test three parts
        let table_ref = parse_multi_part_table_reference("catalog.schema.table");
        assert!(matches!(
            table_ref,
            TableReference::Full { catalog, schema, table }
            if catalog == "catalog".into() && schema == "schema".into() && table == "table".into()
        ));

        // Test quoted identifiers
        let table_ref = parse_multi_part_table_reference(r#""My.Catalog"."Schema"."Table""#);
        assert!(matches!(
            table_ref,
            TableReference::Full { catalog, schema, table }
            if catalog == "My.Catalog".into() && schema == "Schema".into() && table == "Table".into()
        ));

        // Test more than three parts (should join with UNIT_SEPARATOR)
        let table_ref = parse_multi_part_table_reference("a.b.c.d");
        assert!(matches!(
            table_ref,
            TableReference::Bare { table }
            if table == Arc::from(format!("a{UNIT_SEPARATOR}b{UNIT_SEPARATOR}c{UNIT_SEPARATOR}d"))
        ));
    }
}
