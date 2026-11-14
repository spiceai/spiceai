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

//! Compact descriptions of datasets for a model: SQL type names, the `table_schema` listing, and
//! the relevance of a dataset to a search.

use std::{borrow::Cow, fmt::Write};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::common::utils::quote_identifier;

/// `name` as a SQL query writes it: as it is when it is a lowercase identifier, which SQL
/// leaves unchanged, and otherwise in double quotes, with each `"` doubled. A name with
/// capitals, spaces, commas or `--` is therefore one unambiguous token in a listing, and the
/// text a model copies into a query resolves to the column it read.
#[must_use]
pub fn sql_identifier(name: &str) -> Cow<'_, str> {
    if name.is_empty() {
        Cow::Borrowed("\"\"")
    } else {
        quote_identifier(name)
    }
}

/// The SQL type a model writes for `data_type`, as `DataFusion` names it, with a decimal's
/// precision and scale and a list's element type.
#[must_use]
pub fn sql_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        DataType::UInt32 => "INT UNSIGNED".to_string(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        DataType::Float16 | DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::Duration(unit) => format!(
            "DURATION({})",
            match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            }
        ),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BYTEA".to_string(),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => format!("{}[]", sql_type_name(item.data_type())),
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|f| format!(
                    "{} {}",
                    sql_identifier(f.name()),
                    sql_type_name(f.data_type())
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP<{}, {}>",
                sql_type_name(kv[0].data_type()),
                sql_type_name(kv[1].data_type())
            ),
            other => format!("MAP<{}>", sql_type_name(other)),
        },
        DataType::Dictionary(_, value) => sql_type_name(value),
        DataType::RunEndEncoded(_, values) => sql_type_name(values.data_type()),
        DataType::Union(_, _) => "UNION".to_string(),
    }
}

/// The description a column carries: a Spicepod `description`, or a connector's `comment`.
#[must_use]
pub fn field_description(field: &Field) -> Option<&str> {
    let metadata = field.metadata();
    metadata
        .get("description")
        .or_else(|| metadata.get("comment"))
        .map(String::as_str)
        .filter(|d| !d.trim().is_empty())
}

/// The words of `text`, lowercased, split at anything that is not a letter or digit, so
/// `store_sales` and `StoreSales` both give `store` and `sales`, and camel case splits too.
#[must_use]
pub fn search_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut previous_lower = false;
    for c in text.chars() {
        if c.is_alphanumeric() {
            // `storeSales` is two words.
            if c.is_uppercase() && previous_lower && !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            previous_lower = c.is_lowercase() || c.is_numeric();
            word.extend(c.to_lowercase());
        } else {
            previous_lower = false;
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
        }
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

/// What a dataset offers a search to match: its name, description, and columns.
pub struct Searchable<'a> {
    pub name: &'a str,
    pub description: Option<&'a str>,
    /// Each column's name and description.
    pub columns: Vec<(&'a str, Option<&'a str>)>,
}

/// How well `dataset` matches `query`: for each word of the query, 4 when a word of the name
/// starts with it, 2 when a column name's does, and 1 when the description's or a column
/// description's does. 0 is no match. A word of the query counts once, at its best place.
#[must_use]
pub fn search_score(query_words: &[String], dataset: &Searchable<'_>) -> u32 {
    let name_words = search_words(dataset.name);
    let description_words = dataset.description.map(search_words).unwrap_or_default();
    let column_name_words: Vec<String> = dataset
        .columns
        .iter()
        .flat_map(|(name, _)| search_words(name))
        .collect();
    let column_description_words: Vec<String> = dataset
        .columns
        .iter()
        .filter_map(|(_, description)| *description)
        .flat_map(search_words)
        .collect();
    let has = |words: &[String], query: &str| words.iter().any(|w| w.starts_with(query));
    query_words
        .iter()
        .map(|query| {
            if has(&name_words, query) {
                4
            } else if has(&column_name_words, query) {
                2
            } else {
                u32::from(has(&description_words, query) || has(&column_description_words, query))
            }
        })
        .sum()
}

/// How much of a table [`write_table_listing`] describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingDetail {
    /// Column names, types, nullability, descriptions, and keys.
    Full,
    /// Column names, types, and nullability.
    Minimal,
}

/// A table as [`write_table_listing`] describes it.
pub struct TableListing<'a> {
    /// The table's full name, as a model writes it in SQL.
    pub table: &'a str,
    pub description: Option<&'a str>,
    pub schema: &'a Schema,
    /// The columns of the primary key, in key order.
    pub primary_key: Vec<String>,
    /// The columns of each unique constraint.
    pub unique: Vec<Vec<String>>,
}

/// The columns of a key, each as [`sql_identifier`] writes it, separated by `, `.
fn key_columns(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One line of text: newlines and runs of whitespace become single spaces.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Appends `listing` to `out`:
///
/// ```text
/// spice.tpcds.store_sales: TPC-DS store_sales table
///   ss_item_sk BIGINT NOT NULL
///   ss_net_paid DECIMAL(7,2) -- Net paid
///   primary key (ss_item_sk, ss_ticket_number)
/// ```
///
/// Column names, in the column lines, a struct's fields and the keys, are written as
/// [`sql_identifier`] writes them.
///
/// With `columns`, only the columns it names are listed, and a last line says how many others
/// the table has, so that a model never takes a filtered listing for the whole table.
pub fn write_table_listing(
    out: &mut String,
    listing: &TableListing<'_>,
    detail: ListingDetail,
    columns: Option<&[usize]>,
) {
    out.push_str(listing.table);
    if detail == ListingDetail::Full
        && let Some(description) = listing.description.filter(|d| !d.trim().is_empty())
    {
        out.push_str(": ");
        out.push_str(&one_line(description));
    }
    out.push('\n');

    let fields = listing.schema.fields();
    let shown: Vec<usize> = match columns {
        Some(indices) => indices.to_vec(),
        None => (0..fields.len()).collect(),
    };
    for &index in &shown {
        let Some(field) = fields.get(index) else {
            continue;
        };
        let _ = write!(
            out,
            "  {} {}",
            sql_identifier(field.name()),
            sql_type_name(field.data_type())
        );
        if !field.is_nullable() {
            out.push_str(" NOT NULL");
        }
        if detail == ListingDetail::Full
            && let Some(description) = field_description(field)
        {
            out.push_str(" -- ");
            out.push_str(&one_line(description));
        }
        out.push('\n');
    }
    let others = fields.len().saturating_sub(shown.len());
    if others > 0 {
        let _ = writeln!(
            out,
            "  ({others} other column{} not shown)",
            if others == 1 { "" } else { "s" }
        );
    }
    if detail == ListingDetail::Full {
        if !listing.primary_key.is_empty() {
            let _ = writeln!(out, "  primary key ({})", key_columns(&listing.primary_key));
        }
        for unique in &listing.unique {
            let _ = writeln!(out, "  unique ({})", key_columns(unique));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::Fields;

    use super::*;

    #[test]
    fn sql_type_names_carry_what_a_query_needs() {
        let cases = [
            (DataType::Int64, "BIGINT"),
            (DataType::Decimal128(7, 2), "DECIMAL(7,2)"),
            (DataType::Utf8View, "VARCHAR"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "TIMESTAMP WITH TIME ZONE",
            ),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), "TIMESTAMP"),
            (
                DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
                "REAL[]",
            ),
            (
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ])),
                "STRUCT<a INT, b VARCHAR>",
            ),
            (
                DataType::Struct(Fields::from(vec![
                    Field::new("Zip Code", DataType::Utf8, true),
                    Field::new("zip", DataType::Utf8, true),
                ])),
                "STRUCT<\"Zip Code\" VARCHAR, zip VARCHAR>",
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                "VARCHAR",
            ),
        ];
        for (data_type, expected) in cases {
            assert_eq!(sql_type_name(&data_type), expected, "{data_type}");
        }
    }

    #[test]
    fn search_words_split_names_the_way_people_write_them() {
        assert_eq!(
            search_words("spice.tpcds.store_sales"),
            ["spice", "tpcds", "store", "sales"]
        );
        assert_eq!(search_words("StoreSales2024"), ["store", "sales2024"]);
        assert_eq!(search_words("  Net-Paid  "), ["net", "paid"]);
        assert!(search_words(" .,_ ").is_empty());
    }

    #[test]
    fn a_name_match_outranks_a_column_match_which_outranks_a_description_match() {
        let query = search_words("sales");
        let score = |name, description, column| {
            search_score(
                &query,
                &Searchable {
                    name,
                    description,
                    columns: vec![(column, None)],
                },
            )
        };
        assert_eq!(score("spice.public.store_sales", None, "id"), 4);
        assert_eq!(score("spice.public.orders", None, "sales_rep"), 2);
        assert_eq!(
            score("spice.public.orders", Some("All sales orders"), "id"),
            1
        );
        assert_eq!(score("spice.public.orders", Some("Shipments"), "id"), 0);
        // Words match by prefix: `sale` finds `sales`.
        assert_eq!(
            search_score(
                &search_words("sale"),
                &Searchable {
                    name: "store_sales",
                    description: None,
                    columns: vec![],
                },
            ),
            4
        );
    }

    fn listing_schema() -> Schema {
        let net_paid = Field::new("ss_net_paid", DataType::Decimal128(7, 2), true).with_metadata(
            HashMap::from([("description".to_string(), "Net\npaid".to_string())]),
        );
        Schema::new(vec![
            Field::new("ss_item_sk", DataType::Int64, false),
            Field::new("ss_ticket_number", DataType::Int64, false),
            net_paid,
        ])
    }

    #[test]
    fn a_full_listing_has_types_descriptions_and_keys() {
        let schema = listing_schema();
        let mut out = String::new();
        write_table_listing(
            &mut out,
            &TableListing {
                table: "spice.tpcds.store_sales",
                description: Some("TPC-DS store_sales table"),
                schema: &schema,
                primary_key: vec!["ss_item_sk".to_string(), "ss_ticket_number".to_string()],
                unique: vec![vec!["ss_ticket_number".to_string()]],
            },
            ListingDetail::Full,
            None,
        );
        assert_eq!(
            out,
            "spice.tpcds.store_sales: TPC-DS store_sales table\n  ss_item_sk BIGINT NOT NULL\n  ss_ticket_number BIGINT NOT NULL\n  ss_net_paid DECIMAL(7,2) -- Net paid\n  primary key (ss_item_sk, ss_ticket_number)\n  unique (ss_ticket_number)\n"
        );
    }

    #[test]
    fn identifiers_are_written_as_sql_writes_them() {
        assert_eq!(sql_identifier("ss_item_sk"), "ss_item_sk");
        assert_eq!(sql_identifier("_c0"), "_c0");
        assert_eq!(sql_identifier("Price"), "\"Price\"");
        assert_eq!(sql_identifier("2024_total"), "\"2024_total\"");
        assert_eq!(sql_identifier("a, b"), "\"a, b\"");
        assert_eq!(sql_identifier("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(sql_identifier(""), "\"\"");
    }

    /// Unquoted, the key over the one column `a, b` read `primary key (a, b)`, the same as a
    /// key over the columns `a` and `b`, and the column `x -- y` read as the column `x` with
    /// the description `y`.
    #[test]
    fn names_that_need_quotes_are_listed_unambiguously() {
        let schema = Schema::new(vec![
            Field::new("a, b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("x -- y", DataType::Utf8, true),
            Field::new("Price", DataType::Float64, true).with_metadata(HashMap::from([(
                "description".to_string(),
                "List price".to_string(),
            )])),
            Field::new("say \"hi\"", DataType::Utf8, true),
            Field::new("", DataType::Int32, true),
        ]);
        let listing = |primary_key: Vec<&str>| {
            let mut out = String::new();
            write_table_listing(
                &mut out,
                &TableListing {
                    table: "spice.public.t",
                    description: None,
                    schema: &schema,
                    primary_key: primary_key.into_iter().map(str::to_string).collect(),
                    unique: vec![vec!["Price".to_string(), "a".to_string()]],
                },
                ListingDetail::Full,
                None,
            );
            out
        };
        let one_column_key = listing(vec!["a, b"]);
        assert_eq!(
            one_column_key,
            "spice.public.t\n  \"a, b\" INT NOT NULL\n  a INT NOT NULL\n  b INT NOT NULL\n  \"x -- y\" VARCHAR\n  \"Price\" DOUBLE -- List price\n  \"say \"\"hi\"\"\" VARCHAR\n  \"\" INT\n  primary key (\"a, b\")\n  unique (\"Price\", a)\n"
        );
        let two_column_key = listing(vec!["a", "b"]);
        assert!(two_column_key.ends_with("  primary key (a, b)\n  unique (\"Price\", a)\n"));
        assert_ne!(one_column_key, two_column_key);
    }

    #[test]
    fn a_minimal_filtered_listing_counts_the_columns_it_leaves_out() {
        let schema = listing_schema();
        let mut out = String::new();
        write_table_listing(
            &mut out,
            &TableListing {
                table: "spice.tpcds.store_sales",
                description: Some("TPC-DS store_sales table"),
                schema: &schema,
                primary_key: vec!["ss_item_sk".to_string()],
                unique: vec![],
            },
            ListingDetail::Minimal,
            Some(&[2]),
        );
        assert_eq!(
            out,
            "spice.tpcds.store_sales\n  ss_net_paid DECIMAL(7,2)\n  (2 other columns not shown)\n"
        );
    }
}
