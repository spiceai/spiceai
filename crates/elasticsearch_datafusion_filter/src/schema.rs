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

//! Which columns are filterable in Elasticsearch, and how their values map to
//! the query DSL.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Schema};

/// How a single column is represented in the Elasticsearch mapping. Determines both whether a
/// SQL predicate can be pushed down and whether the pushdown is exact.
///
/// Only fields that are actually indexed in Elasticsearch (`index: true`) may appear here — a
/// filter on a non-indexed field would make Elasticsearch reject or mis-answer the query, so
/// such columns must be omitted from the [`EsFilterSchema`] entirely (leaving `DataFusion` to
/// filter them above the scan).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EsFieldType {
    /// ES `boolean`. `term` is exact.
    Boolean,
    /// ES `byte`/`short`/`integer`/`long` (any exactly-representable integer). `term`/`range`
    /// are exact.
    Integer,
    /// ES `float`/`double`/`half_float`/`scaled_float`. Comparisons are pushed but marked
    /// inexact: the JSON round-trip and the SQL literal may not share Elasticsearch's binary
    /// representation, so `DataFusion` re-checks.
    Float,
    /// ES `keyword`: a whole-value, non-analyzed string. `term`/`range`/`prefix` match the
    /// stored value exactly.
    Keyword,
    /// An analyzed `text` field carrying a non-analyzed `keyword` sub-field (the mapping Spice
    /// writes for string columns, e.g. `col.keyword`). Equality/prefix are pushed against the
    /// sub-field but marked inexact: `ignore_above` drops long values from the sub-field and
    /// analysis/collation can differ, so `DataFusion` re-checks.
    TextWithKeyword { keyword_subfield: String },
}

impl EsFieldType {
    /// The Elasticsearch field name to target for exact-value predicates (`term`, `terms`,
    /// `range`, `prefix`). For a `text` field this is its `keyword` sub-field.
    #[must_use]
    pub fn value_field(&self, column: &str) -> String {
        match self {
            EsFieldType::TextWithKeyword { keyword_subfield } => {
                format!("{column}.{keyword_subfield}")
            }
            _ => column.to_string(),
        }
    }

    /// Whether an equality/`IN`/`range` predicate on this field type is exact (`true`) or a
    /// superset that `DataFusion` must re-check (`false`).
    #[must_use]
    pub fn is_exact_for_value_match(&self) -> bool {
        match self {
            EsFieldType::Boolean | EsFieldType::Integer | EsFieldType::Keyword => true,
            EsFieldType::Float | EsFieldType::TextWithKeyword { .. } => false,
        }
    }

    /// Whether a prefix (`LIKE 'x%'`) predicate is expressible against this field type.
    #[must_use]
    pub fn supports_prefix(&self) -> bool {
        matches!(
            self,
            EsFieldType::Keyword | EsFieldType::TextWithKeyword { .. }
        )
    }
}

/// The set of columns that may be filtered in Elasticsearch, keyed by column name.
///
/// A column absent from this map is treated as non-pushable: [`crate::classify_filter`] returns
/// [`datafusion::logical_expr::TableProviderFilterPushDown::Unsupported`] for any predicate that
/// references it.
#[derive(Debug, Clone, Default)]
pub struct EsFilterSchema {
    fields: HashMap<String, EsFieldType>,
}

impl EsFilterSchema {
    #[must_use]
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Register a single column with an explicit Elasticsearch field type.
    #[must_use]
    pub fn with_field(mut self, name: impl Into<String>, field_type: EsFieldType) -> Self {
        self.fields.insert(name.into(), field_type);
        self
    }

    #[must_use]
    pub fn get(&self, column: &str) -> Option<&EsFieldType> {
        self.fields.get(column)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Build a filter schema from an Arrow schema for an **externally-managed** Elasticsearch
    /// index (the SQL connector path), where the Arrow schema was derived from the index
    /// mapping but the `text`-vs-`keyword` distinction for string columns is not preserved.
    ///
    /// Numeric, boolean, and (loss-lessly) string columns are registered; string columns are
    /// registered as analyzed [`EsFieldType::Keyword`]-free `text` and therefore **omitted**,
    /// because pushing an exact-value predicate against an unknown analyzed field could drop
    /// rows. Date/timestamp columns are omitted (format/timezone coercion is not yet modeled).
    #[must_use]
    pub fn from_connector_schema(schema: &Schema) -> Self {
        let mut fields = HashMap::new();
        for field in schema.fields() {
            if let Some(ft) = arrow_type_to_es_numeric(field.data_type()) {
                fields.insert(field.name().clone(), ft);
            }
        }
        Self { fields }
    }

    /// Build a filter schema for a **Spice-managed** search index, where string columns are
    /// mapped as analyzed `text` with a `keyword` sub-field. Only the columns named in
    /// `filterable` (those Spice mapped with `index: true`) are registered.
    #[must_use]
    pub fn from_spice_managed(schema: &Schema, filterable: &[String]) -> Self {
        let mut fields = HashMap::new();
        for name in filterable {
            let Ok(field) = schema.field_with_name(name) else {
                continue;
            };
            if let Some(ft) = arrow_type_to_es_numeric(field.data_type()) {
                fields.insert(name.clone(), ft);
            } else if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                fields.insert(
                    name.clone(),
                    EsFieldType::TextWithKeyword {
                        keyword_subfield: "keyword".to_string(),
                    },
                );
            }
        }
        Self { fields }
    }
}

/// The Elasticsearch mapping info for one field, decoupled from any particular client's wire
/// representation (so this crate does not need to depend on one) — just the `type` and, for a
/// `text` field, the name of any keyword-typed multi-field sibling.
#[derive(Debug, Clone)]
pub struct EsMappingField {
    /// The Elasticsearch field `type` (e.g. `"keyword"`, `"text"`, `"long"`, `"boolean"`).
    pub field_type: String,
    /// The name of a `keyword`/`constant_keyword`/`wildcard`-typed multi-field sibling (e.g.
    /// `"keyword"` for `title.keyword`), if the mapping declares one. Only meaningful when
    /// `field_type` is an analyzed text type.
    pub keyword_subfield: Option<String>,
}

impl EsFilterSchema {
    /// Build a filter schema for an **externally-managed** index from its real Elasticsearch
    /// mapping, rather than the Arrow schema `from_connector_schema` is limited to. Unlike that
    /// method, a `keyword`-mapped string is exact-filterable, and a `text` field with a
    /// `keyword` multi-field sibling is inexact-filterable against that sibling — only a bare
    /// analyzed `text` field with no such sibling is omitted, because pushing an exact-value
    /// predicate against it could drop rows.
    #[must_use]
    pub fn from_mapping<'a>(
        mapping: impl IntoIterator<Item = (&'a str, &'a EsMappingField)>,
    ) -> Self {
        let mut fields = HashMap::new();
        for (name, info) in mapping {
            let field_type = match info.field_type.as_str() {
                "boolean" => Some(EsFieldType::Boolean),
                "byte" | "short" | "integer" | "long" | "unsigned_long" => {
                    Some(EsFieldType::Integer)
                }
                "float" | "half_float" | "double" | "scaled_float" => Some(EsFieldType::Float),
                "keyword" | "wildcard" | "constant_keyword" => Some(EsFieldType::Keyword),
                "text" | "match_only_text" => info
                    .keyword_subfield
                    .clone()
                    .map(|keyword_subfield| EsFieldType::TextWithKeyword { keyword_subfield }),
                _ => None,
            };
            if let Some(field_type) = field_type {
                fields.insert(name.to_string(), field_type);
            }
        }
        Self { fields }
    }
}

/// Map an Arrow numeric/boolean type to its Elasticsearch field type, mirroring
/// `arrow_type_to_es_mapping` on the write path. Returns `None` for strings, dates, and
/// anything else (the caller decides how to handle those).
fn arrow_type_to_es_numeric(dt: &DataType) -> Option<EsFieldType> {
    match dt {
        DataType::Boolean => Some(EsFieldType::Boolean),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(EsFieldType::Integer),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(EsFieldType::Float),
        _ => None,
    }
}
