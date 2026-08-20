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

/// The `ignore_above` Spice sets on the `.keyword` multi-field it attaches to every managed
/// `text` column (see `runtime`'s Elasticsearch mapping builder): a source value longer than this
/// (in characters) has no entry in the sub-field at all. Kept as one constant so the write path
/// and this crate's pushdown safety checks cannot silently drift apart.
pub const SPICE_MANAGED_KEYWORD_IGNORE_ABOVE: usize = 256;

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
    /// ES `float`/`double`. Comparisons are pushed but marked inexact: the JSON round-trip and
    /// the SQL literal may not share Elasticsearch's binary representation, so `DataFusion`
    /// re-checks. Unlike [`EsFieldType::QuantizedFloat`], the indexed value round-trips the
    /// source value exactly (no scaling/rounding), so a range clause is always a superset.
    Float,
    /// ES `half_float`/`scaled_float`. The indexed value is *quantized* (rounded, or scaled then
    /// rounded to an integer) rather than round-tripped, so a `range`/`BETWEEN` boundary compares
    /// the quantized value against a query threshold and can exclude a source row that actually
    /// satisfies the SQL predicate — a false negative no `DataFusion` re-check can recover.
    /// Equality still pushes: Elasticsearch applies the same quantization to the query literal,
    /// so a `term` match is still a safe superset.
    QuantizedFloat,
    /// ES `keyword`/`wildcard`/`constant_keyword`: a whole-value, non-analyzed string.
    /// `term`/`range`/`prefix` match the stored value exactly.
    ///
    /// `ignore_above` (`Some(n)`) means the field itself silently omits values longer than `n`
    /// characters, same subset hazard as [`EsFieldType::TextWithKeyword`]'s — reject
    /// over-length equality/`IN` literals and never push `prefix`. `None` means the mapping was
    /// read directly and declares no `ignore_above` (verified unbounded), *except* when
    /// constructed without real mapping data (e.g. in tests), where it just means "not modeled".
    ///
    /// `has_normalizer` is `true` when the mapping configures a `normalizer` on this field.
    /// Elasticsearch compares normalized indexed terms while SQL compares the raw `_source`
    /// value. A normalizer is not order-preserving (e.g. a lowercase normalizer makes indexed
    /// `"z"` sort before `"a"` even though the raw values compare the other way), so a `range`
    /// clause is not provably a superset — see [`Self::supports_range`]. Equality/`IN` are also
    /// affected: a `term` match against the normalized indexed value is a superset of the raw SQL
    /// comparison (e.g. source `"ABC"` indexes to `"abc"` and matches a `term` query for `"abc"`,
    /// even though SQL `col = 'abc'` must not), so `DataFusion` must re-check — see
    /// [`Self::is_exact_for_value_match`]. Prefix is unaffected: applied identically to the
    /// indexed value and the query term, a normalizer preserves prefix relationships even though
    /// it does not preserve ordering or exact membership.
    Keyword {
        ignore_above: Option<usize>,
        has_normalizer: bool,
    },
    /// An analyzed `text` field carrying a non-analyzed `keyword` sub-field (the mapping Spice
    /// writes for string columns, e.g. `col.keyword`). Equality/prefix are pushed against the
    /// sub-field but marked inexact: analysis/collation can differ, so `DataFusion` re-checks.
    ///
    /// `ignore_above` (`Some(n)`) means the sub-field silently omits values longer than `n`
    /// characters — a source row can satisfy the SQL predicate while having no entry at all in
    /// the sub-field, which is a *subset*, not a superset. Predicates against such a field must
    /// reject literals over the threshold (equality/`IN`) and never push `prefix` at all (an
    /// unseen longer matching value could always exist). `None` means the limit is not known —
    /// callers must not use it as evidence the field is unbounded (see `from_mapping`).
    ///
    /// `has_normalizer` carries the same range-pushdown hazard documented on
    /// [`Self::Keyword`], for the `keyword` sub-field.
    TextWithKeyword {
        keyword_subfield: String,
        ignore_above: Option<usize>,
        has_normalizer: bool,
    },
}

impl EsFieldType {
    /// The Elasticsearch field name to target for exact-value predicates (`term`, `terms`,
    /// `range`, `prefix`). For a `text` field this is its `keyword` sub-field.
    #[must_use]
    pub fn value_field(&self, column: &str) -> String {
        match self {
            EsFieldType::TextWithKeyword {
                keyword_subfield, ..
            } => format!("{column}.{keyword_subfield}"),
            _ => column.to_string(),
        }
    }

    /// Whether an equality/`IN`/`range` predicate on this field type is exact (`true`) or a
    /// superset that `DataFusion` must re-check (`false`).
    ///
    /// A `Keyword` with `has_normalizer` is not exact even for equality: Elasticsearch matches on
    /// the *normalized* indexed term, so e.g. a lowercase normalizer makes source value `"ABC"`
    /// match a `term` query for `"abc"`, while SQL `col = 'abc'` must not — the `term` match is a
    /// superset `DataFusion` has to re-check against the raw `_source` value.
    #[must_use]
    pub fn is_exact_for_value_match(&self) -> bool {
        match self {
            EsFieldType::Boolean | EsFieldType::Integer => true,
            EsFieldType::Keyword { has_normalizer, .. } => !has_normalizer,
            EsFieldType::Float
            | EsFieldType::QuantizedFloat
            | EsFieldType::TextWithKeyword { .. } => false,
        }
    }

    /// Whether a `range`/`BETWEEN` predicate is expressible against this field type at all.
    /// `false` for [`EsFieldType::Boolean`] (nonsensical), [`EsFieldType::QuantizedFloat`]
    /// (quantization can make a boundary comparison exclude a true SQL match — see the variant's
    /// docs — so there is no safe superset to push), a [`EsFieldType::Keyword`]/
    /// [`EsFieldType::TextWithKeyword`] with `ignore_above` set (a document whose value exceeds
    /// that length has no entry in the field at all, so no boundary comparison — regardless of
    /// where the query literal falls — can be trusted not to exclude a row that truly satisfies
    /// the SQL predicate; same subset hazard [`Self::supports_prefix`] guards against), and a
    /// `Keyword`/`TextWithKeyword` with a `normalizer` configured: normalization is not
    /// order-preserving, so the indexed term order can disagree with the raw `_source` order SQL
    /// compares, making a range boundary unsafe regardless of `ignore_above`.
    #[must_use]
    pub fn supports_range(&self) -> bool {
        match self {
            EsFieldType::Boolean | EsFieldType::QuantizedFloat => false,
            EsFieldType::Keyword {
                ignore_above,
                has_normalizer,
            }
            | EsFieldType::TextWithKeyword {
                ignore_above,
                has_normalizer,
                ..
            } => ignore_above.is_none() && !has_normalizer,
            EsFieldType::Integer | EsFieldType::Float => true,
        }
    }

    /// Whether a prefix (`LIKE 'x%'`) predicate is expressible against this field type. `false`
    /// whenever `ignore_above` is `Some`: an unseen value longer than the threshold could still
    /// match the prefix while having no entry in the index at all, and no length check on the
    /// prefix literal itself can rule that out.
    ///
    /// A configured `normalizer` does not affect this: applied identically to the indexed value
    /// and the query term, it preserves prefix relationships even though it doesn't preserve
    /// ordering — unlike [`Self::supports_range`], this is unaffected by `has_normalizer`.
    #[must_use]
    pub fn supports_prefix(&self) -> bool {
        match self {
            EsFieldType::Keyword { ignore_above, .. }
            | EsFieldType::TextWithKeyword { ignore_above, .. } => ignore_above.is_none(),
            _ => false,
        }
    }

    /// Whether `value` (already confirmed type-compatible via `value_matches_field`) can actually
    /// be found by this field's exact-value clause. `false` only when this field's `ignore_above`
    /// the literal exceeds — Elasticsearch never indexed anything that could match it, so pushing
    /// the clause would silently exclude a row whose real value equals the literal.
    #[must_use]
    pub fn accepts_value_length(&self, value: &serde_json::Value) -> bool {
        let ignore_above = match self {
            EsFieldType::TextWithKeyword { ignore_above, .. }
            | EsFieldType::Keyword { ignore_above, .. } => *ignore_above,
            _ => None,
        };
        match ignore_above {
            Some(limit) => value.as_str().is_none_or(|s| s.chars().count() <= limit),
            None => true,
        }
    }
}

/// A registered column: its Elasticsearch representation, plus whether an `exists`-based
/// `IS [NOT] NULL` pre-filter is safe to push for it.
#[derive(Debug, Clone)]
struct ColumnEntry {
    field_type: EsFieldType,
    supports_null_check: bool,
    /// Whether this column is known to hold at most one value per document. Elasticsearch never
    /// distinguishes a scalar field from an array of the same type in its mapping — any field can
    /// hold multiple values, and a `term`/`range` clause matches if *any* element satisfies it,
    /// which is not the same as a scalar SQL comparison. `true` only when Spice itself controls
    /// what is written to the field (a Spice-managed index, or a schema/tests constructed via
    /// [`EsFilterSchema::with_field`]); a field read from a real, externally-managed mapping (see
    /// [`EsFilterSchema::from_mapping`]) has no such guarantee, so exactness is capped to
    /// `Inexact` for it regardless of what [`EsFieldType::is_exact_for_value_match`] would
    /// otherwise say.
    confirmed_scalar: bool,
    /// Whether the field (or, for `text`, its keyword sibling) has Elasticsearch `doc_values`
    /// enabled. `false` means a `range`/`BETWEEN` clause against it is not safe to issue.
    has_doc_values: bool,
}

/// The set of columns that may be filtered in Elasticsearch, keyed by column name.
///
/// A column absent from this map is treated as non-pushable: [`crate::classify_filter`] returns
/// [`datafusion::logical_expr::TableProviderFilterPushDown::Unsupported`] for any predicate that
/// references it.
#[derive(Debug, Clone, Default)]
pub struct EsFilterSchema {
    fields: HashMap<String, ColumnEntry>,
}

impl EsFilterSchema {
    #[must_use]
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Register a single column with an explicit Elasticsearch field type. `IS [NOT] NULL`
    /// pushdown is assumed safe (no `null_value` sentinel) — use this for Spice-managed schemas
    /// and tests; [`Self::from_mapping`] verifies it against the real mapping instead.
    #[must_use]
    pub fn with_field(mut self, name: impl Into<String>, field_type: EsFieldType) -> Self {
        self.fields.insert(
            name.into(),
            ColumnEntry {
                field_type,
                supports_null_check: true,
                confirmed_scalar: true,
                has_doc_values: true,
            },
        );
        self
    }

    #[must_use]
    pub fn get(&self, column: &str) -> Option<&EsFieldType> {
        self.fields.get(column).map(|entry| &entry.field_type)
    }

    /// Whether an `exists`/`must_not exists` pre-filter is safe to push for `column`'s
    /// `IS [NOT] NULL`. `false` when the real mapping declares a `null_value` sentinel: a
    /// document with an explicit source `null` still has an indexed value, so `exists` can't
    /// tell it apart from a genuine one — `must_not exists` (`IS NULL`) would then wrongly
    /// exclude that row from the pre-filtered candidates, which no above-scan recheck restores.
    #[must_use]
    pub fn supports_null_check(&self, column: &str) -> bool {
        self.fields
            .get(column)
            .is_some_and(|entry| entry.supports_null_check)
    }

    /// Whether `column` is known to hold at most one value per document — a field's exactness
    /// documented on [`ColumnEntry::confirmed_scalar`] is capped by this.
    #[must_use]
    pub fn is_confirmed_scalar(&self, column: &str) -> bool {
        self.fields
            .get(column)
            .is_some_and(|entry| entry.confirmed_scalar)
    }

    /// Whether a `range`/`BETWEEN` clause is safe to issue against `column` — `false` when the
    /// real mapping declares `doc_values: false` for it.
    #[must_use]
    pub fn has_doc_values(&self, column: &str) -> bool {
        self.fields
            .get(column)
            .is_some_and(|entry| entry.has_doc_values)
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
            if let Some(field_type) = arrow_type_to_es_numeric(field.data_type()) {
                fields.insert(
                    field.name().clone(),
                    ColumnEntry {
                        field_type,
                        supports_null_check: true,
                        confirmed_scalar: true,
                        has_doc_values: true,
                    },
                );
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
            let field_type = if let Some(field_type) = arrow_type_to_es_numeric(field.data_type()) {
                field_type
            } else if matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                // Spice's own write path never configures a `normalizer` on the `.keyword`
                // sub-field it attaches to managed string columns.
                EsFieldType::TextWithKeyword {
                    keyword_subfield: "keyword".to_string(),
                    ignore_above: Some(SPICE_MANAGED_KEYWORD_IGNORE_ABOVE),
                    has_normalizer: false,
                }
            } else {
                continue;
            };
            // Spice's own write path never sets a `null_value` sentinel on managed mappings, and
            // never writes more than one value into a column it declared scalar in the Arrow
            // schema, so both are safe to assume here.
            fields.insert(
                name.clone(),
                ColumnEntry {
                    field_type,
                    supports_null_check: true,
                    confirmed_scalar: true,
                    has_doc_values: true,
                },
            );
        }
        Self { fields }
    }
}

/// The Elasticsearch mapping info for one field, decoupled from any particular client's wire
/// representation (so this crate does not need to depend on one) — just the `type` and, for a
/// `text` field, the name of any keyword-typed multi-field sibling.
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent Elasticsearch mapping property (null_value/index/doc_values/normalizer presence), not a state machine — they can be set in any combination"
)]
pub struct EsMappingField {
    /// The Elasticsearch field `type` (e.g. `"keyword"`, `"text"`, `"long"`, `"boolean"`).
    pub field_type: String,
    /// The name of a `keyword`/`constant_keyword`/`wildcard`-typed multi-field sibling (e.g.
    /// `"keyword"` for `title.keyword`), if the mapping declares one. Only meaningful when
    /// `field_type` is an analyzed text type.
    pub keyword_subfield: Option<String>,
    /// `ignore_above` on whichever field the pushdown will actually query for an exact-value
    /// predicate: the keyword sibling for `text`, or the field itself for
    /// `keyword`/`wildcard`/`constant_keyword`. `None` means the real mapping was read and
    /// declares no `ignore_above` (verified unbounded).
    pub keyword_ignore_above: Option<usize>,
    /// Whether `null_value` is set on this field, or (for `text`) on its keyword sibling — see
    /// [`EsFieldType`] docs on why that makes an `exists` pre-filter unsafe.
    pub has_null_value: bool,
    /// Whether the field the pushdown will actually query — the keyword sibling for `text`, or
    /// the field itself otherwise — is indexed (`index: false` in the mapping means Elasticsearch
    /// cannot search it at all, so it must never be classified as pushable). Elasticsearch
    /// defaults this to `true` when the mapping declares no `index` parameter.
    pub indexed: bool,
    /// Whether that same field has Elasticsearch `doc_values` enabled. `false` means a
    /// `range`/`BETWEEN` clause against it is not safe to issue. Elasticsearch defaults this to
    /// `true` when the mapping declares no `doc_values` parameter.
    pub has_doc_values: bool,
    /// Whether a `normalizer` is configured on whichever field the pushdown will actually query
    /// for an exact-value predicate — the keyword sibling for `text`, or the field itself for
    /// `keyword`/`wildcard`/`constant_keyword` — same target as `keyword_ignore_above` above.
    /// `true` makes range/`BETWEEN` pushdown against the field unsafe (see
    /// [`EsFieldType::supports_range`]); equality/`IN`/prefix are unaffected.
    pub has_normalizer: bool,
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
            // `index: false` means Elasticsearch never indexed the field at all — no clause
            // against it can be trusted to search correctly, so it must not be classified as
            // pushable/filterable.
            if !info.indexed {
                continue;
            }
            let field_type = match info.field_type.as_str() {
                "boolean" => Some(EsFieldType::Boolean),
                "byte" | "short" | "integer" | "long" | "unsigned_long" => {
                    Some(EsFieldType::Integer)
                }
                "float" | "double" => Some(EsFieldType::Float),
                // Quantizing types: a range/BETWEEN boundary can exclude a true SQL match (see
                // `EsFieldType::QuantizedFloat`), so these must not be treated as plain `Float`.
                "half_float" | "scaled_float" => Some(EsFieldType::QuantizedFloat),
                "keyword" | "wildcard" | "constant_keyword" => Some(EsFieldType::Keyword {
                    ignore_above: info.keyword_ignore_above,
                    has_normalizer: info.has_normalizer,
                }),
                "text" | "match_only_text" => {
                    info.keyword_subfield.clone().map(|keyword_subfield| {
                        EsFieldType::TextWithKeyword {
                            keyword_subfield,
                            ignore_above: info.keyword_ignore_above,
                            has_normalizer: info.has_normalizer,
                        }
                    })
                }
                _ => None,
            };
            if let Some(field_type) = field_type {
                fields.insert(
                    name.to_string(),
                    ColumnEntry {
                        field_type,
                        supports_null_check: !info.has_null_value,
                        // Elasticsearch mappings carry no scalar-vs-array signal at all — any
                        // field can hold multiple values, and a `term`/`range` clause matches if
                        // *any* element does, which a scalar SQL comparison does not mean. With
                        // no way to confirm this field is single-valued, cap it to `Inexact`
                        // rather than risk a false `Exact` (see `ColumnEntry::confirmed_scalar`).
                        confirmed_scalar: false,
                        has_doc_values: info.has_doc_values,
                    },
                );
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
        DataType::Float32 | DataType::Float64 => Some(EsFieldType::Float),
        // The write path (`arrow_type_to_es_mapping`) maps `Float16` to Elasticsearch
        // `half_float`, whose indexed value is quantized rather than round-tripped exactly, so
        // it shares `half_float`/`scaled_float`'s `QuantizedFloat` classification, not `Float`'s.
        DataType::Float16 => Some(EsFieldType::QuantizedFloat),
        _ => None,
    }
}
