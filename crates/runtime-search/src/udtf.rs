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

//! UDTF argument types and expression builders for vector search and text search.
//!
//! These types are extracted from the runtime so that crates outside of
//! `runtime` can construct UDTF call expressions without depending on
//! `DataFusion` or `embeddings` internals.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chunking::Chunker;
use itertools::Itertools;
use spicepod::component::embeddings::EmbeddingAggregation;

use datafusion::common::Column;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::{FieldMetadata, ScalarFunction};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;

// ---------------------------------------------------------------------------
// Shared utility
// ---------------------------------------------------------------------------

/// Convert a [`Column`] expression (as produced by DataFusion SQL parsing of
/// a dotted table name) back into a [`TableReference`].
#[must_use]
pub fn table_ref_from_column_expr(c: &Column) -> TableReference {
    let table: Arc<str> = c.name.clone().into();
    let schema: Option<&str> = c.relation.as_ref().map(TableReference::table);
    let catalog: Option<&str> = c.relation.as_ref().and_then(TableReference::schema);
    match (catalog, schema) {
        // Catalog without schema is impossible.
        (None | Some(_), None) => TableReference::Bare { table },
        (None, Some(s)) => TableReference::Partial {
            schema: s.into(),
            table,
        },
        (Some(c), Some(s)) => TableReference::Full {
            catalog: c.into(),
            schema: s.into(),
            table,
        },
    }
}

/// Convert a [`TableReference`] into the [`Column`] encoding used by UDTFs
/// that accept a table name as an expression argument.
#[must_use]
pub fn to_column_expr(tbl: &TableReference) -> Column {
    match tbl {
        TableReference::Bare { table } => Column::new_unqualified(table.to_string()),
        TableReference::Partial { schema, table } => Column::new(
            Some(TableReference::Bare {
                table: Arc::clone(schema),
            }),
            table.to_string(),
        ),
        TableReference::Full {
            catalog,
            schema,
            table,
        } => Column::new(
            Some(TableReference::Partial {
                schema: Arc::clone(catalog),
                table: Arc::clone(schema),
            }),
            table.to_string(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

pub static VECTOR_SEARCH_UDTF_NAME: &str = "vector_search";

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DistanceMetric {
    /// Cosine similarity (default).
    Cosine,
    /// Negated Euclidean distance.
    L2,
    /// Dot product.
    Dot,
}

impl DistanceMetric {
    pub fn parse(s: &str) -> DataFusionResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" | "cos" => Ok(Self::Cosine),
            "l2" | "euclidean" | "euclid" => Ok(Self::L2),
            "dot" | "inner" | "ip" => Ok(Self::Dot),
            other => Err(DataFusionError::Plan(format!(
                "Unsupported distance_metric '{other}' for {VECTOR_SEARCH_UDTF_NAME}. Supported: 'cosine', 'l2', 'dot'."
            ))),
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2",
            Self::Dot => "dot",
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct VectorSearchTableFuncArgs {
    pub tbl: TableReference,
    pub query: String,
    pub queries: Vec<String>,
    pub column: Option<String>,
    pub limit: Option<usize>,
    pub include_score: Option<bool>,
    pub distance_metric: Option<DistanceMetric>,
}

impl VectorSearchTableFuncArgs {
    /// Build the expression list for a `vector_search(...)` UDTF invocation.
    pub fn to_expr(&self) -> DataFusionResult<Vec<Expr>> {
        let query_expr = if self.queries.len() > 1 {
            let make_array = datafusion::functions_nested::make_array::make_array_udf();
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array,
                self.queries
                    .iter()
                    .map(|q| Expr::Literal(ScalarValue::Utf8(Some(q.clone())), None))
                    .collect(),
            ))
        } else {
            let q = self
                .queries
                .first()
                .cloned()
                .unwrap_or_else(|| self.query.clone());
            Expr::Literal(ScalarValue::Utf8(Some(q)), None)
        };
        let mut expr = vec![Expr::Column(to_column_expr(&self.tbl)), query_expr];

        if let Some(col) = self.column.as_ref() {
            expr.push(Expr::Column(Column::new_unqualified(col)));
        }
        if let Some(limit) = self.limit {
            let limit_u64 = u64::try_from(limit).map_err(|_| {
                DataFusionError::Plan(format!(
                    "vector_search: limit value {limit} is out of range for u64."
                ))
            })?;
            expr.push(Expr::Literal(ScalarValue::UInt64(Some(limit_u64)), None));
        }
        if let Some(include_score) = self.include_score {
            expr.push(Expr::Literal(
                ScalarValue::Boolean(Some(include_score)),
                None,
            ));
        }
        if let Some(metric) = self.distance_metric {
            let meta = FieldMetadata::new(BTreeMap::from([(
                "spice.parameter_name".to_string(),
                "distance_metric".to_string(),
            )]));
            expr.push(Expr::Literal(
                ScalarValue::Utf8(Some(metric.as_str().to_string())),
                Some(meta),
            ));
        }
        Ok(expr)
    }

    /// Check [`Self::column`] is valid, attempt to pick a default, and retrieve the associated [`EmbeddingColumnConfig`].
    pub fn get_column_and_config(
        &self,
        embedded_columns: &HashMap<String, EmbeddingColumnConfig>,
    ) -> DataFusionResult<(String, EmbeddingColumnConfig)> {
        let cfg = self
            .column
            .as_ref()
            .and_then(|c| embedded_columns.get(c))
            .cloned();
        match (self.column.as_deref(), cfg) {
            (Some(col), Some(cfg)) => Ok((col.to_string(), cfg)),
            (Some(col), None) => {
                let mut available: Vec<String> = embedded_columns.keys().cloned().collect();
                available.sort();
                let suggestion = closest_column(col, &available)
                    .map(|s| format!(" Did you mean '{s}'?"))
                    .unwrap_or_default();
                Err(DataFusionError::Plan(format!(
                    "User function 'vector_search' is called on table '{}' that does not have an embedding index on '{col}' column. Indexed column(s): {}.{suggestion}",
                    self.tbl,
                    available.iter().join(", ")
                )))
            }
            (None, _) => {
                if embedded_columns.len() > 1 {
                    let mut available: Vec<String> = embedded_columns.keys().cloned().collect();
                    available.sort();
                    return Err(DataFusionError::Plan(format!(
                        "User function 'vector_search' is called on table '{}' that has {} vector search columns ({}). Must call 'vector_search' with column parameter, e.g. `vector_search(\"my table\", 'my query', my_embedded_col)`.",
                        self.tbl,
                        embedded_columns.len(),
                        available.iter().join(", ")
                    )));
                }
                if let Some((col, cfg)) = embedded_columns.iter().next() {
                    Ok((col.clone(), cfg.clone()))
                } else {
                    Err(DataFusionError::Plan(format!(
                        "User function 'vector_search' is called on table '{}' that has no associated embedding index.",
                        self.tbl,
                    )))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Embedding column config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EmbeddingInputMode {
    Scalar,
    ListMulti {
        aggregation: EmbeddingAggregation,
        max_elements_per_row: usize,
    },
}

impl EmbeddingInputMode {
    #[must_use]
    pub fn is_list_multi(&self) -> bool {
        matches!(self, Self::ListMulti { .. })
    }
}

#[derive(Clone)]
pub struct EmbeddingColumnConfig {
    pub model_name: String,
    pub vector_size: i32,
    pub in_base_table: bool,
    pub chunker: Option<Arc<dyn Chunker>>,
    pub input_mode: EmbeddingInputMode,
}

impl std::fmt::Debug for EmbeddingColumnConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingColumnConfig")
            .field("model_name", &self.model_name)
            .field("vector_size", &self.vector_size)
            .field("in_base_table", &self.in_base_table)
            .field("input_mode", &self.input_mode)
            .finish_non_exhaustive()
    }
}

/// Find the closest column name via Levenshtein distance.
pub fn closest_column(target: &str, candidates: &[String]) -> Option<String> {
    let target_lower = target.to_lowercase();
    let (best, distance) = candidates
        .iter()
        .map(|c| {
            (
                c,
                util::levenshtein::distance(&target_lower, &c.to_lowercase()),
            )
        })
        .min_by_key(|(_, d)| *d)?;
    let threshold = target.len().div_ceil(2).max(2);
    if distance <= threshold {
        Some(best.clone())
    } else {
        None
    }
}

/// Parse a scalar value as a u64 limit parameter.
pub fn parse_limit_scalar(scalar: &ScalarValue) -> DataFusionResult<u64> {
    match scalar {
        ScalarValue::Int64(Some(limit)) => u64::try_from(*limit).map_err(|_| {
            DataFusionError::Plan(format!(
                "Limit argument must be a non-negative integer, but got {limit}."
            ))
        }),
        ScalarValue::UInt64(Some(limit)) => Ok(*limit),
        ScalarValue::Utf8(Some(limit_str)) => limit_str.parse::<u64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "Limit argument must be a non-negative integer, but got '{limit_str}'."
            ))
        }),
        _ => Err(DataFusionError::Plan(format!(
            "Limit argument must be a non-negative integer, but got {scalar}."
        ))),
    }
}

// ---------------------------------------------------------------------------
// Text search
// ---------------------------------------------------------------------------

pub static TEXT_SEARCH_UDTF_NAME: &str = "text_search";

#[derive(Debug, PartialEq, Clone)]
pub struct TextSearchTableFuncArgs {
    pub tbl: TableReference,
    pub query: String,
    pub column: Option<String>,
    pub limit: Option<usize>,
    pub include_score: Option<bool>,
}

impl TextSearchTableFuncArgs {
    /// Build the expression list for a `text_search(...)` UDTF invocation.
    #[must_use]
    pub fn to_expr(&self) -> Vec<Expr> {
        let mut expr = vec![
            Expr::Column(to_column_expr(&self.tbl)),
            Expr::Literal(ScalarValue::Utf8(Some(self.query.clone())), None),
        ];

        if let Some(col) = self.column.as_ref() {
            expr.push(Expr::Column(Column::new_unqualified(col)));
        }

        if let Some(limit) = self.limit {
            expr.push(Expr::Literal(
                ScalarValue::UInt64(Some(u64::try_from(limit).unwrap_or(u64::MAX))),
                None,
            ));
        }

        if let Some(include_score) = self.include_score {
            expr.push(Expr::Literal(
                ScalarValue::Boolean(Some(include_score)),
                None,
            ));
        }

        expr
    }

    /// Find column to perform full text search upon.
    pub fn column(&self, search_fields: &[String]) -> DataFusionResult<String> {
        if let Some(col) = &self.column {
            if !search_fields.contains(col) {
                return Err(DataFusionError::Plan(format!(
                    "User function 'text_search' is called on table '{}' that does not have a full text search index on '{col}' column. Index is on column(s): {}.{}",
                    self.tbl,
                    search_fields.join(", "),
                    closest_column(col, search_fields)
                        .map(|s| format!(" Did you mean '{s}'?"))
                        .unwrap_or_default()
                )));
            }
            Ok(col.clone())
        } else {
            let mut fields = search_fields.iter();
            match (fields.next(), fields.next()) {
                (Some(field), None) => Ok(field.clone()),
                (Some(_), Some(_)) => Err(DataFusionError::Plan(format!(
                    "User function 'text_search' is called on table '{}' that has {} full text search columns ({}). Must call 'text_search' with column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`",
                    self.tbl,
                    search_fields.len(),
                    search_fields.join(", ")
                ))),
                _ => Err(DataFusionError::Plan(format!(
                    "User function 'text_search' is called on table '{}' that has no associated full text search index",
                    self.tbl
                ))),
            }
        }
    }
}
