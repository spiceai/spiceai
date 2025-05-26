/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashMap, fmt::Display};

use arrow::array::{LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow::error::ArrowError;
use arrow_schema::{Schema, SchemaRef};
use arrow_tools::format::to_markdown_documents;
use datafusion::common::utils::quote_identifier;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use itertools::Itertools;
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::convert_string_arrow_to_iterator;
use crate::datafusion::query::write_to_json_string;

use super::{Error, FormattingSnafu, RecordProcessingSnafu, Result};

pub type ModelKey = String;

#[derive(Debug, Default)]
pub struct VectorSearchTableResult {
    pub data: Vec<RecordBatch>,

    pub primary_keys: Vec<String>,
    pub additional_columns: Vec<String>,
}

impl VectorSearchTableResult {
    /// Return the underlying [`RecordBatch`]s as a pretty formatted table.
    pub fn to_pretty(&self) -> Result<impl Display, ArrowError> {
        to_markdown_documents(&self.data, SEARCH_VALUE_COLUMN_NAME)
    }

    /// Return the primary keys of the [`VectorSearch::individual_search`] as an array of JSON objects.
    ///
    /// Each element is a mapping of the primary key column to its value.
    pub fn primary_keys_json(&self) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let primary_key_projection = get_projection(&self.schema(), &self.primary_keys);
        let primary_keys_records = self
            .data
            .iter()
            .map(|s| s.project(&primary_key_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        if primary_keys_records
            .first()
            .is_some_and(|p| p.num_rows() > 0)
        {
            let pk_str = write_to_json_string(&primary_keys_records).context(FormattingSnafu)?;
            serde_json::from_str(&pk_str)
                .boxed()
                .context(FormattingSnafu)
        } else {
            Ok(vec![])
        }
    }

    /// Return the additional columns of the [`VectorSearch::individual_search`] as an array of JSON objects.
    ///
    /// Each element is a mapping of the additional column name to its value.
    pub fn addition_columns_json(&self) -> Result<Vec<HashMap<String, serde_json::Value>>> {
        let additional_columns_projection =
            get_projection(&self.schema(), &self.additional_columns);
        let additional_columns_records = self
            .data
            .iter()
            .map(|s| s.project(&additional_columns_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        if additional_columns_records
            .first()
            .is_some_and(|p| p.num_rows() > 0)
        {
            let additional_str =
                write_to_json_string(&additional_columns_records).context(FormattingSnafu)?;
            serde_json::from_str(additional_str.as_str())
                .boxed()
                .context(FormattingSnafu)
        } else {
            Ok(vec![])
        }
    }

    /// Return the distance of each search result.
    pub fn score_values(&self) -> Result<Vec<f64>> {
        let Some(scores) = self
            .data
            .iter()
            .map(|s| s.column_by_name(SEARCH_SCORE_COLUMN_NAME).cloned())
            .collect::<Option<Vec<_>>>()
        else {
            return Err(Error::EmbeddingError {
                source: "No distances returned".into(),
            });
        };

        let scores: Option<Vec<_>> = scores
            .iter()
            .flat_map(|v| {
                if let Some(col) = v.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    col.iter().collect::<Vec<Option<f64>>>()
                } else {
                    vec![]
                }
            })
            .collect();
        let Some(scores) = scores else {
            return Err(Error::EmbeddingError {
                source: "Empty embedding scores returned unexpectedly".into(),
            });
        };

        Ok(scores)
    }

    /// Return the input column that was embedded.
    pub fn embedding_columns_list(&self) -> Result<Vec<String>> {
        let embedding_projection =
            get_projection(&self.schema(), &[SEARCH_VALUE_COLUMN_NAME.to_string()]);
        let embedding_records = self
            .data
            .iter()
            .map(|s| s.project(&embedding_projection))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()
            .context(RecordProcessingSnafu)?;

        let result = embedding_records
            .iter()
            .flat_map(|v| {
                convert_string_arrow_to_iterator!(v.column(0))
                    .map(|v| v.map(|vv| vv.unwrap_or_default().to_string()).collect_vec())
                    .unwrap_or_default()
            })
            .collect();

        Ok(result)
    }

    /// Retuns the Schema of the full underlying data.
    pub fn schema(&self) -> SchemaRef {
        self.data
            .first()
            .map_or(Schema::empty().into(), RecordBatch::schema)
    }

    pub fn to_matches(&self, table: &TableReference) -> Result<Vec<Match>> {
        // Early exit on no data.
        if self.data.first().is_none_or(|d| d.num_rows() == 0) {
            return Ok(vec![]);
        }
        let primary_keys_json = self.primary_keys_json()?;
        let additional_columns_json = self.addition_columns_json()?;
        let values = self.embedding_columns_list()?;
        let scores = self.score_values()?;

        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let Some(score) = scores.get(i) else {
                    return Err(Error::EmbeddingError {
                        source: format!("No distance returned for {i}th result").into(),
                    });
                };

                Ok(Match {
                    value: value.clone(),
                    score: *score,
                    dataset: table.to_string(),
                    primary_key: primary_keys_json.get(i).cloned().unwrap_or_default(),
                    metadata: additional_columns_json.get(i).cloned().unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<Match>>>()
    }
}

/// The results of [`CandidateGeneration::search`]'s on a single table.
pub struct VectorSearchGenerationTableResult {
    pub data: Vec<SendableRecordBatchStream>,
    pub primary_keys: Vec<String>,
}

pub type VectorSearchResult = HashMap<TableReference, VectorSearchTableResult>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Match {
    /// The value of the match (e.g., document snippet, identifier, etc.)
    value: String,

    /// The similarity of the match to the query
    score: f64,

    /// The name of the dataset where the match was found
    dataset: String,

    /// Primary key(s) identifying the matched item in the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub primary_key: HashMap<String, serde_json::Value>,

    /// Additional metadata for the match, requested explicitly by the user.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Match {
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn score(&self) -> f64 {
        self.score
    }

    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    #[must_use]
    pub fn primary_key(&self) -> &HashMap<String, serde_json::Value> {
        &self.primary_key
    }

    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, serde_json::Value> {
        &self.metadata
    }
}

pub fn to_matches_sorted(result: &VectorSearchResult, limit: usize) -> Result<Vec<Match>> {
    let output = result
        .iter()
        .map(|(a, b)| b.to_matches(a))
        .collect::<Result<Vec<_>>>()?;

    let mut matches: Vec<_> = output.into_iter().flatten().collect();
    // Sort by score in descending order
    matches.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    matches.truncate(limit);
    Ok(matches)
}

/// Convert a list of column names to a list of column indices. If a column name is not found in the schema, it is ignored.
fn get_projection(schema: &SchemaRef, column_names: &[String]) -> Vec<usize> {
    tracing::trace!("vector search result schema: {schema:?}");
    tracing::trace!("vector search projection column names: {column_names:?}");
    column_names
        .iter()
        .filter_map(|name| {
            schema
                .index_of(quote_identifier(name).to_string().as_str())
                .ok()
                .or(schema.index_of(name.as_str()).ok())
        })
        .collect_vec()
}
