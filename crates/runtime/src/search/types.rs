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

use arrow::array::{LargeStringArray, RecordBatch};
use arrow::array::{StringArray, StringViewArray};
use arrow::error::ArrowError;
use arrow_schema::{Schema, SchemaRef};
use arrow_tools::format::to_markdown_documents;
use datafusion::sql::TableReference;
use futures::StreamExt;
use itertools::Itertools;
use search::aggregation::AggregationResult;
use search::collect_batches;
use search::{SEARCH_SCORE_COLUMN_NAME, aggregation::Error as SearchError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{collections::HashMap, fmt::Display};

use crate::convert_string_arrow_to_iterator;
use crate::datafusion::query::write_to_json_string;
use crate::search::candidate::vector::VECTOR_DISTANCE_COLUMN_NAME;
use crate::search::util::get_projection;
use crate::search::{Error, FormattingSnafu, RecordProcessingSnafu, SearchAggregationSnafu};

use super::Result;

pub type ModelKey = String;

pub type VectorSearchResult = HashMap<TableReference, AggregationResult>;

#[derive(Debug, Default)]
pub struct VectorSearchTableResult {
    pub data: Vec<RecordBatch>,

    pub primary_keys: Vec<String>,
    // original data, not the embedding vector.
    pub embedding_column: String,
    pub additional_columns: Vec<String>,
}

impl VectorSearchTableResult {
    /// Return the underlying [`RecordBatch`]s as a pretty formatted table.
    pub fn to_pretty(&self) -> Result<impl Display, ArrowError> {
        to_markdown_documents(
            &self.data,
            &self.embedding_column,
            None,
            self.primary_keys.as_slice(),
        )
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
    pub fn distance_values(&self) -> Result<Vec<f64>> {
        let Some(distances) = self
            .data
            .iter()
            .map(|s| s.column_by_name(VECTOR_DISTANCE_COLUMN_NAME).cloned())
            .collect::<Option<Vec<_>>>()
        else {
            return Err(Error::EmbeddingError {
                source: "No distances returned".into(),
            });
        };

        let distances: Option<Vec<_>> = distances
            .iter()
            .flat_map(|v| {
                if let Some(col) = v.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    col.iter().collect::<Vec<Option<f64>>>()
                } else {
                    vec![]
                }
            })
            .collect();
        let Some(distances) = distances else {
            return Err(Error::EmbeddingError {
                source: "Empty embedding distances returned unexpectedly".into(),
            });
        };

        Ok(distances)
    }

    /// Return the input column that was embedded.
    pub fn embedding_columns_list(&self) -> Result<Vec<String>> {
        let embedding_projection = get_projection(&self.schema(), &[self.embedding_column.clone()]);
        let embedding_records = self
            .data
            .iter()
            .map(|s| s.project(&embedding_projection))
            .collect::<std::result::Result<Vec<RecordBatch>, ArrowError>>()
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
        let distances = self.distance_values()?;

        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let Some(distance) = distances.get(i) else {
                    return Err(Error::EmbeddingError {
                        source: format!("No distance returned for {i}th result").into(),
                    });
                };

                Ok(Match {
                    value: value.clone(),
                    score: 1.0 - *distance,
                    dataset: table.to_string(),
                    primary_key: primary_keys_json.get(i).cloned().unwrap_or_default(),
                    metadata: additional_columns_json.get(i).cloned().unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<Match>>>()
    }
}

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

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum MatchType {
    Single(Value),
    Multiple(Vec<Value>),
}

impl From<Vec<Value>> for MatchType {
    fn from(mut value: Vec<Value>) -> Self {
        if value.len() == 1 {
            let Some(v) = value.pop() else {
                unreachable!("The value array must have one element");
            };
            return MatchType::Single(v);
        }
        MatchType::Multiple(value)
    }
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

pub async fn to_matches_sorted(result: VectorSearchResult, limit: usize) -> Result<Vec<Match>> {
    let mut matches: Vec<Match> = Vec::new();
    for (a, b) in result {
        let mut o = to_matches(&a, b).await.context(SearchAggregationSnafu)?;
        matches.append(&mut o);
    }
    // Sort by score in descending order
    matches.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    matches.truncate(limit);
    Ok(matches)
}

/// Consumes [`AggregationResult`] and converts all results to [`Match`] format.
pub async fn to_matches(
    tbl: &TableReference,
    mut result: AggregationResult,
) -> Result<Vec<Match>, SearchError> {
    let mut output = vec![];
    while let Some(Ok(rb)) = result.data.next().await {
        let data = result.data_json(&rb)?;
        let primary_key = result.primary_key_json(&rb)?;

        // Collect the highlights for each column. Value of map is a vector rows, each of which contains the highlights for that row.
        let matches = result
            .matches
            .iter()
            .map(|(underlying, derived_cols)| {
                let z = result
                    .columns_as_json(&rb, derived_cols)?
                    .into_iter()
                    .map(|x| x.into_values().collect_vec())
                    .collect::<Vec<_>>();
                Ok((underlying.clone(), z))
            })
            .collect::<std::result::Result<HashMap<String, Vec<Vec<Value>>>, SearchError>>()?;

        let matches = transpose_and_convert(matches);

        let scores = result.score_values(&rb)?;
        let mut matches = data
            .into_iter()
            .zip(primary_key)
            .zip(matches)
            .zip(scores)
            .map(|(((data, primary_key), matches), score)| {
                let value = match matches.values().find_or_first(|_| true).as_ref() {
                    Some(MatchType::Single(Value::String(s))) => s.clone(),
                    Some(MatchType::Multiple(v)) if matches!(v.first(), Some(Value::String(_))) => {
                        match v.first() {
                            Some(Value::String(s)) => s.clone(),
                            _ => String::new(), // Should be unreachable.
                        }
                    }
                    _ => String::new(), // Should be unreachable.
                };
                Match {
                    score,
                    dataset: tbl.to_string(),
                    primary_key,
                    metadata: data,
                    value,
                }
            })
            .collect::<Vec<_>>();
        output.append(&mut matches);
    }

    Ok(output)
}

pub async fn to_pretty(agg: AggregationResult) -> Result<impl Display, ArrowError> {
    // Add primary keys, 'score' & additional data columns to the document header.
    let header_fields = [
        vec![SEARCH_SCORE_COLUMN_NAME.to_string()],
        agg.primary_key.clone(),
        agg.data_columns.clone(),
    ]
    .concat();
    let rb = collect_batches(agg.data).await?;

    // For each record batch, create markdown documents for each column in `agg.matches`.
    let doc_sets: Vec<String> = agg
        .matches
        .iter()
        .map(|(derived_from, highlight_columns)| {
            highlight_columns
                .iter()
                .map(|col| {
                    to_markdown_documents(
                        rb.as_slice(),
                        col,
                        Some(derived_from.as_str()),
                        header_fields.as_slice(),
                    )
                })
                .collect::<Result<Vec<String>, ArrowError>>()
        })
        .collect::<Result<Vec<Vec<String>>, ArrowError>>()?
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect::<Vec<String>>();

    Ok(doc_sets.join("\n"))
}

/// Convert a map of {column name -> column values}, to a per-row representation.
fn transpose_and_convert(
    column_format: HashMap<String, Vec<Vec<Value>>>,
) -> Vec<HashMap<String, MatchType>> {
    let max_rows = column_format
        .values()
        .map(std::vec::Vec::len)
        .max()
        .unwrap_or(0);

    let key_count = column_format.len();
    let mut rows: Vec<_> = (0..max_rows)
        .map(|_| HashMap::with_capacity(key_count))
        .collect();

    for (key, vv) in column_format {
        for (i, row_values) in vv.into_iter().enumerate() {
            rows[i].insert(key.clone(), row_values.into());
        }
    }

    rows
}
