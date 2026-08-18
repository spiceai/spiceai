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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    common::Column, execution::SendableRecordBatchStream, prelude::SessionContext,
    sql::TableReference,
};
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu};

use crate::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, VectorSearchGenerationResult};

pub mod reciprocal_rank;
pub mod relative_score;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generated candidate data missing required column '{col}'."))]
    CandidateMissingRequiredColumn { col: String },

    #[snafu(display("No candidates generated"))]
    NoCandidatesGenerated,

    #[snafu(display(
        "Cannot aggregate candidates from multiple sources (e.g. embedded columns of a dataset) when the dataset has no primary key."
    ))]
    NoPrimaryKey,

    #[snafu(display(
        "Generated candidates have inconsistent columns. From {:?}. And {:?}.",
        s1.fields().iter().map(|f| format!("{}: {} (nullable={})", f.name(), f.data_type(), f.is_nullable())).collect::<Vec<_>>().join(", "),
        s2.fields().iter().map(|f| format!("{}: {} (nullable={})", f.name(), f.data_type(), f.is_nullable())).collect::<Vec<_>>().join(", "),
    ))]
    InconsistentColumns { s1: SchemaRef, s2: SchemaRef },

    #[snafu(display("Failed to aggregate search results: {source}"))]
    DatafusionError {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "The resulting aggregation result is inconsistent, which is an unexpected error. {source}"
    ))]
    InconsistentAggregationResult {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(self, Error::NoPrimaryKey)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Standard interface for algorithms that decide how to aggregate the results from [`super::generation::CandidateGeneration::search`] into a single-ordered set.
///
/// Candidates (in `candidate_sets`) are expected to have columns as per [`super::generation::CandidateGeneration::search`] documentation. Any additional columns are expected to be common across all [`SendableRecordBatchStream`] in `candidate_sets`.
#[async_trait]
pub trait CandidateAggregation: Sync + Send {
    /// Returns the number of candidates each input should provide before aggregation.
    ///
    /// Most aggregations only need the final result limit. Algorithms that can
    /// improve recall by considering additional candidates, such as RRF, should
    /// override this while the final `limit` remains the user-visible cap.
    #[must_use]
    fn candidate_pool_size(&self, limit: usize) -> usize {
        limit
    }

    /// Consumes `generation_results` and decides how to order the underlying [`SendableRecordBatchStream`] data into a single [`SendableRecordBatchStream`].
    ///
    /// Expect `data` to be non empty, and one [`VectorSearchGenerationResult::data`] to be non-empty.
    async fn aggregate(
        &self,
        mut data: Vec<VectorSearchGenerationResult>,
        primary_keys: Vec<Column>,
        limit: usize,
    ) -> Result<AggregationResult>;
}

pub struct AggregationResult {
    pub data: SendableRecordBatchStream,

    /// Primary key column name(s) in `data`.
    pub primary_key: Vec<String>,

    /// Additional columns names in `data`.
    /// Note: This does not include the [`SEARCH_SCORE_COLUMN_NAME`] column.
    pub data_columns: Vec<String>,

    /// Map of underlying table column (which may not be in `data_columns`, but the underlying table)
    /// to all the columns in `data` that derived from it.
    ///
    /// Example
    /// ```json
    /// {
    ///   "body": ["body_fts", "body_similarity"]
    /// }
    /// ```
    /// Note: `"body"` need not be in `data_columns`.
    pub matches: HashMap<String, Vec<String>>,
}

impl std::fmt::Debug for AggregationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregationResult")
            .field("schema", &self.data.schema())
            .field("primary_keys", &self.primary_key.as_slice())
            .field("data", &self.data_columns.as_slice())
            .field("matches", &self.matches)
            .finish_non_exhaustive()
    }
}

fn from_single_input(
    input: VectorSearchGenerationResult,
    primary_key: Vec<Column>,
) -> AggregationResult {
    let VectorSearchGenerationResult {
        data,
        derived_from: derived_column,
    } = input;

    let primary_key: Vec<_> = primary_key.into_iter().map(|c| c.flat_name()).collect();

    // Results from [`super::generation::CandidateGeneration::search`] outputs the matches as the
    // `SEARCH_VALUE_COLUMN_NAME` column, so we directly know the mapping.
    let mut matches = HashMap::new();
    matches.insert(derived_column, vec![SEARCH_VALUE_COLUMN_NAME.to_string()]);

    // All remaining columns in the data are considered additional columns.
    let data_columns: Vec<_> = data
        .schema()
        .fields()
        .iter()
        .filter_map(|f| {
            if f.name() == SEARCH_SCORE_COLUMN_NAME
                || f.name() == SEARCH_VALUE_COLUMN_NAME
                || primary_key.contains(f.name())
            {
                None
            } else {
                Some(f.name().clone())
            }
        })
        .collect();

    AggregationResult {
        data,
        primary_key,
        data_columns,
        matches,
    }
}

impl AggregationResult {
    /// Returns a row-based JSON representation of the primary key column(s) in a [`RecordBatch`].
    pub fn primary_key_json(&self, rb: &RecordBatch) -> Result<Vec<HashMap<String, Value>>> {
        self.columns_as_json(rb, &self.primary_key)
    }

    /// Returns a row-based JSON representation of the data columns in a [`RecordBatch`].
    pub fn data_json(&self, rb: &RecordBatch) -> Result<Vec<HashMap<String, Value>>> {
        self.columns_as_json(rb, &self.data_columns)
    }

    /// Returns the [`SEARCH_SCORE_COLUMN_NAME`] values from a [`RecordBatch`].
    pub fn score_values(&self, rb: &RecordBatch) -> Result<Vec<f64>> {
        let Some(scores) = rb.column_by_name(SEARCH_SCORE_COLUMN_NAME) else {
            return Err(Error::InconsistentAggregationResult {
                source: Box::from("No scores returned in search result aggregation".to_string()),
            });
        };

        if let Some(col) = scores.as_any().downcast_ref::<arrow::array::Float64Array>() {
            match col.iter().collect::<Option<Vec<f64>>>() {
                Some(v) => Ok(v),
                None => Err(Error::InconsistentAggregationResult {
                    source: Box::from(
                        "No scores returned in search result aggregation".to_string(),
                    ),
                }),
            }
        } else {
            Err(Error::InconsistentAggregationResult {
                source: Box::from("No scores returned in search result aggregation".to_string()),
            })
        }
    }

    /// Converts a subset of columns from a [`RecordBatch`] into a row-based JSON representation.
    pub fn columns_as_json(
        &self,
        rb: &RecordBatch,
        cols: &[String],
    ) -> Result<Vec<HashMap<String, Value>>> {
        let idx = cols
            .iter()
            .filter_map(|c| {
                let (a, _) = rb.schema().column_with_name(c)?;
                Some(a)
            })
            .collect::<Vec<_>>();

        let rb = rb
            .project(idx.as_slice())
            .boxed()
            .context(InconsistentAggregationResultSnafu)?;

        let str = write_to_json_string(&[rb]).context(InconsistentAggregationResultSnafu)?;

        serde_json::from_str(&str)
            .boxed()
            .context(InconsistentAggregationResultSnafu)
    }

    /// Provides a user-friendly representation of the result.
    #[must_use]
    pub fn display_json(&self) -> Value {
        json!({
            "primary_key_columns": self.primary_key,
            "additional_columns": self.data_columns,
            "queried_columns": self.matches.keys().collect::<Vec<_>>()
        })
    }
}

pub fn write_to_json_string(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
    writer.finish()?;

    String::from_utf8(writer.into_inner()).boxed()
}

// Construct a [`AggregationResult`] from a single table in a [`SessionContext`].
async fn result_from_table(
    ctx: &SessionContext,
    tbl: &TableReference,
    match_field: &str,
    primary_key: &[String],
) -> Result<AggregationResult> {
    let df = ctx.table(tbl.clone()).await.context(DatafusionSnafu)?;
    let data_columns = df
        .schema()
        .columns()
        .iter()
        .filter_map(|c| {
            let name = c.name().to_string();
            if primary_key.contains(&name) {
                return None;
            }
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str()) {
                return None;
            }
            Some(name)
        })
        .collect();
    let data = df.execute_stream().await.context(DatafusionSnafu)?;

    Ok(AggregationResult {
        data,
        primary_key: primary_key.to_vec(),
        data_columns,
        matches: [(
            match_field.to_string(),
            vec![SEARCH_VALUE_COLUMN_NAME.to_string()],
        )]
        .into(),
    })
}

/// Returns a list of additional columns in the schema that are not part of the primary key or the expected
/// search columns (i.e. score or underlying value).
fn additional_columns_of_schema(schema: &SchemaRef, primary_key: &[Column]) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let name = f.name();
            let col = Column::from_name(f.name());
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str())
                || primary_key.contains(&col)
            {
                return None;
            }
            Some(col)
        })
        .collect()
}

/// Verifies that all streams have the same schema and contain the required columns: [`SEARCH_VALUE_COLUMN_NAME`], [`SEARCH_SCORE_COLUMN_NAME`].
fn verify_schema_compatibility(schemas: &[SchemaRef]) -> Result<()> {
    let Some(schema) = schemas.iter().find(|s| !s.fields.is_empty()) else {
        return Ok(());
    };

    for s in schemas {
        if s.fields().is_empty() {
            // Empty schema -> empty data
            continue;
        }
        if s.column_with_name(SEARCH_VALUE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_VALUE_COLUMN_NAME.to_string(),
            });
        }

        if s.column_with_name(SEARCH_SCORE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_SCORE_COLUMN_NAME.to_string(),
            });
        }

        // Check that the schema is the same across all streams (i.e. all same as the first).
        // Ensure ALL columns are in the first schema with matching types.
        // Note: We don't check nullability because different search sources may have different
        // nullability for the same logical column (e.g., vector search vs full-text search).
        // DataFusion can handle the union/join with different nullability.
        // We use semantic type equality (e.g., Utf8 and LargeUtf8 are compatible) because
        // different search sources may produce different string representations (e.g., chunked
        // text produces LargeUtf8 while non-chunked produces Utf8).
        let correct_columns = s.fields().iter().all(|f| {
            let Some((_, f2)) = schema.column_with_name(f.name()) else {
                return false;
            };
            are_types_compatible(f2.data_type(), f.data_type())
        });
        if schema.fields().len() != s.fields().len() || !correct_columns {
            return Err(Error::InconsistentColumns {
                s1: Arc::clone(schema),
                s2: Arc::clone(s),
            });
        }
    }

    Ok(())
}

fn ith_search_value_column(i: usize) -> String {
    format!("{SEARCH_VALUE_COLUMN_NAME}_{i}")
}

/// Checks if two data types are compatible for schema aggregation purposes.
///
/// String types (`Utf8`, `LargeUtf8`, `Utf8View`) are considered compatible because
/// different search sources may produce different string representations:
/// - Non-chunked columns typically produce `Utf8`
/// - Chunked columns (via substring operations) may produce `LargeUtf8`
/// - View-based outputs may produce `Utf8View`
///
/// DataFusion can handle these differences during JOIN/COALESCE operations via implicit casting.
fn are_types_compatible(t1: &DataType, t2: &DataType) -> bool {
    if t1 == t2 {
        return true;
    }

    // Treat all string types as compatible
    let is_string_type =
        |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);

    if is_string_type(t1) && is_string_type(t2) {
        return true;
    }

    false
}
