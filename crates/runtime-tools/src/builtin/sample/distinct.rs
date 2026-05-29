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
use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use datafusion::sql::TableReference;
use itertools::Itertools;
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use tracing::Span;
use tracing_futures::Instrument;
use util::security::{quote_sql_identifier, quote_table_reference};

use arrow::compute::concat;
use futures::{StreamExt, TryStreamExt};
use runtime_query_engine::query_engine::{QueryEngine, QueryRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::SampleFrom;

// Max columns for parallel distinct sampling per tool call
const MAX_CONCURRENT_COLUMNS_SCANS: usize = 5;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DistinctColumnsParams {
    #[serde(rename = "dataset")]
    /// The SQL dataset to sample data from.
    pub tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    pub limit: usize,

    /// The columns to sample distinct values from. If None, all columns are sampled.
    pub cols: Option<Vec<String>>,
}

impl Display for DistinctColumnsParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

impl DistinctColumnsParams {
    /// Sample distinct values from a column in a table.
    /// For the number of distinct values, d
    ///  - If `d < n`, all distinct values are returned, concatenated with `n - d` duplicate rows.
    ///  - If `d >= n`, `n` distinct values are sampled, but no guarantee on which rows are returned.
    async fn sample_distinct_from_column(
        df: Arc<dyn QueryEngine>,
        tbl: &TableReference,
        column: &str,
        n: usize,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        // Ensure that we still get `n` rows when `len(distinct(col)) < n`, whilst
        // stilling getting all possible distinct values.
        let tbl_quoted = quote_table_reference(tbl);
        let col = quote_sql_identifier(column);
        Self::_sample_col(
            Arc::clone(&df),
            &format!(
                "SELECT {col} FROM (
                SELECT {col}, 1 as priority
                FROM (SELECT DISTINCT {col} FROM {tbl_quoted})
                UNION ALL
                SELECT {col}, 2 as priority
                FROM {tbl_quoted}
            ) combined
            ORDER BY priority, {col}
            LIMIT {n}"
            ),
        )
        .await
    }

    async fn sample_from_column(
        df: Arc<dyn QueryEngine>,
        tbl: &TableReference,
        col: &str,
        n: usize,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        let tbl_quoted = quote_table_reference(tbl);
        let col = quote_sql_identifier(col);
        Self::_sample_col(
            Arc::clone(&df),
            &format!("SELECT {col} FROM {tbl_quoted} LIMIT {n}"),
        )
        .await
    }

    async fn _sample_col(
        df: Arc<dyn QueryEngine>,
        query: &str,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        let stream = df.execute_query(QueryRequest::new(query)).await?;

        let column = stream
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
            .iter()
            .map(|batch| Arc::clone(batch.column(0)))
            .collect_vec();

        let array_slices: Vec<&dyn arrow::array::Array> =
            column.iter().map(AsRef::as_ref).collect();

        concat(array_slices.as_slice())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }
}

impl SampleFrom for DistinctColumnsParams {
    async fn sample(
        &self,
        df: Arc<dyn QueryEngine>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let tbl = TableReference::parse_str(self.tbl.as_str());
        let Some(provider) = df.get_table(&tbl).await else {
            return Err("Table not found".into());
        };

        let schema = provider.schema();

        let columns = schema.fields();

        let mut result: Vec<ArrayRef> = Vec::with_capacity(columns.len());

        let current_span = Span::current();

        let data_sample_futures = columns.iter().map(|column| {
            let tbl = tbl.clone();
            let df = Arc::clone(&df);
            let span = current_span.clone();
            async move {
                // Only sample distinctly from columns that are specified in the `cols` field, if `cols` is None and distinct sampling is supported
                if column_supports_distinct_sampling(column)
                    && (self.cols.is_none()
                        || self
                            .cols
                            .as_ref()
                            .is_some_and(|cols| cols.contains(column.name())))
                {
                    Self::sample_distinct_from_column(df, &tbl, column.name(), self.limit).await
                } else {
                    Self::sample_from_column(df, &tbl, column.name(), self.limit).await
                }
            }
            .instrument(span)
        });

        let data_samples = futures::stream::iter(data_sample_futures)
            .boxed()
            // Use `buffered` (ordered) to preserve column order to match the schema.
            .buffered(MAX_CONCURRENT_COLUMNS_SCANS)
            .try_collect::<Vec<_>>()
            .await?;

        result.extend(data_samples);

        // Sampling data will return at most `limit` rows, but may return fewer if the table has fewer rows or empty
        let mut min_sample_rows_count = self.limit;
        for column_data in &result {
            min_sample_rows_count = min_sample_rows_count.min(column_data.len());
        }

        // If the number of rows sampled is less than the limit, ensure that all columns have the same length
        // as different samling methods may return different number of rows in this case.
        // It is safe to truncate as such rows contain duplicate values
        if min_sample_rows_count < self.limit {
            result = result
                .into_iter()
                .map(|col| col.slice(0, min_sample_rows_count))
                .collect();
        }

        RecordBatch::try_new(schema, result).boxed()
    }
}

fn column_supports_distinct_sampling(column: &arrow_schema::Field) -> bool {
    // We can only sample distinct values from types that implements SortField
    !matches!(
        column.data_type(),
        DataType::FixedSizeList(_, _)
            | DataType::List(_)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Union(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use datafusion::datasource::MemTable;

    #[tokio::test]
    async fn samples_reserved_keyword_column_names() {
        let runtime = crate::Runtime::builder().build().await;
        let df = runtime.datafusion();
        let schema = Arc::new(Schema::new(vec![
            Field::new("interval", DataType::Int64, false),
            Field::new("group", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef,
            ],
        )
        .expect("test batch should be valid");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
            .expect("mem table should be valid");
        df.ctx
            .register_table("audiences", Arc::new(table))
            .expect("test table should register");

        let params = DistinctColumnsParams {
            tbl: "audiences".to_string(),
            limit: 3,
            cols: None,
        };

        let df = Arc::clone(&df) as Arc<dyn QueryEngine>;
        let sample = params
            .sample(Arc::clone(&df))
            .await
            .expect("sampling keyword columns should succeed");

        assert_eq!(sample.num_columns(), 2);
        assert_eq!(sample.num_rows(), 3);
    }
}
