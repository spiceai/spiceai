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
use arrow::array::{ArrayRef, RecordBatch};
use datafusion::sql::TableReference;
use itertools::Itertools;
use std::sync::Arc;

use crate::datafusion::{query::Protocol, DataFusion};
use arrow::compute::concat;
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use super::SampleFrom;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct DistinctColumnsParams {
    /// The SQL dataset to sample data from.
    pub tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    pub limit: usize,

    /// The columns to sample distinct values from. If None, all columns are sampled.
    pub cols: Option<Vec<String>>,
}

impl DistinctColumnsParams {
    /// Sample distinct values from a column in a table.
    /// For the number of distinct values, d
    ///  - If `d < n`, all distinct values are returned, concatenated with `n - d` duplicate rows.
    ///  - If `d >= n`, `n` distinct values are sampled, but no guarantee on which rows are returned.
    async fn sample_distinct_from_column(
        df: Arc<DataFusion>,
        tbl: &TableReference,
        col: &str,
        n: usize,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        // Ensure that we still get `n` rows when `len(distinct(col)) < n`, whilst
        // stilling getting all possible distinct values.
        Self::_sample_col(
            Arc::clone(&df),
            &format!(
                "SELECT {col} FROM (
                SELECT {col}, 1 as priority
                FROM (SELECT DISTINCT {col} FROM {tbl})
                UNION ALL
                SELECT {col}, 2 as priority
                FROM {tbl}
            ) combined
            ORDER BY priority, {col}
            LIMIT {n}"
            ),
        )
        .await
    }

    async fn sample_from_column(
        df: Arc<DataFusion>,
        tbl: &TableReference,
        col: &str,
        n: usize,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        Self::_sample_col(
            Arc::clone(&df),
            &format!("SELECT {col} FROM {tbl} LIMIT {n}"),
        )
        .await
    }

    async fn _sample_col(
        df: Arc<DataFusion>,
        query: &str,
    ) -> Result<ArrayRef, Box<dyn std::error::Error + Send + Sync>> {
        let result = df
            .query_builder(query, Protocol::Internal)
            .build()
            .run()
            .await
            .boxed()?;

        let column = result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .boxed()?
            .iter()
            .map(|batch| Arc::clone(batch.column(0)))
            .collect_vec();

        let array_slices: Vec<&dyn arrow::array::Array> =
            column.iter().map(AsRef::as_ref).collect();

        concat(array_slices.as_slice()).boxed()
    }
}

impl SampleFrom for DistinctColumnsParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let tbl = TableReference::from(self.tbl.clone());
        let Some(provider) = df.get_table(tbl.clone()).await else {
            return Err("Table not found".into());
        };

        let schema = provider.schema();

        let columns = schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<String>>();

        let mut result: Vec<ArrayRef> = Vec::with_capacity(columns.len());

        for (i, column) in columns.iter().enumerate() {
            // Only sample distinctly from columns that are specified in the `cols` field, or if `cols` is None.
            let column_data = if self.cols.is_none()
                || self.cols.as_ref().is_some_and(|cols| cols.contains(column))
            {
                Self::sample_distinct_from_column(Arc::clone(&df), &tbl, column, self.limit).await?
            } else {
                Self::sample_from_column(Arc::clone(&df), &tbl, column, self.limit).await?
            };
            result.insert(i, column_data);
        }

        RecordBatch::try_new(Arc::clone(&schema), result).boxed()
    }
}
