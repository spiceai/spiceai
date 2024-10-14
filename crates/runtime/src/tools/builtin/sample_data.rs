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
use arrow::{
    array::{ArrayRef, RecordBatch},
    util::pretty::pretty_format_batches,
};
use async_trait::async_trait;
use datafusion::sql::TableReference;
use itertools::Itertools;
use std::{collections::HashMap, sync::Arc};

use crate::{
    datafusion::{query::Protocol, DataFusion},
    tools::{parameters, SpiceModelTool},
    Runtime,
};
use arrow::compute::concat;
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SampleDataToolParams {
    /// The SQL datasets to sample data from. If None, search across all public datasets.
    datasets: Option<Vec<String>>,

    /// The number of rows, each with distinct values per column, to sample.
    n: usize,
}

pub struct SampleDataTool {
    name: String,
    description: Option<String>,
}

impl SampleDataTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }

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
        let query = format!(
            "SELECT {col} FROM (
                SELECT {col}, 1 as priority
                FROM (SELECT DISTINCT {col} FROM {tbl})
                UNION ALL
                SELECT {col}, 2 as priority
                FROM {tbl}
            ) combined
            ORDER BY priority, {col}
            LIMIT {n}"
        );

        let result = df
            .query_builder(&query, Protocol::Internal)
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

    /// Sample data from a SQL dataset
    ///
    /// Returns a sample of the data from the SQL dataset where each column has `n` distinct values.
    ///
    pub async fn sample(
        df: Arc<DataFusion>,
        tbl: &TableReference,
        n: usize,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
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
            let column_data =
                Self::sample_distinct_from_column(Arc::clone(&df), tbl, column, n).await?;
            result.insert(i, column_data);
        }

        RecordBatch::try_new(Arc::clone(&schema), result).boxed()
    }
}

impl Default for SampleDataTool {
    fn default() -> Self {
        Self::new(
            "sample_data",
            Some("Return a sample of SQL data".to_string()),
        )
    }
}

impl From<SampleDataTool> for spicepod::component::tool::Tool {
    fn from(val: SampleDataTool) -> Self {
        spicepod::component::tool::Tool {
            from: format!("builtin:{}", val.name()),
            name: val.name().to_string(),
            description: val.description().map(ToString::to_string),
            params: HashMap::default(),
            depends_on: Vec::default(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for SampleDataTool {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SampleDataToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sample_data", tool = self.name(), input = arg);

        async {
            let req: SampleDataToolParams = serde_json::from_str(arg)?;

            let mut results = serde_json::Map::new();

            let tables = req
                .datasets
                .map(|ds| ds.iter().map(TableReference::from).collect_vec())
                .unwrap_or(rt.datafusion().get_user_table_names());

            for tbl in tables {
                let result = Self::sample(rt.datafusion(), &tbl, req.n).await?;
                let serial = pretty_format_batches(&[result]).boxed()?;
                results.insert(tbl.to_string(), Value::String(format!("{serial}")));
            }

            Ok(Value::Object(results))
        }
        .instrument(span.clone())
        .await
    }
}
