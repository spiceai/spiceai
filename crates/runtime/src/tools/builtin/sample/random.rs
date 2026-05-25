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

use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

use super::SampleFrom;
use arrow::{array::RecordBatch, compute::concat_batches};
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use runtime_datafusion::query_engine::{QueryEngine, QueryRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::Span;
use tracing_futures::Instrument;
use util::security::quote_table_reference;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RandomSampleParams {
    #[serde(rename = "dataset")]
    /// The SQL dataset to sample data from.
    pub tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    pub limit: usize,
}

impl Display for RandomSampleParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

impl SampleFrom for RandomSampleParams {
    async fn sample(
        &self,
        df: Arc<dyn QueryEngine>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let current_span = Span::current();
        let tbl = TableReference::parse_str(self.tbl.as_str());
        let tbl_quoted = quote_table_reference(&tbl);

        let batches = async {
            let stream = df
                .execute_query(QueryRequest::new(format!(
                    "SELECT * FROM {tbl} LIMIT {limit}",
                    limit = self.limit,
                    tbl = tbl_quoted,
                )))
                .await?;

            stream
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
        }
        .instrument(current_span)
        .await?;

        let schema = Arc::new(df.get_arrow_schema(tbl).await?);

        concat_batches(&schema, batches.iter())
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }
}
