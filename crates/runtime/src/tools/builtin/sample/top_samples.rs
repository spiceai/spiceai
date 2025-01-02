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
use crate::datafusion::DataFusion;
use arrow::{array::RecordBatch, compute::concat_batches};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TopSamplesParams {
    #[serde(rename = "dataset")]
    /// The SQL dataset to sample data from.
    pub tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    pub limit: usize,

    /// How to order the samples before retrieving the top N.
    pub order_by: String,
}

impl Display for TopSamplesParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

impl SampleFrom for TopSamplesParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let order_by = if self.order_by.to_lowercase().starts_with("order by") {
            self.order_by.clone()[8..].to_string()
        } else {
            self.order_by.clone()
        };

        let batches = df
            .query_builder(&format!(
                "SELECT * FROM {tbl} ORDER BY {order_by} LIMIT {limit}",
                limit = self.limit,
                tbl = self.tbl,
            ))
            .build()
            .run()
            .await
            .boxed()?
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .boxed()?;

        let schema = Arc::new(df.get_arrow_schema(self.tbl.as_str()).await.boxed()?);

        concat_batches(&schema, batches.iter().collect::<Vec<&RecordBatch>>()).boxed()
    }
}
