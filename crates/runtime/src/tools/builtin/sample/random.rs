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

use std::sync::Arc;

use super::SampleFrom;
use crate::datafusion::{query::Protocol, DataFusion};
use arrow::{array::RecordBatch, compute::concat_batches};
use futures::TryStreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct RandomSampleParams {
    /// The SQL dataset to sample data from.
    tbl: String,
    /// The number of rows, each with distinct values per column, to sample.
    limit: usize,
}

impl SampleFrom for RandomSampleParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let batches = df
            .query_builder(
                &format!(
                    "SELECT * FROM {tbl} LIMIT {limit}",
                    limit = self.limit,
                    tbl = self.tbl
                ),
                Protocol::Internal,
            )
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
