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

use crate::datafusion::DataFusion;
use arrow::array::RecordBatch;
use distinct::DistinctColumnsParams;
use random::RandomSampleParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use top_samples::TopSamplesParams;

pub mod distinct;
pub mod random;
pub mod tool;
pub mod top_samples;

pub trait SampleFrom: Send + Sync {
    /// Given the parameters for sampling data, return a RecordBatch with the sampled data.
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExploreTableMethod {
    DistinctColumns,
    RandomSample,
    TopNSample,
}

impl ExploreTableMethod {
    pub fn name(&self) -> &str {
        match self {
            ExploreTableMethod::DistinctColumns => "sample_distinct_columns",
            ExploreTableMethod::RandomSample => "random_sample",
            ExploreTableMethod::TopNSample => "top_n_sample",
        }
    }

    pub fn description(&self) -> &str {
        match self {
            ExploreTableMethod::DistinctColumns => "Sample distinct values from a table.",
            ExploreTableMethod::RandomSample => "Sample random rows from a table.",
            ExploreTableMethod::TopNSample => "Sample the top N rows from a table.",
        }
    }
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExploreTableParams {
    DistinctColumns(DistinctColumnsParams),
    RandomSample(RandomSampleParams),
    TopNSample(TopSamplesParams),
}

impl SampleFrom for ExploreTableParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ExploreTableParams::DistinctColumns(params) => params.sample(df).await,
            ExploreTableParams::RandomSample(params) => params.sample(df).await,
            ExploreTableParams::TopNSample(params) => params.sample(df).await,
        }
    }
}
