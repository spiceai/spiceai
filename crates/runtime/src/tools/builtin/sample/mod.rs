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
    /// Given the parameters for sampling data, return a [`RecordBatch`] with the sampled data.
    fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> impl std::future::Future<
        Output = Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;
}

#[derive(Debug, Clone, JsonSchema, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum SampleTableMethod {
    #[serde(rename = "distinct")]
    DistinctColumns,

    #[serde(rename = "random")]
    RandomSample,

    #[serde(rename = "top_n")]
    TopNSample,
}

impl SampleTableMethod {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            SampleTableMethod::DistinctColumns => "sample_distinct_columns",
            SampleTableMethod::RandomSample => "random_sample",
            SampleTableMethod::TopNSample => "top_n_sample",
        }
    }

    #[must_use]
    pub fn description(&self) -> &str {
        match self {
            SampleTableMethod::DistinctColumns => {
                "Generate synthetic data by sampling distinct column values from a table."
            }
            SampleTableMethod::RandomSample => "Sample random rows from a table.",
            SampleTableMethod::TopNSample => {
                "Sample the top N rows from a table based on a specified ordering"
            }
        }
    }

    /// For the given method, attempt to parse the arguments into the appropriate [`SampleTableParams`].
    pub fn parse_args(&self, args: &str) -> Result<SampleTableParams, serde_json::Error> {
        match self {
            SampleTableMethod::DistinctColumns => Ok(SampleTableParams::DistinctColumns(
                serde_json::from_str(args)?,
            )),
            SampleTableMethod::RandomSample => {
                Ok(SampleTableParams::RandomSample(serde_json::from_str(args)?))
            }
            SampleTableMethod::TopNSample => {
                Ok(SampleTableParams::TopNSample(serde_json::from_str(args)?))
            }
        }
    }
}

/// The unique parameters for sampling data for a given [`SampleTableMethod`] tool.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SampleTableParams {
    TopNSample(TopSamplesParams),
    DistinctColumns(DistinctColumnsParams),
    RandomSample(RandomSampleParams),
}

impl From<&SampleTableParams> for SampleTableMethod {
    fn from(params: &SampleTableParams) -> Self {
        match params {
            SampleTableParams::DistinctColumns(_) => SampleTableMethod::DistinctColumns,
            SampleTableParams::RandomSample(_) => SampleTableMethod::RandomSample,
            SampleTableParams::TopNSample(_) => SampleTableMethod::TopNSample,
        }
    }
}

impl SampleTableParams {
    #[must_use]
    pub fn dataset(&self) -> &str {
        match self {
            SampleTableParams::DistinctColumns(params) => &params.tbl,
            SampleTableParams::RandomSample(params) => &params.tbl,
            SampleTableParams::TopNSample(params) => &params.tbl,
        }
    }
}

impl Display for SampleTableParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SampleTableParams::DistinctColumns(params) => write!(f, "DistinctColumns({params})"),
            SampleTableParams::RandomSample(params) => write!(f, "RandomSample({params})"),
            SampleTableParams::TopNSample(params) => write!(f, "TopNSample({params})"),
        }
    }
}

impl SampleFrom for SampleTableParams {
    async fn sample(
        &self,
        df: Arc<DataFusion>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            SampleTableParams::DistinctColumns(params) => params.sample(df).await,
            SampleTableParams::RandomSample(params) => params.sample(df).await,
            SampleTableParams::TopNSample(params) => params.sample(df).await,
        }
    }
}
