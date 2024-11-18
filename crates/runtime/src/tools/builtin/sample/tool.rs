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
use crate::{
    tools::{utils::parameters, SpiceModelTool},
    Runtime,
};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use serde_json::Value;
use snafu::ResultExt;
use std::sync::Arc;
use tracing::Span;
use tracing_futures::Instrument;

use super::{
    distinct::DistinctColumnsParams, RandomSampleParams, SampleFrom, SampleTableMethod,
    TopSamplesParams,
};

/// A tool to sample data from a table in a variety of ways. How data is sampled is determined by
/// the [`ExploreTableMethod`] and the corresponding [`SampleFrom`].
pub struct SampleDataTool {
    method: SampleTableMethod,

    // Overrides
    name: Option<String>,
    description: Option<String>,
}

impl SampleDataTool {
    #[must_use]
    pub fn new(method: SampleTableMethod) -> Self {
        Self {
            method,
            name: None,
            description: None,
        }
    }

    #[must_use]
    pub fn with_overrides(mut self, name: Option<&str>, description: Option<&str>) -> Self {
        self.name = name.map(ToString::to_string);
        self.description = description.map(ToString::to_string);
        self
    }
}

#[async_trait]
impl SpiceModelTool for SampleDataTool {
    fn name(&self) -> &str {
        match self.name {
            Some(ref name) => name,
            None => self.method.name(),
        }
    }

    fn description(&self) -> Option<&str> {
        match self.description {
            Some(ref desc) => Some(desc.as_str()),
            None => Some(self.method.description()),
        }
    }

    fn parameters(&self) -> Option<Value> {
        match &self.method {
            SampleTableMethod::DistinctColumns => parameters::<DistinctColumnsParams>(),
            SampleTableMethod::RandomSample => parameters::<RandomSampleParams>(),
            SampleTableMethod::TopNSample => parameters::<TopSamplesParams>(),
        }
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let params = self.method.parse_args(arg).boxed()?;
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sample_data", tool = self.name(), input = format!("{params}"), sample_method = self.method.name());

        async {
            let batch = params.sample(rt.datafusion()).await?;
            let serial = pretty_format_batches(&[batch]).boxed()?;

            Ok(Value::String(format!("{serial}")))
        }
        .instrument(span.clone())
        .await
    }
}
