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
use crate::{
    datafusion::DataFusion,
    tools::{SpiceModelTool, utils::parameters},
};
use arrow::util::pretty::pretty_format_batches;
use arrow_tools::record_batch::{truncate_numeric_column_length, truncate_string_columns};
use async_trait::async_trait;
use datafusion::sql::TableReference;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing::Span;
use tracing_futures::Instrument;

use super::{
    RandomSampleParams, SampleFrom, SampleTableMethod, TopSamplesParams,
    distinct::DistinctColumnsParams,
};

/// A tool to sample data from a table in a variety of ways. How data is sampled is determined by
/// the [`ExploreTableMethod`] and the corresponding [`SampleFrom`].
pub struct SampleDataTool {
    method: SampleTableMethod,

    df: Arc<DataFusion>,

    // Overrides
    name: Option<String>,
    description: Option<String>,

    table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

impl SampleDataTool {
    #[must_use]
    pub fn new(df: Arc<DataFusion>, method: SampleTableMethod) -> Self {
        Self {
            df,
            method,
            name: None,
            description: None,
            table_allowlist: None,
        }
    }

    #[must_use]
    pub fn with_overrides(mut self, name: Option<&str>, description: Option<&str>) -> Self {
        self.name = name.map(ToString::to_string);
        self.description = description.map(ToString::to_string);
        self
    }

    #[must_use]
    pub fn with_table_allowlist(mut self, allowlist: Option<ResolvedTableAwareAllowlist>) -> Self {
        self.table_allowlist = allowlist;
        self
    }
}

#[async_trait]
impl SpiceModelTool for SampleDataTool {
    fn name(&self) -> Cow<'_, str> {
        match self.name {
            Some(ref name) => name.into(),
            None => self.method.name().into(),
        }
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        match self.description {
            Some(ref desc) => Some(desc.into()),
            None => Some(self.method.description().into()),
        }
    }

    fn parameters(&self) -> Option<Value> {
        match &self.method {
            SampleTableMethod::DistinctColumns => parameters::<DistinctColumnsParams>(),
            SampleTableMethod::RandomSample => parameters::<RandomSampleParams>(),
            SampleTableMethod::TopNSample => parameters::<TopSamplesParams>(),
        }
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let params = self.method.parse_args(arg).boxed()?;
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sample_data", tool = self.name().to_string(), input = format!("{params}"), sample_method = self.method.name());

        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            // Check table allowlist before sampling
            if let Some(ref allowlist) = self.table_allowlist {
                let table_ref = TableReference::parse_str(params.dataset());
                if !allowlist.table_is_allowed(&table_ref) {
                    return Err("Table not found".into());
                }
            }

            let mut batch = params.sample(Arc::clone(&self.df)).await?;

            // truncate large text fields
            batch = truncate_string_columns(&batch, 512)?;
            batch = truncate_numeric_column_length(&batch, 8)?;

            let serial = pretty_format_batches(&[batch]).boxed()?;
            Ok(Value::String(format!("{serial}")))
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
