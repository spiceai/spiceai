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

use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{
    tools::{utils::parameters, SpiceModelTool},
    Runtime,
};
use async_trait::async_trait;
use secrecy::SecretString;
use serde_json::Value;
use snafu::ResultExt;
use tracing::Span;
use tracing_futures::Instrument;

use super::{SearchEngine, WebSearchParams};

pub struct WebSearchTool {
    name: Option<String>,
    description: Option<String>,
    engine: SearchEngine,
}

impl WebSearchTool {
    pub fn try_new(
        name: &str,
        description: Option<String>,
        params: &HashMap<String, SecretString>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            name: Some(name.to_string()),
            description,
            engine: SearchEngine::try_from(params)?,
        })
    }
}

#[async_trait]
impl SpiceModelTool for WebSearchTool {
    fn name(&self) -> Cow<'_, str> {
        match &self.name {
            Some(name) => Cow::Borrowed(name),
            None => Cow::Owned(self.engine.engine_type().to_string()),
        }
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        match &self.description {
            Some(ref desc) => Some(desc.into()),
            None => Some(self.engine.engine_type().description().into()),
        }
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<WebSearchParams>()
    }

    async fn call(
        &self,
        arg: &str,
        _rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::websearch", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let req: WebSearchParams = serde_json::from_str(arg)?;
            let resp = self.engine.search(req).await?;
            serde_json::to_value(resp).boxed()
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
