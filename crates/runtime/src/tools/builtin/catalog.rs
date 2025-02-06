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

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::tools::{catalog::SpiceToolCatalog, factory::ToolFactory, options::SpiceToolsOptions};

use super::{
    document_similarity::DocumentSimilarityTool,
    get_readiness::GetReadinessTool,
    list_datasets::ListDatasetsTool,
    sample::{tool::SampleDataTool, SampleTableMethod},
    sql::SqlTool,
    table_schema::TableSchemaTool,
    web_search::WebSearchTool,
    SpiceModelTool,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unknown builtin tool: {id}"))]
    UnknownBuiltinTool { id: String },

    #[snafu(display("Failed to construct tool '{id}'. Error: {source}"))]
    FailedToConstructTool {
        id: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct BuiltinToolCatalog {}

impl BuiltinToolCatalog {
    pub(crate) fn construct_builtin(
        id: &str,
        name: Option<&str>,
        description: Option<String>,
        params: &HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            "websearch" => Ok(Arc::new(
                WebSearchTool::try_new(name, description, params)
                    .context(FailedToConstructToolSnafu { id: id.to_string() })?,
            )),
            "get_readiness" => Ok(Arc::new(GetReadinessTool::new(name, description))),
            "document_similarity" => Ok(Arc::new(DocumentSimilarityTool::new(name, description))),
            "table_schema" => Ok(Arc::new(TableSchemaTool::new(name, description))),
            "sql" => Ok(Arc::new(SqlTool::new(name, description))),
            "sample_distinct_columns" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::DistinctColumns)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "random_sample" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::RandomSample)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "top_n_sample" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::TopNSample)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "list_datasets" => {
                let table_allowlist: Option<Vec<&str>> = params
                    .get("table_allowlist")
                    .map(|t| t.expose_secret().split(',').map(str::trim).collect());
                Ok(Arc::new(ListDatasetsTool::new(
                    name,
                    description,
                    table_allowlist,
                )))
            }
            _ => Err(Error::UnknownBuiltinTool { id: id.to_string() }),
        }
    }
}

impl ToolFactory for BuiltinToolCatalog {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let id = component
            .from
            .split_once(':')
            .map_or(component.from.as_str(), |(_, id)| id);

        Self::construct_builtin(
            id,
            Some(component.name.as_str()),
            component.description.clone(),
            &params_with_secrets,
        )
        .boxed()
    }
}

#[async_trait]
impl SpiceToolCatalog for BuiltinToolCatalog {
    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        let mut tools = vec![];
        for t in SpiceToolsOptions::Auto.tools_by_name() {
            match Self::construct_builtin(t, None, None, &HashMap::new()) {
                Ok(tool) => tools.push(tool),
                Err(e) => tracing::warn!("Failed to construct builtin tool: '{}'. Error: {}", t, e),
            }
        }
        tools
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        Self::construct_builtin(name, None, None, &HashMap::new()).ok()
    }

    fn name(&self) -> &'static str {
        "auto"
    }
}
