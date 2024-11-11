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

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::tools::{catalog::SpiceToolCatalog, factory::ToolFactory};

use super::{
    document_similarity::DocumentSimilarityTool,
    get_builtin_tools,
    get_readiness::GetReadinessTool,
    list_datasets::ListDatasetsTool,
    sample::{tool::SampleDataTool, SampleTableMethod},
    sql::SqlTool,
    table_schema::TableSchemaTool,
    SpiceModelTool,
};

pub struct BuiltinToolCatalog {}

impl BuiltinToolCatalog {
    // Must be in sync with [`super::get_builtin_tools`].
    pub(crate) fn construct_builtin(
        id: &str,
        name: Option<&str>,
        description: Option<String>,
        params: &HashMap<String, SecretString>,
    ) -> Option<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);
        match id {
            "get_readiness" => Some(Arc::new(GetReadinessTool::new(name, description))),
            "document_similarity" => Some(Arc::new(DocumentSimilarityTool::new(name, description))),
            "table_schema" => Some(Arc::new(TableSchemaTool::new(name, description))),
            "sql" => Some(Arc::new(SqlTool::new(name, description))),
            "sample_distinct_columns" => Some(Arc::new(
                SampleDataTool::new(SampleTableMethod::DistinctColumns)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "random_sample" => Some(Arc::new(
                SampleDataTool::new(SampleTableMethod::RandomSample)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "top_n_sample" => Some(Arc::new(
                SampleDataTool::new(SampleTableMethod::TopNSample)
                    .with_overrides(Some(name), description.as_deref()),
            )),
            "list_datasets" => {
                let table_allowlist: Option<Vec<&str>> = params
                    .get("table_allowlist")
                    .map(|t| t.expose_secret().split(',').map(str::trim).collect());
                Some(Arc::new(ListDatasetsTool::new(
                    name,
                    description,
                    table_allowlist,
                )))
            }
            _ => None,
        }
    }
}

impl ToolFactory for BuiltinToolCatalog {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("builtin", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `builtin:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        Self::construct_builtin(
            id,
            Some(component.name.as_str()),
            component.description.clone(),
            &params_with_secrets,
        )
        .ok_or_else(|| format!("Unknown builtin tool: {id}").into())
    }
}

#[async_trait]
impl SpiceToolCatalog for BuiltinToolCatalog {
    async fn all(&self) -> Vec<Arc<dyn SpiceModelTool>> {
        get_builtin_tools()
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        Self::construct_builtin(name, None, None, &HashMap::new())
    }

    fn name(&self) -> &str {
        "builtin"
    }
}
