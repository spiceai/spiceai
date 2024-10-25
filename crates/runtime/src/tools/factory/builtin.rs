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

use std::{collections::HashMap, sync::Arc};

use secrecy::SecretString;
use spicepod::component::tool::Tool;

use crate::tools::{
    builtin::{
        document_similarity::DocumentSimilarityTool,
        get_readiness::GetReadinessTool,
        list_datasets::ListDatasetsTool,
        sample::{tool::SampleDataTool, SampleTableMethod},
        sql::SqlTool,
        table_schema::TableSchemaTool,
    },
    SpiceModelTool,
};

use super::ToolFactory;

pub struct BuiltinToolFactory {}

/// Builtin tools must also be added to [`crate::tools::get_builtin_tools`] and [`crate::tools::get_builtin_tool_spec`].
impl ToolFactory for BuiltinToolFactory {
    fn construct(
        &self,
        component: &Tool,
        _params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(("builtin", id)) = component.from.split_once(':') else {
            return Err(format!(
                "Invalid component `from` field. Expected: `builtin:<tool_id>`. Error: {}",
                component.from
            )
            .into());
        };

        let name = component.name.clone();
        let description = component.description.clone();

        match id {
            "get_readiness" => Ok(Arc::new(GetReadinessTool::new(&name, description))),
            "document_similarity" => Ok(Arc::new(DocumentSimilarityTool::new(&name, description))),
            "table_schema" => Ok(Arc::new(TableSchemaTool::new(&name, description))),
            "sql" => Ok(Arc::new(SqlTool::new(&name, description))),
            "sample_distinct_columns" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::DistinctColumns)
                    .with_overrides(Some(name.as_str()), description.as_deref()),
            )),
            "random_sample" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::RandomSample)
                    .with_overrides(Some(name.as_str()), description.as_deref()),
            )),
            "top_n_sample" => Ok(Arc::new(
                SampleDataTool::new(SampleTableMethod::TopNSample)
                    .with_overrides(Some(name.as_str()), description.as_deref()),
            )),
            "list_datasets" => {
                let table_allowlist: Option<Vec<&str>> = component
                    .params
                    .get("table_allowlist")
                    .map(|t| t.split(',').map(str::trim).collect());
                Ok(Arc::new(ListDatasetsTool::new(
                    &name,
                    description,
                    table_allowlist,
                )))
            }
            _ => Err(format!("Unknown builtin tool: {id}").into()),
        }
    }
}
