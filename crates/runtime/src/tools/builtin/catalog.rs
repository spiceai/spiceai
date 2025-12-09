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

use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::{
    Runtime,
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    tools::{
        catalog::SpiceToolCatalog, factory::IndividualToolFactory, options::SpiceToolsOptions,
    },
};

use super::{
    SpiceModelTool,
    get_readiness::GetReadinessTool,
    list_datasets::ListDatasetsTool,
    sample::{SampleTableMethod, tool::SampleDataTool},
    search::SearchTool,
    sql::SqlTool,
    table_schema::TableSchemaTool,
    web_search::WebSearchTool,
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

pub struct BuiltinToolCatalog {
    rt: Arc<Runtime>,
}
impl BuiltinToolCatalog {
    pub(crate) fn new(rt: Arc<Runtime>) -> Self {
        Self { rt }
    }

    pub(crate) fn name() -> &'static str {
        "auto"
    }

    pub(crate) fn construct_builtin(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
        params: &HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);

        // Get default description if none is provided
        let description = match (id, description) {
            (_, Some(desc)) => desc, // Use provided description if available
            ("websearch", None) => "Search the web for information",
            ("get_readiness", None) => "Get the readiness status of the Spice.ai runtime",
            ("search", None) => "Search across available, searchable datasets in Spice.ai runtime",
            ("table_schema", None) => "Get the schema of the Spice.ai dataset",
            ("sql", None) => "Execute SQL queries (PostgreSQL dialect) using the Spice.ai runtime",
            ("sample_distinct_columns", None) => {
                "Sample distinct column values from a Spice.ai dataset"
            }
            ("random_sample", None) => "Get a random sample of rows from a Spice.ai dataset",
            ("top_n_sample", None) => {
                "Get top N samples from a Spice.ai dataset based on a specified ordering"
            }
            ("list_datasets", None) => "List available datasets",
            (_, None) => "",
        };
        let table_allowlist: Option<ResolvedTableAwareAllowlist> = params
            .get("table_allowlist")
            .map(|t| {
                let tables = t
                    .expose_secret()
                    .split(',')
                    .map(ToString::to_string)
                    .collect::<Vec<String>>();
                ResolvedTableAwareAllowlist::with_defaults(
                    SPICE_DEFAULT_CATALOG,
                    SPICE_DEFAULT_SCHEMA,
                )
                .with_table_patterns(tables)
            })
            .transpose()
            .boxed()
            .context(FailedToConstructToolSnafu { id })?;

        match id {
            "websearch" => Ok(Arc::new(
                WebSearchTool::try_new(name, Some(description), params)
                    .context(FailedToConstructToolSnafu { id: id.to_string() })?,
            )),
            "get_readiness" => Ok(Arc::new(GetReadinessTool::new(
                Arc::clone(&self.rt),
                Some(name),
                Some(description),
            ))),
            "search" => Ok(Arc::new(
                SearchTool::new(Arc::clone(&self.rt), Some(name), Some(description))
                    .with_table_allowlist(table_allowlist),
            )),
            "table_schema" => Ok(Arc::new(
                TableSchemaTool::new(Arc::clone(&self.rt), Some(name), Some(description))
                    .with_table_allowlist(table_allowlist),
            )),
            "sql" => Ok(Arc::new(SqlTool::new(
                self.rt.datafusion(),
                Some(name),
                Some(description),
                table_allowlist,
            ))),
            "sample_distinct_columns" => Ok(Arc::new(
                SampleDataTool::new(self.rt.datafusion(), SampleTableMethod::DistinctColumns)
                    .with_overrides(Some(name), Some(description)),
            )),
            "random_sample" => Ok(Arc::new(
                SampleDataTool::new(self.rt.datafusion(), SampleTableMethod::RandomSample)
                    .with_overrides(Some(name), Some(description)),
            )),
            "top_n_sample" => Ok(Arc::new(
                SampleDataTool::new(self.rt.datafusion(), SampleTableMethod::TopNSample)
                    .with_overrides(Some(name), Some(description)),
            )),
            "list_datasets" => Ok(Arc::new(ListDatasetsTool::new(
                Some(name),
                Some(description),
                table_allowlist,
                Arc::clone(&self.rt),
            ))),
            _ => Err(Error::UnknownBuiltinTool { id: id.to_string() }),
        }
    }
}

impl IndividualToolFactory for BuiltinToolCatalog {
    fn construct(
        &self,
        component: &Tool,
        params_with_secrets: HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>> {
        let id = component
            .from
            .split_once(':')
            .map_or(component.from.as_str(), |(_, id)| id);

        self.construct_builtin(
            id,
            Some(component.name.as_str()),
            component.description.as_deref(),
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
            match self.construct_builtin(t, None, None, &HashMap::new()) {
                Ok(tool) => tools.push(tool),
                Err(e) => tracing::warn!("Failed to construct builtin tool: '{}'. Error: {}", t, e),
            }
        }
        tools
    }

    async fn get(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.construct_builtin(name, None, None, &HashMap::new())
            .ok()
    }

    fn name(&self) -> &str {
        Self::name()
    }
}
