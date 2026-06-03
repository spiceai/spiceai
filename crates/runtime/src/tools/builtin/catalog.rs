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

use runtime_query_engine::allowlist::ResolvedTableAwareAllowlist;
use runtime_query_engine::query_engine::QueryEngine;
use runtime_tools::factory::IndividualToolFactory;
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use spicepod::component::tool::Tool;
use std::{collections::HashMap, sync::Arc};

use crate::status;
use app::App;
use cache::TabledCacheProvider;
use cache::result::search::CachedSearchResult;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use runtime_tools::catalog::SpiceToolCatalog;
use runtime_tools::options::SpiceToolsOptions;
use tokio::sync::RwLock;

use runtime_tools::builtin::get_current_datetime::GetCurrentDateTimeTool;
use runtime_tools::builtin::sample::{SampleTableMethod, tool::SampleDataTool};
use tools::SpiceModelTool;

use runtime_tools::builtin::list_datasets::ListDatasetsTool;
use runtime_tools::builtin::search::SearchTool;
use runtime_tools::builtin::sql::SqlTool;
use runtime_tools::builtin::table_schema::TableSchemaTool;

use super::get_readiness::GetReadinessTool;

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

#[derive(Clone)]
pub struct BuiltinToolCatalog {
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,
    status: Arc<status::RuntimeStatus>,
    search_cache: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,
    /// An optional table allowlist. Overriden by any per-tool `table_allowlist` param.
    model_table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

impl BuiltinToolCatalog {
    pub(crate) fn new(
        df: Arc<dyn QueryEngine>,
        app: Arc<RwLock<Option<Arc<App>>>>,
        status: Arc<status::RuntimeStatus>,
        search_cache: Option<Arc<dyn TabledCacheProvider<CachedSearchResult> + Send + Sync>>,
    ) -> Self {
        Self {
            df,
            app,
            status,
            search_cache,
            model_table_allowlist: None,
        }
    }

    /// Create a new `BuiltinToolCatalog` with a table allowlist applied to all tools.
    #[must_use]
    pub fn with_table_allowlist(mut self, allowlist: ResolvedTableAwareAllowlist) -> Self {
        self.model_table_allowlist = Some(allowlist);
        self
    }

    pub(crate) fn name() -> &'static str {
        "auto"
    }

    pub(crate) fn is_builtin_tool(name: &str) -> bool {
        [
            "get_readiness",
            "get_current_datetime",
            "search",
            "table_schema",
            "sql",
            "sample_distinct_columns",
            "random_sample",
            "top_n_sample",
            "list_datasets",
        ]
        .contains(&name)
    }

    pub(crate) fn construct_builtin(
        &self,
        id: &str,
        name: Option<&str>,
        description: Option<&str>,
        params: &HashMap<String, SecretString>,
    ) -> Result<Arc<dyn SpiceModelTool>> {
        let name = name.unwrap_or(id);

        // Built-in tool defaults live inside each tool's own constructor so the
        // canonical description has a single source of truth. When the operator
        // has not supplied a description, pass `None` through and let the tool
        // pick its default. Otherwise use the operator override verbatim.
        let description: Option<&str> = description;

        // Use model-level table allowlist if set, otherwise parse from params
        let table_allowlist: Option<ResolvedTableAwareAllowlist> =
            if let Some(allowlist) = params.get("table_allowlist") {
                let tables = allowlist
                    .expose_secret()
                    .split(',')
                    .map(ToString::to_string)
                    .collect::<Vec<String>>();
                Some(
                    ResolvedTableAwareAllowlist::with_defaults(
                        SPICE_DEFAULT_CATALOG,
                        SPICE_DEFAULT_SCHEMA,
                    )
                    .with_table_patterns(tables)
                    .boxed()
                    .context(FailedToConstructToolSnafu { id })?,
                )
            } else {
                self.model_table_allowlist.clone()
            };

        match id {
            "get_readiness" => Ok(Arc::new(GetReadinessTool::new(
                Arc::clone(&self.status),
                Some(name),
                description,
            ))),
            "get_current_datetime" => Ok(Arc::new(GetCurrentDateTimeTool::new(
                Some(name),
                description,
            ))),
            "search" => Ok(Arc::new(
                SearchTool::new(
                    Arc::clone(&self.df),
                    Arc::clone(&self.app),
                    self.search_cache.clone(),
                    crate::search::util::RuntimeTableProviderExplorer,
                    Some(name),
                    description,
                )
                .with_table_allowlist(table_allowlist),
            )),
            "table_schema" => Ok(Arc::new(
                TableSchemaTool::new(
                    Arc::clone(&self.df),
                    Arc::clone(&self.app),
                    Some(name),
                    description,
                )
                .with_table_allowlist(table_allowlist),
            )),
            "sql" => Ok(Arc::new(SqlTool::new(
                Arc::clone(&self.df),
                Some(name),
                description,
                table_allowlist,
            ))),
            "sample_distinct_columns" => Ok(Arc::new(
                SampleDataTool::new(Arc::clone(&self.df), SampleTableMethod::DistinctColumns)
                    .with_overrides(Some(name), description)
                    .with_table_allowlist(table_allowlist),
            )),
            "random_sample" => Ok(Arc::new(
                SampleDataTool::new(Arc::clone(&self.df), SampleTableMethod::RandomSample)
                    .with_overrides(Some(name), description)
                    .with_table_allowlist(table_allowlist),
            )),
            "top_n_sample" => Ok(Arc::new(
                SampleDataTool::new(Arc::clone(&self.df), SampleTableMethod::TopNSample)
                    .with_overrides(Some(name), description)
                    .with_table_allowlist(table_allowlist),
            )),
            "list_datasets" => Ok(Arc::new(ListDatasetsTool::new(
                Some(name),
                description,
                table_allowlist,
                Arc::clone(&self.df),
                Arc::clone(&self.app),
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
        for t in SpiceToolsOptions::All.tools_by_name() {
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
