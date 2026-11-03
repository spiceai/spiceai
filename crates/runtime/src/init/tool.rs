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
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::{
    Runtime, UnableToInitializeLlmToolSnafu,
    datafusion::udf::{remove_code_executing_function, remove_user_function_from_deny_list},
    status,
    tools::{self, Tooling, factory::default_available_catalogs},
};
use app::App;
use futures::future::join_all;
use opentelemetry::KeyValue;
use runtime_metrics as metrics;
use runtime_secrets::get_params_with_secrets;
use runtime_tools::catalog::SpiceToolCatalog;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;
use tokio::sync::oneshot;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

/// How long startup waits for a Spicepod tool to load before it loads models and workers.
const TOOL_LOAD_WAIT: Duration = Duration::from_mins(2);

impl Runtime {
    pub(crate) async fn load_tools(self: Arc<Self>) {
        if let Some(app) = self.read_app().await {
            join_all(app.tools.iter().map(|tool| {
                tracing::debug!("Loading tool [{}] from {}...", tool.name, tool.from);
                Arc::clone(&self).load_tool(tool.clone())
            }))
            .await;
        }

        let mut spawned_tasks = vec![];
        let cloned_self = Arc::clone(&self);

        // Load all built-in tools, regardless if they are in the spicepod.
        // This will enable loading each tool in the catalog, and the catalog as a whole. E.g:
        //   `tools: models, builtin`
        //   `tools: sql, load_memory`
        for ctlg in default_available_catalogs(&self) {
            self.insert_tool_catalog(&ctlg).await;
            for tool in ctlg.all().await {
                let cloned_self = Arc::clone(&cloned_self);
                let handle = tokio::spawn(async move {
                    cloned_self.insert_tool(tool.into()).await;
                });
                spawned_tasks.push(handle);
            }
        }

        let _ = join_all(spawned_tasks).await;
    }

    async fn insert_tool_catalog(&self, t: &Arc<dyn SpiceToolCatalog>) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(
            name.clone(),
            Tooling::Catalog {
                tools: Arc::clone(t),
                default_catalog_names: crate::tools::factory::default_catalog_names()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            },
        );
        #[cfg(feature = "mcp")]
        {
            runtime_tools::mcp::attach_mcp_schema_snapshot(t.as_ref(), &self.mcp_schemas);
            self.refresh_mcp_tool_schemas(&tools_map);
        }
        tracing::trace!("Tool catalog {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool_catalog", name.clone())]);
        self.status
            .update_tool_catalog(&name, status::ComponentStatus::Ready);
    }

    async fn insert_tool(&self, t: Tooling) {
        let name = t.name().to_string();
        let mut tools_map = self.tools.write().await;

        tools_map.insert(name.clone(), t);
        #[cfg(feature = "mcp")]
        {
            if let Some(Tooling::Catalog { tools: catalog, .. }) = tools_map.get(&name) {
                runtime_tools::mcp::attach_mcp_schema_snapshot(catalog.as_ref(), &self.mcp_schemas);
            }
            self.refresh_mcp_tool_schemas(&tools_map);
        }
        tracing::trace!("Tool {} ready to use", name.clone());
        metrics::tools::COUNT.add(1, &[KeyValue::new("tool", name.clone())]);
        self.status
            .update_tool(&name, status::ComponentStatus::Ready);
    }

    /// When a spicepod `tools:` entry has `as_sql: true` + a `signature:`,
    /// register a `DataFusion` async UDF whose invocation calls back into
    /// the tool. The UDF is always `Volatile` and added to the federation
    /// deny-list for correctness.
    async fn maybe_register_tool_as_udf(&self, decl: &Tool, tooling: &Tooling) {
        if !decl.as_sql {
            return;
        }
        let functions_enabled = self
            .read_app()
            .await
            .is_some_and(|app| app.runtime.functions.enabled);
        if !functions_enabled {
            tracing::warn!(
                tool = %decl.name,
                "`as_sql: true` but user-defined functions are disabled. Set `runtime.functions.enabled: true` to expose tools as SQL functions."
            );
            return;
        }
        let Some(sig) = decl.signature.as_ref() else {
            tracing::warn!(
                tool = %decl.name,
                "`as_sql: true` but no `signature:` — skipping SQL registration"
            );
            return;
        };
        let inner: Arc<dyn crate::tools::SpiceModelTool> = match tooling {
            Tooling::Tool(t) | Tooling::FunctionTool(t) => Arc::clone(t),
            Tooling::Catalog { .. } => {
                tracing::warn!(
                    tool = %decl.name,
                    "Tool catalogs cannot currently be exposed as SQL UDFs — skipping"
                );
                return;
            }
        };
        match crate::datafusion::tool_udf::build_scalar_udf(inner, &decl.name, sig) {
            Ok(udf) => {
                if crate::datafusion::udf::register_async_user_udf(&self.df.ctx, &udf, &decl.name) {
                    self.tool_udfs.lock().insert(decl.name.clone());
                    tracing::info!(name = %decl.name, "Exposed tool as SQL function");
                }
            }
            Err(e) => {
                tracing::warn!(
                    tool = %decl.name,
                    "Skipping SQL exposure for tool: {e}"
                );
            }
        }
    }

    /// Applies the `tools:` changes from `current_app` to `new_app`: a removed or changed tool
    /// is stopped and removed, and an added or changed tool is loaded. Returns the names of
    /// the tools that changed.
    pub(crate) async fn apply_tool_diff(
        self: &Arc<Self>,
        current_app: &App,
        new_app: &App,
    ) -> HashSet<String> {
        let unchanged = |tool: &Tool, other: &App| other.tools.iter().any(|t| t == tool);
        let mut changed = HashSet::new();
        for tool in current_app.tools.iter().filter(|t| !unchanged(t, new_app)) {
            self.remove_tool(&tool.name).await;
            changed.insert(tool.name.clone());
        }
        let loads = new_app
            .tools
            .iter()
            .filter(|t| !unchanged(t, current_app))
            .map(|tool| {
                changed.insert(tool.name.clone());
                Arc::clone(self).load_tool(tool.clone())
            })
            .collect::<Vec<_>>();
        join_all(loads).await;
        changed
    }

    /// Stops the load of Spicepod tool `name` and removes it, with its SQL function.
    async fn remove_tool(&self, name: &str) {
        self.tool_loads.stop(name).await;
        let mut tools_map = self.tools.write().await;
        // A function exposed as a tool under the same name belongs to the function diff.
        if matches!(
            tools_map.get(name),
            Some(Tooling::Tool(_) | Tooling::Catalog { .. })
        ) {
            tools_map.remove(name);
            metrics::tools::COUNT.add(-1, &[KeyValue::new("tool", name.to_string())]);
        }
        #[cfg(feature = "mcp")]
        self.refresh_mcp_tool_schemas(&tools_map);
        drop(tools_map);
        if self.tool_udfs.lock().remove(name) {
            self.df.ctx.deregister_udf(name);
            remove_user_function_from_deny_list(name);
            remove_code_executing_function(name);
            // A cached plan holds the function, and through it the tool.
            self.df.clear_cached_plans_and_results().await;
        }
        self.status
            .update_tool(name, status::ComponentStatus::Disabled);
        tracing::info!("Tool '{name}' removed");
    }

    /// Loads `tool`, retrying until it loads, the runtime shuts down, or a Spicepod change stops
    /// it. Returns once it loads, or after [`TOOL_LOAD_WAIT`] with the retries still running, so
    /// that one tool cannot hold back models and workers. A tool that loads after that reloads
    /// the models that use it.
    async fn load_tool(self: Arc<Self>, tool: Tool) {
        let (loaded_tx, loaded) = oneshot::channel();
        let shutdown = self.status.shutdown_token();
        let name = tool.name.clone();
        let rt = Arc::clone(&self);
        self.tool_loads.start(&name, async move {
            tokio::select! {
                () = rt.load_tool_until_loaded(&tool) => {
                    if loaded_tx.send(()).is_err() {
                        rt.reload_models_using_late_tool(&tool.name).await;
                    }
                }
                () = shutdown.cancelled() => {}
            }
        });
        if tokio::time::timeout(TOOL_LOAD_WAIT, loaded).await.is_err() {
            tracing::warn!(
                "Tool '{name}' did not load within {} seconds. Models and workers load without it while it keeps retrying.",
                TOOL_LOAD_WAIT.as_secs()
            );
        }
    }

    async fn reload_models_using_late_tool(self: &Arc<Self>, name: &str) {
        let _apply = self.apply_app_lock.lock().await;
        if let Some(app) = self.read_app().await {
            self.reload_models_using_tools(
                &app,
                &HashSet::from([name.to_string()]),
                &HashSet::new(),
                &HashSet::new(),
                &HashSet::new(),
            )
            .await;
        }
    }

    async fn load_tool_until_loaded(self: &Arc<Self>, tool: &Tool) {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(None)
            .max_duration(Some(Duration::from_mins(1)))
            .build();

        let _ = retry(retry_strategy, || async {
            self.status
                .update_tool(&tool.name, status::ComponentStatus::Initializing);
            let params_with_secrets: HashMap<String, SecretString> =
                get_params_with_secrets(self.secrets(), &tool.params).await;

            let env_with_secrets: HashMap<String, SecretString> =
                get_params_with_secrets(self.secrets(), &tool.env).await;

            match tools::factory::forge(
                tool,
                params_with_secrets,
                Arc::clone(self),
                env_with_secrets,
            )
            .await
            .context(UnableToInitializeLlmToolSnafu)
            {
                Ok(t) => {
                    self.maybe_register_tool_as_udf(tool, &t).await;
                    self.insert_tool(t).await;
                    Ok(())
                }
                Err(e) => {
                    metrics::tools::LOAD_ERROR.add(1, &[]);
                    self.status.update_tool(
                        &tool.name,
                        status::ComponentStatus::error_with_message(e.to_string()),
                    );
                    tracing::warn!(
                        "Unable to load tool '{}' from spicepod. Error: {}",
                        tool.name,
                        e,
                    );
                    Err(RetryError::transient(e))
                }
            }
        })
        .await;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::tools::factory::ToolFactory;
    use app::AppBuilder;
    use runtime_tools::factory::IndividualToolFactory;
    use serde_json::Value;

    /// A tool whose description is its `version` param, so a test can tell which
    /// configuration is loaded.
    struct Versioned {
        name: String,
        version: String,
    }

    #[async_trait::async_trait]
    impl crate::tools::SpiceModelTool for Versioned {
        fn name(&self) -> Cow<'_, str> {
            self.name.as_str().into()
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            Some(self.version.as_str().into())
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(&self, _: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::Null)
        }
    }

    struct VersionedFactory;

    impl IndividualToolFactory for VersionedFactory {
        fn construct(
            &self,
            component: &Tool,
            _params_with_secrets: HashMap<String, SecretString>,
        ) -> Result<Arc<dyn crate::tools::SpiceModelTool>, Box<dyn std::error::Error + Send + Sync>>
        {
            Ok(Arc::new(Versioned {
                name: component.name.clone(),
                version: component.params.get("version").cloned().unwrap_or_default(),
            }))
        }
    }

    /// A tool loaded by [`register_versioned_tools`]'s factory, with `version` as its description.
    pub(crate) fn versioned_tool(name: &str, version: &str) -> Tool {
        serde_json::from_value(serde_json::json!({
            "from": format!("versioned:{name}"),
            "name": name,
            "params": {"version": version},
        }))
        .expect("a valid tool")
    }

    pub(crate) async fn register_versioned_tools(rt: &Runtime) {
        rt.tool_factories().lock().await.insert(
            "versioned".to_string(),
            ToolFactory::Tool(Arc::new(VersionedFactory)),
        );
    }

    async fn loaded_version(rt: &Runtime, name: &str) -> Option<String> {
        match rt.tools.read().await.get(name) {
            Some(Tooling::Tool(t)) => t.description().map(Cow::into_owned),
            _ => None,
        }
    }

    #[tokio::test]
    async fn a_reload_removes_changes_and_adds_tools() {
        let before = AppBuilder::new("tool_diff")
            .with_tool(versioned_tool("removed", "1"))
            .with_tool(versioned_tool("changed", "1"))
            .with_tool(versioned_tool("kept", "1"))
            .build();
        let after = AppBuilder::new("tool_diff")
            .with_tool(versioned_tool("changed", "2"))
            .with_tool(versioned_tool("kept", "1"))
            .with_tool(versioned_tool("added", "1"))
            .build();
        let rt = Arc::new(Runtime::builder().with_app(before).build().await);
        register_versioned_tools(&rt).await;
        Arc::clone(&rt).load_tools().await;

        assert!(Arc::clone(&rt).apply_app(Arc::new(after)).await);

        assert_eq!(loaded_version(&rt, "removed").await, None);
        assert_eq!(loaded_version(&rt, "changed").await.as_deref(), Some("2"));
        assert_eq!(loaded_version(&rt, "kept").await.as_deref(), Some("1"));
        assert_eq!(loaded_version(&rt, "added").await.as_deref(), Some("1"));
    }

    /// A plan cached over a tool's SQL function would keep calling the removed tool.
    #[tokio::test]
    async fn removing_a_sql_tool_discards_cached_plans_over_it() {
        let mut tool = versioned_tool("lookup", "1");
        tool.as_sql = true;
        tool.signature = Some(
            serde_json::from_value(serde_json::json!({
                "args": [{"name": "x", "type": "utf8"}],
                "returns": "utf8",
            }))
            .expect("a valid signature"),
        );
        let mut runtime = spicepod::component::runtime::Runtime::default();
        runtime.functions.enabled = true;
        let app = |tools: Vec<Tool>| {
            let mut builder = AppBuilder::new("sql_tool").with_runtime(runtime.clone());
            for tool in tools {
                builder = builder.with_tool(tool);
            }
            Arc::new(builder.build())
        };
        let rt = Arc::new(
            Runtime::builder()
                .with_app(app(vec![tool]).as_ref().clone())
                .build()
                .await,
        );
        register_versioned_tools(&rt).await;
        Arc::clone(&rt).load_tools().await;
        rt.df
            .cache_one_plan("SELECT lookup('a')")
            .await
            .expect("the query plans while the tool is loaded");
        assert_eq!(rt.df.cached_plan_count().await, Some(1));

        assert!(Arc::clone(&rt).apply_app(app(vec![])).await);

        assert_eq!(rt.df.cached_plan_count().await, Some(0));
        rt.df
            .cache_one_plan("SELECT lookup('a')")
            .await
            .expect_err("the query no longer plans");
    }

    #[tokio::test]
    async fn a_tool_that_never_loads_does_not_hold_back_models() {
        let tool: Tool =
            serde_json::from_value(serde_json::json!({"from": "missing:tool", "name": "broken"}))
                .expect("a valid tool");
        let app = AppBuilder::new("tool_load_wait").with_tool(tool).build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        tokio::time::pause();
        tokio::time::timeout(TOOL_LOAD_WAIT * 2, Arc::clone(&rt).load_models())
            .await
            .expect("models load without the tool that never loads");
        assert!(!rt.tools.read().await.contains_key("broken"));
    }
}
