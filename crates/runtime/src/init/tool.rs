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
    Runtime, UnableToInitializeLlmToolSnafu, status,
    tools::{
        self, Tooling,
        factory::{ToolFactoryNotFound, default_available_catalogs},
    },
};
use futures::future::join_all;
use opentelemetry::KeyValue;
use runtime_metrics as metrics;
use runtime_secrets::get_params_with_secrets;
use runtime_tools::catalog::SpiceToolCatalog;
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::tool::Tool;
use tokio::sync::oneshot;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

impl Runtime {
    /// Loads the built-in tool catalogs, then every Spicepod `tools:` entry.
    ///
    /// The built-ins need nothing outside the runtime, so they register first: a Spicepod tool
    /// that cannot load must not keep them (e.g. `sql`) out of the tool list. Spicepod tools
    /// load concurrently, each in its own task, and this returns once each has made its first
    /// attempt. A tool that fails is logged, left out of the tool list, and retried in the
    /// background unless its configuration is invalid; it does not hold up the other tools or
    /// `/v1/ready`.
    pub(crate) async fn load_tools(self: Arc<Self>) {
        let mut spawned_tasks = vec![];
        let mut builtin_names = HashSet::new();

        // Load all built-in tools, regardless if they are in the spicepod.
        // This will enable loading each tool in the catalog, and the catalog as a whole. E.g:
        //   `tools: models, builtin`
        //   `tools: sql, load_memory`
        for ctlg in default_available_catalogs(&self) {
            builtin_names.insert(ctlg.name().to_string());
            self.insert_tool_catalog(&ctlg).await;
            for tool in ctlg.all().await {
                let tool: Tooling = tool.into();
                builtin_names.insert(tool.name().to_string());
                let cloned_self = Arc::clone(&self);
                let handle = tokio::spawn(async move {
                    cloned_self.insert_tool(tool).await;
                });
                spawned_tasks.push(handle);
            }
        }

        let _ = join_all(spawned_tasks).await;

        let builtin_names = Arc::new(builtin_names);
        if let Some(app) = self.read_app().await {
            join_all(app.tools.iter().map(|tool| {
                tracing::debug!("Loading tool [{}] from {}...", tool.name, tool.from);
                self.load_tool(tool, Arc::clone(&builtin_names))
            }))
            .await;
        }
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

    /// Loads `tool` in its own task: a failed attempt is retried in the background with
    /// backoff until it succeeds, unless the tool's configuration is invalid. Returns after the
    /// first attempt.
    async fn load_tool(self: &Arc<Self>, tool: &Tool, builtin_names: Arc<HashSet<String>>) {
        self.tool_loads.stop(&tool.name).await;
        self.status
            .update_tool(&tool.name, status::ComponentStatus::Initializing);
        let (first_attempt_done, first_attempt) = oneshot::channel();
        self.tool_loads.start(
            &tool.name,
            Arc::clone(self).load_tool_until_loaded(
                tool.clone(),
                builtin_names,
                first_attempt_done,
            ),
        );
        // Dropped without a send when the load is stopped.
        let _ = first_attempt.await;
    }

    async fn load_tool_until_loaded(
        self: Arc<Self>,
        tool: Tool,
        builtin_names: Arc<HashSet<String>>,
        first_attempt_done: oneshot::Sender<()>,
    ) {
        let shutdown = self.status.shutdown_token();
        let attempts = async {
            let mut retry = needs_retry(&self.try_load_tool(&tool, &builtin_names).await);
            let _ = first_attempt_done.send(());
            let mut backoff = FibonacciBackoffBuilder::new()
                .max_retries(None)
                .max_duration(Some(Duration::from_mins(1)))
                .build();
            while retry && let Some(delay) = backoff.next_duration() {
                tokio::time::sleep(delay).await;
                retry = needs_retry(&self.try_load_tool(&tool, &builtin_names).await);
            }
        };
        tokio::select! {
            () = attempts => {}
            () = shutdown.cancelled() => {}
        }
    }

    /// One attempt to load `tool`, reported in its status, metrics and logs. The status stays
    /// as it was until the attempt finishes, so a background retry does not report the tool
    /// as initializing again.
    async fn try_load_tool(
        self: &Arc<Self>,
        tool: &Tool,
        builtin_names: &HashSet<String>,
    ) -> Result<(), crate::Error> {
        let params_with_secrets: HashMap<String, SecretString> =
            get_params_with_secrets(self.secrets(), &tool.params).await;

        let env_with_secrets: HashMap<String, SecretString> =
            get_params_with_secrets(self.secrets(), &tool.env).await;

        let forged = tools::factory::forge(
            tool,
            params_with_secrets,
            Arc::clone(self),
            env_with_secrets,
        )
        .await
        .context(UnableToInitializeLlmToolSnafu);

        match forged {
            Ok(t) => {
                self.maybe_register_tool_as_udf(tool, &t).await;
                if builtin_names.contains(t.name().as_ref()) {
                    // A built-in of the same name takes precedence, as it always has.
                    tracing::warn!(
                        "Tool '{}' has the same name as a built-in tool; using the built-in.",
                        tool.name
                    );
                    self.status
                        .update_tool(&tool.name, status::ComponentStatus::Ready);
                } else {
                    self.insert_tool(t).await;
                }
                Ok(())
            }
            Err(e) => {
                metrics::tools::LOAD_ERROR.add(1, &[]);
                self.status.update_tool(
                    &tool.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
                if is_permanent_tool_failure(&e) {
                    tracing::warn!(
                        "Unable to load tool '{}' from spicepod. It is not available until its configuration is fixed. Error: {}",
                        tool.name,
                        e,
                    );
                } else {
                    tracing::warn!(
                        "Unable to load tool '{}' from spicepod; retrying in the background. Error: {}",
                        tool.name,
                        e,
                    );
                }
                Err(e)
            }
        }
    }
}

fn needs_retry(attempt: &Result<(), crate::Error>) -> bool {
    attempt
        .as_ref()
        .is_err_and(|err| !is_permanent_tool_failure(err))
}

/// Returns `true` when a tool load failure is a mistake in the Spicepod, which no retry can
/// clear: a `from` no tool factory handles, or an invalid MCP directive or `mcp_args`.
fn is_permanent_tool_failure(err: &crate::Error) -> bool {
    let crate::Error::UnableToInitializeLlmTool { source } = err else {
        return false;
    };
    if source.downcast_ref::<ToolFactoryNotFound>().is_some() {
        return true;
    }
    #[cfg(feature = "mcp")]
    if source
        .downcast_ref::<runtime_tools::mcp::Error>()
        .is_some_and(runtime_tools::mcp::Error::is_configuration_error)
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use app::AppBuilder;
    use serde_json::json;

    use super::*;

    fn tool(name: &str, from: &str) -> Tool {
        serde_json::from_value(json!({ "name": name, "from": from })).expect("a valid tool")
    }

    async fn runtime_with(spicepod_tools: Vec<Tool>) -> Arc<Runtime> {
        let mut app = AppBuilder::new("tool_loading");
        for spicepod_tool in spicepod_tools {
            app = app.with_tool(spicepod_tool);
        }
        let rt = Arc::new(Runtime::builder().with_app(app.build()).build().await);
        tools::factory::register_all_factories(Arc::clone(&rt)).await;
        rt
    }

    fn tool_status(rt: &Runtime, name: &str) -> Option<status::ComponentStatus> {
        rt.status.get_component_status(&format!("tool:{name}"))
    }

    /// A Spicepod tool that cannot load is left out, and the tools around it, including the
    /// built-ins, still load and the runtime reports ready.
    #[tokio::test]
    async fn a_failing_tool_does_not_hide_the_other_tools_or_block_readiness() {
        let rt = runtime_with(vec![
            tool("my_sql", "builtin:sql"),
            tool("missing", "not_a_source:anything"),
        ])
        .await;

        tokio::time::timeout(Duration::from_secs(30), Arc::clone(&rt).load_tools())
            .await
            .expect("loading the tools finishes after each tool's first attempt");

        let loaded = rt.tools.read().await;
        assert!(
            loaded.contains_key("sql"),
            "the built-in sql tool is missing"
        );
        assert!(
            loaded.contains_key("my_sql"),
            "the good Spicepod tool is missing"
        );
        assert!(!loaded.contains_key("missing"));
        drop(loaded);
        assert!(matches!(
            tool_status(&rt, "missing"),
            Some(status::ComponentStatus::Error(_))
        ));
        assert!(matches!(
            tool_status(&rt, "my_sql"),
            Some(status::ComponentStatus::Ready)
        ));
        assert!(
            rt.status.is_ready(),
            "a failed tool must not block /v1/ready"
        );
    }

    /// A Spicepod tool named like a built-in does not replace the built-in.
    #[tokio::test]
    async fn a_builtin_tool_keeps_its_name() {
        let rt = runtime_with(vec![tool("sql", "builtin:list_datasets")]).await;
        Arc::clone(&rt).load_tools().await;

        let loaded = rt.tools.read().await;
        let description = |name: &str| match loaded.get(name) {
            Some(Tooling::Tool(t)) => t.description().map(std::borrow::Cow::into_owned),
            other => panic!("{name} is not a single tool: {}", other.is_some()),
        };
        assert_ne!(
            description("sql"),
            description("list_datasets"),
            "the Spicepod tool replaced the built-in sql tool"
        );
    }

    /// A Spicepod mistake is not retried; an unreachable server is.
    #[cfg(feature = "mcp")]
    #[tokio::test]
    async fn only_a_spicepod_mistake_is_permanent() {
        let unused = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
        let refused_from = format!(
            "mcp:http://{}/mcp",
            unused.local_addr().expect("the free port's address")
        );
        drop(unused);

        let refused_tool = tool("refused", &refused_from);
        let unknown_tool = tool("unknown", "not_a_source:anything");
        let rt = runtime_with(vec![refused_tool.clone(), unknown_tool.clone()]).await;
        let builtin_names = HashSet::new();

        let refused = rt
            .try_load_tool(&refused_tool, &builtin_names)
            .await
            .expect_err("nothing listens on the port");
        assert!(!is_permanent_tool_failure(&refused), "{refused}");

        let unknown = rt
            .try_load_tool(&unknown_tool, &builtin_names)
            .await
            .expect_err("no such source");
        assert!(is_permanent_tool_failure(&unknown), "{unknown}");
    }
}
