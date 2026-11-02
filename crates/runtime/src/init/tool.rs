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
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

/// How long startup waits for a Spicepod tool to load before it loads models and workers.
const TOOL_LOAD_WAIT: Duration = Duration::from_mins(2);

impl Runtime {
    /// Loads the built-in tool catalogs, then every Spicepod `tools:` entry.
    ///
    /// The built-ins need nothing outside the runtime, so they register first: a Spicepod tool
    /// that cannot load must not keep them (e.g. `sql`) out of the tool list. Spicepod tools
    /// load concurrently; [`Self::load_tool`] says how long this waits for each.
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
                Arc::clone(&self).load_tool(tool.clone(), Arc::clone(&builtin_names))
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

    /// Loads `tool`, retrying until it loads or the runtime shuts down, unless its configuration
    /// is invalid, which no retry can clear. Returns once it loads or fails for good, or after
    /// [`TOOL_LOAD_WAIT`] with the retries still running, so that one tool cannot hold back
    /// models and workers. A model loaded before the tool does not get it.
    async fn load_tool(self: Arc<Self>, tool: Tool, builtin_names: Arc<HashSet<String>>) {
        // Loading the tools again replaces the retries of an earlier load instead of adding to
        // them.
        self.tool_loads.stop(&tool.name).await;
        // A tool that already failed stays failed until an attempt succeeds, so loading the
        // tools again does not make it block readiness.
        let already_failed = matches!(
            self.status
                .get_component_status(&format!("tool:{}", tool.name)),
            Some(status::ComponentStatus::Error(_))
        );
        if !already_failed {
            self.status
                .update_tool(&tool.name, status::ComponentStatus::Initializing);
        }
        let (done_tx, done) = oneshot::channel();
        let shutdown = self.status.shutdown_token();
        let name = tool.name.clone();
        let rt = Arc::clone(&self);
        self.tool_loads.start(&name, async move {
            tokio::select! {
                () = rt.load_tool_until_loaded(&tool, &builtin_names) => {
                    let _ = done_tx.send(());
                }
                () = shutdown.cancelled() => {}
            }
        });
        // `done` also resolves, without a send, when the load is stopped or shut down.
        if tokio::time::timeout(TOOL_LOAD_WAIT, done).await.is_err() {
            tracing::warn!(
                "Tool '{name}' did not load within {} seconds. Models and workers load without it while it keeps retrying.",
                TOOL_LOAD_WAIT.as_secs()
            );
        }
    }

    /// Attempts to load `tool` until an attempt succeeds or fails for a reason no retry can
    /// clear.
    async fn load_tool_until_loaded(
        self: &Arc<Self>,
        tool: &Tool,
        builtin_names: &HashSet<String>,
    ) {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(None)
            .max_duration(Some(Duration::from_mins(1)))
            .build();

        let _ = retry(retry_strategy, || async {
            self.try_load_tool(tool, builtin_names).await.map_err(|e| {
                if is_permanent_tool_failure(&e) {
                    RetryError::permanent(e)
                } else {
                    RetryError::transient(e)
                }
            })
        })
        .await;
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use app::AppBuilder;
    use serde_json::json;

    use super::*;
    use crate::tools::factory::ToolFactory;

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

    /// One load attempt, as the tool's factory saw it: whether it came after the tools loaded
    /// again, the tool's status, and whether the runtime reported ready.
    type Attempt = (bool, Option<status::ComponentStatus>, bool);

    /// A tool source whose server is never reachable. Each attempt is recorded.
    struct Unreachable {
        status: Arc<status::RuntimeStatus>,
        reloaded: Arc<AtomicBool>,
        attempts: Arc<parking_lot::Mutex<Vec<Attempt>>>,
    }

    #[async_trait::async_trait]
    impl runtime_tools::factory::ToolCatalogFactory for Unreachable {
        async fn construct(
            &self,
            component: &Tool,
            _params_with_secrets: HashMap<String, SecretString>,
            _env: HashMap<String, SecretString>,
        ) -> Result<Arc<dyn SpiceToolCatalog>, Box<dyn std::error::Error + Send + Sync>> {
            self.attempts.lock().push((
                self.reloaded.load(Ordering::SeqCst),
                self.status
                    .get_component_status(&format!("tool:{}", component.name)),
                self.status.is_ready(),
            ));
            Err("the server is unreachable".into())
        }
    }

    /// Registers [`Unreachable`] as the `unreachable` tool source.
    async fn register_unreachable(
        rt: &Runtime,
    ) -> (Arc<AtomicBool>, Arc<parking_lot::Mutex<Vec<Attempt>>>) {
        let reloaded = Arc::new(AtomicBool::new(false));
        let attempts = Arc::default();
        rt.tool_factories().lock().await.insert(
            "unreachable".to_string(),
            ToolFactory::Catalog(Arc::new(Unreachable {
                status: Arc::clone(&rt.status),
                reloaded: Arc::clone(&reloaded),
                attempts: Arc::clone(&attempts),
            })),
        );
        (reloaded, attempts)
    }

    #[tokio::test]
    async fn a_tool_that_never_loads_does_not_hold_back_models() {
        let tool: Tool = serde_json::from_value(
            serde_json::json!({"from": "unreachable:tool", "name": "broken"}),
        )
        .expect("a valid tool");
        let app = AppBuilder::new("tool_load_wait").with_tool(tool).build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);
        let (_, attempts) = register_unreachable(&rt).await;

        tokio::time::pause();
        tokio::time::timeout(TOOL_LOAD_WAIT * 2, Arc::clone(&rt).load_models())
            .await
            .expect("models load without the tool that never loads");
        assert!(!rt.tools.read().await.contains_key("broken"));
        assert!(
            attempts.lock().len() > 1,
            "the tool is retried while models wait for it"
        );
    }

    /// A Spicepod tool that cannot load is left out, and the tools around it, including the
    /// built-ins, still load and the runtime reports ready.
    #[tokio::test]
    async fn a_failing_tool_does_not_hide_the_other_tools_or_block_readiness() {
        let rt = runtime_with(vec![
            tool("my_sql", "builtin:sql"),
            tool("broken", "unreachable:x"),
        ])
        .await;
        register_unreachable(&rt).await;

        tokio::time::pause();
        let load = tokio::spawn(Arc::clone(&rt).load_tools());
        // While the broken tool is still retrying and `load_tools` still waits on it.
        tokio::time::sleep(TOOL_LOAD_WAIT / 2).await;
        assert!(!load.is_finished(), "load_tools waits for the broken tool");

        let loaded = rt.tools.read().await;
        assert!(
            loaded.contains_key("sql"),
            "the built-in sql tool is missing"
        );
        assert!(
            loaded.contains_key("my_sql"),
            "the good Spicepod tool is missing"
        );
        assert!(!loaded.contains_key("broken"));
        drop(loaded);
        assert!(matches!(
            tool_status(&rt, "broken"),
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
        load.await.expect("load_tools finishes");
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

    /// A failed tool stays failed through its background retries and when the tools load again
    /// (a second `load_models` does that), so neither puts it back to initializing and blocks
    /// readiness, and loading again replaces its retries instead of adding to them.
    #[tokio::test]
    async fn a_failed_tool_stays_failed_across_retries_and_reloads() {
        let rt = runtime_with(vec![tool("flaky", "unreachable:x")]).await;
        let (reloaded, attempts) = register_unreachable(&rt).await;

        tokio::time::pause();
        Arc::clone(&rt).load_tools().await;
        tokio::time::sleep(Duration::from_mins(3)).await;
        let first_load = attempts.lock().len();
        reloaded.store(true, Ordering::SeqCst);
        Arc::clone(&rt).load_tools().await;
        tokio::time::sleep(Duration::from_mins(3)).await;

        let attempts = attempts.lock().clone();
        let reload = attempts.len() - first_load;
        assert!(
            first_load > 1,
            "the first load was not retried: {attempts:?}"
        );
        assert!(reload > 1, "the reload was not retried: {attempts:?}");
        // Retries back off the same way after each load, so one retry loop makes about as many
        // attempts in the same time, and two would make about twice as many.
        assert!(
            reload <= first_load + 1,
            "loading the tools again added a second retry loop: {first_load} attempts, then {reload}"
        );
        assert!(
            matches!(
                attempts[0],
                (false, Some(status::ComponentStatus::Initializing), false)
            ),
            "the first attempt: {:?}",
            attempts[0]
        );
        for attempt in &attempts[1..] {
            assert!(
                matches!(attempt, (_, Some(status::ComponentStatus::Error(_)), true)),
                "a retry or a reload put the failed tool back to initializing or blocked readiness: {attempt:?}"
            );
        }
    }

    /// Counts `Unable to load tool '<name>'` warnings, one per failed attempt.
    #[derive(Default, Clone)]
    struct LoadWarnings(Arc<parking_lot::Mutex<HashMap<String, usize>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LoadWarnings {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut message = Message(String::new());
            event.record(&mut message);
            if let Some(rest) = message.0.strip_prefix("Unable to load tool '")
                && let Some((name, _)) = rest.split_once('\'')
            {
                *self.0.lock().entry(name.to_string()).or_default() += 1;
            }
        }
    }

    /// A Spicepod mistake (a `from` no tool source handles, invalid `mcp_args`) is attempted once
    /// and not retried; an unreachable MCP server is retried.
    #[cfg(feature = "mcp")]
    #[tokio::test]
    async fn only_a_spicepod_mistake_is_not_retried() {
        use tracing_subscriber::layer::SubscriberExt;

        let warnings = LoadWarnings::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(warnings.clone()));
        let unused = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a free port");
        let refused_from = format!(
            "mcp:http://{}/mcp",
            unused.local_addr().expect("the free port's address")
        );
        drop(unused);
        let bad_args: Tool = serde_json::from_value(json!({
            "name": "bad_args",
            "from": "mcp:echo",
            "params": { "mcp_args": "\"unterminated" }
        }))
        .expect("a valid tool");
        let rt = runtime_with(vec![
            tool("unknown", "not_a_source:anything"),
            bad_args,
            tool("refused", &refused_from),
        ])
        .await;

        tokio::time::pause();
        let started = tokio::time::Instant::now();
        Arc::clone(&rt).load_tools().await;
        tokio::time::sleep(Duration::from_mins(10).saturating_sub(started.elapsed())).await;

        let warnings = warnings.0.lock().clone();
        assert_eq!(warnings.get("unknown"), Some(&1), "{warnings:?}");
        assert_eq!(warnings.get("bad_args"), Some(&1), "{warnings:?}");
        assert!(
            warnings
                .get("refused")
                .is_some_and(|attempts| *attempts > 1),
            "an unreachable server is retried: {warnings:?}"
        );
    }
}
