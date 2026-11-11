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
};

use crate::model::try_to_evaluate_model;
use crate::{
    Runtime, model::ENABLE_MODEL_SUPPORT_MESSAGE,
    model::provider_models::get_available_models_hint, status,
};
use app::App;
use llms::chat::DistributedBackendSetting;
use llms::evaluate::Evaluate;
use model_capabilities::{Capability, DecisionsBackend, ModelCapabilities};
use opentelemetry::KeyValue;
use runtime_metrics as metrics;
use runtime_secrets::get_params_with_secrets;
use runtime_tools::options::SpiceToolsOptions;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use spicepod::component::model::{Model as SpicepodModel, ModelSource};
use telemetry::timing::TimeMeasurement;
use tokio::{sync::oneshot, task::JoinHandle};
use util::fibonacci_backoff::FibonacciBackoffBuilder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load LLM: {name}. {source}"))]
    FailedToLoadLLM {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Model {name} includes a non-existent path: {path}. Verify the model configuration and ensure all paths are correct. For details, visit https://spiceai.org/docs/components/models",
    ))]
    ReferencedPathDoesNotExist { name: String, path: String },
}

impl Runtime {
    pub(crate) async fn load_models(self: Arc<Self>) {
        let app_lock = self.app.read().await;

        // Load tools before loading models.
        Arc::clone(&self).load_tools().await;

        if !cfg!(feature = "models") && app_lock.as_ref().is_some_and(|s| !s.models.is_empty()) {
            tracing::error!(
                "Cannot load models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}"
            );
            return;
        }

        if let Some(app) = app_lock.as_ref() {
            for model in &app.models {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
            }
        }
    }

    /// Loads `m`. A load that fails for a reason other than its Spicepod configuration is
    /// retried in the background with backoff until it succeeds, so the model, and the
    /// runtime's readiness, recover once the provider accepts it.
    ///
    /// The caller sets the model's status first; each attempt sets it to error or ready.
    /// Returns after the first attempt, or when a Spicepod change stops the load.
    async fn load_model(self: &Arc<Self>, m: &SpicepodModel) {
        self.model_loads.stop(&m.name).await;
        let (first_attempt_done, first_attempt) = oneshot::channel();
        self.model_loads.start(
            &m.name,
            Arc::clone(self).load_model_until_loaded(m.clone(), first_attempt_done),
        );
        // Dropped without a send when the load is stopped.
        let _ = first_attempt.await;
    }

    async fn load_model_until_loaded(
        self: Arc<Self>,
        m: SpicepodModel,
        first_attempt_done: oneshot::Sender<()>,
    ) {
        let shutdown = self.status.shutdown_token();
        let attempts = async {
            let mut retry = needs_retry(&self.try_load_model(&m).await);
            let _ = first_attempt_done.send(());
            let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
            while retry && let Some(delay) = backoff.next_duration() {
                tokio::time::sleep(delay).await;
                retry = needs_retry(&self.try_load_model(&m).await);
            }
        };
        tokio::select! {
            () = attempts => {}
            () = shutdown.cancelled() => {}
        }
    }

    /// One attempt to load `m`, reported in its status, metrics and logs.
    async fn try_load_model(&self, m: &SpicepodModel) -> Result<(), Error> {
        let source = m.get_source();
        let source_str = source.clone().map(|s| s.to_string()).unwrap_or_default();
        let model = m.clone();
        let _guard = TimeMeasurement::new(
            &metrics::models::LOAD_DURATION_MS,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str.clone()),
            ],
        );

        tracing::info!("Loading model [{}] from {}...", m.name, m.from);

        // TODO: Have downstream code using model parameters to accept `Hashmap<String, Value>`.
        // This will require handling secrets with `Value` type.
        let p = m
            .params
            .clone()
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                match v.as_str() {
                    Some(s) => (k, s.to_string()),
                    None => (k, v.to_string()),
                }
            })
            .collect::<HashMap<_, _>>();
        let params = get_params_with_secrets(self.secrets(), &p).await;

        // A node other than the first of a distributed model never reads the weights: the first
        // reads them and sends each node its share, so the files need exist only there.
        if matches!(source, Some(ModelSource::File))
            && !distributed_node_rank(&params).is_some_and(|rank| rank > 0)
        {
            // Verify all referenced local files exist before attempting to load the model and determine its type.
            // Otherwise, we will fail to determine the model type and the error will be confusing.
            if let Err(err) = verify_local_files_exist(m) {
                metrics::models::LOAD_ERROR.add(1, &[]);
                self.status.update_model(
                    &model.name,
                    status::ComponentStatus::error_with_message(err.to_string()),
                );
                tracing::warn!("{err}");
                return Err(err);
            }
        }

        let result = match ModelCapabilities::of(m) {
            Some(capabilities) => self.load_model_handlers(m, &params, capabilities).await,
            None => Err(Error::FailedToLoadLLM {
                name: m.name.clone(),
                source: Box::new(crate::Error::UnableToInitializeLlm {
                    source: Box::new(llms::chat::Error::UnknownModelSource {
                        from: m.from.clone(),
                    }),
                }),
            }),
        };
        match result {
            Ok(()) => {
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
                metrics::models::COUNT.add(
                    1,
                    &[
                        KeyValue::new("model", m.name.clone()),
                        KeyValue::new("source", source_str),
                    ],
                );
                self.status
                    .update_model(&model.name, status::ComponentStatus::Ready);
                Ok(())
            }
            Err(e) => {
                metrics::models::LOAD_ERROR.add(1, &[]);
                self.status.update_model(
                    &model.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );

                // Try to fetch available models to help user debug the issue
                let error_msg = e.to_string();
                let is_model_not_found = error_msg.contains("not valid")
                    || error_msg.contains("not found")
                    || error_msg.contains("ModelNotFound");

                if is_model_not_found
                    && let Some(model_source) = &source
                    && let Some(hint) = get_available_models_hint(model_source, &params).await
                {
                    tracing::warn!("{e}{hint}");
                } else {
                    tracing::warn!("{e}");
                }
                Err(e)
            }
        }
    }

    /// Builds a handler for each capability the model has and registers them, then the
    /// capabilities themselves.
    async fn load_model_handlers(
        &self,
        m: &SpicepodModel,
        params: &HashMap<String, SecretString>,
        capabilities: ModelCapabilities,
    ) -> Result<(), Error> {
        let failed = |source: Box<dyn std::error::Error + Send + Sync>| Error::FailedToLoadLLM {
            name: m.name.clone(),
            source,
        };
        // Every request the model serves, chat or decisions, takes a permit from it.
        let rate_controller = crate::model::rate_limit::build_model_rate_controller(m, params);

        let (chat, responses, messages) = if capabilities.has(Capability::Chat) {
            let loaded = self
                .load_llm(m.clone(), params.clone(), &capabilities)
                .await
                .map_err(|e| failed(Box::new(e)))?;
            (Some(loaded.chat), loaded.responses, loaded.messages)
        } else {
            (None, None, None)
        };

        let evaluator = match capabilities.decisions() {
            Some(DecisionsBackend::Native) => {
                let evaluator =
                    try_to_evaluate_model(m, params, &self.secrets(), Arc::clone(&rate_controller))
                        .await
                        .map_err(|e| failed(Box::new(e)))?;
                // A model with chat was already probed through it.
                if chat.is_none() {
                    Evaluate::health(evaluator.as_ref())
                        .await
                        .map_err(|e| failed(Box::new(e)))?;
                }
                Some(evaluator)
            }
            Some(DecisionsBackend::ViaChat) => chat
                .as_ref()
                .map(|chat| chat.evaluator(&m.name, Arc::clone(&rate_controller))),
            None => None,
        };

        if let Some(chat) = chat {
            // Read before the model is ready, so the first `/v1/models` that lists it as
            // ready reports its limits too. A hosted model's come from its provider's model
            // API, read once per endpoint and kept, so `/v1/models` reads the kept copy.
            let _ = chat.chat.token_limits().await;
            let completion_llms = self.completion_llms();
            completion_llms
                .write()
                .await
                .insert(m.name.clone(), chat.chat);
        }
        if let Some(responses) = responses {
            let responses_llms = self.responses_llms();
            responses_llms
                .write()
                .await
                .insert(m.name.clone(), responses);
        }
        if let Some(messages) = messages {
            let messages_llms = self.messages_llms();
            messages_llms.write().await.insert(m.name.clone(), messages);
        }
        if let Some(evaluator) = evaluator {
            let evaluate_models = self.llm_runtime_stores.evaluate_models();
            evaluate_models
                .write()
                .await
                .insert(m.name.clone(), evaluator);
        }
        let model_rate_controllers = self.model_rate_controllers();
        model_rate_controllers
            .write()
            .await
            .insert(m.name.clone(), rate_controller);
        let node_ranks = self.model_node_ranks();
        let mut node_ranks = node_ranks.write().await;
        match distributed_node_rank(params) {
            Some(rank) => node_ranks.insert(m.name.clone(), rank),
            None => node_ranks.remove(&m.name),
        };
        drop(node_ranks);
        // Last, so a request never finds the capabilities without the handlers.
        let model_capabilities = self.model_capabilities();
        model_capabilities
            .write()
            .await
            .insert(m.name.clone(), capabilities);
        Ok(())
    }

    async fn remove_model(&self, m: &SpicepodModel) {
        // First, so a request never finds the capabilities without the handlers.
        let model_capabilities = self.model_capabilities();
        model_capabilities.write().await.remove(&m.name);

        let completion_llms = self.completion_llms();
        let mut llm_map = completion_llms.write().await;
        llm_map.remove(&m.name);
        drop(llm_map);

        let responses_llms = self.responses_llms();
        let mut responses_map = responses_llms.write().await;
        responses_map.remove(&m.name);
        drop(responses_map);

        let messages_llms = self.messages_llms();
        let mut messages_map = messages_llms.write().await;
        messages_map.remove(&m.name);
        drop(messages_map);

        let evaluate_models = self.llm_runtime_stores.evaluate_models();
        let mut evaluate_map = evaluate_models.write().await;
        evaluate_map.remove(&m.name);
        drop(evaluate_map);

        let model_rate_controllers = self.model_rate_controllers();
        let mut rc_map = model_rate_controllers.write().await;
        rc_map.remove(&m.name);
        drop(rc_map);

        self.model_node_ranks().write().await.remove(&m.name);

        tracing::info!("Model [{}] has been unloaded", m.name);
        let source_str = m.get_source().map(|s| s.to_string()).unwrap_or_default();
        metrics::models::COUNT.add(
            -1,
            &[
                KeyValue::new("model", m.name.clone()),
                KeyValue::new("source", source_str),
            ],
        );
    }

    async fn update_model(self: &Arc<Self>, m: &SpicepodModel) {
        // First, so a load of the old configuration cannot register after it is removed.
        self.model_loads.stop(&m.name).await;
        self.status
            .update_model(&m.name, status::ComponentStatus::Refreshing);
        self.remove_model(m).await;
        self.load_model(m).await;
    }

    /// Returns the names of the models it loaded or reloaded.
    pub(crate) async fn apply_model_diff(
        self: &Arc<Self>,
        current_app: &Arc<App>,
        new_app: &Arc<App>,
    ) -> HashSet<String> {
        let mut loaded = HashSet::new();
        for model in &new_app.models {
            if let Some(current_model) = current_app.models.iter().find(|m| m.name == model.name) {
                if current_model != model {
                    self.update_model(model).await;
                    loaded.insert(model.name.clone());
                }
            } else {
                self.status
                    .update_model(&model.name, status::ComponentStatus::Initializing);
                self.load_model(model).await;
                loaded.insert(model.name.clone());
            }
        }

        // Remove models that are no longer in the app
        for model in &current_app.models {
            if !new_app.models.iter().any(|m| m.name == model.name) {
                self.model_loads.stop(&model.name).await;
                self.status
                    .update_model(&model.name, status::ComponentStatus::Disabled);
                self.remove_model(model).await;
            }
        }
        loaded
    }

    /// Reloads each model of `app` that can use one of the changed Spicepod `tools` or
    /// `function_tools`, because a model gets its tools when it loads. Skips the models in
    /// `loaded`, except on a change to `tools` for those that use the installed app's `tools:`
    /// (`auto`, `all`, `search_registry`), which `loaded` got from the app before this change.
    /// That grouping never includes function tools.
    pub(crate) async fn reload_models_using_tools(
        self: &Arc<Self>,
        app: &App,
        tools: &HashSet<String>,
        function_tools: &HashSet<String>,
        loaded: &HashSet<String>,
    ) {
        if tools.is_empty() && function_tools.is_empty() {
            return;
        }
        for model in &app.models {
            let names_changed = |names: &[String]| {
                !loaded.contains(&model.name)
                    && names.iter().any(|name| {
                        function_tools.contains(name)
                            || tools
                                .contains(name.split_once(':').map_or(name.as_str(), |(c, _)| c))
                    })
            };
            let reload = match model_tool_use(model) {
                ToolUse::None => false,
                ToolUse::Configured(names) => !tools.is_empty() || names_changed(&names),
                ToolUse::Named(names) => names_changed(&names),
            };
            if reload {
                tracing::info!(
                    "Reloading model [{}] to apply a change to its tools",
                    model.name
                );
                self.update_model(model).await;
            }
        }
    }
}

/// Which tools a model's `tools` param selects.
enum ToolUse {
    None,
    /// The installed app's `tools:` and the built-in tools, plus any tools named beside them.
    Configured(Vec<String>),
    /// Tools and tool catalogs by name; `catalog:tool` names one tool of a catalog.
    Named(Vec<String>),
}

fn model_tool_use(model: &SpicepodModel) -> ToolUse {
    let Some(value) = model
        .params
        .get("tools")
        .or_else(|| model.params.get("spice_tools"))
    else {
        return ToolUse::None;
    };
    let spec = value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string);
    match spec.parse::<SpiceToolsOptions>() {
        Ok(
            SpiceToolsOptions::Auto | SpiceToolsOptions::All | SpiceToolsOptions::SearchRegistry,
        ) => ToolUse::Configured(vec![]),
        // A list can include a grouping, e.g. `auto, my_other_tool`.
        Ok(SpiceToolsOptions::Specific(names))
            if names.iter().any(|name| {
                name.parse::<SpiceToolsOptions>()
                    .is_ok_and(|o| o.includes_all_available_tools())
            }) =>
        {
            ToolUse::Configured(names)
        }
        Ok(SpiceToolsOptions::Specific(names)) => ToolUse::Named(names),
        Ok(SpiceToolsOptions::Nsql | SpiceToolsOptions::Disabled) | Err(_) => ToolUse::None,
    }
}

/// The loads in progress of one kind of component, keyed by name, from the first attempt
/// through its retries, so a Spicepod change can stop the load of a configuration it replaces
/// or removes.
#[derive(Default)]
pub(crate) struct LoadTasks {
    tasks: parking_lot::Mutex<HashMap<String, JoinHandle<()>>>,
}

impl LoadTasks {
    /// Runs `load` as the load of `name`. The task is tracked before it starts, so
    /// [`Self::stop`] cannot miss it.
    pub(crate) fn start(&self, name: &str, load: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.tasks.lock();
        if let Some(previous) = tasks.insert(name.to_string(), tokio::spawn(load)) {
            previous.abort();
        }
    }

    /// Stops the load of `name`. When this returns, no attempt to load it is
    /// running and none will start.
    pub(crate) async fn stop(&self, name: &str) {
        let task = self.tasks.lock().remove(name);
        if let Some(task) = task {
            task.abort();
            // Waits until an attempt in progress is dropped.
            let _ = task.await;
        }
    }
}

fn needs_retry(attempt: &Result<(), Error>) -> bool {
    attempt
        .as_ref()
        .is_err_and(|err| !is_permanent_model_failure(err))
}

/// Returns `true` when a model load failure is a mistake in the Spicepod, which no retry
/// can clear. A Spicepod change loads the model again.
fn is_permanent_model_failure(err: &Error) -> bool {
    match err {
        Error::ReferencedPathDoesNotExist { .. } => true,
        Error::FailedToLoadLLM { source, .. } => {
            model_error(source.as_ref()).is_some_and(llms::chat::Error::is_configuration_error)
        }
    }
}

/// The model error a load failure wraps, directly or in [`crate::Error::UnableToInitializeLlm`].
fn model_error<'a>(
    source: &'a (dyn std::error::Error + Send + Sync + 'static),
) -> Option<&'a llms::chat::Error> {
    match source.downcast_ref::<crate::Error>() {
        Some(crate::Error::UnableToInitializeLlm { source }) => source.downcast_ref(),
        _ => source.downcast_ref(),
    }
}

/// This node's rank in a model split across several nodes (`distributed_backend: ring`), or
/// `None` for a model that runs on one node. Rank 0 serves the model; every other rank holds a
/// share of it for rank 0. It reads the params with their secrets and environment variables
/// resolved, and parses them as the model's loader does, so both agree on which node this is: a
/// missing or empty `node_rank` is rank 0, and one the loader refuses is `None`.
fn distributed_node_rank(params: &HashMap<String, SecretString>) -> Option<usize> {
    let ring = params.get("distributed_backend").is_some_and(|backend| {
        backend.expose_secret().parse::<DistributedBackendSetting>()
            == Ok(DistributedBackendSetting::Ring)
    });
    if !ring {
        return None;
    }
    match params.get("node_rank").map(|rank| rank.expose_secret().trim()) {
        None | Some("") => Some(0),
        Some(rank) => rank.parse::<usize>().ok(),
    }
}

fn verify_local_files_exist(m: &SpicepodModel) -> Result<(), Error> {
    for f in m.get_all_files() {
        if !std::path::Path::new(&f.path).exists() {
            return Err(Error::ReferencedPathDoesNotExist {
                name: m.name.clone(),
                path: f.path,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
#[cfg(feature = "models")]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use app::AppBuilder;
    use axum::{
        Json, Router,
        extract::State,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::post,
    };
    use serde_json::{Value, json};
    use tokio::sync::Semaphore;

    use super::*;

    /// An `OpenAI` provider that refuses every request, as an account without credits
    /// does, until `accepting` is set. A request is answered only once it takes a permit
    /// from `answering`, so a test can hold one open.
    struct Provider {
        accepting: AtomicBool,
        requests: AtomicUsize,
        answering: Semaphore,
    }

    impl Default for Provider {
        fn default() -> Self {
            Self {
                accepting: AtomicBool::new(false),
                requests: AtomicUsize::new(0),
                answering: Semaphore::new(Semaphore::MAX_PERMITS),
            }
        }
    }

    async fn chat(State(provider): State<Arc<Provider>>) -> Response {
        provider.requests.fetch_add(1, Ordering::SeqCst);
        let _answer = provider
            .answering
            .acquire()
            .await
            .expect("the provider's semaphore is never closed");
        if !provider.accepting.load(Ordering::SeqCst) {
            let refusal = json!({"error": {"message": "You exceeded your current quota.", "type": "insufficient_quota", "param": null, "code": "insufficient_quota"}});
            return (StatusCode::FORBIDDEN, Json(refusal)).into_response();
        }
        Json(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 0,
            "model": "gpt-4o-mini",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }))
        .into_response()
    }

    /// Starts `provider` and returns the model served by it.
    async fn model_served_by(provider: &Arc<Provider>) -> SpicepodModel {
        let routes = Router::new()
            .route("/v1/chat/completions", post(chat))
            .with_state(Arc::clone(provider));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock provider");
        let address = listener.local_addr().expect("mock provider address");
        tokio::spawn(async move { axum::serve(listener, routes).await });

        let mut model = SpicepodModel::new("openai:gpt-4o-mini", "flaky");
        for (key, value) in [
            ("endpoint", format!("http://{address}/v1")),
            ("openai_api_key", "test-key".to_string()),
        ] {
            model.params.insert(key.to_string(), Value::String(value));
        }
        model
    }

    async fn runtime_with(model: SpicepodModel) -> Arc<Runtime> {
        let app = AppBuilder::new("model_retry").with_model(model).build();
        Arc::new(Runtime::builder().with_app(app).build().await)
    }

    #[tokio::test]
    async fn a_model_its_provider_refused_loads_once_the_provider_accepts_it() {
        let provider = Arc::new(Provider::default());
        let rt = runtime_with(model_served_by(&provider).await).await;

        Arc::clone(&rt).load_models().await;
        let status = rt.status.get_model_status("flaky");
        assert!(
            matches!(&status, Some(status::ComponentStatus::Error(Some(message))) if message.contains("You exceeded your current quota.")),
            "the refused load must report the provider's reason, got {status:?}"
        );
        assert!(!rt.status.is_ready());

        provider.accepting.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let status = rt.status.get_model_status("flaky");
            if matches!(status, Some(status::ComponentStatus::Ready)) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the model never loaded after the provider accepted it: status {status:?}, {} provider requests",
                provider.requests.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(rt.status.is_ready());
        assert!(rt.completion_llms().read().await.contains_key("flaky"));
    }

    /// Loads a model with `tools` set to `tools` beside the tools `lookup` and `other`, applies
    /// a change to their versions, and returns whether the model was reloaded.
    async fn reloaded_by(tools: &str, lookup: &str, other: &str) -> bool {
        use crate::init::tool::tests::{register_versioned_tools, versioned_tool};

        let provider = Arc::new(Provider::default());
        provider.accepting.store(true, Ordering::SeqCst);
        let mut model = model_served_by(&provider).await;
        model
            .params
            .insert("tools".to_string(), Value::String(tools.to_string()));
        let app = |lookup: &str, other: &str| {
            Arc::new(
                AppBuilder::new("tool_reload")
                    .with_tool(versioned_tool("lookup", lookup))
                    .with_tool(versioned_tool("other", other))
                    .with_model(model.clone())
                    .build(),
            )
        };
        let rt = Arc::new(
            Runtime::builder()
                .with_app(app("1", "1").as_ref().clone())
                .build()
                .await,
        );
        register_versioned_tools(&rt).await;
        Arc::clone(&rt).load_models().await;
        let loads = provider.requests.load(Ordering::SeqCst);
        assert!(loads > 0, "the model never reached the provider");

        assert!(Arc::clone(&rt).apply_app(app(lookup, other)).await);
        provider.requests.load(Ordering::SeqCst) > loads
    }

    /// A model gets its tools when it loads, so a change to a tool it names reloads it, and a
    /// change to another tool does not.
    #[tokio::test]
    async fn a_model_reloads_when_a_tool_it_names_changes() {
        assert!(
            !reloaded_by("lookup", "1", "2").await,
            "a change to a tool the model does not use reloaded it"
        );
        assert!(
            reloaded_by("lookup", "2", "1").await,
            "a change to a tool the model names did not reload it"
        );
    }

    #[tokio::test]
    async fn a_model_with_auto_in_its_tool_list_reloads_when_any_tool_changes() {
        assert!(reloaded_by("auto, lookup", "1", "2").await);
    }

    /// `auto` never includes function tools, so one named beside it reaches the model by name.
    #[tokio::test]
    async fn a_model_reloads_when_a_function_tool_it_names_beside_auto_changes() {
        let provider = Arc::new(Provider::default());
        provider.accepting.store(true, Ordering::SeqCst);
        let mut model = model_served_by(&provider).await;
        model.params.insert(
            "tools".to_string(),
            Value::String("auto, add_one".to_string()),
        );
        let mut runtime = spicepod::component::runtime::Runtime::default();
        runtime.functions.enabled = true;
        let app = |description: &str| {
            let function = serde_json::from_value(json!({
                "name": "add_one",
                "from": "sql",
                "description": description,
                "signature": {"args": [{"name": "x", "type": "int64"}], "returns": "int64"},
                "body": "x + 1",
                "as_tool": true,
            }))
            .expect("a valid function");
            Arc::new(
                AppBuilder::new("function_tool_reload")
                    .with_runtime(runtime.clone())
                    .with_function(function)
                    .with_model(model.clone())
                    .build(),
            )
        };
        let rt = Arc::new(
            Runtime::builder()
                .with_app(app("1").as_ref().clone())
                .build()
                .await,
        );
        Arc::clone(&rt).load_models().await;
        let loads = provider.requests.load(Ordering::SeqCst);
        assert!(loads > 0, "the model never reached the provider");

        assert!(Arc::clone(&rt).apply_app(app("2")).await);
        assert!(provider.requests.load(Ordering::SeqCst) > loads);
    }

    /// A model removed from the Spicepod while its first attempt waits on the provider is
    /// not retried, and does not register when the provider recovers.
    #[tokio::test]
    async fn a_model_removed_during_its_first_attempt_is_not_retried() {
        let provider = Arc::new(Provider {
            answering: Semaphore::new(0),
            ..Provider::default()
        });
        let model = model_served_by(&provider).await;
        let with_model = Arc::new(
            AppBuilder::new("model_retry")
                .with_model(model.clone())
                .build(),
        );
        let without_model = Arc::new(AppBuilder::new("model_retry").build());
        let rt = runtime_with(model).await;

        let initial_load = tokio::spawn(Arc::clone(&rt).load_models());
        let deadline = Instant::now() + Duration::from_secs(30);
        while provider.requests.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "the first attempt never reached the provider"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        rt.apply_model_diff(&with_model, &without_model).await;
        tokio::time::timeout(Duration::from_secs(10), initial_load)
            .await
            .expect("removing the model must end its first attempt")
            .expect("the initial load does not panic");

        provider.accepting.store(true, Ordering::SeqCst);
        provider.answering.add_permits(Semaphore::MAX_PERMITS);
        // A retry would run within 1.3 s: the first backoff is 1 s with 30% jitter.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            provider.requests.load(Ordering::SeqCst),
            1,
            "the removed model was retried"
        );
        assert!(!rt.completion_llms().read().await.contains_key("flaky"));
        let status = rt.status.get_model_status("flaky");
        assert!(
            matches!(status, Some(status::ComponentStatus::Disabled)),
            "{status:?}"
        );
    }

    /// The error each load attempt returns decides whether it is retried: a provider's
    /// refusal is, a Spicepod mistake is not.
    #[tokio::test]
    async fn only_a_spicepod_mistake_is_permanent() {
        let provider = Arc::new(Provider::default());
        let refused_model = model_served_by(&provider).await;
        let unknown_source = SpicepodModel::new("not_a_provider:model", "unknown");
        let rt = runtime_with(refused_model.clone()).await;

        let refused = rt
            .try_load_model(&refused_model)
            .await
            .expect_err("the provider refuses");
        assert!(!is_permanent_model_failure(&refused), "{refused}");

        let unknown = rt
            .try_load_model(&unknown_source)
            .await
            .expect_err("no such provider");
        assert!(is_permanent_model_failure(&unknown), "{unknown}");

        let dir = tempfile::tempdir().expect("a directory for the weights");
        let weights = dir.path().join("model.pth");
        std::fs::write(&weights, b"").expect("writes the pickle weights");
        let pickled = SpicepodModel::new(format!("file:{}", weights.display()), "pickled");
        let rejected = rt
            .try_load_model(&pickled)
            .await
            .expect_err("pickle weights are refused");
        assert!(
            rejected
                .to_string()
                .contains("Refusing to load model weight file"),
            "{rejected}"
        );
        assert!(is_permanent_model_failure(&rejected), "{rejected}");
    }

    /// The rank `/v1/models` reports, which also decides whether this node reads the weights,
    /// is the one the loader reads: `parse_distributed_config` takes a missing or empty
    /// `node_rank` as rank 0 and refuses one that is not a number, and a model without
    /// `distributed_backend: ring` runs on one node.
    #[test]
    fn a_models_node_rank_is_read_as_its_loader_reads_it() {
        let params = |pairs: &[(&str, &str)]| -> HashMap<String, SecretString> {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
                .collect()
        };
        for (pairs, expected) in [
            (&[("distributed_backend", "ring"), ("node_rank", "2")][..], Some(2)),
            (&[("distributed_backend", "Ring"), ("node_rank", " 1 ")][..], Some(1)),
            (&[("distributed_backend", "ring")][..], Some(0)),
            (&[("distributed_backend", "ring"), ("node_rank", "")][..], Some(0)),
            (&[("distributed_backend", "ring"), ("node_rank", "first")][..], None),
            (&[("distributed_backend", "none"), ("node_rank", "1")][..], None),
            (&[("node_rank", "1")][..], None),
            (&[][..], None),
        ] {
            assert_eq!(distributed_node_rank(&params(pairs)), expected, "{pairs:?}");
        }
    }
}
