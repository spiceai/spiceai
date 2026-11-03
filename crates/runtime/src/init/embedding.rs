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

use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{
    Result, Runtime, UnableToInitializeEmbeddingModelSnafu,
    embeddings::task::TaskEmbed,
    model::{ENABLE_MODEL_SUPPORT_MESSAGE, try_to_embedding},
    status,
};
use app::App;
use llms::embeddings::{Embed, Error as EmbedError};
use opentelemetry::KeyValue;
use runtime_metrics as metrics;
use snafu::prelude::*;
use spicepod::component::embeddings::Embeddings;

/// How long a Spicepod change waits for each model it loads before it applies the rest of the
/// change. The datasets that use a model still loading retry until it loads.
pub(crate) const LOAD_WAIT_IN_APPLY: Duration = Duration::from_mins(2);

impl Runtime {
    pub(crate) async fn load_embeddings(self: &Arc<Self>) {
        let app_opt = self.read_app().await;

        if !cfg!(feature = "models") && app_opt.as_ref().is_some_and(|s| !s.embeddings.is_empty()) {
            tracing::error!(
                "Cannot load embedding models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}"
            );
            return;
        }

        if let Some(app) = app_opt.as_ref() {
            for in_embed in &app.embeddings {
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Initializing);
                self.load_embedding(in_embed, None).await;
            }
        }
    }

    /// Loads `in_embed`. A load that fails for a reason other than its Spicepod configuration is
    /// retried in the background with backoff until it succeeds, so the model and the datasets
    /// that use it recover once its provider answers.
    ///
    /// The caller sets the model's status first; each attempt sets it to error or ready.
    /// Returns after the first attempt, after `wait` with the load still running, or when a
    /// Spicepod change stops the load.
    async fn load_embedding(self: &Arc<Self>, in_embed: &Embeddings, wait: Option<Duration>) {
        let runtime = Arc::clone(self);
        let in_embed = in_embed.clone();
        let name = in_embed.name.clone();
        let attempted = self
            .embedding_loads
            .load_until_loaded(&name, self.status.shutdown_token(), wait, move || {
                let runtime = Arc::clone(&runtime);
                let in_embed = in_embed.clone();
                async move {
                    runtime
                        .try_load_embedding(&in_embed)
                        .await
                        .is_err_and(|e| !is_permanent_embedding_failure(&e))
                }
            })
            .await;
        if !attempted {
            tracing::warn!(
                "Embedding Model {name} did not load within {} seconds. The Spicepod change applies without it while it keeps loading.",
                LOAD_WAIT_IN_APPLY.as_secs()
            );
        }
    }

    /// One attempt to load `in_embed`, reported in its status, metrics and logs.
    async fn try_load_embedding(&self, in_embed: &Embeddings) -> Result<()> {
        match self.build_embedding(in_embed).await {
            Ok(e) => {
                self.embeds
                    .write()
                    .await
                    .insert(in_embed.name.clone(), Arc::new(e) as Arc<dyn Embed>);

                tracing::info!("Embedding Model {} ready", in_embed.name);
                metrics::embeddings::COUNT.add(1, &embedding_labels(in_embed));
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Ready);
                Ok(())
            }
            Err(e) => {
                metrics::embeddings::LOAD_ERROR.add(1, &[]);
                self.status.update_embedding(
                    &in_embed.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
                tracing::warn!(
                    "Failed to load Embedding Model {}. {} Verify configuration and try again.\nFor details, visit https://spiceai.org/docs/components/embeddings",
                    in_embed.name,
                    e
                );
                Err(e)
            }
        }
    }

    async fn build_embedding(&self, in_embed: &Embeddings) -> Result<TaskEmbed> {
        let l = try_to_embedding(
            in_embed,
            Arc::clone(&self.secrets),
            self.token_provider_registry(),
            self.datafusion().embeddings_cache_provider(),
        )
        .await
        .boxed()
        .context(UnableToInitializeEmbeddingModelSnafu)?;
        l.health()
            .await
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)?;

        TaskEmbed::new(in_embed.name.as_str(), l)
            .await
            .boxed()
            .context(UnableToInitializeEmbeddingModelSnafu)
    }

    async fn remove_embedding(&self, in_embed: &Embeddings) {
        if self.embeds.write().await.remove(&in_embed.name).is_some() {
            tracing::info!("Embedding Model {} has been unloaded", in_embed.name);
            metrics::embeddings::COUNT.add(-1, &embedding_labels(in_embed));
        }
        // Cached embeddings are keyed by model name or provider model id, not by configuration.
        if let Some(cache) = self.datafusion().embeddings_cache_provider() {
            cache.invalidate_all().await;
        }
    }

    /// Applies the `embeddings:` of `new_app`: loads the added models, reloads the changed ones
    /// and unloads the removed ones. The datasets that use them reload in
    /// [`Runtime::apply_dataset_diff`].
    pub(crate) async fn apply_embedding_diff(self: &Arc<Self>, current_app: &App, new_app: &App) {
        let changed = changed_embeddings(current_app, new_app);
        for in_embed in &new_app.embeddings {
            if !changed.contains(&in_embed.name) {
                continue;
            }
            if let Some(current) = current_app
                .embeddings
                .iter()
                .find(|e| e.name == in_embed.name)
            {
                // First, so a load of the old configuration cannot register after it is removed.
                self.embedding_loads.stop(&in_embed.name).await;
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Refreshing);
                self.remove_embedding(current).await;
            } else {
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Initializing);
            }
            self.load_embedding(in_embed, Some(LOAD_WAIT_IN_APPLY))
                .await;
        }

        for in_embed in &current_app.embeddings {
            if !new_app.embeddings.iter().any(|e| e.name == in_embed.name) {
                self.embedding_loads.stop(&in_embed.name).await;
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Disabled);
                self.remove_embedding(in_embed).await;
            }
        }
    }
}

/// Names of the embedding models that `new_app` adds, changes or removes.
pub(crate) fn changed_embeddings(current_app: &App, new_app: &App) -> HashSet<String> {
    current_app
        .embeddings
        .iter()
        .chain(&new_app.embeddings)
        .filter(|e| {
            current_app.embeddings.iter().find(|c| c.name == e.name)
                != new_app.embeddings.iter().find(|n| n.name == e.name)
        })
        .map(|e| e.name.clone())
        .collect()
}

fn embedding_labels(in_embed: &Embeddings) -> [KeyValue; 2] {
    [
        KeyValue::new("embeddings", in_embed.name.clone()),
        KeyValue::new(
            "source",
            in_embed
                .get_prefix()
                .map(|x| x.to_string())
                .unwrap_or_default(),
        ),
    ]
}

/// Returns `true` when an embedding model load failure is a mistake in the Spicepod, which no
/// retry can clear. A Spicepod change loads the model again.
fn is_permanent_embedding_failure(err: &crate::Error) -> bool {
    let crate::Error::UnableToInitializeEmbeddingModel { source } = err else {
        return false;
    };
    match source.downcast_ref::<EmbedError>() {
        Some(EmbedError::FailedToInstantiateEmbeddingModel { source }) => {
            source.is::<runtime_parameters_typed::ParamsError>()
                || source
                    .downcast_ref::<EmbedError>()
                    .is_some_and(EmbedError::is_configuration_error)
        }
        Some(err) => err.is_configuration_error(),
        None => false,
    }
}

#[cfg(test)]
#[cfg(feature = "models")]
pub(crate) mod tests {
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
    use llms::embeddings::EmbeddingInput;
    use serde_json::{Value, json};

    use super::*;

    /// An embedding and rerank provider that refuses every request, as an account without
    /// credits does, until `accepting` is set.
    #[derive(Default)]
    pub(crate) struct Provider {
        pub(crate) accepting: AtomicBool,
        pub(crate) requests: AtomicUsize,
    }

    impl Provider {
        pub(crate) fn accepting() -> Arc<Self> {
            let provider = Arc::new(Self::default());
            provider.accepting.store(true, Ordering::SeqCst);
            provider
        }

        fn refusal(&self) -> Option<Response> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            if self.accepting.load(Ordering::SeqCst) {
                return None;
            }
            let refusal = json!({"error": {"message": "You exceeded your current quota.", "type": "insufficient_quota", "param": null, "code": "insufficient_quota"}});
            Some((StatusCode::FORBIDDEN, Json(refusal)).into_response())
        }
    }

    async fn embeddings(
        State(provider): State<Arc<Provider>>,
        Json(body): Json<Value>,
    ) -> Response {
        if let Some(refusal) = provider.refusal() {
            return refusal;
        }
        let inputs = body["input"].as_array().map_or(1, Vec::len);
        let data: Vec<Value> = (0..inputs)
            .map(|index| json!({"object": "embedding", "index": index, "embedding": vec![0.5_f32; 1536]}))
            .collect();
        Json(json!({"object": "list", "data": data, "model": "text-embedding-3-small", "usage": {"prompt_tokens": 1, "total_tokens": 1}}))
            .into_response()
    }

    async fn rerank(State(provider): State<Arc<Provider>>) -> Response {
        if let Some(refusal) = provider.refusal() {
            return refusal;
        }
        Json(json!({"results": [{"index": 0, "relevance_score": 1.0}]})).into_response()
    }

    /// Starts `provider` and returns its base URL.
    pub(crate) async fn serve(provider: &Arc<Provider>) -> String {
        let routes = Router::new()
            .route("/v1/embeddings", post(embeddings))
            .route("/rerank", post(rerank))
            .with_state(Arc::clone(provider));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock provider");
        let address = listener.local_addr().expect("mock provider address");
        tokio::spawn(async move { axum::serve(listener, routes).await });
        format!("http://{address}")
    }

    pub(crate) fn embedding(name: &str, url: &str) -> Embeddings {
        serde_json::from_value(json!({
            "name": name,
            "from": "openai:text-embedding-3-small",
            "params": {"endpoint": format!("{url}/v1"), "openai_api_key": "test-key"},
        }))
        .expect("a valid embedding model")
    }

    pub(crate) async fn wait_until(what: &str, mut done: impl AsyncFnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done().await {
            assert!(Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn an_embedding_model_its_provider_refused_loads_once_the_provider_accepts_it() {
        let provider = Arc::new(Provider::default());
        let app = AppBuilder::new("embedding_retry")
            .with_embedding(embedding("flaky", &serve(&provider).await))
            .build();
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        rt.load_embeddings().await;
        let status = rt.status.get_embedding_status("flaky");
        assert!(
            matches!(&status, Some(status::ComponentStatus::Error(Some(message))) if message.contains("You exceeded your current quota.")),
            "the refused load must report the provider's reason, got {status:?}"
        );
        assert!(!rt.status.is_ready());

        provider.accepting.store(true, Ordering::SeqCst);
        wait_until(
            "the embedding model never loaded after the provider accepted it",
            async || rt.embeds().read().await.contains_key("flaky"),
        )
        .await;
        assert!(matches!(
            rt.status.get_embedding_status("flaky"),
            Some(status::ComponentStatus::Ready)
        ));
        assert!(rt.status.is_ready());
    }

    /// A mistake in the Spicepod ends the load; a refusal from the provider does not.
    #[tokio::test]
    async fn a_configuration_error_ends_the_load_and_a_provider_refusal_does_not() {
        let url = serve(&Arc::new(Provider::default())).await;
        let rt = Runtime::builder().build().await;
        let mut invalid = embedding("invalid", &url);
        invalid.params.insert(
            "openai_usage_tier".to_string(),
            Value::String("no_such_tier".to_string()),
        );

        let err = rt
            .try_load_embedding(&invalid)
            .await
            .expect_err("an invalid param fails the load");
        assert!(is_permanent_embedding_failure(&err), "{err}");
        let err = rt
            .try_load_embedding(&embedding("refused", &url))
            .await
            .expect_err("the provider refuses the load");
        assert!(!is_permanent_embedding_failure(&err), "{err}");
    }

    /// A Spicepod change applies to embedding models as it does to models: a changed model
    /// serves its new configuration, a removed one stops serving, also while its load is
    /// retrying, and an added one loads.
    #[tokio::test]
    async fn a_spicepod_change_reloads_adds_and_removes_embedding_models() {
        let old = Provider::accepting();
        let new = Provider::accepting();
        let refusing = Arc::new(Provider::default());
        let (old_url, new_url) = (serve(&old).await, serve(&new).await);
        // Its own provider model id, so the embeddings cache cannot answer its health check.
        let mut removed = embedding("removed", &serve(&refusing).await);
        removed.from = "openai:text-embedding-ada-002".to_string();
        let before = Arc::new(
            AppBuilder::new("embedding_reload")
                .with_embedding(embedding("changed", &old_url))
                .with_embedding(removed)
                .build(),
        );
        let after = Arc::new(
            AppBuilder::new("embedding_reload")
                .with_embedding(embedding("changed", &new_url))
                .with_embedding(embedding("added", &old_url))
                .build(),
        );
        let rt = Arc::new(
            Runtime::builder()
                .with_app(before.as_ref().clone())
                .build()
                .await,
        );
        rt.load_embeddings().await;
        assert!(!rt.embeds().read().await.contains_key("removed"));
        let probe = || EmbeddingInput::String("probe".to_string());
        rt.embeds()
            .read()
            .await
            .get("changed")
            .cloned()
            .expect("the model serves")
            .embed(probe())
            .await
            .expect("the model embeds");

        assert!(Arc::clone(&rt).apply_app(after).await);
        let refused = refusing.requests.load(Ordering::SeqCst);
        refusing.accepting.store(true, Ordering::SeqCst);

        let changed = rt
            .embeds()
            .read()
            .await
            .get("changed")
            .cloned()
            .expect("the changed model serves");
        let (old_requests, new_requests) = (
            old.requests.load(Ordering::SeqCst),
            new.requests.load(Ordering::SeqCst),
        );
        changed
            .embed(probe())
            .await
            .expect("the changed model embeds");
        assert_eq!(
            (
                old.requests.load(Ordering::SeqCst),
                new.requests.load(Ordering::SeqCst)
            ),
            (old_requests, new_requests + 1),
            "the changed model must call its new endpoint, not the old one or its cached results"
        );

        assert!(rt.embeds().read().await.contains_key("added"));
        assert!(rt.status.is_ready());

        // Past the next retries the removed model's load would make.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            refusing.requests.load(Ordering::SeqCst),
            refused,
            "the removed model's load must stop retrying"
        );
        assert!(!rt.embeds().read().await.contains_key("removed"));
        assert!(matches!(
            rt.status.get_embedding_status("removed"),
            Some(status::ComponentStatus::Disabled)
        ));
    }
}
