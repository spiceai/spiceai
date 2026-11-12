/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Load reranker components into the runtime's `rerankers` store, and apply Spicepod changes to
//! them. Parallels [`crate::init::embedding`]: a load that fails for a reason other than its
//! configuration is retried in the background, and failures are logged per component rather than
//! aborting startup.

use std::{sync::Arc, time::Duration};

use app::App;
use opentelemetry::KeyValue;
use spicepod::component::rerankers::Reranker;

use crate::{
    Runtime,
    init::embedding::LOAD_WAIT_IN_APPLY,
    model::{
        ENABLE_MODEL_SUPPORT_MESSAGE,
        rerank::{Error, HealthFailedSnafu},
        try_to_rerank_model,
    },
    status,
};
use runtime_metrics as metrics;
use snafu::ResultExt;

impl Runtime {
    pub(crate) async fn load_rerankers(self: &Arc<Self>) {
        let app_opt = self.read_app().await;
        let Some(app) = app_opt.as_ref() else {
            return;
        };
        if app.rerankers.is_empty() {
            return;
        }

        if !cfg!(feature = "models") {
            tracing::error!(
                "Cannot load rerankers without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}"
            );
            return;
        }

        for reranker in &app.rerankers {
            self.status
                .update_reranker(&reranker.name, status::ComponentStatus::Initializing);
            self.load_reranker(reranker, None).await;
        }
    }

    /// Loads `reranker`, retrying a load that fails for a reason other than its Spicepod
    /// configuration in the background. The caller sets its status first. Returns after the
    /// first attempt, after `wait` with the load still running, or when a Spicepod change stops
    /// the load.
    async fn load_reranker(self: &Arc<Self>, reranker: &Reranker, wait: Option<Duration>) {
        let runtime = Arc::clone(self);
        let reranker = reranker.clone();
        let name = reranker.name.clone();
        let attempted = self
            .reranker_loads
            .load_until_loaded(&name, self.status.shutdown_token(), wait, move || {
                let runtime = Arc::clone(&runtime);
                let reranker = reranker.clone();
                async move {
                    runtime
                        .try_load_reranker(&reranker)
                        .await
                        .is_err_and(|e| !e.is_configuration_error())
                }
            })
            .await;
        if !attempted {
            tracing::warn!(
                "Reranker {name} did not load within {} seconds. The Spicepod change applies without it while it keeps loading.",
                LOAD_WAIT_IN_APPLY.as_secs()
            );
        }
    }

    /// One attempt to load `reranker`, reported in its status, metrics and logs.
    async fn try_load_reranker(&self, reranker: &Reranker) -> Result<(), Error> {
        let load_result = async {
            let rr = try_to_rerank_model(reranker, Arc::clone(&self.secrets)).await?;
            rr.health().await.context(HealthFailedSnafu {
                name: reranker.name.clone(),
            })?;
            Ok::<_, Error>(rr)
        }
        .await;

        match load_result {
            Ok(r) => {
                self.rerankers
                    .write()
                    .await
                    .insert(reranker.name.clone(), r);
                tracing::info!("Reranker Model {} ready", reranker.name);
                metrics::rerankers::COUNT
                    .add(1, &[KeyValue::new("reranker", reranker.name.clone())]);
                self.status
                    .update_reranker(&reranker.name, status::ComponentStatus::Ready);
                Ok(())
            }
            Err(e) => {
                metrics::rerankers::LOAD_ERROR.add(1, &[]);
                self.status.update_reranker(
                    &reranker.name,
                    status::ComponentStatus::error_with_message(e.to_string()),
                );
                tracing::warn!(
                    "Failed to load Reranker {}. {} Verify configuration and try again.",
                    reranker.name,
                    e
                );
                Err(e)
            }
        }
    }

    async fn remove_reranker(&self, name: &str) {
        if self.rerankers.write().await.remove(name).is_some() {
            tracing::info!("Reranker Model {name} has been unloaded");
            metrics::rerankers::COUNT.add(-1, &[KeyValue::new("reranker", name.to_string())]);
        }
    }

    /// Applies the `rerankers:` of `new_app`: loads the added rerankers, reloads the changed ones
    /// and unloads the removed ones.
    pub(crate) async fn apply_reranker_diff(self: &Arc<Self>, current_app: &App, new_app: &App) {
        for reranker in &new_app.rerankers {
            match current_app
                .rerankers
                .iter()
                .find(|r| r.name == reranker.name)
            {
                Some(current) if current == reranker => continue,
                Some(_) => {
                    // First, so a load of the old configuration cannot register after it is removed.
                    self.reranker_loads.stop(&reranker.name).await;
                    self.status
                        .update_reranker(&reranker.name, status::ComponentStatus::Refreshing);
                    self.remove_reranker(&reranker.name).await;
                }
                None => {
                    self.status
                        .update_reranker(&reranker.name, status::ComponentStatus::Initializing);
                }
            }
            self.load_reranker(reranker, Some(LOAD_WAIT_IN_APPLY)).await;
        }

        for reranker in &current_app.rerankers {
            if !new_app.rerankers.iter().any(|r| r.name == reranker.name) {
                self.reranker_loads.stop(&reranker.name).await;
                self.status
                    .update_reranker(&reranker.name, status::ComponentStatus::Disabled);
                self.remove_reranker(&reranker.name).await;
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "models")]
mod tests {
    use std::sync::atomic::Ordering;

    use app::AppBuilder;
    use serde_json::json;

    use super::*;
    use crate::init::embedding::tests::{Provider, serve, wait_until};

    #[tokio::test]
    async fn a_reranker_its_provider_refused_loads_once_the_provider_accepts_it() {
        let provider = Arc::new(Provider::default());
        let reranker: Reranker = serde_json::from_value(json!({
            "name": "flaky",
            "from": format!("{}/rerank", serve(&provider).await),
        }))
        .expect("a valid reranker");
        let mut app = AppBuilder::new("reranker_retry").build();
        app.rerankers.push(reranker);
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        rt.load_rerankers().await;
        assert!(!rt.rerankers().read().await.contains_key("flaky"));
        assert!(!rt.status.is_ready());

        provider.accepting.store(true, Ordering::SeqCst);
        wait_until(
            "the reranker never loaded after the provider accepted it",
            async || rt.rerankers().read().await.contains_key("flaky"),
        )
        .await;
        assert!(rt.status.is_ready());
    }

    /// A Spicepod change applies to rerankers as it does to embedding models: a changed reranker
    /// serves its new configuration, a removed one stops serving, and an added one loads.
    #[tokio::test]
    async fn a_spicepod_change_reloads_adds_and_removes_rerankers() {
        let old = Provider::accepting();
        let new = Provider::accepting();
        let (old_url, new_url) = (serve(&old).await, serve(&new).await);
        let reranker = |name: &str, url: &str| -> Reranker {
            serde_json::from_value(json!({"name": name, "from": format!("{url}/rerank")}))
                .expect("a valid reranker")
        };
        let mut before = AppBuilder::new("reranker_reload").build();
        before.rerankers = vec![reranker("changed", &old_url), reranker("removed", &old_url)];
        let mut after = AppBuilder::new("reranker_reload").build();
        after.rerankers = vec![reranker("changed", &new_url), reranker("added", &old_url)];
        let rt = Arc::new(Runtime::builder().with_app(before).build().await);
        rt.load_rerankers().await;
        assert!(rt.rerankers().read().await.contains_key("removed"));

        assert!(Arc::clone(&rt).apply_app(Arc::new(after)).await);

        let changed = rt
            .rerankers()
            .read()
            .await
            .get("changed")
            .cloned()
            .expect("the changed reranker serves");
        let (old_requests, new_requests) = (
            old.requests.load(Ordering::SeqCst),
            new.requests.load(Ordering::SeqCst),
        );
        changed
            .rerank("query", &["document".to_string()])
            .await
            .expect("the changed reranker ranks");
        assert_eq!(
            (
                old.requests.load(Ordering::SeqCst),
                new.requests.load(Ordering::SeqCst)
            ),
            (old_requests, new_requests + 1),
            "the changed reranker must call its new endpoint, not the old one"
        );
        assert!(!rt.rerankers().read().await.contains_key("removed"));
        assert!(rt.rerankers().read().await.contains_key("added"));
    }
}
