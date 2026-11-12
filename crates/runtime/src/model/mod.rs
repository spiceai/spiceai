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
use futures::future::join_all;
use model_capabilities::CapabilityStore;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

mod chat;
mod embed;
pub mod evaluate;
pub(crate) mod messages;
pub(crate) mod messages_tool_history;
pub(crate) mod metrics;
mod model_context;
pub(crate) mod nsql;
pub mod params;
pub(crate) mod provider_models;
pub(crate) mod rate_limit;
pub mod rerank;
mod responses;
mod responses_tool_history;
mod responses_via_chat;
mod tool_trace;
mod tool_use;
mod tool_use_messages;
mod tool_use_responses;
mod util;
mod wrapper;

pub use chat::{LLMChatCompletionsModelStore, LoadedChatModel, try_to_chat_model};
pub use embed::{EmbeddingModelStore, try_to_embedding};
pub use evaluate::{EvaluateModelStore, try_to_evaluate_model};
pub use messages::{LLMMessagesModelStore, try_to_messages_model};
pub use model_context::{
    ModelContextExtension, ModelContextLayer, add_tools_used, track_ai_inferences_with_spice_count,
};
pub use rerank::{RerankerModelStore, try_to_rerank_model};
pub use responses::{LLMResponsesModelStore, try_to_responses_model};
pub use responses_via_chat::ResponsesViaChat;
pub use tool_use::ToolUsingChat;
pub use tool_use_responses::ToolUsingResponses;

#[derive(Clone)]
pub struct LlmRuntimeStores {
    completion_llms: Arc<RwLock<LLMChatCompletionsModelStore>>,
    responses_llms: Arc<RwLock<LLMResponsesModelStore>>,
    messages_llms: Arc<RwLock<LLMMessagesModelStore>>,
    evaluate_models: Arc<RwLock<EvaluateModelStore>>,
    rate_controllers: Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>>,
    capabilities: Arc<RwLock<CapabilityStore>>,
}

impl Default for LlmRuntimeStores {
    fn default() -> Self {
        Self {
            completion_llms: Arc::new(RwLock::new(HashMap::new())),
            responses_llms: Arc::new(RwLock::new(HashMap::new())),
            messages_llms: Arc::new(RwLock::new(HashMap::new())),
            evaluate_models: Arc::new(RwLock::new(HashMap::new())),
            rate_controllers: Arc::new(RwLock::new(HashMap::new())),
            capabilities: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl LlmRuntimeStores {
    #[must_use]
    pub fn completion_llms(&self) -> Arc<RwLock<LLMChatCompletionsModelStore>> {
        Arc::clone(&self.completion_llms)
    }

    #[must_use]
    pub fn responses_llms(&self) -> Arc<RwLock<LLMResponsesModelStore>> {
        Arc::clone(&self.responses_llms)
    }

    /// The Messages API handlers built when a model loads, by model name (see
    /// [`LLMMessagesModelStore`]).
    #[must_use]
    pub fn messages_llms(&self) -> Arc<RwLock<LLMMessagesModelStore>> {
        Arc::clone(&self.messages_llms)
    }

    #[must_use]
    pub fn evaluate_models(&self) -> Arc<RwLock<EvaluateModelStore>> {
        Arc::clone(&self.evaluate_models)
    }

    #[must_use]
    pub fn rate_controllers(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>> {
        Arc::clone(&self.rate_controllers)
    }

    #[must_use]
    pub fn capabilities(&self) -> Arc<RwLock<CapabilityStore>> {
        Arc::clone(&self.capabilities)
    }

    /// Takes every model out of the stores. Capabilities go first, so a request never finds a
    /// model's capabilities without its handlers.
    pub(crate) async fn take_all(&self) -> UnloadedModels {
        self.capabilities.write().await.clear();
        let mut unloaded = UnloadedModels::default();
        unloaded.extend(std::mem::take(&mut *self.completion_llms.write().await).into_values());
        unloaded.extend(std::mem::take(&mut *self.responses_llms.write().await).into_values());
        unloaded.extend(std::mem::take(&mut *self.messages_llms.write().await).into_values());
        unloaded.extend(std::mem::take(&mut *self.evaluate_models.write().await).into_values());
        self.rate_controllers.write().await.clear();
        unloaded
    }
}

/// Models taken out of a runtime's stores, which [`Self::free`] drops.
///
/// Dropping a local model can block until its engine has freed the model's memory (see
/// `llms_llamacpp`'s decode thread), so the models are not dropped on an async worker thread.
#[derive(Default)]
#[must_use = "call `free`: dropping these models in place can block the async worker thread"]
pub(crate) struct UnloadedModels(Vec<Box<dyn Any + Send>>);

impl UnloadedModels {
    pub(crate) fn extend<M: Any + Send>(&mut self, models: impl IntoIterator<Item = M>) {
        self.0.extend(
            models
                .into_iter()
                .map(|model| Box::new(model) as Box<dyn Any + Send>),
        );
    }

    /// Drops each model on a blocking thread of its own, so that slow ones stop in parallel,
    /// and returns once all of them are dropped.
    pub(crate) async fn free(self) {
        join_all(
            self.0
                .into_iter()
                .map(|model| tokio::task::spawn_blocking(move || drop(model))),
        )
        .await;
    }
}

pub static ENABLE_MODEL_SUPPORT_MESSAGE: &str = "To enable model support, either: \n  1) `spice install ai` \n  2) Build spiced binary with flag `--features models`.";

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc as std_mpsc,
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use llms::chat::{Chat, nsql::SqlGeneration};
    use tokio::sync::oneshot;

    use super::{LlmRuntimeStores, UnloadedModels};

    /// A chat model that counts how many times it is dropped.
    struct CountsDrops(Arc<AtomicUsize>);

    impl Drop for CountsDrops {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Chat for CountsDrops {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }
    }

    #[tokio::test]
    async fn take_all_empties_the_stores_and_free_drops_the_models() {
        let stores = LlmRuntimeStores::default();
        let drops = Arc::new(AtomicUsize::new(0));
        stores.completion_llms().write().await.insert(
            "chat".to_string(),
            Arc::new(CountsDrops(Arc::clone(&drops))),
        );

        let unloaded = stores.take_all().await;
        assert!(stores.completion_llms().read().await.is_empty());
        assert_eq!(drops.load(Ordering::SeqCst), 0, "taken, not yet dropped");

        unloaded.free().await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    /// Blocks in `drop` until the test releases it, as a local model does while its engine
    /// frees the model.
    struct BlocksInDrop {
        started: Option<oneshot::Sender<()>>,
        release: std_mpsc::Receiver<()>,
        released: Arc<AtomicBool>,
    }

    impl Drop for BlocksInDrop {
        fn drop(&mut self) {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            let released = self.release.recv_timeout(Duration::from_secs(5)).is_ok();
            self.released.store(released, Ordering::SeqCst);
        }
    }

    // On the single thread of this test's runtime, a drop on the async thread would keep the
    // test from releasing it until the drop times out.
    #[tokio::test]
    async fn free_drops_off_the_async_thread() {
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let released = Arc::new(AtomicBool::new(false));
        let mut unloaded = UnloadedModels::default();
        unloaded.extend([BlocksInDrop {
            started: Some(started_tx),
            release: release_rx,
            released: Arc::clone(&released),
        }]);

        tokio::join!(unloaded.free(), async move {
            let _ = started_rx.await;
            let _ = release_tx.send(());
        });
        assert!(released.load(Ordering::SeqCst));
    }
}
