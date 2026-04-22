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

//! Load reranker components into the runtime's `rerankers` store at startup.
//! Parallels [`crate::init::embedding::load_embeddings`]; failures are logged
//! per-component rather than aborting startup, matching the tolerant policy
//! used for embeddings and chat models.

use std::sync::Arc;

use crate::{Runtime, model::try_to_rerank_model, status};

impl Runtime {
    pub(crate) async fn load_rerankers(&self) {
        let app_opt = self.read_app().await;
        let Some(app) = app_opt.as_ref() else {
            return;
        };
        if app.rerankers.is_empty() {
            return;
        }

        for reranker in &app.rerankers {
            self.status
                .update_embedding(&reranker.name, status::ComponentStatus::Initializing);
            match try_to_rerank_model(reranker, Arc::clone(&self.secrets)).await {
                Ok(r) => {
                    let mut rerankers = self.rerankers.write().await;
                    rerankers.insert(reranker.name.clone(), r);
                    tracing::info!("Reranker Model {} ready", reranker.name);
                    self.status
                        .update_embedding(&reranker.name, status::ComponentStatus::Ready);
                }
                Err(e) => {
                    self.status.update_embedding(
                        &reranker.name,
                        status::ComponentStatus::error_with_message(e.to_string()),
                    );
                    tracing::warn!(
                        "Failed to load Reranker {}. {} Verify configuration and try again.",
                        reranker.name,
                        e
                    );
                }
            }
        }
    }
}
