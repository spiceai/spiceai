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

use std::sync::Arc;

use crate::{
    embeddings::task::TaskEmbed,
    metrics,
    model::{try_to_embedding, ENABLE_MODEL_SUPPORT_MESSAGE},
    status, Result, Runtime, UnableToInitializeEmbeddingModelSnafu,
};
use llms::embeddings::Embed;
use opentelemetry::KeyValue;
use snafu::prelude::*;
use spicepod::component::embeddings::Embeddings;

impl Runtime {
    #[allow(dead_code)]
    pub(crate) async fn load_embeddings(&self) {
        let app_opt = self.app.read().await;

        if !cfg!(feature = "models") && app_opt.as_ref().is_some_and(|s| !s.embeddings.is_empty()) {
            tracing::error!("Cannot load embedding models without the 'models' feature enabled. {ENABLE_MODEL_SUPPORT_MESSAGE}");
            return;
        };

        if let Some(app) = app_opt.as_ref() {
            for in_embed in &app.embeddings {
                self.status
                    .update_embedding(&in_embed.name, status::ComponentStatus::Initializing);
                match self.load_embedding(in_embed).await {
                    Ok(e) => {
                        let mut embeds_map = self.embeds.write().await;

                        embeds_map.insert(in_embed.name.clone(), Box::new(e) as Box<dyn Embed>);

                        tracing::info!("Embedding [{}] ready to embed", in_embed.name);
                        metrics::embeddings::COUNT.add(
                            1,
                            &[
                                KeyValue::new("embeddings", in_embed.name.clone()),
                                KeyValue::new(
                                    "source",
                                    in_embed
                                        .get_prefix()
                                        .map(|x| x.to_string())
                                        .unwrap_or_default(),
                                ),
                            ],
                        );
                        self.status
                            .update_embedding(&in_embed.name, status::ComponentStatus::Ready);
                    }
                    Err(e) => {
                        metrics::embeddings::LOAD_ERROR.add(1, &[]);
                        self.status
                            .update_embedding(&in_embed.name, status::ComponentStatus::Error);
                        tracing::warn!("Failed to load embedding {}.\nError: {}\nVerify configuration and try again.\nFor details, visit https://spiceai.org/docs/components/embeddings", in_embed.name, e);
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    /// Loads a specific Embedding model from the spicepod. If an error occurs, no retry attempt is made.
    async fn load_embedding(&self, in_embed: &Embeddings) -> Result<TaskEmbed> {
        let l = try_to_embedding(in_embed, Arc::clone(&self.secrets))
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
}
