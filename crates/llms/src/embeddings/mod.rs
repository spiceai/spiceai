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
#![allow(clippy::missing_errors_doc)]

pub use async_openai::types::embeddings::EmbeddingInput;

// The contract lives in `embed-api`, below every provider; re-exported so existing
// `llms::embeddings::…` paths resolve, including SNAFU's generated context selectors.
pub use embed_api::*;
use std::sync::Arc;

#[cfg(feature = "local_embed")]
pub mod candle;

#[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub async fn get_or_infer_size(inner: &Arc<dyn Embed>) -> Result<i32> {
    let size = inner.size();
    if size != -1 {
        // Don't need to infer.
        return Ok(size);
    }
    match inner
        .embed(EmbeddingInput::String("infer_size".to_string()))
        .await
    {
        Ok(vec) => match vec.first() {
            Some(first) => {
                tracing::trace!("Inferred size of embedding model vectors={}", first.len());
                Ok(first.len() as i32)
            }
            None => Err(Error::FailedToCreateEmbedding {
                source: "Failed to infer size of embedding model, empty response".into(),
            }),
        },
        Err(e) => {
            tracing::warn!("Failed to infer size of embedding model");
            Err(e)
        }
    }
}
