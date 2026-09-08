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

use async_openai::types::embeddings::{CreateEmbeddingResponse, Embedding, EmbeddingVector};

use crate::Sizeable;
use crate::sizing::{ENTRY_OVERHEAD_BYTES, f32_vectors_heap_size};

#[derive(Debug, Clone)]
pub enum CachedEmbeddingResult {
    Response(CreateEmbeddingResponse),
    Vector(Vec<Vec<f32>>),
}

/// The heap one embedding owns, excluding the struct itself.
///
/// The vector is matched rather than measured with `EmbeddingVector::len()`:
/// on the base64 arm that method decodes the whole string to count floats, and
/// `expect`s on invalid base64. A weigher runs on every insert, so it must be
/// cheap and it must not panic on a value the cache is already holding.
fn embedding_heap_size(embedding: &Embedding) -> usize {
    embedding.object.capacity()
        + match &embedding.embedding {
            EmbeddingVector::Float(floats) => floats.capacity() * std::mem::size_of::<f32>(),
            EmbeddingVector::Base64(encoded) => encoded.capacity(),
        }
}

impl Sizeable for CachedEmbeddingResult {
    fn get_memory_size(&self) -> usize {
        let payload = match self {
            CachedEmbeddingResult::Response(response) => {
                response.object.capacity()
                    + response.model.capacity()
                    + response.data.len() * std::mem::size_of::<Embedding>()
                    + response.data.iter().map(embedding_heap_size).sum::<usize>()
            }
            CachedEmbeddingResult::Vector(vectors) => f32_vectors_heap_size(vectors),
        };

        std::mem::size_of::<Self>() + payload + ENTRY_OVERHEAD_BYTES
    }
}

#[cfg(test)]
mod tests {
    use async_openai::types::embeddings::EmbeddingUsage;

    use super::*;

    fn response(embeddings: Vec<EmbeddingVector>) -> CachedEmbeddingResult {
        CachedEmbeddingResult::Response(CreateEmbeddingResponse {
            object: "list".to_string(),
            model: "text-embedding-3-small".to_string(),
            data: embeddings
                .into_iter()
                .enumerate()
                .map(|(index, embedding)| Embedding {
                    index: u32::try_from(index).unwrap_or_default(),
                    object: "embedding".to_string(),
                    embedding,
                })
                .collect(),
            usage: EmbeddingUsage {
                prompt_tokens: 0,
                total_tokens: 0,
            },
        })
    }

    /// `EmbeddingVector::len()` decodes the whole string to count floats on the
    /// base64 arm, and `expect`s on input it cannot decode. A weigher runs on
    /// every insert, so sizing must not depend on either.
    #[test]
    fn a_base64_embedding_is_sized_without_decoding_it() {
        let undecodable = response(vec![EmbeddingVector::Base64("!not base64!".to_string())]);
        let long = response(vec![EmbeddingVector::Base64("A".repeat(8_192))]);

        assert!(
            undecodable.get_memory_size() > 0,
            "sizing must not depend on the payload being decodable"
        );
        assert!(
            long.get_memory_size() >= 8_192,
            "a base64 embedding must be charged the string it holds, got {}",
            long.get_memory_size()
        );
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/12931>:
    /// the vector arm charged every vector the *first* one's length.
    #[test]
    fn a_ragged_vector_batch_is_charged_per_vector() {
        let ragged = CachedEmbeddingResult::Vector(vec![vec![0.0_f32; 1], vec![0.0_f32; 4_096]]);

        assert!(
            ragged.get_memory_size() > 4_096 * std::mem::size_of::<f32>(),
            "the long vector must be charged in full, got {}",
            ragged.get_memory_size()
        );
    }

    #[test]
    fn an_empty_response_is_still_billed() {
        assert!(
            response(Vec::new()).get_memory_size() > 0,
            "an entry the cache is holding is never free"
        );
    }
}
