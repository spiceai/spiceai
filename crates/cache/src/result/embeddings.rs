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
use crate::sizing::{ENTRY_OVERHEAD_BYTES, arc_heap_size, f32_vectors_heap_size};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CachedEmbeddingResult {
    /// Shared so a cache hit is an `Arc` clone rather than a deep copy of the
    /// `OpenAI` response (large embedding payloads).
    Response(Arc<CreateEmbeddingResponse>),
    /// Shared so a cache hit does not deep-clone `Vec<Vec<f32>>` on the get path.
    Vector(Arc<Vec<Vec<f32>>>),
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
        // `size_of::<Self>()` is only the enum discriminant + Arc pointer.
        // Bill the Arc header and pointee struct via `arc_heap_size`, then the
        // heap those pointees own (string/vec buffers, embedding payloads).
        match self {
            CachedEmbeddingResult::Response(response) => {
                std::mem::size_of::<Self>()
                    + arc_heap_size::<CreateEmbeddingResponse>()
                    + response.object.capacity()
                    + response.model.capacity()
                    + response.data.capacity() * std::mem::size_of::<Embedding>()
                    + response.data.iter().map(embedding_heap_size).sum::<usize>()
                    + ENTRY_OVERHEAD_BYTES
            }
            CachedEmbeddingResult::Vector(vectors) => {
                std::mem::size_of::<Self>()
                    + arc_heap_size::<Vec<Vec<f32>>>()
                    + f32_vectors_heap_size(vectors.as_ref())
                    + ENTRY_OVERHEAD_BYTES
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_openai::types::embeddings::EmbeddingUsage;

    use super::*;

    fn response(embeddings: Vec<EmbeddingVector>) -> CachedEmbeddingResult {
        CachedEmbeddingResult::Response(Arc::new(CreateEmbeddingResponse {
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
        }))
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
        let ragged =
            CachedEmbeddingResult::Vector(Arc::new(vec![vec![0.0_f32; 1], vec![0.0_f32; 4_096]]));

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

    #[test]
    fn arc_payloads_are_billed_beyond_the_enum_pointer() {
        let empty = response(Vec::new());
        let pointer_only = std::mem::size_of::<CachedEmbeddingResult>() + ENTRY_OVERHEAD_BYTES;
        assert!(
            empty.get_memory_size() > pointer_only,
            "Arc header + pointee must be billed; got {} vs pointer_only {}",
            empty.get_memory_size(),
            pointer_only
        );
        let min_arc = pointer_only + crate::sizing::arc_heap_size::<CreateEmbeddingResponse>();
        assert!(
            empty.get_memory_size() >= min_arc,
            "expected at least arc_heap_size over the enum pointer, got {}",
            empty.get_memory_size()
        );
    }
}
