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

use async_openai::types::embeddings::CreateEmbeddingResponse;

use crate::Sizeable;

#[derive(Debug, Clone)]
pub enum CachedEmbeddingResult {
    Response(CreateEmbeddingResponse),
    Vector(Vec<Vec<f32>>),
}

impl Sizeable for CachedEmbeddingResult {
    fn get_memory_size(&self) -> usize {
        match self {
            CachedEmbeddingResult::Response(resp) => resp
                .data
                .iter()
                .map(|e| e.embedding.len() * std::mem::size_of::<f32>())
                .sum(),
            CachedEmbeddingResult::Vector(vectors) => {
                vectors.len()
                    * vectors
                        .first()
                        .map_or(0, |v| v.len() * std::mem::size_of::<f32>())
            }
        }
    }
}
