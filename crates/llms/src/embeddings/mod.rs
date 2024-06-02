/*
Copyright 2024 The Spice.ai OSS Authors
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
use async_trait::async_trait;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to prepare input for embedding: {source}"))]
    FailedToPrepareInput {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create embedding: {source}"))]
    FailedToCreateEmbedding {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingInput {
    String(String),
    Tokens(Vec<u32>),
    StringBatch(Vec<String>),
    TokensBatch(Vec<Vec<u32>>),
}

#[async_trait]
pub trait Embed: Sync + Send {
    async fn embed(&mut self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>>;

    /// Returns the size of the embedding vector returned by the model.
    fn size(&self) -> i32;
}
