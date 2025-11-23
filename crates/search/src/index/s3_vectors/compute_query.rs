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

use async_trait::async_trait;
use data_components::s3_vectors::compute_query::ComputeQueryVector;
use llms::embeddings::Embed;
use snafu::ResultExt;
use std::sync::Arc;

#[derive(Debug)]
pub struct EmbedQuery(pub Arc<dyn Embed>);

#[async_trait]
impl ComputeQueryVector for EmbedQuery {
    async fn compute_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(vec) = self
            .0
            .embed(llms::embeddings::EmbeddingInput::String(query.to_string()))
            .await
            .boxed()?
            .pop()
        else {
            return Err(Box::from(
                "no embedding vector created for query".to_string(),
            ));
        };
        Ok(vec)
    }
}
