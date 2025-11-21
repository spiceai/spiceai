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
use std::sync::Arc;
use tokio::sync::OnceCell;

/// [`ComputeQueryVector`] allows lazy calculation of embedding vectors.
///
/// This is useful to allow database query planning to describe and handle/instantiate embedding models without calculating the underlying vector (importantly allows [`S3VectorsQueryTable`] to be instantiated in a non-async setting).
#[async_trait]
pub trait ComputeQueryVector: std::fmt::Debug + Send + Sync {
    async fn compute_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>>;
}

/// An implementation of [`ComputeQueryVector`] that lazily performs [`ComputeQueryVector::compute_vector`] only once, and then shares that result for all subsequent calls.
///
/// Importantly, the `query` payload must be known at construction.
///
/// Useful for when the same embedding call is performed several times in a query (e.g. Spilling or partitioned vector indexes).
#[allow(clippy::type_complexity)]
#[derive(Clone, Debug)]
pub struct CachedQueryVector {
    inner: Arc<dyn ComputeQueryVector>,
    query: Arc<String>,
    cached: Arc<OnceCell<Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>>>>,
}

impl CachedQueryVector {
    pub fn new(inner: Arc<dyn ComputeQueryVector>, query: String) -> Self {
        Self {
            inner,
            query: Arc::new(query),
            cached: Arc::new(OnceCell::new()),
        }
    }
}

#[async_trait]
impl ComputeQueryVector for CachedQueryVector {
    async fn compute_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        if query != self.query.as_ref() {
            return Err(Box::from(
                "CachedQueryVector called with different query than it was constructed with"
                    .to_string(),
            ));
        }
        match self
            .cached
            .get_or_init(|| async { self.inner.compute_vector(&self.query).await })
            .await
        {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(Box::from(e.to_string())),
        }
    }
}
