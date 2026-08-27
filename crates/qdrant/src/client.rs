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

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Error::{ClientBuild, Qdrant as QdrantErr};
use crate::error::Result;

use crate::payload::{PointData, SearchResult};
use crate::scroll::ScrollPage;

#[derive(Clone)]
pub struct QdrantConnection {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub connect_timeout: Option<Duration>,
}

impl std::fmt::Debug for QdrantConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantConnection")
            .field("endpoint", &self.endpoint)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

#[async_trait]
pub trait QdrantStore: Send + Sync + std::fmt::Debug + 'static {
    async fn collection_exists(&self, collection: &str) -> Result<bool>;

    async fn ensure_collection(
        &self,
        collection: &str,
        dimension: u64,
        distance: qdrant_client::qdrant::Distance,
    ) -> Result<()>;

    async fn upsert(
        &self,
        collection: &str,
        points: Vec<PointData>,
        batch_size: usize,
    ) -> Result<()>;

    async fn delete_by_ids(
        &self,
        collection: &str,
        ids: Vec<qdrant_client::qdrant::PointId>,
    ) -> Result<()>;

    async fn search(
        &self,
        collection: &str,
        vector: Vec<f32>,
        limit: u64,
        filter: Option<qdrant_client::qdrant::Filter>,
    ) -> Result<Vec<SearchResult>>;

    async fn scroll(
        &self,
        collection: &str,
        page_size: u32,
        offset: Option<qdrant_client::qdrant::PointId>,
    ) -> Result<ScrollPage>;

    async fn create_field_index(
        &self,
        collection: &str,
        field_name: &str,
        field_type: qdrant_client::qdrant::FieldType,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct Qdrant {
    client: qdrant_client::Qdrant,
}

impl std::fmt::Debug for Qdrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Qdrant").finish_non_exhaustive()
    }
}

impl Qdrant {
    /// Builds a client for a Qdrant server.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot be built from the endpoint,
    /// API key, and timeout in `connection`.
    pub fn new(connection: &QdrantConnection) -> Result<Self> {
        let mut builder = qdrant_client::config::QdrantConfig::from_url(&connection.endpoint);
        if let Some(api_key) = &connection.api_key {
            builder = builder.api_key(api_key.clone());
        }
        if let Some(timeout) = connection.connect_timeout {
            builder = builder.timeout(timeout);
        }
        let client = builder.build().map_err(|source| ClientBuild {
            endpoint: connection.endpoint.clone(),
            source,
        })?;
        Ok(Self { client })
    }
}

#[async_trait]
impl QdrantStore for Qdrant {
    async fn collection_exists(&self, collection: &str) -> Result<bool> {
        self.client
            .collection_exists(collection)
            .await
            .map_err(|source| QdrantErr { source })
    }

    async fn ensure_collection(
        &self,
        collection: &str,
        dimension: u64,
        distance: qdrant_client::qdrant::Distance,
    ) -> Result<()> {
        if self.collection_exists(collection).await? {
            return Ok(());
        }

        let request = qdrant_client::qdrant::CreateCollection {
            collection_name: collection.to_string(),
            vectors_config: Some(qdrant_client::qdrant::VectorsConfig {
                config: Some(qdrant_client::qdrant::vectors_config::Config::Params(
                    qdrant_client::qdrant::VectorParams {
                        size: dimension,
                        distance: distance as i32,
                        ..Default::default()
                    },
                )),
            }),
            ..Default::default()
        };
        self.client
            .create_collection(request)
            .await
            .map_err(|source| QdrantErr { source })?;
        Ok(())
    }

    async fn upsert(
        &self,
        collection: &str,
        points: Vec<PointData>,
        batch_size: usize,
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        let request = qdrant_client::qdrant::UpsertPoints {
            collection_name: collection.to_string(),
            wait: Some(true),
            points: points.into_iter().map(From::from).collect(),
            ..Default::default()
        };
        let batch_size = if batch_size == 0 {
            crate::DEFAULT_UPSERT_BATCH_SIZE
        } else {
            batch_size
        };
        self.client
            .upsert_points_chunked(request, batch_size)
            .await
            .map_err(|source| QdrantErr { source })?;
        Ok(())
    }

    async fn delete_by_ids(
        &self,
        collection: &str,
        ids: Vec<qdrant_client::qdrant::PointId>,
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let request = qdrant_client::qdrant::DeletePointsBuilder::new(collection)
            .points(ids)
            .wait(true)
            .build();
        self.client
            .delete_points(request)
            .await
            .map_err(|source| QdrantErr { source })?;
        Ok(())
    }

    async fn search(
        &self,
        collection: &str,
        vector: Vec<f32>,
        limit: u64,
        filter: Option<qdrant_client::qdrant::Filter>,
    ) -> Result<Vec<SearchResult>> {
        let mut builder =
            qdrant_client::qdrant::SearchPointsBuilder::new(collection, vector, limit)
                .with_payload(true)
                .with_vectors(true);
        if let Some(filter) = filter {
            builder = builder.filter(filter);
        }
        let response = self
            .client
            .search_points(builder)
            .await
            .map_err(|source| QdrantErr { source })?;
        Ok(response.result.into_iter().map(From::from).collect())
    }

    async fn scroll(
        &self,
        collection: &str,
        page_size: u32,
        offset: Option<qdrant_client::qdrant::PointId>,
    ) -> Result<ScrollPage> {
        let page_size = if page_size == 0 {
            crate::DEFAULT_SCROLL_PAGE_SIZE
        } else {
            page_size
        };
        let mut request = qdrant_client::qdrant::ScrollPointsBuilder::new(collection)
            .limit(page_size)
            .with_payload(true)
            .with_vectors(true);
        if let Some(offset) = offset {
            request = request.offset(offset);
        }
        let page = self
            .client
            .scroll(request)
            .await
            .map_err(|source| QdrantErr { source })?;
        let next = page.next_page_offset;
        Ok(ScrollPage {
            points: page.result,
            next_page_offset: next,
        })
    }

    async fn create_field_index(
        &self,
        collection: &str,
        field_name: &str,
        field_type: qdrant_client::qdrant::FieldType,
    ) -> Result<()> {
        self.client
            .create_field_index(
                qdrant_client::qdrant::CreateFieldIndexCollectionBuilder::new(
                    collection, field_name, field_type,
                ),
            )
            .await
            .map(|_| ())
            .map_err(|source| QdrantErr { source })
    }
}
