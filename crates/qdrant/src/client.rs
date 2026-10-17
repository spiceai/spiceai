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

use crate::error::Error::{ClientBuild, CollectionMismatch, Qdrant as QdrantErr};
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

    async fn validate_collection(
        &self,
        collection: &str,
        dimension: u64,
        distance: qdrant_client::qdrant::Distance,
    ) -> Result<()> {
        let info = self
            .client
            .collection_info(collection)
            .await
            .map_err(|source| QdrantErr { source })?;
        let Some(result) = info.result else {
            return Err(CollectionMismatch {
                collection: collection.to_string(),
                expected: format!(
                    "vector size {dimension} and distance '{}'",
                    distance.as_str_name()
                ),
                actual: "no readable collection info".to_string(),
            });
        };
        let vectors_config = result
            .config
            .as_ref()
            .and_then(|c| c.params.as_ref())
            .and_then(|p| p.vectors_config.as_ref());
        let expected = format!(
            "vector size {dimension} and distance '{}'",
            distance.as_str_name()
        );
        let actual = match actual_vector_params(vectors_config) {
            Ok((size, actual_distance)) if size == dimension && actual_distance == distance => {
                return Ok(());
            }
            Ok((size, actual_distance)) => format!(
                "vector size {size} and distance '{}'",
                actual_distance.as_str_name()
            ),
            Err(detail) => detail,
        };
        Err(CollectionMismatch {
            collection: collection.to_string(),
            expected,
            actual,
        })
    }
}

fn actual_vector_params(
    vectors_config: Option<&qdrant_client::qdrant::VectorsConfig>,
) -> std::result::Result<(u64, qdrant_client::qdrant::Distance), String> {
    use qdrant_client::qdrant::vectors_config::Config;
    let Some(vectors_config) = vectors_config else {
        return Err("no vector configuration".to_string());
    };
    match &vectors_config.config {
        Some(Config::Params(params)) => {
            let distance = qdrant_client::qdrant::Distance::try_from(params.distance)
                .map_err(|_| format!("unknown distance {}", params.distance))?;
            Ok((params.size, distance))
        }
        Some(Config::ParamsMap(_)) => Err("named vectors".to_string()),
        None => Err("no vector configuration".to_string()),
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
            self.validate_collection(collection, dimension, distance)
                .await?;
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

#[cfg(test)]
mod tests {
    use qdrant_client::qdrant::{Distance, VectorParams, VectorsConfig, vectors_config::Config};
    use std::collections::HashMap;

    use super::actual_vector_params;

    fn single_vector_config(size: u64, distance: Distance) -> VectorsConfig {
        VectorsConfig {
            config: Some(Config::Params(VectorParams {
                size,
                distance: distance as i32,
                ..Default::default()
            })),
        }
    }

    #[test]
    fn single_vector_params_round_trip() {
        let config = single_vector_config(4, Distance::Cosine);
        assert_eq!(
            actual_vector_params(Some(&config)).expect("params"),
            (4, Distance::Cosine)
        );
    }

    #[test]
    fn named_vectors_and_missing_config_are_reported_not_matched() {
        let named = VectorsConfig {
            config: Some(Config::ParamsMap(qdrant_client::qdrant::VectorParamsMap {
                map: HashMap::from([(
                    "image".to_string(),
                    VectorParams {
                        size: 4,
                        distance: Distance::Cosine as i32,
                        ..Default::default()
                    },
                )]),
            })),
        };
        assert_eq!(
            actual_vector_params(Some(&named)).expect_err("named vectors"),
            "named vectors".to_string()
        );
        assert_eq!(
            actual_vector_params(None).expect_err("missing config"),
            "no vector configuration".to_string()
        );
        assert_eq!(
            actual_vector_params(Some(&VectorsConfig { config: None })).expect_err("empty config"),
            "no vector configuration".to_string()
        );
    }
}
