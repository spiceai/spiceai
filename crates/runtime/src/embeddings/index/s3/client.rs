/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Metrics and caching middleware for S3 Vectors API client.
//!
//! This module provides a middleware that wraps an `S3Vectors` implementation
//! and adds metrics collection and response caching.

use async_trait::async_trait;
use s3_vectors::{
    CreateIndexError, CreateIndexInput, CreateIndexOutput, CreateVectorBucketError,
    CreateVectorBucketInput, CreateVectorBucketOutput, DeleteIndexError, DeleteIndexInput,
    DeleteIndexOutput, DeleteVectorBucketError, DeleteVectorBucketInput, DeleteVectorBucketOutput,
    DeleteVectorBucketPolicyError, DeleteVectorBucketPolicyInput, DeleteVectorBucketPolicyOutput,
    DeleteVectorsError, DeleteVectorsInput, DeleteVectorsOutput, GetIndexError, GetIndexInput,
    GetIndexOutput, GetVectorBucketError, GetVectorBucketInput, GetVectorBucketOutput,
    GetVectorBucketPolicyError, GetVectorBucketPolicyInput, GetVectorBucketPolicyOutput,
    GetVectorsError, GetVectorsInput, GetVectorsOutput, ListIndexesError, ListIndexesInput,
    ListIndexesOutput, ListVectorBucketsError, ListVectorBucketsInput, ListVectorBucketsOutput,
    ListVectorsError, ListVectorsInput, ListVectorsOutput, PutVectorBucketPolicyError,
    PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput, PutVectorsError, PutVectorsInput,
    PutVectorsOutput, QueryVectorsError, QueryVectorsInput, QueryVectorsOutput, S3Vectors,
    SdkError,
};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::timing::TimeMeasurement;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

const TTL_DURATION_MINIMUM: Duration = Duration::from_secs(5);

/// Metrics and caching middleware for S3 Vectors API.
///
/// Wraps an `S3Vectors` implementation and adds:
/// - Latency, request count, and error count metrics
/// - Tracing spans for each operation
/// - TTL-based caching for `list_indexes` responses
pub struct S3VectorsTelemetryMiddleware<T: S3Vectors + Send + Sync + ?Sized> {
    inner: Arc<T>,
    list_indexes_cache: RwLock<HashMap<String, (ListIndexesOutput, Instant)>>,
    ttl: Option<Duration>,
}

impl<T: S3Vectors + Send + Sync + ?Sized> S3VectorsTelemetryMiddleware<T> {
    /// Creates a new metrics/cache middleware wrapping the given client.
    ///
    /// If `ttl` is provided, `list_indexes` results will be cached for that duration.
    /// Minimum TTL is 5 seconds.
    pub fn new(client: Arc<T>, ttl: Option<Duration>) -> Self {
        let ttl = ttl.map(|d| {
            if d < TTL_DURATION_MINIMUM {
                tracing::warn!("S3 vector index poll interval minimum is 5s.");
                TTL_DURATION_MINIMUM
            } else {
                d
            }
        });

        Self {
            inner: client,
            list_indexes_cache: RwLock::new(HashMap::new()),
            ttl,
        }
    }
}

#[async_trait]
impl<T: S3Vectors + Send + Sync + 'static + ?Sized> S3Vectors for S3VectorsTelemetryMiddleware<T> {
    async fn create_index(
        &self,
        input: &CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::create_index::LATENCY, &[]);
        super::metrics::create_index::REQUESTS.add(1, &[]);

        let result = self
            .inner
            .create_index(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__create_index",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                bucket_arn = input.vector_bucket_arn()
            ))
            .await
            .inspect_err(|_| super::metrics::create_index::ERRORS.add(1, &[]));

        // Invalidate cache on successful creation
        if result.is_ok()
            && self.ttl.is_some()
            && let Some(bucket) = &input.vector_bucket_name
        {
            let mut cache = self.list_indexes_cache.write().await;
            cache.remove(bucket);
        }

        result
    }

    async fn create_vector_bucket(
        &self,
        input: &CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::create_vector_bucket::LATENCY, &[]);
        super::metrics::create_vector_bucket::REQUESTS.add(1, &[]);

        self.inner
            .create_vector_bucket(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__create_vector_bucket",
                bucket_name = input.vector_bucket_name(),
            ))
            .await
            .inspect_err(|_| super::metrics::create_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn delete_index(
        &self,
        input: &DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_index::LATENCY, &[]);
        super::metrics::delete_index::REQUESTS.add(1, &[]);

        self.inner
            .delete_index(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__delete_index",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn()
            ))
            .await
            .inspect_err(|_| super::metrics::delete_index::ERRORS.add(1, &[]))
    }

    async fn delete_vector_bucket(
        &self,
        input: &DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_vector_bucket::LATENCY, &[]);
        super::metrics::delete_vector_bucket::REQUESTS.add(1, &[]);

        self.inner
            .delete_vector_bucket(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__delete_vector_bucket",
                bucket_name = input.vector_bucket_name(),
                vector_bucket_arn = input.vector_bucket_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::delete_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: &DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        let _guard =
            TimeMeasurement::new(&super::metrics::delete_vector_bucket_policy::LATENCY, &[]);
        super::metrics::delete_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.inner
            .delete_vector_bucket_policy(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__delete_vector_bucket_policy",
                bucket_name = input.vector_bucket_name(),
                vector_bucket_arn = input.vector_bucket_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::delete_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn delete_vectors(
        &self,
        input: &DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::delete_vectors::LATENCY, &[]);
        super::metrics::delete_vectors::REQUESTS.add(1, &[]);

        self.inner
            .delete_vectors(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__delete_vectors",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::delete_vectors::ERRORS.add(1, &[]))
    }

    async fn get_vector_bucket_policy(
        &self,
        input: &GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vector_bucket_policy::LATENCY, &[]);
        super::metrics::get_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.inner
            .get_vector_bucket_policy(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__get_vector_bucket_policy",
                bucket_name = input.vector_bucket_name(),
                bucket_arn = input.vector_bucket_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::get_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn get_index(
        &self,
        input: &GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_index::LATENCY, &[]);
        super::metrics::get_index::REQUESTS.add(1, &[]);

        self.inner
            .get_index(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__get_index",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::get_index::ERRORS.add(1, &[]))
    }

    async fn get_vector_bucket(
        &self,
        input: &GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vector_bucket::LATENCY, &[]);
        super::metrics::get_vector_bucket::REQUESTS.add(1, &[]);

        self.inner
            .get_vector_bucket(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__get_vector_bucket",
                bucket_name = input.vector_bucket_name(),
                bucket_arn = input.vector_bucket_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::get_vector_bucket::ERRORS.add(1, &[]))
    }

    async fn get_vectors(
        &self,
        input: &GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::get_vectors::LATENCY, &[]);
        super::metrics::get_vectors::REQUESTS.add(1, &[]);

        self.inner
            .get_vectors(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__get_vectors",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn(),
                return_data = input.return_data(),
                return_metadata = input.return_metadata(),
            ))
            .await
            .inspect_err(|_| super::metrics::get_vectors::ERRORS.add(1, &[]))
    }

    async fn list_indexes(
        &self,
        input: &ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        // Check cache if next_token is None (full list)
        let is_full_list = input.next_token.is_none();
        if is_full_list && let Some(ttl) = self.ttl {
            // Fast path: check with read lock first
            {
                let cache = self.list_indexes_cache.read().await;
                if let Some(bucket) = &input.vector_bucket_name
                    && let Some((cached_output, timestamp)) = cache.get(bucket)
                    && timestamp.elapsed() < ttl
                {
                    return Ok(cached_output.clone());
                }
            }
            // Read lock dropped here before API call
        }

        let result = {
            let _guard = TimeMeasurement::new(&super::metrics::list_indexes::LATENCY, &[]);
            super::metrics::list_indexes::REQUESTS.add(1, &[]);

            self.inner
                .list_indexes(input)
                .instrument(info_span!(
                    target: "task_history",
                    "s3_vectors__list_indexes",
                    bucket_name = input.vector_bucket_name(),
                    bucket_arn = input.vector_bucket_arn(),
                    max_results = input.max_results()
                ))
                .await
                .inspect_err(|_| super::metrics::list_indexes::ERRORS.add(1, &[]))
        };

        // Cache successful full list results with double-check pattern
        if is_full_list
            && let Some(ttl) = self.ttl
            && let Ok(output) = &result
            && let Some(bucket) = input.vector_bucket_name.clone()
        {
            let mut cache = self.list_indexes_cache.write().await;
            // Check again - another thread might have cached during our API call
            if let Some((_, timestamp)) = cache.get(&bucket)
                && timestamp.elapsed() < ttl
            {
                // Fresh cache exists, don't overwrite with potentially older data
                return result;
            }
            cache.insert(bucket, (output.clone(), Instant::now()));
        }

        result
    }

    async fn list_vector_buckets(
        &self,
        input: &ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::list_vector_buckets::LATENCY, &[]);
        super::metrics::list_vector_buckets::REQUESTS.add(1, &[]);

        self.inner
            .list_vector_buckets(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__list_vector_buckets",
            ))
            .await
            .inspect_err(|_| super::metrics::list_vector_buckets::ERRORS.add(1, &[]))
    }

    async fn list_vectors(
        &self,
        input: &ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::list_vectors::LATENCY, &[]);
        super::metrics::list_vectors::REQUESTS.add(1, &[]);

        self.inner
            .list_vectors(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__list_vectors",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn(),
                max_results = input.max_results(),
                return_metadata = input.return_metadata(),
                return_data = input.return_data()
            ))
            .await
            .inspect_err(|_| super::metrics::list_vectors::ERRORS.add(1, &[]))
    }

    async fn put_vector_bucket_policy(
        &self,
        input: &PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        let _guard = TimeMeasurement::new(&super::metrics::put_vector_bucket_policy::LATENCY, &[]);
        super::metrics::put_vector_bucket_policy::REQUESTS.add(1, &[]);

        self.inner
            .put_vector_bucket_policy(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__put_vector_bucket_policy",
                bucket_name = input.vector_bucket_name(),
                bucket_arn = input.vector_bucket_arn(),
            ))
            .await
            .inspect_err(|_| super::metrics::put_vector_bucket_policy::ERRORS.add(1, &[]))
    }

    async fn put_vectors(
        &self,
        input: &PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::put_vectors::LATENCY, &[]);
        super::metrics::put_vectors::REQUESTS.add(1, &[]);

        self.inner
            .put_vectors(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__put_vectors",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn()
            ))
            .await
            .inspect_err(|_| super::metrics::put_vectors::ERRORS.add(1, &[]))
    }

    async fn query_vectors(
        &self,
        input: &QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        let _guard = TimeMeasurement::new(&super::metrics::query_vectors::LATENCY, &[]);
        super::metrics::query_vectors::REQUESTS.add(1, &[]);

        self.inner
            .query_vectors(input)
            .instrument(info_span!(
                target: "task_history",
                "s3_vectors__query_vectors",
                bucket_name = input.vector_bucket_name(),
                index_name = input.index_name(),
                index_arn = input.index_arn(),
                top_k = input.top_k(),
                return_metadata = input.return_metadata(),
                return_distance = input.return_distance()
            ))
            .await
            .inspect_err(|_| super::metrics::query_vectors::ERRORS.add(1, &[]))
    }
}

#[cfg(test)]
mod tests {
    // Type alias for backward compatibility
    pub type S3VectorClient = S3VectorsTelemetryMiddleware<dyn S3Vectors + Send + Sync>;

    use std::{sync::Arc, time::Duration};

    use s3_vectors::{
        CreateIndexInput, DataType, DistanceMetric, ListIndexesInput, mock::MockClient,
    };
    use tokio::time::{pause, resume};

    use super::*;

    async fn create_test_index(client: &S3VectorClient, bucket_name: &str, index_name: &str) {
        let create_input = CreateIndexInput::builder()
            .vector_bucket_name(bucket_name)
            .index_name(index_name)
            .data_type(DataType::Float32)
            .dimension(128)
            .distance_metric(DistanceMetric::Cosine)
            .build()
            .expect("test assertion");
        let _create_result = client
            .create_index(&create_input)
            .await
            .expect("test assertion");
    }

    #[tokio::test]
    async fn test_cache_hit_within_ttl() {
        pause();
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, Some(Duration::from_secs(10)));

        create_test_index(&client, "test-bucket", "test-index").await;

        // populate cache
        let input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input).await.expect("test assertion");

        // within TTL should use cache
        let output = client.list_indexes(&input).await.expect("test assertion");

        assert!(!output.indexes().is_empty());
        assert_eq!(mock_client.get_list_indexes_call_count("test-bucket"), 1);
        resume();
    }

    #[tokio::test]
    async fn test_cache_miss_after_ttl_expires() {
        pause();
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, Some(Duration::from_secs(5)));

        create_test_index(&client, "test-bucket", "test-index").await;

        // populate cache
        let input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input).await.expect("test assertion");

        // advance time past TTL
        tokio::time::advance(Duration::from_secs(6)).await;

        // miss cache and call client again
        let output = client.list_indexes(&input).await.expect("test assertion");

        assert!(!output.indexes().is_empty());
        assert_eq!(mock_client.get_list_indexes_call_count("test-bucket"), 2);
        resume();
    }

    #[tokio::test]
    async fn test_no_caching_when_ttl_none() {
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, None);

        create_test_index(&client, "test-bucket", "test-index").await;

        let input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input).await.expect("test assertion");

        let output = client.list_indexes(&input).await.expect("test assertion");

        assert!(!output.indexes().is_empty());
        assert_eq!(mock_client.get_list_indexes_call_count("test-bucket"), 2);
    }

    #[tokio::test]
    async fn test_pagination_bypasses_cache() {
        pause();
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, Some(Duration::from_secs(10)));

        create_test_index(&client, "test-bucket", "test-index").await;

        let input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input).await.expect("test assertion");

        // call with next_token should bypass cache
        let input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .next_token("some-token")
            .build()
            .expect("test assertion");
        let output = client.list_indexes(&input).await.expect("test assertion");

        assert!(!output.indexes().is_empty());
        assert_eq!(mock_client.get_list_indexes_call_count("test-bucket"), 2);
        resume();
    }

    #[tokio::test]
    async fn test_cache_invalidation_on_create_index() {
        pause();
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, Some(Duration::from_secs(10)));

        let list_input = ListIndexesInput::builder()
            .vector_bucket_name("test-bucket")
            .build()
            .expect("test assertion");
        let _ = client
            .list_indexes(&list_input)
            .await
            .expect("test assertion");

        // create index should invalidate cache
        create_test_index(&client, "test-bucket", "new-index").await;

        let output = client
            .list_indexes(&list_input)
            .await
            .expect("test assertion");

        assert!(!output.indexes().is_empty());
        assert_eq!(mock_client.get_list_indexes_call_count("test-bucket"), 2);
        resume();
    }

    #[tokio::test]
    async fn test_different_buckets_cached_separately() {
        pause();
        let mock_client = Arc::new(MockClient::new());
        mock_client.reset_call_counts();
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        let client = S3VectorClient::new(client, Some(Duration::from_secs(10)));

        // Create indexes in both buckets
        create_test_index(&client, "bucket1", "test-index1").await;
        create_test_index(&client, "bucket2", "test-index2").await;

        let input1 = ListIndexesInput::builder()
            .vector_bucket_name("bucket1")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input1).await.expect("test assertion");

        let input2 = ListIndexesInput::builder()
            .vector_bucket_name("bucket2")
            .build()
            .expect("test assertion");
        let _ = client.list_indexes(&input2).await.expect("test assertion");

        let result = client.list_indexes(&input1).await.expect("test assertion");
        assert!(!result.indexes().is_empty());

        let result = client.list_indexes(&input2).await.expect("test assertion");
        assert!(!result.indexes().is_empty());

        assert_eq!(mock_client.get_list_indexes_call_count("bucket1"), 1);
        assert_eq!(mock_client.get_list_indexes_call_count("bucket2"), 1);

        resume();
    }
}
