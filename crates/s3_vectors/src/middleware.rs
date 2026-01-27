/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Middleware pattern for `S3Vectors` clients.
//!
//! This module provides a base middleware implementation that wraps another `S3Vectors`
//! implementation, allowing for cross-cutting concerns like logging, retrying, and metrics
//! to be added in a composable way.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use s3_vectors::{S3Vectors, S3VectorsClient};
//! use s3_vectors::middleware::S3VectorsMiddleware;
//!
//! // Wrap a client with retry middleware
//! let client = Arc::new(S3VectorsClient::new(&config));
//! let with_retry = RetryMiddleware::new(client);
//!
//! // Chain middleware
//! let with_logging = LoggingMiddleware::new(Arc::new(with_retry));
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3vectors::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        create_index::{CreateIndexError, CreateIndexInput, CreateIndexOutput},
        create_vector_bucket::{
            CreateVectorBucketError, CreateVectorBucketInput, CreateVectorBucketOutput,
        },
        delete_index::{DeleteIndexError, DeleteIndexInput, DeleteIndexOutput},
        delete_vector_bucket::{
            DeleteVectorBucketError, DeleteVectorBucketInput, DeleteVectorBucketOutput,
        },
        delete_vector_bucket_policy::{
            DeleteVectorBucketPolicyError, DeleteVectorBucketPolicyInput,
            DeleteVectorBucketPolicyOutput,
        },
        delete_vectors::{DeleteVectorsError, DeleteVectorsInput, DeleteVectorsOutput},
        get_index::{GetIndexError, GetIndexInput, GetIndexOutput},
        get_vector_bucket::{GetVectorBucketError, GetVectorBucketInput, GetVectorBucketOutput},
        get_vector_bucket_policy::{
            GetVectorBucketPolicyError, GetVectorBucketPolicyInput, GetVectorBucketPolicyOutput,
        },
        get_vectors::{GetVectorsError, GetVectorsInput, GetVectorsOutput},
        list_indexes::{ListIndexesError, ListIndexesInput, ListIndexesOutput},
        list_vector_buckets::{
            ListVectorBucketsError, ListVectorBucketsInput, ListVectorBucketsOutput,
        },
        list_vectors::{ListVectorsError, ListVectorsInput, ListVectorsOutput},
        put_vector_bucket_policy::{
            PutVectorBucketPolicyError, PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput,
        },
        put_vectors::{PutVectorsError, PutVectorsInput, PutVectorsOutput},
        query_vectors::{QueryVectorsError, QueryVectorsInput, QueryVectorsOutput},
    },
};

use crate::S3Vectors;

/// A middleware wrapper that delegates all calls to an inner `S3Vectors` implementation.
///
/// This struct provides a base implementation that can be extended to add cross-cutting
/// concerns. Override specific methods to add custom behavior while delegating others
/// to the inner client.
pub struct S3VectorsMiddleware<T: S3Vectors + Send + Sync> {
    inner: Arc<T>,
}

impl<T: S3Vectors + Send + Sync> S3VectorsMiddleware<T> {
    /// Creates a new middleware wrapping the given inner client.
    #[must_use]
    pub fn new(inner: Arc<T>) -> Self {
        Self { inner }
    }

    /// Returns a reference to the inner client.
    #[must_use]
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }
}

#[async_trait]
impl<T: S3Vectors + Send + Sync + 'static> S3Vectors for S3VectorsMiddleware<T> {
    async fn create_index(
        &self,
        input: &CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError, HttpResponse>> {
        self.inner.create_index(input).await
    }

    async fn create_vector_bucket(
        &self,
        input: &CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError, HttpResponse>> {
        self.inner.create_vector_bucket(input).await
    }

    async fn delete_index(
        &self,
        input: &DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError, HttpResponse>> {
        self.inner.delete_index(input).await
    }

    async fn delete_vector_bucket(
        &self,
        input: &DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError, HttpResponse>> {
        self.inner.delete_vector_bucket(input).await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: &DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError, HttpResponse>>
    {
        self.inner.delete_vector_bucket_policy(input).await
    }

    async fn delete_vectors(
        &self,
        input: &DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError, HttpResponse>> {
        self.inner.delete_vectors(input).await
    }

    async fn get_index(
        &self,
        input: &GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError, HttpResponse>> {
        self.inner.get_index(input).await
    }

    async fn get_vector_bucket(
        &self,
        input: &GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError, HttpResponse>> {
        self.inner.get_vector_bucket(input).await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: &GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError, HttpResponse>>
    {
        self.inner.get_vector_bucket_policy(input).await
    }

    async fn get_vectors(
        &self,
        input: &GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError, HttpResponse>> {
        self.inner.get_vectors(input).await
    }

    async fn list_indexes(
        &self,
        input: &ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError, HttpResponse>> {
        self.inner.list_indexes(input).await
    }

    async fn list_vector_buckets(
        &self,
        input: &ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError, HttpResponse>> {
        self.inner.list_vector_buckets(input).await
    }

    async fn list_vectors(
        &self,
        input: &ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError, HttpResponse>> {
        self.inner.list_vectors(input).await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: &PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError, HttpResponse>>
    {
        self.inner.put_vector_bucket_policy(input).await
    }

    async fn put_vectors(
        &self,
        input: &PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError, HttpResponse>> {
        self.inner.put_vectors(input).await
    }

    async fn query_vectors(
        &self,
        input: &QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError, HttpResponse>> {
        self.inner.query_vectors(input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockClient;

    #[tokio::test]
    async fn test_middleware_delegates_to_inner() {
        let mock = Arc::new(MockClient::new());
        let middleware = S3VectorsMiddleware::new(Arc::clone(&mock));

        let input = CreateIndexInput::builder()
            .vector_bucket_name("test-bucket")
            .index_name("test-index")
            .dimension(384)
            .build()
            .expect("valid input");

        let result = middleware.create_index(&input).await;
        assert!(result.is_ok());
        assert_eq!(mock.get_create_index_call_count(), 1);
    }
}
