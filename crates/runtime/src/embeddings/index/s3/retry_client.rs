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

//! Retry middleware for S3 Vectors API client.
//!
//! This module provides a retry middleware that wraps an `S3Vectors` implementation
//! and adds retry logic with configurable backoff strategy and parallelism control.

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_credential_types::provider::error::CredentialsError;
use s3_vectors::{
    CreateIndexError, CreateIndexInput, CreateIndexOutput, CreateVectorBucketError,
    CreateVectorBucketInput, CreateVectorBucketOutput, DeleteIndexError, DeleteIndexInput,
    DeleteIndexOutput, DeleteVectorBucketError, DeleteVectorBucketInput, DeleteVectorBucketOutput,
    DeleteVectorBucketPolicyError, DeleteVectorBucketPolicyInput, DeleteVectorBucketPolicyOutput,
    DeleteVectorsError, DeleteVectorsInput, DeleteVectorsOutput, GetIndexError, GetIndexInput,
    GetIndexOutput, GetVectorBucketError, GetVectorBucketInput, GetVectorBucketOutput,
    GetVectorBucketPolicyError, GetVectorBucketPolicyInput, GetVectorBucketPolicyOutput,
    GetVectorsError, GetVectorsInput, GetVectorsOutput, HttpResponse, ListIndexesError,
    ListIndexesInput, ListIndexesOutput, ListVectorBucketsError, ListVectorBucketsInput,
    ListVectorBucketsOutput, ListVectorsError, ListVectorsInput, ListVectorsOutput,
    PutVectorBucketPolicyError, PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput,
    PutVectorsError, PutVectorsInput, PutVectorsOutput, QueryVectorsError, QueryVectorsInput,
    QueryVectorsOutput, S3Vectors, SdkError,
};
use tokio::sync::Semaphore;
use util::fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder};
use util::{RetryError, retry};

/// Classifies an SDK error as transient or permanent for retry purposes.
fn classify_error<E>(
    error: SdkError<E, HttpResponse>,
    is_transient_service_error: impl FnOnce(&E) -> bool,
) -> RetryError<SdkError<E>> {
    match &error {
        SdkError::TimeoutError(_) => RetryError::transient(error),
        SdkError::ServiceError(service_error) => {
            if is_transient_service_error(service_error.err()) {
                RetryError::transient(error)
            } else {
                RetryError::permanent(error)
            }
        }
        SdkError::DispatchFailure(d) => {
            let credentials_not_loaded = d
                .as_connector_error()
                .and_then(|e| e.source())
                .and_then(|s| s.downcast_ref::<CredentialsError>())
                .is_some_and(|ce| matches!(ce, CredentialsError::CredentialsNotLoaded(_)));

            if credentials_not_loaded {
                RetryError::permanent(error)
            } else {
                RetryError::transient(error)
            }
        }
        _ => RetryError::permanent(error),
    }
}

/// Builder for `S3VectorsRetryMiddleware`.
pub struct S3VectorsRetryMiddlewareBuilder<T: S3Vectors + Send + Sync + ?Sized> {
    inner: Arc<T>,
    retry_strategy: FibonacciBackoff,
    max_parallelism: usize,
    operation_timeout: Duration,
}

impl<T: S3Vectors + Send + Sync + ?Sized> S3VectorsRetryMiddlewareBuilder<T> {
    /// Creates a new builder with the given inner client.
    #[must_use]
    pub fn new(inner: Arc<T>) -> Self {
        Self {
            inner,
            retry_strategy: FibonacciBackoffBuilder::new().max_retries(Some(10)).build(),
            max_parallelism: 10,
            operation_timeout: Duration::from_secs(300),
        }
    }

    /// Sets the retry strategy.
    #[must_use]
    #[expect(unused)]
    pub fn retry_strategy(mut self, retry_strategy: FibonacciBackoff) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    /// Sets the maximum parallelism (concurrent operations).
    #[must_use]
    #[expect(unused)]
    pub fn max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    /// Sets the operation timeout.
    #[must_use]
    #[expect(unused)]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Builds the retry middleware.
    #[must_use]
    pub fn build(self) -> S3VectorsRetryMiddleware<T> {
        S3VectorsRetryMiddleware {
            inner: self.inner,
            retry_strategy: self.retry_strategy,
            semaphore: Semaphore::new(self.max_parallelism),
            operation_timeout: self.operation_timeout,
        }
    }
}

/// Retry middleware for S3 Vectors API.
///
/// Wraps an `S3Vectors` implementation and adds:
/// - Automatic retry with configurable backoff strategy
/// - Parallelism control via semaphore
/// - Operation timeout
pub struct S3VectorsRetryMiddleware<T: S3Vectors + Send + Sync + ?Sized> {
    inner: Arc<T>,
    retry_strategy: FibonacciBackoff,
    semaphore: Semaphore,
    operation_timeout: Duration,
}

impl<T: S3Vectors + Send + Sync + ?Sized> S3VectorsRetryMiddleware<T> {
    /// Returns a reference to the inner client.
    #[must_use]
    #[expect(unused)]
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }

    /// Executes an operation with retry, timeout, and parallelism control.
    async fn execute_with_retry<O, E, F, Fut>(
        &self,
        operation: F,
        is_transient: impl Fn(&E) -> bool + Clone,
    ) -> Result<O, SdkError<E>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<O, SdkError<E>>>,
        E: std::fmt::Debug,
    {
        tokio::time::timeout(
            self.operation_timeout,
            retry(self.retry_strategy.clone(), || async {
                let _permit = self.semaphore.acquire().await;
                match operation().await {
                    Ok(result) => Ok(result),
                    Err(e) => Err(classify_error(e, |err| is_transient(err))),
                }
            }),
        )
        .await
        .map_err(|_| {
            SdkError::construction_failure(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Operation timed out",
            ))
        })?
    }
}

/// Trait for checking if an error is a transient service error.
trait IsTransientError {
    fn is_transient(&self) -> bool;
}

macro_rules! impl_is_transient {
    ($error_type:ty, $($variant:ident),+ $(,)?) => {
        impl IsTransientError for $error_type {
            fn is_transient(&self) -> bool {
                matches!(
                    self,
                    $(Self::$variant(_))|+
                ) || self.meta().code() == Some("RequestTimeoutException")
            }
        }
    };
}

impl_is_transient!(
    CreateIndexError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    CreateVectorBucketError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    DeleteIndexError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    DeleteVectorBucketError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    DeleteVectorBucketPolicyError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    DeleteVectorsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    GetIndexError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    GetVectorBucketError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    GetVectorBucketPolicyError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    GetVectorsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    ListIndexesError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    ListVectorBucketsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    ListVectorsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    PutVectorBucketPolicyError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    PutVectorsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);
impl_is_transient!(
    QueryVectorsError,
    ServiceUnavailableException,
    TooManyRequestsException,
    InternalServerException
);

#[async_trait]
impl<T: S3Vectors + Send + Sync + 'static + ?Sized> S3Vectors for S3VectorsRetryMiddleware<T> {
    async fn create_index(
        &self,
        input: &CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        self.execute_with_retry(
            || self.inner.create_index(input),
            CreateIndexError::is_transient,
        )
        .await
    }

    async fn create_vector_bucket(
        &self,
        input: &CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        self.execute_with_retry(
            || self.inner.create_vector_bucket(input),
            CreateVectorBucketError::is_transient,
        )
        .await
    }

    async fn delete_index(
        &self,
        input: &DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        self.execute_with_retry(
            || self.inner.delete_index(input),
            DeleteIndexError::is_transient,
        )
        .await
    }

    async fn delete_vector_bucket(
        &self,
        input: &DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        self.execute_with_retry(
            || self.inner.delete_vector_bucket(input),
            DeleteVectorBucketError::is_transient,
        )
        .await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: &DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        self.execute_with_retry(
            || self.inner.delete_vector_bucket_policy(input),
            DeleteVectorBucketPolicyError::is_transient,
        )
        .await
    }

    async fn delete_vectors(
        &self,
        input: &DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        self.execute_with_retry(
            || self.inner.delete_vectors(input),
            DeleteVectorsError::is_transient,
        )
        .await
    }

    async fn get_index(
        &self,
        input: &GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        self.execute_with_retry(|| self.inner.get_index(input), GetIndexError::is_transient)
            .await
    }

    async fn get_vector_bucket(
        &self,
        input: &GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        self.execute_with_retry(
            || self.inner.get_vector_bucket(input),
            GetVectorBucketError::is_transient,
        )
        .await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: &GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        self.execute_with_retry(
            || self.inner.get_vector_bucket_policy(input),
            GetVectorBucketPolicyError::is_transient,
        )
        .await
    }

    async fn get_vectors(
        &self,
        input: &GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        self.execute_with_retry(
            || self.inner.get_vectors(input),
            GetVectorsError::is_transient,
        )
        .await
    }

    async fn list_indexes(
        &self,
        input: &ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        self.execute_with_retry(
            || self.inner.list_indexes(input),
            ListIndexesError::is_transient,
        )
        .await
    }

    async fn list_vector_buckets(
        &self,
        input: &ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        self.execute_with_retry(
            || self.inner.list_vector_buckets(input),
            ListVectorBucketsError::is_transient,
        )
        .await
    }

    async fn list_vectors(
        &self,
        input: &ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        self.execute_with_retry(
            || self.inner.list_vectors(input),
            ListVectorsError::is_transient,
        )
        .await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: &PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        self.execute_with_retry(
            || self.inner.put_vector_bucket_policy(input),
            PutVectorBucketPolicyError::is_transient,
        )
        .await
    }

    async fn put_vectors(
        &self,
        input: &PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        self.execute_with_retry(
            || self.inner.put_vectors(input),
            PutVectorsError::is_transient,
        )
        .await
    }

    async fn query_vectors(
        &self,
        input: &QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        self.execute_with_retry(
            || self.inner.query_vectors(input),
            QueryVectorsError::is_transient,
        )
        .await
    }
}
