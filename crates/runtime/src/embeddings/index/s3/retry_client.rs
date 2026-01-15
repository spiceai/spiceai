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

use std::error::Error;
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
    GetVectorsError, GetVectorsInput, GetVectorsOutput, ListIndexesError, ListIndexesInput,
    ListIndexesOutput, ListVectorBucketsError, ListVectorBucketsInput, ListVectorBucketsOutput,
    ListVectorsError, ListVectorsInput, ListVectorsOutput, PutVectorBucketPolicyError,
    PutVectorBucketPolicyInput, PutVectorBucketPolicyOutput, PutVectorsError, PutVectorsInput,
    PutVectorsOutput, QueryVectorsError, QueryVectorsInput, QueryVectorsOutput, S3Vectors,
    SdkError,
};
use tokio::sync::Semaphore;
use util::fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder};
use util::{RetryError, retry};

pub struct S3VectorRetryClientBuilder {
    client: Arc<dyn S3Vectors + Send + Sync>,
    retry_strategy: FibonacciBackoff,
    max_parallelism: usize,
    operation_timeout: Duration,
}

impl S3VectorRetryClientBuilder {
    #[must_use]
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>) -> Self {
        Self {
            client,
            retry_strategy: FibonacciBackoffBuilder::new().max_retries(Some(10)).build(),
            max_parallelism: 10,
            operation_timeout: Duration::from_secs(300), // 5 minute default timeout
        }
    }

    #[must_use]
    #[expect(unused)]
    pub fn retry_strategy(mut self, retry_strategy: FibonacciBackoff) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    #[must_use]
    #[expect(unused)]
    pub fn max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    #[must_use]
    #[expect(unused)]
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    #[must_use]
    pub fn build(self) -> S3VectorRetryClient {
        S3VectorRetryClient {
            client: self.client,
            retry_strategy: self.retry_strategy,
            semaphore: Semaphore::new(self.max_parallelism),
            operation_timeout: self.operation_timeout,
        }
    }
}

pub struct S3VectorRetryClient {
    client: Arc<dyn S3Vectors + Send + Sync>,
    retry_strategy: FibonacciBackoff,
    semaphore: Semaphore,
    operation_timeout: Duration,
}

#[async_trait]
impl S3Vectors for S3VectorRetryClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        tokio::time::timeout(
            self.operation_timeout,
            retry(self.retry_strategy.clone(), || async {
                let _permit = self.semaphore.acquire().await;
                match self.client.create_index(input.clone()).await {
                    Ok(result) => Ok(result),
                    Err(e) => match &e {
                        SdkError::ServiceError(service_error) => match service_error.err() {
                            CreateIndexError::ServiceUnavailableException(_)
                            | CreateIndexError::TooManyRequestsException(_)
                            | CreateIndexError::InternalServerException(_) => {
                                Err(RetryError::transient(e))
                            }
                            err if err.meta().code() == Some("RequestTimeoutException") => {
                                Err(RetryError::transient(e))
                            }
                            CreateIndexError::AccessDeniedException(_)
                            | CreateIndexError::ConflictException(_)
                            | CreateIndexError::NotFoundException(_)
                            | _ => Err(RetryError::permanent(e)),
                        },
                        SdkError::DispatchFailure(d) => {
                            let credentials_not_loaded = d
                                .as_connector_error()
                                .and_then(|e| e.source())
                                .and_then(|s| s.downcast_ref::<CredentialsError>())
                                .is_some_and(|ce| {
                                    matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                                });

                            if credentials_not_loaded {
                                Err(RetryError::permanent(e))
                            } else {
                                Err(RetryError::transient(e))
                            }
                        }
                        _ => Err(RetryError::permanent(e)),
                    },
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

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.create_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        CreateVectorBucketError::ServiceUnavailableException(_)
                        | CreateVectorBucketError::TooManyRequestsException(_)
                        | CreateVectorBucketError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        CreateVectorBucketError::AccessDeniedException(_)
                        | CreateVectorBucketError::ConflictException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_index(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::TimeoutError(_) => Err(RetryError::transient(e)),
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteIndexError::ServiceUnavailableException(_)
                        | DeleteIndexError::TooManyRequestsException(_)
                        | DeleteIndexError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        DeleteIndexError::AccessDeniedException(_) | _ => {
                            Err(RetryError::permanent(e))
                        }
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorBucketError::ServiceUnavailableException(_)
                        | DeleteVectorBucketError::TooManyRequestsException(_)
                        | DeleteVectorBucketError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorBucketError::AccessDeniedException(_)
                        | DeleteVectorBucketError::ConflictException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorBucketPolicyError::ServiceUnavailableException(_)
                        | DeleteVectorBucketPolicyError::TooManyRequestsException(_)
                        | DeleteVectorBucketPolicyError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorBucketPolicyError::AccessDeniedException(_)
                        | DeleteVectorBucketPolicyError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorsError::ServiceUnavailableException(_)
                        | DeleteVectorsError::TooManyRequestsException(_)
                        | DeleteVectorsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorsError::AccessDeniedException(_)
                        | DeleteVectorsError::NotFoundException(_)
                        | DeleteVectorsError::KmsDisabledException(_)
                        | DeleteVectorsError::KmsInvalidKeyUsageException(_)
                        | DeleteVectorsError::KmsInvalidStateException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_index(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetIndexError::ServiceUnavailableException(_)
                        | GetIndexError::TooManyRequestsException(_)
                        | GetIndexError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        GetIndexError::AccessDeniedException(_)
                        | GetIndexError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorBucketError::ServiceUnavailableException(_)
                        | GetVectorBucketError::TooManyRequestsException(_)
                        | GetVectorBucketError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorBucketError::AccessDeniedException(_)
                        | GetVectorBucketError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorBucketPolicyError::ServiceUnavailableException(_)
                        | GetVectorBucketPolicyError::TooManyRequestsException(_)
                        | GetVectorBucketPolicyError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorBucketPolicyError::AccessDeniedException(_)
                        | GetVectorBucketPolicyError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorsError::ServiceUnavailableException(_)
                        | GetVectorsError::TooManyRequestsException(_)
                        | GetVectorsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorsError::AccessDeniedException(_)
                        | GetVectorsError::NotFoundException(_)
                        | GetVectorsError::KmsDisabledException(_)
                        | GetVectorsError::KmsInvalidKeyUsageException(_)
                        | GetVectorsError::KmsInvalidStateException(_)
                        | GetVectorsError::KmsNotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_indexes(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListIndexesError::ServiceUnavailableException(_)
                        | ListIndexesError::TooManyRequestsException(_)
                        | ListIndexesError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        ListIndexesError::AccessDeniedException(_)
                        | ListIndexesError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_vector_buckets(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListVectorBucketsError::ServiceUnavailableException(_)
                        | ListVectorBucketsError::TooManyRequestsException(_)
                        | ListVectorBucketsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        ListVectorBucketsError::AccessDeniedException(_) | _ => {
                            Err(RetryError::permanent(e))
                        }
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListVectorsError::ServiceUnavailableException(_)
                        | ListVectorsError::TooManyRequestsException(_)
                        | ListVectorsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        ListVectorsError::AccessDeniedException(_)
                        | ListVectorsError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.put_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        PutVectorBucketPolicyError::ServiceUnavailableException(_)
                        | PutVectorBucketPolicyError::TooManyRequestsException(_)
                        | PutVectorBucketPolicyError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        PutVectorBucketPolicyError::AccessDeniedException(_)
                        | PutVectorBucketPolicyError::NotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.put_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        PutVectorsError::ServiceUnavailableException(_)
                        | PutVectorsError::TooManyRequestsException(_)
                        | PutVectorsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        PutVectorsError::AccessDeniedException(_)
                        | PutVectorsError::NotFoundException(_)
                        | PutVectorsError::KmsDisabledException(_)
                        | PutVectorsError::KmsInvalidKeyUsageException(_)
                        | PutVectorsError::KmsInvalidStateException(_)
                        | PutVectorsError::KmsNotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.query_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        QueryVectorsError::ServiceUnavailableException(_)
                        | QueryVectorsError::TooManyRequestsException(_)
                        | QueryVectorsError::InternalServerException(_) => {
                            Err(RetryError::transient(e))
                        }
                        err if err.meta().code() == Some("RequestTimeoutException") => {
                            Err(RetryError::transient(e))
                        }
                        QueryVectorsError::AccessDeniedException(_)
                        | QueryVectorsError::NotFoundException(_)
                        | QueryVectorsError::KmsDisabledException(_)
                        | QueryVectorsError::KmsInvalidKeyUsageException(_)
                        | QueryVectorsError::KmsInvalidStateException(_)
                        | QueryVectorsError::KmsNotFoundException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(d) => {
                        let credentials_not_loaded = d
                            .as_connector_error()
                            .and_then(|e| e.source())
                            .and_then(|s| s.downcast_ref::<CredentialsError>())
                            .is_some_and(|ce| {
                                matches!(ce, CredentialsError::CredentialsNotLoaded(_))
                            });

                        if credentials_not_loaded {
                            Err(RetryError::permanent(e))
                        } else {
                            Err(RetryError::transient(e))
                        }
                    }
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }
}
