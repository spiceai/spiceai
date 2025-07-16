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
    PutVectorsOutput, QueryVectorsError, QueryVectorsInput, QueryVectorsOutput, RusotoError,
    S3Vectors, S3VectorsClient,
};
use tokio::sync::Semaphore;
use util::fibonacci_backoff::{FibonacciBackoff, FibonacciBackoffBuilder};
use util::{RetryError, retry};

pub struct S3VectorRetryClientBuilder {
    client: S3VectorsClient,
    retry_strategy: FibonacciBackoff,
    max_parallelism: usize,
}

impl S3VectorRetryClientBuilder {
    #[must_use]
    pub fn new(client: S3VectorsClient) -> Self {
        Self {
            client,
            retry_strategy: FibonacciBackoffBuilder::new().max_retries(Some(10)).build(),
            max_parallelism: 10,
        }
    }

    #[must_use]
    #[allow(unused)]
    pub fn retry_strategy(mut self, retry_strategy: FibonacciBackoff) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    #[must_use]
    #[allow(unused)]
    pub fn max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    #[must_use]
    pub fn build(self) -> S3VectorRetryClient {
        S3VectorRetryClient {
            client: self.client,
            retry_strategy: self.retry_strategy,
            semaphore: Semaphore::new(self.max_parallelism),
        }
    }
}

pub struct S3VectorRetryClient {
    client: S3VectorsClient,
    retry_strategy: FibonacciBackoff,
    semaphore: Semaphore,
}

#[async_trait]
impl S3Vectors for S3VectorRetryClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, RusotoError<CreateIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.create_index(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        CreateIndexError::AccessDenied(_)
                        | CreateIndexError::Conflict(_)
                        | CreateIndexError::InternalServer(_)
                        | CreateIndexError::NotFound(_)
                        | CreateIndexError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        CreateIndexError::ServiceUnavailable(_)
                        | CreateIndexError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, RusotoError<CreateVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.create_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        CreateVectorBucketError::AccessDenied(_)
                        | CreateVectorBucketError::Conflict(_)
                        | CreateVectorBucketError::InternalServer(_)
                        | CreateVectorBucketError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        CreateVectorBucketError::ServiceUnavailable(_)
                        | CreateVectorBucketError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_index(
        &self,
        input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, RusotoError<DeleteIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_index(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        DeleteIndexError::AccessDenied(_)
                        | DeleteIndexError::InternalServer(_)
                        | DeleteIndexError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        DeleteIndexError::ServiceUnavailable(_)
                        | DeleteIndexError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vector_bucket(
        &self,
        input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, RusotoError<DeleteVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        DeleteVectorBucketError::AccessDenied(_)
                        | DeleteVectorBucketError::Conflict(_)
                        | DeleteVectorBucketError::InternalServer(_)
                        | DeleteVectorBucketError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        DeleteVectorBucketError::ServiceUnavailable(_)
                        | DeleteVectorBucketError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vector_bucket_policy(
        &self,
        input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, RusotoError<DeleteVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        DeleteVectorBucketPolicyError::AccessDenied(_)
                        | DeleteVectorBucketPolicyError::InternalServer(_)
                        | DeleteVectorBucketPolicyError::NotFound(_)
                        | DeleteVectorBucketPolicyError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        DeleteVectorBucketPolicyError::ServiceUnavailable(_)
                        | DeleteVectorBucketPolicyError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn delete_vectors(
        &self,
        input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, RusotoError<DeleteVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.delete_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        DeleteVectorsError::AccessDenied(_)
                        | DeleteVectorsError::InternalServer(_)
                        | DeleteVectorsError::NotFound(_)
                        | DeleteVectorsError::KmsDisabled(_)
                        | DeleteVectorsError::KmsInvalidKeyUsage(_)
                        | DeleteVectorsError::KmsInvalidState(_)
                        | DeleteVectorsError::KmsNotFound(_)
                        | DeleteVectorsError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        DeleteVectorsError::ServiceUnavailable(_)
                        | DeleteVectorsError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, RusotoError<GetIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_index(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        GetIndexError::AccessDenied(_)
                        | GetIndexError::InternalServer(_)
                        | GetIndexError::NotFound(_)
                        | GetIndexError::ServiceQuotaExceeded(_) => Err(RetryError::permanent(e)),
                        GetIndexError::ServiceUnavailable(_)
                        | GetIndexError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vector_bucket(
        &self,
        input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, RusotoError<GetVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vector_bucket(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        GetVectorBucketError::AccessDenied(_)
                        | GetVectorBucketError::InternalServer(_)
                        | GetVectorBucketError::NotFound(_)
                        | GetVectorBucketError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        GetVectorBucketError::ServiceUnavailable(_)
                        | GetVectorBucketError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vector_bucket_policy(
        &self,
        input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, RusotoError<GetVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        GetVectorBucketPolicyError::AccessDenied(_)
                        | GetVectorBucketPolicyError::InternalServer(_)
                        | GetVectorBucketPolicyError::NotFound(_)
                        | GetVectorBucketPolicyError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        GetVectorBucketPolicyError::ServiceUnavailable(_)
                        | GetVectorBucketPolicyError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn get_vectors(
        &self,
        input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, RusotoError<GetVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.get_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        GetVectorsError::AccessDenied(_)
                        | GetVectorsError::InternalServer(_)
                        | GetVectorsError::NotFound(_)
                        | GetVectorsError::KmsDisabled(_)
                        | GetVectorsError::KmsInvalidKeyUsage(_)
                        | GetVectorsError::KmsInvalidState(_)
                        | GetVectorsError::KmsNotFound(_)
                        | GetVectorsError::ServiceQuotaExceeded(_) => Err(RetryError::permanent(e)),
                        GetVectorsError::ServiceUnavailable(_)
                        | GetVectorsError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, RusotoError<ListIndexesError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_indexes(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        ListIndexesError::AccessDenied(_)
                        | ListIndexesError::InternalServer(_)
                        | ListIndexesError::NotFound(_)
                        | ListIndexesError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        ListIndexesError::ServiceUnavailable(_)
                        | ListIndexesError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_vector_buckets(
        &self,
        input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, RusotoError<ListVectorBucketsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_vector_buckets(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        ListVectorBucketsError::AccessDenied(_)
                        | ListVectorBucketsError::InternalServer(_)
                        | ListVectorBucketsError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        ListVectorBucketsError::ServiceUnavailable(_)
                        | ListVectorBucketsError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, RusotoError<ListVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.list_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        ListVectorsError::AccessDenied(_)
                        | ListVectorsError::InternalServer(_)
                        | ListVectorsError::NotFound(_)
                        | ListVectorsError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        ListVectorsError::ServiceUnavailable(_)
                        | ListVectorsError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn put_vector_bucket_policy(
        &self,
        input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, RusotoError<PutVectorBucketPolicyError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.put_vector_bucket_policy(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        PutVectorBucketPolicyError::AccessDenied(_)
                        | PutVectorBucketPolicyError::InternalServer(_)
                        | PutVectorBucketPolicyError::NotFound(_)
                        | PutVectorBucketPolicyError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        PutVectorBucketPolicyError::ServiceUnavailable(_)
                        | PutVectorBucketPolicyError::TooManyRequests(_) => {
                            Err(RetryError::transient(e))
                        }
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, RusotoError<PutVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.put_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        PutVectorsError::AccessDenied(_)
                        | PutVectorsError::InternalServer(_)
                        | PutVectorsError::NotFound(_)
                        | PutVectorsError::KmsDisabled(_)
                        | PutVectorsError::KmsInvalidKeyUsage(_)
                        | PutVectorsError::KmsInvalidState(_)
                        | PutVectorsError::KmsNotFound(_)
                        | PutVectorsError::ServiceQuotaExceeded(_) => Err(RetryError::permanent(e)),
                        PutVectorsError::ServiceUnavailable(_)
                        | PutVectorsError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn query_vectors(
        &self,
        input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, RusotoError<QueryVectorsError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self.client.query_vectors(input.clone()).await {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    RusotoError::Service(service_error) => match service_error {
                        QueryVectorsError::AccessDenied(_)
                        | QueryVectorsError::InternalServer(_)
                        | QueryVectorsError::NotFound(_)
                        | QueryVectorsError::KmsDisabled(_)
                        | QueryVectorsError::KmsInvalidKeyUsage(_)
                        | QueryVectorsError::KmsInvalidState(_)
                        | QueryVectorsError::KmsNotFound(_)
                        | QueryVectorsError::ServiceQuotaExceeded(_) => {
                            Err(RetryError::permanent(e))
                        }
                        QueryVectorsError::ServiceUnavailable(_)
                        | QueryVectorsError::TooManyRequests(_) => Err(RetryError::transient(e)),
                    },
                    RusotoError::HttpDispatch(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }
}
