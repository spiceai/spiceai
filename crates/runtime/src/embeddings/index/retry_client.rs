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
    Client, CreateIndexError, CreateIndexInput, CreateIndexOutput, CreateVectorBucketError,
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
    client: Client,
    retry_strategy: FibonacciBackoff,
    max_parallelism: usize,
}

impl S3VectorRetryClientBuilder {
    #[must_use]
    pub fn new(client: Client) -> Self {
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
    client: Client,
    retry_strategy: FibonacciBackoff,
    semaphore: Semaphore,
}

#[async_trait]
impl S3Vectors for S3VectorRetryClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self
                .client
                .create_index()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_data_type(input.data_type.clone())
                .set_dimension(input.dimension)
                .set_distance_metric(input.distance_metric.clone())
                .set_metadata_configuration(input.metadata_configuration.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        CreateIndexError::ServiceUnavailableException(_)
                        | CreateIndexError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        CreateIndexError::AccessDeniedException(_)
                        | CreateIndexError::ConflictException(_)
                        | CreateIndexError::InternalServerException(_)
                        | CreateIndexError::NotFoundException(_)
                        | CreateIndexError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }

    async fn create_vector_bucket(
        &self,
        input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError>> {
        retry(self.retry_strategy.clone(), || async {
            let _permit = self.semaphore.acquire().await;
            match self
                .client
                .create_vector_bucket()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_encryption_configuration(input.encryption_configuration.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        CreateVectorBucketError::ServiceUnavailableException(_)
                        | CreateVectorBucketError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        CreateVectorBucketError::AccessDeniedException(_)
                        | CreateVectorBucketError::ConflictException(_)
                        | CreateVectorBucketError::InternalServerException(_)
                        | CreateVectorBucketError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .delete_index()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteIndexError::ServiceUnavailableException(_)
                        | DeleteIndexError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        DeleteIndexError::AccessDeniedException(_)
                        | DeleteIndexError::InternalServerException(_)
                        | DeleteIndexError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .delete_vector_bucket()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorBucketError::ServiceUnavailableException(_)
                        | DeleteVectorBucketError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorBucketError::AccessDeniedException(_)
                        | DeleteVectorBucketError::ConflictException(_)
                        | DeleteVectorBucketError::InternalServerException(_)
                        | DeleteVectorBucketError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .delete_vector_bucket_policy()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorBucketPolicyError::ServiceUnavailableException(_)
                        | DeleteVectorBucketPolicyError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorBucketPolicyError::AccessDeniedException(_)
                        | DeleteVectorBucketPolicyError::InternalServerException(_)
                        | DeleteVectorBucketPolicyError::NotFoundException(_)
                        | DeleteVectorBucketPolicyError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .delete_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_keys(input.keys.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        DeleteVectorsError::ServiceUnavailableException(_)
                        | DeleteVectorsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        DeleteVectorsError::AccessDeniedException(_)
                        | DeleteVectorsError::InternalServerException(_)
                        | DeleteVectorsError::NotFoundException(_)
                        | DeleteVectorsError::KmsDisabledException(_)
                        | DeleteVectorsError::KmsInvalidKeyUsageException(_)
                        | DeleteVectorsError::KmsInvalidStateException(_)
                        | DeleteVectorsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .get_index()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetIndexError::ServiceUnavailableException(_)
                        | GetIndexError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        GetIndexError::AccessDeniedException(_)
                        | GetIndexError::InternalServerException(_)
                        | GetIndexError::NotFoundException(_)
                        | GetIndexError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .get_vector_bucket()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorBucketError::ServiceUnavailableException(_)
                        | GetVectorBucketError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorBucketError::AccessDeniedException(_)
                        | GetVectorBucketError::InternalServerException(_)
                        | GetVectorBucketError::NotFoundException(_)
                        | GetVectorBucketError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .get_vector_bucket_policy()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorBucketPolicyError::ServiceUnavailableException(_)
                        | GetVectorBucketPolicyError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorBucketPolicyError::AccessDeniedException(_)
                        | GetVectorBucketPolicyError::InternalServerException(_)
                        | GetVectorBucketPolicyError::NotFoundException(_)
                        | GetVectorBucketPolicyError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .get_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_keys(input.keys.clone())
                .set_return_data(Some(true))
                .set_return_metadata(Some(true))
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        GetVectorsError::ServiceUnavailableException(_)
                        | GetVectorsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        GetVectorsError::AccessDeniedException(_)
                        | GetVectorsError::InternalServerException(_)
                        | GetVectorsError::NotFoundException(_)
                        | GetVectorsError::KmsDisabledException(_)
                        | GetVectorsError::KmsInvalidKeyUsageException(_)
                        | GetVectorsError::KmsInvalidStateException(_)
                        | GetVectorsError::KmsNotFoundException(_)
                        | GetVectorsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .list_indexes()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .set_max_results(input.max_results)
                .set_next_token(input.next_token.clone())
                .set_max_results(input.max_results)
                .set_prefix(input.prefix.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListIndexesError::ServiceUnavailableException(_)
                        | ListIndexesError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        ListIndexesError::AccessDeniedException(_)
                        | ListIndexesError::InternalServerException(_)
                        | ListIndexesError::NotFoundException(_)
                        | ListIndexesError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .list_vector_buckets()
                .set_max_results(input.max_results)
                .set_next_token(input.next_token.clone())
                .set_prefix(input.prefix.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListVectorBucketsError::ServiceUnavailableException(_)
                        | ListVectorBucketsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        ListVectorBucketsError::AccessDeniedException(_)
                        | ListVectorBucketsError::InternalServerException(_)
                        | ListVectorBucketsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .list_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_max_results(input.max_results)
                .set_next_token(input.next_token.clone())
                .set_segment_count(input.segment_count)
                .set_segment_index(input.segment_index)
                .set_return_data(input.return_data)
                .set_return_metadata(input.return_metadata)
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        ListVectorsError::ServiceUnavailableException(_)
                        | ListVectorsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        ListVectorsError::AccessDeniedException(_)
                        | ListVectorsError::InternalServerException(_)
                        | ListVectorsError::NotFoundException(_)
                        | ListVectorsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .put_vector_bucket_policy()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_vector_bucket_arn(input.vector_bucket_arn.clone())
                .set_policy(input.policy.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        PutVectorBucketPolicyError::ServiceUnavailableException(_)
                        | PutVectorBucketPolicyError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        PutVectorBucketPolicyError::AccessDeniedException(_)
                        | PutVectorBucketPolicyError::InternalServerException(_)
                        | PutVectorBucketPolicyError::NotFoundException(_)
                        | PutVectorBucketPolicyError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .put_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_vectors(input.vectors.clone())
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        PutVectorsError::ServiceUnavailableException(_)
                        | PutVectorsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        PutVectorsError::AccessDeniedException(_)
                        | PutVectorsError::InternalServerException(_)
                        | PutVectorsError::NotFoundException(_)
                        | PutVectorsError::KmsDisabledException(_)
                        | PutVectorsError::KmsInvalidKeyUsageException(_)
                        | PutVectorsError::KmsInvalidStateException(_)
                        | PutVectorsError::KmsNotFoundException(_)
                        | PutVectorsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
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
            match self
                .client
                .query_vectors()
                .set_vector_bucket_name(input.vector_bucket_name.clone())
                .set_index_name(input.index_name.clone())
                .set_index_arn(input.index_arn.clone())
                .set_query_vector(input.query_vector.clone())
                .set_top_k(input.top_k)
                .set_filter(input.filter.clone())
                .set_return_metadata(input.return_metadata)
                .set_return_distance(input.return_distance)
                .send()
                .await
            {
                Ok(result) => Ok(result),
                Err(e) => match &e {
                    SdkError::ServiceError(service_error) => match service_error.err() {
                        QueryVectorsError::ServiceUnavailableException(_)
                        | QueryVectorsError::TooManyRequestsException(_) => {
                            Err(RetryError::transient(e))
                        }
                        QueryVectorsError::AccessDeniedException(_)
                        | QueryVectorsError::InternalServerException(_)
                        | QueryVectorsError::NotFoundException(_)
                        | QueryVectorsError::KmsDisabledException(_)
                        | QueryVectorsError::KmsInvalidKeyUsageException(_)
                        | QueryVectorsError::KmsInvalidStateException(_)
                        | QueryVectorsError::KmsNotFoundException(_)
                        | QueryVectorsError::ServiceQuotaExceededException(_)
                        | _ => Err(RetryError::permanent(e)),
                    },
                    SdkError::DispatchFailure(_) => Err(RetryError::transient(e)),
                    _ => Err(RetryError::permanent(e)),
                },
            }
        })
        .await
    }
}
