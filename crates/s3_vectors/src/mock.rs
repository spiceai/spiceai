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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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
    types::{
        DataType, DistanceMetric, Index, IndexSummary, ListOutputVector,
        error::ServiceQuotaExceededException,
    },
};
use aws_smithy_runtime_api::{client::result::ServiceError, http::StatusCode};
use aws_smithy_types::body::SdkBody;
pub use aws_smithy_types::{DateTime, Document, Number, error::operation::BuildError};

use crate::S3Vectors;

#[derive(Default, Debug)]
pub struct MockData {
    pub indexes: HashMap<String, Vec<aws_sdk_s3vectors::types::IndexSummary>>,
    pub vectors: HashMap<String, Vec<ListOutputVector>>,
    pub vector_counts: HashMap<String, usize>, // Track number of vectors per index
    pub quota_limits: HashMap<String, usize>,  // Configurable quota limits per index
}

#[derive(Debug, Clone, Default)]
pub struct MockClient {
    pub data: Arc<Mutex<MockData>>,
}

impl MockClient {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a quota limit for a specific index (number of vectors allowed)
    pub fn set_quota_limit(&self, index_name: &str, limit: usize) {
        let mut data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };
        data.quota_limits.insert(index_name.to_string(), limit);
    }

    /// Get the current vector count for an index
    #[must_use]
    pub fn get_vector_count(&self, index_name: &str) -> usize {
        let data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };
        *data.vector_counts.get(index_name).unwrap_or(&0)
    }
}

#[async_trait]
impl S3Vectors for MockClient {
    async fn create_index(
        &self,
        input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError, HttpResponse>> {
        let bucket_name = input.vector_bucket_name().unwrap_or_default();
        let index_name = input.index_name().unwrap_or_default();

        let mut data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };

        let bucket_indexes = data.indexes.entry(bucket_name.to_string()).or_default();

        let index_summary = IndexSummary::builder()
            .index_name(index_name)
            .vector_bucket_name(bucket_name)
            .index_arn(format!("arn:aws:s3vectors:::{bucket_name}:{index_name}"))
            .creation_time(DateTime::from_secs(0))
            .build()
            .map_err(|_| SdkError::construction_failure("build"))?;

        bucket_indexes.push(index_summary);

        data.vector_counts.insert(index_name.to_string(), 0);

        Ok(CreateIndexOutput::builder().build())
    }

    async fn create_vector_bucket(
        &self,
        _input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError, HttpResponse>> {
        Ok(CreateVectorBucketOutput::builder().build())
    }

    async fn delete_index(
        &self,
        _input: DeleteIndexInput,
    ) -> Result<DeleteIndexOutput, SdkError<DeleteIndexError, HttpResponse>> {
        unimplemented!()
    }

    async fn delete_vector_bucket(
        &self,
        _input: DeleteVectorBucketInput,
    ) -> Result<DeleteVectorBucketOutput, SdkError<DeleteVectorBucketError, HttpResponse>> {
        unimplemented!()
    }

    async fn delete_vector_bucket_policy(
        &self,
        _input: DeleteVectorBucketPolicyInput,
    ) -> Result<DeleteVectorBucketPolicyOutput, SdkError<DeleteVectorBucketPolicyError, HttpResponse>>
    {
        unimplemented!()
    }

    async fn delete_vectors(
        &self,
        _input: DeleteVectorsInput,
    ) -> Result<DeleteVectorsOutput, SdkError<DeleteVectorsError, HttpResponse>> {
        unimplemented!()
    }

    async fn get_index(
        &self,
        input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError, HttpResponse>> {
        let bucket_name = input.vector_bucket_name().unwrap_or_default();
        let index_name = input.index_name().unwrap_or_default();

        let data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };

        let empty = Vec::<IndexSummary>::new();
        let bucket_indexes = data
            .indexes
            .get(bucket_name)
            .map_or(empty.as_slice(), Vec::as_slice);
        let index_summary = bucket_indexes
            .iter()
            .find(|idx| idx.index_name() == index_name);

        match index_summary {
            Some(index) => {
                let index_details = Index::builder()
                    .index_name(index.index_name())
                    .vector_bucket_name(bucket_name)
                    .index_arn(format!(
                        "arn:aws:s3vectors:::{bucket_name}:{}",
                        index.index_name()
                    ))
                    .creation_time(DateTime::from_secs(1))
                    .data_type(DataType::Float32)
                    .dimension(384)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()
                    .map_err(|_| SdkError::construction_failure("build"))?;

                Ok(GetIndexOutput::builder().index(index_details).build())
            }
            None => {
                panic!("Index not found");
            }
        }
    }

    async fn get_vector_bucket(
        &self,
        _input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError, HttpResponse>> {
        Ok(GetVectorBucketOutput::builder().build())
    }

    async fn get_vector_bucket_policy(
        &self,
        _input: GetVectorBucketPolicyInput,
    ) -> Result<GetVectorBucketPolicyOutput, SdkError<GetVectorBucketPolicyError, HttpResponse>>
    {
        unimplemented!()
    }

    async fn get_vectors(
        &self,
        _input: GetVectorsInput,
    ) -> Result<GetVectorsOutput, SdkError<GetVectorsError, HttpResponse>> {
        unimplemented!()
    }

    async fn list_indexes(
        &self,
        input: ListIndexesInput,
    ) -> Result<ListIndexesOutput, SdkError<ListIndexesError, HttpResponse>> {
        let bucket_name = input.vector_bucket_name().unwrap_or_default();
        let data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };
        let mut bucket_indexes = data.indexes.get(bucket_name).cloned().unwrap_or_default();

        // Filter by prefix if provided
        if let Some(prefix) = input.prefix() {
            bucket_indexes.retain(|idx| idx.index_name().starts_with(prefix));
        }

        Ok(ListIndexesOutput::builder()
            .set_indexes(Some(bucket_indexes))
            .build()?)
    }

    async fn list_vector_buckets(
        &self,
        _input: ListVectorBucketsInput,
    ) -> Result<ListVectorBucketsOutput, SdkError<ListVectorBucketsError, HttpResponse>> {
        unimplemented!()
    }

    async fn list_vectors(
        &self,
        input: ListVectorsInput,
    ) -> Result<ListVectorsOutput, SdkError<ListVectorsError, HttpResponse>> {
        let index_name = input.index_name().unwrap_or_default();
        let data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };
        let index_vectors = data.vectors.get(index_name).cloned().unwrap_or_default();

        Ok(ListVectorsOutput::builder()
            .set_vectors(Some(index_vectors))
            .build()?)
    }

    async fn put_vector_bucket_policy(
        &self,
        _input: PutVectorBucketPolicyInput,
    ) -> Result<PutVectorBucketPolicyOutput, SdkError<PutVectorBucketPolicyError, HttpResponse>>
    {
        unimplemented!()
    }

    async fn put_vectors(
        &self,
        input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError, HttpResponse>> {
        let index_name = input.index_name().unwrap_or_default();
        let num_vectors = input.vectors().len();

        let mut data = match self.data.lock() {
            Ok(lock) => lock,
            Err(e) => e.into_inner(),
        };

        let current_count = *data.vector_counts.get(index_name).unwrap_or(&0);
        let quota_limit = *data.quota_limits.get(index_name).unwrap_or(&usize::MAX);

        if current_count + num_vectors > quota_limit {
            let service_error = ServiceQuotaExceededException::builder()
                .message("Vector quota exceeded")
                .build()
                .map_err(|_| SdkError::construction_failure("build"))?;

            return Err(SdkError::ServiceError(
                ServiceError::builder()
                    .source(PutVectorsError::ServiceQuotaExceededException(
                        service_error,
                    ))
                    .raw(HttpResponse::new(
                        StatusCode::try_from(402)
                            .map_err(|_| SdkError::construction_failure("status code"))?,
                        SdkBody::empty(),
                    ))
                    .build(),
            ));
        }

        let new_count = current_count + num_vectors;
        data.vector_counts.insert(index_name.to_string(), new_count);

        Ok(PutVectorsOutput::builder().build())
    }

    async fn query_vectors(
        &self,
        _input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError, HttpResponse>> {
        unimplemented!()
    }
}
