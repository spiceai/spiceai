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
    types::ListOutputVector,
};
pub use aws_smithy_types::{DateTime, Document, Number, error::operation::BuildError};

use crate::S3Vectors;

#[derive(Default, Debug)]
pub struct MockData {
    pub indexes: HashMap<String, Vec<aws_sdk_s3vectors::types::IndexSummary>>,
    pub vectors: HashMap<String, Vec<ListOutputVector>>,
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
}

#[async_trait]
impl S3Vectors for MockClient {
    async fn create_index(
        &self,
        _input: CreateIndexInput,
    ) -> Result<CreateIndexOutput, SdkError<CreateIndexError, HttpResponse>> {
        unimplemented!()
    }

    async fn create_vector_bucket(
        &self,
        _input: CreateVectorBucketInput,
    ) -> Result<CreateVectorBucketOutput, SdkError<CreateVectorBucketError, HttpResponse>> {
        unimplemented!()
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
        _input: GetIndexInput,
    ) -> Result<GetIndexOutput, SdkError<GetIndexError, HttpResponse>> {
        unimplemented!()
    }

    async fn get_vector_bucket(
        &self,
        _input: GetVectorBucketInput,
    ) -> Result<GetVectorBucketOutput, SdkError<GetVectorBucketError, HttpResponse>> {
        unimplemented!()
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
        let bucket_indexes = data.indexes.get(bucket_name).cloned().unwrap_or_default();

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
        _input: PutVectorsInput,
    ) -> Result<PutVectorsOutput, SdkError<PutVectorsError, HttpResponse>> {
        unimplemented!()
    }

    async fn query_vectors(
        &self,
        _input: QueryVectorsInput,
    ) -> Result<QueryVectorsOutput, SdkError<QueryVectorsError, HttpResponse>> {
        unimplemented!()
    }
}
