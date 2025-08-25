/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use arrow::error::ArrowError;
use s3_vectors::{
    BuildError, CreateIndexError, CreateVectorBucketError, Document, GetIndexError,
    GetVectorBucketError, PutVectorsError, QueryVectorsError,
};
use s3_vectors_metadata_filter::MetadataFilter;
use snafu::Snafu;

pub mod list_provider;
pub mod query_provider;
mod vector_table;
pub use vector_table::{S3VectorTableResult, S3VectorsTable};
mod metadata_column;
pub use metadata_column::{MetadataColumn, MetadataColumns};

/// The JSON key within an S3 vector record that is the primary key.
pub static S3_VECTOR_PRIMARY_KEY_NAME: &str = "key";

/// The JSON key within an S3 vector record that is the embedding data.
pub static S3_VECTOR_EMBEDDING_NAME: &str = "data";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to s3vector. {source} Report an issue on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InternalError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to write vectors to S3 Vectors. {source}"))]
    S3VectorPutVectorError { source: PutVectorsError },

    #[snafu(display("Failed to query vectors from S3 Vectors. {source}"))]
    S3VectorQueryVectorsError { source: QueryVectorsError },

    #[snafu(display(
        "Failed to query vectors from S3 Vectors due to an unsupported filter: {filter_pre} {filter:?}"
    ))]
    S3VectorQueryVectorsInvalidFilterError {
        filter_pre: MetadataFilter,
        filter: Document,
    },

    #[snafu(display("Failed to create index in S3 Vectors. {source}"))]
    S3VectorCreateIndexError { source: CreateIndexError },

    #[snafu(display("Failed to create bucket in S3 Vectors. {source}"))]
    S3VectorCreateBucketError { source: CreateVectorBucketError },

    #[snafu(display("Failed to get bucket from S3 Vectors. {source}"))]
    S3VectorGetBucketError { source: GetVectorBucketError },

    #[snafu(display("Failed to get index from S3 Vectors. {source}"))]
    S3VectorGetIndexError { source: GetIndexError },

    #[snafu(display("Failed to construct a request to send to S3 Vectors. {source}"))]
    S3VectorBuildError { source: BuildError },

    #[snafu(display("Failed to infer schema from S3 vector. {source}"))]
    InferSchemaError { source: ArrowError },

    #[snafu(display(
        "S3 vector does not exist, and cannot be created from an S3 vector ARN. Specify a s3 vector bucket and index name."
    ))]
    CreateIndexUsingArn,

    #[snafu(display("Failed to load AWS credentials. {message}"))]
    UnableToLoadCredentials { message: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// [`S3VectorIdentifier`] uniquely identifies a S3 vector index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum S3VectorIdentifier {
    IndexArn(String),
    Index {
        bucket_name: String,
        index_name: String,
    },
}

impl S3VectorIdentifier {
    /// Return (index arn, bucket name and index name) based on how the vector index is identified.
    #[must_use]
    pub fn index_identifier_variables(&self) -> (Option<String>, Option<String>, Option<String>) {
        match self {
            Self::Index {
                bucket_name,
                index_name,
            } => (None, Some(bucket_name.clone()), Some(index_name.clone())),
            Self::IndexArn(arn) => (Some(arn.clone()), None, None),
        }
    }
}
