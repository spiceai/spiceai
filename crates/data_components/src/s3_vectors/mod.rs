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
use datafusion::error::DataFusionError;
use s3_vectors::{
    BuildError, CreateIndexError, CreateVectorBucketError, DistanceMetric, Document, GetIndexError,
    GetVectorBucketError, GetVectorsError, ListIndexesError, ListIndexesInput, PutVectorsError,
    QueryVectorsError, S3Vectors,
};
use s3_vectors_metadata_filter::MetadataFilter;
use snafu::{ResultExt as _, Snafu};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub mod list_provider;
pub mod partition;
pub mod query_provider;
mod spill;
pub use spill::{Error as SpillIndexError, SpillIndex};
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
    S3VectorPutVectorError { source: Box<PutVectorsError> },

    #[snafu(display("Failed to query vectors from S3 Vectors. {source}"))]
    S3VectorQueryVectorsError { source: Box<QueryVectorsError> },

    #[snafu(display("Failed to get vectors from S3 Vectors. {source}"))]
    S3VectorGetVectorsError { source: Box<GetVectorsError> },

    #[snafu(display(
        "Failed to query vectors from S3 Vectors due to an unsupported filter: {filter_pre} {filter:?}"
    ))]
    S3VectorQueryVectorsInvalidFilterError {
        filter_pre: MetadataFilter,
        filter: Document,
    },

    #[snafu(display("Failed to create index in S3 Vectors. {source}"))]
    S3VectorCreateIndexError { source: Box<CreateIndexError> },

    #[snafu(display("Failed to create bucket in S3 Vectors. {source}"))]
    S3VectorCreateBucketError {
        source: Box<CreateVectorBucketError>,
    },

    #[snafu(display("Failed to get bucket from S3 Vectors. {source}"))]
    S3VectorGetBucketError { source: Box<GetVectorBucketError> },

    #[snafu(display("Failed to get index from S3 Vectors. {source}"))]
    S3VectorGetIndexError { source: Box<GetIndexError> },

    #[snafu(display("Failed to construct a request to send to S3 Vectors. {source}"))]
    S3VectorBuildError { source: BuildError },

    #[snafu(display("Failed to infer schema from S3 vector. {source}"))]
    InferSchemaError { source: ArrowError },

    #[snafu(display(
        "S3 vector does not exist, and cannot be created from an S3 vector ARN. Specify a s3 vector bucket and index name."
    ))]
    CreateIndexUsingArn,

    #[snafu(display(
        "Failed to load AWS credentials to connect to S3 Vectors. Verify the AWS credentials are available in the environment. For help configuring AWS authentication visit https://spiceai.org/docs/components/vectors/s3_vectors#authentication"
    ))]
    UnableToLoadCredentials { message: String },

    #[snafu(display(
        "Invalid distance metric specified for S3 vector index: '{distance_metric}'. Must be one of: {} or {}.",
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean
    ))]
    InvalidDistanceMetric { distance_metric: DistanceMetric },

    #[snafu(display(
        "S3 vector index already exists with {exists} distance metric, but {specified} distance metric specified"
    ))]
    IncompatibleDistanceMetric {
        exists: DistanceMetric,
        specified: DistanceMetric,
    },
    #[snafu(display("S3 vector indexes cannot be listed"))]
    S3VectorListIndexesError { source: Box<ListIndexesError> },

    #[snafu(display("Spill index error: {source}"))]
    SpillIndexError { source: SpillIndexError },

    #[snafu(display("Exceeded maximum spill attempts while writing vectors"))]
    MaxSpillAttemptsReached,
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

impl Display for S3VectorIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IndexArn(arn) => write!(f, "{arn}"),
            Self::Index {
                bucket_name,
                index_name,
            } => write!(f, "{bucket_name}/{index_name}"),
        }
    }
}

impl S3VectorIdentifier {
    /// Return (index arn, bucket name and index name) based on how the vector index is identified.
    /// For virtual indexes, returns the bucket name and virtual index name for writing operations.
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

    /// Gets the bucket name for this identifier, if available.
    #[must_use]
    pub fn bucket_name(&self) -> Option<&str> {
        match self {
            Self::Index { bucket_name, .. } => Some(bucket_name),
            Self::IndexArn(_) => None,
        }
    }

    /// Gets the virtual index name for this identifier, if it's a virtual index.
    #[must_use]
    pub fn index_name(&self) -> Option<&str> {
        if let Self::Index { index_name, .. } = self {
            Some(index_name)
        } else {
            None
        }
    }
}

/// Lists index names with the given prefix in the specified bucket.
pub async fn list_index_names(
    client: &Arc<dyn S3Vectors + Send + Sync>,
    bucket_name: &str,
    prefix: &str,
) -> Result<Vec<String>, DataFusionError> {
    let list_indexes_output = client
        .list_indexes(
            ListIndexesInput::builder()
                .set_vector_bucket_name(Some(bucket_name.to_string()))
                .set_prefix(Some(prefix.to_string()))
                .build()
                .boxed()
                .map_err(DataFusionError::External)?,
        )
        .await
        .map_err(|e| {
            DataFusionError::External(
                Error::S3VectorListIndexesError {
                    source: Box::new(e.into_service_error()),
                }
                .into(),
            )
        })?;

    Ok(list_indexes_output
        .indexes()
        .iter()
        .map(|idx| idx.index_name().to_string())
        .collect())
}

async fn fetch_all_index_names(
    client: &Arc<dyn S3Vectors + Send + Sync>,
    bucket_name: Option<&str>,
    index_name: Option<&str>,
) -> Result<Option<Vec<String>>, DataFusionError> {
    if let (Some(bucket_name), Some(index_name)) = (bucket_name, index_name) {
        // Use the base name (without spill suffix) as prefix to get all related indexes
        let base_name = if let Ok(Some(spill)) = SpillIndex::parse(index_name) {
            spill.base_name
        } else {
            index_name.to_string()
        };
        Ok(Some(
            list_index_names(client, bucket_name, &base_name).await?,
        ))
    } else {
        Ok(None)
    }
}
