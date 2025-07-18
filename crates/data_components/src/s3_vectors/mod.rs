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
        "Failed to s3vector.\n{source}\nReport an issue on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InternalError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("An error occured interacting with the S3 vector API.\n{source}\n"))]
    S3Vector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(""))]
    InferSchemaError { source: ArrowError },

    #[snafu(display(
        "S3 vector does not exist, and cannot be created from an S3 vector ARN. Specify a s3 vector bucket and index name."
    ))]
    CreateIndexUsingArn,
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
