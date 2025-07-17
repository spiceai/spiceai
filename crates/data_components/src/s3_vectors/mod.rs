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
use s3_vectors::custom::CoreError as S3VectorError;
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
    S3Vector { source: S3VectorError },

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

#[cfg(test)]
pub mod tests {

    use std::sync::Arc;

    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use s3_vectors::{
        CreateIndexInput, CreateVectorBucketInput, DeleteIndexInput, DeleteVectorBucketInput,
        PutInputVector, PutVectorsInput, S3Vectors, S3VectorsClient, S3VectorsCredentialProvider,
        VectorData,
    };
    use serde_json::json;

    use crate::s3_vectors::{
        list_provider::S3VectorsListTable, query_provider::S3VectorsQueryTable,
        vector_table::S3VectorsTable,
    };

    use super::*;

    async fn prepare_index(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        bucket_name: &str,
        index_name: &str,
    ) {
        client
            .delete_index(DeleteIndexInput {
                vector_bucket_name: Some(bucket_name.into()),
                index_arn: None,
                index_name: Some(index_name.into()),
            })
            .await
            .expect("delete_index");
        client
            .delete_vector_bucket(DeleteVectorBucketInput {
                vector_bucket_name: Some(bucket_name.into()),
                vector_bucket_arn: None,
            })
            .await
            .expect("delete_vector_bucket");
        let _output = client
            .create_vector_bucket(CreateVectorBucketInput {
                encryption_configuration: None,
                vector_bucket_name: bucket_name.to_string(),
            })
            .await
            .expect("create_vector_bucket");

        let _bucket = client
            .create_index(CreateIndexInput {
                data_type: "float32".into(),
                dimension: 3,
                distance_metric: "cosine".into(),
                index_name: index_name.into(),
                metadata_configuration: None,
                vector_bucket_name: Some(bucket_name.into()),
                vector_bucket_arn: None,
            })
            .await
            .expect("create_index");
        let _ = client
            .put_vectors(PutVectorsInput {
                index_name: Some(index_name.into()),
                index_arn: None,
                vector_bucket_name: Some(bucket_name.into()),
                vectors: vec![
                    PutInputVector {
                        data: VectorData {
                            float_32: Some(vec![1.0, 2.0, 3.0]),
                        },
                        key: "v1".into(),
                        metadata: Some(serde_json::Map::from_iter([
                            ("description".into(), json!("vector 1")),
                            ("categories".into(), json!(["test", "example"])),
                            ("msrp".into(), json!(100.0)),
                            ("count".into(), json!(10)),
                        ])),
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: Some(vec![4.0, 5.0, 6.0]),
                        },
                        key: "v2".into(),
                        metadata: Some(serde_json::Map::from_iter([
                            ("description".into(), json!("vector 2")),
                            ("categories".into(), json!(["test", "eggs"])),
                            ("msrp".into(), json!(200.0)),
                            ("count".into(), json!(20)),
                        ])),
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: Some(vec![7.0, 8.0, 9.0]),
                        },
                        key: "v3".into(),
                        metadata: Some(serde_json::Map::from_iter([
                            ("description".into(), json!("vector 3")),
                            ("categories".into(), json!(["test", "bacon"])),
                            ("msrp".into(), json!(300.0)),
                            ("count".into(), json!(30)),
                        ])),
                    },
                    PutInputVector {
                        data: VectorData {
                            float_32: Some(vec![2.0, 2.0, 2.0]),
                        },
                        key: "v4".into(),
                        metadata: Some(serde_json::Map::from_iter([
                            ("description".into(), json!("vector 4")),
                            ("categories".into(), json!(["eggs", "bacon"])),
                            ("msrp".into(), json!(400.0)),
                            ("count".into(), json!(40)),
                        ])),
                    },
                ],
            })
            .await
            .expect("put_vectors");
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_list_vectors() -> Result<(), String> {
        let (credential_provider, _) = S3VectorsCredentialProvider::from_env()
            .await
            .map_err(|e| e.to_string())?;

        let client = Arc::new(
            S3VectorsClient::try_new("us-west-2", credential_provider).expect("bad region"),
        ) as Arc<dyn S3Vectors + Send + Sync>;
        let bucket_name: &'static str = "spice-s3-jeadie-vectors";
        let index_name = "lookup";

        prepare_index(&client, bucket_name, index_name).await;

        let tbl: S3VectorsListTable = S3VectorsTable::try_new_vector_index(
            bucket_name,
            index_name,
            client,
            MetadataColumns::none(),
        )
        .await
        .expect("could not create S3VectorsTable")
        .expect("No S3VectorsTable was returned")
        .into();

        println!("tbl.schema: {:?}", tbl.schema());

        let ctx = SessionContext::new();
        ctx.register_table("s3_vectors", Arc::new(tbl))
            .expect("could not register S3VectorsTable");

        ctx.sql("SELECT * FROM s3_vectors LIMIT 4")
            .await
            .expect("could not execute query")
            .show()
            .await
            .expect("could not collect results");

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_s3_query_vectors() -> Result<(), String> {
        let (credential_provider, _) = S3VectorsCredentialProvider::from_env()
            .await
            .map_err(|e| e.to_string())?;

        let client = Arc::new(
            S3VectorsClient::try_new("us-west-2", credential_provider).expect("bad region"),
        ) as Arc<dyn S3Vectors + Send + Sync>;
        let bucket_name: &'static str = "spice-s3-jeadie-vectors";
        let index_name = "test";

        prepare_index(&client, bucket_name, index_name).await;

        let tbl = Arc::new(S3VectorsQueryTable::new(
            S3VectorsTable::try_new_vector_index(
                bucket_name,
                index_name,
                client,
                MetadataColumns::none(),
            )
            .await
            .expect("could not create S3VectorsTable")
            .expect("No S3VectorsTable was returned"),
            vec![1.0, -1.0, 3.0],
        )) as Arc<dyn TableProvider>;
        println!("tbl.schema: {:?}", tbl.schema());

        let ctx = SessionContext::new();
        ctx.register_table("s3_vectors", tbl)
            .expect("could not register S3VectorsTable");

        ctx.sql("SELECT * FROM s3_vectors WHERE msrp > 100 LIMIT 101")
            .await
            .expect("could not execute query")
            .show()
            .await
            .expect("could not collect results");

        ctx.sql("explain SELECT * FROM s3_vectors WHERE msrp > 100 and description!='somethind random' LIMIT 10")
            .await
            .expect("could not execute query")
            .show()
            .await
            .expect("could not collect results");

        ctx.sql("explain SELECT * FROM s3_vectors WHERE description IN ('vector 4', 'vector 2') AND msrp IN (300, 300.0, 100.0) LIMIT 10")
            .await
            .expect("could not execute query")
            .show()
            .await
            .expect("could not collect results");

        Ok(())
    }
}
