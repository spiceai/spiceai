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
use std::{collections::HashMap, sync::Arc};

use crate::s3_vectors::{MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME};

use super::{Error, Result, S3VectorIdentifier};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Constraint, Constraints};
use s3_vectors::{
    CreateIndexInput, CreateVectorBucketInput, GetIndexError, GetIndexInput, GetVectorBucketError,
    GetVectorBucketInput, MetadataConfiguration, PUT_VECTORS_MAX_ITEMS, PutInputVector,
    PutVectorsInput, S3Vectors, VectorData, VectorMetadata, custom::inner_service_error,
};
use serde_json::Value;

/// An S3 Vector index.
#[derive(Clone)]
pub struct S3VectorsTable {
    pub(super) idx: S3VectorIdentifier,
    pub(super) client: Arc<dyn S3Vectors + Send + Sync>,

    // The SQL schema of the index. Expects to have:
    // - `data` Float32
    // - `key` Utf8
    // - `metadata` will be flattened. types will be inferred as per `arrow_json`.
    pub(super) schema: SchemaRef,

    pub(super) constraints: Constraints,
}

impl std::fmt::Debug for S3VectorsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorsListTable")
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .field("index_identifier", &self.idx)
            .finish_non_exhaustive()
    }
}

pub enum S3VectorTableResult {
    IndexDoesNotExist,
    BucketDoesNotExist,
    Table(S3VectorsTable),
}

impl S3VectorTableResult {
    #[must_use]
    pub fn table(self) -> Option<S3VectorsTable> {
        match self {
            S3VectorTableResult::Table(table) => Some(table),
            _ => None,
        }
    }
}

impl S3VectorsTable {
    pub async fn try_new_arn(
        index_arn: impl Into<String>,
        client: Arc<dyn S3Vectors + Send + Sync>,
        columns: MetadataColumns,
    ) -> Result<Option<Self>> {
        Self::try_new_table(
            S3VectorIdentifier::IndexArn(index_arn.into()),
            client,
            columns,
        )
        .await
        .map(S3VectorTableResult::table)
    }

    // Returns an [`S3VectorTableResult`] if the [`S3VectorIdentifier`] does not exist. Use [`Self::try_create_new_identifier`].
    pub async fn try_new_table(
        id: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        columns: MetadataColumns,
    ) -> Result<S3VectorTableResult> {
        if !Self::check_if_bucket_exists(&client, &id).await? {
            return Ok(S3VectorTableResult::BucketDoesNotExist);
        }
        if !Self::check_if_index_exists(&id, &client).await? {
            return Ok(S3VectorTableResult::IndexDoesNotExist);
        }
        let schema = Self::compute_schema(columns);
        let constraints = Self::primary_key(&schema);
        Ok(S3VectorTableResult::Table(Self {
            idx: id,
            client,
            schema,
            constraints,
        }))
    }

    pub async fn try_create_new_table(
        id: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        dimension: i64,
        columns: MetadataColumns,
    ) -> Result<Option<Self>> {
        let non_filterable_metadata_columns = columns.non_filterable_names();

        match Self::try_new_table(id.clone(), Arc::clone(&client), columns.clone()).await? {
            S3VectorTableResult::Table(slf) => Ok(Some(slf)),
            S3VectorTableResult::BucketDoesNotExist => {
                Self::create_bucket(&client, &id).await?;
                Self::create_index(&client, dimension, &id, non_filterable_metadata_columns)
                    .await?;
                Self::try_new_table(id, client, columns)
                    .await
                    .map(S3VectorTableResult::table)
            }
            S3VectorTableResult::IndexDoesNotExist => {
                Self::create_index(&client, dimension, &id, non_filterable_metadata_columns)
                    .await?;
                Self::try_new_table(id, client, columns)
                    .await
                    .map(S3VectorTableResult::table)
            }
        }
    }

    pub async fn try_new_vector_index(
        bucket_name: impl Into<String>,
        index_name: impl Into<String>,
        client: Arc<dyn S3Vectors + Send + Sync>,
        columns: MetadataColumns,
    ) -> Result<Option<Self>> {
        Self::try_new_table(
            S3VectorIdentifier::Index {
                bucket_name: bucket_name.into(),
                index_name: index_name.into(),
            },
            client,
            columns,
        )
        .await
        .map(S3VectorTableResult::table)
    }

    #[must_use]
    pub fn new(
        index: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        schema: SchemaRef,
    ) -> Self {
        let constraints = Self::primary_key(&schema);
        Self {
            idx: index,
            client,
            schema,
            constraints,
        }
    }

    async fn create_index(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        dimension: i64,
        vector_id: &S3VectorIdentifier,
        non_filterable_metadata_columns: Vec<String>,
    ) -> Result<()> {
        let S3VectorIdentifier::Index {
            bucket_name,
            index_name,
        } = vector_id
        else {
            return Err(Error::CreateIndexUsingArn);
        };

        let metadata_configuration = if non_filterable_metadata_columns.is_empty() {
            None
        } else {
            Some(MetadataConfiguration {
                non_filterable_metadata_keys: non_filterable_metadata_columns,
            })
        };

        client
            .create_index(CreateIndexInput {
                data_type: "float32".to_string(),
                dimension,
                distance_metric: "cosine".to_string(),
                index_name: index_name.clone(),
                metadata_configuration,
                vector_bucket_arn: None,
                vector_bucket_name: Some(bucket_name.clone()),
            })
            .await
            .map_err(|e| Error::S3Vector { source: e.into() })?;
        Ok(())
    }

    async fn create_bucket(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        id: &S3VectorIdentifier,
    ) -> Result<()> {
        let S3VectorIdentifier::Index { bucket_name, .. } = id else {
            return Err(Error::CreateIndexUsingArn);
        };
        client
            .create_vector_bucket(CreateVectorBucketInput {
                vector_bucket_name: bucket_name.clone(),
                encryption_configuration: None,
            })
            .await
            .map_err(|e| Error::S3Vector { source: e.into() })?;
        Ok(())
    }

    async fn check_if_bucket_exists(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        id: &S3VectorIdentifier,
    ) -> Result<bool> {
        let bucket_name_opt = match id {
            S3VectorIdentifier::Index { bucket_name, .. } => Some(bucket_name.clone()),
            S3VectorIdentifier::IndexArn(_) => None,
        };
        match client
            .get_vector_bucket(GetVectorBucketInput {
                vector_bucket_arn: None,
                vector_bucket_name: bucket_name_opt,
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(e)
                if matches!(
                    inner_service_error::<GetVectorBucketError>(&e),
                    Some(GetVectorBucketError::NotFound(_))
                ) =>
            {
                Ok(false)
            }
            Err(e) => Err(Error::S3Vector { source: e.into() }),
        }
    }

    /// Returns whether the index exists.
    async fn check_if_index_exists(
        index: &S3VectorIdentifier,
        client: &Arc<dyn S3Vectors + Send + Sync>,
    ) -> Result<bool> {
        let (index_arn, vector_bucket_name, index_name) = index.index_identifier_variables();
        match client
            .get_index(GetIndexInput {
                index_arn,
                vector_bucket_name,
                index_name,
            })
            .await
        {
            Err(e)
                if matches!(
                    inner_service_error::<GetIndexError>(&e),
                    Some(GetIndexError::NotFound(_msg))
                ) =>
            {
                Ok(false)
            }
            Ok(_) => Ok(true),
            Err(e) => Err(Error::S3Vector { source: e.into() }),
        }
    }

    fn compute_schema(columns: MetadataColumns) -> SchemaRef {
        Arc::new(Schema::new(
            [
                columns.into_iter().map(|c| c.field()).collect(),
                vec![
                    Arc::new(Field::new_list(
                        S3_VECTOR_EMBEDDING_NAME,
                        Field::new("item", DataType::Float32, false),
                        false,
                    )),
                    Arc::new(Field::new(
                        S3_VECTOR_PRIMARY_KEY_NAME,
                        DataType::Utf8,
                        false,
                    )),
                ],
            ]
            .concat(),
        ))
    }

    fn primary_key(schema: &SchemaRef) -> Constraints {
        schema
            .column_with_name(S3_VECTOR_PRIMARY_KEY_NAME)
            .map(|(i, _)| Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![i])]))
            .unwrap_or_default()
    }

    /// Writes new data to the s3 vector index.
    ///
    /// Inputs are expected to have equal length.
    ///   `data.len() == key.len() == metadata[key].len()`, for all `key` in `metadata.keys()`.
    ///
    /// For `None` values of `key`, the row will not be inserted.
    pub async fn write_data(
        &self,
        data: Vec<Option<Vec<f32>>>,
        key: Vec<Option<String>>,
        metadata: HashMap<String, Vec<Option<Value>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        let vectors: Vec<PutInputVector> = data
            .into_iter()
            .zip(key.into_iter())
            .enumerate()
            .filter_map(|(i, (data, key))| {
                let key = key?.to_string();
                let meta: VectorMetadata = metadata
                    .iter()
                    .filter_map(|(k, v)| {
                        let value = v.get(i)?.as_ref()?;
                        Some((k.clone(), value.clone()))
                    })
                    .collect();

                Some(PutInputVector {
                    key,
                    metadata: if meta.is_empty() { None } else { Some(meta) },
                    data: VectorData { float_32: data },
                })
            })
            .collect();

        let (index_arn, vector_bucket_name, index_name) = self.idx.index_identifier_variables();

        for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
            self.client
                .put_vectors(PutVectorsInput {
                    index_arn: index_arn.clone(),
                    index_name: index_name.clone(),
                    vector_bucket_name: vector_bucket_name.clone(),
                    vectors: chunk.to_vec(),
                })
                .await
                .map_err(|e| Error::S3Vector { source: e.into() })?;
        }

        tracing::info!(
            "S3 Vectors Index updated; records={} records, duration={duration:?}",
            vectors.len(),
            duration = start.elapsed()
        );

        Ok(())
    }
}
