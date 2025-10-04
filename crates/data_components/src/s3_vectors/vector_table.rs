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
use crate::s3_vectors::{
    MetadataColumn, MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    S3VectorBuildSnafu,
};
use arrow_tools::record_batch::replace_column_in_record;
use std::{collections::HashMap, error::Error as StdError, sync::Arc};

use super::{Error, Result, S3VectorIdentifier};
use arrow::{
    array::RecordBatch,
    compute::cast,
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};

use aws_credential_types::provider::error::CredentialsError;
use datafusion::{
    common::{Constraint, Constraints},
    error::DataFusionError,
};

use s3_vectors::{
    CreateIndexInput, CreateVectorBucketInput, DistanceMetric, Document, GetIndexError,
    GetIndexInput, GetIndexOutput, GetVectorBucketError, GetVectorBucketInput,
    MetadataConfiguration, PUT_VECTORS_MAX_ITEMS, PutInputVector, PutVectorsInput, S3Vectors,
    SdkError, VectorData,
};
use s3_vectors_metadata_filter::json_value_to_document;
use serde_json::Value;
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

/// An S3 Vector index.
#[derive(Clone)]
pub struct S3VectorsTable {
    pub idx: S3VectorIdentifier,
    pub client: Arc<dyn S3Vectors + Send + Sync>,

    // The SQL schema of the index. Expects to have:
    // - `data` Float32
    // - `key` Utf8
    // - `metadata` will be flattened. types will be inferred as per `arrow_json`.
    pub schema: SchemaRef,

    pub(super) constraints: Constraints,

    pub dimension: i64,
    pub columns: MetadataColumns,
    pub distance_metric: DistanceMetric,
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
    // Returns an [`S3VectorTableResult`] if the [`S3VectorIdentifier`] does not exist. Use [`Self::try_create_new_identifier`].
    pub async fn try_new_table(
        id: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        dimension: i64,
        columns: MetadataColumns,
        distance_metric: &DistanceMetric,
    ) -> Result<S3VectorTableResult> {
        if !Self::check_if_bucket_exists(&client, &id).await? {
            return Ok(S3VectorTableResult::BucketDoesNotExist);
        }
        match Self::get_index_if_exists(&id, &client).await? {
            Some(GetIndexOutput {
                index: Some(index), ..
            }) => {
                if index.distance_metric() != distance_metric {
                    return Err(Error::IncompatibleDistanceMetric {
                        exists: index.distance_metric,
                        specified: distance_metric.clone(),
                    });
                }
                let schema = Self::compute_schema(index.dimension(), columns.clone());
                let constraints = Self::primary_key(&schema);
                Ok(S3VectorTableResult::Table(Self {
                    idx: id,
                    client,
                    schema,
                    constraints,
                    dimension,
                    columns,
                    distance_metric: distance_metric.clone(),
                }))
            }
            None | Some(GetIndexOutput { index: None, .. }) => {
                Ok(S3VectorTableResult::IndexDoesNotExist)
            }
        }
    }

    pub async fn try_create_new_table(
        id: S3VectorIdentifier,
        client: Arc<dyn S3Vectors + Send + Sync>,
        dimension: i64,
        columns: MetadataColumns,
        distance_metric: Option<impl Into<DistanceMetric>>,
    ) -> Result<Option<Self>> {
        let non_filterable_metadata_columns = columns.non_filterable_names();

        let distance_metric = match distance_metric.map(Into::into) {
            // Default to `DistanceMetric::Cosine` for backwards compatibility.
            Some(DistanceMetric::Cosine) | None => DistanceMetric::Cosine,
            Some(DistanceMetric::Euclidean) => DistanceMetric::Euclidean,
            Some(distance_metric) => {
                return Err(Error::InvalidDistanceMetric { distance_metric });
            }
        };

        match Self::try_new_table(
            id.clone(),
            Arc::clone(&client),
            dimension,
            columns.clone(),
            &distance_metric,
        )
        .await?
        {
            S3VectorTableResult::Table(slf) => Ok(Some(slf)),
            S3VectorTableResult::BucketDoesNotExist => {
                Self::create_bucket(&client, &id).await?;
                Self::create_index(
                    &client,
                    dimension,
                    &id,
                    non_filterable_metadata_columns,
                    &distance_metric,
                )
                .await?;
                Self::try_new_table(id, client, dimension, columns, &distance_metric)
                    .await
                    .map(S3VectorTableResult::table)
            }
            S3VectorTableResult::IndexDoesNotExist => {
                Self::create_index(
                    &client,
                    dimension,
                    &id,
                    non_filterable_metadata_columns,
                    &distance_metric,
                )
                .await?;
                Self::try_new_table(id, client, dimension, columns, &distance_metric)
                    .await
                    .map(S3VectorTableResult::table)
            }
        }
    }

    async fn create_index(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        dimension: i64,
        vector_id: &S3VectorIdentifier,
        non_filterable_metadata_columns: Vec<String>,
        distance_metric: &DistanceMetric,
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
            Some(
                MetadataConfiguration::builder()
                    .set_non_filterable_metadata_keys(Some(non_filterable_metadata_columns))
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
        };

        client
            .create_index(
                CreateIndexInput::builder()
                    .data_type(s3_vectors::DataType::Float32)
                    .dimension(dimension.try_into().unwrap_or(i32::MAX))
                    .distance_metric(distance_metric.clone())
                    .index_name(index_name)
                    .set_metadata_configuration(metadata_configuration)
                    .vector_bucket_name(bucket_name)
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
            .await
            .map_err(|e| Error::S3VectorCreateIndexError {
                source: e.into_service_error(),
            })?;
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
            .create_vector_bucket(
                CreateVectorBucketInput::builder()
                    .vector_bucket_name(bucket_name.clone())
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
            .await
            .map_err(|e| Error::S3VectorCreateBucketError {
                source: e.into_service_error(),
            })?;
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
            .get_vector_bucket(
                GetVectorBucketInput::builder()
                    .set_vector_bucket_name(bucket_name_opt)
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(e))
                if matches!(&e.err(), GetVectorBucketError::NotFoundException(_)) =>
            {
                Ok(false)
            }
            Err(e) => match &e {
                SdkError::DispatchFailure(d) => {
                    if let Some(credentials_error) = d
                        .as_connector_error()
                        .and_then(|e| e.source())
                        .and_then(|s| s.downcast_ref::<CredentialsError>())
                        .map(ToString::to_string)
                    {
                        return Err(Error::UnableToLoadCredentials {
                            message: credentials_error,
                        });
                    }
                    Err(Error::S3VectorGetBucketError {
                        source: e.into_service_error(),
                    })
                }
                _ => Err(Error::S3VectorGetBucketError {
                    source: e.into_service_error(),
                }),
            },
        }
    }

    /// Returns whether the index exists.
    async fn get_index_if_exists(
        index: &S3VectorIdentifier,
        client: &Arc<dyn S3Vectors + Send + Sync>,
    ) -> Result<Option<GetIndexOutput>> {
        let (index_arn, vector_bucket_name, index_name) = index.index_identifier_variables();
        match client
            .get_index(
                GetIndexInput::builder()
                    .set_index_arn(index_arn)
                    .set_vector_bucket_name(vector_bucket_name)
                    .set_index_name(index_name)
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
            .await
        {
            Err(SdkError::ServiceError(e))
                if matches!(&e.err(), GetIndexError::NotFoundException(_msg)) =>
            {
                Ok(None)
            }
            Ok(output) => Ok(Some(output)),
            Err(e) => Err(Error::S3VectorGetIndexError {
                source: e.into_service_error(),
            }),
        }
    }

    pub(crate) fn is_filterable_column(&self, column: &str) -> bool {
        let Ok(f) = self.schema.field_with_name(column) else {
            return false;
        };
        f.metadata().get("filterable").eq(&Some(&true.to_string()))
    }

    fn compute_schema(embedding_dimension: i32, columns: MetadataColumns) -> SchemaRef {
        Arc::new(Schema::new(
            [
                columns
                    .into_iter()
                    .map(|c| {
                        let f = c.field();
                        Field::new(f.name().clone(), f.data_type().clone(), f.is_nullable())
                            .with_metadata(
                                [(
                                    "filterable".to_string(),
                                    (matches!(c, MetadataColumn::Filterable(_))).to_string(),
                                )]
                                .into(),
                            )
                            .into()
                    })
                    .collect(),
                vec![
                    Arc::new(Field::new_fixed_size_list(
                        S3_VECTOR_EMBEDDING_NAME,
                        Field::new("item", DataType::Float32, false),
                        embedding_dimension,
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
    /// For `None` values of either `key` or `data`, the row will not be inserted.
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
                let data = data?;
                let meta: HashMap<String, Document> = metadata
                    .iter()
                    .filter_map(|(k, v)| {
                        let value = v.get(i)?.as_ref()?;
                        let meta = json_value_to_document(value.clone());

                        if matches!(meta, Document::Null) {
                            return None;
                        }

                        Some((k.clone(), meta))
                    })
                    .collect();

                let put_input_vector = PutInputVector::builder()
                    .key(key)
                    .set_metadata(if meta.is_empty() {
                        None
                    } else {
                        Some(Document::Object(meta))
                    })
                    .data(VectorData::Float32(data))
                    .build()
                    .ok()?;

                Some(put_input_vector)
            })
            .collect();

        let (index_arn, vector_bucket_name, index_name) = self.idx.index_identifier_variables();

        for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
            self.client
                .put_vectors(
                    PutVectorsInput::builder()
                        .set_index_arn(index_arn.clone())
                        .set_index_name(index_name.clone())
                        .set_vector_bucket_name(vector_bucket_name.clone())
                        .set_vectors(Some(chunk.to_vec()))
                        .build()
                        .context(S3VectorBuildSnafu)?,
                )
                .await
                .map_err(|e| Error::S3VectorPutVectorError {
                    source: e.into_service_error(),
                })?;
        }

        tracing::info!(
            "S3 Vectors Index {index_name} updated; records={records}, duration={duration:?}",
            index_name = self.idx,
            records = vectors.len(),
            duration = start.elapsed()
        );

        Ok(())
    }
}

// For a [`SchemaRef`] with [`FixedSizeListArray`]s, convert them to [`ListArray`] and return the associated size for each column name.
//
// This is useful when JSON decoding data with [`FixedSizeListArray`] since arrow_json has not implemented JSON reading of [`FixedSizeListArray`].
pub(super) fn loosen_vector_schema(s: &SchemaRef) -> (SchemaRef, HashMap<String, DataType>) {
    let mut sizes: HashMap<String, DataType> = HashMap::default();
    let fields: Vec<_> = s
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::FixedSizeList(inner, n) => {
                sizes.insert(
                    f.name().clone(),
                    DataType::FixedSizeList(Arc::clone(inner), *n),
                );
                Arc::unwrap_or_clone(Arc::clone(f))
                    .with_data_type(DataType::List(Arc::clone(inner)))
                    .into()
            }
            _ => Arc::clone(f),
        })
        .collect();

    (Arc::new(Schema::new(fields)), sizes)
}

pub(super) fn make_fixed_sizes(
    mut rb: RecordBatch,
    vector_sizes: &HashMap<String, DataType>,
) -> Result<RecordBatch, ArrowError> {
    for (col, fixed_size_type) in vector_sizes {
        if let Some(arr) = rb.column_by_name(col) {
            rb = replace_column_in_record(rb.clone(), col, &cast(arr, fixed_size_type)?)?;
        }
    }
    Ok(rb)
}

pub(super) async fn send_vector_data(
    tx: &Sender<Result<RecordBatch, DataFusionError>>,
    rb: RecordBatch,
    vector_sizes: &HashMap<String, DataType>,
) {
    let _ = match make_fixed_sizes(rb, vector_sizes) {
        Ok(v) => {
            tx.send(Ok(v)).await
        }
        Err(e) => {
            tx
                .send(Err(DataFusionError::ArrowError(
                    Box::new(e),
                    Some("Successfully decoded S3 vector JSON response, but could not convert appropriate vectors or metadata to `FixedSizeListArray`.".to_string())
                )))
                .await
        }
    };
}
