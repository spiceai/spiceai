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
    S3VectorBuildSnafu, spill::MAX_SPILL_SEQUENCE,
};
use arrow_tools::record_batch::replace_column_in_record;
use std::{
    collections::HashMap,
    error::Error as StdError,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

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
    CreateIndexError, CreateIndexInput, CreateVectorBucketError, CreateVectorBucketInput,
    DistanceMetric, Document, GetIndexError, GetIndexInput, GetIndexOutput, GetVectorBucketError,
    GetVectorBucketInput, MetadataConfiguration, PUT_VECTORS_MAX_ITEMS, PutInputVector,
    PutVectorsError, PutVectorsInput, S3Vectors, SdkError, VectorData,
};
use s3_vectors_metadata_filter::json_value_to_document;
use serde_json::Value;
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

/// An S3 Vector index.
#[derive(Clone)]
pub struct S3VectorsTable {
    pub idx: Arc<S3VectorIdentifier>,
    pub spill_index: Arc<AtomicU8>,
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
            .field("index_identifier", &self.current_index())
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
    /// Returns the current index identifier, accounting for spilling.
    #[must_use]
    pub fn current_index(&self) -> S3VectorIdentifier {
        let spill_num = self.spill_index.load(Ordering::SeqCst);
        if spill_num == 0 {
            (*self.idx).clone()
        } else {
            match &*self.idx {
                S3VectorIdentifier::Index {
                    bucket_name,
                    index_name,
                } => {
                    let spill_name = format!("{index_name}.{spill_num:02}");
                    S3VectorIdentifier::Index {
                        bucket_name: bucket_name.clone(),
                        index_name: spill_name,
                    }
                }
                S3VectorIdentifier::IndexArn(_) => (*self.idx).clone(),
            }
        }
    }

    /// Returns the next index identifier, incrementing the spill index
    ///
    /// # Errors
    /// Returns an error if there is no next index
    pub fn next_index(&self) -> Result<S3VectorIdentifier> {
        let old_spill_index =
            self.spill_index
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                    if x >= MAX_SPILL_SEQUENCE {
                        None
                    } else {
                        Some(x + 1)
                    }
                });

        let max_exceeded = old_spill_index.is_err();
        if max_exceeded {
            return Err(Error::MaxSpillAttemptsReached);
        }

        Ok(self.current_index())
    }

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
                    idx: Arc::new(id),
                    spill_index: Arc::new(AtomicU8::new(0)),
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

        match client
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
        {
            Ok(_) => Ok(()),
            Err(e) => match &e {
                SdkError::ServiceError(service_error)
                    if matches!(service_error.err(), CreateIndexError::ConflictException(_)) =>
                {
                    // Check if the index exists now (it might have been created by another thread)
                    match Self::get_index_if_exists(vector_id, client).await? {
                        Some(_) => Ok(()), // Index exists, treat as success
                        None => Err(Error::S3VectorCreateIndexError {
                            source: Box::new(e.into_service_error()),
                        }),
                    }
                }
                _ => Err(Error::S3VectorCreateIndexError {
                    source: Box::new(e.into_service_error()),
                }),
            },
        }
    }

    async fn create_bucket(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        id: &S3VectorIdentifier,
    ) -> Result<()> {
        let S3VectorIdentifier::Index { bucket_name, .. } = id else {
            return Err(Error::CreateIndexUsingArn);
        };
        match client
            .create_vector_bucket(
                CreateVectorBucketInput::builder()
                    .vector_bucket_name(bucket_name.clone())
                    .build()
                    .context(S3VectorBuildSnafu)?,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => match &e {
                SdkError::ServiceError(service_error)
                    if matches!(
                        service_error.err(),
                        CreateVectorBucketError::ConflictException(_)
                    ) =>
                {
                    // Check if the bucket exists now (it might have been created by another thread)
                    if Self::check_if_bucket_exists(client, id).await? {
                        Ok(()) // Bucket exists, treat as success
                    } else {
                        Err(Error::S3VectorCreateBucketError {
                            source: Box::new(e.into_service_error()),
                        })
                    }
                }
                _ => Err(Error::S3VectorCreateBucketError {
                    source: Box::new(e.into_service_error()),
                }),
            },
        }
    }

    async fn check_if_bucket_exists(
        client: &Arc<dyn S3Vectors + Send + Sync>,
        id: &S3VectorIdentifier,
    ) -> Result<bool> {
        let bucket_name_opt = id.bucket_name().map(ToString::to_string);
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
                        source: Box::new(e.into_service_error()),
                    })
                }
                _ => Err(Error::S3VectorGetBucketError {
                    source: Box::new(e.into_service_error()),
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
                source: Box::new(e.into_service_error()),
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

        for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
            self.write_chunk_with_spilling(chunk).await?;
        }

        let current_index = self.current_index();

        tracing::info!(
            "S3 Vectors Index {index_name} updated; records={records}, duration={duration:?}",
            index_name = &current_index,
            records = vectors.len(),
            duration = start.elapsed()
        );

        Ok(())
    }

    /// Writes a chunk of vectors, handling spilling to additional indexes when capacity is exceeded.
    async fn write_chunk_with_spilling(&self, chunk: &[PutInputVector]) -> Result<()> {
        let mut current_index = self.current_index();

        loop {
            let (index_arn, vector_bucket_name, index_name) =
                current_index.index_identifier_variables();

            let result = self
                .client
                .put_vectors(
                    PutVectorsInput::builder()
                        .set_index_arn(index_arn.clone())
                        .set_index_name(index_name.clone())
                        .set_vector_bucket_name(vector_bucket_name.clone())
                        .set_vectors(Some(chunk.to_vec()))
                        .build()
                        .context(S3VectorBuildSnafu)?,
                )
                .await;

            match result {
                Ok(_) => {
                    return Ok(());
                }
                Err(SdkError::ServiceError(service_error)) => {
                    if Self::is_capacity_exceeded_error(service_error.err()) {
                        // Increment spill index and try to create a new index
                        current_index = self.next_index()?;
                        Self::create_index(
                            &self.client,
                            self.dimension,
                            &current_index,
                            self.columns.non_filterable_names(),
                            &self.distance_metric,
                        )
                        .await?;
                    } else {
                        return Err(Error::S3VectorPutVectorError {
                            source: Box::new(service_error.into_err()),
                        });
                    }
                }
                Err(e) => {
                    return Err(Error::S3VectorPutVectorError {
                        source: Box::new(e.into_service_error()),
                    });
                }
            }
        }
    }

    fn is_capacity_exceeded_error(error: &PutVectorsError) -> bool {
        matches!(error, PutVectorsError::ServiceQuotaExceededException(_))
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

#[cfg(test)]
mod tests {
    use super::*;

    use s3_vectors::{DataType as S3DataType, mock::MockClient};
    use std::sync::Arc;

    fn create_test_table(
        client: Arc<dyn S3Vectors + Send + Sync>,
        index_name: &str,
    ) -> S3VectorsTable {
        S3VectorsTable {
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: "test-bucket".to_string(),
                index_name: index_name.to_string(),
            }),
            spill_index: Arc::new(AtomicU8::new(0)),
            client,
            schema: Arc::new(Schema::new(vec![
                Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                Field::new_fixed_size_list(
                    S3_VECTOR_EMBEDDING_NAME,
                    Field::new("item", DataType::Float32, false),
                    3,
                    false,
                ),
            ])),
            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        }
    }

    fn create_test_vectors(count: usize) -> Vec<PutInputVector> {
        (0..count)
            .filter_map(|i| {
                PutInputVector::builder()
                    .key(format!("key{i}"))
                    .data(VectorData::Float32(vec![1.0, 2.0, 3.0]))
                    .build()
                    .ok()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_write_chunk_without_spilling() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let table = create_test_table(
            Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>,
            "test-index",
        );

        // Create the bucket and index first
        table
            .client
            .create_vector_bucket(
                CreateVectorBucketInput::builder()
                    .vector_bucket_name("test-bucket")
                    .build()?,
            )
            .await?;

        table
            .client
            .create_index(
                CreateIndexInput::builder()
                    .index_name("test-index")
                    .vector_bucket_name("test-bucket")
                    .data_type(S3DataType::Float32)
                    .dimension(3)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()?,
            )
            .await?;

        let vectors = create_test_vectors(5);
        let result = table.write_chunk_with_spilling(&vectors).await;

        assert!(result.is_ok());
        assert_eq!(mock_client.get_vector_count("test-index"), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_chunk_with_spilling() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let table = create_test_table(
            Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>,
            "test-index",
        );

        // Set a low quota limit for main index and potential spill indexes
        mock_client.set_quota_limit("test-index", 3);
        mock_client.set_quota_limit("test-index.01", 3);
        mock_client.set_quota_limit("test-index.02", 3);

        // Create the bucket and index first
        table
            .client
            .create_vector_bucket(
                CreateVectorBucketInput::builder()
                    .vector_bucket_name("test-bucket")
                    .build()?,
            )
            .await?;

        table
            .client
            .create_index(
                CreateIndexInput::builder()
                    .index_name("test-index")
                    .vector_bucket_name("test-bucket")
                    .data_type(S3DataType::Float32)
                    .dimension(3)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()?,
            )
            .await?;

        let vectors = create_test_vectors(3);
        let result = table.write_chunk_with_spilling(&vectors).await;
        assert!(result.is_ok());
        let vectors = create_test_vectors(3);
        let result = table.write_chunk_with_spilling(&vectors).await;
        assert!(result.is_ok());
        let vectors = create_test_vectors(3);
        let result = table.write_chunk_with_spilling(&vectors).await;
        assert!(result.is_ok());

        assert_eq!(mock_client.get_vector_count("test-index"), 3);
        assert_eq!(mock_client.get_vector_count("test-index.01"), 3);
        assert_eq!(mock_client.get_vector_count("test-index.02"), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_chunk_spilling_exhausted() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let table = create_test_table(
            Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>,
            "test-index",
        );

        // Set quota limits for main index and 99 spill indexes (01-99)
        mock_client.set_quota_limit("test-index", 1);
        for i in 1..=99 {
            let index_name = format!("test-index.{i:02}");
            mock_client.set_quota_limit(&index_name, 1);
        }

        table
            .client
            .create_vector_bucket(
                CreateVectorBucketInput::builder()
                    .vector_bucket_name("test-bucket")
                    .build()?,
            )
            .await?;

        table
            .client
            .create_index(
                CreateIndexInput::builder()
                    .index_name("test-index")
                    .vector_bucket_name("test-bucket")
                    .data_type(S3DataType::Float32)
                    .dimension(3)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()?,
            )
            .await?;

        for _ in 0..100 {
            let vectors = create_test_vectors(1);
            table.write_chunk_with_spilling(&vectors).await?;
        }

        let vectors = create_test_vectors(1);
        let result = table.write_chunk_with_spilling(&vectors).await;

        assert!(result.is_err());

        Ok(())
    }
}
