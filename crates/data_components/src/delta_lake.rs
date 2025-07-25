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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use chrono::TimeZone;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::DFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, lit};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use delta_kernel::Table;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::expressions::{BinaryExpressionOp, DecimalData, Expression, Scalar};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::schema::{DecimalType, PrimitiveType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{ExpressionRef, Predicate};
use indexmap::IndexMap;
use object_store::ObjectMeta;
use pruning::{can_be_evaluted_for_partition_pruning, prune_partitions};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use url::Url;

use crate::Read;

mod pruning;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to the Delta Lake Table.\nVerify the Delta Lake Table configuration is valid, and try again.\nReceived the following error while connecting: {source}"
    ))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display(
        "Delta Lake Table checkpoint files are missing or incorrect.\nRecreate the checkpoint for the Delta Lake Table and try again.\n{source}"
    ))]
    DeltaCheckpointError { source: delta_kernel::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DeltaTableFactory {
    params: HashMap<String, SecretString>,
}

impl std::fmt::Debug for DeltaTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableFactory")
            .field("params", &self.params.keys())
            .finish_non_exhaustive()
    }
}

impl DeltaTableFactory {
    #[must_use]
    pub fn new(params: HashMap<String, SecretString>) -> Self {
        Self { params }
    }
}

#[async_trait]
impl Read for DeltaTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let delta_path = table_reference.table().to_string();
        let delta: DeltaTable = DeltaTable::from(delta_path, self.params.clone()).boxed()?;
        Ok(Arc::new(delta))
    }
}

#[derive(Debug)]
pub struct DeltaTable {
    table: Table,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    arrow_schema: SchemaRef,
    delta_schema: delta_kernel::schema::SchemaRef,
}

impl DeltaTable {
    pub fn from(table_location: String, options: HashMap<String, SecretString>) -> Result<Self> {
        let table = Table::try_from_uri(ensure_folder_location(table_location))
            .map_err(handle_delta_error)?;

        let mut storage_options: HashMap<String, String> = HashMap::new();
        for (key, value) in options {
            match key.as_ref() {
                "token" | "endpoint" => {}
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.expose_secret().to_string());
                }
                _ => {
                    storage_options.insert(key.to_string(), value.expose_secret().to_string());
                }
            }
        }

        let mut load_credentials_from_environment = true;
        if let (Some(_), Some(_)) = (
            storage_options.get("aws_access_key_id"),
            storage_options.get("aws_secret_access_key"),
        ) {
            load_credentials_from_environment = false;
        }

        let table_object_store = match (
            load_credentials_from_environment,
            object_store_aws_sdk::get_sdk_config(),
        ) {
            (true, Some(sdk_config)) => {
                let region = storage_options.get("aws_region").map(ToString::to_string);
                object_store_aws_sdk::from_s3_url_and_config(table.location(), region, sdk_config)
                    .ok()
            }
            _ => None,
        };

        let engine = match table_object_store {
            Some(object_store) => Arc::new(DefaultEngine::new(
                object_store.into(),
                Arc::new(TokioBackgroundExecutor::new()),
            )),
            None => Arc::new(
                DefaultEngine::try_new(
                    table.location(),
                    storage_options,
                    Arc::new(TokioBackgroundExecutor::new()),
                )
                .map_err(handle_delta_error)?,
            ),
        };

        let snapshot = table
            .snapshot(engine.as_ref(), None)
            .map_err(handle_delta_error)?;

        let arrow_schema = Self::get_schema(&snapshot);
        let delta_schema = snapshot.schema();

        Ok(Self {
            table,
            engine,
            arrow_schema: Arc::new(arrow_schema),
            delta_schema,
        })
    }

    fn get_schema(snapshot: &Snapshot) -> Schema {
        let schema = snapshot.schema();

        let mut fields: Vec<Field> = vec![];
        for field in schema.fields() {
            fields.push(Field::new(
                field.name(),
                map_delta_data_type_to_arrow_data_type(&field.data_type),
                field.nullable,
            ));
        }

        Schema::new(fields)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_parquet_exec(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        schema: &Arc<Schema>,
        partition_cols: &[Field],
        parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
        partitioned_files: &[PartitionedFile],
        physical_expr: &Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn ExecutionPlan> {
        // this is needed to pass the plan_extension
        let projection = Some(
            projection
                .cloned()
                .unwrap_or((0..self.arrow_schema.fields().len()).collect::<Vec<_>>()),
        );

        let new_projections = projection.map(|projection| {
            projection
                .iter()
                .map(|&x| {
                    let field = self.arrow_schema.field(x);

                    if let Ok(i) = schema.index_of(field.name()) {
                        return i;
                    }

                    if let Some(i) = partition_cols.iter().position(|r| r == field) {
                        return schema.fields.len() + i;
                    }

                    unreachable!("all projected fields should be mapped to new projected position");
                })
                .collect::<Vec<_>>()
        });
        let parquet_source = ParquetSource::new(TableParquetOptions::default())
            .with_parquet_file_reader_factory(Arc::clone(parquet_file_reader_factory))
            .with_predicate(Arc::clone(schema), Arc::clone(physical_expr));

        let file_scan_config_builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::clone(schema),
            Arc::new(parquet_source),
        )
        .with_limit(limit)
        .with_projection(new_projections)
        .with_table_partition_cols(partition_cols.to_vec())
        .with_file_group(FileGroup::new(partitioned_files.to_vec()));

        DataSourceExec::from_data_source(file_scan_config_builder.build())
    }
}

fn ensure_folder_location(table_location: String) -> String {
    if table_location.ends_with('/') {
        table_location
    } else {
        format!("{table_location}/")
    }
}

#[allow(clippy::cast_possible_wrap)]
fn map_delta_data_type_to_arrow_data_type(
    delta_data_type: &delta_kernel::schema::DataType,
) -> DataType {
    match delta_data_type {
        delta_kernel::schema::DataType::Primitive(primitive_type) => match primitive_type {
            delta_kernel::schema::PrimitiveType::String => DataType::Utf8,
            delta_kernel::schema::PrimitiveType::Long => DataType::Int64,
            delta_kernel::schema::PrimitiveType::Integer => DataType::Int32,
            delta_kernel::schema::PrimitiveType::Short => DataType::Int16,
            delta_kernel::schema::PrimitiveType::Byte => DataType::Int8,
            delta_kernel::schema::PrimitiveType::Float => DataType::Float32,
            delta_kernel::schema::PrimitiveType::Double => DataType::Float64,
            delta_kernel::schema::PrimitiveType::Boolean => DataType::Boolean,
            delta_kernel::schema::PrimitiveType::Binary => DataType::Binary,
            delta_kernel::schema::PrimitiveType::Date => DataType::Date32,
            delta_kernel::schema::PrimitiveType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            delta_kernel::schema::PrimitiveType::TimestampNtz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            delta_kernel::schema::PrimitiveType::Decimal(d) => {
                DataType::Decimal128(d.precision(), d.scale() as i8)
            }
        },
        delta_kernel::schema::DataType::Array(array_type) => DataType::List(Arc::new(Field::new(
            "item",
            map_delta_data_type_to_arrow_data_type(array_type.element_type()),
            array_type.contains_null(),
        ))),
        delta_kernel::schema::DataType::Struct(struct_type) => {
            let mut fields: Vec<Field> = vec![];
            for field in struct_type.fields() {
                fields.push(Field::new(
                    field.name(),
                    map_delta_data_type_to_arrow_data_type(field.data_type()),
                    field.nullable,
                ));
            }
            DataType::Struct(fields.into())
        }
        delta_kernel::schema::DataType::Map(map_type) => {
            let key_type = map_delta_data_type_to_arrow_data_type(map_type.key_type());
            let value_type = map_delta_data_type_to_arrow_data_type(map_type.value_type());
            DataType::Map(
                Arc::new(Field::new_struct(
                    map_type.type_name.clone(),
                    vec![
                        Arc::new(Field::new("key", key_type, false)),
                        Arc::new(Field::new(
                            "value",
                            value_type,
                            map_type.value_contains_null(),
                        )),
                    ],
                    false,
                )),
                false,
            )
        }
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    #[allow(clippy::too_many_lines)]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        let snapshot = self
            .table
            .snapshot(self.engine.as_ref(), None)
            .map_err(map_delta_error_to_datafusion_err)?;

        let df_schema = DFSchema::try_from(Arc::clone(&self.arrow_schema))?;

        let store = self
            .engine
            .get_object_store_for_url(self.table.location())
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Failed to get object store for table location".to_string(),
                )
            })?;
        let parquet_file_reader_factory = Arc::new(DefaultParquetFileReaderFactory::new(store))
            as Arc<dyn ParquetFileReaderFactory>;
        let projected_delta_schema = project_delta_schema(
            &self.arrow_schema,
            Arc::clone(&self.delta_schema),
            projection,
        );
        let engine = Arc::clone(&self.engine);

        // Clone the filters since we need to move them into the spawn_blocking closure
        let filters_clone = filters.to_vec();

        // The following Delta Lake scan is blocking - run it in a separate blocking task to prevent the Tokio runtime from starving
        let (scan_context, parquet_file_reader_factory, df_schema) =
            tokio::task::spawn_blocking(move || {
                // We'll convert all filters for delta_kernel predicates since
                // partition pruning is already handled separately later in the code

                let mut scan_builder =
                    ScanBuilder::new(Arc::new(snapshot)).with_schema(projected_delta_schema);

                // Convert and apply predicate if possible
                if let Some(predicate) = filters_to_delta_kernel_predicate(&filters_clone) {
                    tracing::debug!(
                        "Using delta_kernel predicate for filter pushdown: {predicate:?}"
                    );
                    scan_builder = scan_builder.with_predicate(Some(Arc::new(predicate)));
                }

                let scan = scan_builder
                    .build()
                    .map_err(map_delta_error_to_datafusion_err)?;

                let table_root = scan.table_root();
                let mut scan_context = ScanContext::new(Arc::clone(&engine), table_root.clone());

                let scan_iter = scan
                    .scan_metadata(engine.as_ref())
                    .map_err(map_delta_error_to_datafusion_err)?;

                for scan_result in scan_iter {
                    let scan = scan_result.map_err(map_delta_error_to_datafusion_err)?;
                    scan_context = scan
                        .visit_scan_files(scan_context, handle_scan_file)
                        .map_err(map_delta_error_to_datafusion_err)?;
                }

                Ok::<_, datafusion::error::DataFusionError>((
                    scan_context,
                    parquet_file_reader_factory,
                    df_schema,
                ))
            })
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Delta Scan panicked: {e}"))
            })??;

        if let Some(err) = scan_context.errs.into_iter().next() {
            return Err(err);
        }

        // In Delta Lake, all files must have the same partition columns,
        // but Delta allows NULL values for the partition columns, represented in the filesystem as `__HIVE_DEFAULT_PARTITION__`.
        //
        // user_id=__HIVE_DEFAULT_PARTITION__/
        //   day=2024-01-01/
        //     part-00000.parquet
        // user_id=123/
        //   day=2024-01-01/
        //     part-00001.parquet
        //
        // In the above example, the partition columns are `user_id` and `day`.
        // The `user_id` column has a NULL value for the first file and a value of `123` for the second file.
        //
        // The `delta_kernel` library skips returning the partition columns for files that have a NULL value for the partition columns.
        // Which means that the partition columns will not be returned in the `partition_values` field of the `PartitionedFile` object.
        // We handle this by keeping track of all the partition columns we find in the `all_partition_columns` variable and if one
        // doesn't have a value, we add a NULL value for that field to the `partition_values` field of the `PartitionedFile` object.
        let mut partitioned_files: Vec<PartitionedFile> = vec![];
        let all_partition_columns = scan_context
            .files
            .iter()
            .flat_map(|file| {
                file.partition_values.iter().filter_map(|(k, _)| {
                    let schema = self.schema();
                    schema.field_with_name(k).ok().cloned()
                })
            })
            // Use an IndexMap to preserve insertion order
            .fold(IndexMap::new(), |mut acc, field| {
                acc.insert(field, ());
                acc
            });
        for file in scan_context.files {
            let mut partitioned_file = file.partitioned_file;
            partitioned_file.partition_values = all_partition_columns
                .iter()
                .map(|(field, ())| {
                    if let Some((_, value)) = file
                        .partition_values
                        .iter()
                        .find(|(k, _)| *k == field.name())
                    {
                        ScalarValue::try_from_string(value.clone(), field.data_type())
                    } else {
                        // This will create a null value typed for the field
                        Ok(ScalarValue::try_from(field.data_type())?)
                    }
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;

            // If there is a selection vector, create a ParquetAccessPlan that will be used to skip rows based on the selection vector
            if let Some(selection_vector) = file.selection_vector {
                let access_plan = get_parquet_access_plan(
                    &parquet_file_reader_factory,
                    &partitioned_file,
                    selection_vector,
                )
                .await?;
                partitioned_file = partitioned_file.with_extensions(Arc::new(access_plan));
            }

            partitioned_files.push(partitioned_file);
        }

        let partition_cols = all_partition_columns
            .into_iter()
            .map(|(field, ())| field)
            .collect::<Vec<_>>();

        let table_partition_col_names = partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        // Split the filters into partition filters and the rest
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });
        tracing::trace!("partition_filters: {partition_filters:?}");
        tracing::trace!("filters: {filters:?}");

        let num_partition_files = partitioned_files.len();
        let filtered_partitioned_files =
            prune_partitions(partitioned_files, &partition_filters, &partition_cols)?;

        tracing::debug!(
            "Partition pruning yielded {} files (out of {num_partition_files})",
            filtered_partitioned_files.len(),
        );

        let filter = conjunction(filters).unwrap_or_else(|| lit(true));
        let physical_expr = state.create_physical_expr(filter, &df_schema)?;

        let schema = self.arrow_schema.project(
            &self
                .arrow_schema
                .fields
                .iter()
                .enumerate()
                .filter_map(|(i, f)| (!partition_cols.contains(f)).then_some(i))
                .collect::<Vec<_>>(),
        )?;

        Ok(self.create_parquet_exec(
            projection,
            limit,
            &Arc::new(schema),
            &partition_cols,
            &parquet_file_reader_factory,
            &filtered_partitioned_files,
            &physical_expr,
        ))
    }
}

struct ScanContext {
    pub errs: Vec<datafusion::error::DataFusionError>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    pub files: Vec<PartitionFileContext>,
    table_root: Url,
}

impl ScanContext {
    fn new(engine: Arc<DefaultEngine<TokioBackgroundExecutor>>, table_root: Url) -> Self {
        Self {
            engine,
            errs: Vec::new(),
            files: Vec::new(),
            table_root,
        }
    }
}

fn project_delta_schema(
    arrow_schema: &SchemaRef,
    schema: delta_kernel::schema::SchemaRef,
    projections: Option<&Vec<usize>>,
) -> delta_kernel::schema::SchemaRef {
    if let Some(projections) = projections {
        let projected_fields = projections
            .iter()
            .filter_map(|i| schema.field(arrow_schema.field(*i).name()))
            .cloned()
            .collect::<Vec<_>>();
        Arc::new(delta_kernel::schema::Schema::new(projected_fields))
    } else {
        schema
    }
}

struct PartitionFileContext {
    partitioned_file: PartitionedFile,
    selection_vector: Option<Vec<bool>>,
    partition_values: HashMap<String, String>,

    /// These are transforms that Delta wants to apply to the physical data read from the Parquet files.
    /// Currently this is only used for adding partition columns and mapping the columns read from the Parquet files
    /// into the correct place in the output schema.
    ///
    /// Both of these functions are already handled for us by the `DataFusion` `ParquetExec`. However, we may need to
    /// revisit this if more complex transformations are required.
    ///
    /// See: <https://github.com/delta-io/delta-kernel-rs/blob/7e62d12def00f248eccef23e7672fd4db553274f/kernel/src/scan/mod.rs#L444>
    _transform: Option<ExpressionRef>,
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
fn handle_scan_file(
    scan_context: &mut ScanContext,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    partition_values: HashMap<String, String>,
) {
    let root_url = &scan_context.table_root;

    let path = if root_url.path().ends_with('/') {
        format!("{}{}", root_url.path(), path)
    } else {
        format!("{}/{}", root_url.path(), path)
    };

    let partitioned_file_path = match object_store::path::Path::from_url_path(&path) {
        Ok(path) => path,
        Err(e) => {
            scan_context
                .errs
                .push(datafusion::error::DataFusionError::Execution(format!(
                    "Error parsing file path: {e}",
                )));
            return;
        }
    };

    tracing::trace!("partitioned_file_path: {partitioned_file_path:?}");

    let partitioned_file_object_meta = ObjectMeta {
        location: partitioned_file_path,
        last_modified: chrono::Utc.timestamp_nanos(0),
        size: size as u64,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile::from(partitioned_file_object_meta);

    // Get the selection vector (i.e. inverse deletion vector)
    let selection_vector =
        match dv_info.get_selection_vector(scan_context.engine.as_ref(), root_url) {
            Ok(selection_vector) => selection_vector,
            Err(e) => {
                scan_context
                    .errs
                    .push(datafusion::error::DataFusionError::Execution(format!(
                        "Error getting selection vector: {e}",
                    )));
                return;
            }
        };

    scan_context.files.push(PartitionFileContext {
        partitioned_file,
        selection_vector,
        partition_values,
        _transform: transform,
    });
}

fn map_delta_error_to_datafusion_err(e: delta_kernel::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn get_row_group_access(
    selection_vector: &[bool],
    row_group_row_start: usize,
    row_group_num_rows: usize,
) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| !x)
    {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| x)
    {
        return RowGroupAccess::Scan;
    }

    let mask =
        selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows].to_vec();

    // If some rows are deleted, get a row selection that skips the deleted rows
    let row_selection = RowSelection::from_filters(&[mask.into()]);
    RowGroupAccess::Selection(row_selection)
}

fn get_full_selection_vector(selection_vector: &[bool], total_rows: usize) -> Vec<bool> {
    let mut new_selection_vector = vec![true; total_rows];
    let copy_len = std::cmp::min(selection_vector.len(), total_rows);
    new_selection_vector[..copy_len].copy_from_slice(&selection_vector[..copy_len]);
    new_selection_vector
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
async fn get_parquet_access_plan(
    parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: &PartitionedFile,
    selection_vector: Vec<bool>,
) -> Result<ParquetAccessPlan, datafusion::error::DataFusionError> {
    let mut parquet_file_reader = parquet_file_reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        &ExecutionPlanMetricsSet::new(),
    )?;

    let parquet_metadata = parquet_file_reader.get_metadata(None).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error getting parquet metadata: {e}"
        ))
    })?;

    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for (i, row_group) in parquet_metadata.row_groups().iter().enumerate() {
        // If all rows in the row group are deleted, skip the row group
        tracing::debug!(
            "Row group {i} num_rows={} row_group_row_start={row_group_row_start}",
            row_group.num_rows()
        );
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    tracing::debug!("Created ParquetAccessPlan with {row_groups:?}");
    Ok(ParquetAccessPlan::new(row_groups))
}

/// Convert a `DataFusion` filter expression to a `delta_kernel` expression
#[allow(clippy::too_many_lines)]
#[allow(
    deprecated,
    reason = "Needed to exhaustively match on all expression types"
)]
fn to_delta_kernel_expr(expr: &Expr) -> Option<Expression> {
    match expr {
        Expr::BinaryExpr(binary) => {
            let left = to_delta_kernel_expr(&binary.left)?;
            let right = to_delta_kernel_expr(&binary.right)?;

            Some(to_delta_kernel_binary_expression(binary.op, left, right)?)
        }
        Expr::Column(col) => {
            let field_names = vec![col.name.as_str()];
            Some(Expression::column(field_names))
        }
        Expr::Literal(value) => Some(Expression::literal(to_delta_kernel_scalar(value.clone())?)),
        Expr::IsNull(expr) => {
            let expr = to_delta_kernel_expr(expr)?;
            Some(Expression::is_null(expr).into())
        }
        Expr::IsNotNull(expr) => {
            let expr = to_delta_kernel_expr(expr)?;
            Some(Expression::is_not_null(expr).into())
        }
        Expr::Not(expr) => {
            let expr = into_predicate(to_delta_kernel_expr(expr)?)?;
            Some(Predicate::not(expr).into())
        }
        Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::ScalarFunction(_)
        | Expr::Alias(_)
        | Expr::ScalarVariable(_, _)
        | Expr::ScalarSubquery(_)
        | Expr::InSubquery(_)
        | Expr::Exists(_)
        | Expr::Wildcard { .. }
        | Expr::Unnest { .. }
        | Expr::OuterReferenceColumn(_, _)
        | Expr::AggregateFunction { .. }
        | Expr::WindowFunction { .. }
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::GroupingSet(_)
        | Expr::Placeholder(_) => {
            // Other expression types are not supported for Delta kernel predicates
            None
        }
    }
}

fn into_predicate(expr: Expression) -> Option<Predicate> {
    match expr {
        Expression::Predicate(predicate) => Some(*predicate),
        _ => None,
    }
}

fn to_delta_kernel_binary_expression(
    op: Operator,
    left: Expression,
    right: Expression,
) -> Option<Expression> {
    match op {
        Operator::Plus => Some(Expression::binary(BinaryExpressionOp::Plus, left, right)),
        Operator::Minus => Some(Expression::binary(BinaryExpressionOp::Minus, left, right)),
        Operator::Multiply => Some(Expression::binary(
            BinaryExpressionOp::Multiply,
            left,
            right,
        )),
        Operator::Divide => Some(Expression::binary(BinaryExpressionOp::Divide, left, right)),
        Operator::Lt => Some(Predicate::lt(left, right).into()),
        Operator::LtEq => Some(Predicate::le(left, right).into()),
        Operator::Gt => Some(Predicate::gt(left, right).into()),
        Operator::GtEq => Some(Predicate::ge(left, right).into()),
        Operator::Eq => Some(Predicate::eq(left, right).into()),
        Operator::NotEq => Some(Predicate::ne(left, right).into()),
        Operator::And => Some(Predicate::and(into_predicate(left)?, into_predicate(right)?).into()),
        Operator::Or => Some(Predicate::or(into_predicate(left)?, into_predicate(right)?).into()),
        Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch
        | Operator::LikeMatch
        | Operator::ILikeMatch
        | Operator::NotLikeMatch
        | Operator::NotILikeMatch
        | Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft
        | Operator::StringConcat
        | Operator::AtArrow
        | Operator::ArrowAt
        | Operator::Arrow
        | Operator::LongArrow
        | Operator::HashArrow
        | Operator::Modulo
        | Operator::HashLongArrow
        | Operator::AtAt
        | Operator::IntegerDivide
        | Operator::HashMinus
        | Operator::AtQuestion
        | Operator::Question
        | Operator::QuestionAnd
        | Operator::QuestionPipe => None,
    }
}

#[allow(clippy::cast_sign_loss)]
#[allow(clippy::too_many_lines)]
fn to_delta_kernel_scalar(scalar: ScalarValue) -> Option<Scalar> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(Scalar::Byte(v)),
        ScalarValue::Int8(None) => Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
            PrimitiveType::Byte,
        ))),
        ScalarValue::UInt8(Some(v)) => Some(Scalar::Short(i16::from(v))),
        ScalarValue::Int16(Some(v)) => Some(Scalar::Short(v)),
        ScalarValue::UInt8(None) | ScalarValue::Int16(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Short),
        )),
        ScalarValue::Int32(Some(v)) => Some(Scalar::Integer(v)),
        ScalarValue::UInt16(Some(v)) => Some(Scalar::Integer(i32::from(v))),
        ScalarValue::UInt16(None) | ScalarValue::Int32(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer),
        )),
        ScalarValue::Int64(Some(v)) => Some(Scalar::Long(v)),
        ScalarValue::UInt32(Some(v)) => Some(Scalar::Long(i64::from(v))),
        ScalarValue::UInt64(Some(v)) => {
            if let Ok(v) = i64::try_from(v) {
                Some(Scalar::Long(v))
            } else {
                None // Cannot represent u64 > i64::MAX in delta_kernel
            }
        }
        ScalarValue::UInt64(None) | ScalarValue::UInt32(None) | ScalarValue::Int64(None) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::Long,
            )))
        }
        ScalarValue::Boolean(Some(v)) => Some(Scalar::Boolean(v)),
        ScalarValue::Boolean(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Boolean),
        )),
        ScalarValue::Float16(Some(v)) => Some(Scalar::Float(f32::from(v))),
        ScalarValue::Float32(Some(v)) => Some(Scalar::Float(v)),
        ScalarValue::Float16(None) | ScalarValue::Float32(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Float),
        )),
        ScalarValue::Float64(Some(v)) => Some(Scalar::Double(v)),
        ScalarValue::Float64(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Double),
        )),
        ScalarValue::Decimal128(Some(v), p, s) => Some(Scalar::Decimal(
            DecimalData::try_new(v, DecimalType::try_new(p, s as u8).ok()?).ok()?,
        )),
        ScalarValue::Decimal128(None, p, s) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::Decimal(DecimalType::try_new(p, s as u8).ok()?),
            )))
        }
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Some(Scalar::String(v)),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::String,
            )))
        }
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Some(Scalar::Binary(v)),
        ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::FixedSizeBinary(_, None)
        | ScalarValue::LargeBinary(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Binary),
        )),
        ScalarValue::Date32(Some(v)) => Some(Scalar::Date(v)),
        ScalarValue::Date32(None) | ScalarValue::Date64(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Date),
        )),
        ScalarValue::Date64(Some(v)) => {
            // Convert milliseconds to days since epoch
            let days = v / (24 * 60 * 60 * 1000);
            if let Ok(days) = i32::try_from(days) {
                Some(Scalar::Date(days))
            } else {
                None
            }
        }
        ScalarValue::TimestampSecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v * 1_000_000)), // Convert to microseconds
        ScalarValue::TimestampSecond(Some(v), None) => Some(Scalar::TimestampNtz(v * 1_000_000)), // Convert to microseconds
        ScalarValue::TimestampMillisecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v * 1000)), // Convert to microseconds
        ScalarValue::TimestampMillisecond(Some(v), None) => Some(Scalar::TimestampNtz(v * 1000)), // Convert to microseconds
        ScalarValue::TimestampMicrosecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v)),
        ScalarValue::TimestampMicrosecond(Some(v), None) => Some(Scalar::TimestampNtz(v)),
        ScalarValue::TimestampNanosecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v / 1000)), // Convert to microseconds
        ScalarValue::TimestampNanosecond(Some(v), None) => Some(Scalar::TimestampNtz(v / 1000)), // Convert to microseconds
        ScalarValue::TimestampSecond(None, Some(_))
        | ScalarValue::TimestampMillisecond(None, Some(_))
        | ScalarValue::TimestampMicrosecond(None, Some(_))
        | ScalarValue::TimestampNanosecond(None, Some(_)) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Timestamp),
        )),
        ScalarValue::TimestampSecond(None, None)
        | ScalarValue::TimestampMillisecond(None, None)
        | ScalarValue::TimestampMicrosecond(None, None)
        | ScalarValue::TimestampNanosecond(None, None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::TimestampNtz),
        )),
        ScalarValue::Null
        | ScalarValue::Decimal256(_, _, _)
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Time32Second(_)
        | ScalarValue::Time32Millisecond(_)
        | ScalarValue::Time64Microsecond(_)
        | ScalarValue::Time64Nanosecond(_)
        | ScalarValue::IntervalYearMonth(_)
        | ScalarValue::IntervalDayTime(_)
        | ScalarValue::IntervalMonthDayNano(_)
        | ScalarValue::DurationSecond(_)
        | ScalarValue::DurationMillisecond(_)
        | ScalarValue::DurationMicrosecond(_)
        | ScalarValue::DurationNanosecond(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::Dictionary(_, _) => None,
    }
}

/// Convert a list of `DataFusion` filter expressions to a single `delta_kernel` expression
///
/// This function processes multiple `DataFusion` expressions and returns a predicate for `delta_kernel`.
fn filters_to_delta_kernel_predicate(filters: &[Expr]) -> Option<Predicate> {
    if filters.is_empty() {
        return None;
    }

    let mut predicates = Vec::new();
    for filter in filters {
        if let Some(expr) = to_delta_kernel_expr(filter) {
            predicates.push(expr);
        }
    }

    if predicates.is_empty() {
        None
    } else if predicates.len() == 1 {
        let expr = predicates.pop()?;
        Some(into_predicate(expr)?)
    } else {
        // Multiple predicates are present, so we need to combine them using an AND operation
        let predicates = predicates
            .into_iter()
            .filter_map(into_predicate)
            .collect::<Vec<_>>();
        Some(Predicate::and_from(predicates))
    }
}

fn handle_delta_error(delta_error: delta_kernel::Error) -> Error {
    match delta_error {
        delta_kernel::Error::InvalidCheckpoint(_) => Error::DeltaCheckpointError {
            source: delta_error,
        },
        _ => Error::DeltaTableError {
            source: delta_error,
        },
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::{Operator, col, lit, not};
    use datafusion::parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::similar_names)]
    fn test_to_delta_kernel_expr() {
        // Test basic column reference
        let col_expr = col("name");
        let dk_expr = to_delta_kernel_expr(&col_expr);
        assert!(dk_expr.is_some(), "Column expression should be supported");

        // Test basic literal
        let lit_expr = lit("value");
        let dk_expr = to_delta_kernel_expr(&lit_expr);
        assert!(dk_expr.is_some(), "Literal expression should be supported");

        // Test comparison operators
        // Equality
        let eq_expr = col("age").eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&eq_expr);
        assert!(dk_expr.is_some(), "Equality expression should be supported");

        // Less than
        let lt_expr = col("age").lt(lit(30));
        let dk_expr = to_delta_kernel_expr(&lt_expr);
        assert!(
            dk_expr.is_some(),
            "Less than expression should be supported"
        );

        // Greater than
        let gt_expr = col("age").gt(lit(30));
        let dk_expr = to_delta_kernel_expr(&gt_expr);
        assert!(
            dk_expr.is_some(),
            "Greater than expression should be supported"
        );

        // Less than or equal
        let lte_expr = col("age").lt_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&lte_expr);
        assert!(
            dk_expr.is_some(),
            "Less than or equal expression should be supported"
        );

        // Greater than or equal
        let gte_expr = col("age").gt_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&gte_expr);
        assert!(
            dk_expr.is_some(),
            "Greater than or equal expression should be supported"
        );

        // Not equal
        let neq_expr = col("age").not_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&neq_expr);
        assert!(
            dk_expr.is_some(),
            "Not equal expression should be supported"
        );

        // Test arithmetic operators using binary expressions directly
        // Addition
        let add_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Plus,
            Box::new(lit(5)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(add_expr));
        assert!(dk_expr.is_some(), "Addition expression should be supported");

        // Subtraction
        let sub_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Minus,
            Box::new(lit(5)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(sub_expr));
        assert!(
            dk_expr.is_some(),
            "Subtraction expression should be supported"
        );

        // Multiplication
        let mul_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Multiply,
            Box::new(lit(2)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(mul_expr));
        assert!(
            dk_expr.is_some(),
            "Multiplication expression should be supported"
        );

        // Division
        let div_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Divide,
            Box::new(lit(2)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(div_expr));
        assert!(dk_expr.is_some(), "Division expression should be supported");

        // Test null check
        let is_null_expr = col("optional_field").is_null();
        let dk_expr = to_delta_kernel_expr(&is_null_expr);
        assert!(dk_expr.is_some(), "IsNull expression should be supported");

        // Test is_not_null
        let is_not_null_expr = col("required_field").is_not_null();
        let dk_expr = to_delta_kernel_expr(&is_not_null_expr);
        assert!(
            dk_expr.is_some(),
            "IsNotNull expression should be supported"
        );

        // Test NOT expression
        let not_expr = not(col("active").eq(lit(false)));
        let dk_expr = to_delta_kernel_expr(&not_expr);
        assert!(dk_expr.is_some(), "Not expression should be supported");

        // Test unsupported expressions
        let case_expr = datafusion::logical_expr::case(col("status"))
            .when(lit("active"), lit(1))
            .otherwise(lit(0))
            .expect("Failed to create case expression");
        let dk_expr = to_delta_kernel_expr(&case_expr);
        assert!(
            dk_expr.is_none(),
            "CASE expressions should not be supported"
        );

        let in_list_expr = datafusion::logical_expr::in_list(
            col("status"),
            vec![lit("active"), lit("pending")],
            false,
        );
        let dk_expr = to_delta_kernel_expr(&in_list_expr);
        assert!(
            dk_expr.is_none(),
            "IN LIST expressions should not be supported"
        );

        let alias_expr = col("age").alias("years");
        let dk_expr = to_delta_kernel_expr(&alias_expr);
        assert!(
            dk_expr.is_none(),
            "ALIAS expressions should not be supported"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_to_delta_kernel_scalar() {
        // Test string scalar
        let scalar = ScalarValue::Utf8(Some("test".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert string scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "test"));

        // Test other string types
        let scalar = ScalarValue::Utf8View(Some("test_view".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Utf8View scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "test_view"));

        let scalar = ScalarValue::LargeUtf8(Some("large_test".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert LargeUtf8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "large_test"));

        // Test integer scalars
        let scalar = ScalarValue::Int8(Some(8));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Byte(v) if v == 8));

        let scalar = ScalarValue::Int16(Some(16));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int16 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Short(v) if v == 16));

        let scalar = ScalarValue::Int32(Some(32));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Integer(v) if v == 32));

        let scalar = ScalarValue::Int64(Some(64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 64));

        // Test unsigned integer conversion
        let scalar = ScalarValue::UInt8(Some(8));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Short(v) if v == 8));

        let scalar = ScalarValue::UInt16(Some(16));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt16 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Integer(v) if v == 16));

        let scalar = ScalarValue::UInt32(Some(32));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 32));

        let scalar = ScalarValue::UInt64(Some(64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 64));

        // Test large UInt64 conversion (edge case)
        let max_i64 = i64::MAX as u64;
        let scalar = ScalarValue::UInt64(Some(max_i64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert max UInt64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == i64::MAX));

        // Test UInt64 that's too large to fit in i64 (should return None)
        let too_large = (i64::MAX as u64) + 1;
        let scalar = ScalarValue::UInt64(Some(too_large));
        let dk_scalar = to_delta_kernel_scalar(scalar);
        assert!(dk_scalar.is_none());

        // Test float scalars without Float16 (not available in this crate)
        let scalar = ScalarValue::Float32(Some(32.5));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Float32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Float(v) if (v - 32.5).abs() < f32::EPSILON));

        let scalar = ScalarValue::Float64(Some(64.5));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Float64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Double(v) if (v - 64.5).abs() < f64::EPSILON));

        // Test boolean scalar
        let scalar = ScalarValue::Boolean(Some(true));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Boolean scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Boolean(v) if v));

        // Test null scalars
        let scalar = ScalarValue::Int32(None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int32 null scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Null(dt) if matches!(dt, delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer)))
        );

        let scalar = ScalarValue::Utf8(None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Utf8 null scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Null(dt) if matches!(dt, delta_kernel::schema::DataType::Primitive(PrimitiveType::String)))
        );

        // Test timestamp scalar with different time units
        let scalar = ScalarValue::TimestampSecond(Some(10), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampSecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 10_000_000)); // Converted to microseconds

        let scalar = ScalarValue::TimestampMillisecond(Some(10_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampMillisecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 10_000_000)); // Converted to microseconds

        let scalar = ScalarValue::TimestampMicrosecond(Some(1_000_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampMicrosecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 1_000_000));

        let scalar = ScalarValue::TimestampNanosecond(Some(1_000_000_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampNanosecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 1_000_000)); // Converted to microseconds

        // Test timestamp with timezone
        let scalar = ScalarValue::TimestampMicrosecond(Some(1_000_000), Some("UTC".into()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Timestamp with timezone scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Timestamp(v) if v == 1_000_000));

        // Test decimal scalar
        let scalar = ScalarValue::Decimal128(Some(1234), 10, 2);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Decimal128 scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Decimal(v) if v == DecimalData::try_new(1234, DecimalType::try_new(10, 2).expect("valid decimal")).expect("valid decimal"))
        );

        // Test binary data
        let binary_data = vec![1, 2, 3, 4];
        let scalar = ScalarValue::Binary(Some(binary_data.clone()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Binary scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Binary(v) if v == binary_data));

        // Test Date32
        let scalar = ScalarValue::Date32(Some(18000)); // Some number of days since epoch
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Date32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Date(v) if v == 18000));

        // Test Date64
        let days = 100;
        let millis = i64::from(days) * 24 * 60 * 60 * 1000;
        let scalar = ScalarValue::Date64(Some(millis));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Date64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Date(v) if v == days));

        // Test unsupported types (we don't need to test the exact construction since we only care about the return value)
        let dk_scalar = to_delta_kernel_scalar(ScalarValue::Null);
        assert!(dk_scalar.is_none());
    }

    #[test]
    fn test_filters_to_delta_kernel_expr() {
        // Test empty filters
        let filters: Vec<Expr> = vec![];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_none(), "Empty filters should return None");

        // Test single filter (equality)
        let filters = vec![col("age").eq(lit(30))];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_some(), "Single filter should be converted");

        // Test multiple filters
        let filters = vec![col("age").gt(lit(20)), col("name").eq(lit("John"))];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Multiple filters should be converted to a single expression"
        );

        // Test filters with unsupported expressions
        let case_expr = datafusion::logical_expr::case(col("status"))
            .when(lit("active"), lit(1))
            .otherwise(lit(0))
            .expect("Failed to create case expression for unsupported expressions test");

        let filters = vec![col("age").gt(lit(20)), case_expr.clone()];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Mix of supported and unsupported filters should return the supported ones"
        );

        // Test filters with only unsupported expressions
        let filters = vec![case_expr.clone()];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_none(),
            "Only unsupported filters should return None"
        );

        // Test with multiple unsupported expressions
        let filters = vec![
            case_expr,
            datafusion::logical_expr::in_list(
                col("status"),
                vec![lit("active"), lit("pending")],
                false,
            ),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_none(),
            "Multiple unsupported filters should return None"
        );

        // Test AND variadic operator
        let filters = vec![
            col("age").gt(lit(20)).and(col("name").eq(lit("John"))),
            col("active").eq(lit(true)),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "AND variadic operator should be supported"
        );

        // Test OR variadic operator
        let filters = vec![
            col("age").gt(lit(20)).or(col("name").eq(lit("John"))),
            col("active").eq(lit(true)),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "OR variadic operator should be supported"
        );

        // Test nested variadic operators
        let filters = vec![
            col("age")
                .gt(lit(20))
                .and(col("name").eq(lit("John")))
                .or(col("active").eq(lit(true))),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Nested variadic operators should be supported"
        );
    }

    #[test]
    fn test_complex_filters_to_delta_kernel_expr() {
        // Test simple comparison expressions
        let filter = col("category").eq(lit("electronics"));
        let filters = vec![filter];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Simple equality expression should be supported"
        );

        // Test NOT expressions
        let filter = not(col("deleted").eq(lit(true)));
        let filters = vec![filter];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_some(), "NOT expression should be supported");
    }

    #[test]
    fn test_get_row_group_access() {
        // Test case where all rows are selected (should use Scan)
        let selection_vector = &[true, true, true, true, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);
        assert_eq!(row_group_access, RowGroupAccess::Scan);

        // Test case where all rows are deleted (should use Skip)
        let selection_vector = &[false, false, false, false, false];
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);
        assert_eq!(row_group_access, RowGroupAccess::Skip);

        // Test case with mixed selection (should use Selection)
        let selection_vector = &[true, true, true, false, true];
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        // Expected selection should have:
        // - Select first 3 rows
        // - Skip 1 row
        // - Select 1 row
        let selectors = vec![
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(1),
        ];
        assert_eq!(
            row_group_access,
            RowGroupAccess::Selection(selectors.into())
        );
    }

    #[test]
    fn test_get_row_group_access_with_offset() {
        // Test with offset starting row
        // Full selection vector: [true, true, true, true, true, false, false, false, true, true]
        let selection_vector = &[
            true, true, true, true, true, false, false, false, true, true,
        ];
        let row_group_row_start = 5; // Start at index 5
        let row_group_num_rows = 5; // Take 5 rows (5-9)

        // The selection should consider rows 5-9: [false, false, false, true, true]
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        // Expected selectors:
        // - Skip first 3 rows (false, false, false)
        // - Select last 2 rows (true, true)
        let selectors = vec![RowSelector::skip(3), RowSelector::select(2)];
        assert_eq!(
            row_group_access,
            RowGroupAccess::Selection(selectors.into())
        );
    }

    #[test]
    fn test_get_full_selection_vector() {
        // Test expanding a shorter selection vector to a longer one
        let selection_vector = &[true, false, true];
        let total_rows = 5;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should copy the provided values and fill the rest with true
        assert_eq!(full_vector, vec![true, false, true, true, true]);

        // Test truncating a longer selection vector to a shorter one
        let selection_vector = &[true, false, true, false, true];
        let total_rows = 3;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should only copy the first 3 values
        assert_eq!(full_vector, vec![true, false, true]);

        // Test with empty selection vector
        let selection_vector = &[];
        let total_rows = 3;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should create a vector of all true values
        assert_eq!(full_vector, vec![true, true, true]);
    }

    #[test]
    fn test_get_table_location() {
        // Test path with trailing slash (should remain unchanged)
        assert_eq!(
            ensure_folder_location("s3://my_bucket/".to_string()),
            "s3://my_bucket/"
        );

        // Test path without trailing slash (should add slash)
        assert_eq!(
            ensure_folder_location("s3://my_bucket".to_string()),
            "s3://my_bucket/"
        );

        // Test path with nested folders
        assert_eq!(
            ensure_folder_location("s3://my_bucket/data/table".to_string()),
            "s3://my_bucket/data/table/"
        );
    }
}
