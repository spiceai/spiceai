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
use datafusion::common::DFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{lit, Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::state::{DvInfo, GlobalScanState, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Table;
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
    #[snafu(display("Failed to connect to the Delta Lake Table.\nVerify the Delta Lake Table configuration is valid, and try again.\nReceived the following error while connecting: {source}"))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display("Delta Lake Table checkpoint files are missing or incorrect.\nRecreate the checkpoint for the Delta Lake Table and try again.\n{source}"))]
    DeltaCheckpointError { source: delta_kernel::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DeltaTableFactory {
    params: HashMap<String, SecretString>,
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
                "token" | "endpoint" => {
                    continue;
                }
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.expose_secret().clone());
                }
                _ => {
                    storage_options.insert(key.to_string(), value.expose_secret().clone());
                }
            }
        }

        let engine = Arc::new(
            DefaultEngine::try_new(
                table.location(),
                storage_options,
                Arc::new(TokioBackgroundExecutor::new()),
            )
            .map_err(handle_delta_error)?,
        );

        let snapshot = table
            .snapshot(engine.as_ref(), None)
            .map_err(handle_delta_error)?;

        let arrow_schema = Self::get_schema(&snapshot);
        let delta_schema = snapshot.schema().clone();

        Ok(Self {
            table,
            engine,
            arrow_schema: Arc::new(arrow_schema),
            delta_schema: Arc::new(delta_schema),
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
        let file_scan_config =
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), Arc::clone(schema))
                .with_limit(limit)
                .with_projection(new_projections)
                .with_table_partition_cols(partition_cols.to_vec())
                .with_file_group(partitioned_files.to_vec());
        let exec = ParquetExec::builder(file_scan_config)
            .with_parquet_file_reader_factory(Arc::clone(parquet_file_reader_factory))
            .with_predicate(Arc::clone(physical_expr))
            .build();

        Arc::new(exec)
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
            delta_kernel::schema::PrimitiveType::Decimal(p, s) => {
                DataType::Decimal128(*p, *s as i8)
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

        let scan = ScanBuilder::new(Arc::new(snapshot))
            .with_schema(projected_delta_schema)
            // technically filter can be converted into predicates but right now delta_kernel
            // ignores it
            .build()
            .map_err(map_delta_error_to_datafusion_err)?;
        let engine = Arc::clone(&self.engine);
        let scan_state = scan.global_scan_state();

        let mut scan_context = ScanContext::new(scan_state, Arc::clone(&self.engine));

        let scan_iter = scan
            .scan_data(engine.as_ref())
            .map_err(map_delta_error_to_datafusion_err)?;

        for scan_result in scan_iter {
            let data = scan_result.map_err(map_delta_error_to_datafusion_err)?;
            scan_context = delta_kernel::scan::state::visit_scan_files(
                data.0.as_ref(),
                data.1.as_ref(),
                scan_context,
                handle_scan_file,
            )
            .map_err(map_delta_error_to_datafusion_err)?;
        }

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
    scan_state: GlobalScanState,
    pub files: Vec<PartitionFileContext>,
}

impl ScanContext {
    fn new(
        scan_state: GlobalScanState,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    ) -> Self {
        Self {
            scan_state,
            engine,
            errs: Vec::new(),
            files: Vec::new(),
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
    partition_values: HashMap<String, String>,
) {
    let root_url = match Url::parse(&scan_context.scan_state.table_root) {
        Ok(url) => url,
        Err(e) => {
            scan_context
                .errs
                .push(datafusion::error::DataFusionError::Execution(format!(
                    "Error parsing table root URL: {e}",
                )));
            return;
        }
    };

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
        size: size as usize,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile::from(partitioned_file_object_meta);

    // Get the selection vector (i.e. inverse deletion vector)
    let selection_vector =
        match dv_info.get_selection_vector(scan_context.engine.as_ref(), &root_url) {
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
    new_selection_vector[..selection_vector.len()].copy_from_slice(selection_vector);
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

    let parquet_metadata = parquet_file_reader.get_metadata().await.map_err(|e| {
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
    use datafusion::parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    #[test]
    fn test_get_row_group_access() {
        let selection_vector = &[true, true, true, true, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        assert_eq!(row_group_access, RowGroupAccess::Scan);

        let selection_vector = &[false, false, false, false, false];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        assert_eq!(row_group_access, RowGroupAccess::Skip);

        let selection_vector = &[true, true, true, false, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

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
    fn test_get_table_location() {
        assert_eq!(
            ensure_folder_location("s3://my_bucket/".to_string()),
            "s3://my_bucket/"
        );
        assert_eq!(
            ensure_folder_location("s3://my_bucket".to_string()),
            "s3://my_bucket/"
        );
    }
}
