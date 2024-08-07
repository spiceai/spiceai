/*
Copyright 2024 The Spice.ai OSS Authors

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
use datafusion::common::DFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{lit, Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::state::{DvInfo, GlobalScanState, Stats};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Table;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use url::Url;

use crate::Read;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("An error occured with Delta Table: {source}"))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display("An error occured with handling Arrow data: {source}"))]
    ArrowError { source: arrow::error::ArrowError },
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

pub struct DeltaTable {
    table: Table,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    arrow_schema: SchemaRef,
    delta_schema: delta_kernel::schema::SchemaRef,
}

impl DeltaTable {
    pub fn from(table_location: String, options: HashMap<String, SecretString>) -> Result<Self> {
        let table =
            Table::try_from_uri(ensure_folder_location(table_location)).context(DeltaTableSnafu)?;

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
            .context(DeltaTableSnafu)?,
        );

        let snapshot = table
            .snapshot(engine.as_ref(), None)
            .context(DeltaTableSnafu)?;

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
            DataType::Dictionary(Box::new(key_type), Box::new(value_type))
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

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        let snapshot = self
            .table
            .snapshot(self.engine.as_ref(), None)
            .map_err(map_delta_error_to_datafusion_err)?;

        let df_schema = DFSchema::try_from(Arc::clone(&self.arrow_schema))?;

        let filter = conjunction(filters.to_vec()).unwrap_or_else(|| lit(true));
        let physical_expr = state.create_physical_expr(filter, &df_schema)?;

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

        let mut partitioned_files: Vec<PartitionedFile> = vec![];
        for file in scan_context.files {
            let mut partitioned_file = file.partitioned_file;

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

        // FileScanConfig requires an ObjectStoreUrl, but it isn't actually used because we pass in a ParquetFileReaderFactory
        // which specifies which object store to read from.
        let file_scan_config = FileScanConfig::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::clone(&self.arrow_schema),
        )
        .with_limit(limit)
        .with_projection(projection.cloned())
        .with_file_group(partitioned_files);
        let exec = ParquetExec::builder(file_scan_config)
            .with_parquet_file_reader_factory(Arc::clone(&parquet_file_reader_factory))
            .with_predicate(Arc::clone(&physical_expr))
            .build();

        Ok(Arc::new(exec))
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
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::cast_sign_loss)]
fn handle_scan_file(
    scan_context: &mut ScanContext,
    path: &str,
    size: i64,
    _stats: Option<Stats>,
    dv_info: DvInfo,
    _partition_values: HashMap<String, String>,
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
    let path = format!("{}/{path}", root_url.path());

    let partitioned_file = PartitionedFile::new(path.clone(), size as u64);

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
