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

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::common::{project_schema, DFSchema};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{
    FileScanConfig, ParquetExec, ParquetFileReaderFactory,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{lit, Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties,
};
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::state::{DvInfo, GlobalScanState};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Table;
use futures::StreamExt;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::fmt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::Sender;
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("An error occured with Delta Table: {source}"))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display("An error occured with handling Arrow data: {source}"))]
    ArrowError { source: arrow::error::ArrowError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DeltaTable {
    table: Table,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    arrow_schema: SchemaRef,
    delta_schema: delta_kernel::schema::SchemaRef,
}

impl DeltaTable {
    pub fn from(
        table_location: String,
        storage_options: HashMap<String, SecretString>,
    ) -> Result<Self> {
        let table = Table::try_from_uri(table_location).context(DeltaTableSnafu)?;

        let storage_options: HashMap<String, String> = storage_options
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().clone()))
            .collect();

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

        Ok(Arc::new(DeltaTableExecutionPlan::try_new(
            snapshot,
            Arc::clone(&self.engine),
            &self.arrow_schema,
            Arc::clone(&self.delta_schema),
            parquet_file_reader_factory,
            projection,
            physical_expr,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct DeltaTableExecutionPlan {
    snapshot: Arc<Snapshot>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    projected_arrow_schema: SchemaRef,
    projected_delta_schema: delta_kernel::schema::SchemaRef,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    parquet_file_scan_config: FileScanConfig,
    filter: Arc<dyn PhysicalExpr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

#[derive(Clone)]
struct ScanContext {
    tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    parquet_file_scan_config: FileScanConfig,
    scan_state: GlobalScanState,
    task_context: Arc<TaskContext>,
    filter: Arc<dyn PhysicalExpr>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
}

impl ScanContext {
    fn new(
        tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
        parquet_file_scan_config: FileScanConfig,
        scan_state: GlobalScanState,
        task_context: Arc<TaskContext>,
        filter: Arc<dyn PhysicalExpr>,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    ) -> Self {
        Self {
            tx,
            parquet_file_reader_factory,
            parquet_file_scan_config,
            scan_state,
            task_context,
            filter,
            engine,
        }
    }
}

impl DeltaTableExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        snapshot: Snapshot,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
        arrow_schema: &SchemaRef,
        delta_schema: delta_kernel::schema::SchemaRef,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
        projections: Option<&Vec<usize>>,
        filter: Arc<dyn PhysicalExpr>,
        limit: Option<usize>,
    ) -> Result<Self, datafusion::error::DataFusionError> {
        let projected_arrow_schema = project_schema(arrow_schema, projections)?;
        let projected_delta_schema = project_delta_schema(arrow_schema, delta_schema, projections);

        // FileScanConfig requires an ObjectStoreUrl, but it isn't actually used because we pass in a ParquetFileReaderFactory
        // which specifies which object store to read from.
        let parquet_file_scan_config =
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), Arc::clone(arrow_schema))
                .with_limit(limit)
                .with_projection(projections.cloned());
        Ok(Self {
            snapshot: Arc::new(snapshot),
            engine,
            projected_arrow_schema: Arc::clone(&projected_arrow_schema),
            projected_delta_schema,
            parquet_file_reader_factory,
            parquet_file_scan_config,
            filter,
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_arrow_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
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

impl DisplayAs for DeltaTableExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        write!(
            f,
            "DeltaTableExecutionPlan projection=[{}] filters=[{}] limit=[{:?}]",
            columns.join(", "),
            self.filter,
            self.limit,
        )
    }
}

impl ExecutionPlan for DeltaTableExecutionPlan {
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_arrow_schema)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, datafusion::error::DataFusionError> {
        let scan = ScanBuilder::new(Arc::clone(&self.snapshot))
            .with_schema(Arc::clone(&self.projected_delta_schema))
            .build()
            .map_err(map_delta_error_to_datafusion_err)?;
        let engine = Arc::clone(&self.engine);
        let scan_state = scan.global_scan_state();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<
            Result<RecordBatch, datafusion::error::DataFusionError>,
        >(1);

        let scan_context = ScanContext::new(
            tx.clone(),
            Arc::clone(&self.parquet_file_reader_factory),
            self.parquet_file_scan_config.clone(),
            scan_state,
            context,
            Arc::clone(&self.filter),
            Arc::clone(&self.engine),
        );

        let join_handle = tokio::task::spawn_blocking(move || {
            let scan_iter = match scan
                .scan_data(engine.as_ref())
                .map_err(map_delta_error_to_datafusion_err)
            {
                Ok(iter) => iter,
                Err(e) => {
                    tx.blocking_send(Err(e)).ok();
                    return;
                }
            };

            for scan_result in scan_iter {
                let data = match scan_result.map_err(map_delta_error_to_datafusion_err) {
                    Ok(data) => data,
                    Err(e) => {
                        tx.blocking_send(Err(e)).ok();
                        continue;
                    }
                };
                match delta_kernel::scan::state::visit_scan_files(
                    data.0.as_ref(),
                    data.1.as_ref(),
                    scan_context.clone(),
                    handle_scan_file,
                )
                .map_err(map_delta_error_to_datafusion_err)
                {
                    Ok(scan_context) => drop(scan_context),
                    Err(e) => {
                        tx.blocking_send(Err(e)).ok();
                        continue;
                    }
                };
            }
        });

        let receiver_stream = stream! {
            let mut num_rows_processed = 0;
            let mut batch_num = 0;
            while let Some(batch) = rx.recv().await {
                if let Ok(batch) = &batch {
                    num_rows_processed += batch.num_rows();
                    batch_num += 1;
                    tracing::debug!("Processed {num_rows_processed} rows so far: batch_num {batch_num}");
                }
                yield batch;
            }

            if let Err(e) = join_handle.await {
                yield Err(datafusion::error::DataFusionError::External(Box::new(e)));
            };
        };

        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), receiver_stream);
        Ok(Box::pin(stream_adapter))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        Ok(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[allow(clippy::needless_pass_by_value)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
fn handle_scan_file(
    scan_context: &mut ScanContext,
    path: &str,
    size: i64,
    dv_info: DvInfo,
    _partition_values: HashMap<String, String>,
) {
    let root_url = match Url::parse(&scan_context.scan_state.table_root) {
        Ok(url) => url,
        Err(e) => {
            scan_context
                .tx
                .blocking_send(Err(datafusion::error::DataFusionError::Execution(format!(
                    "Error parsing table root URL: {e}",
                ))))
                .ok();
            return;
        }
    };
    let path = format!("{}/{path}", root_url.path());
    let file_scan_config = scan_context
        .parquet_file_scan_config
        .clone()
        .with_file(PartitionedFile::new(path, size as u64));
    let exec = ParquetExec::builder(file_scan_config)
        .with_parquet_file_reader_factory(Arc::clone(&scan_context.parquet_file_reader_factory))
        .with_predicate(Arc::clone(&scan_context.filter))
        .build();

    let stream = match exec.execute(0, Arc::clone(&scan_context.task_context)) {
        Ok(stream) => stream,
        Err(e) => {
            scan_context.tx.blocking_send(Err(e)).ok();
            return;
        }
    };

    // get the selection vector (i.e. deletion vector)
    let mut selection_vector =
        match dv_info.get_selection_vector(scan_context.engine.as_ref(), &root_url) {
            Ok(selection_vector) => selection_vector,
            Err(e) => {
                scan_context
                    .tx
                    .blocking_send(Err(datafusion::error::DataFusionError::Execution(format!(
                        "Error getting selection vector: {e}",
                    ))))
                    .ok();
                return;
            }
        };

    let tx = scan_context.tx.clone();
    tokio::task::spawn(async move {
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let record_batch = match batch {
                Ok(batch) => batch,
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    continue;
                }
            };
            // need to split the dv_mask. what's left in dv_mask covers this result, and rest
            // will cover the following results
            let rest = selection_vector
                .as_mut()
                .map(|mask| mask.split_off(record_batch.num_rows()));
            let batch = if let Some(mask) = selection_vector.clone() {
                // apply the selection vector
                filter_record_batch(&record_batch, &mask.into())
                    .map_err(map_arrow_error_to_datafusion_err)
            } else {
                Ok(record_batch)
            };
            selection_vector = rest;
            tx.send(batch).await.ok();
        }
    });
}

fn map_delta_error_to_datafusion_err(e: delta_kernel::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn map_arrow_error_to_datafusion_err(
    e: arrow::error::ArrowError,
) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}
