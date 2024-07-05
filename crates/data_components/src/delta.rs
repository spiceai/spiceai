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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::project_schema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Table;
use futures::stream;
use futures::StreamExt;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::fmt;
use std::{collections::HashMap, sync::Arc};

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
    schema: SchemaRef,
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

        let schema = Self::get_schema(&snapshot);

        Ok(Self {
            table,
            engine,
            schema: Arc::new(schema),
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

fn map_delta_data_type_to_arrow_data_type(
    delta_data_type: &delta_kernel::schema::DataType,
) -> DataType {
    match delta_data_type {
        delta_kernel::schema::DataType::Primitive(_) => todo!(),
        delta_kernel::schema::DataType::Array(_) => todo!(),
        delta_kernel::schema::DataType::Struct(_) => todo!(),
        delta_kernel::schema::DataType::Map(_) => todo!(),
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        let snapshot = self
            .table
            .snapshot(self.engine.as_ref(), None)
            .map_err(map_delta_error_to_datafusion_err)?;

        Ok(Arc::new(DeltaTableExecutionPlan::try_new(
            snapshot,
            Arc::clone(&self.engine),
            &self.schema,
            projection,
            filters,
            limit,
        )?))
    }
}

#[derive(Debug)]
struct DeltaTableExecutionPlan {
    snapshot: Arc<Snapshot>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    projected_schema: SchemaRef,
    properties: PlanProperties,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl DeltaTableExecutionPlan {
    pub fn try_new(
        snapshot: Snapshot,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
        schema: &SchemaRef,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Self, datafusion::error::DataFusionError> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            snapshot: Arc::new(snapshot),
            engine,
            projected_schema: Arc::clone(&projected_schema),
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        })
    }
}

impl DisplayAs for DeltaTableExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();
        let filters = self
            .filters
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        write!(
            f,
            "DeltaTableExecutionPlan projection=[{}] filters=[{}] limit=[{:?}]",
            columns.join(", "),
            filters.join(", "),
            self.limit,
        )
    }
}

impl ExecutionPlan for DeltaTableExecutionPlan {
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, datafusion::error::DataFusionError> {
        let scan = ScanBuilder::new(Arc::clone(&self.snapshot))
            .build()
            .map_err(map_delta_error_to_datafusion_err)?;

        let mut batches = vec![];
        for res in scan
            .execute(self.engine.as_ref())
            .map_err(map_delta_error_to_datafusion_err)?
        {
            let data = res.raw_data.map_err(map_delta_error_to_datafusion_err)?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))
                .map_err(map_delta_error_to_datafusion_err)?
                .into();
            let batch = if let Some(mask) = res.mask {
                filter_record_batch(&record_batch, &mask.into())
                    .map_err(map_arrow_error_to_datafusion_err)?
            } else {
                record_batch
            };
            batches.push(batch);
        }
        let stream_adapter =
            RecordBatchStreamAdapter::new(self.schema(), stream::iter(batches).map(Ok));
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

fn map_delta_error_to_datafusion_err(e: delta_kernel::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn map_arrow_error_to_datafusion_err(
    e: arrow::error::ArrowError,
) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ArrowError(e, None)
}
