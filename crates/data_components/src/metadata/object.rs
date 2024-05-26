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

use std::{any::Any, fmt,  sync::Arc};

use arrow::{array::RecordBatch, datatypes::{DataType, Field, Schema, SchemaRef}};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    common::{project_schema, Constraint, Constraints}, datasource::{TableProvider, TableType}, error::{DataFusionError, Result as DataFusionResult}, execution::{context::SessionState, SendableRecordBatchStream, TaskContext}, logical_expr::{Expr, TableProviderFilterPushDown}, physical_expr::EquivalenceProperties, physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties
    }
};

use futures::StreamExt;
use object_store::{ObjectStore, path::Path};

use futures::Stream;


pub struct ObjectStoreMetadataTable {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreMetadataTable {
    pub fn new(store: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self { store })
    }
    
    pub fn constraints(&self) -> Constraints {
        Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]) // "location"
        ])
    }
}


#[async_trait]
impl TableProvider for ObjectStoreMetadataTable {

    fn as_any(&self) -> &dyn Any {
        self
    }


    /// Schema of [`ObjectStoreMetadataTable`] defined in relation to [`object_store::ObjectMeta`]. 
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("last_modified", DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None), false),
            Field::new("size", DataType::UInt64, false),
            Field::new("e_tag", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
        ]))
    }

    fn constraints(&self) -> Option<&Constraints> {
        // TODO: Implement this.
        // Constraints::new_unverified(vec![
        //     Constraint::PrimaryKey(vec![0]) // "location"
        // ])

        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }


    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema(), projection)?;
        Ok(Arc::new(ObjectStoreMetadataExec::new(
            projected_schema,
            filters,
            limit,
            self.store.clone()
        )))
    }
    
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

pub struct ObjectStoreMetadataExec {
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    limit: Option<usize>,
    store: Arc<dyn ObjectStore>,
    properties: PlanProperties,
}

impl ExecutionPlan for ObjectStoreMetadataExec {
    fn name(&self) -> &'static str {
        "ObjectStoreMetadataExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            to_sendable_stream(self.store.clone(), Some("prefix".to_string())),
        )))
    }
}

impl ObjectStoreMetadataExec {
    pub fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {

        Self {
            projected_schema: projected_schema.clone(),
            filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            store
        }
    }
    

    /// Querying a prefix, p of the object store, is equivalent to the filter: `TODO`.
    fn prefix(&self) -> Option<String> {
        
        // TODO: Implement the filter for the prefix.
        self.filters.iter()
            .filter(|&_f| false).next()
            .map(|f| f.to_string())
    }

}

pub fn to_sendable_stream(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static  {
    stream! {
        let mut object_stream = store.list(prefix.map(|p| Path::from(p)).as_ref());
        while let Some(item) = object_stream.next().await {
            match item {
                Ok(object_meta) => {
                    let x = object_meta.location;
                }
                Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
            }
        }
    }
}

impl std::fmt::Debug for ObjectStoreMetadataExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} prefix={}", self.name(), self.prefix().unwrap_or_default())
    }
}

impl DisplayAs for ObjectStoreMetadataExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} prefix={}", self.name(), self.prefix().unwrap_or_default())
    }
}