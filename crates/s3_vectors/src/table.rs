use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3vectors::types::Index;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::Result,
    datasource::{TableType, sink::DataSinkExec},
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::{S3Vectors, put_vectors::PutVectorsSink};

static NAME: &str = "S3VectorsTable";

pub struct S3VectorsTable {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
}

impl fmt::Debug for S3VectorsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

#[async_trait]
impl TableProvider for S3VectorsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let schema = Arc::clone(&self.schema);

        let sink = Arc::new(PutVectorsSink::new(client, index, schema));
        let sort_order = None;
        let exec = DataSinkExec::new(input, sink, sort_order);
        Ok(Arc::new(exec))
    }
}
