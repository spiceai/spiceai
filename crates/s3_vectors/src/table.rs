use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3vectors::types::{Index, VectorData};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::{Session, TableProvider, memory::DataSourceExec},
    common::Result,
    datasource::{TableType, sink::DataSinkExec, source::DataSource},
    logical_expr::{Expr, TableProviderFilterPushDown, dml::InsertOp, expr::ScalarFunction},
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
};

use crate::{
    S3Vectors, list_vectors::ListVectorsSource, put_vectors::PutVectorsSink,
    query_vectors::QueryVectorsSource,
};

static NAME: &str = "S3VectorsTable";

pub struct S3VectorsTable {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
}

impl S3VectorsTable {
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>, index: Index) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new("distance", DataType::Float32, true),
        ]));
        Self {
            client,
            index,
            schema,
        }
    }
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut query_vector = None;
        let mut top_k = None;

        if let Some(filter) = filters.iter().find(|f| is_cosine_similarity_udf(f)) {
            if let Expr::ScalarFunction(ScalarFunction { args, .. }) = filter {
                if args.len() == 3 {
                    query_vector = extract_vector_from_expr(&args[1])?;
                    top_k = extract_top_k_from_expr(&args[2])?;
                }
            }
        }

        let source: Arc<dyn DataSource> =
            if let (Some(query_vector), Some(top_k)) = (query_vector, top_k) {
                Arc::new(QueryVectorsSource::new(
                    Arc::clone(&self.client),
                    self.index.clone(),
                    self.schema(),
                    query_vector,
                    top_k,
                ))
            } else {
                Arc::new(ListVectorsSource::new(
                    Arc::clone(&self.client),
                    self.index.clone(),
                    self.schema(),
                ))
            };

        Ok(Arc::new(DataSourceExec::new(source)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if is_cosine_similarity_udf(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
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

fn is_cosine_similarity_udf(expr: &Expr) -> bool {
    if let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr {
        func.name() == "cosine_similarity"
    } else {
        false
    }
}

fn extract_vector_from_expr(expr: &Expr) -> Result<Option<VectorData>> {
    if let Expr::Literal(ScalarValue::Utf8(Some(s)), _) = expr {
        let s = s.trim();
        if s.starts_with('[') && s.ends_with(']') {
            let floats: std::result::Result<Vec<f32>, _> = s[1..s.len() - 1]
                .split(',')
                .map(|s| s.trim().parse::<f32>())
                .collect();
            if let Ok(floats) = floats {
                return Ok(Some(VectorData::Float32(floats)));
            }
        }
    }
    Ok(None)
}

fn extract_top_k_from_expr(expr: &Expr) -> Result<Option<i32>> {
    if let Expr::Literal(ScalarValue::Int64(Some(k)), _) = expr {
        Ok(Some(*k as i32))
    } else {
        Ok(None)
    }
}
