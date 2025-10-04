use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3vectors::types::{Index, VectorData};
use datafusion::{
    arrow::{
        array::{Array, Float32Array, ListArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::{Session, TableProvider, memory::DataSourceExec},
    common::{
        Result,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
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

const DEFAULT_TOP_K: usize = 10;

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
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut query_vector = None;

        if let Some(filter) = filters.iter().find(|f| contains_cosine_similarity_udf(f)) {
            let _ = filter.apply(|e| {
                if let Expr::ScalarFunction(ScalarFunction { args, .. }) = e {
                    if args.len() == 2 {
                        query_vector = extract_vector_from_expr(&args[1]);
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }

        let source: Arc<dyn DataSource> = if let Some(query_vector) = query_vector {
            let top_k = limit.unwrap_or(DEFAULT_TOP_K) as i32;
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
                if contains_cosine_similarity_udf(f) {
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

fn contains_cosine_similarity_udf(expr: &Expr) -> bool {
    match expr.apply(|e| {
        Ok(if is_cosine_similarity_udf(e) {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    }) {
        Ok(TreeNodeRecursion::Stop) => true,
        _ => false,
    }
}

fn extract_vector_from_expr(expr: &Expr) -> Option<VectorData> {
    if let Expr::Literal(ScalarValue::List(array), _) = expr {
        if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
            let values = list_array.values();
            if let Some(float32_array) = values.as_any().downcast_ref::<Float32Array>() {
                return Some(VectorData::Float32(float32_array.values().to_vec()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_client::MockClient;
    use aws_sdk_s3vectors::types::DistanceMetric;
    use aws_smithy_types::DateTime;
    use datafusion::{
        arrow::util::pretty::pretty_format_batches,
        logical_expr::{ColumnarValue, ScalarUDF, Volatility},
        prelude::{SessionContext, create_udf},
    };

    fn cosine_similarity() -> ScalarUDF {
        create_udf(
            "cosine_similarity",
            vec![
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            ],
            DataType::Float32,
            Volatility::Immutable,
            Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(1.0))))),
        )
    }

    #[tokio::test]
    async fn test_scan_list_vectors() -> Result<()> {
        let client = Arc::new(MockClient::new());
        let index_name = "test_index";
        let index_arn = "test_arn";
        let bucket_name = "test_bucket";

        let index = Index::builder()
            .index_name(index_name)
            .vector_bucket_name(bucket_name)
            .index_arn(index_arn)
            .creation_time(DateTime::from_secs(7))
            .data_type(aws_sdk_s3vectors::types::DataType::Float32)
            .dimension(3)
            .distance_metric(DistanceMetric::Cosine)
            .build()
            .expect("valid index");

        let table = S3VectorsTable::new(client, index);
        let ctx = SessionContext::new();
        let udf = cosine_similarity();
        ctx.register_udf(udf);

        ctx.register_table("s3_vectors", Arc::new(table))?;

        let df = ctx
            .sql("EXPLAIN SELECT key, data FROM s3_vectors LIMIT 10")
            .await?;
        let batches = df.collect().await?;
        let explain = pretty_format_batches(&batches)?.to_string();
        assert!(!explain.contains("QueryVectorsSource"));
        assert!(explain.contains("ListVectorsSource"));

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_query_vectors() -> Result<()> {
        let client = Arc::new(MockClient::new());
        let index_name = "test_index";
        let index_arn = "test_arn";
        let bucket_name = "test_bucket";

        let index = Index::builder()
            .index_name(index_name)
            .vector_bucket_name(bucket_name)
            .index_arn(index_arn)
            .creation_time(DateTime::from_secs(7))
            .data_type(aws_sdk_s3vectors::types::DataType::Float32)
            .dimension(3)
            .distance_metric(DistanceMetric::Cosine)
            .build()
            .expect("valid index");

        let table = S3VectorsTable::new(client, index);
        let ctx = SessionContext::new();
        let udf = cosine_similarity();
        ctx.register_udf(udf);

        ctx.register_table("s3_vectors", Arc::new(table))?;

        let df = ctx.sql("EXPLAIN SELECT key, data FROM s3_vectors WHERE cosine_similarity(data, make_array(1.0, 2.0, 3.0)) > 0.8 LIMIT 10").await?;
        let batches = df.collect().await?;
        let explain = pretty_format_batches(&batches)?.to_string();
        assert!(explain.contains("QueryVectorsSource"));
        assert!(!explain.contains("ListVectorsSource"));

        Ok(())
    }
}
