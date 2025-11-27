use crate::mongodb::connection_pool::MongoDBConnectionPool;
use crate::mongodb::utils::expression::{combine_exprs_with_and, expr_to_mongo_filter};
use crate::mongodb::Error;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use mongodb::bson::Document;
use serde_json;
use std::{any::Any, fmt, sync::Arc};

#[derive(Debug)]
pub struct MongoDBTable {
    pool: Arc<MongoDBConnectionPool>,
    schema: SchemaRef,
    table_reference: Arc<TableReference>,
}

impl MongoDBTable {
    pub async fn new(
        pool: &Arc<MongoDBConnectionPool>,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self, Error> {
        let table_reference = table_reference.into();
        let schema = pool.connect().await?.get_schema(&table_reference).await?;

        Ok(Self {
            pool: Arc::clone(pool),
            schema,
            table_reference: Arc::new(table_reference),
        })
    }
}

#[async_trait]
impl TableProvider for MongoDBTable {
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
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MongoDBExec::new(
            Arc::clone(&self.table_reference),
            Arc::clone(&self.pool),
            Arc::clone(&self.schema),
            projection,
            filters,
            limit,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        supports_filters_pushdown(filters)
    }
}

fn supports_filters_pushdown(
    filters: &[&Expr],
) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
    let filter_push_down: Vec<TableProviderFilterPushDown> = filters
        .iter()
        .map(|f| match expr_to_mongo_filter(f) {
            Some(_) => TableProviderFilterPushDown::Exact,
            None => TableProviderFilterPushDown::Unsupported,
        })
        .collect();

    Ok(filter_push_down)
}

#[derive(Debug)]
struct MongoDBExec {
    table_reference: Arc<TableReference>,
    pool: Arc<MongoDBConnectionPool>,
    projected_schema: SchemaRef,
    filters_doc: Document,
    limit: Option<i32>,
    properties: PlanProperties,
}

impl MongoDBExec {
    pub fn new(
        table_reference: Arc<TableReference>,
        pool: Arc<MongoDBConnectionPool>,
        schema: SchemaRef,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let mut projected_schema = project_schema(&schema, projections)?;

        // If no columns are specified, use _id - otherwise mongo returns an error
        if projected_schema.fields.is_empty() {
            let idx = schema.index_of("_id")?;
            projected_schema = SchemaRef::from(schema.project(&[idx])?);
        }

        let limit = limit
            .map(|u| {
                let Ok(u) = u32::try_from(u) else {
                    return Err(DataFusionError::Execution(
                        "Value is too large to fit in a u32".to_string(),
                    ));
                };
                if let Ok(u) = i32::try_from(u) {
                    Ok(u)
                } else {
                    Err(DataFusionError::Execution(
                        "Value is too large to fit in an i32".to_string(),
                    ))
                }
            })
            .transpose()?;

        let combined_exprs = combine_exprs_with_and(filters);

        let mongo_filters_doc = match combined_exprs {
            Some(e) => expr_to_mongo_filter(&e).ok_or(DataFusionError::Execution(
                "Failed to convert expressions".to_string(),
            ))?,
            None => Document::new(),
        };

        Ok(Self {
            table_reference: Arc::clone(&table_reference),
            pool,
            projected_schema: Arc::clone(&projected_schema),
            filters_doc: mongo_filters_doc,
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        })
    }
}

impl DisplayAs for MongoDBExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>();

        let filters = serde_json::to_string(&self.filters_doc).map_err(|_| fmt::Error)?;

        write!(
            f,
            "MongoDBExec projection=[{}] filters=[{}]",
            columns.join(", "),
            filters,
        )
    }
}

impl ExecutionPlan for MongoDBExec {
    fn name(&self) -> &'static str {
        "MongoDBExec"
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        let schema = self.schema();

        let table_reference = Arc::clone(&self.table_reference);
        let pool = Arc::clone(&self.pool);
        let projected_schema = Arc::clone(&self.projected_schema);
        let filters_doc = self.filters_doc.clone();
        let limit = self.limit;

        let stream = futures::stream::once(async move {
            let conn = pool.connect().await.map_err(to_execution_error)?;

            conn.query_arrow(&table_reference, &projected_schema, &filters_doc, limit)
                .await
                .map_err(to_execution_error)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_execution_error(
    e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, Operator};

    #[tokio::test]
    async fn test_supports_filters_pushdown_supported() {
        let expr = col("foo").eq(lit(42));
        let res = supports_filters_pushdown(&[&expr]).unwrap();
        assert_eq!(res, vec![TableProviderFilterPushDown::Exact]);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_unsupported() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("foo")),
            op: Operator::Modulo,
            right: Box::new(lit("bar")),
        });
        let res = supports_filters_pushdown(&[&expr]).unwrap();
        assert_eq!(res, vec![TableProviderFilterPushDown::Unsupported]);
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_mixed() {
        let exprs = vec![
            col("foo").eq(lit(10)),
            col("bar").not_eq(lit("baz")),
            col("foo").is_null(),
        ];
        let refs: Vec<&Expr> = exprs.iter().collect();
        let res = supports_filters_pushdown(&refs).unwrap();
        assert_eq!(
            res,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported,
            ]
        );
    }
}
