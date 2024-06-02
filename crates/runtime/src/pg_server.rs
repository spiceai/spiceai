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

use std::{cmp::Ordering, sync::Arc};

use arrow::{array::{Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListBuilder, NullArray, RecordBatch, StringArray, StringBuilder, UInt32Builder}, datatypes::{DataType, Field, Schema, SchemaRef}};
use datafusion::{catalog::schema::{MemorySchemaProvider, SchemaProvider}, common::{cast::as_list_array, not_impl_err, plan_datafusion_err, plan_err}, datasource::{function::{TableFunction, TableFunctionImpl}, MemTable, TableProvider, TableType}, error::DataFusionError, execution::{context::{ExecutionProps, SessionContext, SessionState}, TaskContext}, logical_expr::{create_udf, simplify::SimplifyContext, ColumnarValue, Expr, LogicalPlanBuilder, ReturnTypeFunction, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility}, optimizer::simplify_expressions::ExprSimplifier, physical_plan::{functions::make_scalar_function, memory::MemoryExec, udf, ExecutionPlan}, scalar::ScalarValue, sql::TableReference};
use metadata::PgCatalog;
use pgwire::{api::{auth::noop::NoopStartupHandler, query::PlaceholderExtendedQueryHandler, StatelessMakeHandler}, tokio::process_socket};
use snafu::Snafu;

use datafusion::common::Result as DFResult;


use std::{any::Any, any::type_name};

use async_trait::async_trait;

use pgwire::api::MakeHandler;

use tokio::{net::TcpListener, sync::RwLock};



use crate::{dataconnector::DataConnectorError, datafusion::{DataFusion, SPICE_DEFAULT_CATALOG}};

mod handlers;
mod datatypes;
mod version;
mod current_schema;
mod metadata;

// https://github.com/dataclod/dataclod/blob/2e5f29366464a0f15c24542c71c92baf1bb35294/src/datafusion-extra/src/sqlbuiltin/udf/array_upper.rs

mod array_upper;
mod current_schemas;

#[derive(Debug, Snafu)]
pub enum Error {
    // #[snafu(display("Unable to register parquet file: {source}"))]
    // RegisterParquet { source: crate::datafusion::Error },

    // #[snafu(display("{source}"))]
    // DataFusion {
    //     source: datafusion::error::DataFusionError,
    // },

    // #[snafu(display("Unable to start Flight server: {source}"))]
    // UnableToStartFlightServer { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(bind_address: std::net::SocketAddr, df: Arc<RwLock<DataFusion>>) -> Result<()> {

    let df_copy = Arc::clone(&df);
    
    let ctx = &df_copy.read().await.ctx;

    
    
    // let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;

    let spice_catalog = ctx.catalog(SPICE_DEFAULT_CATALOG).unwrap();

    let schema = PgCatalog::new(spice_catalog).build_metadata_schema().unwrap();

    // let pg_type = Arc::new(PgTypesView::new());

    schema.register_table("pg_type".into(), Arc::new(PgTypesView::new())).unwrap();
    // schema.register_table("pg_class".into(), Arc::new(PgCatalogClassProvider::new())).unwrap();
    // schema.register_table("pg_description".into(), Arc::new(PgCatalogDescriptionProvider::new())).unwrap();
    // schema.register_table("pg_namespace".into(), Arc::new(PgCatalogNamespaceProvider::new())).unwrap();
    schema.register_table("pg_attribute".into(), Arc::new(PgCatalogAttributeProvider::new())).unwrap();
    schema.register_table("pg_attrdef".into(), Arc::new(PgCatalogAttrdefProvider::new())).unwrap();


    let pg_namespace_table_provider = schema
        .table("pg_namespace")
        .await.unwrap().unwrap();

    ctx.catalog(SPICE_DEFAULT_CATALOG)
        .unwrap()
        .register_schema("pg_catalog", Arc::new(schema))
        .unwrap();

    let pg_namespace_table_reference = TableReference::full(
        SPICE_DEFAULT_CATALOG,
        "public",
        "pg_namespace",
    );
    
    ctx
        .register_table(pg_namespace_table_reference, pg_namespace_table_provider).unwrap();
    

    ctx.register_udtf(
        "generate_series",
        Arc::new(GenerateSeriesUDTF {}),
    );
    ctx.register_udf(array_upper::create_udf());
    ctx.register_udf(current_schema::create_udf());
    ctx.register_udf(current_schemas::create_udf());
    ctx.register_udf(version::create_udf());
    ctx.register_udf(create_pg_get_expr_udf());


    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(
        handlers::DfSessionService::new(df),
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let listener = TcpListener::bind(bind_address).await.unwrap();
    tracing::info!("Spice PostgreSQL compatible layer listening on {bind_address}");
    //metrics::counter!("spiced_runtime_pg_server_start").increment(1);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }

    Ok(())
}


#[derive(Debug)]
pub struct PgTypesView {
    schema: SchemaRef,
}

impl Default for PgTypesView {
    fn default() -> Self {
        Self::new()
    }
}

impl PgTypesView {
    pub fn new() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![
                Field::new("oid", DataType::Utf8, false),
                Field::new("typname", DataType::Utf8, false),
                Field::new("typnamespace", DataType::Utf8, false),
                Field::new("typowner", DataType::Utf8, false),
                Field::new("typlen", DataType::Int16, false),
                Field::new("typbyval", DataType::Boolean, false),
                Field::new("typtype", DataType::Utf8, false),
                Field::new("typcategory", DataType::Utf8, false),
                Field::new("typispreferred", DataType::Boolean, false),
                Field::new("typisdefined", DataType::Boolean, false),
                Field::new("typdelim", DataType::Utf8, false),
                Field::new("typrelid", DataType::Utf8, false),
                Field::new("typelem", DataType::Utf8, false),
                Field::new("typarray", DataType::Utf8, false),
                Field::new("typinput", DataType::Utf8, false),
                Field::new("typoutput", DataType::Utf8, false),
                Field::new("typreceive", DataType::Utf8, false),
                Field::new("typsend", DataType::Utf8, false),
                Field::new("typmodin", DataType::Utf8, false),
                Field::new("typmodout", DataType::Utf8, false),
                Field::new("typanalyze", DataType::Utf8, false),
                Field::new("typalign", DataType::Utf8, false),
                Field::new("typstorage", DataType::Utf8, false),
                Field::new("typnotnull", DataType::Boolean, false),
                Field::new("typbasetype", DataType::Utf8, false),
                Field::new("typtypmod", DataType::Int32, false),
                Field::new("typndims", DataType::Int32, false),
                Field::new("typcollation", DataType::Utf8, false),
                Field::new("typdefaultbin", DataType::Binary, true),
                Field::new("typdefault", DataType::Utf8, true),
                Field::new("typacl", DataType::Utf8, true),
            ])),
        }
    }
}

#[async_trait]
impl TableProvider for PgTypesView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[],
            self.schema.clone(),
            projection.cloned(),
        ).unwrap()))
    }
}

// struct PgNamespace {
//     oid: u32,
//     nspname: &'static str,
//     nspowner: u32,
//     nspacl: &'static str,
// }

// struct PgCatalogNamespaceBuilder {
//     oid: UInt32Builder,
//     nspname: StringBuilder,
//     nspowner: UInt32Builder,
//     nspacl: StringBuilder,
// }

// impl PgCatalogNamespaceBuilder {
//     fn new() -> Self {

//         Self {
//             oid: UInt32Builder::new(),
//             nspname: StringBuilder::new(),
//             nspowner: UInt32Builder::new(),
//             nspacl: StringBuilder::new(),
//         }
//     }

//     fn add_namespace(&mut self, ns: &PgNamespace) {
//         self.oid.append_value(ns.oid);
//         self.nspname.append_value(ns.nspname);
//         self.nspowner.append_value(ns.nspowner);
//         self.nspacl.append_value(ns.nspacl);
//     }

//     fn finish(mut self) -> Vec<Arc<dyn Array>> {
//         let mut columns: Vec<Arc<dyn Array>> = vec![];
//         columns.push(Arc::new(self.oid.finish()));
//         columns.push(Arc::new(self.nspname.finish()));
//         columns.push(Arc::new(self.nspowner.finish()));
//         columns.push(Arc::new(self.nspacl.finish()));

//         columns
//     }
// }

// pub struct PgCatalogNamespaceProvider {
//     data: Arc<Vec<ArrayRef>>,
// }

// impl PgCatalogNamespaceProvider {
//     pub fn new() -> Self {
//         let mut builder = PgCatalogNamespaceBuilder::new();
//         builder.add_namespace(&PgNamespace {
//             oid: 11,
//             nspname: "pg_catalog",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });
//         builder.add_namespace(&PgNamespace {
//             oid: 2200,
//             nspname: "public",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });
//         builder.add_namespace(&PgNamespace {
//             oid: 13000,
//             nspname: "information_schema",
//             nspowner: 10,
//             nspacl: "{test=UC/test,=U/test}",
//         });

//         Self {
//             data: Arc::new(builder.finish()),
//         }
//     }
// }

// #[async_trait]
// impl TableProvider for PgCatalogNamespaceProvider {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn table_type(&self) -> TableType {
//         TableType::View
//     }

//     fn schema(&self) -> SchemaRef {
//         Arc::new(Schema::new(vec![
//             Field::new("oid", DataType::UInt32, false),
//             Field::new("nspname", DataType::Utf8, false),
//             Field::new("nspowner", DataType::UInt32, false),
//             Field::new("nspacl", DataType::Utf8, true),
//         ]))
//     }

//     async fn scan(
//         &self,
//         _state: &SessionState,
//         projection: Option<&Vec<usize>>,
//         // filters and limit can be used here to inject some push-down operations if needed
//         _filters: &[Expr],
//         _limit: Option<usize>,
//     ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
//         let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

//         Ok(Arc::new(MemoryExec::try_new(
//             &[vec![batch]],
//             self.schema(),
//             projection.clone().cloned(),
//         )?))
//     }
// }

// // Helper function to evaluate an expression to a scalar value
// async fn evaluate_expr(ctx: &SessionContext, expr: &Expr) -> std::result::Result<ScalarValue, DataFusionError> {
//     let plan = LogicalPlanBuilder::empty(false).project(vec![expr.clone()]).unwrap().build().unwrap();
//     let df = ctx.execute_logical_plan(plan).await?;
//     let batches = df.collect().await?;
//     if let Some(batch) = batches.first() {
//         if let Some(array) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
//             return Ok(ScalarValue::Int64(Some(array.value(0))));
//         }
//     }
//     Err(DataFusionError::Plan("Failed to evaluate expression".to_string()))
// }

pub struct GenerateSeriesUDTF;

impl TableFunctionImpl for GenerateSeriesUDTF {
    fn call(&self, exprs: &[Expr]) -> std::result::Result<Arc<dyn TableProvider>, DataFusionError> {
        if exprs.len() < 1 || exprs.len() > 3 {
            return plan_err!("generate_series takes 1, 2 or 3 arguments");
        }

        let execution_props = ExecutionProps::new();
        let info = SimplifyContext::new(&execution_props);
        let simplifier = ExprSimplifier::new(info);
        let start = simplifier.simplify(exprs[0].clone())?;
        let stop = if exprs.len() > 1 {
            simplifier.simplify(exprs[1].clone())?
        } else {
            Expr::Literal(ScalarValue::Int64(Some(10))) // Default stop value
        };
        let step = if exprs.len() == 3 {
            simplifier.simplify(exprs[2].clone())?
        } else {
            Expr::Literal(ScalarValue::Int64(Some(1))) // Default step value
        };

        match (start, stop, step) {
            (
                Expr::Literal(ScalarValue::Int64(Some(start))),
                Expr::Literal(ScalarValue::Int64(Some(stop))),
                Expr::Literal(ScalarValue::Int64(Some(step))),
            ) => Ok(Arc::new(GenerateSeriesTable { start, stop, step })),
            _ => plan_err!("generate_series arguments must be integer literals"),
        }
    }
}

struct GenerateSeriesTable<T: Send + Sync> {
    start: T,
    stop: T,
    step: T,
}

#[async_trait]
impl TableProvider for GenerateSeriesTable<i64> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "generate_series",
            DataType::Int64,
            false,
        )]))
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let array: Int64Array = (self.start..=self.stop)
            .step_by(self.step as usize)
            .collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        ).unwrap()))
    }
}
struct PgClass {
    oid: u32,
    relname: String,
    relnamespace: u32,
    reltype: u32,
    relam: u32,
    relfilenode: u32,
    reltoastrelid: u32,
    relisshared: bool,
    relkind: String,
    relnatts: i32,
    relhasrules: bool,
    relreplident: String,
    relfrozenxid: i32,
    relminmxid: i32,
}

struct PgCatalogClassBuilder {
    oid: UInt32Builder,
    relname: StringBuilder,
    relnamespace: UInt32Builder,
    reltype: UInt32Builder,
    reloftype: UInt32Builder,
    relowner: UInt32Builder,
    relam: UInt32Builder,
    relfilenode: UInt32Builder,
    reltablespace: UInt32Builder,
    relpages: Int32Builder,
    reltuples: Int32Builder,
    relallvisible: Int32Builder,
    reltoastrelid: UInt32Builder,
    relhasindex: BooleanBuilder,
    relisshared: BooleanBuilder,
    relpersistence: StringBuilder,
    relkind: StringBuilder,
    relnatts: Int32Builder,
    relchecks: Int32Builder,
    relhasrules: BooleanBuilder,
    relhastriggers: BooleanBuilder,
    relhassubclass: BooleanBuilder,
    relrowsecurity: BooleanBuilder,
    relforcerowsecurity: BooleanBuilder,
    relispopulated: BooleanBuilder,
    relreplident: StringBuilder,
    relispartition: BooleanBuilder,
    relrewrite: UInt32Builder,
    relfrozenxid: Int32Builder,
    relminmxid: Int32Builder,
    relacl: StringBuilder,
    reloptions: StringBuilder,
    relpartbound: StringBuilder,
    // This column was removed after PostgreSQL 12, but it's required to support Tableau Desktop with ODBC
    // True if we generate an OID for each row of the relation
    relhasoids: BooleanBuilder,
}

impl PgCatalogClassBuilder {

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.relname.finish()));
        columns.push(Arc::new(self.relnamespace.finish()));
        columns.push(Arc::new(self.reltype.finish()));
        columns.push(Arc::new(self.reloftype.finish()));
        columns.push(Arc::new(self.relowner.finish()));
        columns.push(Arc::new(self.relam.finish()));
        columns.push(Arc::new(self.relfilenode.finish()));
        columns.push(Arc::new(self.reltablespace.finish()));
        columns.push(Arc::new(self.relpages.finish()));
        columns.push(Arc::new(self.reltuples.finish()));
        columns.push(Arc::new(self.relallvisible.finish()));
        columns.push(Arc::new(self.reltoastrelid.finish()));
        columns.push(Arc::new(self.relhasindex.finish()));
        columns.push(Arc::new(self.relisshared.finish()));
        columns.push(Arc::new(self.relpersistence.finish()));
        columns.push(Arc::new(self.relkind.finish()));
        columns.push(Arc::new(self.relnatts.finish()));
        columns.push(Arc::new(self.relchecks.finish()));
        columns.push(Arc::new(self.relhasrules.finish()));
        columns.push(Arc::new(self.relhastriggers.finish()));
        columns.push(Arc::new(self.relhassubclass.finish()));
        columns.push(Arc::new(self.relrowsecurity.finish()));
        columns.push(Arc::new(self.relforcerowsecurity.finish()));
        columns.push(Arc::new(self.relispopulated.finish()));
        columns.push(Arc::new(self.relreplident.finish()));
        columns.push(Arc::new(self.relispartition.finish()));
        columns.push(Arc::new(self.relrewrite.finish()));
        columns.push(Arc::new(self.relfrozenxid.finish()));
        columns.push(Arc::new(self.relminmxid.finish()));
        columns.push(Arc::new(self.relacl.finish()));
        columns.push(Arc::new(self.reloptions.finish()));
        columns.push(Arc::new(self.relpartbound.finish()));
        columns.push(Arc::new(self.relhasoids.finish()));

        columns
    }

    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(),
            relname: StringBuilder::new(),
            relnamespace: UInt32Builder::new(),
            reltype: UInt32Builder::new(),
            reloftype: UInt32Builder::new(),
            relowner: UInt32Builder::new(),
            relam: UInt32Builder::new(),
            relfilenode: UInt32Builder::new(),
            reltablespace: UInt32Builder::new(),
            relpages: Int32Builder::new(),
            reltuples: Int32Builder::new(),
            relallvisible: Int32Builder::new(),
            reltoastrelid: UInt32Builder::new(),
            relhasindex: BooleanBuilder::new(),
            relisshared: BooleanBuilder::new(),
            relpersistence: StringBuilder::new(),
            relkind: StringBuilder::new(),
            relnatts: Int32Builder::new(),
            relchecks: Int32Builder::new(),
            relhasrules: BooleanBuilder::new(),
            relhastriggers: BooleanBuilder::new(),
            relhassubclass: BooleanBuilder::new(),
            relrowsecurity: BooleanBuilder::new(),
            relforcerowsecurity: BooleanBuilder::new(),
            relispopulated: BooleanBuilder::new(),
            relreplident: StringBuilder::new(),
            relispartition: BooleanBuilder::new(),
            relrewrite: UInt32Builder::new(),
            relfrozenxid: Int32Builder::new(),
            relminmxid: Int32Builder::new(),
            relacl: StringBuilder::new(),
            reloptions: StringBuilder::new(),
            relpartbound: StringBuilder::new(),
            relhasoids: BooleanBuilder::new(),
        }
    }
}

pub struct PgCatalogClassProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogClassProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogClassBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogClassProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relname", DataType::Utf8, false),
            // info_schma: 13000; pg_catalog: 11; user defined tables: 2200
            Field::new("relnamespace", DataType::UInt32, false),
            Field::new("reltype", DataType::UInt32, false),
            Field::new("reloftype", DataType::UInt32, false),
            Field::new("relowner", DataType::UInt32, false),
            //user defined tables: 2; system tables: 0 | 2
            Field::new("relam", DataType::UInt32, false),
            // TODO: to check that 0 if fine
            Field::new("relfilenode", DataType::UInt32, false),
            Field::new("reltablespace", DataType::UInt32, false),
            Field::new("relpages", DataType::Int32, false),
            Field::new("reltuples", DataType::Int32, false),
            Field::new("relallvisible", DataType::Int32, false),
            // TODO: sometimes is not 0. Check that 0 is fine
            Field::new("reltoastrelid", DataType::UInt32, false),
            Field::new("relhasindex", DataType::Boolean, false),
            //user defined tables: FALSE; system tables: FALSE | TRUE
            Field::new("relisshared", DataType::Boolean, false),
            Field::new("relpersistence", DataType::Utf8, false),
            // Tables: r; Views: v
            Field::new("relkind", DataType::Utf8, false),
            // number of columns in table
            Field::new("relnatts", DataType::Int32, false),
            Field::new("relchecks", DataType::Int32, false),
            //user defined tables: FALSE; system tables: FALSE | TRUE
            Field::new("relhasrules", DataType::Boolean, false),
            Field::new("relhastriggers", DataType::Boolean, false),
            Field::new("relhassubclass", DataType::Boolean, false),
            Field::new("relrowsecurity", DataType::Boolean, false),
            Field::new("relforcerowsecurity", DataType::Boolean, false),
            Field::new("relispopulated", DataType::Boolean, false),
            //user defined tables: p; system tables: n
            Field::new("relreplident", DataType::Utf8, false),
            Field::new("relispartition", DataType::Boolean, false),
            Field::new("relrewrite", DataType::UInt32, false),
            // TODO: can be not 0; check that 0 is fine
            Field::new("relfrozenxid", DataType::Int32, false),
            // Tables: 1; Other: v
            Field::new("relminmxid", DataType::Int32, false),
            Field::new("relacl", DataType::Utf8, true),
            Field::new("reloptions", DataType::Utf8, true),
            Field::new("relpartbound", DataType::Utf8, true),
            Field::new("relhasoids", DataType::Boolean, false),
        ]))
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone().cloned(),
        )?))
    }
}

struct PgCatalogDescriptionBuilder {
    objoid: UInt32Builder,
    classoid: UInt32Builder,
    objsubid: Int32Builder,
    description: StringBuilder,
}

impl PgCatalogDescriptionBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            objoid: UInt32Builder::new(),
            classoid: UInt32Builder::new(),
            objsubid: Int32Builder::new(),
            description: StringBuilder::new(),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.objoid.finish()));
        columns.push(Arc::new(self.classoid.finish()));
        columns.push(Arc::new(self.objsubid.finish()));
        columns.push(Arc::new(self.description.finish()));

        columns
    }
}

pub struct PgCatalogDescriptionProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogDescriptionProvider {
    pub fn new() -> Self {
        let builder = PgCatalogDescriptionBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogDescriptionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("classoid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone().cloned(),
        )?))
    }
}

struct PgCatalogAttributeBuilder {
    attrelid: UInt32Builder,
    attname: StringBuilder,
    atttypid: UInt32Builder,
    attstattarget: Int32Builder,
    attlen: Int16Builder,
    attnum: Int16Builder,
    attndims: UInt32Builder,
    attcacheoff: Int32Builder,
    // TODO: Add support for casts within case and switch back to Int32
    atttypmod: Int64Builder,
    attbyval: BooleanBuilder,
    attalign: StringBuilder,
    attstorage: StringBuilder,
    attcompression: StringBuilder,
    attnotnull: BooleanBuilder,
    atthasdef: BooleanBuilder,
    atthasmissing: BooleanBuilder,
    attidentity: StringBuilder,
    attgenerated: StringBuilder,
    attisdropped: BooleanBuilder,
    attislocal: BooleanBuilder,
    attinhcount: UInt32Builder,
    attcollation: UInt32Builder,
    attacl: StringBuilder,
    attoptions: StringBuilder,
    attfdwoptions: StringBuilder,
    attmissingval: StringBuilder,
}

impl PgCatalogAttributeBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            attrelid: UInt32Builder::new(),
            attname: StringBuilder::new(),
            atttypid: UInt32Builder::new(),
            attstattarget: Int32Builder::new(),
            attlen: Int16Builder::new(),
            attnum: Int16Builder::new(),
            attndims: UInt32Builder::new(),
            attcacheoff: Int32Builder::new(),
            atttypmod: Int64Builder::new(),
            attbyval: BooleanBuilder::new(),
            attalign: StringBuilder::new(),
            attstorage: StringBuilder::new(),
            attcompression: StringBuilder::new(),
            attnotnull: BooleanBuilder::new(),
            atthasdef: BooleanBuilder::new(),
            atthasmissing: BooleanBuilder::new(),
            attidentity: StringBuilder::new(),
            attgenerated: StringBuilder::new(),
            attisdropped: BooleanBuilder::new(),
            attislocal: BooleanBuilder::new(),
            attinhcount: UInt32Builder::new(),
            attcollation: UInt32Builder::new(),
            attacl: StringBuilder::new(),
            attoptions: StringBuilder::new(),
            attfdwoptions: StringBuilder::new(),
            attmissingval: StringBuilder::new(),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.attrelid.finish()));
        columns.push(Arc::new(self.attname.finish()));
        columns.push(Arc::new(self.atttypid.finish()));
        columns.push(Arc::new(self.attstattarget.finish()));
        columns.push(Arc::new(self.attlen.finish()));
        columns.push(Arc::new(self.attnum.finish()));
        columns.push(Arc::new(self.attndims.finish()));
        columns.push(Arc::new(self.attcacheoff.finish()));
        columns.push(Arc::new(self.atttypmod.finish()));
        columns.push(Arc::new(self.attbyval.finish()));
        columns.push(Arc::new(self.attalign.finish()));
        columns.push(Arc::new(self.attstorage.finish()));
        columns.push(Arc::new(self.attcompression.finish()));
        columns.push(Arc::new(self.attnotnull.finish()));
        columns.push(Arc::new(self.atthasdef.finish()));
        columns.push(Arc::new(self.atthasmissing.finish()));
        columns.push(Arc::new(self.attidentity.finish()));
        columns.push(Arc::new(self.attgenerated.finish()));
        columns.push(Arc::new(self.attisdropped.finish()));
        columns.push(Arc::new(self.attislocal.finish()));
        columns.push(Arc::new(self.attinhcount.finish()));
        columns.push(Arc::new(self.attcollation.finish()));
        columns.push(Arc::new(self.attacl.finish()));
        columns.push(Arc::new(self.attoptions.finish()));
        columns.push(Arc::new(self.attfdwoptions.finish()));
        columns.push(Arc::new(self.attmissingval.finish()));

        columns
    }
}

pub struct PgCatalogAttributeProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogAttributeProvider {
    pub fn new() -> Self {
        let mut builder = PgCatalogAttributeBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAttributeProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::UInt32, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", DataType::UInt32, false),
            Field::new("attstattarget", DataType::Int32, false),
            Field::new("attlen", DataType::Int16, false),
            Field::new("attnum", DataType::Int16, true),
            Field::new("attndims", DataType::UInt32, false),
            Field::new("attcacheoff", DataType::Int32, false),
            Field::new("atttypmod", DataType::Int64, false),
            Field::new("attbyval", DataType::Boolean, false),
            Field::new("attalign", DataType::Utf8, false),
            Field::new("attstorage", DataType::Utf8, false),
            Field::new("attcompression", DataType::Utf8, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("atthasmissing", DataType::Boolean, false),
            Field::new("attidentity", DataType::Utf8, false),
            Field::new("attgenerated", DataType::Utf8, false),
            Field::new("attisdropped", DataType::Boolean, false),
            Field::new("attislocal", DataType::Boolean, false),
            Field::new("attinhcount", DataType::UInt32, false),
            Field::new("attcollation", DataType::UInt32, false),
            Field::new("attacl", DataType::Utf8, true),
            Field::new("attoptions", DataType::Utf8, true),
            Field::new("attfdwoptions", DataType::Utf8, true),
            Field::new("attmissingval", DataType::Utf8, true),
        ]))
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[],
            self.schema().clone(),
            projection.cloned(),
        ).unwrap()))
    }
}

struct PgCatalogAttrdefBuilder {
    oid: UInt32Builder,
    adrelid: UInt32Builder,
    adnum: UInt32Builder,
    adbin: StringBuilder,
}

impl PgCatalogAttrdefBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            oid: UInt32Builder::new(),
            adrelid: UInt32Builder::new(),
            adnum: UInt32Builder::new(),
            adbin: StringBuilder::new(),
        }
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];
        columns.push(Arc::new(self.oid.finish()));
        columns.push(Arc::new(self.adrelid.finish()));
        columns.push(Arc::new(self.adnum.finish()));
        columns.push(Arc::new(self.adbin.finish()));

        columns
    }
}

pub struct PgCatalogAttrdefProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl PgCatalogAttrdefProvider {
    pub fn new() -> Self {
        let builder = PgCatalogAttrdefBuilder::new();

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for PgCatalogAttrdefProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("adrelid", DataType::UInt32, false),
            Field::new("adnum", DataType::UInt32, false),
            Field::new("adbin", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[],
            self.schema().clone(),
            projection.cloned(),
        ).unwrap()))
    }
}
macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<datafusion::arrow::array::GenericStringArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<datafusion::arrow::array::GenericStringArray<$T>>()
                ))
            })?
    }};
}

pub fn create_pg_get_expr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let inputs = downcast_string_arg!(args[0], "input", i32);

        let result = inputs
            .iter()
            .map::<Option<String>, _>(|_| None)
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_catalog.pg_get_expr",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}
