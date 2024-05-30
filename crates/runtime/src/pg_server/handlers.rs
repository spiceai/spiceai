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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::*;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::QueryParser;
use pgwire::api::stmt::StoredStatement;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tokio::sync::{Mutex, RwLock};

use crate::datafusion::DataFusion;
use crate::pg_server::datatypes::into_pg_type;

use super::datatypes;


// use crate::datatypes::{self, into_pg_type};



// Protocol level transaction support: https://github.com/sunng87/pgwire/issues/170

pub(crate) struct DfSessionService {
    df: Arc<RwLock<DataFusion>>,
    parser: Arc<Parser>,
}

impl DfSessionService {
    pub fn new(df: Arc<RwLock<DataFusion>>) -> DfSessionService {
        // let session_context = Arc::new(Mutex::new(session_context));
        let parser = Arc::new(Parser {
            df: df.clone(),
        });
        DfSessionService {
            df,
            parser
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        
        println!("SimpleQueryHandler do_query: {:?}", query);

        match self.df.read().await.ctx
            .sql(query)
            .await
            .map_err(to_pg_wire_error) {
                Ok(df) => {
                    match datatypes::encode_dataframe(df).await {
                        Ok(resp) => {
                            Ok(vec![Response::Query(resp)])
                        },
                        Err(e) => {
                            println!("Internal encoding error: {:?}", e);
                            Ok(vec![Response::EmptyQuery])
                        }
                    }
                },
                Err(e) => {
                    println!("error handled internally: {:?}", e);
                    Ok(vec![Response::EmptyQuery])
                    // Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    //     "ERROR".to_owned(),
                    //     "XX000".to_owned(),
                    //     format!("{:?}", e),
                    // )))])
                }

            }
    }
}

pub(crate) struct Parser {
    //session_context: Arc<Mutex<SessionContext>>,
    df: Arc<RwLock<DataFusion>>
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = LogicalPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {

       //self.df.read().await.ctx.state();

       // https://github.com/sunng87/pgwire/issues/79

        println!("parse_sql: {sql}");

        let mut sql = sql.to_string();

        let transaction_keywords = ["BEGIN", "COMMIT", "ROLLBACK"];

        // if sql.trim().is_empty() {
        //     //let empty_schema = DFSchema::empty();
        //     let empty_df = self.df.read().await.ctx
        //         .read_empty().expect("should create empty dataframe");

        //     let plan = empty_df.logical_plan();
        //     return Ok(plan.clone());
        // }

        if sql.trim_start().to_uppercase().starts_with("BEGIN") {
            sql = "start transaction read only".to_string();
        }

        // Check if the SQL query starts with any transaction-related keyword
        if transaction_keywords.iter().any(|&kw| sql.trim_start().to_uppercase().starts_with(kw)) {
            // println!("Ignoring transaction-related query: {sql}");
            // https://github.com/apache/datafusion/blob/904f0db73cf2c0049822c7045e09824a7453a02d/datafusion/sql/tests/sql_integration.rs#L352
            //sql = "start transaction read only".to_string();

            // sql = "SET TIMEZONE TO 'UTC'".to_string();

            // let empty_schema = DFSchema::empty();
            // let empty_schema_ref = DFSchemaRef::new(empty_schema);

            // let empty_df = self.df.read().await.ctx
            // .read_empty().unwrap();

            // let plan = empty_df.logical_plan();


            // // let plan = self.df.read().await.ctx
            // //     .state()
            // //     .create_logical_plan("")
            // //     .await
            // //     .unwrap();
        
            // return Ok(plan.clone());

            // self.df.read().await.ctx
            //     .sql(&sql)
            //     .await
            //     .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            // return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
            //     std::io::ErrorKind::Unsupported,
            //     "Transaction-related queries are ignored",
            // )))); // or return an appropriate empty/default Statement

            //let empty_plan = LogicalPlanBuilder::empty(false).build().map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    
            
            // let schema = DFSchema::empty();
            // let empty_plan = LogicalPlan::Projection {
            //     expr: vec![],
            //     input: Arc::new(LogicalPlan::EmptyRelation {
            //         produce_one_row: false,
            //         schema,
            //     }),
            //     schema,
            // };
            
            // return Ok(empty_plan);

        }



        // let context = self.session_context.lock().await;
        // let state = context.state();

        let state = self.df.read().await.ctx.state();

        let logical_plan = state
            .create_logical_plan(&sql)
            .await
            .map_err(to_pg_wire_error)?;
        let optimised = state
            .optimize(&logical_plan)
            .map_err(to_pg_wire_error)?;

        println!("parse_sql: {sql} -> {:?}", optimised);
        Ok(optimised)
    }
}

#[async_trait]
impl ExtendedQueryHandler for DfSessionService {
    type Statement = LogicalPlan;

    type QueryParser = Parser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement;

        println!("do_describe_statement: {:?}", plan);

        let schema = plan.schema();
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref())?;
        let params = plan
            .get_parameter_types()
            .map_err(to_pg_wire_error)?;

        dbg!(&params);
        let mut param_types = Vec::with_capacity(params.len());
        for param_type in params.into_values() {
            if let Some(datatype) = param_type {
                let pgtype = into_pg_type(&datatype)?;
                param_types.push(pgtype);
            } else {
                param_types.push(Type::UNKNOWN);
            }
        }

        println!("done: {:?}", fields);

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &target.statement.statement;
        let schema = plan.schema();
        let fields = datatypes::df_schema_to_pg_fields(schema.as_ref())?;

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let plan = &portal.statement.statement;

        println!("do_query: {:?}", plan);

        let param_values = datatypes::deserialize_parameters(
            portal,
            &plan
                .get_parameter_types()
                .map_err(to_pg_wire_error)?
                .values()
                .map(|v| v.as_ref())
                .collect::<Vec<Option<&DataType>>>(),
        )?;

        let plan = plan
            .clone()
            .replace_params_with_values(&param_values)
            .map_err(to_pg_wire_error)?;

        match self
            .df.read().await.ctx
            .execute_logical_plan(plan)
            .await
            .map_err(to_pg_wire_error) {
                Ok(df) => {
                    match datatypes::encode_dataframe(df).await {
                        Ok(resp) => {
                            Ok(Response::Query(resp))
                        },
                        Err(e) => {
                            println!("Internal encoding error: {:?}", e);
                            Ok(Response::EmptyQuery)
                        }
                    }
                },
                Err(e) => {
                    println!("error handled internally: {:?}", e);
                    Ok(Response::EmptyQuery)
                    // Ok(Response::Error(Box::new(ErrorInfo::new(
                    //     "ERROR".to_owned(),
                    //     "XX000".to_owned(),
                    //     format!("{:?}", e),
                    // )))
                }
            }
    }
}

fn to_pg_wire_error(e: DataFusionError) -> PgWireError {
    
    println!("error: {:?}", e);
    PgWireError::ApiError(Box::new(e))

}