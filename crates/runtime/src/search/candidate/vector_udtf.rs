/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::embeddings::udtf::{
    VECTOR_SEARCH_UDTF_NAME, VectorSearchTableFunc, VectorSearchTableFuncArgs,
};
use datafusion::common::Column;
use datafusion::datasource::provider_as_source;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::logical_expr::{LogicalPlanBuilder, SortExpr};
use datafusion::prelude::{DataFrame, Expr as LogicalExpr};

use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use datafusion_expr::sqlparser::ast::Ident;
use itertools::Itertools;
use search::generation::{CandidateGeneration, Error as SearchGenerationError};
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use snafu::ResultExt;

use crate::datafusion::DataFusion;

pub struct VectorUDTFGeneration {
    df: Arc<DataFusion>,
    tbl: TableReference,
    primary_keys: Vec<String>,
    embedding_column: String,
    is_chunked: bool,
}

impl VectorUDTFGeneration {
    pub fn new(
        df: &Arc<DataFusion>,
        tbl: &TableReference,
        primary_keys: &[String],
        embedding_column: &str,
        is_chunked: bool,
    ) -> Self {
        Self {
            df: Arc::clone(df),
            tbl: tbl.clone(),
            primary_keys: primary_keys.to_vec(),
            embedding_column: embedding_column.to_string(),
            is_chunked,
        }
    }
    fn construct_udtf_sql_dataframe(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> Result<DataFrame, DataFusionError> {
        let pre_limit = if opt_filters.is_empty() {
            Some(limit)
        } else {
            None
        };

        let udtf_args = VectorSearchTableFunc::to_expr(&VectorSearchTableFuncArgs {
            tbl: self.tbl.clone(),
            query,
            column: Some(self.embedding_column.clone()),
            limit: pre_limit,
            include_score: Some(true),
        });
        let udtf_provider = self
            .df
            .ctx
            .table_function(VECTOR_SEARCH_UDTF_NAME)?
            .create_table_provider(udtf_args.as_slice())?;
        let mut udtf = DataFrame::new(
            self.df.ctx.state(),
            LogicalPlanBuilder::scan(
                format!("{VECTOR_SEARCH_UDTF_NAME}()"),
                provider_as_source(udtf_provider),
                None,
            )?
            .build()?,
        );

        // Parsing logical [`Expr`] are schema dependent.
        let filters: Vec<LogicalExpr> = opt_filters
            .iter()
            .map(|f| {
                self.df
                    .ctx
                    .state()
                    .create_logical_expr(f.to_string().as_str(), udtf.schema())
            })
            .collect::<Result<Vec<_>, _>>()?;

        if let Some(filter) = filters.iter().cloned().reduce(LogicalExpr::and) {
            udtf = udtf.filter(filter)?;
        }

        let search_value = if self.is_chunked {
            "match".to_string()
        } else {
            self.embedding_column.clone()
        };

        let projection: Vec<String> = self
            .primary_keys
            .iter()
            .cloned()
            .chain(addition_projection.iter().filter_map(|&e| match e {
                Expr::Identifier(Ident { value, .. }) => Some(value.to_string()),
                _ => None,
            }))
            .chain([
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                format!("\"{search_value}\" as {SEARCH_VALUE_COLUMN_NAME}"),
            ])
            .unique()
            .collect();

        let projection_ref = projection.iter().map(String::as_str).collect::<Vec<_>>();

        udtf.select_exprs(&projection_ref)?
            .sort(vec![SortExpr::new(
                LogicalExpr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                false,
                false,
            )])?
            .limit(0, Some(limit))
    }
}

#[async_trait::async_trait]
impl CandidateGeneration for VectorUDTFGeneration {
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> search::generation::Result<SendableRecordBatchStream> {
        let dataframe = self
            .construct_udtf_sql_dataframe(query, opt_filters, addition_projection, limit)
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?;
        let data = self
            .df
            .query_from_logical_plan(dataframe.logical_plan())
            .run()
            .await
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?
            .data;

        Ok(data)
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> Result<Vec<bool>, SearchGenerationError> {
        Ok(vec![])
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, _projection: &[&Expr]) -> Result<Vec<bool>, SearchGenerationError> {
        Ok(vec![])
    }

    fn value_derived_from(&self) -> String {
        self.embedding_column.clone()
    }
}
