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

use crate::search::full_text::udtf::{
    TEXT_SEARCH_UDTF_NAME, TextSearchTableFunc, TextSearchTableFuncArgs,
};
use datafusion::common::Column;
use datafusion::datasource::provider_as_source;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::logical_expr::{LogicalPlanBuilder, SortExpr};
use datafusion::prelude::{DataFrame, Expr as LogicalExpr};

use datafusion::sql::sqlparser::ast::Ident;
use datafusion::{execution::SendableRecordBatchStream, sql::TableReference};
use itertools::Itertools;
use search::generation::text_search::FullTextSearchFieldIndex;
use search::generation::{CandidateGeneration, Error as SearchGenerationError};
use search::{SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME};
use snafu::ResultExt;
use tonic::async_trait;

use crate::datafusion::DataFusion;

pub struct TextSearchCandidate {
    inner: Arc<FullTextSearchFieldIndex>,
    df: Arc<DataFusion>,
    tbl: TableReference,
}

impl TextSearchCandidate {
    pub(crate) fn new(
        inner: Arc<FullTextSearchFieldIndex>,
        df: Arc<DataFusion>,
        tbl: TableReference,
    ) -> Self {
        Self { inner, df, tbl }
    }

    fn construct_udtf_sql_dataframe(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> Result<DataFrame, DataFusionError> {
        let udtf_args = TextSearchTableFunc::to_expr(&TextSearchTableFuncArgs {
            tbl: self.tbl.clone(),
            query,
            column: Some(self.inner.field.clone()),
            limit: Some(limit),
            include_score: Some(true),
        });

        let udtf_provider = self
            .df
            .ctx
            .table_function(TEXT_SEARCH_UDTF_NAME)?
            .create_table_provider(udtf_args.as_slice())?;

        let mut udtf = DataFrame::new(
            self.df.ctx.state(),
            LogicalPlanBuilder::scan(
                format!("{TEXT_SEARCH_UDTF_NAME}()"),
                provider_as_source(udtf_provider),
                None,
            )?
            .build()?,
        );

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

        let projection: Vec<String> = self
            .inner
            .primary_key
            .iter()
            .cloned()
            .chain(addition_projection.iter().map(|&e| format!("{e}")))
            .chain([
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                format!("\"{}\" as {SEARCH_VALUE_COLUMN_NAME}", self.inner.field),
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

#[async_trait]
impl CandidateGeneration for TextSearchCandidate {
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
        let query = self.df.query_from_logical_plan(dataframe.logical_plan());

        tracing::trace!("running SQL: {}", query.display_sql());

        Ok(query
            .run()
            .await
            .boxed()
            .map_err(|e| SearchGenerationError::InternalError { source: e })?
            .data)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> search::generation::Result<Vec<bool>> {
        Ok((0..filters.len()).map(|_| false).collect::<Vec<_>>())
    }

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, projection: &[&Expr]) -> search::generation::Result<Vec<bool>> {
        let columns = self.inner.all_columns();

        let cols_found = projection
            .iter()
            .map(|expr| {
                if let Expr::Identifier(Ident { value, .. }) = expr {
                    columns.contains(value) || value == SEARCH_SCORE_COLUMN_NAME
                } else {
                    false
                }
            })
            .collect();

        Ok(cols_found)
    }

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String {
        self.inner.field.clone()
    }
}
