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

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{Filter, Join, LogicalPlan, Projection, TableProviderFilterPushDown, TableScan},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

use crate::index::SearchIndex;

/// Performs a search on a given [`SearchIndex`] and combine with the underlying [`TableProvider`]
/// if required by filters or additional columns in the projection.
#[derive(Debug, Clone)]
pub struct SearchQueryProvider {
    pub search_index_query: Arc<dyn TableProvider>,
    pub table_provider: Arc<dyn TableProvider>,
    pub primary_key: Vec<String>,
    pub pre_limit: Option<usize>,
}

impl SearchQueryProvider {
    pub fn new(
        search_index_query: Arc<dyn TableProvider>,
        table_provider: Arc<dyn TableProvider>,
        primary_key: Vec<String>,
    ) -> Self {
        Self {
            search_index_query,
            primary_key,
            table_provider,
            pre_limit: None,
        }
    }

    pub fn try_from_index(
        search_index: Arc<dyn SearchIndex>,
        table_provider: Arc<dyn TableProvider>,
        query: String,
        limit: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let search_index_query = search_index.query_table_provider(query.as_str())?;
        Ok(Self::new(
            search_index_query,
            table_provider,
            search_index
                .primary_fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        ))
    }
}

#[async_trait]
impl TableProvider for SearchQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields = self
            .search_index_query
            .schema()
            .fields()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        fields.extend(self.table_provider.schema().fields().into_iter().cloned());

        let mut fields = fields.into_iter().collect::<Vec<_>>();
        fields.sort_unstable();

        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // If one of the two has a pushdown threshold, it can be used.
        // TODO: anything we have columns for, should at least be inexact
        Ok(self
            .table_provider
            .supports_filters_pushdown(filters)?
            .iter()
            .zip(self.search_index_query.supports_filters_pushdown(filters)?)
            .map(|(a, b)| match (a, b) {
                (TableProviderFilterPushDown::Exact, _)
                | (_, TableProviderFilterPushDown::Exact) => TableProviderFilterPushDown::Exact,
                (TableProviderFilterPushDown::Inexact, _)
                | (_, TableProviderFilterPushDown::Inexact) => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let search_index = LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("search_index"),
            Arc::new(DefaultTableSource::new(Arc::clone(
                &self.search_index_query,
            ))),
            None,
            vec![],
            self.pre_limit,
        )?);

        let base_table = LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("base_table"),
            Arc::new(DefaultTableSource::new(
                Arc::clone(&self.table_provider) as Arc<dyn TableProvider>
            )),
            None,
            vec![],
            None,
        )?);

        // Build join conditions based on primary keys
        let on: Vec<(Expr, Expr)> = self
            .primary_key
            .iter()
            .map(|c| {
                (
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("search_index")),
                        c,
                    )),
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("base_table")),
                        c,
                    )),
                )
            })
            .collect();

        let join_schema = Arc::new(search_index.schema().join(base_table.schema())?);

        let join = LogicalPlan::Join(Join {
            left: search_index.into(),
            right: base_table.into(),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            on,
            filter: None,
            schema: Arc::clone(&join_schema),
            null_equality: NullEquality::NullEqualsNothing,
        });

        // Pick which columns we want.
        // Any column in index, use instead of base table.
        // This helps physical planning reduce use of base table.
        let lp = LogicalPlan::Projection(Projection::try_new(
            join_schema
                .columns()
                .iter()
                .filter_map(|c| {
                    if c.relation
                        .as_ref()
                        .is_some_and(|tbl| tbl.to_string() == "search_index".to_string())
                    {
                        return Some(Expr::Column(c.clone()));
                    }

                    // Add from base table, if not in search index.
                    if self
                        .search_index_query
                        .schema()
                        .column_with_name(c.name())
                        .is_none()
                    {
                        return Some(Expr::Column(c.clone()));
                    }

                    None
                })
                .collect(),
            join.into(),
        )?);

        let filtered = if let Some(f) = filters.iter().cloned().reduce(Expr::and) {
            LogicalPlan::Filter(Filter::try_new(f, lp.into())?)
        } else {
            lp
        };

        // TODO add back match (needs to be in schema too).
        // let with_columns = self.add_match_column(inner_proj.as_ref(), sort)?;

        // Final projection to match requested schema
        let schema_proj: DFSchema = match projection {
            None => {
                DFSchema::from_unqualified_fields(self.schema().fields.clone(), HashMap::default())?
            }
            Some(idx) => {
                let projected = self
                    .schema()
                    .project(idx)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                DFSchema::from_unqualified_fields(projected.fields, HashMap::default())?
            }
        };

        let final_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(filtered),
            schema_proj.into(),
        ));
        state.create_physical_plan(&final_proj).await
    }
}
