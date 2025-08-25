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

use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema, JoinConstraint, JoinType},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{Filter, Join, LogicalPlan, Projection, Sort, SortExpr, TableScan},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    generation::text_search::{index::FullTextDatabaseIndex, query::FullTextSearchQuery},
};

/// [`TextSearchIndexProvider`] performs full text search on a column within a [`FullTextDatabaseIndex`] for a given query, and augments the results with the `underlying`, associated [`TableProvider`] (i.e. [`FullTextDatabaseIndex`] is an index on the `underlying` [`TableProvider`]).
#[derive(Debug, Clone)]
pub struct TextSearchIndexProvider {
    pub query: String,
    pub column: String,
    pub pre_limit: Option<usize>,
    pub index: FullTextDatabaseIndex,
    pub underlying: Arc<dyn TableProvider>,
}

impl TextSearchIndexProvider {
    // Returns true if [`FullTextSearchQuery`] can handle...
    fn text_index_table_is_sufficient(
        &self,
        index_table: &FullTextSearchQuery,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<bool, ArrowError> {
        Ok(
            self.text_index_table_has_projection(index_table, projection)?
                && Self::text_index_table_has_filter_columns(index_table, filters),
        )
    }

    fn text_index_table_has_filter_columns(
        index_table: &FullTextSearchQuery,
        filters: &[Expr],
    ) -> bool {
        let columns_in_index: Vec<Column> = index_table
            .schema()
            .fields()
            .iter()
            .map(|f| Column::from_qualified_name(f.name()))
            .collect();
        let columns_in_index_ref: HashSet<&Column> = columns_in_index.iter().collect();

        filters
            .iter()
            .all(|f| f.column_refs().is_subset(&columns_in_index_ref))
    }

    fn text_index_table_has_projection(
        &self,
        index_table: &FullTextSearchQuery,
        projection: Option<&Vec<usize>>,
    ) -> Result<bool, ArrowError> {
        let Some(proj_idx) = projection else {
            // Only way for full projection to be handled by index is if table only has: primary key, column to search on, score column.
            return Ok(self.schema().fields.len() == self.index.primary_key.len() + 2);
        };
        let projection_names: HashSet<String> = self
            .schema()
            .project(proj_idx)?
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let columns_in_index: HashSet<String> = index_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        Ok(projection_names.is_subset(&columns_in_index))
    }

    // Reduce a projection to the columns the base table should include. Will ignore
    // - `SEARCH_SCORE_COLUMN_NAME`
    // - Search column (avoid duplicating from index).
    fn projection_for_underlying(&self, projection: Option<&Vec<usize>>) -> Vec<usize> {
        let search_column_is_pk = self.index.column_is_part_of_pk(&self.column);

        // Continue to include the search column if the search column is the primary key
        // This retains the column for the later table join operations
        let search_column_idx = (!search_column_is_pk)
            .then_some(
                self.schema()
                    .column_with_name(&self.column)
                    .map(|(idx, _)| idx),
            )
            .flatten();

        let search_score_idx = self
            .schema()
            .column_with_name(SEARCH_SCORE_COLUMN_NAME)
            .map(|(idx, _)| idx);

        // find the projection indexes for the primary key columns
        let row_id_projections = self
            .index
            .primary_key
            .iter()
            .filter_map(|pk| self.schema().column_with_name(pk).map(|(idx, _)| idx))
            .collect::<Vec<_>>();

        // join the underlying projection with the required row_id projections. Ensure they are de-duplicated
        // if projection is supplied but doesn't include the row_id, the later table join will fail
        let projection: HashSet<usize> = projection
            .cloned()
            .map(|proj| proj.into_iter().chain(row_id_projections).collect())
            .unwrap_or((0..self.schema().fields().len()).collect()); // if not projection is supplied, we default to every column anyway

        projection
            .into_iter()
            .filter(|&idx| {
                !(search_column_idx.is_some_and(|i| i == idx)
                    || search_score_idx.is_some_and(|i| i == idx))
            })
            .collect()
    }

    fn construct_join(
        &self,
        index_table_scan: LogicalPlan,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<LogicalPlan, DataFusionError> {
        tracing::trace!("Projection for underlying: {projection:?}");
        let underlying_table_scan = LogicalPlan::TableScan(TableScan::try_new(
            "base_table",
            Arc::new(DefaultTableSource::new(
                Arc::clone(&self.underlying) as Arc<dyn TableProvider>
            )),
            Some(self.projection_for_underlying(projection)),
            vec![],
            None,
        )?);

        let primary_key_columns: Vec<Column> = self
            .index
            .primary_key
            .iter()
            .map(|pk| Column::new_unqualified(pk.clone()))
            .collect();
        let pk_col_refs: HashSet<&Column> = primary_key_columns.iter().collect();

        // If the filter affects the primary key, we must apply after we have removed the duplicate primary key column.
        let (pre_join_filters, post_join_filters): (Vec<Expr>, Vec<Expr>) = filters
            .iter()
            .cloned()
            .partition(|f| f.column_refs().is_disjoint(&pk_col_refs)); // If disjoint, can safely apply filter pre join.

        let on: Vec<(Expr, Expr)> = primary_key_columns
            .iter()
            .map(|c| {
                (
                    Expr::Column(Column::new_unqualified(c.name())),
                    Expr::Column(Column::new_unqualified(c.name())),
                )
            })
            .collect();
        let join_schema = index_table_scan
            .schema()
            .join(underlying_table_scan.schema())?;
        let join = LogicalPlan::Join(Join {
            left: Arc::new(index_table_scan),
            right: Arc::new(underlying_table_scan),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            on,
            filter: pre_join_filters.into_iter().reduce(Expr::and),
            schema: join_schema.into(),
            null_equals_null: false,
        });

        // DataFusion will not deduplicate the `Join::on` key. For simplicity with non-join
        // case, we will remove first.
        let deduped_schema = DFSchema::new_with_metadata(
            join.schema()
                .iter()
                .filter(|(tbl, f)| {
                    !(self.index.primary_key.contains(f.name())
                        && tbl.is_some_and(|t| *t == TableReference::parse_str("base_table")))
                })
                .map(|(tbl, f)| (tbl.cloned(), Arc::clone(f)))
                .collect(),
            HashMap::default(),
        )?;

        let proj = LogicalPlan::Projection(Projection::new_from_schema(
            join.into(),
            deduped_schema.into(),
        ));

        if let Some(filter) = post_join_filters.into_iter().reduce(Expr::and) {
            Ok(LogicalPlan::Filter(Filter::try_new(filter, proj.into())?))
        } else {
            Ok(proj)
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for TextSearchIndexProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields: Vec<_> = self.underlying.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME.to_string(),
            arrow_schema::DataType::Float64,
            false,
        )));
        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let Some(field_index) = self
            .index
            .full_text_search_field_index(self.column.as_str())
            .await
            .ok()
        else {
            // This shouldn't be reachable as we checked `col` above. Instead of `unreachable!`, provide user friendly error.
            return Err(DataFusionError::Internal(format!(
                "text_search() missing required search field {}",
                self.column.as_str()
            )));
        };
        let index_table = Arc::new(FullTextSearchQuery {
            index: field_index,
            query: self.query.clone(),
            pre_limit: self.pre_limit,
        });
        let index_table_scan = LogicalPlan::TableScan(TableScan::try_new(
            "index_table",
            Arc::new(DefaultTableSource::new(
                Arc::clone(&index_table) as Arc<dyn TableProvider>
            )),
            None,
            vec![],
            None,
        )?);

        // Only join on base table if required.
        let base_logical_plan: LogicalPlan = if self
            .text_index_table_is_sufficient(&index_table, projection, filters)
            .map_err(|e| DataFusionError::ArrowError(e, None))?
        {
            // Let DataFusion handle pushing filters.
            tracing::trace!("Index table is sufficient");
            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                LogicalPlan::Filter(Filter::try_new(filter, index_table_scan.into())?)
            } else {
                index_table_scan
            }
        } else {
            tracing::trace!("Index table is insufficient");
            self.construct_join(index_table_scan, projection, filters)?
        };

        let sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr {
                expr: Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                asc: false,
                nulls_first: false,
            }],
            input: Arc::new(base_logical_plan),
            fetch: limit,
        });

        let schema_proj: SchemaRef = match projection {
            None => self.schema(),
            Some(idx) => self
                .schema()
                .project(idx)
                .map_err(|e| DataFusionError::ArrowError(e, None))?
                .into(),
        };

        let final_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(sort),
            Arc::new(DFSchema::from_unqualified_fields(
                schema_proj.fields().clone(),
                HashMap::default(),
            )?),
        ));

        state.create_physical_plan(&final_proj).await
    }
}
