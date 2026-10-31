/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `UPDATE` / `DELETE` for a table that renders its expressions as SQL.
//!
//! The analyzer has replaced integer `+` / `-` / `*` with checked kernels by
//! the time `DataFusion` hands an `UPDATE`'s assignments and a `DELETE`'s
//! filters to the table. A table that writes them into SQL for another
//! database would send `__spice_checked_add(..)`, which that database does not
//! have. A table that evaluates them in `DataFusion` must keep the kernels, so
//! this is opt-in per table, not a property of the write path.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_federation::FederatedTableProviderAdaptor;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

use super::restore_integer_operators;

/// Wraps `provider`, a table that renders `UPDATE` / `DELETE` expressions as
/// SQL, so it receives integer `+` / `-` / `*` instead of the checked kernels.
///
/// The database then applies its own overflow rules, as it does for a
/// federated read. Every other call reaches `provider` unchanged.
#[must_use]
pub fn sql_rendered_dml(provider: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    // Federation finds a table by downcasting to the adaptor, so keep it
    // outermost, as `metadata_enriched_table_provider` does.
    if let Some(adaptor) = provider.downcast_ref::<FederatedTableProviderAdaptor>() {
        let Some(table_provider) = &adaptor.table_provider else {
            return provider;
        };
        return Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            Arc::clone(&adaptor.source),
            sql_rendered_dml(Arc::clone(table_provider)),
        ));
    }

    SpiceTable::over(Arc::new(SqlRenderedDml), provider)
}

/// The layer [`sql_rendered_dml`] stacks.
#[derive(Debug)]
struct SqlRenderedDml;

#[async_trait]
impl TableLayer for SqlRenderedDml {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        match walk {
            // Reads, inserts and indexes pass through untouched.
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
            // A walk that would run `UPDATE` / `DELETE` on the table beneath
            // must not skip the rewrite.
            LayerWalk::Write | LayerWalk::RetentionDelete => None,
        }
    }

    async fn delete_from(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        below.delete_from(state, restore_all(filters)?).await
    }

    async fn update(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let assignments = assignments
            .into_iter()
            .map(|(column, expr)| restore_integer_operators(expr).map(|expr| (column, expr)))
            .collect::<Result<Vec<_>>>()?;
        below
            .update(state, assignments, restore_all(filters)?)
            .await
    }
}

fn restore_all(exprs: Vec<Expr>) -> Result<Vec<Expr>> {
    exprs.into_iter().map(restore_integer_operators).collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::error::Result;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::{SessionContext, col, lit};
    use parking_lot::Mutex;

    use super::sql_rendered_dml;
    use crate::checked_arithmetic::op::checked_add_udf;

    /// A table that records the expressions an `UPDATE` or `DELETE` hands it.
    #[derive(Debug)]
    struct RecordingTable {
        schema: SchemaRef,
        received: Mutex<Vec<Expr>>,
    }

    impl RecordingTable {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                received: Mutex::new(Vec::new()),
            })
        }

        fn empty_plan(&self) -> Arc<dyn ExecutionPlan> {
            Arc::new(EmptyExec::new(Arc::clone(&self.schema)))
        }
    }

    #[async_trait]
    impl TableProvider for RecordingTable {
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
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self.empty_plan())
        }

        async fn delete_from(
            &self,
            _state: &dyn Session,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.received.lock().extend(filters);
            Ok(self.empty_plan())
        }

        async fn update(
            &self,
            _state: &dyn Session,
            assignments: Vec<(String, Expr)>,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let mut received = self.received.lock();
            received.extend(assignments.into_iter().map(|(_, expr)| expr));
            received.extend(filters);
            Ok(self.empty_plan())
        }
    }

    /// `UPDATE .. SET id = id + 1 WHERE id + 1 > 2` and
    /// `DELETE .. WHERE id + 1 > 2`, with the `+` as the analyzer leaves it.
    /// Returns that checked `+`.
    async fn update_and_delete(table: &Arc<dyn TableProvider>) -> Expr {
        let state = SessionContext::new().state();
        let checked = Expr::ScalarFunction(ScalarFunction::new_udf(
            checked_add_udf(),
            vec![col("id"), lit(1_i64)],
        ));
        table
            .update(
                &state,
                vec![("id".to_string(), checked.clone())],
                vec![checked.clone().gt(lit(2_i64))],
            )
            .await
            .expect("the update plans");
        table
            .delete_from(&state, vec![checked.clone().gt(lit(2_i64))])
            .await
            .expect("the delete plans");
        checked
    }

    /// Before this layer, `UPDATE a SET b = b + 1` on a `write_through`
    /// `PostgreSQL` dataset sent `__spice_checked_add("b", 1)` to `PostgreSQL`
    /// and failed with `db error`.
    #[tokio::test]
    async fn a_sql_rendered_table_receives_integer_operators() {
        let recording = RecordingTable::new();
        let table = sql_rendered_dml(Arc::clone(&recording) as Arc<dyn TableProvider>);

        update_and_delete(&table).await;

        let plus = col("id") + lit(1_i64);
        assert_eq!(
            *recording.received.lock(),
            vec![
                plus.clone(),
                plus.clone().gt(lit(2_i64)),
                plus.gt(lit(2_i64)),
            ],
            "the table beneath must receive `+`, not the checked kernel"
        );
    }

    /// The contrast: a table that evaluates the expressions itself is not
    /// wrapped, and keeps the checked kernel, so an overflowing `UPDATE` or
    /// `DELETE` there is still an error, not a wrap.
    #[tokio::test]
    async fn an_unwrapped_table_keeps_the_checked_kernel() {
        let recording = RecordingTable::new();
        let table = Arc::clone(&recording) as Arc<dyn TableProvider>;

        let checked = update_and_delete(&table).await;

        assert_eq!(
            *recording.received.lock(),
            vec![
                checked.clone(),
                checked.clone().gt(lit(2_i64)),
                checked.gt(lit(2_i64)),
            ]
        );
    }
}
