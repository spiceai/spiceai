/*
Copyright 2026, Spice AI, Inc.

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

//! Local Cayenne DML handler plus MERGE logical-input preparation helpers.
//!
//! # MERGE planning flow
//!
//! Only one statement shape is supported (enforced upstream by
//! `datafusion_dml`'s MERGE parser): a single `WHEN MATCHED THEN UPDATE SET …`
//! clause with an `ON` condition that is a conjunction of
//! `target_col = source_col` equalities. `WHEN NOT MATCHED … INSERT` and
//! `WHEN MATCHED … DELETE` are rejected at parse time.
//!
//! Planning proceeds in three stages:
//!
//! ```text
//! 1. Logical rewrite      build_local_merge_plan_input: INNER JOIN target ⋈ source
//!    (plan time)          on the ON keys, then project to the target schema with
//!                         SET expressions substituted — one row per matched
//!                         target row, already carrying its updated values.
//! 2. Extension node       the joined/projected plan + MergeParams are wrapped in
//!                         the generic datafusion_dml::DmlExtensionNode.
//! 3. Physical planning    DmlExtensionPlanner calls CayenneDmlHandler::merge_exec,
//!                         which wraps the planned join input in CayenneMergeExec
//!                         (validate no duplicate keys → delete matched rows →
//!                         re-insert updated rows).
//! ```
//!
//! [`CayenneDmlHandler`] is the single-node handler; the runtime crate has a
//! distributed variant that reuses [`build_local_merge_plan_input`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::provider_as_source;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Column, Expr, JoinType, col};
use datafusion::sql::TableReference;
use datafusion_common::ResolvedTableReference;
use datafusion_dml::{CatalogDmlHandler, MergeParams};

use super::physical_plans::CayenneMergeExec;

/// Catalog DML handler for local (single-node) Cayenne operations.
///
/// This handler intentionally overlays only `MERGE`. Other DML operations
/// use the default `CatalogDmlHandler` implementations, which delegate to
/// standard `DataFusion` DML machinery.
#[derive(Debug)]
pub struct CayenneDmlHandler {
    default_catalog: &'static str,
    default_schema: &'static str,
}

impl CayenneDmlHandler {
    /// Creates a new local Cayenne DML handler using the provided default
    /// catalog and schema for unresolved table references.
    #[must_use]
    pub fn new(default_catalog: &'static str, default_schema: &'static str) -> Self {
        Self {
            default_catalog,
            default_schema,
        }
    }
}

#[async_trait]
impl CatalogDmlHandler for CayenneDmlHandler {
    fn name(&self) -> &'static str {
        "cayenne"
    }

    async fn merge_exec(
        &self,
        params: MergeParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [join_physical] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "Local MERGE requires exactly one physical input".to_string(),
            ));
        };

        let target_provider = resolve_table(
            session_state,
            &params.target_table,
            self.default_catalog,
            self.default_schema,
        )
        .await
        .ok_or(DataFusionError::Plan(format!(
            "Table {} not found",
            params.target_table
        )))?;

        let target_key_columns: Vec<String> = params
            .on_keys
            .iter()
            .map(|(target, _)| target.clone())
            .collect();

        Ok(Arc::new(CayenneMergeExec::new(
            Arc::clone(join_physical),
            target_provider,
            session_state.clone(),
            target_key_columns,
        )))
    }
}

/// Prepared local MERGE input for the generic [`datafusion_dml::DmlExtensionNode`].
#[derive(Debug)]
pub struct LocalMergePlanInput {
    /// Typed MERGE metadata consumed by the generic DML extension pipeline.
    pub params: MergeParams,
    /// Joined and projected logical input whose rows match the target schema.
    pub projected_input: LogicalPlan,
}

/// Build the typed [`MergeParams`] and joined/projected logical input used by
/// the generic DML extension pipeline for local Cayenne `MERGE`.
///
/// The returned `projected_input` yields rows shaped like the target table,
/// with `SET` expressions already applied. The local [`CayenneDmlHandler`]
/// consumes the corresponding physical plan and executes delete + insert.
///
/// # Errors
///
/// Returns an error when either table cannot be resolved, an assignment targets
/// a missing column, or an assignment expression fails to parse against the
/// joined target/source schema.
#[expect(clippy::too_many_arguments)]
pub async fn build_local_merge_plan_input(
    session_state: &SessionState,
    default_catalog: &str,
    default_schema: &str,
    target_table: &TableReference,
    source_table: &TableReference,
    target_qualifier: &str,
    source_qualifier: &str,
    on_keys: &[(String, String)],
    assignment_sql: &[(String, String)],
) -> DFResult<LocalMergePlanInput> {
    let target_provider =
        resolve_table(session_state, target_table, default_catalog, default_schema)
            .await
            .ok_or(DataFusionError::Plan(format!(
                "Table {target_table} not found"
            )))?;
    let source_provider =
        resolve_table(session_state, source_table, default_catalog, default_schema)
            .await
            .ok_or(DataFusionError::Plan(format!(
                "Table {source_table} not found"
            )))?;

    let target_scan = LogicalPlanBuilder::scan(
        target_qualifier,
        provider_as_source(Arc::clone(&target_provider)),
        None,
    )?
    .build()?;
    let source_scan = LogicalPlanBuilder::scan(
        source_qualifier,
        provider_as_source(Arc::clone(&source_provider)),
        None,
    )?
    .build()?;

    let (left_keys, right_keys): (Vec<Column>, Vec<Column>) = on_keys
        .iter()
        .map(|(target, source)| {
            (
                Column::new(Some(target_qualifier.to_string()), target),
                Column::new(Some(source_qualifier.to_string()), source),
            )
        })
        .unzip();

    let joined = LogicalPlanBuilder::from(target_scan)
        .join(source_scan, JoinType::Inner, (left_keys, right_keys), None)?
        .build()?;

    let target_schema = target_provider.schema();
    let target_field_names: HashSet<&str> = target_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();

    for (column_name, _) in assignment_sql {
        if !target_field_names.contains(column_name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "MERGE SET column '{column_name}' does not exist in target table"
            )));
        }
    }

    let joined_schema = joined.schema();
    let assignments: Vec<(String, Expr)> = assignment_sql
        .iter()
        .map(|(column, value_sql)| {
            let expr = session_state.create_logical_expr(value_sql, joined_schema)?;
            Ok((column.clone(), expr))
        })
        .collect::<DFResult<Vec<_>>>()?;

    let assignment_map: HashMap<&str, &Expr> = assignments
        .iter()
        .map(|(column, expr)| (column.as_str(), expr))
        .collect();

    let project_exprs: Vec<Expr> = target_schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name();
            let expr = if let Some(assignment) = assignment_map.get(col_name.as_str()) {
                (*assignment).clone()
            } else {
                col(Column::new(Some(target_qualifier.to_string()), col_name))
            };
            Ok(expr.alias(col_name))
        })
        .collect::<DFResult<Vec<_>>>()?;

    let projected_input = LogicalPlanBuilder::from(joined)
        .project(project_exprs)?
        .build()?;

    Ok(LocalMergePlanInput {
        params: MergeParams {
            target_table: target_table.clone(),
            source_table: source_table.clone(),
            target_qualifier: target_qualifier.to_string(),
            source_qualifier: source_qualifier.to_string(),
            on_keys: on_keys.to_vec(),
            assignments,
            original_sql: None,
        },
        projected_input,
    })
}

async fn resolve_table(
    session_state: &SessionState,
    table_ref: &TableReference,
    default_catalog: &str,
    default_schema: &str,
) -> Option<Arc<dyn datafusion::datasource::TableProvider>> {
    let ResolvedTableReference {
        catalog,
        schema,
        table,
    } = table_ref.clone().resolve(default_catalog, default_schema);
    let catalog = session_state.catalog_list().catalog(&catalog)?;
    let schema = catalog.schema(&schema)?;
    schema.table(&table).await.ok()?
}
