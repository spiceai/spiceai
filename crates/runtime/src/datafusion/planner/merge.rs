/*
Copyright 2026 The Spice.ai OSS Authors

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

//! MERGE INTO planning for the unified planner.
//!
//! Phase 1 supports only:
//! ```sql
//! MERGE INTO target [AS t]
//! USING source [AS s]
//! ON <equality_conjunction>
//! WHEN MATCHED THEN UPDATE SET col1 = expr1, ...
//! ```
//!
//! Both source and target must be Cayenne catalog tables. Target must not
//! have `primary_key` or `on_conflict` configured.

use std::sync::Arc;

use cayenne::ddl::LocalMergePlanInput;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr as SQLExpr, MergeAction, MergeClause, MergeClauseKind,
    Statement as SQLStatement, TableFactor,
};
use datafusion_dml::{DmlExtensionNode, DmlNodeOp, MergeParams};

use super::{PlannerContext, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, is_cayenne_table};
use crate::config::ClusterRole;

/// Plan a `MERGE INTO` statement for Cayenne execution.
///
/// Parses the AST, validates phase 1 constraints, normalizes the ON clause
/// into bare `(target_col, source_col)` key pairs, and produces a generic
/// `DmlExtensionNode`.
///
/// - In scheduler mode, the node carries only typed MERGE metadata plus the
///   original SQL text and has no logical inputs; execution forwards the MERGE
///   to executors.
/// - In local mode, the node carries a joined/projected logical input whose
///   rows already match the target schema; local execution consumes the planned
///   physical input and performs delete+insert.
pub(super) async fn plan_distributed_merge(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
    original_sql: &str,
) -> DFResult<LogicalPlan> {
    let Statement::Statement(sql_stmt) = statement else {
        return Err(DataFusionError::Internal(
            "Expected Statement::Statement for MERGE".to_string(),
        ));
    };
    let SQLStatement::Merge(merge) = *sql_stmt else {
        return Err(DataFusionError::Internal(
            "Expected SQLStatement::Merge".to_string(),
        ));
    };
    let table = merge.table;
    let source = merge.source;
    let on = merge.on;
    let clauses = merge.clauses;
    let output = merge.output;

    if output.is_some() {
        return Err(DataFusionError::Plan(
            "MERGE with OUTPUT clause is not supported".to_string(),
        ));
    }

    let (target_name, target_alias) = extract_table_ref(&table, "target")?;
    let target_ref = TableReference::parse_str(&target_name);

    let target_qualifier = target_alias
        .clone()
        .unwrap_or_else(|| target_ref.table().to_string());

    let (source_name, source_alias) = extract_table_ref(&source, "source")?;
    let source_ref = TableReference::parse_str(&source_name);
    let source_qualifier = source_alias
        .clone()
        .unwrap_or_else(|| source_ref.table().to_string());

    if !is_cayenne_table(session, &target_ref) {
        return Err(DataFusionError::Plan(format!(
            "MERGE target '{target_name}' is not a Cayenne catalog table"
        )));
    }
    if !is_cayenne_table(session, &source_ref) {
        return Err(DataFusionError::Plan(format!(
            "MERGE source '{source_name}' is not a Cayenne catalog table"
        )));
    }

    validate_target_metadata(session, &target_ref, &target_name).await?;

    let target_qualifiers: Vec<&str> = std::iter::once(target_ref.table())
        .chain(target_alias.as_deref())
        .collect();
    let source_qualifiers: Vec<&str> = std::iter::once(source_ref.table())
        .chain(source_alias.as_deref())
        .collect();
    let on_keys = parse_on_keys(&on, &target_qualifiers, &source_qualifiers)?;

    if on_keys.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE ON clause must contain at least one equality predicate".to_string(),
        ));
    }

    if clauses.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "MERGE currently supports exactly one WHEN clause, got {}",
            clauses.len()
        )));
    }
    let assignment_sql =
        validate_and_extract_assignments(&clauses[0], target_ref.table(), target_alias.as_deref())?;

    if !matches!(ctx.cluster_role, Some(ClusterRole::Scheduler)) {
        let LocalMergePlanInput {
            params,
            projected_input,
            ..
        } = cayenne::ddl::build_local_merge_plan_input(
            session,
            SPICE_DEFAULT_CATALOG,
            SPICE_DEFAULT_SCHEMA,
            &target_ref,
            &source_ref,
            &target_qualifier,
            &source_qualifier,
            &on_keys,
            &assignment_sql,
        )
        .await?;

        return Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(DmlExtensionNode::new_with_count_output(
                DmlNodeOp::Merge(Box::new(params)),
                ctx.cayenne_dml_handler()?,
                vec![Arc::new(projected_input)],
            )),
        }));
    }

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DmlExtensionNode::new_with_count_output(
            DmlNodeOp::Merge(Box::new(MergeParams {
                target_table: target_ref,
                source_table: source_ref,
                target_qualifier,
                source_qualifier,
                on_keys,
                // Scheduler-mode distributed MERGE forwards the original SQL
                // directly to executors, so no typed assignment expressions
                // are needed here.
                assignments: Vec::new(),
                original_sql: Some(original_sql.to_string()),
            })),
            ctx.cayenne_dml_handler()?,
            Vec::new(),
        )),
    }))
}

/// Extract a table name and optional alias from a `TableFactor`.
///
/// Only `TableFactor::Table` (plain table reference) is supported in phase 1.
fn extract_table_ref(factor: &TableFactor, role: &str) -> DFResult<(String, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(DataFusionError::Plan(format!(
            "MERGE {role} must be a table reference (subqueries are not supported)"
        )));
    };
    let table_name = name.to_string();
    let alias_name = alias.as_ref().map(|a| a.name.value.clone());
    Ok((table_name, alias_name))
}

/// Validate that the target table does not have `primary_key` or `on_conflict`
/// configured, which would conflict with MERGE's delete+insert execution.
///
/// Uses the Cayenne metadata catalog directly (via `CayenneSchemaProvider`)
/// rather than downcasting the `TableProvider`, because partitioned tables
/// are wrapped in `PartitionTableProvider` which doesn't expose the inner
/// `CayenneTableProvider`.
async fn validate_target_metadata(
    session: &SessionState,
    target_ref: &TableReference,
    target_name: &str,
) -> DFResult<()> {
    use cayenne::CayenneSchemaProvider;

    let catalog_name = target_ref.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let schema_name = target_ref.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);
    let table_name = target_ref.table();

    let catalog_list = session.catalog_list();
    let catalog = catalog_list
        .catalog(catalog_name)
        .ok_or_else(|| DataFusionError::Plan(format!("Catalog '{catalog_name}' not found")))?;
    let schema = catalog
        .schema(schema_name)
        .ok_or_else(|| DataFusionError::Plan(format!("Schema '{schema_name}' not found")))?;

    // Downcast to CayenneSchemaProvider to access the metadata catalog.
    // This works for all Cayenne table shapes (plain and partitioned).
    let Some(cayenne_schema) = schema.as_any().downcast_ref::<CayenneSchemaProvider>() else {
        // Not a Cayenne schema — skip validation (caller already verified
        // this is a Cayenne catalog table via `is_cayenne_table`).
        return Ok(());
    };

    let full_name = format!("{schema_name}/{table_name}");
    let metadata = cayenne_schema
        .metadata_catalog()
        .get_table(&full_name)
        .await
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to read metadata for table '{target_name}': {e}"
            ))
        })?;

    if !metadata.primary_key.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "MERGE is not supported on table '{target_name}' because it has a primary key configured. \
             MERGE uses delete+insert execution which conflicts with primary key on-conflict behavior."
        )));
    }
    if metadata.on_conflict.is_some() {
        return Err(DataFusionError::Plan(format!(
            "MERGE is not supported on table '{target_name}' because it has on_conflict configured."
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ON clause parsing
// ---------------------------------------------------------------------------

/// Parse the ON clause into normalized `(target_col, source_col)` pairs.
///
/// Iteratively flattens AND conjunctions, validates each conjunct is an
/// equality between qualified column references, and uses the provided
/// qualifier lists (table name + optional alias) to attribute each side
/// to target or source. Returns bare column names.
fn parse_on_keys(
    on_expr: &SQLExpr,
    target_qualifiers: &[&str],
    source_qualifiers: &[&str],
) -> DFResult<Vec<(String, String)>> {
    let conjuncts = flatten_and_conjuncts(on_expr);
    let mut keys = Vec::with_capacity(conjuncts.len());

    for conjunct in conjuncts {
        let SQLExpr::BinaryOp { left, op, right } = conjunct else {
            return Err(DataFusionError::Plan(format!(
                "MERGE ON clause must contain only equality predicates, found: {conjunct}"
            )));
        };
        if *op != BinaryOperator::Eq {
            return Err(DataFusionError::Plan(format!(
                "MERGE ON clause must contain only equality predicates (AND-connected), \
                 found operator: {op}"
            )));
        }

        let (lhs_qualifier, lhs_col) = extract_column_ref(left)?;
        let (rhs_qualifier, rhs_col) = extract_column_ref(right)?;

        let lhs_is_target = matches_qualifier(lhs_qualifier.as_ref(), target_qualifiers);
        let rhs_is_target = matches_qualifier(rhs_qualifier.as_ref(), target_qualifiers);
        let lhs_is_source = matches_qualifier(lhs_qualifier.as_ref(), source_qualifiers);
        let rhs_is_source = matches_qualifier(rhs_qualifier.as_ref(), source_qualifiers);

        if lhs_is_target && rhs_is_source {
            keys.push((lhs_col, rhs_col));
        } else if lhs_is_source && rhs_is_target {
            keys.push((rhs_col, lhs_col));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Cannot determine target/source for MERGE ON predicate: {conjunct}. \
                 Use table aliases to disambiguate (e.g., t.id = s.id)."
            )));
        }
    }

    Ok(keys)
}

/// Iteratively flatten an AND-connected expression tree into leaf conjuncts.
fn flatten_and_conjuncts(expr: &SQLExpr) -> Vec<&SQLExpr> {
    let mut result = Vec::new();
    let mut stack = vec![expr];

    while let Some(e) = stack.pop() {
        match e {
            SQLExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                // Push right first so left is processed first (preserves order).
                stack.push(right);
                stack.push(left);
            }
            SQLExpr::Nested(inner) => {
                stack.push(inner);
            }
            other => result.push(other),
        }
    }

    result
}

/// Extract `(optional_qualifier, column_name)` from a column reference.
fn extract_column_ref(expr: &SQLExpr) -> DFResult<(Option<String>, String)> {
    match expr {
        SQLExpr::Identifier(ident) => Ok((None, ident.value.clone())),
        SQLExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Ok((Some(parts[0].value.clone()), parts[1].value.clone()))
        }
        _ => Err(DataFusionError::Plan(format!(
            "MERGE ON clause columns must be simple column references, found: {expr}"
        ))),
    }
}

/// Check if a column qualifier matches any of the accepted qualifiers
/// (table name or alias) for a given side.
fn matches_qualifier(qualifier: Option<&String>, accepted: &[&str]) -> bool {
    match qualifier {
        Some(q) => accepted.iter().any(|a| q.eq_ignore_ascii_case(a)),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// WHEN clause validation
// ---------------------------------------------------------------------------

/// Validate the WHEN clause is `WHEN MATCHED THEN UPDATE SET ...` and extract
/// the assignment pairs as `(column_name, value_sql)`.
///
/// `target_table_name` and `target_alias` are used to validate that qualified
/// assignment targets (e.g., `t.qty`) reference the target, not the source.
fn validate_and_extract_assignments(
    clause: &MergeClause,
    target_table_name: &str,
    target_alias: Option<&str>,
) -> DFResult<Vec<(String, String)>> {
    if clause.clause_kind != MergeClauseKind::Matched {
        return Err(DataFusionError::Plan(format!(
            "Only WHEN MATCHED is supported in MERGE, found: WHEN {}",
            clause.clause_kind
        )));
    }
    if clause.predicate.is_some() {
        return Err(DataFusionError::Plan(
            "WHEN MATCHED with additional predicates (AND ...) is not supported".to_string(),
        ));
    }

    let MergeAction::Update(update) = &clause.action else {
        return Err(DataFusionError::Plan(format!(
            "Only UPDATE SET is supported in WHEN MATCHED, found: {}",
            clause.action
        )));
    };
    if update.update_predicate.is_some() || update.delete_predicate.is_some() {
        return Err(DataFusionError::Plan(
            "MERGE UPDATE predicates are not supported".to_string(),
        ));
    }
    let assignments = &update.assignments;

    if assignments.is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE UPDATE SET must have at least one assignment".to_string(),
        ));
    }

    let mut result = Vec::with_capacity(assignments.len());
    let mut seen_columns = std::collections::HashSet::new();
    for assignment in assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                // Extract bare column name from potentially qualified name.
                // e.g., `t.qty` -> `qty`, `qty` -> `qty`
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .map(|p| match p.as_ident() {
                        Some(ident) => Ok(ident.value.clone()),
                        None => Err(DataFusionError::Plan(format!(
                            "Invalid assignment target '{name}': function-based identifiers are not supported"
                        ))),
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                match parts.as_slice() {
                    [column] => column.clone(),
                    [qualifier, column] => {
                        // Qualified targets must reference the MERGE target
                        // relation. Reject source-qualified targets like `s.qty`.
                        let qualifier_matches_target = qualifier
                            .eq_ignore_ascii_case(target_table_name)
                            || target_alias.is_some_and(|a| qualifier.eq_ignore_ascii_case(a));
                        if !qualifier_matches_target {
                            return Err(DataFusionError::Plan(format!(
                                "Invalid assignment target '{name}': qualifier '{qualifier}' does not \
                                 match MERGE target '{target_table_name}'{}",
                                target_alias.map_or_else(String::new, |alias| format!(
                                    " or alias '{alias}'"
                                ))
                            )));
                        }
                        column.clone()
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Invalid assignment target '{name}': expected [qualifier.]column"
                        )));
                    }
                }
            }
            AssignmentTarget::Tuple(_) => {
                return Err(DataFusionError::Plan(
                    "Tuple assignments are not supported in MERGE UPDATE SET".to_string(),
                ));
            }
        };
        if !seen_columns.insert(col_name.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate assignment target column '{col_name}' in MERGE UPDATE SET"
            )));
        }
        let value_sql = assignment.value.to_string();
        result.push((col_name, value_sql));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{MergeClause, Statement as SQLStatement};
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse_merge(sql: &str) -> SQLStatement {
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("should parse");
        stmts.into_iter().next().expect("should have a statement")
    }

    fn extract_on_and_clauses(stmt: SQLStatement) -> (Box<SQLExpr>, Vec<MergeClause>) {
        let SQLStatement::Merge(merge) = stmt else {
            panic!("Expected Merge statement");
        };
        (merge.on, merge.clauses)
    }

    // --- ON clause tests ---

    #[test]
    fn test_parse_on_keys_single() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn test_parse_on_keys_composite() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.a = s.a AND t.b = s.b \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string())
            ]
        );
    }

    #[test]
    fn test_parse_on_keys_reversed_order() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON s.id = t.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn test_parse_on_keys_no_alias_uses_table_name() {
        let stmt = parse_merge(
            "MERGE INTO target USING source ON target.id = source.id \
             WHEN MATCHED THEN UPDATE SET name = source.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        // No aliases — only table names as qualifiers.
        let keys = parse_on_keys(&on, &["target"], &["source"]).expect("should parse");
        assert_eq!(keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn test_parse_on_keys_rejects_non_equality() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id > s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let err = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect_err("should fail");
        assert!(err.to_string().contains("equality"));
    }

    #[test]
    fn test_parse_on_keys_rejects_unqualified() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON id = id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let err = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect_err("should fail");
        assert!(err.to_string().contains("disambiguate"));
    }

    #[test]
    fn test_parse_on_keys_parenthesized() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s \
             ON (t.a = s.a) AND (t.b = s.b) \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(
            keys,
            vec![
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string())
            ]
        );
    }

    #[test]
    fn test_parse_on_keys_different_column_names() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.src_id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(keys, vec![("id".to_string(), "src_id".to_string())]);
    }

    #[test]
    fn test_parse_on_keys_rejects_literal() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = 42 \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let err = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect_err("should fail");
        assert!(err.to_string().contains("column references"));
    }

    #[test]
    fn test_parse_on_keys_rejects_or() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.a = s.a OR t.b = s.b \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let err = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect_err("should fail");
        assert!(err.to_string().contains("equality"));
    }

    #[test]
    fn test_parse_on_keys_rejects_both_sides_same_table() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.a = t.b \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let err = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect_err("should fail");
        assert!(err.to_string().contains("disambiguate"));
    }

    #[test]
    fn test_parse_on_keys_case_insensitive_qualifier() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON T.id = S.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let keys = parse_on_keys(&on, &["target", "t"], &["source", "s"]).expect("should parse");
        assert_eq!(keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn test_flatten_and_conjuncts_deeply_nested() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s \
             ON t.a = s.a AND t.b = s.b AND t.c = s.c \
             WHEN MATCHED THEN UPDATE SET val = s.val",
        );
        let (on, _) = extract_on_and_clauses(stmt);
        let conjuncts = flatten_and_conjuncts(&on);
        assert_eq!(conjuncts.len(), 3);
    }

    // --- WHEN clause tests ---

    #[test]
    fn test_validate_matched_update_set() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value + 1",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let assignments = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect("should succeed");
        assert_eq!(assignments[1].0, "value");
        assert_eq!(assignments[1].1, "s.value + 1");
    }

    #[test]
    fn test_reject_not_matched() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let err = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect_err("should fail");
        assert!(err.to_string().contains("WHEN MATCHED"));
    }

    #[test]
    fn test_reject_matched_delete() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let err = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect_err("should fail");
        assert!(err.to_string().contains("UPDATE SET"));
    }

    // --- Table ref extraction tests ---

    #[test]
    fn test_extract_table_ref_plain() {
        let stmt = parse_merge(
            "MERGE INTO my_table USING source ON 1=1 WHEN MATCHED THEN UPDATE SET a = 1",
        );
        let SQLStatement::Merge(merge) = stmt else {
            panic!();
        };
        let table = merge.table;
        let (name, alias) = extract_table_ref(&table, "target").expect("should succeed");
        assert_eq!(name, "my_table");
        assert_eq!(alias, None);
    }

    #[test]
    fn test_extract_table_ref_with_alias() {
        let stmt = parse_merge(
            "MERGE INTO my_table AS t USING other AS s ON 1=1 \
             WHEN MATCHED THEN UPDATE SET a = 1",
        );
        let SQLStatement::Merge(merge) = stmt else {
            panic!();
        };
        let table = merge.table;
        let source = merge.source;
        let (name, alias) = extract_table_ref(&table, "target").expect("should succeed");
        assert_eq!(name, "my_table");
        assert_eq!(alias, Some("t".to_string()));

        let (name, alias) = extract_table_ref(&source, "source").expect("should succeed");
        assert_eq!(name, "other");
        assert_eq!(alias, Some("s".to_string()));
    }

    // --- Assignment validation tests ---

    #[test]
    fn test_assignment_strips_qualifier() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.name = s.name, t.value = s.value",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let assignments = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect("should succeed");
        assert_eq!(assignments.len(), 2);
        // Qualified `t.name` should be stripped to bare `name`
        assert_eq!(assignments[0].0, "name");
        assert_eq!(assignments[1].0, "value");
    }

    #[test]
    fn test_assignment_bare_column() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let assignments = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect("should succeed");
        assert_eq!(assignments[0].0, "name");
    }

    #[test]
    fn test_assignment_rejects_duplicate_target() {
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, name = s.other",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let err = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect_err("should fail");
        assert!(
            err.to_string().contains("Duplicate assignment target"),
            "got: {err}"
        );
    }

    #[test]
    fn test_assignment_rejects_triple_qualified() {
        let stmt = parse_merge(
            "MERGE INTO cat.schema.target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET cat.schema.target.name = s.name",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let err = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect_err("should fail");
        assert!(
            err.to_string().contains("expected [qualifier.]column"),
            "got: {err}"
        );
    }

    #[test]
    fn test_assignment_rejects_source_qualified_target() {
        // `s.qty` as an assignment target should be rejected since `s` is the source alias
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET s.qty = s.qty",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let err = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect_err("should fail");
        assert!(
            err.to_string().contains("does not match MERGE target"),
            "got: {err}"
        );
    }

    #[test]
    fn test_assignment_accepts_table_name_qualifier() {
        // Using the table name (not alias) as qualifier should work
        let stmt = parse_merge(
            "MERGE INTO target AS t USING source AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET target.qty = s.qty",
        );
        let (_, clauses) = extract_on_and_clauses(stmt);
        let assignments = validate_and_extract_assignments(&clauses[0], "target", Some("t"))
            .expect("should succeed");
        assert_eq!(assignments[0].0, "qty");
    }
}
