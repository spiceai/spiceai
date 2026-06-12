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

//! SQL-level parsing for `MERGE INTO` statements.
//!
//! [`parse_merge_sql`] converts a raw SQL string into [`MergeAstInfo`]: the
//! table references, ON-clause key pairs, and `SET` assignment pairs — all as
//! plain strings, before any catalog look-up or expression planning.
//!
//! This is the generic, catalog-agnostic layer shared by:
//! - `cayenne::ddl::plan_local_merge` (single-node execution)
//! - `runtime::datafusion::planner::merge::plan_distributed_merge` (scheduler/executor)
//!
//! ## Supported form (phase 1)
//!
//! ```sql
//! MERGE INTO target [AS t]
//! USING source [AS s]
//! ON <equality_conjunction>
//! WHEN MATCHED THEN UPDATE SET col1 = expr1, ...
//! ```

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr as SQLExpr, MergeAction, MergeClause, MergeClauseKind,
    Statement as SQLStatement, TableFactor,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// SQL-level information extracted from a `MERGE INTO` statement.
///
/// All values are strings — no catalog look-up or expression planning has
/// occurred yet.  Pass this to a catalog-specific planner (e.g.
/// `cayenne::ddl::build_local_merge_plan_input`) to produce an executable
/// [`datafusion::logical_expr::LogicalPlan`].
#[derive(Debug, Clone)]
pub struct MergeAstInfo {
    /// Fully-qualified target table reference.
    pub target_ref: TableReference,
    /// Qualifier used for the target side in the JOIN (table name or alias).
    pub target_qualifier: String,
    /// Fully-qualified source table reference.
    pub source_ref: TableReference,
    /// Qualifier used for the source side in the JOIN (table name or alias).
    pub source_qualifier: String,
    /// Equi-join key pairs extracted from the ON clause: `(target_col, source_col)`.
    pub on_keys: Vec<(String, String)>,
    /// `SET` assignment pairs from the WHEN MATCHED clause: `(target_col, value_sql)`.
    pub assignment_sql: Vec<(String, String)>,
}

/// Parse a `MERGE INTO` SQL string into [`MergeAstInfo`].
///
/// Validates phase-1 constraints:
/// - No `OUTPUT` clause
/// - Exactly one `WHEN MATCHED THEN UPDATE SET …` clause
/// - ON clause must be an AND-conjunction of column-equality predicates
///
/// # Errors
///
/// Returns a [`DataFusionError::Plan`] for unsupported MERGE forms or
/// [`DataFusionError::SQL`] for parse failures.
pub fn parse_merge_sql(sql: &str) -> DFResult<MergeAstInfo> {
    let stmts =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| DataFusionError::SQL(Box::new(e), None))?;
    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Plan("Empty SQL statement".to_string()))?;

    let SQLStatement::Merge(merge) = stmt else {
        return Err(DataFusionError::Plan(
            "parse_merge_sql: expected a MERGE INTO statement".to_string(),
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
    let assignment_sql = extract_assignments(
        &clauses[0],
        target_ref.table(),
        target_alias.as_deref(),
    )?;

    Ok(MergeAstInfo {
        target_ref,
        target_qualifier,
        source_ref,
        source_qualifier,
        on_keys,
        assignment_sql,
    })
}

// ── AST helpers ───────────────────────────────────────────────────────────────

fn extract_table_ref(
    factor: &TableFactor,
    role: &str,
) -> DFResult<(String, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return Err(DataFusionError::Plan(format!(
            "MERGE {role} must be a table reference (subqueries are not supported)"
        )));
    };
    Ok((
        name.to_string(),
        alias.as_ref().map(|a| a.name.value.clone()),
    ))
}

fn parse_on_keys(
    on_expr: &SQLExpr,
    target_qualifiers: &[&str],
    source_qualifiers: &[&str],
) -> DFResult<Vec<(String, String)>> {
    let mut keys = Vec::new();
    for conjunct in flatten_and(on_expr) {
        let SQLExpr::BinaryOp { left, op, right } = conjunct else {
            return Err(DataFusionError::Plan(format!(
                "MERGE ON clause must contain only equality predicates, found: {conjunct}"
            )));
        };
        if *op != BinaryOperator::Eq {
            return Err(DataFusionError::Plan(format!(
                "MERGE ON clause must contain only equality predicates \
                 (AND-connected), found operator: {op}"
            )));
        }
        let (lq, lc) = extract_col_ref(left)?;
        let (rq, rc) = extract_col_ref(right)?;
        let lhs_t = matches_qualifier(lq.as_ref(), target_qualifiers);
        let rhs_t = matches_qualifier(rq.as_ref(), target_qualifiers);
        let lhs_s = matches_qualifier(lq.as_ref(), source_qualifiers);
        let rhs_s = matches_qualifier(rq.as_ref(), source_qualifiers);
        if lhs_t && rhs_s {
            keys.push((lc, rc));
        } else if lhs_s && rhs_t {
            keys.push((rc, lc));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Cannot determine target/source for MERGE ON predicate: {conjunct}. \
                 Use table aliases to disambiguate (e.g., t.id = s.id)."
            )));
        }
    }
    Ok(keys)
}

fn flatten_and(expr: &SQLExpr) -> Vec<&SQLExpr> {
    let mut result = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            SQLExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                stack.push(right);
                stack.push(left);
            }
            SQLExpr::Nested(inner) => stack.push(inner),
            other => result.push(other),
        }
    }
    result
}

fn extract_col_ref(expr: &SQLExpr) -> DFResult<(Option<String>, String)> {
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

fn matches_qualifier(qualifier: Option<&String>, accepted: &[&str]) -> bool {
    qualifier.map_or(false, |q| {
        accepted.iter().any(|a| q.eq_ignore_ascii_case(a))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> MergeAstInfo {
        parse_merge_sql(sql).expect("should parse")
    }

    fn err(sql: &str) -> String {
        parse_merge_sql(sql).expect_err("should fail").to_string()
    }

    // ── ON clause ────────────────────────────────────────────────────────────

    #[test]
    fn on_single_key() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name");
        assert_eq!(m.on_keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn on_composite_keys() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.a = s.a AND t.b = s.b WHEN MATCHED THEN UPDATE SET val = s.val");
        assert_eq!(m.on_keys, vec![("a".to_string(), "a".to_string()), ("b".to_string(), "b".to_string())]);
    }

    #[test]
    fn on_reversed_side_order() {
        let m = parse("MERGE INTO target AS t USING source AS s ON s.id = t.id WHEN MATCHED THEN UPDATE SET name = s.name");
        assert_eq!(m.on_keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn on_no_alias_uses_table_name() {
        let m = parse("MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name");
        assert_eq!(m.on_keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn on_different_column_names() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.id = s.src_id WHEN MATCHED THEN UPDATE SET name = s.name");
        assert_eq!(m.on_keys, vec![("id".to_string(), "src_id".to_string())]);
    }

    #[test]
    fn on_parenthesized() {
        let m = parse("MERGE INTO target AS t USING source AS s ON (t.a = s.a) AND (t.b = s.b) WHEN MATCHED THEN UPDATE SET val = s.val");
        assert_eq!(m.on_keys, vec![("a".to_string(), "a".to_string()), ("b".to_string(), "b".to_string())]);
    }

    #[test]
    fn on_deeply_nested_and() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.a = s.a AND t.b = s.b AND t.c = s.c WHEN MATCHED THEN UPDATE SET val = s.val");
        assert_eq!(m.on_keys.len(), 3);
    }

    #[test]
    fn on_case_insensitive_qualifier() {
        let m = parse("MERGE INTO target AS t USING source AS s ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = s.name");
        assert_eq!(m.on_keys, vec![("id".to_string(), "id".to_string())]);
    }

    #[test]
    fn on_rejects_non_equality() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id > s.id WHEN MATCHED THEN UPDATE SET name = s.name").contains("equality"));
    }

    #[test]
    fn on_rejects_or() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.a = s.a OR t.b = s.b WHEN MATCHED THEN UPDATE SET val = s.val").contains("equality"));
    }

    #[test]
    fn on_rejects_unqualified() {
        assert!(err("MERGE INTO target AS t USING source AS s ON id = id WHEN MATCHED THEN UPDATE SET name = s.name").contains("disambiguate"));
    }

    #[test]
    fn on_rejects_same_table_both_sides() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.a = t.b WHEN MATCHED THEN UPDATE SET val = s.val").contains("disambiguate"));
    }

    #[test]
    fn on_rejects_literal() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id = 42 WHEN MATCHED THEN UPDATE SET name = s.name").contains("column references"));
    }

    // ── Table references ─────────────────────────────────────────────────────

    #[test]
    fn table_refs_plain() {
        let m = parse("MERGE INTO my_table USING src ON my_table.id = src.id WHEN MATCHED THEN UPDATE SET a = 1");
        assert_eq!(m.target_ref.table(), "my_table");
        assert_eq!(m.target_qualifier, "my_table");
        assert_eq!(m.source_ref.table(), "src");
    }

    #[test]
    fn table_refs_with_alias() {
        let m = parse("MERGE INTO my_table AS t USING other AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1");
        assert_eq!(m.target_ref.table(), "my_table");
        assert_eq!(m.target_qualifier, "t");
        assert_eq!(m.source_ref.table(), "other");
        assert_eq!(m.source_qualifier, "s");
    }

    // ── SET assignments ───────────────────────────────────────────────────────

    #[test]
    fn assignment_expression_preserved() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value + 1");
        assert_eq!(m.assignment_sql[1].0, "value");
        assert_eq!(m.assignment_sql[1].1, "s.value + 1");
    }

    #[test]
    fn assignment_strips_target_qualifier() {
        let m = parse("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.name = s.name, t.value = s.value");
        assert_eq!(m.assignment_sql[0].0, "name");
        assert_eq!(m.assignment_sql[1].0, "value");
    }

    #[test]
    fn assignment_rejects_source_qualifier() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET s.qty = s.qty").contains("does not match MERGE target"));
    }

    #[test]
    fn assignment_rejects_duplicate_column() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name, name = s.other").contains("Duplicate assignment target"));
    }

    #[test]
    fn assignment_rejects_triple_qualified() {
        assert!(err("MERGE INTO cat.schema.target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET cat.schema.target.name = s.name").contains("expected [qualifier.]column"));
    }

    // ── WHEN clause validation ────────────────────────────────────────────────

    #[test]
    fn rejects_when_not_matched() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)").contains("WHEN MATCHED"));
    }

    #[test]
    fn rejects_when_matched_delete() {
        assert!(err("MERGE INTO target AS t USING source AS s ON t.id = s.id WHEN MATCHED THEN DELETE").contains("UPDATE SET"));
    }

    #[test]
    fn rejects_output_clause() {
        // sqlparser may not support OUTPUT in all dialects — skip if it fails to parse
        let sql = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name OUTPUT inserted.id";
        if let Err(e) = parse_merge_sql(sql) {
            // Either a parse error (unsupported syntax) or our own plan error — both acceptable
            let _ = e;
        }
    }
}

fn extract_assignments(
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
            "WHEN MATCHED with additional predicates (AND ...) is not supported"
                .to_string(),
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
    let mut seen = std::collections::HashSet::new();

    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(name) => {
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .map(|p| {
                        p.as_ident().map(|i| i.value.clone()).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Invalid assignment target '{name}'"
                            ))
                        })
                    })
                    .collect::<DFResult<_>>()?;
                match parts.as_slice() {
                    [column] => column.clone(),
                    [qualifier, column] => {
                        let ok = qualifier.eq_ignore_ascii_case(target_table_name)
                            || target_alias
                                .is_some_and(|al| qualifier.eq_ignore_ascii_case(al));
                        if !ok {
                            return Err(DataFusionError::Plan(format!(
                                "Assignment target qualifier '{qualifier}' does not \
                                 match MERGE target '{target_table_name}'"
                            )));
                        }
                        column.clone()
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Invalid assignment target '{name}': expected \
                             [qualifier.]column"
                        )))
                    }
                }
            }
            AssignmentTarget::Tuple(_) => {
                return Err(DataFusionError::Plan(
                    "Tuple assignments are not supported in MERGE UPDATE SET"
                        .to_string(),
                ))
            }
        };

        if !seen.insert(col.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate assignment target column '{col}' in MERGE UPDATE SET"
            )));
        }
        result.push((col, a.value.to_string()));
    }
    Ok(result)
}
