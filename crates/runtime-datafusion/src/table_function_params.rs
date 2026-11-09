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

//! Substitutes query parameters into table-function arguments before planning.
//!
//! `DataFusion` invokes table functions (`text_search`, `vector_search`, `rrf`,
//! ...) during planning, but binds parameters only afterwards, with
//! [`LogicalPlan::with_param_values`](datafusion::logical_expr::LogicalPlan::with_param_values).
//! Without this pass, `text_search(t, $1)` would receive an unbound placeholder.
//! Placeholders elsewhere in the statement are left for `with_param_values`.

use std::ops::ControlFlow;

use datafusion::{
    common::ParamValues,
    error::{DataFusionError, Result},
    logical_expr::Expr,
    sql::{
        parser::{CopyToSource, Statement},
        sqlparser::ast::{self, TableFactor, Value, VisitMut, VisitorMut, visit_expressions_mut},
        unparser::Unparser,
    },
};

/// Whether any table-function argument in a statement is a placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum TableFunctionPlaceholders {
    Absent,
    /// The plan embeds the substituted values, so it must not be cached.
    Present,
}

/// Replaces placeholders in the table-function arguments of `statement` with
/// literals from `params`. With `params = None`, only detects them.
///
/// # Errors
///
/// If `params` has no value for such a placeholder, or a value cannot be
/// unparsed to SQL.
pub fn bind_table_function_args(
    statement: &mut Statement,
    params: Option<&ParamValues>,
) -> Result<TableFunctionPlaceholders> {
    let mut binder = TableFunctionArgBinder {
        params,
        found: false,
    };
    let flow = match statement {
        Statement::Statement(statement) => statement.visit(&mut binder),
        Statement::Explain(explain) => {
            return bind_table_function_args(&mut explain.statement, params);
        }
        Statement::CopyTo(copy) => match &mut copy.source {
            CopyToSource::Query(query) => query.visit(&mut binder),
            CopyToSource::Relation(_) => ControlFlow::Continue(()),
        },
        Statement::CreateExternalTable(_) | Statement::Reset(_) => ControlFlow::Continue(()),
    };
    if let ControlFlow::Break(error) = flow {
        return Err(error);
    }
    Ok(if binder.found {
        TableFunctionPlaceholders::Present
    } else {
        TableFunctionPlaceholders::Absent
    })
}

struct TableFunctionArgBinder<'a> {
    params: Option<&'a ParamValues>,
    found: bool,
}

impl TableFunctionArgBinder<'_> {
    fn bind<V: VisitMut>(&mut self, args: &mut V) -> ControlFlow<DataFusionError> {
        visit_expressions_mut(args, |expr| {
            let ast::Expr::Value(value) = expr else {
                return ControlFlow::Continue(());
            };
            let Value::Placeholder(id) = &value.value else {
                return ControlFlow::Continue(());
            };
            self.found = true;
            let Some(params) = self.params else {
                return ControlFlow::Continue(());
            };
            match placeholder_literal(params, id) {
                Ok(literal) => {
                    *expr = literal;
                    ControlFlow::Continue(())
                }
                Err(error) => ControlFlow::Break(error),
            }
        })
    }
}

impl VisitorMut for TableFunctionArgBinder<'_> {
    type Break = DataFusionError;

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        match table_factor {
            // `FROM text_search(t, $1)`
            TableFactor::Table {
                args: Some(args), ..
            } => self.bind(args),
            // `FROM LATERAL f($1)`
            TableFactor::Function { args, .. } => self.bind(args),
            // `FROM TABLE(f($1))`
            TableFactor::TableFunction { expr, .. } => self.bind(expr),
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Looks up `id` as `LogicalPlan::with_param_values` does and unparses the
/// value to a SQL literal.
fn placeholder_literal(params: &ParamValues, id: &str) -> Result<ast::Expr> {
    let value = params.get_placeholders_with_values(id)?.value;
    Unparser::default().expr_to_sql(&Expr::Literal(value, None))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::{
        dataframe::DataFrame, prelude::SessionContext, scalar::ScalarValue, sql::parser::DFParser,
    };

    use super::*;

    fn parse(sql: &str) -> Statement {
        DFParser::parse_sql(sql)
            .expect("valid SQL")
            .pop_front()
            .expect("one statement")
    }

    fn bind(sql: &str, params: Option<&ParamValues>) -> (String, TableFunctionPlaceholders) {
        let mut statement = parse(sql);
        let found = bind_table_function_args(&mut statement, params).expect("bind");
        (statement.to_string(), found)
    }

    fn named(values: &[(&str, ScalarValue)]) -> ParamValues {
        ParamValues::from(
            values
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn binds_positional_and_named_placeholders_in_table_function_args() {
        let params = ParamValues::from(vec![
            ScalarValue::Utf8(Some("brown fox".to_string())),
            ScalarValue::Int64(Some(10)),
        ]);
        let (sql, found) = bind(
            "SELECT id FROM text_search(corpus, $1, limit => $2)",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert_eq!(
            sql,
            "SELECT id FROM text_search(corpus, 'brown fox', limit => 10)"
        );

        let params = named(&[("query", ScalarValue::Utf8(Some("it's".to_string())))]);
        let (sql, _) = bind(
            "SELECT id FROM vector_search(corpus, :query)",
            Some(&params),
        );
        assert_eq!(sql, "SELECT id FROM vector_search(corpus, 'it''s')");
    }

    #[test]
    fn binds_placeholders_in_nested_table_function_calls() {
        let params = named(&[("query", ScalarValue::Utf8(Some("fox".to_string())))]);
        let (sql, found) = bind(
            "SELECT * FROM rrf(text_search(corpus, :query), vector_search(corpus, :query))",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert_eq!(
            sql,
            "SELECT * FROM rrf(text_search(corpus, 'fox'), vector_search(corpus, 'fox'))"
        );
    }

    #[test]
    fn leaves_placeholders_outside_table_function_args() {
        let params = ParamValues::from(vec![
            ScalarValue::Utf8(Some("fox".to_string())),
            ScalarValue::Utf8(Some("b".to_string())),
            ScalarValue::Int64(Some(5)),
        ]);
        let (sql, found) = bind(
            "SELECT id FROM text_search(corpus, $1) WHERE tenant = $2 LIMIT $3",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert_eq!(
            sql,
            "SELECT id FROM text_search(corpus, 'fox') WHERE tenant = $2 LIMIT $3"
        );

        let (sql, found) = bind("SELECT id FROM corpus WHERE tenant = $1", Some(&params));
        assert_eq!(found, TableFunctionPlaceholders::Absent);
        assert_eq!(sql, "SELECT id FROM corpus WHERE tenant = $1");
    }

    #[test]
    fn binds_table_functions_in_subqueries_ctes_and_explain() {
        let params = ParamValues::from(vec![ScalarValue::Utf8(Some("fox".to_string()))]);
        let (sql, found) = bind(
            "WITH hits AS (SELECT id FROM text_search(corpus, $1)) \
             SELECT * FROM hits WHERE id IN (SELECT id FROM vector_search(corpus, $1))",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert!(!sql.contains('$'), "{sql}");

        let (sql, found) = bind(
            "EXPLAIN SELECT * FROM text_search(corpus, $1)",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert!(!sql.contains('$'), "{sql}");

        let (sql, found) = bind(
            "COPY (SELECT * FROM text_search(corpus, $1)) TO 'out.parquet'",
            Some(&params),
        );
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert!(!sql.contains('$'), "{sql}");
    }

    #[test]
    fn detects_placeholders_without_params() {
        let (sql, found) = bind("SELECT * FROM text_search(corpus, $1)", None);
        assert_eq!(found, TableFunctionPlaceholders::Present);
        assert_eq!(sql, "SELECT * FROM text_search(corpus, $1)");

        let (_, found) = bind("SELECT * FROM text_search(corpus, 'fox')", None);
        assert_eq!(found, TableFunctionPlaceholders::Absent);
    }

    #[test]
    fn missing_value_is_an_error() {
        let mut statement = parse("SELECT * FROM text_search(corpus, :query)");
        let error = bind_table_function_args(&mut statement, Some(&named(&[])))
            .expect_err("no value for :query");
        assert!(error.to_string().contains("query"), "{error}");
    }

    /// Each literal must plan back to the exact `ScalarValue` that
    /// `with_param_values` would bind.
    #[tokio::test]
    async fn literal_plans_to_the_bound_value() {
        let ctx = SessionContext::new();
        let values = [
            ScalarValue::Utf8(Some("brown 'fox'".to_string())),
            ScalarValue::Int64(Some(-7)),
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Float64(Some(0.25)),
            ScalarValue::Boolean(Some(true)),
        ];
        for value in values {
            let params = ParamValues::from(vec![value.clone()]);
            let literal = placeholder_literal(&params, "$1").expect("literal");
            let df: DataFrame = ctx
                .sql(&format!("SELECT {literal}"))
                .await
                .expect("plan literal");
            let batches = df.collect().await.expect("collect");
            let column = batches[0].column(0);
            let planned = ScalarValue::try_from_array(column, 0).expect("scalar");
            assert_eq!(planned, value, "literal {literal}");
        }
    }
}
