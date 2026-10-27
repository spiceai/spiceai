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

//! Computes async function calls (such as `ai()`) in a projection below the node that
//! uses them.
//!
//! `DataFusion`'s physical planner evaluates an async function only in a projection, a
//! filter, or the argument of a single aggregate. Anywhere else, such as a GROUP BY key,
//! an ORDER BY key or a window function, the call is planned as a plain scalar function
//! and fails at execution with "async functions should not be called directly".
//! [`AsyncCallPlacement`] rewrites
//!
//! ```text
//! Aggregate: groupBy=[[ai(t.c)]], aggr=[[count(*)]]
//!   TableScan: t
//! ```
//!
//! into
//!
//! ```text
//! Projection: __async_call_1 AS ai(t.c), count(*)
//!   Aggregate: groupBy=[[__async_call_1]], aggr=[[count(*)]]
//!     Projection: t.c, ai(t.c) AS __async_call_1
//!       TableScan: t
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, Result};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::logical_plan::{Aggregate, LogicalPlan, Projection, Sort, Window};
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

const ASYNC_CALL_PREFIX: &str = "__async_call";

fn is_async_call(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(function) if function.func.as_async().is_some())
}

fn has_async_call(expr: &Expr) -> bool {
    expr.exists(|e| Ok(is_async_call(e))).unwrap_or(false)
}

/// Whether any of the node's own expressions calls an async function.
fn node_has_async_call(plan: &LogicalPlan) -> bool {
    plan.apply_expressions(|e| {
        Ok(if has_async_call(e) {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .is_ok_and(|recursion| recursion == TreeNodeRecursion::Stop)
}

/// Moves async function calls out of the nodes the physical planner cannot evaluate them
/// in; see the module documentation. Register it after the other optimizer rules.
#[derive(Debug, Default)]
pub struct AsyncCallPlacement;

impl OptimizerRule for AsyncCallPlacement {
    fn name(&self) -> &'static str {
        "async_call_placement"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !matches!(
            plan,
            LogicalPlan::Aggregate(_) | LogicalPlan::Sort(_) | LogicalPlan::Window(_)
        ) || !node_has_async_call(&plan)
        {
            return Ok(Transformed::no(plan));
        }
        let aliases = config.alias_generator();
        match plan {
            LogicalPlan::Aggregate(aggregate) => place_aggregate(aggregate, aliases),
            LogicalPlan::Sort(sort) => place_sort(sort, aliases),
            LogicalPlan::Window(window) => place_window(window, aliases),
            other => return Ok(Transformed::no(other)),
        }
        .map(Transformed::yes)
    }
}

/// Computes every aggregate key and argument call below the aggregate. An aggregate
/// with more than one async argument is moved too: the physical planner numbers each
/// aggregate's async columns from the same index, so they collide.
fn place_aggregate(aggregate: Aggregate, aliases: &AliasGenerator) -> Result<LogicalPlan> {
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    } = aggregate;
    let placed = Placed::below(group_expr.iter().chain(&aggr_expr), input, aliases)?;
    let group_expr = placed.replace_all(group_expr)?;
    let aggr_expr = placed.replace_all(aggr_expr)?;
    let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(placed.plan),
        group_expr,
        aggr_expr,
    )?);
    // Restore the output names by position: a GROUPING SETS key cannot carry an alias.
    let exprs = aggregate
        .schema()
        .columns()
        .into_iter()
        .zip(schema.iter())
        .map(|(column, (qualifier, field))| {
            if column.relation.as_ref() == qualifier && column.name == *field.name() {
                Expr::Column(column)
            } else {
                Expr::Column(column).alias_qualified(qualifier.cloned(), field.name())
            }
        })
        .collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(aggregate),
    )?))
}

fn place_sort(sort: Sort, aliases: &AliasGenerator) -> Result<LogicalPlan> {
    let Sort { expr, input, fetch } = sort;
    let schema = Arc::clone(input.schema());
    let placed = Placed::below(expr.iter().map(|s| &s.expr), input, aliases)?;
    let expr = expr
        .into_iter()
        .map(|s| {
            Ok(SortExpr {
                expr: placed.replace(s.expr)?,
                ..s
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let sorted = LogicalPlan::Sort(Sort {
        expr,
        input: Arc::new(placed.plan),
        fetch,
    });
    keep_columns(sorted, &schema)
}

fn place_window(window: Window, aliases: &AliasGenerator) -> Result<LogicalPlan> {
    let Window {
        input,
        window_expr,
        schema,
    } = window;
    let placed = Placed::below(window_expr.iter(), input, aliases)?;
    let names = NamePreserver::new_for_projection();
    let window_expr = window_expr
        .into_iter()
        .map(|e| {
            let saved = names.save(&e);
            Ok(saved.restore(placed.replace(e)?))
        })
        .collect::<Result<Vec<_>>>()?;
    let windowed = LogicalPlan::Window(Window::try_new(window_expr, Arc::new(placed.plan))?);
    keep_columns(windowed, &schema)
}

/// Projects `plan` to the columns of `schema`, dropping the async call columns.
fn keep_columns(plan: LogicalPlan, schema: &DFSchemaRef) -> Result<LogicalPlan> {
    let exprs = schema.columns().into_iter().map(Expr::Column).collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan),
    )?))
}

/// A projection that computes the async calls of some expressions, and the column each
/// call becomes.
struct Placed {
    plan: LogicalPlan,
    replacements: HashMap<Expr, Expr>,
}

impl Placed {
    /// Adds a projection below a node that computes each distinct outermost async call
    /// in `exprs` once, next to the input's columns.
    fn below<'a>(
        exprs: impl Iterator<Item = &'a Expr>,
        input: Arc<LogicalPlan>,
        aliases: &AliasGenerator,
    ) -> Result<Self> {
        let mut projected: Vec<Expr> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let mut replacements = HashMap::new();
        for expr in exprs {
            expr.apply(|e| {
                if !is_async_call(e) {
                    return Ok(TreeNodeRecursion::Continue);
                }
                if !replacements.contains_key(e) {
                    let alias = aliases.next(ASYNC_CALL_PREFIX);
                    projected.push(e.clone().alias(&alias));
                    replacements.insert(e.clone(), Expr::Column(Column::new_unqualified(alias)));
                }
                Ok(TreeNodeRecursion::Jump)
            })?;
        }
        Ok(Self {
            plan: LogicalPlan::Projection(Projection::try_new(projected, input)?),
            replacements,
        })
    }

    fn replace(&self, expr: Expr) -> Result<Expr> {
        expr.transform_down(|e| match self.replacements.get(&e) {
            Some(column) => Ok(Transformed::new(
                column.clone(),
                true,
                TreeNodeRecursion::Jump,
            )),
            None => Ok(Transformed::no(e)),
        })
        .map(|t| t.data)
    }

    fn replace_all(&self, exprs: Vec<Expr>) -> Result<Vec<Expr>> {
        exprs.into_iter().map(|e| self.replace(e)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::common::cast::as_string_array;
    use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::prelude::SessionContext;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Upper-cases a string, as an async function, and counts the rows it is called on.
    #[derive(Debug)]
    struct AsyncUpper {
        signature: Signature,
        rows: Arc<AtomicUsize>,
    }

    impl PartialEq for AsyncUpper {
        fn eq(&self, other: &Self) -> bool {
            Arc::ptr_eq(&self.rows, &other.rows)
        }
    }

    impl Eq for AsyncUpper {}

    impl Hash for AsyncUpper {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.signature.hash(state);
        }
    }

    impl ScalarUDFImpl for AsyncUpper {
        fn name(&self) -> &'static str {
            "async_upper"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            unreachable!("async_upper is invoked asynchronously")
        }
    }

    #[async_trait]
    impl AsyncScalarUDFImpl for AsyncUpper {
        async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            self.rows.fetch_add(args.number_rows, Ordering::Relaxed);
            let input = args.args[0].to_array(args.number_rows)?;
            let upper: StringArray = as_string_array(&input)?
                .iter()
                .map(|s| s.map(str::to_uppercase))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(upper) as ArrayRef))
        }
    }

    fn ctx() -> (SessionContext, Arc<AtomicUsize>) {
        let rows = Arc::new(AtomicUsize::new(0));
        let ctx = SessionContext::new();
        ctx.register_udf(
            AsyncScalarUDF::new(Arc::new(AsyncUpper {
                signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
                rows: Arc::clone(&rows),
            }))
            .into_scalar_udf(),
        );
        ctx.add_optimizer_rule(Arc::new(AsyncCallPlacement));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("category", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    "toys", "books", "toys", "games", "books", "books",
                ])),
            ],
        )
        .expect("valid batch");
        ctx.register_batch("t", batch).expect("registers table");
        (ctx, rows)
    }

    async fn query(sql: &str) -> String {
        query_counting(sql).await.0
    }

    /// The query's result, and the number of rows `async_upper` was called on.
    async fn query_counting(sql: &str) -> (String, usize) {
        let (ctx, rows) = ctx();
        let batches = ctx
            .sql(sql)
            .await
            .expect("plans")
            .collect()
            .await
            .expect("executes");
        let result = pretty_format_batches(&batches)
            .expect("formats")
            .to_string();
        (result, rows.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn group_by() {
        let expected = "\
+-------+---+
| c     | n |
+-------+---+
| BOOKS | 3 |
| GAMES | 1 |
| TOYS  | 2 |
+-------+---+";
        assert_eq!(
            query("SELECT async_upper(category) AS c, count(*) AS n FROM t GROUP BY c ORDER BY c")
                .await,
            expected
        );
        assert_eq!(
            query(
                "SELECT async_upper(category) AS c, count(*) AS n FROM t \
                 GROUP BY async_upper(category) ORDER BY c"
            )
            .await,
            expected
        );
    }

    #[tokio::test]
    async fn group_by_rollup() {
        assert_eq!(
            query(
                "SELECT async_upper(category) AS c, count(*) AS n FROM t \
                 GROUP BY ROLLUP(async_upper(category)) ORDER BY c NULLS LAST"
            )
            .await,
            "\
+-------+---+
| c     | n |
+-------+---+
| BOOKS | 3 |
| GAMES | 1 |
| TOYS  | 2 |
|       | 6 |
+-------+---+"
        );
    }

    #[tokio::test]
    async fn order_by() {
        assert_eq!(
            query("SELECT id FROM t ORDER BY async_upper(category), id").await,
            "\
+----+
| id |
+----+
| 2  |
| 5  |
| 6  |
| 4  |
| 1  |
| 3  |
+----+"
        );
        assert_eq!(
            query("SELECT id FROM t ORDER BY async_upper(category) DESC, id DESC LIMIT 2").await,
            "\
+----+
| id |
+----+
| 3  |
| 1  |
+----+"
        );
    }

    /// Sorting by a selected call calls the function once per row.
    #[tokio::test]
    async fn order_by_a_selected_call() {
        let (result, rows) = query_counting(
            "SELECT async_upper(category) AS c FROM t ORDER BY async_upper(category) LIMIT 2",
        )
        .await;
        assert_eq!(
            result,
            "\
+-------+
| c     |
+-------+
| BOOKS |
| BOOKS |
+-------+"
        );
        assert_eq!(rows, 6);
    }

    #[tokio::test]
    async fn distinct_aggregate() {
        assert_eq!(
            query("SELECT count(DISTINCT async_upper(category)) AS n FROM t").await,
            "\
+---+
| n |
+---+
| 3 |
+---+"
        );
    }

    /// Two aggregates over different async calls each read their own call.
    #[tokio::test]
    async fn two_aggregates() {
        assert_eq!(
            query(
                "SELECT min(async_upper(category)) AS lo, \
                 max(async_upper(concat(category, '!'))) AS hi FROM t"
            )
            .await,
            "\
+-------+-------+
| lo    | hi    |
+-------+-------+
| BOOKS | TOYS! |
+-------+-------+"
        );
    }

    #[tokio::test]
    async fn window() {
        assert_eq!(
            query(
                "SELECT id, row_number() OVER (ORDER BY async_upper(category), id) AS rn \
                 FROM t ORDER BY id"
            )
            .await,
            "\
+----+----+
| id | rn |
+----+----+
| 1  | 5  |
| 2  | 1  |
| 3  | 6  |
| 4  | 4  |
| 5  | 2  |
| 6  | 3  |
+----+----+"
        );
    }

    /// Every async call is computed in a projection, which is where the physical planner
    /// evaluates it.
    #[tokio::test]
    async fn computes_calls_in_projections() {
        let plan = ctx()
            .0
            .sql(
                "SELECT async_upper(category) AS c, count(*) AS n FROM t \
                 GROUP BY c ORDER BY async_upper(concat(c, '!'))",
            )
            .await
            .expect("plans")
            .into_optimized_plan()
            .expect("optimizes");
        assert_eq!(
            plan.to_string(),
            "\
Projection: c, n
  Sort: __async_call_2 ASC NULLS LAST
    Projection: __async_call_1 AS c, count(Int64(1)) AS n, async_upper(concat(__async_call_1, Utf8(\"!\"))) AS __async_call_2
      Aggregate: groupBy=[[__async_call_1]], aggr=[[count(Int64(1))]]
        Projection: async_upper(t.category) AS __async_call_1
          TableScan: t projection=[category]"
        );
    }
}
