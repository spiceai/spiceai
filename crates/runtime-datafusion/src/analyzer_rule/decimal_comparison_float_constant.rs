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

//! Folds a constant float operand of a comparison with a decimal into the
//! decimal literal `DataFusion` compares it as, before federation.
//!
//! `DataFusion` plans `0.06` as `Float64`, and type coercion compares a decimal
//! with a float as decimals: `l_discount <= 0.06 + 0.01` becomes
//! `CAST(l_discount AS DECIMAL(30, 15)) <= CAST(0.06 + 0.01 AS DECIMAL(30, 15))`,
//! whose right side is `0.070000000000000` — the cast rounds the float sum
//! `0.06999999999999999` to the scale. A float literal on its own becomes the
//! decimal its SQL token spells ([`ExactFloatLiteralDecimalCast`]).
//!
//! Federation runs before type coercion, so the SQL a source receives carries
//! the uncoerced comparison and the source applies its own rules. `MySQL` reads
//! a float literal written as `6E-2` as a `DOUBLE` and compares a `DECIMAL` with
//! a `DOUBLE` as doubles, so `l_discount <= 6E-2 + 1E-2` drops the rows at
//! `0.07`. This rule runs before federation and replaces such a constant with
//! the decimal literal coercion would compare against, so every source
//! compares exact decimals and gets the rows `DataFusion` gets.
//!
//! The rewrite only does early what coercion and constant folding do anyway:
//! comparing the decimal with a decimal literal of the type coercion chose
//! coerces to that same type. A constant whose cast fails (it overflows the
//! type) is left as it was, so the query fails or answers as before.
//!
//! [`ExactFloatLiteralDecimalCast`]: super::ExactFloatLiteralDecimalCast

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{Between, BinaryExpr, Cast, InList};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;

use super::exact_float_decimal_cast::exact_literal_decimal;

/// Replaces constant float operands of decimal comparisons with decimal
/// literals. See the module docs.
#[derive(Debug, Default)]
pub struct DecimalComparisonFloatConstant;

impl DecimalComparisonFloatConstant {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for DecimalComparisonFloatConstant {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan)
            .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "decimal_comparison_float_constant"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // The columns an expression of this node can read, as type coercion
    // resolves them.
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = &plan {
        schema.merge(&DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?);
    }
    let name_preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);
        expr.transform_up(|expr| Ok(rewrite_expr(expr, &schema)))
            .map(|transformed| transformed.update_data(|expr| original_name.restore(expr)))
    })
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            let (left, right) = match (
                decimal_operand(&right, &left, schema),
                decimal_operand(&left, &right, schema),
            ) {
                (Some(left), _) => (Transformed::yes(Box::new(left)), Transformed::no(right)),
                (None, Some(right)) => (Transformed::no(left), Transformed::yes(Box::new(right))),
                (None, None) => {
                    return Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right }));
                }
            };
            Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                left: left.data,
                op,
                right: right.data,
            }))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let new_low = decimal_operand(&expr, &low, schema);
            let new_high = decimal_operand(&expr, &high, schema);
            let transformed = new_low.is_some() || new_high.is_some();
            let between = Expr::Between(Between {
                low: new_low.map_or(low, Box::new),
                high: new_high.map_or(high, Box::new),
                expr,
                negated,
            });
            if transformed {
                Transformed::yes(between)
            } else {
                Transformed::no(between)
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let mut transformed = false;
            let list = list
                .into_iter()
                .map(|item| match decimal_operand(&expr, &item, schema) {
                    Some(decimal) => {
                        transformed = true;
                        decimal
                    }
                    None => item,
                })
                .collect();
            let in_list = Expr::InList(InList {
                expr,
                list,
                negated,
            });
            if transformed {
                Transformed::yes(in_list)
            } else {
                Transformed::no(in_list)
            }
        }
        expr => Transformed::no(expr),
    }
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// The decimal literal `constant` is compared as when the other operand,
/// `against`, is a decimal and `constant` is a constant float expression;
/// `None` otherwise, or when its cast to that decimal fails.
fn decimal_operand(against: &Expr, constant: &Expr, schema: &DFSchema) -> Option<Expr> {
    let decimal_type = against.get_type(schema).ok()?;
    if !decimal_type.is_decimal() || !is_constant(constant) {
        return None;
    }
    let float_type = constant.get_type(schema).ok()?;
    if !float_type.is_floating() {
        return None;
    }
    let target = comparison_coercion(&decimal_type, &float_type).filter(DataType::is_decimal)?;
    if let Expr::Literal(value, metadata) = constant
        && let Some(decimal) = exact_literal_decimal(value, metadata.as_ref(), &target)
    {
        return Some(Expr::Literal(decimal, None));
    }
    // What the optimizer folds the coerced cast to: the float evaluated, then
    // cast by Arrow.
    let simplifier = ExprSimplifier::new(SimplifyContext::default());
    let cast = Expr::Cast(Cast::new(Box::new(constant.clone()), target.clone()));
    let folded = simplifier
        .coerce(cast, DFSchema::empty_ref())
        .and_then(|cast| simplifier.simplify(cast))
        .ok()?;
    match folded {
        Expr::Literal(value, _) if value.data_type() == target && !value.is_null() => {
            Some(Expr::Literal(value, None))
        }
        _ => None,
    }
}

/// Whether `expr` reads nothing but literals through deterministic scalar
/// operations, so it folds to one value.
fn is_constant(expr: &Expr) -> bool {
    !expr.is_volatile()
        && !expr
            .exists(|node| {
                Ok(!matches!(
                    node,
                    Expr::Literal(..)
                        | Expr::BinaryExpr(_)
                        | Expr::Negative(_)
                        | Expr::Cast(_)
                        | Expr::TryCast(_)
                        | Expr::ScalarFunction(_)
                ))
            })
            .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit, table_scan};
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect};

    use super::*;

    fn scan() -> LogicalPlanBuilder {
        let schema = Schema::new(vec![
            Field::new("disc", DataType::Decimal128(15, 2), true),
            Field::new("d", DataType::Decimal256(65, 30), true),
            Field::new("f", DataType::Float64, true),
        ]);
        table_scan(Some("m"), &schema, None).expect("table scan should build")
    }

    fn analyzed_sql(predicate: Expr) -> (String, String) {
        let plan = scan()
            .filter(predicate)
            .expect("filter should build")
            .build()
            .expect("plan should build");
        let plan = DecimalComparisonFloatConstant::new()
            .analyze(plan, &ConfigOptions::default())
            .expect("rule should analyze the plan");
        let sql = |dialect: &dyn datafusion::sql::unparser::dialect::Dialect| {
            Unparser::new(dialect)
                .plan_to_sql(&plan)
                .expect("plan should unparse")
                .to_string()
        };
        (sql(&MySqlDialect {}), sql(&PostgreSqlDialect {}))
    }

    #[test]
    fn float_arithmetic_against_a_decimal_folds_to_the_rounded_decimal() {
        let (mysql, postgres) =
            analyzed_sql(col("disc").between(lit(0.06) - lit(0.01), lit(0.06) + lit(0.01)));
        insta::assert_snapshot!("float_arithmetic_against_a_decimal_mysql", mysql);
        insta::assert_snapshot!("float_arithmetic_against_a_decimal_postgres", postgres);
    }

    #[test]
    fn a_lossy_float_literal_keeps_its_spelling() {
        let spelling = "99999999999999999999999999999999999.999999999999999999999999999999";
        let literal = Expr::Literal(
            ScalarValue::Float64(Some(1e35)),
            Some(datafusion::common::metadata::FieldMetadata::new(
                [(
                    super::super::exact_float_decimal_cast::FLOAT_LITERAL_SPELLING_KEY.to_string(),
                    spelling.to_string(),
                )]
                .into(),
            )),
        );
        let (mysql, _) = analyzed_sql(col("d").lt(literal));
        insta::assert_snapshot!("a_lossy_float_literal_keeps_its_spelling", mysql);
    }

    #[test]
    fn literals_in_an_in_list_and_on_either_side_fold() {
        let (mysql, _) = analyzed_sql(
            col("disc")
                .in_list(vec![lit(0.05), lit(1.5) * lit(2.0)], false)
                .and(lit(0.07).gt_eq(col("disc"))),
        );
        insta::assert_snapshot!("literals_in_an_in_list_and_on_either_side", mysql);
    }

    #[test]
    fn float_comparisons_and_non_constants_are_left_alone() {
        let (mysql, _) = analyzed_sql(
            col("f")
                .lt(lit(0.07))
                .and(col("disc").lt(col("f") * lit(0.5)))
                .and(col("disc").lt(lit(1e300))),
        );
        insta::assert_snapshot!("float_comparisons_and_non_constants_are_left_alone", mysql);
    }

    #[test]
    fn a_projected_comparison_keeps_its_name() {
        let plan = scan()
            .project(vec![col("disc").lt_eq(lit(0.06) + lit(0.01))])
            .expect("projection should build")
            .build()
            .expect("plan should build");
        let before = plan.schema().field(0).name().clone();
        let plan = DecimalComparisonFloatConstant::new()
            .analyze(plan, &ConfigOptions::default())
            .expect("rule should analyze the plan");
        assert_eq!(plan.schema().field(0).name(), &before);
    }
}
