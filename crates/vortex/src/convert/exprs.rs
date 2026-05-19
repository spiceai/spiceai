// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Schema;
use datafusion_common::Result as DFResult;
use datafusion_common::exec_datafusion_err;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_expr::Operator as DFOperator;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_plan::expressions as df_expr;
use itertools::Itertools;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::arrow::FromArrowType;
use vortex::expr::Expression;
use vortex::expr::and_collect;
use vortex::expr::cast;
use vortex::expr::get_item;
use vortex::expr::is_null;
use vortex::expr::list_contains;
use vortex::expr::lit;
use vortex::expr::not;
use vortex::expr::pack;
use vortex::expr::root;
use vortex::expr::zip_expr;
use vortex::scalar::Scalar;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::like::Like;
use vortex::scalar_fn::fns::like::LikeOptions;
use vortex::scalar_fn::fns::operators::Operator;

use crate::convert::FromDataFusion;

/// Result of splitting a projection into Vortex expressions and leftover `DataFusion` projections.
pub struct ProcessedProjection {
    /// Projection expression evaluated by the Vortex scan.
    pub scan_projection: Expression,
    /// Projection expressions evaluated by `DataFusion` after the scan.
    pub leftover_projection: ProjectionExprs,
}

/// Tries to convert the expressions into a vortex conjunction. Will return `None` iff the input conjunction is empty.
pub(crate) fn make_vortex_predicate(
    expr_convertor: &dyn ExpressionConvertor,
    predicate: &[Arc<dyn PhysicalExpr>],
) -> DFResult<Option<Expression>> {
    let exprs: Vec<_> = predicate
        .iter()
        .map(|e| expr_convertor.convert(e.as_ref()))
        .collect::<DFResult<_>>()?;

    Ok(and_collect(exprs))
}

/// Trait for converting `DataFusion` expressions to Vortex ones.
pub trait ExpressionConvertor: Send + Sync {
    /// Can an expression be pushed down given a specific schema
    fn can_be_pushed_down(&self, expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool;

    /// Try and convert a `DataFusion` [`PhysicalExpr`] into a Vortex [`Expression`].
    ///
    /// # Errors
    ///
    /// Returns an error when the expression cannot be represented as a Vortex expression.
    fn convert(&self, expr: &dyn PhysicalExpr) -> DFResult<Expression>;

    /// Split a projection into Vortex expressions that can be pushed down and leftover
    /// `DataFusion` projections that need to be evaluated after the scan.
    ///
    /// # Errors
    ///
    /// Returns an error when the projected expressions cannot be converted or mapped to the schemas.
    fn split_projection(
        &self,
        source_projection: ProjectionExprs,
        input_schema: &Schema,
        output_schema: &Schema,
    ) -> DFResult<ProcessedProjection>;

    /// Create a projection that reads only the required columns without pushing down
    /// any expressions. All projection logic is applied after the scan.
    ///
    /// # Errors
    ///
    /// Returns an error when the projection cannot be mapped to the input schema.
    fn no_pushdown_projection(
        &self,
        source_projection: ProjectionExprs,
        input_schema: &Schema,
    ) -> DFResult<ProcessedProjection> {
        // Get all unique column indices referenced by the projection
        let column_indices = source_projection.column_indices();

        // Create scan projection that reads the required columns
        let scan_columns: Vec<(String, Expression)> = column_indices
            .into_iter()
            .map(|idx| {
                let field = input_schema.field(idx);
                let name = field.name().clone();
                (name.clone(), get_item(name, root()))
            })
            .collect();

        Ok(ProcessedProjection {
            scan_projection: pack(scan_columns, Nullability::NonNullable),
            leftover_projection: source_projection,
        })
    }
}

/// The default [`ExpressionConvertor`].
#[derive(Default)]
pub struct DefaultExpressionConvertor {}

impl DefaultExpressionConvertor {
    /// Attempts to convert a `DataFusion` `ScalarFunctionExpr` to a Vortex expression.
    fn try_convert_scalar_function(&self, scalar_fn: &ScalarFunctionExpr) -> DFResult<Expression> {
        if let Some(get_field_fn) = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(scalar_fn)
        {
            // DataFusion's GetFieldFunc flattens nested field access into a single call
            // with multiple field name arguments. For example, `outer.inner.leaf` becomes
            // get_field(Column("outer"), "inner", "leaf"). We build a chain of get_item
            // calls for each field name in the path.
            let (source_expr, field_names) = get_field_fn
                .args()
                .split_first()
                .ok_or_else(|| exec_datafusion_err!("get_field missing source expression"))?;

            let mut result = self.convert(source_expr.as_ref())?;
            for expr in field_names {
                let field_name = expr
                    .as_any()
                    .downcast_ref::<df_expr::Literal>()
                    .ok_or_else(|| exec_datafusion_err!("get_field field name must be a literal"))?
                    .value()
                    .try_as_str()
                    .flatten()
                    .ok_or_else(|| {
                        exec_datafusion_err!("get_field field name must be a UTF-8 string")
                    })?;
                result = get_item(field_name.to_string(), result);
            }
            return Ok(result);
        }

        Err(exec_datafusion_err!(
            "Unsupported ScalarFunctionExpr: {}",
            scalar_fn.name()
        ))
    }

    fn try_convert_case_expr(&self, case_expr: &df_expr::CaseExpr) -> DFResult<Expression> {
        let mut else_expr = if let Some(else_expr) = case_expr.else_expr() {
            self.convert(else_expr.as_ref())?
        } else {
            return Err(exec_datafusion_err!(
                "CASE expression without ELSE is not supported for pushdown"
            ));
        };

        if let Some(base_expr) = case_expr.expr() {
            let base_expr = self.convert(base_expr.as_ref())?;
            for (when_expr, then_expr) in case_expr.when_then_expr().iter().rev() {
                let when_expr = self.convert(when_expr.as_ref())?;
                let then_expr = self.convert(then_expr.as_ref())?;
                else_expr = zip_expr(
                    then_expr,
                    else_expr,
                    Binary.new_expr(Operator::Eq, [base_expr.clone(), when_expr]),
                );
            }
        } else {
            for (when_expr, then_expr) in case_expr.when_then_expr().iter().rev() {
                let when_expr = self.convert(when_expr.as_ref())?;
                let then_expr = self.convert(then_expr.as_ref())?;
                else_expr = zip_expr(then_expr, else_expr, when_expr);
            }
        }

        Ok(else_expr)
    }
}

impl ExpressionConvertor for DefaultExpressionConvertor {
    fn can_be_pushed_down(&self, expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
        can_be_pushed_down_impl(expr, schema)
    }

    fn convert(&self, df: &dyn PhysicalExpr) -> DFResult<Expression> {
        // TODO(joe): Don't return an error when we have an unsupported node, bubble up "TRUE" as in keep
        //  for that node, up to any `and` or `or` node.
        if let Some(binary_expr) = df.as_any().downcast_ref::<df_expr::BinaryExpr>() {
            let left = self.convert(binary_expr.left().as_ref())?;
            let right = self.convert(binary_expr.right().as_ref())?;
            let operator = try_operator_from_df(*binary_expr.op())?;

            return Ok(Binary.new_expr(operator, [left, right]));
        }

        if let Some(col_expr) = df.as_any().downcast_ref::<df_expr::Column>() {
            return Ok(get_item(col_expr.name().to_owned(), root()));
        }

        if let Some(like) = df.as_any().downcast_ref::<df_expr::LikeExpr>() {
            let child = self.convert(like.expr().as_ref())?;
            let pattern = self.convert(like.pattern().as_ref())?;
            return Ok(Like.new_expr(
                LikeOptions {
                    negated: like.negated(),
                    case_insensitive: like.case_insensitive(),
                },
                [child, pattern],
            ));
        }

        if let Some(literal) = df.as_any().downcast_ref::<df_expr::Literal>() {
            let value = Scalar::from_df(literal.value()).map_err(|e| {
                exec_datafusion_err!("Failed to convert literal to a Vortex scalar: {e}")
            })?;
            return Ok(lit(value));
        }

        if let Some(cast_expr) = df.as_any().downcast_ref::<df_expr::CastExpr>() {
            let cast_dtype = DType::from_arrow((cast_expr.cast_type(), Nullability::Nullable));
            let child = self.convert(cast_expr.expr().as_ref())?;
            return Ok(cast(child, cast_dtype));
        }

        if let Some(cast_col_expr) = df.as_any().downcast_ref::<df_expr::CastColumnExpr>() {
            let target = cast_col_expr.target_field();

            let target_dtype = DType::from_arrow((target.data_type(), target.is_nullable().into()));
            let child = self.convert(cast_col_expr.expr().as_ref())?;
            return Ok(cast(child, target_dtype));
        }

        if let Some(is_null_expr) = df.as_any().downcast_ref::<df_expr::IsNullExpr>() {
            let arg = self.convert(is_null_expr.arg().as_ref())?;
            return Ok(is_null(arg));
        }

        if let Some(is_not_null_expr) = df.as_any().downcast_ref::<df_expr::IsNotNullExpr>() {
            let arg = self.convert(is_not_null_expr.arg().as_ref())?;
            return Ok(not(is_null(arg)));
        }

        if let Some(in_list) = df.as_any().downcast_ref::<df_expr::InListExpr>() {
            let value = self.convert(in_list.expr().as_ref())?;
            let list_elements: Vec<_> = in_list
                .list()
                .iter()
                .map(|e| {
                    if let Some(lit) = e.as_any().downcast_ref::<df_expr::Literal>() {
                        Scalar::from_df(lit.value()).map_err(|e| {
                            exec_datafusion_err!(
                                "Failed to convert IN list literal to a Vortex scalar: {e}"
                            )
                        })
                    } else {
                        Err(exec_datafusion_err!("Failed to cast sub-expression"))
                    }
                })
                .try_collect()?;

            let list = Scalar::list(
                list_elements[0].dtype().clone(),
                list_elements,
                Nullability::Nullable,
            );
            let expr = list_contains(lit(list), value);

            return Ok(if in_list.negated() { not(expr) } else { expr });
        }

        if let Some(dynamic_filter) = df
            .as_any()
            .downcast_ref::<df_expr::DynamicFilterPhysicalExpr>()
        {
            let current = dynamic_filter.current()?;
            return self.convert(current.as_ref());
        }

        if let Some(scalar_fn) = df.as_any().downcast_ref::<ScalarFunctionExpr>() {
            return self.try_convert_scalar_function(scalar_fn);
        }

        if let Some(case_expr) = df.as_any().downcast_ref::<df_expr::CaseExpr>() {
            return self.try_convert_case_expr(case_expr);
        }

        Err(exec_datafusion_err!(
            "Couldn't convert DataFusion physical {df} expression to a vortex expression"
        ))
    }

    fn split_projection(
        &self,
        source_projection: ProjectionExprs,
        input_schema: &Schema,
        output_schema: &Schema,
    ) -> DFResult<ProcessedProjection> {
        let mut scan_projection = vec![];
        let mut leftover_projection: Vec<ProjectionExpr> = vec![];

        for projection_expr in source_projection.iter() {
            let r = projection_expr.expr.apply(|node| {
                // Vortex evaluates decimal-to-floating casts with different
                // precision/null semantics than DataFusion for some inputs.
                // Keep those expressions above the scan unless the conversion
                // can be made exact.
                if contains_decimal_to_floating_cast(node, input_schema) {
                    scan_projection.extend(
                        collect_columns(node)
                            .into_iter()
                            .map(|c| (c.name().to_string(), get_item(c.name(), root()))),
                    );

                    leftover_projection.push(projection_expr.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }

                // We only pull column children of scalar functions that we can't push into the scan.
                if let Some(scalar_fn_expr) = node.as_any().downcast_ref::<ScalarFunctionExpr>()
                    && !can_scalar_fn_be_pushed_down(scalar_fn_expr, input_schema)
                {
                    scan_projection.extend(
                        collect_columns(node)
                            .into_iter()
                            .map(|c| (c.name().to_string(), get_item(c.name(), root()))),
                    );

                    leftover_projection.push(projection_expr.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }

                // DataFusion assumes different decimal types can be coerced.
                // Vortex expects a perfect match so we don't push it down.
                if let Some(binary_expr) = node.as_any().downcast_ref::<df_expr::BinaryExpr>()
                    && binary_expr.op().is_numerical_operators()
                    && (is_decimal(&binary_expr.left().data_type(input_schema)?)
                        && is_decimal(&binary_expr.right().data_type(input_schema)?))
                {
                    scan_projection.extend(
                        collect_columns(node)
                            .into_iter()
                            .map(|c| (c.name().to_string(), get_item(c.name(), root()))),
                    );

                    leftover_projection.push(projection_expr.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }

                Ok(TreeNodeRecursion::Continue)
            })?;

            // if we didn't stop early
            if matches!(r, TreeNodeRecursion::Continue) {
                scan_projection.push((
                    projection_expr.alias.clone(),
                    self.convert(projection_expr.expr.as_ref())?,
                ));
                leftover_projection.push(ProjectionExpr {
                    expr: Arc::new(df_expr::Column::new_with_schema(
                        projection_expr.alias.as_str(),
                        output_schema,
                    )?),
                    alias: projection_expr.alias.clone(),
                });
            }
        }

        Ok(ProcessedProjection {
            scan_projection: pack(scan_projection, Nullability::NonNullable),
            leftover_projection: leftover_projection.into(),
        })
    }
}

fn try_operator_from_df(value: DFOperator) -> DFResult<Operator> {
    match value {
        DFOperator::Eq => Ok(Operator::Eq),
        DFOperator::NotEq => Ok(Operator::NotEq),
        DFOperator::Lt => Ok(Operator::Lt),
        DFOperator::LtEq => Ok(Operator::Lte),
        DFOperator::Gt => Ok(Operator::Gt),
        DFOperator::GtEq => Ok(Operator::Gte),
        DFOperator::And => Ok(Operator::And),
        DFOperator::Or => Ok(Operator::Or),
        DFOperator::Plus => Ok(Operator::Add),
        DFOperator::Minus => Ok(Operator::Sub),
        DFOperator::Multiply => Ok(Operator::Mul),
        DFOperator::Divide => Ok(Operator::Div),
        DFOperator::IsDistinctFrom
        | DFOperator::IsNotDistinctFrom
        | DFOperator::RegexMatch
        | DFOperator::RegexIMatch
        | DFOperator::RegexNotMatch
        | DFOperator::RegexNotIMatch
        | DFOperator::LikeMatch
        | DFOperator::ILikeMatch
        | DFOperator::NotLikeMatch
        | DFOperator::NotILikeMatch
        | DFOperator::BitwiseAnd
        | DFOperator::BitwiseOr
        | DFOperator::BitwiseXor
        | DFOperator::BitwiseShiftRight
        | DFOperator::BitwiseShiftLeft
        | DFOperator::StringConcat
        | DFOperator::AtArrow
        | DFOperator::ArrowAt
        | DFOperator::Modulo
        | DFOperator::Arrow
        | DFOperator::LongArrow
        | DFOperator::HashArrow
        | DFOperator::HashLongArrow
        | DFOperator::AtAt
        | DFOperator::IntegerDivide
        | DFOperator::HashMinus
        | DFOperator::AtQuestion
        | DFOperator::Question
        | DFOperator::QuestionAnd
        | DFOperator::QuestionPipe => {
            tracing::debug!(operator = %value, "Can't pushdown binary_operator operator");
            Err(exec_datafusion_err!(
                "Unsupported datafusion operator {value}"
            ))
        }
    }
}

fn can_be_pushed_down_impl(df_expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    if contains_decimal_to_floating_cast(df_expr, schema) {
        tracing::debug!(%df_expr, "DataFusion expression contains decimal-to-floating cast and can't be pushed down");
        return false;
    }

    let expr = df_expr.as_any();
    if let Some(binary) = expr.downcast_ref::<df_expr::BinaryExpr>() {
        can_binary_be_pushed_down(binary, schema)
    } else if let Some(col) = expr.downcast_ref::<df_expr::Column>() {
        schema
            .field_with_name(col.name())
            .ok()
            .is_some_and(|field| supported_data_types(field.data_type()))
    } else if let Some(like) = expr.downcast_ref::<df_expr::LikeExpr>() {
        can_be_pushed_down_impl(like.expr(), schema)
            && can_be_pushed_down_impl(like.pattern(), schema)
    } else if let Some(lit) = expr.downcast_ref::<df_expr::Literal>() {
        supported_data_types(&lit.value().data_type())
    } else if let Some(cast_expr) = expr.downcast_ref::<df_expr::CastExpr>() {
        // CastExpr child must be an expression type that convert() can handle
        is_convertible_expr(cast_expr.expr())
    } else if let Some(cast_col_expr) = expr.downcast_ref::<df_expr::CastColumnExpr>() {
        // CastColumnExpr child must be an expression type that convert() can handle
        is_convertible_expr(cast_col_expr.expr())
    } else if let Some(is_null) = expr.downcast_ref::<df_expr::IsNullExpr>() {
        can_be_pushed_down_impl(is_null.arg(), schema)
    } else if let Some(is_not_null) = expr.downcast_ref::<df_expr::IsNotNullExpr>() {
        can_be_pushed_down_impl(is_not_null.arg(), schema)
    } else if let Some(in_list) = expr.downcast_ref::<df_expr::InListExpr>() {
        can_be_pushed_down_impl(in_list.expr(), schema)
            && in_list
                .list()
                .iter()
                .all(|e| can_be_pushed_down_impl(e, schema))
    } else if let Some(scalar_fn) = expr.downcast_ref::<ScalarFunctionExpr>() {
        can_scalar_fn_be_pushed_down(scalar_fn, schema)
    } else if let Some(case_expr) = expr.downcast_ref::<df_expr::CaseExpr>() {
        can_case_be_pushed_down(case_expr, schema)
    } else if let Some(dynamic_filter) = expr.downcast_ref::<df_expr::DynamicFilterPhysicalExpr>() {
        match dynamic_filter.current() {
            Ok(current) => can_be_pushed_down_impl(&current, schema),
            Err(err) => {
                tracing::debug!(%err, "DataFusion dynamic filter current expression can't be read");
                false
            }
        }
    } else {
        tracing::debug!(%df_expr, "DataFusion expression can't be pushed down");
        false
    }
}

/// Checks if an expression type is one that `convert()` can handle.
/// This is less restrictive than `can_be_pushed_down` since it only checks
/// expression types, not data type support.
fn is_convertible_expr(df_expr: &Arc<dyn PhysicalExpr>) -> bool {
    let expr = df_expr.as_any();

    // Expression types that convert() handles
    expr.downcast_ref::<df_expr::BinaryExpr>().is_some()
        || expr.downcast_ref::<df_expr::Column>().is_some()
        || expr.downcast_ref::<df_expr::LikeExpr>().is_some()
        || expr.downcast_ref::<df_expr::Literal>().is_some()
        || expr
            .downcast_ref::<df_expr::CastExpr>()
            .is_some_and(|e| is_convertible_expr(e.expr()))
        || expr
            .downcast_ref::<df_expr::CastColumnExpr>()
            .is_some_and(|e| is_convertible_expr(e.expr()))
        || expr.downcast_ref::<df_expr::IsNullExpr>().is_some()
        || expr.downcast_ref::<df_expr::IsNotNullExpr>().is_some()
        || expr.downcast_ref::<df_expr::InListExpr>().is_some()
        || expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|sf| ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(sf).is_some())
}

fn can_binary_be_pushed_down(binary: &df_expr::BinaryExpr, schema: &Schema) -> bool {
    let is_op_supported = try_operator_from_df(*binary.op()).is_ok();
    is_op_supported
        && can_be_pushed_down_impl(binary.left(), schema)
        && can_be_pushed_down_impl(binary.right(), schema)
}

fn contains_decimal_to_floating_cast(df_expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> bool {
    let expr = df_expr.as_any();

    if let Some(cast) = expr.downcast_ref::<df_expr::CastExpr>() {
        let casts_to_floating = matches!(cast.cast_type(), DataType::Float32 | DataType::Float64);
        if casts_to_floating {
            let casts_from_decimal = cast
                .expr()
                .data_type(schema)
                .map_or(true, |data_type| is_decimal(&data_type));

            if casts_from_decimal {
                return true;
            }
        }
    }

    if let Some(dynamic_filter) = expr.downcast_ref::<df_expr::DynamicFilterPhysicalExpr>()
        && let Ok(current) = dynamic_filter.current()
        && contains_decimal_to_floating_cast(&current, schema)
    {
        return true;
    }

    df_expr
        .children()
        .into_iter()
        .any(|child| contains_decimal_to_floating_cast(child, schema))
}

fn can_case_be_pushed_down(case_expr: &df_expr::CaseExpr, schema: &Schema) -> bool {
    case_expr
        .expr()
        .is_none_or(|base_expr| can_be_pushed_down_impl(base_expr, schema))
        && case_expr
            .when_then_expr()
            .iter()
            .all(|(when_expr, then_expr)| {
                can_be_pushed_down_impl(when_expr, schema)
                    && can_be_pushed_down_impl(then_expr, schema)
            })
        && case_expr
            .else_expr()
            .is_some_and(|else_expr| can_be_pushed_down_impl(else_expr, schema))
}

fn supported_data_types(dt: &DataType) -> bool {
    use DataType::{
        Binary, BinaryView, Boolean, Date32, Date64, Dictionary, LargeBinary, LargeUtf8, Time32,
        Time64, Timestamp, Utf8, Utf8View,
    };

    // For dictionary types, check if the value type is supported.
    if let Dictionary(_, value_type) = dt {
        return supported_data_types(value_type.as_ref());
    }

    let is_supported = dt.is_null()
        || dt.is_numeric()
        || matches!(
            dt,
            Boolean
                | Utf8
                | LargeUtf8
                | Utf8View
                | Binary
                | LargeBinary
                | BinaryView
                | Date32
                | Date64
                | Timestamp(_, _)
                | Time32(_)
                | Time64(_)
        );

    if !is_supported {
        tracing::debug!("DataFusion data type {dt:?} is not supported");
    }

    is_supported
}

/// Checks if a scalar function can be pushed down.
/// Currently only `GetFieldFunc` is supported, and its arguments must also be pushable.
fn can_scalar_fn_be_pushed_down(scalar_fn: &ScalarFunctionExpr, schema: &Schema) -> bool {
    ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(scalar_fn).is_some()
        && scalar_fn
            .args()
            .iter()
            .all(|arg| can_be_pushed_down_impl(arg, schema))
}

// TODO(adam): Replace with `DataType::is_decimal` once its released.
fn is_decimal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use arrow_schema::TimeUnit as ArrowTimeUnit;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator as DFOperator;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_plan::expressions as df_expr;
    use insta::assert_snapshot;
    use rstest::rstest;
    use vortex::dtype::Field as VortexField;
    use vortex::dtype::FieldPath;
    use vortex::dtype::FieldPathSet;
    use vortex::expr::pruning::checked_pruning_expr;

    use super::*;
    use crate::common_tests::TestSessionContext;

    #[rstest::fixture]
    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
            Field::new(
                "created_at",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "unsupported_list",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ])
    }

    #[test]
    fn test_make_vortex_predicate_empty() {
        let expr_convertor = DefaultExpressionConvertor::default();
        let result = make_vortex_predicate(&expr_convertor, &[])
            .expect("empty predicate conversion should succeed");
        assert!(result.is_none());
    }

    #[test]
    fn test_make_vortex_predicate_single() {
        let expr_convertor = DefaultExpressionConvertor::default();
        let col_expr = Arc::new(df_expr::Column::new("test", 0)) as Arc<dyn PhysicalExpr>;
        let result = make_vortex_predicate(&expr_convertor, &[col_expr])
            .expect("single predicate conversion should succeed");
        assert!(result.is_some());
    }

    #[test]
    fn test_make_vortex_predicate_multiple() {
        let expr_convertor = DefaultExpressionConvertor::default();
        let col1 = Arc::new(df_expr::Column::new("col1", 0)) as Arc<dyn PhysicalExpr>;
        let col2 = Arc::new(df_expr::Column::new("col2", 1)) as Arc<dyn PhysicalExpr>;
        let result = make_vortex_predicate(&expr_convertor, &[col1, col2])
            .expect("multiple predicate conversion should succeed");
        assert!(result.is_some());
        // Result should be an AND expression combining the two columns
    }

    #[rstest]
    #[case::eq(DFOperator::Eq, Operator::Eq)]
    #[case::not_eq(DFOperator::NotEq, Operator::NotEq)]
    #[case::lt(DFOperator::Lt, Operator::Lt)]
    #[case::lte(DFOperator::LtEq, Operator::Lte)]
    #[case::gt(DFOperator::Gt, Operator::Gt)]
    #[case::gte(DFOperator::GtEq, Operator::Gte)]
    #[case::and(DFOperator::And, Operator::And)]
    #[case::or(DFOperator::Or, Operator::Or)]
    #[case::plus(DFOperator::Plus, Operator::Add)]
    #[case::plus(DFOperator::Minus, Operator::Sub)]
    #[case::plus(DFOperator::Multiply, Operator::Mul)]
    #[case::plus(DFOperator::Divide, Operator::Div)]
    fn test_operator_conversion_supported(
        #[case] df_op: DFOperator,
        #[case] expected_vortex_op: Operator,
    ) {
        let result = try_operator_from_df(df_op).expect("supported operator converts");
        assert_eq!(result, expected_vortex_op);
    }

    #[rstest]
    #[case::modulo(DFOperator::Modulo)]
    #[case::bitwise_and(DFOperator::BitwiseAnd)]
    #[case::regex_match(DFOperator::RegexMatch)]
    #[case::like_match(DFOperator::LikeMatch)]
    fn test_operator_conversion_unsupported(#[case] df_op: DFOperator) {
        let result = try_operator_from_df(df_op);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported datafusion operator")
        );
    }

    #[test]
    fn test_expr_from_df_column() {
        let col_expr = df_expr::Column::new("test_column", 0);
        let result = DefaultExpressionConvertor::default()
            .convert(&col_expr)
            .unwrap();

        assert_snapshot!(result.display_tree().to_string(), @r"
        vortex.get_item(test_column)
        └── input: vortex.root()
        ");
    }

    #[test]
    fn test_expr_from_df_literal() {
        let literal_expr = df_expr::Literal::new(ScalarValue::Int32(Some(42)));
        let result = DefaultExpressionConvertor::default()
            .convert(&literal_expr)
            .unwrap();

        assert_snapshot!(result.display_tree().to_string(), @"vortex.literal(42i32)");
    }

    #[test]
    fn test_expr_from_df_binary() {
        let left = Arc::new(df_expr::Column::new("left", 0)) as Arc<dyn PhysicalExpr>;
        let right =
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let binary_expr = df_expr::BinaryExpr::new(left, DFOperator::Eq, right);

        let result = DefaultExpressionConvertor::default()
            .convert(&binary_expr)
            .unwrap();

        assert_snapshot!(result.display_tree().to_string(), @r"
        vortex.binary(=)
        ├── lhs: vortex.get_item(left)
        │   └── input: vortex.root()
        └── rhs: vortex.literal(42i32)
        ");
    }

    #[test]
    fn test_expr_from_dynamic_filter_in_list_uses_current_expression() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let column = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let values = vec![
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(3)))) as Arc<dyn PhysicalExpr>,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(7)))) as Arc<dyn PhysicalExpr>,
        ];
        let in_list = Arc::new(
            df_expr::InListExpr::try_new(Arc::clone(&column), values, false, &schema)
                .expect("IN-list expression should be valid"),
        ) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = df_expr::DynamicFilterPhysicalExpr::new(
            vec![column],
            Arc::new(df_expr::Literal::new(ScalarValue::Boolean(Some(true)))),
        );
        dynamic_filter
            .update(in_list)
            .expect("dynamic filter update should succeed");

        let result = DefaultExpressionConvertor::default()
            .convert(&dynamic_filter)
            .expect("dynamic IN-list should convert through its current expression");

        let display = result.display_tree().to_string();
        assert!(display.contains("vortex.list.contains"));
        assert!(display.contains("vortex.get_item(id)"));
    }

    #[test]
    fn test_in_list_conversion_produces_pruning_expression() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let column = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let values = vec![
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(3)))) as Arc<dyn PhysicalExpr>,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(7)))) as Arc<dyn PhysicalExpr>,
        ];
        let in_list = df_expr::InListExpr::try_new(column, values, false, &schema)
            .expect("IN-list expression should be valid");

        let result = DefaultExpressionConvertor::default()
            .convert(&in_list)
            .expect("IN-list should convert to a Vortex expression");
        let (pruning_expr, _required_stats) = checked_pruning_expr(
            &result,
            &FieldPathSet::from_iter([
                FieldPath::from_iter([
                    VortexField::Name("id".into()),
                    VortexField::Name("min".into()),
                ]),
                FieldPath::from_iter([
                    VortexField::Name("id".into()),
                    VortexField::Name("max".into()),
                ]),
            ]),
        )
        .expect("converted IN-list should support min/max pruning");

        let pruning_display = pruning_expr.to_string();
        assert!(pruning_display.contains("id_min"));
        assert!(pruning_display.contains("id_max"));
    }

    #[rstest]
    #[case::like_normal(false, false)]
    #[case::like_negated(true, false)]
    #[case::like_case_insensitive(false, true)]
    #[case::like_negated_case_insensitive(true, true)]
    fn test_expr_from_df_like(#[case] negated: bool, #[case] case_insensitive: bool) {
        let expr = Arc::new(df_expr::Column::new("text_col", 0)) as Arc<dyn PhysicalExpr>;
        let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
            "test%".to_string(),
        )))) as Arc<dyn PhysicalExpr>;
        let like_expr = df_expr::LikeExpr::new(negated, case_insensitive, expr, pattern);

        let result = DefaultExpressionConvertor::default()
            .convert(&like_expr)
            .unwrap();
        let like_opts = result.as_::<Like>();
        assert_eq!(
            like_opts,
            &LikeOptions {
                negated,
                case_insensitive
            }
        );
    }

    #[test]
    fn test_expr_from_df_case_when_with_else() {
        let when_then_expr = vec![(
            Arc::new(df_expr::Column::new("active", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
                "yes".to_string(),
            )))) as Arc<dyn PhysicalExpr>,
        )];
        let case_expr = df_expr::CaseExpr::try_new(
            None,
            when_then_expr,
            Some(Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
                "no".to_string(),
            )))) as Arc<dyn PhysicalExpr>),
        )
        .unwrap();

        let result = DefaultExpressionConvertor::default()
            .convert(&case_expr)
            .unwrap();

        assert_snapshot!(result.display_tree().to_string(), @r#"
        vortex.zip()
        ├── if_true: vortex.literal("yes")
        ├── if_false: vortex.literal("no")
        └── mask: vortex.get_item(active)
            └── input: vortex.root()
        "#);
    }

    #[test]
    fn test_expr_from_df_case_when_without_else_not_pushable() {
        let when_then_expr = vec![(
            Arc::new(df_expr::Column::new("active", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
                "yes".to_string(),
            )))) as Arc<dyn PhysicalExpr>,
        )];
        let case_expr = Arc::new(df_expr::CaseExpr::try_new(None, when_then_expr, None).unwrap())
            as Arc<dyn PhysicalExpr>;

        let schema = Schema::new(vec![Field::new("active", DataType::Boolean, false)]);
        assert!(!can_be_pushed_down_impl(&case_expr, &schema));
    }

    #[rstest]
    // Supported types
    #[case::null(DataType::Null, true)]
    #[case::boolean(DataType::Boolean, true)]
    #[case::int8(DataType::Int8, true)]
    #[case::int16(DataType::Int16, true)]
    #[case::int32(DataType::Int32, true)]
    #[case::int64(DataType::Int64, true)]
    #[case::uint8(DataType::UInt8, true)]
    #[case::uint16(DataType::UInt16, true)]
    #[case::uint32(DataType::UInt32, true)]
    #[case::uint64(DataType::UInt64, true)]
    #[case::float32(DataType::Float32, true)]
    #[case::float64(DataType::Float64, true)]
    #[case::utf8(DataType::Utf8, true)]
    #[case::utf8_view(DataType::Utf8View, true)]
    #[case::binary(DataType::Binary, true)]
    #[case::binary_view(DataType::BinaryView, true)]
    #[case::date32(DataType::Date32, true)]
    #[case::date64(DataType::Date64, true)]
    #[case::timestamp_ms(DataType::Timestamp(ArrowTimeUnit::Millisecond, None), true)]
    #[case::timestamp_us(
        DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(Arc::from("UTC"))),
        true
    )]
    #[case::time32_s(DataType::Time32(ArrowTimeUnit::Second), true)]
    #[case::time64_ns(DataType::Time64(ArrowTimeUnit::Nanosecond), true)]
    // Unsupported types
    #[case::list(
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        false
    )]
    #[case::struct_type(DataType::Struct(vec![Field::new("field", DataType::Int32, true)].into()
    ), false)]
    // Dictionary types - should be supported if value type is supported
    #[case::dict_utf8(
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        true
    )]
    #[case::dict_int32(
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Int32)),
        true
    )]
    #[case::dict_unsupported(
        DataType::Dictionary(
            Box::new(DataType::UInt32),
            Box::new(DataType::List(Arc::new(Field::new("item", DataType::Int32, true))))
        ),
        false
    )]
    fn test_supported_data_types(#[case] data_type: DataType, #[case] expected: bool) {
        assert_eq!(supported_data_types(&data_type), expected);
    }

    #[rstest]
    fn test_can_be_pushed_down_column_supported(test_schema: Schema) {
        let col_expr = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;

        assert!(can_be_pushed_down_impl(&col_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_column_unsupported_type(test_schema: Schema) {
        let col_expr =
            Arc::new(df_expr::Column::new("unsupported_list", 5)) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&col_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_column_not_found(test_schema: Schema) {
        let col_expr = Arc::new(df_expr::Column::new("nonexistent", 99)) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&col_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_literal_supported(test_schema: Schema) {
        let lit_expr =
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

        assert!(can_be_pushed_down_impl(&lit_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_literal_unsupported(test_schema: Schema) {
        // Use a simpler unsupported type - Duration is not supported
        let unsupported_literal = ScalarValue::DurationSecond(Some(42));
        let lit_expr =
            Arc::new(df_expr::Literal::new(unsupported_literal)) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&lit_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_binary_supported(test_schema: Schema) {
        let left = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right =
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let binary_expr = Arc::new(df_expr::BinaryExpr::new(left, DFOperator::Eq, right))
            as Arc<dyn PhysicalExpr>;

        assert!(can_be_pushed_down_impl(&binary_expr, &test_schema));
    }

    #[test]
    fn test_decimal_to_floating_cast_not_pushed_down() {
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(15, 2),
            true,
        )]);
        let amount = Arc::new(df_expr::Column::new("amount", 0)) as Arc<dyn PhysicalExpr>;
        let cast = Arc::new(df_expr::CastExpr::new(amount, DataType::Float64, None))
            as Arc<dyn PhysicalExpr>;
        let literal = Arc::new(df_expr::Literal::new(ScalarValue::Float64(Some(1.0))))
            as Arc<dyn PhysicalExpr>;
        let predicate = Arc::new(df_expr::BinaryExpr::new(cast, DFOperator::Lt, literal))
            as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&predicate, &schema));
    }

    #[test]
    fn test_decimal_to_decimal_predicate_can_be_pushed_down() {
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(15, 2),
            true,
        )]);
        let amount = Arc::new(df_expr::Column::new("amount", 0)) as Arc<dyn PhysicalExpr>;
        let literal = Arc::new(df_expr::Literal::new(ScalarValue::Decimal128(
            Some(100),
            15,
            2,
        ))) as Arc<dyn PhysicalExpr>;
        let predicate = Arc::new(df_expr::BinaryExpr::new(amount, DFOperator::Lt, literal))
            as Arc<dyn PhysicalExpr>;

        assert!(can_be_pushed_down_impl(&predicate, &schema));
    }

    #[test]
    fn test_dynamic_filter_decimal_to_floating_not_pushed_down() {
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(15, 2),
            true,
        )]);
        let amount = Arc::new(df_expr::Column::new("amount", 0)) as Arc<dyn PhysicalExpr>;
        let cast = Arc::new(df_expr::CastExpr::new(
            Arc::clone(&amount),
            DataType::Float64,
            None,
        ));
        let literal = Arc::new(df_expr::Literal::new(ScalarValue::Float64(Some(1.0))));
        let current = Arc::new(df_expr::BinaryExpr::new(cast, DFOperator::Lt, literal));
        let dynamic_filter = Arc::new(df_expr::DynamicFilterPhysicalExpr::new(
            vec![amount],
            current,
        )) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&dynamic_filter, &schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_binary_unsupported_operator(test_schema: Schema) {
        let left = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right =
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let binary_expr = Arc::new(df_expr::BinaryExpr::new(
            left,
            DFOperator::AtQuestion,
            right,
        )) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&binary_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_binary_unsupported_operand(test_schema: Schema) {
        let left = Arc::new(df_expr::Column::new("unsupported_list", 5)) as Arc<dyn PhysicalExpr>;
        let right =
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
        let binary_expr = Arc::new(df_expr::BinaryExpr::new(left, DFOperator::Eq, right))
            as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&binary_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_like_supported(test_schema: Schema) {
        let expr = Arc::new(df_expr::Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
        let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
            "test%".to_string(),
        )))) as Arc<dyn PhysicalExpr>;
        let like_expr =
            Arc::new(df_expr::LikeExpr::new(false, false, expr, pattern)) as Arc<dyn PhysicalExpr>;

        assert!(can_be_pushed_down_impl(&like_expr, &test_schema));
    }

    #[rstest]
    fn test_can_be_pushed_down_like_unsupported_operand(test_schema: Schema) {
        let expr = Arc::new(df_expr::Column::new("unsupported_list", 5)) as Arc<dyn PhysicalExpr>;
        let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
            "test%".to_string(),
        )))) as Arc<dyn PhysicalExpr>;
        let like_expr =
            Arc::new(df_expr::LikeExpr::new(false, false, expr, pattern)) as Arc<dyn PhysicalExpr>;

        assert!(!can_be_pushed_down_impl(&like_expr, &test_schema));
    }

    // https://github.com/vortex-data/vortex/issues/6211
    #[tokio::test]
    async fn test_cast_int_to_string() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(r#"copy (select 1 as id) to 'example.vortex'"#)
            .await?
            .show()
            .await?;

        ctx.session
            .sql(r#"select cast(id as string) as sid from 'example.vortex' where id > 0"#)
            .await?
            .show()
            .await?;

        ctx.session
            .sql(r#"select id from 'example.vortex' where cast (id as string) == '1'"#)
            .await?
            .show()
            .await?;

        // This fails as it pushes string cast to the scan
        ctx.session
            .sql(r#"select cast(id as string) from 'example.vortex'"#)
            .await?
            .collect()
            .await?;

        Ok(())
    }
}
