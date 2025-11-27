use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, Ident,
    VisitorMut,
};
use std::ops::ControlFlow;

#[derive(Default)]
pub struct SQLiteBetweenVisitor {}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum OpSide {
    Left,
    Right,
}

impl VisitorMut for SQLiteBetweenVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        Self::rebuild_between(expr);

        ControlFlow::Continue(())
    }
}

/// This AST visitor is used to convert BETWEEN expressions into `decimal_cmp` expressions.
/// This is necessary with `SQLite` because some floating point values are not accurately comparable when used in the <low> or <high> position of the BETWEEN expression.
/// For example, `BETWEEN 0.06+0.01` will cause a floating point precision error that returns invalid results.
///
/// This visitor instead converts the expression into two equivalent `decimal_cmp` expressions, for accurate arbitrary precision comparisons.
impl SQLiteBetweenVisitor {
    fn rebuild_between(expr: &mut Expr) {
        // <expr> [ NOT ] BETWEEN <low> AND <high>
        if let Expr::Between {
            expr: input_expr,
            negated,
            low,
            high,
        } = expr
        {
            // if low or high contains numeric values (including in an expression), we can convert it to
            // decimal_cmp(<expr>, <low>) >= 0 and decimal_cmp(<expr>, <high>) <= 0
            // when negated is true, >= becomes < and <= becomes >

            if Self::between_value_is_numeric(low) && Self::between_value_is_numeric(high) {
                Self::wrap_numeric_values_in_decimal(low);
                Self::wrap_numeric_values_in_decimal(high);

                // right now, <expr> BETWEEN decimal(<low>) AND decimal(<high>)
                // build each new half as a new Expr::BinaryOp

                // lhs - decimal_cmp(<expr>, decimal(<low>)) [>= | <] 0
                let lhs = Self::build_decimal_cmp_side(
                    input_expr,
                    low,
                    Self::build_cmp_operator(OpSide::Left, *negated),
                );

                // rhs - decimal_cmp(<expr>, decimal(<high>)) [<= | >] 0
                let rhs = Self::build_decimal_cmp_side(
                    input_expr,
                    high,
                    Self::build_cmp_operator(OpSide::Right, *negated),
                );

                // replace the original BETWEEN expr with the new AND binary op
                *expr = Expr::BinaryOp {
                    left: Box::new(lhs),
                    op: BinaryOperator::And,
                    right: Box::new(rhs),
                };
            }
        }
    }

    fn between_value_is_numeric(expr: &mut Expr) -> bool {
        match expr {
            Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(_, _),
                ..
            }) => true,
            Expr::BinaryOp { left, op, right } => {
                if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) {
                    if let Expr::Value(ast::ValueWithSpan {
                        value: ast::Value::Number(_, _),
                        ..
                    }) = left.as_ref()
                    {
                        if let Expr::Value(ast::ValueWithSpan {
                            value: ast::Value::Number(_, _),
                            ..
                        }) = right.as_ref()
                        {
                            return true;
                        }
                    }
                }
                false
            }
            Expr::Nested(nested_expr) => Self::between_value_is_numeric(nested_expr),
            _ => false,
        }
    }

    fn wrap_numeric_values_in_decimal(expr: &mut Expr) {
        match expr {
            Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(s, _),
                ..
            }) => {
                // if expr is a numeric literal, wrap it in a decimal scalar
                *expr = Expr::Function(ast::Function {
                    name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                        "decimal",
                    ))]),
                    args: ast::FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            ast::Value::SingleQuotedString(s.clone()).into(),
                        )))],
                        clauses: Vec::new(),
                    }),
                    over: None,
                    uses_odbc_syntax: false,
                    parameters: ast::FunctionArguments::None,
                    filter: None,
                    null_treatment: None,
                    within_group: Vec::new(),
                });
            }
            Expr::BinaryOp { left, op: _, right } => {
                Self::wrap_numeric_values_in_decimal(left);
                Self::wrap_numeric_values_in_decimal(right);
            }
            Expr::Nested(nested_expr) => {
                Self::wrap_numeric_values_in_decimal(nested_expr);
            }
            _ => {}
        }
    }

    fn build_cmp_operator(side: OpSide, negated: bool) -> BinaryOperator {
        match side {
            OpSide::Left => {
                if negated {
                    BinaryOperator::Lt
                } else {
                    BinaryOperator::GtEq
                }
            }
            OpSide::Right => {
                if negated {
                    BinaryOperator::Gt
                } else {
                    BinaryOperator::LtEq
                }
            }
        }
    }

    fn build_decimal_cmp_side(
        input_expr: &mut Expr,
        comparison_expr: &mut Expr,
        comparison_op: BinaryOperator,
    ) -> Expr {
        let right = Expr::Value(ast::Value::Number("0".to_string(), false).into());
        let left = Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                "decimal_cmp",
            ))]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(input_expr.clone())),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(comparison_expr.clone())),
                ],
                clauses: Vec::new(),
            }),
            over: None,
            uses_odbc_syntax: false,
            parameters: ast::FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: Vec::new(),
        });

        Expr::BinaryOp {
            left: Box::new(left),
            op: comparison_op,
            right: Box::new(right),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_rebuild_between_into_decimal_cmp() {
        let mut expr = Expr::Between {
            expr: Box::new(Expr::Identifier(Ident::new("age"))),
            negated: false,
            low: Box::new(Expr::Value(
                ast::Value::Number("1".to_string(), false).into(),
            )),
            high: Box::new(Expr::Value(
                ast::Value::Number("3".to_string(), false).into(),
            )),
        };

        let _ = SQLiteBetweenVisitor::default().pre_visit_expr(&mut expr);

        assert_eq!(
            expr,
            Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    Ident::new("age")
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                    ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("1".to_string())
                                                        .into()
                                                ),),
                                            )],
                                            clauses: Vec::new(),
                                        },),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::<ast::OrderByExpr>::new(),
                                    }
                                ),)),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::<ast::OrderByExpr>::new(),
                    })),
                    op: BinaryOperator::GtEq,
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                    Ident::new("age")
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                    ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("3".to_string())
                                                        .into()
                                                ),),
                                            )],
                                            clauses: Vec::new(),
                                        },),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::<ast::OrderByExpr>::new(),
                                    }
                                ),)),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::<ast::OrderByExpr>::new(),
                    })),
                    op: BinaryOperator::LtEq,
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
            }
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_rebuild_between_numeric_low_binary_op() {
        let mut expr = Expr::Between {
            expr: Box::new(Expr::Value(
                ast::Value::Number("10".to_string(), false).into(),
            )),
            negated: false,
            low: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(
                    ast::Value::Number("1".to_string(), false).into(),
                )),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(
                    ast::Value::Number("2".to_string(), false).into(),
                )),
            }),
            high: Box::new(Expr::Value(
                ast::Value::Number("20".to_string(), false).into(),
            )),
        };

        let _ = SQLiteBetweenVisitor::default().pre_visit_expr(&mut expr);

        assert_eq!(
            expr,
            Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    ast::Value::Number("10".to_string(), false).into()
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryOp {
                                    left: Box::new(Expr::Function(ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("1".to_string())
                                                        .into()
                                                ))
                                            )],
                                            clauses: Vec::new(),
                                        }),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::new(),
                                    })),
                                    op: BinaryOperator::Plus,
                                    right: Box::new(Expr::Function(ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("2".to_string())
                                                        .into()
                                                ))
                                            )],
                                            clauses: Vec::new(),
                                        }),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::new(),
                                    })),
                                })),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::new(),
                    })),
                    op: BinaryOperator::GtEq,
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    ast::Value::Number("10".to_string(), false).into()
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                    ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString(
                                                        "20".to_string()
                                                    )
                                                    .into()
                                                ),),
                                            )],
                                            clauses: Vec::new(),
                                        },),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::new(),
                                    }
                                ),)),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::new(),
                    })),
                    op: BinaryOperator::LtEq,
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
            }
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_rebuild_not_between_into_decimal_cmp() {
        let mut expr = Expr::Between {
            expr: Box::new(Expr::Value(
                ast::Value::Number("1".to_string(), false).into(),
            )),
            negated: true,
            low: Box::new(Expr::Value(
                ast::Value::Number("2".to_string(), false).into(),
            )),
            high: Box::new(Expr::Value(
                ast::Value::Number("3".to_string(), false).into(),
            )),
        };

        let _ = SQLiteBetweenVisitor::default().pre_visit_expr(&mut expr);

        assert_eq!(
            expr,
            Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    ast::Value::Number("1".to_string(), false).into()
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                    ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("2".to_string())
                                                        .into()
                                                ),),
                                            )],
                                            clauses: Vec::new(),
                                        },),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::new(),
                                    }
                                ),)),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::new(),
                    })),
                    op: BinaryOperator::Lt, // Negated: GtEq becomes Lt
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
                op: BinaryOperator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(Expr::Function(ast::Function {
                        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                            "decimal_cmp"
                        ))]),
                        args: ast::FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    ast::Value::Number("1".to_string(), false).into()
                                ))),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Function(
                                    ast::Function {
                                        name: ast::ObjectName(vec![
                                            ast::ObjectNamePart::Identifier(Ident::new("decimal"))
                                        ]),
                                        args: ast::FunctionArguments::List(FunctionArgumentList {
                                            duplicate_treatment: None,
                                            args: vec![FunctionArg::Unnamed(
                                                FunctionArgExpr::Expr(Expr::Value(
                                                    ast::Value::SingleQuotedString("3".to_string())
                                                        .into()
                                                ),),
                                            )],
                                            clauses: Vec::new(),
                                        },),
                                        over: None,
                                        uses_odbc_syntax: false,
                                        parameters: ast::FunctionArguments::None,
                                        filter: None,
                                        null_treatment: None,
                                        within_group: Vec::new(),
                                    }
                                ),)),
                            ],
                            clauses: Vec::new(),
                        }),
                        over: None,
                        uses_odbc_syntax: false,
                        parameters: ast::FunctionArguments::None,
                        filter: None,
                        null_treatment: None,
                        within_group: Vec::new(),
                    })),
                    op: BinaryOperator::Gt, // Negated: LtEq becomes Gt
                    right: Box::<Expr>::from(Expr::Value(
                        ast::Value::Number("0".to_string(), false).into()
                    )),
                }),
            }
        );
    }

    #[test]
    fn test_rebuild_between_string_low_not_modified() {
        let original_expr = Expr::Between {
            expr: Box::new(Expr::Value(
                ast::Value::Number("1".to_string(), false).into(),
            )),
            negated: false,
            low: Box::new(Expr::Value(
                ast::Value::SingleQuotedString("2".to_string()).into(),
            )),
            high: Box::new(Expr::Value(
                ast::Value::Number("3".to_string(), false).into(),
            )),
        };
        let mut expr = original_expr.clone();

        let _ = SQLiteBetweenVisitor::default().pre_visit_expr(&mut expr);

        // Expect no change because 'low' is a string
        assert_eq!(expr, original_expr);
    }
}
