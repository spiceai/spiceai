use datafusion::sql::sqlparser::ast::{
    Expr, Function, Ident, ObjectNamePart, VisitorMut, WindowType,
};
use std::ops::ControlFlow;

#[derive(PartialEq, Eq)]
enum FunctionType {
    Rank,
    Sum,
    Max,
    Avg,
    Min,
    Count,
}

impl FunctionType {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "RANK" => Some(Self::Rank),
            "SUM" => Some(Self::Sum),
            "MAX" => Some(Self::Max),
            "AVG" => Some(Self::Avg),
            "MIN" => Some(Self::Min),
            "COUNT" => Some(Self::Count),
            _ => None,
        }
    }

    fn rewrite_scalar(&self, func: &mut Function) {
        if self == &Self::Rank {
            MySQLWindowVisitor::remove_frame_clause(func);
        };

        MySQLWindowVisitor::remove_nulls_first_last(func);
    }
}

#[derive(Default)]
pub struct MySQLWindowVisitor {}

impl VisitorMut for MySQLWindowVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr {
            if let Some(ObjectNamePart::Identifier(Ident { value, .. })) = func.name.0.first() {
                // match for some scalars that support window functions
                // all of them need to remove nulls first/last, but only rank removes the frame clause
                if let Some(func_type) = FunctionType::from_str(value) {
                    func_type.rewrite_scalar(func);
                }
            }
        }

        ControlFlow::Continue(())
    }
}

impl MySQLWindowVisitor {
    pub fn remove_nulls_first_last(func: &mut Function) {
        if let Some(WindowType::WindowSpec(spec)) = func.over.as_mut() {
            for order_by in &mut spec.order_by {
                order_by.options.nulls_first = None; // nulls first/last are not supported in MySQL
            }
        }
    }

    pub fn remove_frame_clause(func: &mut Function) {
        if let Some(WindowType::WindowSpec(spec)) = func.over.as_mut() {
            spec.window_frame = None; // frame (window) clauses are ignored for rank() in MySQL: https://dev.mysql.com/doc/refman/8.4/en/window-functions-frames.html
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::sql::sqlparser::{
        self,
        ast::{self, helpers::attached_token::AttachedToken, ObjectName, WindowFrame},
        tokenizer::{Span, Token, TokenWithSpan},
    };

    use super::*;

    #[test]
    fn test_remove_frame_clause() {
        let mut func = Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "RANK".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            args: ast::FunctionArguments::None,
            over: Some(WindowType::WindowSpec(ast::WindowSpec {
                window_name: None,
                partition_by: vec![],
                order_by: vec![sqlparser::ast::OrderByExpr {
                    expr: sqlparser::ast::Expr::Wildcard(AttachedToken(TokenWithSpan::wrap(
                        Token::Char('*'),
                    ))),
                    options: sqlparser::ast::OrderByOptions {
                        asc: None,
                        nulls_first: Some(true),
                    },
                    with_fill: None,
                }],
                window_frame: Some(WindowFrame {
                    units: sqlparser::ast::WindowFrameUnits::Rows,
                    start_bound: sqlparser::ast::WindowFrameBound::CurrentRow,
                    end_bound: None,
                }),
            })),
            parameters: sqlparser::ast::FunctionArguments::None,
            null_treatment: None,
            filter: None,
            within_group: vec![],
            uses_odbc_syntax: false,
        };

        let expected = Some(WindowType::WindowSpec(sqlparser::ast::WindowSpec {
            window_name: None,
            partition_by: vec![],
            order_by: vec![sqlparser::ast::OrderByExpr {
                expr: sqlparser::ast::Expr::Wildcard(AttachedToken(TokenWithSpan::wrap(
                    Token::Char('*'),
                ))),
                options: sqlparser::ast::OrderByOptions {
                    asc: None,
                    nulls_first: Some(true),
                },
                with_fill: None,
            }],
            window_frame: None,
        }));

        MySQLWindowVisitor::remove_frame_clause(&mut func);

        assert_eq!(func.over, expected);
    }

    #[test]
    fn test_remove_nulls_first_last() {
        let mut func = Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "RANK".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            args: sqlparser::ast::FunctionArguments::None,
            over: Some(WindowType::WindowSpec(sqlparser::ast::WindowSpec {
                window_name: None,
                partition_by: vec![],
                order_by: vec![sqlparser::ast::OrderByExpr {
                    expr: sqlparser::ast::Expr::Wildcard(AttachedToken(TokenWithSpan::wrap(
                        Token::Char('*'),
                    ))),
                    options: sqlparser::ast::OrderByOptions {
                        asc: None,
                        nulls_first: Some(true),
                    },
                    with_fill: None,
                }],
                window_frame: Some(WindowFrame {
                    units: sqlparser::ast::WindowFrameUnits::Rows,
                    start_bound: sqlparser::ast::WindowFrameBound::CurrentRow,
                    end_bound: None,
                }),
            })),
            parameters: sqlparser::ast::FunctionArguments::None,
            null_treatment: None,
            filter: None,
            within_group: vec![],
            uses_odbc_syntax: false,
        };

        let expected = Some(WindowType::WindowSpec(sqlparser::ast::WindowSpec {
            window_name: None,
            partition_by: vec![],
            order_by: vec![sqlparser::ast::OrderByExpr {
                expr: sqlparser::ast::Expr::Wildcard(AttachedToken(TokenWithSpan::wrap(
                    Token::Char('*'),
                ))),
                options: sqlparser::ast::OrderByOptions {
                    asc: None,
                    nulls_first: None,
                },
                with_fill: None,
            }],
            window_frame: Some(WindowFrame {
                units: sqlparser::ast::WindowFrameUnits::Rows,
                start_bound: sqlparser::ast::WindowFrameBound::CurrentRow,
                end_bound: None,
            }),
        }));

        MySQLWindowVisitor::remove_nulls_first_last(&mut func);

        assert_eq!(func.over, expected);
    }
}
