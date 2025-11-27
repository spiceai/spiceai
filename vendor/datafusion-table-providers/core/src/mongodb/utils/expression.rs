use datafusion::{
    logical_expr::{Expr, Operator},
    scalar::ScalarValue,
};
use mongodb::bson::{doc, Bson, Document};

pub fn combine_exprs_with_and(exprs: &[Expr]) -> Option<Expr> {
    let mut iter = exprs.iter();
    let first = iter.next()?.clone();
    Some(iter.fold(first, |acc, e| acc.and(e.clone())))
}

pub fn expr_to_mongo_filter(expr: &Expr) -> Option<Document> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                let l = expr_to_mongo_filter(&binary.left)?;
                let r = expr_to_mongo_filter(&binary.right)?;
                Some(doc! { "$and": [l, r] })
            }
            Operator::Or => {
                let l = expr_to_mongo_filter(&binary.left)?;
                let r = expr_to_mongo_filter(&binary.right)?;
                Some(doc! { "$or": [l, r] })
            }
            Operator::Eq => {
                let field = extract_column_name(&binary.left);
                let value = extract_literal_value(&binary.right);
                let field = field?;
                let value = value?;
                Some(doc! { field: value })
            }
            Operator::Gt => {
                let field = extract_column_name(&binary.left);
                let value = extract_literal_value(&binary.right);
                let field = field?;
                let value = value?;
                Some(doc! { field: { "$gt": value } })
            }
            Operator::Lt => {
                let field = extract_column_name(&binary.left)?;
                let value = extract_literal_value(&binary.right)?;
                Some(doc! { field: { "$lt": value } })
            }
            Operator::GtEq => {
                let field = extract_column_name(&binary.left)?;
                let value = extract_literal_value(&binary.right)?;
                Some(doc! { field: { "$gte": value } })
            }
            Operator::LtEq => {
                let field = extract_column_name(&binary.left)?;
                let value = extract_literal_value(&binary.right)?;
                Some(doc! { field: { "$lte": value } })
            }
            Operator::NotEq => {
                let field = extract_column_name(&binary.left)?;
                let value = extract_literal_value(&binary.right)?;
                Some(doc! { field: { "$ne": value } })
            }
            _ => {
                println!("Unsupported operator: {:?}", binary.op);
                None
            }
        },
        _ => {
            println!("Non-binary expr: {expr:?}");
            None
        }
    }
}

fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        _ => None,
    }
}

fn extract_literal_value(expr: &Expr) -> Option<Bson> {
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Utf8(Some(s)) => Some(Bson::String(s.clone())),
            ScalarValue::Utf8(None) => Some(Bson::Null),
            ScalarValue::Int32(Some(i)) => Some(Bson::Int32(*i)),
            ScalarValue::Int32(None) => Some(Bson::Null),
            ScalarValue::Int64(Some(i)) => Some(Bson::Int64(*i)),
            ScalarValue::Int64(None) => Some(Bson::Null),
            ScalarValue::Float32(Some(f)) => Some(Bson::Double(*f as f64)),
            ScalarValue::Float32(None) => Some(Bson::Null),
            ScalarValue::Float64(Some(f)) => Some(Bson::Double(*f)),
            ScalarValue::Float64(None) => Some(Bson::Null),
            ScalarValue::Boolean(Some(b)) => Some(Bson::Boolean(*b)),
            ScalarValue::Boolean(None) => Some(Bson::Null),

            ScalarValue::UInt8(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::UInt16(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::UInt32(Some(i)) => Some(Bson::Int64(*i as i64)),
            ScalarValue::UInt64(Some(i)) => Some(Bson::Int64(*i as i64)),
            ScalarValue::Int8(Some(i)) => Some(Bson::Int32(*i as i32)),
            ScalarValue::Int16(Some(i)) => Some(Bson::Int32(*i as i32)),

            ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None) => Some(Bson::Null),

            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::BinaryExpr;
    use datafusion::prelude::{col, lit};

    #[test]
    fn test_combine_exprs_with_and() {
        let exprs = vec![col("age").gt(lit(30)), col("active").eq(lit(true))];

        let combined = combine_exprs_with_and(&exprs).unwrap();

        // Should produce (age > 30) AND (active = true)
        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::And);
        } else {
            panic!("Expected BinaryExpr with AND operator");
        }
    }

    #[test]
    fn test_simple_eq_filter() {
        let expr = col("name").eq(lit("Alice"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": "Alice" };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_gt_and_eq_filter() {
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("age").gt(lit(21))),
            op: Operator::And,
            right: Box::new(col("status").eq(lit("active"))),
        });

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                { "age": { "$gt": 21 } },
                { "status": "active" }
            ]
        };

        assert_eq!(filter, expected);
    }

    #[test]
    fn test_not_eq_filter() {
        let expr = col("role").not_eq(lit("guest"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "role": { "$ne": "guest" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_unsupported_expr_returns_none() {
        let expr = col("salary").in_list(vec![lit(100), lit(200)], false); // unsupported for now
        assert!(expr_to_mongo_filter(&expr).is_none());
    }

    #[test]
    fn test_lt_filter() {
        let expr = col("age").lt(lit(65));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "age": { "$lt": 65 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_gte_filter() {
        let expr = col("score").gt_eq(lit(85));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "score": { "$gte": 85 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_lte_filter() {
        let expr = col("temperature").lt_eq(lit(100));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "temperature": { "$lte": 100 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_or_filter() {
        let expr = col("department")
            .eq(lit("sales"))
            .or(col("department").eq(lit("marketing")));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                { "department": "sales" },
                { "department": "marketing" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_complex_and_or_filter() {
        let age_and_status = col("age").gt(lit(25)).and(col("status").eq(lit("active")));
        let expr = age_and_status.or(col("priority").eq(lit("high")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                {
                    "$and": [
                        { "age": { "$gt": 25 } },
                        { "status": "active" }
                    ]
                },
                { "priority": "high" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_nested_and_filters() {
        let age_range = col("age").gt(lit(18)).and(col("age").lt(lit(65)));
        let expr = age_range.and(col("country").eq(lit("US")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                {
                    "$and": [
                        { "age": { "$gt": 18 } },
                        { "age": { "$lt": 65 } }
                    ]
                },
                { "country": "US" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_boolean_filter() {
        let expr = col("is_verified").eq(lit(true));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "is_verified": true };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_float_filter() {
        let expr = col("price").gt(lit(99.99));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "price": { "$gt": 99.99 } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_string_comparison_filters() {
        let expr = col("name").gt(lit("M"));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! { "name": { "$gt": "M" } };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_mixed_type_and_filter() {
        let expr = col("age").gt(lit(21)).and(col("name").eq(lit("John")));
        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$and": [
                { "age": { "$gt": 21 } },
                { "name": "John" }
            ]
        };
        assert_eq!(filter, expected);
    }

    #[test]
    fn test_single_expr_combine() {
        let exprs = vec![col("status").eq(lit("active"))];
        let combined = combine_exprs_with_and(&exprs).unwrap();

        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::Eq);
        } else {
            panic!("Expected BinaryExpr with EQ operator");
        }
    }

    #[test]
    fn test_empty_exprs_combine() {
        let exprs: Vec<Expr> = vec![];
        let result = combine_exprs_with_and(&exprs);
        assert!(result.is_none());
    }

    #[test]
    fn test_three_exprs_combine() {
        let exprs = vec![
            col("age").gt(lit(25)),
            col("status").eq(lit("active")),
            col("department").eq(lit("engineering")),
        ];
        let combined = combine_exprs_with_and(&exprs).unwrap();

        if let Expr::BinaryExpr(bin) = &combined {
            assert_eq!(bin.op, Operator::And);
            if let Expr::BinaryExpr(left_bin) = &*bin.left {
                assert_eq!(left_bin.op, Operator::And);
            } else {
                panic!("Expected nested AND on left side");
            }
        } else {
            panic!("Expected BinaryExpr with AND operator");
        }
    }

    #[test]
    fn test_null_literal_filter() {
        let expr = col("optional_field").eq(lit(ScalarValue::Utf8(None)));
        let filter = expr_to_mongo_filter(&expr);

        if let Some(doc) = filter {
            let expected = doc! { "optional_field": mongodb::bson::Bson::Null };
            assert_eq!(doc, expected);
        }
    }

    #[test]
    fn test_wrong_operand_order_returns_none() {
        use datafusion::logical_expr::Expr;

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit("Alice")),
            op: Operator::Eq,
            right: Box::new(col("name")),
        });

        let filter = expr_to_mongo_filter(&expr);
        assert!(
            filter.is_none(),
            "Should return None for unsupported operand order"
        );
    }

    #[test]
    fn test_multiple_or_conditions() {
        let expr = col("status")
            .eq(lit("active"))
            .or(col("status").eq(lit("pending")))
            .or(col("status").eq(lit("review")));

        let filter = expr_to_mongo_filter(&expr).unwrap();
        let expected = doc! {
            "$or": [
                {
                    "$or": [
                        { "status": "active" },
                        { "status": "pending" }
                    ]
                },
                { "status": "review" }
            ]
        };
        assert_eq!(filter, expected);
    }
}
