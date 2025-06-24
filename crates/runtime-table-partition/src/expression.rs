/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow_schema::DataType;
use datafusion::{
    common::{
        DFSchema,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    error::DataFusionError,
    logical_expr::ExprSchemable,
    prelude::{Expr, SessionContext},
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum ValidationError {
    #[snafu(display("Failed to determine data type: {source}"))]
    DataTypeError { source: DataFusionError },
    #[snafu(display("Expression {expr} does not meet criterion: {message}"))]
    CriterionFailed { expr: String, message: String },
    #[snafu(display("Invalid expression: {message}"))]
    InvalidExpression { message: String },
    #[snafu(display("Parsing SQL expression failed: {source}"))]
    ParsingExpression { source: DataFusionError },
}

pub type ValidationResult = Result<(), ValidationError>;

/// Converts the spicepod `partition_by` list of [`String`]s into [`Expr`]s,
/// validating that they meet the expression criteria.
pub fn partition_by_expressions(
    partition_by: Vec<String>,
    ctx: &SessionContext,
    df_schema: &DFSchema,
) -> Result<Vec<Expr>, ValidationError> {
    partition_by
        .iter()
        .map(|sql| {
            let expr = ctx
                .parse_sql_expr(sql, df_schema)
                .context(ParsingExpressionSnafu)?;
            PartitionCriteria.validate(&expr, df_schema)?;
            Ok(expr)
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Trait for defining validation criteria for an Expr.
pub trait Criterion: Send + Sync {
    /// Validate the expression meets a certain criterion.
    ///
    /// # Errors
    /// Returns an error if the validation failed or cannot complete.
    fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult;
}

pub struct PartitionCriteria;

impl Criterion for PartitionCriteria {
    fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult {
        let criteria: Vec<&dyn Criterion> = vec![
            &DataTypeCriterion,
            &SingleColumnCriterion,
            &ForbiddenExpressionCriterion,
        ];

        for criterion in criteria {
            criterion.validate(expr, schema)?;
        }

        Ok(())
    }
}

/// Validates that the [`Expr`]'s data type is String, Number, or Boolean.
struct DataTypeCriterion;

impl Criterion for DataTypeCriterion {
    fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult {
        let (data_type, _nullable) = expr.data_type_and_nullable(schema).context(DataTypeSnafu)?;

        ensure!(
            matches!(
                data_type,
                DataType::Utf8
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Boolean
            ),
            CriterionFailedSnafu {
                expr: expr.to_string(),
                message: format!("Data type {data_type} is not a string, number, or boolean"),
            }
        );

        Ok(())
    }
}

/// Validates that the Expr references exactly one column from the schema.
struct SingleColumnCriterion;

impl Criterion for SingleColumnCriterion {
    fn validate(&self, expr: &Expr, _schema: &DFSchema) -> ValidationResult {
        let num_columns = expr.column_refs().len();
        ensure!(
            num_columns == 1,
            CriterionFailedSnafu {
                expr: expr.to_string(),
                message: format!("Expression references {num_columns}, expected exactly 1")
            }
        );
        Ok(())
    }
}

struct ForbiddenExpressionCriterion;

impl Criterion for ForbiddenExpressionCriterion {
    fn validate(&self, expr: &Expr, _schema: &DFSchema) -> ValidationResult {
        expr.apply(|expr| {
            if matches!(
                expr,
                Expr::Alias(_)
                    | Expr::OuterReferenceColumn(_, _)
                    | Expr::Unnest(_)
                    | Expr::WindowFunction(_)
                    | Expr::AggregateFunction(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::ScalarSubquery(_)
                    | Expr::Placeholder(_)
                    | Expr::GroupingSet(_)
            ) {
                // we do not use the error, just the condition
                Err(DataFusionError::External("".into()))
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .map_err(|_| ValidationError::InvalidExpression {
            message: format!("Unsupported expression {expr}"),
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::functions::datetime::date_trunc;
    use datafusion::logical_expr::expr::{Alias, ScalarFunction};
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::{case, regexp_match};

    use super::*;

    fn create_test_schema() -> DFSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
            Field::new(
                "date",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("sales_volume", DataType::Int32, true),
        ]));
        DFSchema::try_from(schema).expect("schema created")
    }

    #[tokio::test]
    async fn test_partition_expression_criterion() -> Result<(), ValidationError> {
        let schema = create_test_schema();

        let criterion = Arc::new(PartitionCriteria);

        // Valid: region
        let expr = col("region");
        criterion.validate(&expr, &schema).expect("is valid");

        // Valid: a > 5
        let expr = col("a").gt(lit(5));
        criterion.validate(&expr, &schema).expect("is valid");

        // Valid: a % 10
        let expr = col("a") % lit(10);
        criterion.validate(&expr, &schema).expect("is valid");

        // Valid: CASE WHEN a ~* '^DATAFUSION(-cli)*' THEN 'datafusion' ELSE 'other' END
        let expr = case(col("a"))
            .when(
                regexp_match(col("a"), lit("^DATAFUSION(-cli)*"), None),
                lit("datafusion"),
            )
            .otherwise(lit("other"))
            .expect("expression created");
        assert!(criterion.validate(&expr, &schema).is_ok());

        // Invalid: date_trunc('month', date)
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: date_trunc(),
            args: vec![lit("month"), col("date")],
        });
        assert!(criterion.validate(&expr, &schema).is_err());

        // Invalid: Two columns (a + region)
        let expr = col("a") + col("region");
        assert!(criterion.validate(&expr, &schema).is_err());

        // Invalid: Literal (no column)
        let expr = lit(42);
        assert!(criterion.validate(&expr, &schema).is_err());

        // Invalid: Alias
        let expr = Expr::Alias(Alias {
            expr: Box::new(col("region")),
            name: "aliased".to_string(),
            relation: None,
            metadata: None,
        });
        assert!(
            criterion.validate(&expr, &schema).is_err(),
            "forbidden expression"
        );

        // Invalid: Non-existent column
        let expr = col("missing");
        assert!(criterion.validate(&expr, &schema).is_err());

        Ok(())
    }
}
