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

use std::fmt::Write as _;

use arrow_schema::DataType;
use datafusion::{
    common::{
        DFSchema,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    error::DataFusionError,
    logical_expr::ExprSchemable,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to determine data type: {source}"))]
    DataTypeError { source: DataFusionError },
    #[snafu(display("Expression {expr} does not meet the criteria: {criterion} Expression Criteria: {}", PartitionCriteria.doc()))]
    CriterionFailed { expr: String, criterion: String },
    #[snafu(display("Invalid expression: {message}"))]
    InvalidExpression { message: String },
    #[snafu(display("Parsing SQL expression failed: {source}"))]
    ParsingExpression { source: DataFusionError },
    #[snafu(display(
        "Scalar value type {scalar_type} is incompatible with expression type {expr_type}"
    ))]
    IncompatibleTypes {
        scalar_type: String,
        expr_type: String,
    },
}

pub type ValidationResult = Result<(), Error>;

#[derive(Clone, PartialEq, PartialOrd, Eq, Debug, Hash)]
pub struct PartitionedBy {
    pub name: String,
    pub expression: Expr,
}

/// Converts the spicepod `partition_by` list of [`String`]s into [`Expr`]s,
/// validating that they meet the expression criteria.
///
/// # Errors
/// Returns an error if the `partition_by` expressions could not be parsed or
/// validated.
pub fn partition_by_expressions(
    partitioned_by: &[spicepod::partitioning::PartitionedBy],
    ctx: &SessionContext,
    df_schema: &DFSchema,
) -> Result<Vec<PartitionedBy>, Error> {
    let partitioned_by = partitioned_by
        .iter()
        .map(|p| {
            let expression = ctx
                .parse_sql_expr(&p.expression, df_schema)
                .context(ParsingExpressionSnafu)?;
            PartitionCriteria.validate(&expression, df_schema)?;
            Ok(PartitionedBy {
                name: p.name.clone(),
                expression,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(partitioned_by)
}

/// Validates whether a [`ScalarValue`] can be produced by the given [`Expr`].
///
/// # Errors
/// Returns an error if the types are incompatible.
pub fn validate_scalar_compatibility(
    expr: &Expr,
    scalar: &ScalarValue,
    schema: &DFSchema,
) -> ValidationResult {
    let (expr_type, _nullable) = expr.data_type_and_nullable(schema).context(DataTypeSnafu)?;
    let scalar_type = scalar.data_type();

    ensure!(
        expr_type == scalar_type,
        IncompatibleTypesSnafu {
            scalar_type: scalar_type.to_string(),
            expr_type: expr_type.to_string()
        }
    );
    Ok(())
}

/// Trait for defining validation criteria for an Expr.
pub trait Criterion: Send + Sync {
    /// Returns the documentation string for this criterion.
    fn doc(&self) -> String;

    /// Validate the expression meets a certain criterion.
    ///
    /// # Errors
    /// Returns an error if the validation failed or cannot complete.
    fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult;
}

struct PartitionCriteria;

impl PartitionCriteria {
    const CRITERIA: &[&dyn Criterion] = &[
        &DataTypeCriterion,
        &SingleColumnCriterion,
        &ForbiddenExpressionCriterion,
    ];
}

impl Criterion for PartitionCriteria {
    fn doc(&self) -> String {
        let mut criteria_string = String::new();
        for criterion in PartitionCriteria::CRITERIA {
            let _ = writeln!(criteria_string, "- {}", criterion.doc());
        }
        criteria_string
    }

    fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult {
        for criterion in Self::CRITERIA {
            criterion.validate(expr, schema)?;
        }

        Ok(())
    }
}

/// Validates that the [`Expr`]'s data type is String, Number, Boolean, and
/// Timestamp.
struct DataTypeCriterion;

impl Criterion for DataTypeCriterion {
    fn doc(&self) -> String {
        "data type must be a String, Number, Boolean or Timestamp".to_string()
    }

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
                    | DataType::Timestamp(_, _)
            ),
            CriterionFailedSnafu {
                expr: expr.to_string(),
                criterion: self.doc(),
            }
        );

        Ok(())
    }
}

/// Validates that the Expr references exactly one column from the schema.
struct SingleColumnCriterion;

impl Criterion for SingleColumnCriterion {
    fn doc(&self) -> String {
        "expression must reference a single column".to_string()
    }

    fn validate(&self, expr: &Expr, _schema: &DFSchema) -> ValidationResult {
        let num_columns = expr.column_refs().len();
        ensure!(
            num_columns == 1,
            CriterionFailedSnafu {
                expr: expr.to_string(),
                criterion: format!(
                    "Expression references {num_columns} columns, expected exactly 1"
                )
            }
        );
        Ok(())
    }
}

struct ForbiddenExpressionCriterion;

impl Criterion for ForbiddenExpressionCriterion {
    fn doc(&self) -> String {
        "expression must not contain Alias, OuterReferenceColumn, Unnest, WindowFunction,
  AggregateFunction, Exists, InSubquery, ScalarSubquery, Placeholder, or
  GroupingSet"
            .to_string()
    }

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
        .map_err(|_| Error::InvalidExpression {
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
    async fn test_partition_expression_criterion() -> Result<(), Error> {
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
        criterion
            .validate(&expr, &schema)
            .expect("should create expression");

        // Valid: date_trunc('month', date)
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: date_trunc(),
            args: vec![lit("month"), col("date")],
        });
        criterion
            .validate(&expr, &schema)
            .expect("should create expression");

        // Invalid: Two columns (a + region)
        let expr = col("a") + col("region");
        criterion
            .validate(&expr, &schema)
            .expect_err("should be invalid expression");

        // Invalid: Literal (no column)
        let expr = lit(42);
        criterion
            .validate(&expr, &schema)
            .expect_err("should be invalid expression");
        // Invalid: Alias
        let expr = Expr::Alias(Alias {
            expr: Box::new(col("region")),
            name: "aliased".to_string(),
            relation: None,
            metadata: None,
        });
        criterion
            .validate(&expr, &schema)
            .expect_err("should be invalid expression");

        // Invalid: Non-existent column
        let expr = col("missing");
        criterion
            .validate(&expr, &schema)
            .expect_err("should be invalid expression");
        Ok(())
    }
}
