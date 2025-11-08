/*
Copyright 2025 The Spice.ai OSS Authors

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

use datafusion::{
    logical_expr::{Case, expr::ScalarFunction},
    prelude::Expr,
    scalar::ScalarValue,
};
use snafu::prelude::*;
use twox_hash::XxHash64;

const HASH_SEED: u64 = 7;

const INDEX_NAME_MAX_LENGTH: usize = 45;
const COLUMN_NAME_MAX_LENGTH: usize = 5;
const PARTITION_VALUE_MAX_LENGTH: usize = 5;
const PARTITION_BY_MAX_LENGTH: usize = 5;

const _NUM_SEPARATORS: usize = 3; // 3 periods '.' separate the 4 parts
/// See [CreateIndex](https://docs.aws.amazon.com/AmazonS3/latest/API/API_S3VectorBuckets_CreateIndex.html#API_S3VectorBuckets_CreateIndex_RequestSyntax)
const _S3_VECTOR_INDEX_NAME_MAX_LENGTH: usize = 63;

// Check at compile time that we use the full amount allowed from S3
const _: () = {
    assert!(
        INDEX_NAME_MAX_LENGTH
            + COLUMN_NAME_MAX_LENGTH
            + PARTITION_VALUE_MAX_LENGTH
            + PARTITION_BY_MAX_LENGTH
            + _NUM_SEPARATORS
            == _S3_VECTOR_INDEX_NAME_MAX_LENGTH
    );
};

static PARTS_SEPARATOR: &str = ".";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expected exactly 4 parts in index name separated by periods, but found {num_parts}"
    ))]
    IncorrectNumPartsInName { num_parts: usize },
    #[snafu(display("The 'partition_by' expression, {expr}, is not supported"))]
    UnsupportedPartitionByExpression { expr: Box<Expr> },
    #[snafu(display("Index name, '{index}', cannot contain periods when using 'partition_by'"))]
    InvalidIndexNamePeriod { index: String },
    #[snafu(display(
        "Index names are restricted to {INDEX_NAME_MAX_LENGTH} characters when using 'partition_by', but {index} is {len} characters"
    ))]
    InvalidIndexNameLength { index: String, len: usize },
}

#[derive(Debug)]
pub struct PartitionedIndexName {
    index_name: String,
    column_name_hash: String,
    partition_value_hash: String,
    partition_by_hash: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BelongsWith {
    ThisDataset,
    DifferentDataset,
    DifferentColumn,
    DifferentPartitionByExpressions,
}

impl PartitionedIndexName {
    pub fn new(
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
        partition_value: &ScalarValue,
    ) -> Result<Self, Error> {
        validate_index(index_name)?;
        let index_name = truncate(&sanitize_column(index_name), INDEX_NAME_MAX_LENGTH);
        let column_name_hash = truncate(&hash_to_hex(column_name), COLUMN_NAME_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string(partition_by)?),
            PARTITION_BY_MAX_LENGTH,
        );
        let partition_value_hash = truncate(
            &hash_to_hex(&partition_value.to_string()),
            PARTITION_VALUE_MAX_LENGTH,
        );
        Ok(Self {
            index_name,
            column_name_hash,
            partition_value_hash,
            partition_by_hash,
        })
    }

    pub fn common_prefix(
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
    ) -> Result<String, Error> {
        validate_index(index_name)?;
        let index_name = truncate(&sanitize_column(index_name), INDEX_NAME_MAX_LENGTH);
        let column_name_hash = truncate(&hash_to_hex(column_name), COLUMN_NAME_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string(partition_by)?),
            PARTITION_BY_MAX_LENGTH,
        );
        Ok([index_name, column_name_hash, partition_by_hash].join(PARTS_SEPARATOR))
    }

    /// Format an index name suitable for S3 Vectors
    #[must_use]
    pub fn to_index_name(&self) -> String {
        [
            self.index_name.clone(),
            self.column_name_hash.clone(),
            self.partition_by_hash.clone(),
            self.partition_value_hash.clone(),
        ]
        .join(PARTS_SEPARATOR)
    }

    pub fn from_index_name(index_name: &str) -> Result<Self, Error> {
        let parts: Vec<&str> = index_name.split(PARTS_SEPARATOR).collect();
        let num_parts = parts.len();
        ensure!(num_parts == 4, IncorrectNumPartsInNameSnafu { num_parts });
        Ok(Self {
            index_name: parts[0].to_string(),
            column_name_hash: parts[1].to_string(),
            partition_by_hash: parts[2].to_string(),
            partition_value_hash: parts[3].to_string(),
        })
    }

    /// Determines if the partitions come from the same dataset
    #[must_use]
    pub fn belongs_with(
        &self,
        index_name: &str,
        column_name: &str,
        partition_by: &[Expr],
    ) -> BelongsWith {
        let index_name = truncate(&sanitize_column(index_name), INDEX_NAME_MAX_LENGTH);
        let column_name_hash = truncate(&hash_to_hex(column_name), COLUMN_NAME_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string(partition_by).unwrap_or_default()),
            PARTITION_BY_MAX_LENGTH,
        );

        if self.index_name != index_name {
            BelongsWith::DifferentDataset
        } else if self.column_name_hash != column_name_hash {
            BelongsWith::DifferentColumn
        } else if self.partition_by_hash != partition_by_hash {
            BelongsWith::DifferentPartitionByExpressions
        } else {
            BelongsWith::ThisDataset
        }
    }
}

fn sanitize_column(s: &str) -> String {
    s.replace(['_', '.'], "-")
}

fn validate_index(index: &str) -> Result<(), Error> {
    let len = index.len();
    ensure!(
        len <= INDEX_NAME_MAX_LENGTH,
        InvalidIndexNameLengthSnafu {
            index: index.to_string(),
            len
        }
    );
    ensure!(!index.contains('.'), InvalidIndexNamePeriodSnafu { index });

    Ok(())
}

fn truncate(s: &str, len: usize) -> String {
    s.chars().take(len).collect()
}

fn hash_to_hex(input: &str) -> String {
    let hash = XxHash64::oneshot(HASH_SEED, input.as_bytes());
    format!("{hash:x}")
}

// Provide a stable string representation of the expressions
fn to_stable_string(exprs: &[Expr]) -> Result<String, Error> {
    Ok(exprs
        .iter()
        .map(stable_expr_string)
        .collect::<Result<Vec<_>, _>>()?
        .join(PARTS_SEPARATOR))
}

#[allow(clippy::too_many_lines)]
fn stable_expr_string(expr: &Expr) -> Result<String, Error> {
    Ok(match expr {
        Expr::Column(col) => {
            format!("Column({})", col.name())
        }
        Expr::ScalarVariable(_, vars) => {
            format!("ScalarVariable({})", vars.join("."))
        }
        Expr::Literal(scalar, _) => {
            format!("Literal({scalar})")
        }
        Expr::BinaryExpr(binary) => {
            let left = stable_expr_string(&binary.left)?;
            let op = binary.op;
            let right = stable_expr_string(&binary.right)?;
            format!("BinaryExpr({left} {op} {right})")
        }
        Expr::Not(inner) => {
            format!("Not({})", stable_expr_string(inner)?)
        }
        Expr::IsNotNull(inner) => {
            format!("IsNotNull({})", stable_expr_string(inner)?)
        }
        Expr::IsNull(inner) => {
            format!("IsNull({})", stable_expr_string(inner)?)
        }
        Expr::IsTrue(inner) => {
            format!("IsTrue({})", stable_expr_string(inner)?)
        }
        Expr::IsFalse(inner) => {
            format!("IsFalse({})", stable_expr_string(inner)?)
        }
        Expr::IsUnknown(inner) => {
            format!("IsUnknown({})", stable_expr_string(inner)?)
        }
        Expr::IsNotTrue(inner) => {
            format!("IsNotTrue({})", stable_expr_string(inner)?)
        }
        Expr::IsNotFalse(inner) => {
            format!("IsNotFalse({})", stable_expr_string(inner)?)
        }
        Expr::IsNotUnknown(inner) => {
            format!("IsNotUnknown({})", stable_expr_string(inner)?)
        }
        Expr::Negative(inner) => {
            format!("Negative({})", stable_expr_string(inner)?)
        }
        Expr::Between(between) => {
            let expr = stable_expr_string(&between.expr)?;
            let low = stable_expr_string(&between.low)?;
            let high = stable_expr_string(&between.high)?;
            format!(
                "Between({expr}, {negated}, {low}, {high})",
                negated = between.negated
            )
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            let expr = match expr {
                Some(expr) => format!("Some({})", stable_expr_string(expr)?),
                None => "None".to_string(),
            };
            let else_expr = else_expr
                .as_ref()
                .and_then(|e| stable_expr_string(e).ok())
                .unwrap_or_else(|| "None".to_string());
            let when_then_expr = when_then_expr
                .iter()
                .map(|(w, t)| {
                    Ok(format!(
                        "({} => {})",
                        stable_expr_string(w)?,
                        stable_expr_string(t)?
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            format!("Case({expr}, {when_then_expr}, {else_expr})")
        }
        Expr::Cast(cast) => {
            let expr = stable_expr_string(&cast.expr)?;
            format!("Cast({expr}, {})", cast.data_type)
        }
        Expr::TryCast(cast) => {
            let expr = stable_expr_string(&cast.expr)?;
            format!("TryCast({expr}, {})", cast.data_type)
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let args_str = args
                .iter()
                .map(stable_expr_string)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            format!("ScalarFunction({}({args_str}))", func.name())
        }
        Expr::InList(in_list) => {
            let expr = stable_expr_string(&in_list.expr)?;
            let list_str = in_list
                .list
                .iter()
                .map(stable_expr_string)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            format!("InList({expr}, [{list_str}])")
        }
        e => {
            return Err(Error::UnsupportedPartitionByExpression {
                expr: Box::new(e.clone()),
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use datafusion::functions::regex::regexp_match;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::prelude::{case, col, lit};
    use datafusion::scalar::ScalarValue;
    use insta::assert_snapshot;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn belongs_with() -> Result<()> {
        let index_name = "mydataset";
        let column_name = "_my.column";
        let partition_by = &[col(column_name)];

        let this = PartitionedIndexName::from_index_name("mydataset.29d6f.7f7c5.blahh")?;

        assert_eq!(
            this.belongs_with(index_name, column_name, partition_by),
            BelongsWith::ThisDataset
        );
        assert_eq!(
            this.belongs_with(index_name, "_your.column", partition_by),
            BelongsWith::DifferentColumn
        );
        assert_eq!(
            this.belongs_with(index_name, column_name, &[]),
            BelongsWith::DifferentPartitionByExpressions
        );
        assert_eq!(
            this.belongs_with("yourdataset", column_name, partition_by),
            BelongsWith::DifferentDataset
        );

        Ok(())
    }

    #[test]
    fn index_name_length_restricted() {
        let index_name = "a".repeat(INDEX_NAME_MAX_LENGTH + 1);
        let column_name = "col1";
        let partition_value = ScalarValue::from("val");
        let partition_by = vec![col("col1")];

        assert!(
            PartitionedIndexName::new(&index_name, column_name, &partition_by, &partition_value)
                .is_err()
        );
    }

    #[test]
    fn new_index_partition_name() -> Result<()> {
        let index_name = "test_index";
        let column_name = "test_col";
        let partition_value = ScalarValue::from("value");
        let partition_by = vec![col("col1")];

        let result =
            PartitionedIndexName::new(index_name, column_name, &partition_by, &partition_value)?;

        assert_eq!(result.index_name, "test-index");
        assert_eq!(result.column_name_hash.len(), COLUMN_NAME_MAX_LENGTH);
        assert_eq!(
            result.partition_value_hash.len(),
            PARTITION_VALUE_MAX_LENGTH
        );
        assert_eq!(result.partition_by_hash.len(), PARTITION_BY_MAX_LENGTH);

        Ok(())
    }

    #[test]
    fn from_index_name_valid() -> Result<()> {
        let name = "test-index.test-col.abcde.12345";
        let result = PartitionedIndexName::from_index_name(name)?;

        assert_eq!(result.index_name, "test-index");
        assert_eq!(result.column_name_hash, "test-col");
        assert_eq!(result.partition_by_hash, "abcde");
        assert_eq!(result.partition_value_hash, "12345");

        Ok(())
    }

    #[test]
    fn from_index_name_invalid_parts() {
        let name = "test.index.col";
        let result = PartitionedIndexName::from_index_name(name);

        assert!(result.is_err());
    }

    #[test]
    fn sanitize_replaces_underscores() {
        let input = "test_index_name";
        let result = sanitize_column(input);
        assert_eq!(result, "test-index-name");
    }

    #[test]
    fn truncate_limits_length() {
        let input = "a".repeat(10);
        let result = truncate(&input, 5);
        assert_eq!(result, "aaaaa");
    }

    #[test]
    fn hash_to_hex_consistent() {
        let input = "test";
        let result1 = hash_to_hex(input);
        let result2 = hash_to_hex(input);
        assert_eq!(result1, result2);
    }

    #[test]
    fn to_index_name_format() {
        let index = PartitionedIndexName {
            index_name: "idx".to_string(),
            column_name_hash: "col".to_string(),
            partition_by_hash: "abcde".to_string(),
            partition_value_hash: "12345".to_string(),
        };

        let result = index.to_index_name();
        assert_eq!(result, "idx.col.abcde.12345");
    }

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct Bucket {
        signature: Signature,
    }

    impl Bucket {
        #[must_use]
        pub fn new() -> Self {
            Self {
                signature: Signature::any(2, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for Bucket {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &'static str {
            "bucket"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> std::result::Result<DataType, DataFusionError> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> std::result::Result<ColumnarValue, DataFusionError> {
            unimplemented!()
        }
    }

    #[test]
    fn partition_by_stability() -> Result<()> {
        let partition_by = &[col("id").eq(lit(7))];
        assert_snapshot!(to_stable_string(partition_by)?);
        let partition_by = &[Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(Bucket::new())),
            args: vec![lit(10i64), col("a")],
        })];
        assert_snapshot!(to_stable_string(partition_by)?);
        let partition_by = &[col("a") % lit(10)];
        assert_snapshot!(to_stable_string(partition_by)?);
        let partition_by = &[col("region")];
        assert_snapshot!(to_stable_string(partition_by)?);
        let partition_by = &[case(Expr::ScalarFunction(ScalarFunction {
            func: regexp_match(),
            args: vec![col("a"), lit("^DATAFUSION(-cli)*")],
        }))
        .when(lit(true), lit("datafusion"))
        .otherwise(lit("other"))?];
        assert_snapshot!(to_stable_string(partition_by)?);
        Ok(())
    }
}
