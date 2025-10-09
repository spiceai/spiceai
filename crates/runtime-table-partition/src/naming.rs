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

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Case, Expr};
use datafusion::scalar::ScalarValue;
use snafu::prelude::*;
use twox_hash::XxHash64;

const HASH_SEED: u64 = 7;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Expected at least 3 parts in partition name separated by '{separator}', but found {num_parts}"
    ))]
    IncorrectNumPartsInName {
        num_parts: usize,
        separator: &'static str,
    },
    #[snafu(display("The 'partition_by' expression, {expr}, is not supported"))]
    UnsupportedPartitionByExpression { expr: Box<Expr> },
    #[snafu(display(
        "Prefixes are restricted to {max_length} characters when using 'partition_by', but {prefix} is {len} characters"
    ))]
    InvalidPrefixLength {
        prefix: String,
        len: usize,
        max_length: usize,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait PartitionNameConfig {
    const PREFIX_MAX_LENGTH: usize;
    const PARTITION_BY_MAX_LENGTH: usize;
    const PARTITION_VALUE_MAX_LENGTH: usize;

    const PARTS_SEPARATOR: &'static str;

    fn sanitize(prefix: &str) -> String {
        prefix.replace(['_'], "-")
    }

    fn validate(prefix: &str) -> Result<()> {
        let len = prefix.len();
        ensure!(
            len <= Self::PREFIX_MAX_LENGTH,
            InvalidPrefixLengthSnafu {
                prefix: prefix.to_string(),
                len,
                max_length: Self::PREFIX_MAX_LENGTH
            }
        );

        Ok(())
    }

    fn validate_total_length(parts: &[&str]) -> Result<()> {
        let total_length = parts.iter().map(|s| s.len()).sum::<usize>()
            + (parts.len().saturating_sub(1) * Self::PARTS_SEPARATOR.len());

        let max_total_length = Self::PREFIX_MAX_LENGTH
            + Self::PARTITION_BY_MAX_LENGTH
            + Self::PARTITION_VALUE_MAX_LENGTH
            + 2 * Self::PARTS_SEPARATOR.len();
        ensure!(
            total_length <= max_total_length,
            InvalidPrefixLengthSnafu {
                prefix: parts.join(Self::PARTS_SEPARATOR),
                len: total_length,
                max_length: max_total_length
            }
        );

        Ok(())
    }
}

pub struct StandardConfig;

impl PartitionNameConfig for StandardConfig {
    const PREFIX_MAX_LENGTH: usize = 200;
    const PARTITION_BY_MAX_LENGTH: usize = 10;
    const PARTITION_VALUE_MAX_LENGTH: usize = 15;
    const PARTS_SEPARATOR: &'static str = ".";
}

#[derive(Debug)]
pub struct PartitionedName {
    pub prefix: String,
    pub partition_value_hash: String,
    pub partition_by_hash: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BelongsWith {
    ThisDataset,
    DifferentDataset,
    DifferentPartitionByExpressions,
}

impl PartitionedName {
    pub fn new<C: PartitionNameConfig>(
        prefix: &str,
        partition_by: &[Expr],
        partition_value: &ScalarValue,
    ) -> Result<Self> {
        C::validate(prefix)?;

        let prefix = truncate(&C::sanitize(prefix), C::PREFIX_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string::<C>(partition_by)?),
            C::PARTITION_BY_MAX_LENGTH,
        );
        let partition_value_hash = truncate(
            &hash_to_hex(&partition_value.to_string()),
            C::PARTITION_VALUE_MAX_LENGTH,
        );

        Ok(Self {
            prefix,
            partition_value_hash,
            partition_by_hash,
        })
    }

    pub fn common_prefix<C: PartitionNameConfig>(
        prefix: &str,
        partition_by: &[Expr],
    ) -> Result<String> {
        C::validate(prefix)?;

        let prefix = truncate(&C::sanitize(prefix), C::PREFIX_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string::<C>(partition_by)?),
            C::PARTITION_BY_MAX_LENGTH,
        );

        Ok([prefix, partition_by_hash].join(C::PARTS_SEPARATOR))
    }

    #[must_use]
    pub fn to_partition_name<C: PartitionNameConfig>(&self) -> String {
        [
            self.prefix.clone(),
            self.partition_by_hash.clone(),
            self.partition_value_hash.clone(),
        ]
        .join(C::PARTS_SEPARATOR)
    }

    pub fn from_partition_name<C: PartitionNameConfig>(partition_name: &str) -> Result<Self> {
        let parts: Vec<&str> = partition_name.split(C::PARTS_SEPARATOR).collect();
        let num_parts = parts.len();
        ensure!(
            num_parts >= 3,
            IncorrectNumPartsInNameSnafu {
                num_parts,
                separator: C::PARTS_SEPARATOR
            }
        );
        Ok(Self {
            prefix: parts[0..num_parts - 2].join(C::PARTS_SEPARATOR),
            partition_by_hash: parts[num_parts - 2].to_string(),
            partition_value_hash: parts[num_parts - 1].to_string(),
        })
    }

    #[must_use]
    pub fn belongs_with<C: PartitionNameConfig>(
        &self,
        prefix: &str,
        partition_by: &[Expr],
    ) -> BelongsWith {
        let prefix = truncate(&C::sanitize(prefix), C::PREFIX_MAX_LENGTH);
        let partition_by_hash = truncate(
            &hash_to_hex(&to_stable_string::<C>(partition_by).unwrap_or_default()),
            C::PARTITION_BY_MAX_LENGTH,
        );

        if self.prefix != prefix {
            BelongsWith::DifferentDataset
        } else if self.partition_by_hash != partition_by_hash {
            BelongsWith::DifferentPartitionByExpressions
        } else {
            BelongsWith::ThisDataset
        }
    }
}

pub fn truncate(s: &str, len: usize) -> String {
    s.chars().take(len).collect()
}

pub fn hash_to_hex(input: &str) -> String {
    let hash = XxHash64::oneshot(HASH_SEED, input.as_bytes());
    format!("{hash:x}")
}

fn to_stable_string<C: PartitionNameConfig>(exprs: &[Expr]) -> Result<String> {
    Ok(exprs
        .iter()
        .map(stable_expr_string)
        .collect::<Result<Vec<_>>>()?
        .join(C::PARTS_SEPARATOR))
}

fn stable_expr_string(expr: &Expr) -> Result<String> {
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
                .collect::<Result<Vec<_>>>()?
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
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            format!("ScalarFunction({}({args_str}))", func.name())
        }
        Expr::InList(in_list) => {
            let expr = stable_expr_string(&in_list.expr)?;
            let list_str = in_list
                .list
                .iter()
                .map(stable_expr_string)
                .collect::<Result<Vec<_>>>()?
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
    use super::*;

    use std::sync::Arc;

    use arrow_schema::DataType;
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
        let prefix = "mydataset";
        let partition_by = &[col("user_id")];

        let this = PartitionedName::from_partition_name::<StandardConfig>(
            "mydataset.addfee318a.b0543ed9d433290",
        )?;

        assert_eq!(
            this.belongs_with::<StandardConfig>(prefix, partition_by),
            BelongsWith::ThisDataset
        );
        assert_eq!(
            this.belongs_with::<StandardConfig>(prefix, &[]),
            BelongsWith::DifferentPartitionByExpressions
        );
        assert_eq!(
            this.belongs_with::<StandardConfig>("yourdataset", partition_by),
            BelongsWith::DifferentDataset
        );

        Ok(())
    }

    #[test]
    fn prefix_length_restricted() {
        let prefix = "a".repeat(StandardConfig::PREFIX_MAX_LENGTH + 1);
        let partition_value = ScalarValue::from(1);
        let partition_by = vec![col("user_id")];

        assert!(
            PartitionedName::new::<StandardConfig>(&prefix, &partition_by, &partition_value)
                .is_err()
        );
    }

    #[test]
    fn new_partition_name() -> Result<()> {
        let prefix = "my_dataset";
        let partition_value = ScalarValue::from(1);
        let partition_by = vec![col("user_id")];

        let result =
            PartitionedName::new::<StandardConfig>(prefix, &partition_by, &partition_value)?;

        assert_eq!(result.prefix, "my-dataset");
        assert_eq!(
            result.partition_value_hash.len(),
            StandardConfig::PARTITION_VALUE_MAX_LENGTH
        );
        assert_eq!(
            result.partition_by_hash.len(),
            StandardConfig::PARTITION_BY_MAX_LENGTH
        );

        Ok(())
    }

    #[test]
    fn from_partition_name_valid() -> Result<()> {
        let name = "my-dataset.abcde.12345";
        let result = PartitionedName::from_partition_name::<StandardConfig>(name)?;

        assert_eq!(result.prefix, "my-dataset");
        assert_eq!(result.partition_by_hash, "abcde");
        assert_eq!(result.partition_value_hash, "12345");

        Ok(())
    }

    #[test]
    fn from_partition_name_invalid_parts() {
        let name = "mydataset.12345";
        let result = PartitionedName::from_partition_name::<StandardConfig>(name);

        assert!(result.is_err());
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
    fn to_prefix_format() {
        let partition = PartitionedName {
            prefix: "mydataset".to_string(),
            partition_by_hash: "abcde".to_string(),
            partition_value_hash: "12345".to_string(),
        };

        let result = partition.to_partition_name::<StandardConfig>();
        assert_eq!(result, "mydataset.abcde.12345");
    }

    #[derive(Debug)]
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
        assert_snapshot!(to_stable_string::<StandardConfig>(partition_by)?);
        let partition_by = &[Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(Bucket::new())),
            args: vec![lit(10i64), col("a")],
        })];
        assert_snapshot!(to_stable_string::<StandardConfig>(partition_by)?);
        let partition_by = &[col("a") % lit(10)];
        assert_snapshot!(to_stable_string::<StandardConfig>(partition_by)?);
        let partition_by = &[col("region")];
        assert_snapshot!(to_stable_string::<StandardConfig>(partition_by)?);
        let partition_by = &[case(Expr::ScalarFunction(ScalarFunction {
            func: regexp_match(),
            args: vec![col("a"), lit("^DATAFUSION(-cli)*")],
        }))
        .when(lit(true), lit("datafusion"))
        .otherwise(lit("other"))?];
        assert_snapshot!(to_stable_string::<StandardConfig>(partition_by)?);
        Ok(())
    }
}
