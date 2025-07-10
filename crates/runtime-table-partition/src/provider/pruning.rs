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

use datafusion::{
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
    scalar::ScalarValue,
};

/// Determine whether a partition should be pruned from the scan plan based on
/// the query `filters`, the expression that the partition was created from,
/// `partition_by`, and the `partition_value` produced by the `partition_by`
/// `Expr` for this particular partition.
pub(crate) fn prune_partition(
    filters: &[Expr],
    partition_by: &Expr,
    partition_value: &ScalarValue,
) -> bool {
    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) = filter
        {
            if left.as_ref() == partition_by {
                if let Expr::Literal(lit) = right.as_ref() {
                    return lit != partition_value;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;

    use super::*;

    #[test]
    fn test_prune_partition_exact_match() {
        let region_expr = Expr::Column(Column::from_name("region"));
        let partition_value = ScalarValue::Utf8(Some("us-east-1".to_string()));
        let filters = &[region_expr
            .clone()
            .eq(Expr::Literal(partition_value.clone()))];

        let partition_by = region_expr;
        assert!(!prune_partition(filters, &partition_by, &partition_value));

        let partition_value = ScalarValue::Utf8(Some("ap-northeast-2".to_string()));
        assert!(prune_partition(filters, &partition_by, &partition_value));
    }

    #[test]
    #[ignore]
    fn test_prune_partition_range() {
        let column = Expr::Column(Column::from_name("fare_amount"));
        let partition_by = column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(10.0))));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(10.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(9.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(11.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .lt(Expr::Literal(ScalarValue::Float64(Some(9.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
    }
}
