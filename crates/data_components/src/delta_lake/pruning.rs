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

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch},
    compute::and,
    datatypes::{Field, Schema},
};
use datafusion::{
    common::DFSchema,
    datasource::listing::{PartitionedFile, helpers::expr_applicable_for_cols},
    execution::context::ExecutionProps,
    logical_expr::Expr,
    physical_expr::create_physical_expr,
    scalar::ScalarValue,
};

/// Prune the partitions of the given `partitioned_files` based on the given `filters`.
///
/// Modified from: <https://github.com/apache/datafusion/blob/28856e15bd490044d24619e19057160e647aa256/datafusion/catalog-listing/src/helpers.rs#L238>
pub(crate) fn prune_partitions(
    partitioned_files: Vec<PartitionedFile>,
    filters: &[Expr],
    partition_cols: &[Field],
) -> Result<Vec<PartitionedFile>, datafusion::error::DataFusionError> {
    if filters.is_empty() {
        return Ok(partitioned_files);
    }

    // We will use DataFusion itself to evaluate the filters on the partition values, so we need to
    // first extract the partition values from the `PartitionedFile`s and create a `RecordBatch`.
    // First verify all files have the correct number of partition values
    assert!(
        partitioned_files
            .iter()
            .all(|file| file.partition_values.len() == partition_cols.len()),
        "PartitionedFile has a different number of partition values than the number of partition columns"
    );

    let partition_arrays: Vec<ArrayRef> = (0..partition_cols.len())
        .map(|col_idx| {
            ScalarValue::iter_to_array(
                partitioned_files
                    .iter()
                    .map(|file| file.partition_values[col_idx].clone()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    let schema = Arc::new(Schema::new(partition_cols.to_vec()));

    let df_schema =
        DFSchema::from_unqualified_fields(partition_cols.to_vec().into(), HashMap::default())?;

    let batch = RecordBatch::try_new(schema, partition_arrays)?;

    // Now that we have the `RecordBatch`, use DataFusion to evaluate the filters.
    let props = ExecutionProps::new();

    // Applies `filter` to `batch`
    let do_filter = |filter| -> Result<ArrayRef, datafusion::error::DataFusionError> {
        let expr = create_physical_expr(filter, &df_schema, &props)?;
        expr.evaluate(&batch)?.into_array(partitioned_files.len())
    };

    // Compute the conjunction of the filters
    let mask = filters
        .iter()
        .map(|f| do_filter(f).map(|a| a.as_boolean().clone()))
        .reduce(|a, b| Ok(and(&a?, &b?)?));

    let mask = match mask {
        Some(Ok(mask)) => mask,
        Some(Err(err)) => return Err(err),
        None => return Ok(partitioned_files),
    };

    // Don't retain partitions that evaluated to null
    let prepared = match mask.null_count() {
        0 => mask,
        _ => prep_null_mask_filter(&mask),
    };

    // Sanity check
    assert_eq!(prepared.len(), partitioned_files.len());

    // Filter the `PartitionedFile`s based on the mask
    let filtered = partitioned_files
        .into_iter()
        .zip(prepared.values())
        .filter_map(|(p, f)| f.then_some(p))
        .collect();

    Ok(filtered)
}

/// Remove null values by doing a bitmask AND operation with null bits and the boolean bits.
fn prep_null_mask_filter(filter: &BooleanArray) -> BooleanArray {
    let Some(nulls) = filter.nulls() else {
        unreachable!("Filter should have nulls");
    };
    let mask = filter.values() & nulls.inner();
    BooleanArray::new(mask, None)
}

/// Expressions can be used for partition pruning if they can be evaluated using
/// only the partiton columns.
///
/// Taken from: <https://github.com/apache/datafusion/blob/28856e15bd490044d24619e19057160e647aa256/datafusion/core/src/datasource/listing/table.rs#L816>
pub(crate) fn can_be_evaluted_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty() && expr_applicable_for_cols(partition_column_names, expr)
}
