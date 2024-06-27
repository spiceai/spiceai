/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    common::{Constraint, Constraints},
    execution::context::SessionContext,
    logical_expr::{col, count, lit, utils::COUNT_STAR_EXPANSION},
};
use futures::future;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Incoming data violates uniqueness constraint on column(s): {}", unique_cols.join(", ")))]
    BatchViolatesUniquenessConstraint { unique_cols: Vec<String> },

    #[snafu(display("{source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The goal for this function is to determine if all of the data described in `batches` conforms to the constraints described in `constraints`.
///
/// It does this by creating a memory table from the record batches and then running a query against the table to validate the constraints.
pub async fn validate_batch_with_constraints(
    batches: &[RecordBatch],
    constraints: &Constraints,
) -> Result<()> {
    if batches.is_empty() || constraints.is_empty() {
        return Ok(());
    }

    let mut futures = Vec::new();
    for constraint in &**constraints {
        let fut = validate_batch_with_constraint(batches.to_vec(), constraint.clone());
        futures.push(fut);
    }

    future::try_join_all(futures).await?;

    Ok(())
}

#[tracing::instrument(level = "debug", skip(batches))]
async fn validate_batch_with_constraint(
    batches: Vec<RecordBatch>,
    constraint: Constraint,
) -> Result<()> {
    let unique_cols = match constraint {
        Constraint::PrimaryKey(cols) | Constraint::Unique(cols) => cols,
    };

    let schema = batches[0].schema();
    let unique_fields = unique_cols
        .iter()
        .map(|col| schema.field(*col))
        .collect::<Vec<_>>();

    let ctx = SessionContext::new();
    let df = ctx.read_batches(batches).context(DataFusionSnafu)?;

    let Ok(count_name) = count(lit(COUNT_STAR_EXPANSION)).display_name() else {
        unreachable!()
    };

    // This is equivalent to:
    // ```sql
    // SELECT COUNT(1), <unique_field_names> FROM mem_table GROUP BY <unique_field_names> HAVING COUNT(1) > 1
    // ```
    let num_rows = df
        .aggregate(
            unique_fields.iter().map(|f| col(f.name())).collect(),
            vec![count(lit(COUNT_STAR_EXPANSION))],
        )
        .context(DataFusionSnafu)?
        .filter(col(count_name).gt(lit(1)))
        .context(DataFusionSnafu)?
        .count()
        .await
        .context(DataFusionSnafu)?;

    if num_rows > 0 {
        BatchViolatesUniquenessConstraintSnafu {
            unique_cols: unique_fields
                .iter()
                .map(|col| col.name().to_string())
                .collect::<Vec<_>>(),
        }
        .fail()?;
    }

    Ok(())
}

#[must_use]
pub fn get_primary_keys_from_constraints(
    constraints: &Constraints,
    schema: &SchemaRef,
) -> Vec<String> {
    let mut primary_keys: Vec<String> = Vec::new();
    for constraint in constraints.clone() {
        if let Constraint::PrimaryKey(cols) = constraint {
            cols.iter()
                .map(|col| schema.field(*col).name())
                .for_each(|col| {
                    primary_keys.push(col.to_string());
                });
        }
    }
    primary_keys
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::datatypes::SchemaRef;
    use datafusion::{
        common::{Constraints, DFSchema},
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
        sql::sqlparser::ast::{Ident, TableConstraint},
    };

    #[tokio::test]
    async fn test_validate_batch_with_constraints() -> Result<(), Box<dyn std::error::Error>> {
        let parquet_bytes = reqwest::get("https://public-data.spiceai.org/eth.recent_logs.parquet")
            .await?
            .bytes()
            .await?;

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)?.build()?;

        let records = parquet_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        let schema = records[0].schema();

        let constraints =
            get_unique_constraints(&["log_index", "transaction_hash"], Arc::clone(&schema));

        let result = super::validate_batch_with_constraints(&records, &constraints).await;
        assert!(
            result.is_ok(),
            "{}",
            result.expect_err("this returned an error")
        );

        let invalid_constraints = get_unique_constraints(&["block_number"], Arc::clone(&schema));
        let result = super::validate_batch_with_constraints(&records, &invalid_constraints).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("this returned an error").to_string(),
            "Incoming data violates uniqueness constraint on column(s): block_number"
        );

        let invalid_constraints =
            get_unique_constraints(&["block_number", "transaction_hash"], Arc::clone(&schema));
        let result = super::validate_batch_with_constraints(&records, &invalid_constraints).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("this returned an error").to_string(),
            "Incoming data violates uniqueness constraint on column(s): block_number, transaction_hash"
        );

        Ok(())
    }

    pub(crate) fn get_unique_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        Constraints::new_from_table_constraints(
            &[TableConstraint::Unique {
                name: None,
                index_name: None,
                index_type_display: datafusion::sql::sqlparser::ast::KeyOrIndexDisplay::None,
                index_type: None,
                columns: cols.iter().map(|col| Ident::new(*col)).collect(),
                index_options: vec![],
                characteristics: None,
            }],
            &Arc::new(DFSchema::try_from(schema).expect("valid schema")),
        )
        .expect("valid constraints")
    }

    pub(crate) fn get_pk_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        Constraints::new_from_table_constraints(
            &[TableConstraint::PrimaryKey {
                name: None,
                index_name: None,
                index_type: None,
                columns: cols.iter().map(|col| Ident::new(*col)).collect(),
                index_options: vec![],
                characteristics: None,
            }],
            &Arc::new(DFSchema::try_from(schema).expect("valid schema")),
        )
        .expect("valid constraints")
    }
}
