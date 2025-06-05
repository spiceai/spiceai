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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::aggregation::from_single_input;
use crate::{
    SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME, VectorSearchGenerationResult,
    collect_batches,
};

use super::{AggregationResult, CandidateAggregation, DatafusionSnafu};
use super::{Error, Result};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use snafu::ResultExt;

/// Reciprocal Rank Fusion (RRF) is a method for combining multiple ranked sets of search results.
/// The underlying score of the search results is not important, only the rank (per stream order).
/// The rank, for a given entry (for some primary key `a`) is converted to a score using the formula:
/// ```
/// score_a = 1 / (rank_i + offset) + 1 / (rank_j + offset) + ...
/// ```
/// Where `rank_i` is the rank of the i-th stream, and `offset` is a constant (e.g. 60).
pub struct ReciprocalRankFusion;

#[async_trait]
impl CandidateAggregation for ReciprocalRankFusion {
    async fn aggregate(
        &self,
        mut data: Vec<VectorSearchGenerationResult>,
        primary_key: Vec<String>,
        limit: usize,
    ) -> Result<AggregationResult> {
        let num_inputs = data.len();
        // Handle 0, or 1 candidates.
        if num_inputs <= 1 {
            return data
                .pop()
                .map(|d| from_single_input(d, primary_key))
                .ok_or(Error::NoCandidatesGenerated);
        }

        if primary_key.is_empty() {
            return Err(Error::NoPrimaryKey);
        }

        let schemas = data.iter().map(|d| d.data.schema()).collect::<Vec<_>>();
        let () = verify_schema_compatibility(schemas.as_slice())?;

        let ctx = SessionContext::new();
        let mut table_names: Vec<String> = Vec::with_capacity(num_inputs);

        // Find all additional columns in the schema that are not part of the primary key or the expected
        // search columns.
        let mut additional_columns = HashSet::new();
        let mut matches: HashMap<String, Vec<String>> = HashMap::new();

        // Inefficient, but collect each stream, convert to [`MemTable`].
        let mut i = 0;
        for VectorSearchGenerationResult {
            data: stream,
            derived_from,
        } in data
        {
            let schema = stream.schema();
            additional_columns.extend(additional_columns_of_schema(
                &schema,
                primary_key.as_slice(),
            ));

            // Since we know what the `SEARCH_VALUE_COLUMN_NAME` column for the i'th column will be in the final schema,
            // we can add it to the `matches` map now.
            matches
                .get_mut(derived_from.as_str())
                .map(|v| v.push(ith_search_value_column(i)))
                .unwrap_or_else(|| {
                    matches.insert(derived_from.clone(), vec![ith_search_value_column(i)]);
                });

            let data = collect_batches(stream).await.context(DatafusionSnafu)?;

            // If data is empty, don't use.
            if data.first().is_none_or(|rb| rb.num_rows() == 0) {
                continue;
            }

            let table_name = format!("search_candidates_{i}");
            table_names.push(table_name.clone());
            let table = MemTable::try_new(schema, vec![data]).context(DatafusionSnafu)?;
            let _ = ctx
                .register_table(TableReference::bare(table_name), Arc::new(table))
                .context(DatafusionSnafu)?;

            i += 1;
        }

        let additional_columns = additional_columns.into_iter().collect::<Vec<_>>();

        let sql = reciprocal_rank_fusion_sql(
            table_names.as_slice(),
            primary_key.as_slice(),
            additional_columns.as_slice(),
            60,
            limit,
        );
        tracing::debug!("Runnning SQL in standalone context: ```sql\n{sql}\n```");
        let df = ctx.sql(sql.as_str()).await.context(DatafusionSnafu)?;

        let data = df.execute_stream().await.context(DatafusionSnafu)?;

        Ok(AggregationResult {
            data,
            primary_key,
            data_columns: additional_columns.into_iter().collect(),
            matches,
        })
    }
}

/// Returns a list of additional columns in the schema that are not part of the primary key or the expected
/// search columns (i.e. score or underlying value).
fn additional_columns_of_schema(schema: &SchemaRef, primary_key: &[String]) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            let name = f.name();
            if [SEARCH_SCORE_COLUMN_NAME, SEARCH_VALUE_COLUMN_NAME].contains(&name.as_str())
                || primary_key.contains(f.name())
            {
                return None;
            }
            Some(name.clone())
        })
        .collect()
}

/// Verifies that all streams have the same schema and contain the required columns: [`SEARCH_VALUE_COLUMN_NAME`], [`SEARCH_SCORE_COLUMN_NAME`].
fn verify_schema_compatibility(schemas: &[SchemaRef]) -> Result<()> {
    let Some(schema) = schemas.first() else {
        return Ok(());
    };

    for s in schemas {
        if s.fields().is_empty() {
            // Empty schema -> empty data
            continue;
        }
        if s.column_with_name(SEARCH_VALUE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_VALUE_COLUMN_NAME.to_string(),
            });
        }

        if s.column_with_name(SEARCH_SCORE_COLUMN_NAME).is_none() {
            return Err(Error::CandidateMissingRequiredColumn {
                col: SEARCH_SCORE_COLUMN_NAME.to_string(),
            });
        }

        // Check that the schema is the same across all streams (i.e. all same as the first).
        // Ensure each column is in first schema, and equal number of columns.
        let correct_columns = s.fields().iter().any(|f| {
            let Some((_, f2)) = schema.column_with_name(f.name()) else {
                return false;
            };
            f2.data_type() == f.data_type() && f2.is_nullable() == f.is_nullable()
        });
        if schema.fields().len() != s.fields().len() || !correct_columns {
            return Err(Error::InconsistentColumns {
                s1: Arc::clone(schema),
                s2: Arc::clone(s),
            });
        }
    }

    Ok(())
}

fn ith_search_value_column(i: usize) -> String {
    format!("{SEARCH_VALUE_COLUMN_NAME}_{i}")
}

/// Generates the SQL for the RRF aggregation.
fn reciprocal_rank_fusion_sql(
    tables: &[String],
    primary_key: &[String],
    additional_columns: &[String],
    offset: usize,
    limit: usize,
) -> String {
    // 1) Add explicit rank one CTE per table, ranking _only_ by the PK columns
    //
    // ```sql
    //    my_tbl AS (
    //      SELECT *,
    //             ROW_NUMBER() OVER (ORDER BY doc_id, section) AS rank
    //      FROM my_tbl
    //    ),
    // ```
    let pk_list = primary_key.join(", ");
    let cte_defs: String = tables
        .iter()
        .map(|tbl| {
            format!(
                "{tbl} AS (\n    \
                    SELECT\n    \
                        *,\n    \
                        ROW_NUMBER() OVER (ORDER BY {pk_list}) AS rank\n    \
                    FROM {tbl}\n\
                )"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    // 2) Build the RRF sum. This is the rank for each row in each table. If a row (as defined by the PK) is missing, it contributes a score of 0.
    let fusion_sum: String = tables
        .iter()
        .map(|tbl| format!("coalesce(1.0/({tbl}.rank + {offset}), 0)"))
        .collect::<Vec<_>>()
        .join(" + ");

    // 3) Coalesce the PK columns and additional columns across all tables.
    //    Additional columns will be consistent due to join on primary keys
    //    (i.e. if two tables have a given column, the values for a row will be equal).
    let select_keys: String = coalesce_columns(
        [primary_key, additional_columns].concat().as_slice(),
        tables,
    );

    // 4) FULL OUTER JOINs across tables on all PK columns.
    let joins: String = tables[1..]
        .iter()
        .map(|tbl| {
            let cond = primary_key
                .iter()
                .map(|col| format!("{}.{} = {}.{}", tables[0], col, tbl, col))
                .collect::<Vec<_>>()
                .join(" AND\n    ");
            format!("FULL OUTER JOIN {tbl} ON \n    {cond}")
        })
        .collect::<Vec<_>>()
        .join("\n");

    // TODO: instead of `{base}.{SEARCH_VALUE_COLUMN_NAME} as {SEARCH_VALUE_COLUMN_NAME},\n    \`
    let value_cols = tables
        .iter()
        .enumerate()
        .map(|(i, tbl)| {
            format!(
                "{tbl}.{SEARCH_VALUE_COLUMN_NAME} AS {alias}",
                alias = ith_search_value_column(i)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    // Make column for each table. using ith_search_value_column
    format!(
        "WITH {cte_defs}\n\
        SELECT\n    \
           TRUNC({fusion_sum}, 6) AS {SEARCH_SCORE_COLUMN_NAME},\n    \
           {value_cols},\n    \
           {select_keys}\n\
         FROM {base}\n\
         {joins}\n\
         ORDER BY {SEARCH_SCORE_COLUMN_NAME} DESC\n\
         LIMIT {limit};",
        base = tables[0]
    )
}

/// Coalesce the PK columns and additional columns across all tables:
///
/// ```sql
///    coalesce(bm25.doc_id, vector.doc_id, …) AS doc_id,
///    coalesce(bm25.section, vector.section, …) AS section
///  ```
fn coalesce_columns(cols: &[String], tables: &[String]) -> String {
    cols.iter()
        .map(|col| {
            format!(
                "coalesce({cols}) as {col}",
                cols = tables
                    .iter()
                    .map(|tbl| format!("{tbl}.{col}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ")
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_single_table_single_key() {
        insta::assert_snapshot!(reciprocal_rank_fusion_sql(
            vec!["bm25".to_string()].as_slice(),
            ["doc_id".to_string()].as_slice(),
            &[],
            42,
            3,
        ));
    }

    #[test]
    fn test_two_tables_single_key() {
        insta::assert_snapshot!(reciprocal_rank_fusion_sql(
            vec!["bm25".to_string(), "vector".to_string()].as_slice(),
            ["doc_id".to_string()].as_slice(),
            &[],
            5,
            3
        ));
    }

    #[test]
    fn test_three_tables_composite_key() {
        insta::assert_snapshot!(reciprocal_rank_fusion_sql(
            ["t1".to_string(), "t2".to_string(), "t3".to_string()].as_slice(),
            ["doc_id".to_string(), "section".to_string()].as_slice(),
            &[],
            100,
            4
        ));
    }

    #[test]
    fn test_multiple_keys_and_tables() {
        insta::assert_snapshot!(reciprocal_rank_fusion_sql(
            ["alpha".to_string(), "beta".to_string()].as_slice(),
            ["k1".to_string(), "k2".to_string(), "k3".to_string()].as_slice(),
            &[],
            2,
            4
        ));
    }

    #[test]
    fn test_two_tables_additional_columns() {
        insta::assert_snapshot!(reciprocal_rank_fusion_sql(
            vec!["bm25".to_string(), "vector".to_string()].as_slice(),
            ["doc_id".to_string()].as_slice(),
            &["foo".to_string(), "bar".to_string()],
            5,
            3
        ));
    }

    #[test]
    fn test_additional_columns_of_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Int8, false),
            Field::new(SEARCH_VALUE_COLUMN_NAME, DataType::Int8, false),
            Field::new("pk", DataType::Utf8, false),
            Field::new("additional", DataType::Int8, false),
        ]));
        let primary_keys = vec!["pk".to_string()];
        assert_eq!(
            additional_columns_of_schema(&schema, primary_keys.as_slice()),
            vec!["additional".to_string()]
        );
    }
}
