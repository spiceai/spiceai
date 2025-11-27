use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::{
    common::{Constraint, Constraints},
    execution::context::SessionContext,
    functions_aggregate::{count::count, min_max::max_udaf},
    logical_expr::{
        col, expr::WindowFunctionParams, lit, utils::COUNT_STAR_EXPANSION, Expr, WindowFrame,
        WindowFunctionDefinition,
    },
    prelude::ident,
};
use futures::future;
use snafu::prelude::*;
use std::{fmt::Display, sync::Arc};

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

const BATCH_ROW_NUMBER_COLUMN_NAME: &str = "__row_num";
const MAX_ROW_NUMBER_COLUMN_NAME: &str = "__max_row_num";

/// Configuration options for upsert behavior
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UpsertOptions {
    /// Remove duplicates after validation to resolve primary key conflicts
    pub remove_duplicates: bool,
    /// Use "last write wins" behavior - when duplicates are found, keep the row with the highest row number
    pub last_write_wins: bool,
}

impl UpsertOptions {
    /// Create a new instance with default settings
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_remove_duplicates(mut self, remove_duplicates: bool) -> Self {
        self.remove_duplicates = remove_duplicates;
        self
    }

    pub fn with_last_write_wins(mut self, last_write_wins: bool) -> Self {
        self.last_write_wins = last_write_wins;
        self
    }

    pub fn is_default(&self) -> bool {
        !self.remove_duplicates && !self.last_write_wins
    }
}

impl Display for UpsertOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut options = Vec::new();
        if self.remove_duplicates {
            options.push("remove_duplicates");
        }
        if self.last_write_wins {
            options.push("last_write_wins");
        }
        write!(f, "{}", options.join(","))
    }
}

impl TryFrom<&str> for UpsertOptions {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let options = value.split(',').map(str::trim).collect::<Vec<_>>();
        let mut upsert_options = Self::default();

        for option in options {
            match option {
                "remove_duplicates" => upsert_options.remove_duplicates = true,
                "last_write_wins" => upsert_options.last_write_wins = true,
                "" => {}
                _ => {
                    return Err(Error::DataFusion {
                        source: datafusion::error::DataFusionError::Plan(format!(
                            "Unknown upsert option: {}",
                            option
                        )),
                    });
                }
            }
        }

        Ok(upsert_options)
    }
}

/// The goal for this function is to determine if all of the data described in `batches` conforms to the constraints described in `constraints`.
///
/// It does this by creating a memory table from the record batches and then running a query against the table to validate the constraints.
///
/// The `options` parameter allows customizing validation behavior such as duplicate removal.
///
/// Returns the potentially modified batches (e.g., with duplicates removed or last-write-wins applied).
pub async fn validate_batch_with_constraints(
    batches: Vec<RecordBatch>,
    constraints: &Constraints,
    options: &UpsertOptions,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || constraints.is_empty() {
        return Ok(batches);
    }

    // First check if any constraints are violated without attempting to fix them.
    let mut futures = Vec::new();
    for constraint in &**constraints {
        let fut = validate_batch_with_constraint_with_options(
            batches.clone(),
            constraint.clone(),
            UpsertOptions::default(),
        );
        futures.push(fut);
    }

    match future::try_join_all(futures).await {
        Ok(_) => {
            // No constraints were violated, just return.
            return Ok(batches);
        }
        Err(e) => {
            // Some constraints were violated, if we have the default validation options we need to return an error, otherwise we'll try to fix the batches
            if options.is_default() {
                return Err(e);
            }
        }
    };

    // Some constraints were violated, but we can potentially fix them
    // These need to run sequentially since the batches are modified to fix the constraint violations.
    let mut processed_batches = batches;
    for constraint in &**constraints {
        processed_batches = validate_batch_with_constraint_with_options(
            processed_batches,
            constraint.clone(),
            options.clone(),
        )
        .await?;
    }
    Ok(processed_batches)
}

#[tracing::instrument(level = "debug", skip(batches, options))]
async fn validate_batch_with_constraint_with_options(
    batches: Vec<RecordBatch>,
    constraint: Constraint,
    options: UpsertOptions,
) -> Result<Vec<RecordBatch>> {
    let unique_cols = match constraint {
        Constraint::PrimaryKey(cols) | Constraint::Unique(cols) => cols,
    };

    let schema = batches[0].schema();
    let unique_fields = unique_cols
        .iter()
        .map(|col| schema.field(*col))
        .collect::<Vec<_>>();

    // Prepare data with row numbers if last write wins is enabled
    let batches_with_row_nums = if options.last_write_wins {
        add_row_numbers_to_batches(batches)?
    } else {
        batches
    };

    let ctx = SessionContext::new();
    let mut df = ctx
        .read_batches(batches_with_row_nums)
        .context(DataFusionSnafu)?;

    // Apply last write wins logic - keep only the row with the highest row number for each unique key
    if options.last_write_wins {
        df = apply_last_write_wins(&mut df, &unique_fields)
            .await
            .context(DataFusionSnafu)?;
    }

    // Remove duplicates first if requested to resolve primary key conflicts for duplicate rows
    if options.remove_duplicates {
        df = df.distinct().context(DataFusionSnafu)?;
    }

    let count_name = count(lit(COUNT_STAR_EXPANSION)).schema_name().to_string();

    // This is equivalent to:
    // ```sql
    // SELECT COUNT(1), <unique_field_names> FROM mem_table GROUP BY <unique_field_names> HAVING COUNT(1) > 1
    // ```
    let num_rows = df
        .clone()
        .aggregate(
            unique_fields.iter().map(|f| ident(f.name())).collect(),
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

    // Return the processed batches
    let final_batches = df.collect().await.context(DataFusionSnafu)?;
    Ok(final_batches)
}

/// Add row numbers to batches for tracking insertion order in last write wins logic
fn add_row_numbers_to_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let mut batches_with_row_nums = Vec::new();
    let mut global_row_number = 0i64;

    for batch in batches {
        let num_rows = batch.num_rows();
        let row_numbers: Vec<i64> =
            (global_row_number..global_row_number + num_rows as i64).collect();
        global_row_number += num_rows as i64;

        // Create new schema with additional row_num column
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            BATCH_ROW_NUMBER_COLUMN_NAME,
            DataType::Int64,
            false,
        )));
        let new_schema = Arc::new(Schema::new(fields));

        // Create new columns including row_num
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int64Array::from(row_numbers)) as arrow::array::ArrayRef);

        let new_batch =
            RecordBatch::try_new(new_schema, columns).map_err(|e| Error::DataFusion {
                source: datafusion::error::DataFusionError::ArrowError(Box::new(e), None),
            })?;
        batches_with_row_nums.push(new_batch);
    }

    Ok(batches_with_row_nums)
}

/// Apply last write wins logic using window functions to keep only the row with highest row number for each unique key
async fn apply_last_write_wins(
    df: &mut datafusion::dataframe::DataFrame,
    unique_fields: &[&arrow::datatypes::Field],
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    // Create partition by expressions for the unique fields
    let partition_by: Vec<Expr> = unique_fields
        .iter()
        .map(|field| col(field.name()))
        .collect();

    // Create a MAX window function to get the maximum row number for each partition
    let max_row_num_expr =
        Expr::WindowFunction(Box::new(datafusion::logical_expr::expr::WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
            params: WindowFunctionParams {
                args: vec![col(BATCH_ROW_NUMBER_COLUMN_NAME)],
                partition_by,
                order_by: vec![col(BATCH_ROW_NUMBER_COLUMN_NAME).sort(false, true)], // Order by row_num DESC
                window_frame: WindowFrame::new(Some(false)),
                null_treatment: None,
                distinct: false,
                filter: None,
            },
        }));

    // Add the maximum row number as a column with explicit alias
    let df_with_max = df
        .clone()
        .with_column(MAX_ROW_NUMBER_COLUMN_NAME, max_row_num_expr)?;

    // Filter to keep only rows where __row_num equals the max row number for its partition
    let filtered_df = df_with_max
        .filter(col(BATCH_ROW_NUMBER_COLUMN_NAME).eq(col(MAX_ROW_NUMBER_COLUMN_NAME)))?;

    // Get all original columns from the dataframe (excluding __row_num)
    let df_schema = df.schema();
    let original_column_exprs: Vec<Expr> = df_schema
        .fields()
        .iter()
        .filter(|field| field.name() != BATCH_ROW_NUMBER_COLUMN_NAME)
        .map(|field| col(field.name()))
        .collect();

    let final_df = filtered_df.select(original_column_exprs)?;

    Ok(final_df)
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
    use super::*;
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::common::{Constraint, Constraints};

    #[tokio::test]
    async fn test_validate_batch_with_constraints() -> Result<(), Box<dyn std::error::Error>> {
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Fetch the parquet file
        let parquet_bytes = reqwest::get("https://public-data.spiceai.org/taxi_sample.parquet")
            .await?
            .bytes()
            .await?;

        // Read parquet into record batches
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)?
            .build()?;

        let batches: Vec<RecordBatch> = parquet_reader.collect::<Result<Vec<_>, _>>()?;
        let schema = batches[0].schema();

        // Create a unique constraint on vendor_id
        let constraints = get_unique_constraints(&["vendor_id"], schema);

        // This should fail because there are duplicate vendor_ids
        let result =
            validate_batch_with_constraints(batches, &constraints, &UpsertOptions::default())
                .await;
        assert!(result.is_err());

        Ok(())
    }

    pub(crate) fn get_unique_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        // Convert column names to their corresponding indices
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|&col_name| schema.column_with_name(col_name).map(|(index, _)| index))
            .collect();

        Constraints::new_unverified(vec![Constraint::Unique(indices)])
    }

    pub(crate) fn get_pk_constraints(cols: &[&str], schema: SchemaRef) -> Constraints {
        // Convert column names to their corresponding indices
        let indices: Vec<usize> = cols
            .iter()
            .filter_map(|&col_name| schema.column_with_name(col_name).map(|(index, _)| index))
            .collect();

        Constraints::new_unverified(vec![Constraint::PrimaryKey(indices)])
    }

    /// Builder for creating test RecordBatches with specific data patterns
    #[derive(Debug)]
    pub struct TestDataBuilder {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl TestDataBuilder {
        pub fn new(schema: SchemaRef) -> Self {
            Self {
                schema,
                batches: Vec::new(),
            }
        }

        pub fn with_columns(columns: &[(&str, DataType)]) -> Self {
            let fields: Vec<Field> = columns
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            Self::new(schema)
        }

        pub fn add_string_batch(
            self,
            data: Vec<Vec<Option<&str>>>,
        ) -> Result<Self, arrow::error::ArrowError> {
            let columns: Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                (0..self.schema.fields().len())
                    .map(|col_idx| {
                        let column_data: Vec<Option<String>> = data
                            .iter()
                            .map(|row| row.get(col_idx).and_then(|v| v.map(|s| s.to_string())))
                            .collect();
                        Ok(Arc::new(StringArray::from(column_data)) as ArrayRef)
                    })
                    .collect();

            let batch = RecordBatch::try_new(self.schema.clone(), columns?)?;
            let mut new_self = self;
            new_self.batches.push(batch);
            Ok(new_self)
        }

        pub fn add_int_batch(
            self,
            data: Vec<Vec<Option<i32>>>,
        ) -> Result<Self, arrow::error::ArrowError> {
            let columns: Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                (0..self.schema.fields().len())
                    .map(|col_idx| {
                        let column_data: Vec<Option<i32>> = data
                            .iter()
                            .map(|row| row.get(col_idx).copied().flatten())
                            .collect();
                        Ok(Arc::new(Int32Array::from(column_data)) as ArrayRef)
                    })
                    .collect();

            let batch = RecordBatch::try_new(self.schema.clone(), columns?)?;
            let mut new_self = self;
            new_self.batches.push(batch);
            Ok(new_self)
        }

        pub fn build(self) -> Vec<RecordBatch> {
            self.batches
        }
    }

    /// Builder for creating different types of constraints
    #[derive(Debug)]
    pub struct ConstraintBuilder {
        schema: SchemaRef,
    }

    impl ConstraintBuilder {
        pub fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }

        /// Create a unique constraint on the specified columns
        pub fn unique_on(&self, column_names: &[&str]) -> Constraints {
            get_unique_constraints(column_names, self.schema.clone())
        }

        /// Create a primary key constraint on the specified columns
        pub fn primary_key_on(&self, column_names: &[&str]) -> Constraints {
            get_pk_constraints(column_names, self.schema.clone())
        }
    }

    pub enum Expect {
        Pass,
        Fail,
    }

    /// Helper for testing constraint validation scenarios
    pub struct ConstraintTestCase {
        pub name: String,
        pub batches: Vec<RecordBatch>,
        pub constraints: Constraints,
        pub should_pass: Expect,
        pub expected_error_contains: Option<String>,
    }

    impl ConstraintTestCase {
        /// Create a new test case
        pub fn new(
            name: &str,
            batches: Vec<RecordBatch>,
            constraints: Constraints,
            should_pass: Expect,
        ) -> Self {
            Self {
                name: name.to_string(),
                batches,
                constraints,
                should_pass,
                expected_error_contains: None,
            }
        }

        /// Set expected error message substring
        pub fn with_expected_error(mut self, error_substr: &str) -> Self {
            self.expected_error_contains = Some(error_substr.to_string());
            self
        }

        /// Run the test case
        pub async fn run(self) -> Result<(), anyhow::Error> {
            let result = validate_batch_with_constraints(
                self.batches.clone(),
                &self.constraints,
                &UpsertOptions::default(),
            )
            .await;

            match (self.should_pass, result) {
                (Expect::Pass, Ok(_)) => {
                    println!("✓ Test '{}' passed as expected", self.name);
                    Ok(())
                }
                (Expect::Fail, Err(err)) => {
                    if let Some(expected_substr) = &self.expected_error_contains {
                        let err_str = err.to_string();
                        if err_str.contains(expected_substr) {
                            println!(
                                "✓ Test '{}' failed as expected with error: {}",
                                self.name, err_str
                            );
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!(
                                "Test '{}' failed with unexpected error. Expected substring '{}', got: {}",
                                self.name, expected_substr, err_str
                            ))
                        }
                    } else {
                        println!(
                            "✓ Test '{}' failed as expected with error: {}",
                            self.name, err
                        );
                        Ok(())
                    }
                }
                (Expect::Pass, Err(err)) => Err(anyhow::anyhow!(
                    "Test '{}' was expected to pass but failed with error: {}",
                    self.name,
                    err
                )),
                (Expect::Fail, Ok(_)) => Err(anyhow::anyhow!(
                    "Test '{}' was expected to fail but passed",
                    self.name
                )),
            }
        }
    }

    #[tokio::test]
    async fn test_valid_unique_constraint_no_duplicates() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("id", DataType::Utf8),
            ("name", DataType::Utf8),
            ("category", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice"), Some("A")],
                vec![Some("2"), Some("Bob"), Some("B")],
                vec![Some("3"), Some("Charlie"), Some("A")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "valid unique constraint on id",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_unique_constraint_duplicate_values() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("name", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice")],
                vec![Some("1"), Some("Bob")],
                vec![Some("2"), Some("Charlie")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "invalid unique constraint - duplicate ids",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_valid_composite_unique_constraint() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("user_id", DataType::Utf8),
            ("product_id", DataType::Utf8),
            ("rating", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("A"), Some("5")],
                vec![Some("1"), Some("B"), Some("4")],
                vec![Some("2"), Some("A"), Some("3")],
                vec![Some("2"), Some("B"), Some("5")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["user_id", "product_id"]);

        ConstraintTestCase::new(
            "valid composite unique constraint",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_composite_unique_constraint() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("user_id", DataType::Utf8),
            ("product_id", DataType::Utf8),
        ]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("A")],
                vec![Some("1"), Some("B")],
                vec![Some("1"), Some("A")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["user_id", "product_id"]);

        ConstraintTestCase::new(
            "invalid composite unique constraint",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): user_id, product_id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_valid_primary_key_constraint() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("pk", DataType::Utf8), ("data", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("pk1"), Some("data1")],
                vec![Some("pk2"), Some("data2")],
                vec![Some("pk3"), Some("data3")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.primary_key_on(&["pk"]);

        ConstraintTestCase::new(
            "valid primary key constraint",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_invalid_primary_key_constraint() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("pk", DataType::Utf8), ("data", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("pk1"), Some("data1")],
                vec![Some("pk1"), Some("data2")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.primary_key_on(&["pk"]);

        ConstraintTestCase::new(
            "invalid primary key constraint",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): pk")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_multiple_batches_with_valid_constraints() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![vec![Some("1"), Some("A")], vec![Some("2"), Some("B")]])?
            .add_string_batch(vec![vec![Some("3"), Some("C")], vec![Some("4"), Some("D")]])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "multiple batches with valid constraints",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_multiple_batches_with_cross_batch_violations() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        let batches = data_builder
            .add_string_batch(vec![vec![Some("1"), Some("A")], vec![Some("2"), Some("B")]])?
            .add_string_batch(vec![vec![Some("1"), Some("C")], vec![Some("4"), Some("D")]])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "multiple batches with cross-batch violations",
            batches,
            constraints,
            Expect::Fail,
        )
        .with_expected_error("violates uniqueness constraint on column(s): id")
        .run()
        .await
    }

    #[tokio::test]
    async fn test_integer_data_with_valid_constraints() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("int_id", DataType::Int32),
            ("value", DataType::Int32),
        ]);

        let batches = data_builder
            .add_int_batch(vec![
                vec![Some(1), Some(100)],
                vec![Some(2), Some(200)],
                vec![Some(3), Some(300)],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["int_id"]);

        ConstraintTestCase::new(
            "integer data with valid constraints",
            batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_empty_batch_handling() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[("id", DataType::Utf8)]);
        let empty_batches = data_builder.build();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let constraint_builder = ConstraintBuilder::new(schema);
        let constraints = constraint_builder.unique_on(&["id"]);

        ConstraintTestCase::new(
            "empty batches should pass",
            empty_batches,
            constraints,
            Expect::Pass,
        )
        .run()
        .await
    }

    #[tokio::test]
    async fn test_duplicate_removal_resolves_primary_key_conflicts() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("name", DataType::Utf8)]);

        // Create batches with duplicate rows that would violate primary key constraint
        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice")],
                vec![Some("1"), Some("Alice")], // Exact duplicate
                vec![Some("2"), Some("Bob")],
                vec![Some("2"), Some("Bob")], // Another exact duplicate
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.primary_key_on(&["id"]);

        // Without duplicate removal, this should fail
        let result = validate_batch_with_constraints(
            batches.clone(),
            &constraints,
            &UpsertOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "Expected validation to fail with duplicates"
        );

        // With duplicate removal, this should pass
        let options = UpsertOptions::new().with_remove_duplicates(true);
        let result = validate_batch_with_constraints(batches, &constraints, &options).await;
        assert!(
            result.is_ok(),
            "Expected validation to pass after removing duplicates: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_removal_with_partial_duplicates() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("id", DataType::Utf8),
            ("name", DataType::Utf8),
            ("category", DataType::Utf8),
        ]);

        // Create batches where some rows are duplicates and others are unique
        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice"), Some("A")],
                vec![Some("1"), Some("Alice"), Some("A")], // Exact duplicate
                vec![Some("2"), Some("Bob"), Some("B")],   // Unique
                vec![Some("3"), Some("Charlie"), Some("C")], // Unique
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        // With duplicate removal, this should pass
        let options = UpsertOptions::new().with_remove_duplicates(true);
        let result = validate_batch_with_constraints(batches, &constraints, &options).await;
        assert!(
            result.is_ok(),
            "Expected validation to pass after removing duplicates: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_removal_across_multiple_batches() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        // Create multiple batches where duplicates occur across batches
        let batches = data_builder
            .add_string_batch(vec![vec![Some("1"), Some("A")], vec![Some("2"), Some("B")]])?
            .add_string_batch(vec![
                vec![Some("1"), Some("A")], // Duplicate from first batch
                vec![Some("3"), Some("C")],
            ])?
            .add_string_batch(vec![
                vec![Some("2"), Some("B")], // Duplicate from first batch
                vec![Some("4"), Some("D")],
            ])?
            .build();

        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        // Without duplicate removal, this should fail due to cross-batch duplicates
        let result = validate_batch_with_constraints(
            batches.clone(),
            &constraints,
            &UpsertOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "Expected validation to fail with cross-batch duplicates"
        );

        // With duplicate removal, this should pass
        let options = UpsertOptions::new().with_remove_duplicates(true);
        let result = validate_batch_with_constraints(batches, &constraints, &options).await;
        assert!(
            result.is_ok(),
            "Expected validation to pass after removing cross-batch duplicates: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_last_write_wins_basic_behavior() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("value", DataType::Utf8)]);

        // Create batches with duplicate keys but different values - last one should win
        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("first")],
                vec![Some("2"), Some("A")],
            ])?
            .add_string_batch(vec![
                vec![Some("1"), Some("second")], // Should override first value
                vec![Some("3"), Some("B")],
            ])?
            .add_string_batch(vec![
                vec![Some("1"), Some("third")], // Should be the final value
                vec![Some("4"), Some("C")],
            ])?
            .build();

        let original_schema = batches[0].schema();
        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        // Without last write wins, this should fail due to duplicate keys
        let result = validate_batch_with_constraints(
            batches.clone(),
            &constraints,
            &UpsertOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "Expected validation to fail with duplicate keys"
        );

        // With last write wins, this should pass and keep the last value
        let options = UpsertOptions::new().with_last_write_wins(true);
        let result_batches =
            validate_batch_with_constraints(batches, &constraints, &options).await?;

        // Verify schema matches
        assert_eq!(
            result_batches[0].schema(),
            original_schema,
            "Output schema should match input schema"
        );

        // Verify expected row count (should be 4: one for each unique id)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Expected 4 rows after deduplication");

        // Use DataFusion to verify the data
        let ctx = SessionContext::new();
        let df = ctx.read_batches(result_batches)?;

        // Verify that id "1" has value "third"
        let result = df
            .clone()
            .filter(col("id").eq(lit("1")))?
            .select(vec![col("value")])?
            .collect()
            .await?;

        assert_eq!(result.len(), 1, "Should have exactly one batch");
        assert_eq!(
            result[0].num_rows(),
            1,
            "Should have exactly one row for id '1'"
        );

        let value_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to cast value column");
        assert_eq!(
            value_array.value(0),
            "third",
            "Expected last value 'third' for id '1'"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_last_write_wins_within_single_batch() -> Result<(), anyhow::Error> {
        let data_builder =
            TestDataBuilder::with_columns(&[("id", DataType::Utf8), ("name", DataType::Utf8)]);

        // Create batch with duplicates within the same batch
        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("Alice")],
                vec![Some("2"), Some("Bob")],
                vec![Some("1"), Some("Alice_Updated")], // Should win over first Alice
                vec![Some("3"), Some("Charlie")],
            ])?
            .build();

        let original_schema = batches[0].schema();
        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["id"]);

        // With last write wins, this should pass and keep the updated name
        let options = UpsertOptions::new().with_last_write_wins(true);
        let result_batches =
            validate_batch_with_constraints(batches, &constraints, &options).await?;

        // Verify schema matches
        assert_eq!(
            result_batches[0].schema(),
            original_schema,
            "Output schema should match input schema"
        );

        // Verify expected row count (should be 3: one for each unique id)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Expected 3 rows after deduplication");

        // Use DataFusion to verify the data
        let ctx = SessionContext::new();
        let df = ctx.read_batches(result_batches)?;

        // Verify that id "1" has the updated name
        let result = df
            .clone()
            .filter(col("id").eq(lit("1")))?
            .select(vec![col("name")])?
            .collect()
            .await?;

        assert_eq!(result.len(), 1, "Should have exactly one batch");
        assert_eq!(
            result[0].num_rows(),
            1,
            "Should have exactly one row for id '1'"
        );

        let name_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to cast name column");
        assert_eq!(
            name_array.value(0),
            "Alice_Updated",
            "Expected updated name 'Alice_Updated' for id '1'"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_last_write_wins_with_composite_keys() -> Result<(), anyhow::Error> {
        let data_builder = TestDataBuilder::with_columns(&[
            ("user_id", DataType::Utf8),
            ("product_id", DataType::Utf8),
            ("rating", DataType::Utf8),
        ]);

        // Create data with composite key conflicts
        let batches = data_builder
            .add_string_batch(vec![
                vec![Some("1"), Some("A"), Some("3")], // First rating for user 1, product A
                vec![Some("2"), Some("B"), Some("4")],
            ])?
            .add_string_batch(vec![
                vec![Some("1"), Some("A"), Some("5")], // Updated rating (should win)
                vec![Some("3"), Some("C"), Some("2")],
            ])?
            .build();

        let original_schema = batches[0].schema();
        let constraint_builder = ConstraintBuilder::new(batches[0].schema());
        let constraints = constraint_builder.unique_on(&["user_id", "product_id"]);

        // Without last write wins, this should fail
        let result = validate_batch_with_constraints(
            batches.clone(),
            &constraints,
            &UpsertOptions::default(),
        )
        .await;
        assert!(
            result.is_err(),
            "Expected validation to fail with composite key duplicates"
        );

        // With last write wins, this should pass and keep the updated rating
        let options = UpsertOptions::new().with_last_write_wins(true);
        let result_batches =
            validate_batch_with_constraints(batches, &constraints, &options).await?;

        // Verify schema matches
        assert_eq!(
            result_batches[0].schema(),
            original_schema,
            "Output schema should match input schema"
        );

        // Verify expected row count (should be 3: one for each unique combination)
        let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Expected 3 rows after deduplication");

        // Use DataFusion to verify the data
        let ctx = SessionContext::new();
        let df = ctx.read_batches(result_batches)?;

        // Verify that user "1", product "A" has rating "5"
        let result = df
            .clone()
            .filter(
                col("user_id")
                    .eq(lit("1"))
                    .and(col("product_id").eq(lit("A"))),
            )?
            .select(vec![col("rating")])?
            .collect()
            .await?;

        assert_eq!(result.len(), 1, "Should have exactly one batch");
        assert_eq!(
            result[0].num_rows(),
            1,
            "Should have exactly one row for user '1', product 'A'"
        );

        let rating_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Failed to cast rating column");
        assert_eq!(
            rating_array.value(0),
            "5",
            "Expected updated rating '5' for user '1', product 'A'"
        );

        Ok(())
    }

    #[test]
    fn test_upsert_options_try_from_str_empty() {
        let result = UpsertOptions::try_from("");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(!options.remove_duplicates);
        assert!(!options.last_write_wins);
        assert!(options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_remove_duplicates() {
        let result = UpsertOptions::try_from("remove_duplicates");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(options.remove_duplicates);
        assert!(!options.last_write_wins);
        assert!(!options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_last_write_wins() {
        let result = UpsertOptions::try_from("last_write_wins");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(!options.remove_duplicates);
        assert!(options.last_write_wins);
        assert!(!options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_both_options() {
        let result = UpsertOptions::try_from("remove_duplicates,last_write_wins");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(options.remove_duplicates);
        assert!(options.last_write_wins);
        assert!(!options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_both_options_reverse_order() {
        let result = UpsertOptions::try_from("last_write_wins,remove_duplicates");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(options.remove_duplicates);
        assert!(options.last_write_wins);
        assert!(!options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_with_spaces() {
        let result = UpsertOptions::try_from(" remove_duplicates , last_write_wins ");
        assert!(result.is_ok());
        let options = result.unwrap();
        assert!(options.remove_duplicates);
        assert!(options.last_write_wins);
        assert!(!options.is_default());
    }

    #[test]
    fn test_upsert_options_try_from_str_invalid_option() {
        let result = UpsertOptions::try_from("invalid_option");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Unknown upsert option: invalid_option"));
    }

    #[test]
    fn test_upsert_options_try_from_str_mixed_valid_invalid() {
        let result = UpsertOptions::try_from("remove_duplicates,invalid_option");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Unknown upsert option: invalid_option"));
    }

    #[test]
    fn test_upsert_options_try_from_str_multiple_invalid_options() {
        let result = UpsertOptions::try_from("invalid1,invalid2");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Unknown upsert option: invalid1"));
    }

    #[test]
    fn test_upsert_options_try_from_str_case_sensitive() {
        let result = UpsertOptions::try_from("Remove_Duplicates");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains("Unknown upsert option: Remove_Duplicates"));
    }
}
