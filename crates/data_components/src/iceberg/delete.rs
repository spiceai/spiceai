/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Iceberg delete support via equality delete files.
//!
//! This module implements `DELETE FROM` for Iceberg tables by writing equality
//! delete files and committing them via the `RowDeltaAction` transaction.
//!
//! The approach:
//! 1. Compute equality-eligible columns (primitive, non-float types)
//! 2. Scan the table with WHERE filters, projected to only equality columns
//! 3. Write the matching rows as equality delete Parquet files
//! 4. Commit the delete files via `RowDeltaAction`
//!
//! This uses Iceberg's merge-on-read strategy: the delete files are separate
//! from data files, and the Iceberg reader filters them out at read time.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use spice_table::{LayerWalk, TableLayer};

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DFResult, ToDFSchema, tree_node::TreeNode};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::arrow::FieldMatchMode;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, TableProperties};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Error as IcebergError};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

/// The columns an equality delete keys on, as `(field IDs, column indices)` in
/// schema order.
///
/// Iceberg equality deletes match rows *by value*, so a column whose equality is
/// not well defined cannot take part. Floating point is excluded (`NaN` is not
/// equal to itself and `-0.0 == 0.0`), nested types are excluded, and so is any
/// column without a Parquet field ID — the reader resolves delete columns by ID,
/// never by name.
///
/// The delete therefore keys on a *subset* of the row. Two rows agreeing on
/// every returned column are indistinguishable to the delete file, so a `DELETE`
/// whose predicate separates them only by an excluded column removes both. An
/// empty result means no column is eligible at all; the caller must refuse the
/// statement rather than write a delete file that matches the whole table.
fn equality_delete_columns(schema: &ArrowSchema) -> (Vec<i32>, Vec<usize>) {
    let mut equality_ids: Vec<i32> = Vec::new();
    let mut projection_indices: Vec<usize> = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if field.data_type().is_nested()
            || matches!(
                field.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            )
        {
            continue;
        }
        if let Some(field_id) = field
            .metadata()
            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
            .and_then(|v| v.parse::<i32>().ok())
        {
            equality_ids.push(field_id);
            projection_indices.push(idx);
        }
    }

    (equality_ids, projection_indices)
}

/// The columns a predicate references that the equality key cannot carry, sorted
/// and deduplicated.
///
/// An equality delete says "remove rows whose key columns equal these values".
/// That is only the same statement as the original `WHERE` when the predicate is
/// a function of the key columns alone: a row sharing the key values of a matched
/// row then necessarily satisfies the predicate too, so nothing unmatched is
/// removed. The moment the predicate reads a column outside the key — a float, a
/// nested field, anything [`equality_delete_columns`] excludes — the delete file
/// can no longer distinguish a matched row from an unmatched one beside it, and
/// deletes both.
///
/// Sorted so the refusal names the same column every time; `column_refs` returns
/// an unordered set.
fn unkeyable_predicate_columns(
    schema: &ArrowSchema,
    key_indices: &[usize],
    filters: &[datafusion::logical_expr::Expr],
) -> Vec<String> {
    let keyable: std::collections::HashSet<&str> = key_indices
        .iter()
        .map(|idx| schema.field(*idx).name().as_str())
        .collect();

    let mut unkeyable: Vec<String> = filters
        .iter()
        .flat_map(|filter| {
            filter
                .column_refs()
                .into_iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        })
        .filter(|name| !keyable.contains(name.as_str()))
        .collect();
    unkeyable.sort_unstable();
    unkeyable.dedup();
    unkeyable
}

/// The refusal a caller gets when the condition cannot be expressed as an
/// equality key. Built here rather than inline so its wording is pinned by a
/// test: it has to keep naming the table, the offending column, and what the
/// user can do instead.
fn unkeyable_predicate_message(table: &str, unkeyable: &[String], keyable: &[String]) -> String {
    let quoted = |names: &[String]| {
        names
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let usable = if keyable.is_empty() {
        "no column of this table can be matched on".to_string()
    } else {
        format!("this table can only be matched on {}", quoted(keyable))
    };

    format!(
        "Failed to delete from Iceberg table '{table}': the condition reads {}, which an equality delete cannot match on, so rows the condition did not select would be deleted too. \
        Rewrite the condition to use only columns the delete can match, or delete the rows through a source that supports row-level deletes; {usable}. \
        Floating-point and nested columns can never be matched on, and neither can a column without a Parquet field ID. \
        See: https://spiceai.org/docs/components/data-connectors/iceberg",
        quoted(unkeyable)
    )
}

fn to_df_error(e: IcebergError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Execution plan that scans matching rows, writes equality delete files,
/// and commits them via `RowDeltaAction`.
///
/// Output schema: single column `count` (`UInt64`) with the number of deleted rows.
pub(crate) struct IcebergDeleteExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    /// The child plan that produces the rows to delete (a scan with filters applied).
    /// The scan is projected to only include columns eligible for equality deletes.
    input: Arc<dyn ExecutionPlan>,
    /// Pre-computed equality delete field IDs (primitive, non-float columns).
    equality_ids: Vec<i32>,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergDeleteExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        equality_ids: Vec<i32>,
    ) -> Self {
        let count_schema = Self::make_count_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&count_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            table,
            catalog,
            input,
            equality_ids,
            plan_properties,
        }
    }

    fn make_count_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }

    fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
        let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
        RecordBatch::try_from_iter_with_nullable(vec![("count", count_array, false)])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Debug for IcebergDeleteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergDeleteExec")
            .field("table", &self.table.identifier().to_string())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for IcebergDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IcebergDeleteExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergDeleteExec {
    fn name(&self) -> &'static str {
        "IcebergDeleteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(IcebergDeleteExec::new(
            self.table.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&children[0]),
            self.equality_ids.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec only supports partition 0, got {partition}"
            )));
        }

        let table = self.table.clone();
        let catalog = Arc::clone(&self.catalog);
        let input_plan = Arc::clone(&self.input);
        let count_schema = Self::make_count_schema();
        let equality_ids = self.equality_ids.clone();

        let stream = futures::stream::once(async move {
            // Collect all input partitions into a single stream
            let partition_count = input_plan
                .properties()
                .output_partitioning()
                .partition_count();
            let mut total_delete_count: u64 = 0;

            // Get the iceberg schema for the equality delete writer
            let iceberg_schema = Arc::clone(table.metadata().current_schema());

            tracing::debug!(
                table = %table.identifier(),
                equality_id_count = equality_ids.len(),
                ?equality_ids,
                "Writing equality delete files"
            );

            // Set up the equality delete writer
            let file_io = table.file_io().clone();
            let location_generator =
                DefaultLocationGenerator::new(table.metadata()).map_err(to_df_error)?;
            let file_name_generator = DefaultFileNameGenerator::new(
                Uuid::now_v7().to_string(),
                Some("eq-del".to_string()),
                DataFileFormat::Parquet,
            );

            let target_file_size = table
                .metadata()
                .properties()
                .get(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES)
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

            // Build a sub-schema containing only the equality-eligible
            // fields. The input scan is already projected to these columns,
            // so the `EqualityDeleteWriterConfig` projector must map from
            // this sub-schema (not the full table schema) to avoid index
            // out-of-bounds when re-projecting the already-projected batches.
            let equality_id_set: std::collections::HashSet<i32> =
                equality_ids.iter().copied().collect();
            let equality_fields: Vec<_> = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|f| equality_id_set.contains(&f.id))
                .cloned()
                .collect();
            let equality_schema = Arc::new(
                iceberg::spec::Schema::builder()
                    .with_schema_id(iceberg_schema.schema_id())
                    .with_fields(equality_fields)
                    .build()
                    .map_err(to_df_error)?,
            );

            let parquet_writer_builder = ParquetWriterBuilder::new_with_match_mode(
                WriterProperties::default(),
                Arc::clone(&equality_schema),
                FieldMatchMode::Name,
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new(
                parquet_writer_builder,
                target_file_size,
                file_io,
                location_generator,
                file_name_generator,
            );

            let config = EqualityDeleteWriterConfig::new(equality_ids.clone(), equality_schema)
                .map_err(to_df_error)?;

            let writer_builder =
                EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config);

            let mut writer = writer_builder.build(None).await.map_err(to_df_error)?;

            // Read from all partitions and write equality delete files.
            // The input scan is already projected to only include the equality
            // columns, so no additional projection is needed.
            for p in 0..partition_count {
                let mut batch_stream = input_plan.execute(p, Arc::clone(&context))?;
                while let Some(batch_result) = batch_stream.next().await {
                    let batch = batch_result?;
                    if batch.num_rows() == 0 {
                        continue;
                    }

                    let batch_rows = u64::try_from(batch.num_rows()).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "Batch row count {} exceeds u64 range",
                            batch.num_rows()
                        ))
                    })?;
                    total_delete_count =
                        total_delete_count.checked_add(batch_rows).ok_or_else(|| {
                            DataFusionError::Internal(
                                "Total delete row count overflowed u64".to_string(),
                            )
                        })?;
                    writer.write(batch).await.map_err(to_df_error)?;
                }
            }

            // If no rows matched, return count=0
            if total_delete_count == 0 {
                return Self::make_count_batch(0);
            }

            // Close the writer to get the delete files
            let delete_files = writer.close().await.map_err(to_df_error)?;

            if delete_files.is_empty() {
                return Self::make_count_batch(0);
            }

            // Commit via RowDeltaAction
            let tx = Transaction::new(&table);
            let action = tx.row_delta().add_delete_files(delete_files);

            action
                .apply(tx)
                .map_err(to_df_error)?
                .commit(catalog.as_ref())
                .await
                .map_err(to_df_error)?;

            Self::make_count_batch(total_delete_count)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

/// Wrapper that makes an `IcebergTableProvider` support `DELETE FROM`.
///
/// This is registered as the table provider when the Iceberg data connector
/// supports writes, enabling `DELETE FROM` SQL statements.
pub struct IcebergDeletionProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    inner: Arc<dyn datafusion::datasource::TableProvider>,
}

impl IcebergDeletionProvider {
    /// Create a new deletion-capable wrapper around an `IcebergTableProvider`.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: iceberg::NamespaceIdent,
        table_name: String,
        inner: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Self {
        let table_ident = iceberg::TableIdent::new(namespace, table_name);
        Self {
            catalog,
            table_ident,
            inner,
        }
    }

    /// The wrapped provider (the underlying `IcebergTableProvider`). Exposed so
    /// wrapper-peeling helpers can see through this layer to the concrete
    /// Iceberg provider.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn datafusion::datasource::TableProvider> {
        &self.inner
    }
}

impl Debug for IcebergDeletionProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergDeletionProvider")
            .field("table_ident", &self.table_ident.to_string())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableLayer for IcebergDeletionProvider {
    /// Deletion against an Iceberg table is this layer's own behaviour, so only a
    /// read walk may see past it — anything that would route a delete around it
    /// (retention especially) must stop here or the delete lands on the wrong
    /// table.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Option<&'a Arc<dyn datafusion::datasource::TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind
        // for this layer without anyone deciding what it should say.
        match walk {
            // Deletion adds no columns and carries no index of its own, so read
            // discovery and index discovery both reach past it.
            LayerWalk::Read | LayerWalk::Index => Some(below),
            // Everything else stops: a delete routed around this layer would run
            // against the Iceberg table without its deletion semantics, and a
            // source or CDC walk has no business below an Iceberg delete.
            LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::Write
            | LayerWalk::RetentionDelete => None,
        }
    }

    async fn delete_from(
        &self,
        _below: &Arc<dyn datafusion::datasource::TableProvider>,
        state: &dyn Session,
        filters: Vec<datafusion::logical_expr::Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.delete_from_impl(state, &filters).await
    }
}

impl IcebergDeletionProvider {
    async fn delete_from_impl(
        &self,
        state: &dyn Session,
        filters: &[datafusion::logical_expr::Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_df_error)?;

        // Verify format version supports deletes
        if table.metadata().format_version() == iceberg::spec::FormatVersion::V1 {
            return Err(DataFusionError::Plan(
                "DELETE is not supported on Iceberg v1 tables. Upgrade to v2 format.".to_string(),
            ));
        }

        // Derive the equality_ids and scan projection up front. Only primitive
        // non-floating-point columns are eligible for equality deletes per the
        // Iceberg spec. We compute them here so the scan can be projected to
        // only those columns, avoiding reads of float/nested columns that the
        // Iceberg reader cannot resolve by field-id.
        let iceberg_schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(iceberg_schema).map_err(to_df_error)?);

        let (equality_ids, projection_indices) = equality_delete_columns(&arrow_schema);

        // An equality delete file with no key columns imposes no condition, so
        // it would match every row: a `DELETE ... WHERE` would empty the table.
        // Refuse the statement rather than destroy data.
        if equality_ids.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Failed to delete from Iceberg table {}: no column can identify the rows to delete. \
                Equality deletes cannot key on floating-point or nested columns, and every other column must carry a Parquet field ID. \
                Add an integer, string, boolean, date, timestamp, or decimal column to the table. \
                See: https://spiceai.org/docs/components/data-connectors/iceberg",
                self.table_ident
            )));
        }

        // An equality delete can only reproduce a condition built from the columns
        // it keys on. Anything else would delete more than the condition selected,
        // so refuse rather than approximate — losing rows silently is worse than
        // refusing the statement.
        let unkeyable = unkeyable_predicate_columns(&arrow_schema, &projection_indices, filters);
        if !unkeyable.is_empty() {
            let keyable: Vec<String> = projection_indices
                .iter()
                .map(|idx| arrow_schema.field(*idx).name().clone())
                .collect();
            return Err(DataFusionError::Plan(unkeyable_predicate_message(
                &self.table_ident.to_string(),
                &unkeyable,
                &keyable,
            )));
        }

        tracing::debug!(
            table = %table.identifier(),
            schema_field_count = arrow_schema.fields().len(),
            equality_id_count = equality_ids.len(),
            ?equality_ids,
            "Computed equality delete IDs for scan projection"
        );

        // Scan only the equality-eligible columns. This avoids reading
        // float/nested columns that cause field-id resolution errors.
        let scan_plan = self
            .inner
            .scan(state, Some(&projection_indices), filters, None)
            .await?;

        // The Iceberg provider may not push down filters, so add a FilterExec
        // on top of the scan to ensure only matching rows are processed.
        // Filter column references must be unqualified to match the scan
        // output schema (which uses bare column names, not table-qualified).
        let filtered_plan = if filters.is_empty() {
            scan_plan
        } else {
            let scan_schema = scan_plan.schema();
            let df_schema = scan_schema.to_dfschema_ref()?;
            let unqualified_filters: Vec<datafusion::logical_expr::Expr> = filters
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform(|e| {
                            if let datafusion::logical_expr::Expr::Column(mut col) = e {
                                col.relation = None;
                                Ok(datafusion::common::tree_node::Transformed::yes(
                                    datafusion::logical_expr::Expr::Column(col),
                                ))
                            } else {
                                Ok(datafusion::common::tree_node::Transformed::no(e))
                            }
                        })
                        .map(|t| t.data)
                })
                .collect::<DFResult<Vec<_>>>()?;
            let combined_filter = unqualified_filters
                .into_iter()
                .reduce(datafusion::prelude::Expr::and)
                .ok_or_else(|| {
                    DataFusionError::Internal("Filter list unexpectedly empty".to_string())
                })?;
            let physical_filter = datafusion::physical_expr::create_physical_expr(
                &combined_filter,
                &df_schema,
                state.execution_props(),
            )?;
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                physical_filter,
                scan_plan,
            )?) as Arc<dyn ExecutionPlan>
        };

        // Coalesce into a single partition for the delete writer
        let coalesced = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                filtered_plan,
            ),
        );

        Ok(Arc::new(IcebergDeleteExec::new(
            table,
            Arc::clone(&self.catalog),
            coalesced,
            equality_ids,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        equality_delete_columns, unkeyable_predicate_columns, unkeyable_predicate_message,
    };
    use arrow::datatypes::{
        DataType, Field, Field as ArrowField, Fields, Schema as ArrowSchema, SchemaRef, TimeUnit,
    };
    use datafusion::prelude::{col, lit};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn field(name: &str, data_type: DataType, field_id: Option<&str>) -> Field {
        let field = Field::new(name, data_type, true);
        match field_id {
            Some(id) => field.with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )])),
            None => field,
        }
    }

    #[test]
    fn primitive_columns_with_field_ids_are_eligible() {
        let schema = ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("name", DataType::Utf8, Some("2")),
            field("active", DataType::Boolean, Some("3")),
            field("day", DataType::Date32, Some("4")),
            field(
                "seen",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Some("5"),
            ),
            field("amount", DataType::Decimal128(10, 2), Some("6")),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5]);
    }

    /// Floating point has no usable equality (`NaN != NaN`, `-0.0 == 0.0`), so
    /// the Iceberg spec forbids keying a delete on it.
    #[test]
    fn floating_point_columns_are_excluded() {
        let schema = ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("f16", DataType::Float16, Some("2")),
            field("f32", DataType::Float32, Some("3")),
            field("f64", DataType::Float64, Some("4")),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert_eq!(ids, vec![1]);
        assert_eq!(indices, vec![0]);
    }

    #[test]
    fn nested_columns_are_excluded() {
        let inner = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("tags", DataType::List(Arc::clone(&inner)), Some("2")),
            field(
                "meta",
                DataType::Struct(Fields::from(vec![Field::new("k", DataType::Utf8, true)])),
                Some("3"),
            ),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert_eq!(ids, vec![1]);
        assert_eq!(indices, vec![0]);
    }

    /// The Iceberg reader resolves delete columns by field ID, never by name, so
    /// a column without one cannot take part.
    #[test]
    fn columns_without_a_usable_field_id_are_excluded() {
        let schema = ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("no_id", DataType::Utf8, None),
            field("bad_id", DataType::Utf8, Some("not-a-number")),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert_eq!(ids, vec![1]);
        assert_eq!(indices, vec![0]);
    }

    /// The returned IDs and indices are positionally paired: index `n` of the
    /// scan projection carries the column whose field ID is `ids[n]`. A drift
    /// between them would write each row's values under the wrong key.
    #[test]
    fn ids_and_projection_indices_stay_paired_across_skipped_columns() {
        let schema = ArrowSchema::new(vec![
            field("skip_me", DataType::Float64, Some("10")),
            field("id", DataType::Int64, Some("11")),
            field("also_skip", DataType::Utf8, None),
            field("name", DataType::Utf8, Some("13")),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert_eq!(ids, vec![11, 13]);
        assert_eq!(indices, vec![1, 3]);
        for (id, idx) in ids.iter().zip(indices.iter()) {
            let meta = schema.field(*idx).metadata();
            let declared = meta
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .expect("eligible columns carry a field ID");
            assert_eq!(declared, &id.to_string());
        }
    }

    /// The case the caller must refuse: nothing is eligible, so a delete file
    /// built from this schema would carry no key columns and match every row.
    #[test]
    fn a_table_with_no_eligible_column_yields_an_empty_key() {
        let schema = ArrowSchema::new(vec![
            field("x", DataType::Float64, Some("1")),
            field("y", DataType::Float32, Some("2")),
            field("z", DataType::Utf8, None),
        ]);

        let (ids, indices) = equality_delete_columns(&schema);
        assert!(ids.is_empty());
        assert!(indices.is_empty());
    }

    /// `(id, label)` repeat across rows; `price` is what tells them apart — and
    /// `price` is a float, so the delete can never key on it.
    fn float_keyed_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("label", DataType::Utf8, Some("2")),
            field("price", DataType::Float64, Some("3")),
        ])
    }

    fn key_indices(schema: &ArrowSchema) -> Vec<usize> {
        equality_delete_columns(schema).1
    }

    /// The case that must be refused. Two rows can share `(id, label)` and differ
    /// only in `price`, so a delete keyed on `(id, label)` removes both — while
    /// `WHERE price = 1.5` selected one. Approximating here loses a row silently
    /// and unrecoverably, so the statement is refused instead.
    #[test]
    fn a_predicate_on_an_unkeyable_column_is_refused() {
        let schema = float_keyed_schema();
        let unkeyable = unkeyable_predicate_columns(
            &schema,
            &key_indices(&schema),
            &[col("price").eq(lit(1.5))],
        );

        assert_eq!(unkeyable, vec!["price".to_string()]);
    }

    /// A predicate built only from key columns is exact: any row sharing those
    /// values satisfies the same predicate, so nothing unmatched is removed.
    #[test]
    fn a_predicate_on_key_columns_only_is_allowed() {
        let schema = float_keyed_schema();
        let indices = key_indices(&schema);

        for filters in [
            vec![col("id").eq(lit(1_i64))],
            vec![col("label").eq(lit("x"))],
            vec![col("id").eq(lit(1_i64)), col("label").eq(lit("x"))],
            vec![col("id").gt(lit(1_i64)).and(col("label").is_not_null())],
        ] {
            assert!(
                unkeyable_predicate_columns(&schema, &indices, &filters).is_empty(),
                "should be allowed: {filters:?}"
            );
        }
    }

    /// `DELETE FROM t` with no condition removes every row, which an equality
    /// delete reproduces exactly. Refusing it would be wrong.
    #[test]
    fn a_delete_with_no_condition_is_allowed() {
        let schema = float_keyed_schema();
        assert!(unkeyable_predicate_columns(&schema, &key_indices(&schema), &[]).is_empty());
    }

    /// A mixed condition is still refused, and every offending column is named
    /// so the user does not have to rediscover them one error at a time.
    #[test]
    fn every_unkeyable_column_is_reported_at_once_and_in_a_stable_order() {
        let schema = ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("price", DataType::Float64, Some("2")),
            field("ratio", DataType::Float32, Some("3")),
        ]);
        let filters = vec![
            col("ratio").gt(lit(0.5_f32)),
            col("id").eq(lit(1_i64)),
            col("price").eq(lit(1.5)),
        ];

        // Sorted, so the message does not vary run to run: `column_refs` is a set.
        assert_eq!(
            unkeyable_predicate_columns(&schema, &key_indices(&schema), &filters),
            vec!["price".to_string(), "ratio".to_string()]
        );
    }

    /// A column named twice is reported once.
    #[test]
    fn a_repeated_unkeyable_column_is_reported_once() {
        let schema = float_keyed_schema();
        let filters = vec![col("price").gt(lit(1.0)), col("price").lt(lit(9.0))];

        assert_eq!(
            unkeyable_predicate_columns(&schema, &key_indices(&schema), &filters),
            vec!["price".to_string()]
        );
    }

    /// The refusal has to stay actionable: name the table, the column that caused
    /// it, what can be used instead, and where to read more. A reword must not
    /// quietly drop any of those.
    #[test]
    fn the_refusal_names_the_table_the_column_and_the_way_out() {
        let message = unkeyable_predicate_message(
            "sales.orders",
            &["price".to_string()],
            &["id".to_string(), "label".to_string()],
        );

        assert!(message.contains("'sales.orders'"), "{message}");
        assert!(message.contains("'price'"), "{message}");
        assert!(message.contains("'id', 'label'"), "{message}");
        assert!(
            message.contains("rows the condition did not select would be deleted too"),
            "must say what goes wrong, not just that it refused: {message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/components/data-connectors/iceberg"),
            "{message}"
        );
        assert!(!message.contains('\n'), "must stay on one line: {message}");
    }

    /// When nothing is keyable the message must not claim an empty list of usable
    /// columns — that reads as "use these" followed by nothing.
    #[test]
    fn the_refusal_says_so_plainly_when_no_column_is_usable() {
        let message = unkeyable_predicate_message("sales.orders", &["price".to_string()], &[]);

        assert!(
            message.contains("no column of this table can be matched on"),
            "{message}"
        );
    }

    // ---------------------------------------------------------------------
    // End-to-end coverage against a real catalog.
    //
    // These commit Iceberg tables through a REST catalog and a local-filesystem
    // warehouse, so they assert on *rows that survived a delete* rather than on
    // the plan that would have run. That distinction is the point: every earlier
    // attempt at this code planned exactly as intended and still removed the
    // wrong rows, and no unit test could see it.
    //
    // Skipped unless ICEBERG_REST_CATALOG_URI names a running catalog, matching
    // how `hadoop_catalog_test.rs` gates on its own endpoint. Bring one up with:
    //
    //   docker run -d -p 8181:8181 -v /tmp/wh:/tmp/wh \
    //     -e CATALOG_WAREHOUSE=file:///tmp/wh \
    //     -e CATALOG_IO__IMPL=org.apache.iceberg.hadoop.HadoopFileIO \
    //     apache/iceberg-rest-fixture:latest
    //
    // The warehouse is bind-mounted at the *same* path inside the container so
    // the absolute locations the catalog records resolve for this process too.
    // ---------------------------------------------------------------------

    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use iceberg::io::LocalFsStorageFactory;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
    use iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };
    use iceberg_datafusion::IcebergTableProvider;

    use super::IcebergDeletionProvider;

    const CATALOG_URI_ENV: &str = "ICEBERG_REST_CATALOG_URI";
    const WAREHOUSE_ENV: &str = "ICEBERG_REST_WAREHOUSE";

    /// The catalog endpoint and warehouse root, or `None` when the environment
    /// does not offer one and the test should not run.
    fn catalog_env() -> Option<(String, String)> {
        let uri = std::env::var(CATALOG_URI_ENV).ok()?;
        let warehouse = std::env::var(WAREHOUSE_ENV)
            .unwrap_or_else(|_| "file:///tmp/iceberg-e2e-warehouse".to_string());
        (!uri.trim().is_empty()).then_some((uri, warehouse))
    }

    /// A fresh table per test. The name carries the caller so concurrently
    /// running tests never share a table, and a stale one from an earlier run is
    /// dropped rather than reused.
    async fn fresh_table(name: &str) -> (Arc<dyn Catalog>, NamespaceIdent, String) {
        let (uri, warehouse) = catalog_env().expect("checked by the caller");

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "rest",
                HashMap::from([
                    (REST_CATALOG_PROP_URI.to_string(), uri),
                    (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse),
                ]),
            )
            .await
            .expect("load REST catalog");

        let namespace = NamespaceIdent::new("delete_e2e".to_string());
        // Ignore the already-exists error; the namespace outlives a single test.
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;

        let table_ident = iceberg::TableIdent::new(namespace.clone(), name.to_string());
        let _ = catalog.drop_table(&table_ident).await;

        let schema = IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "label", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "price", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()
            .expect("iceberg schema");

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(schema)
                    // Equality deletes need v2; v1 is refused before we get here.
                    .properties(HashMap::from([(
                        "format-version".to_string(),
                        "2".to_string(),
                    )]))
                    .build(),
            )
            .await
            .expect("create table");

        (Arc::new(catalog), namespace, name.to_string())
    }

    /// Two rows share `(id, label)` and differ only in `price` — the pair an
    /// equality delete keyed on `(id, label)` cannot tell apart.
    fn seed_batch() -> arrow::array::RecordBatch {
        let schema: SchemaRef = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("label", DataType::Utf8, true),
            ArrowField::new("price", DataType::Float64, true),
        ]));
        arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1_i64, 1, 2])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5, 9.0])),
            ],
        )
        .expect("seed batch")
    }

    async fn table_provider(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        name: &str,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            IcebergTableProvider::try_new(Arc::clone(catalog), namespace.clone(), name.to_string())
                .await
                .expect("iceberg table provider"),
        )
    }

    /// Every `(id, price)` currently in the table, sorted so the assertion does
    /// not depend on scan order.
    async fn rows_now(
        ctx: &SessionContext,
        provider: &Arc<dyn TableProvider>,
    ) -> Vec<(i64, String)> {
        let plan = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        let mut rows = Vec::new();
        for batch in batches {
            let schema = batch.schema();
            let ids = batch
                .column(schema.index_of("id").expect("id"))
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id is Int64");
            let prices = batch
                .column(schema.index_of("price").expect("price"))
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("price is Float64");
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), format!("{:.1}", prices.value(row))));
            }
        }
        rows.sort();
        rows
    }

    async fn seed(ctx: &SessionContext, provider: &Arc<dyn TableProvider>) {
        let batch = seed_batch();
        let source = MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None)
            .expect("memory source");
        let plan = provider
            .insert_into(
                &ctx.state(),
                Arc::new(DataSourceExec::new(Arc::new(source))),
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect("insert plan");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("insert runs");
    }

    fn deletable(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        name: &str,
        inner: &Arc<dyn TableProvider>,
    ) -> Arc<dyn TableProvider> {
        let layer = IcebergDeletionProvider::new(
            Arc::clone(catalog),
            namespace.clone(),
            name.to_string(),
            Arc::clone(inner),
        );
        spice_table::SpiceTable::over(Arc::new(layer), Arc::clone(inner))
    }

    fn all_three_rows() -> Vec<(i64, String)> {
        vec![
            (1, "1.5".to_string()),
            (1, "2.5".to_string()),
            (2, "9.0".to_string()),
        ]
    }

    /// A condition the key *can* express removes exactly the rows it selected.
    #[tokio::test]
    async fn a_delete_on_a_keyable_column_removes_exactly_those_rows() {
        let Some(_) = catalog_env() else {
            eprintln!("skipping: {CATALOG_URI_ENV} is not set");
            return;
        };
        let (catalog, namespace, name) = fresh_table("keyable").await;
        let ctx = SessionContext::new();
        let inner = table_provider(&catalog, &namespace, &name).await;
        seed(&ctx, &inner).await;
        assert_eq!(rows_now(&ctx, &inner).await, all_three_rows());

        let plan = deletable(&catalog, &namespace, &name, &inner)
            .delete_from(&ctx.state(), vec![col("id").eq(lit(1_i64))])
            .await
            .expect("delete plans");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("delete runs");

        let after = table_provider(&catalog, &namespace, &name).await;
        assert_eq!(
            rows_now(&ctx, &after).await,
            vec![(2, "9.0".to_string())],
            "both id = 1 rows go, and only those"
        );
    }

    /// The reason this guard exists. `price` cannot be part of the key, so
    /// `WHERE price = 1.5` has no faithful equality delete — the closest one also
    /// removes the `price = 2.5` row. It must be refused, and the table must be
    /// untouched afterwards. This is the assertion no unit test could make.
    #[tokio::test]
    async fn a_delete_on_an_unkeyable_column_is_refused_and_loses_no_rows() {
        let Some(_) = catalog_env() else {
            eprintln!("skipping: {CATALOG_URI_ENV} is not set");
            return;
        };
        let (catalog, namespace, name) = fresh_table("unkeyable").await;
        let ctx = SessionContext::new();
        let inner = table_provider(&catalog, &namespace, &name).await;
        seed(&ctx, &inner).await;

        let error = deletable(&catalog, &namespace, &name, &inner)
            .delete_from(&ctx.state(), vec![col("price").eq(lit(1.5_f64))])
            .await
            .expect_err("a condition on an unkeyable column must be refused");

        let message = error.to_string();
        assert!(message.contains("'price'"), "{message}");
        assert!(
            message.contains("rows the condition did not select would be deleted too"),
            "{message}"
        );

        let after = table_provider(&catalog, &namespace, &name).await;
        assert_eq!(
            rows_now(&ctx, &after).await,
            all_three_rows(),
            "a refused delete must leave every row in place"
        );
    }

    #[test]
    fn an_empty_schema_yields_an_empty_key() {
        let (ids, indices) = equality_delete_columns(&ArrowSchema::empty());
        assert!(ids.is_empty());
        assert!(indices.is_empty());
    }
}
