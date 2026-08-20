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

/// The two projections an equality delete needs, which are *not* the same list.
///
/// `equality_ids` is the delete file's key — the columns whose values identify
/// the rows to remove. `scan_indices` is what the scan must actually read: every
/// key column, plus every column the predicate mentions.
///
/// Conflating them breaks the statement in one direction or the other. Reading
/// only the key columns leaves a predicate on an excluded column — a float, say —
/// with nothing to resolve against, so `DELETE ... WHERE price = 1.5` cannot be
/// planned. Keying on everything the scan read is what makes the delete match
/// more rows than the predicate did.
#[derive(Debug, PartialEq, Eq)]
struct EqualityDeletePlan {
    /// Field IDs the delete file keys on, in schema order.
    equality_ids: Vec<i32>,
    /// Column indices the scan projects, ascending.
    scan_indices: Vec<usize>,
}

/// Choose the delete key and the scan projection for a `DELETE`.
///
/// The key is the table's declared identifier fields when it has them and all of
/// them can take part in an equality delete: those fields *are* the row identity,
/// so keying on them removes exactly the rows the predicate matched. Without a
/// usable identity there is no better answer than every eligible column, which
/// narrows the match as far as the schema allows — see [`equality_delete_columns`]
/// for why that is still a subset of the row, and what that costs.
///
/// The scan is widened to cover the predicate's columns regardless, so the filter
/// always has something to bind to.
fn plan_equality_delete(
    schema: &ArrowSchema,
    identifier_field_ids: &std::collections::HashSet<i32>,
    filters: &[datafusion::logical_expr::Expr],
) -> EqualityDeletePlan {
    let (eligible_ids, eligible_indices) = equality_delete_columns(schema);

    // A partial identity is not an identity: keying on the usable part of it
    // would match rows the predicate never selected.
    let identity_is_usable = !identifier_field_ids.is_empty()
        && identifier_field_ids
            .iter()
            .all(|id| eligible_ids.contains(id));

    let (equality_ids, key_indices): (Vec<i32>, Vec<usize>) = if identity_is_usable {
        eligible_ids
            .iter()
            .zip(eligible_indices.iter())
            .filter(|(id, _)| identifier_field_ids.contains(*id))
            .map(|(id, idx)| (*id, *idx))
            .unzip()
    } else {
        (eligible_ids, eligible_indices)
    };

    let mut scan_indices = key_indices;
    for filter in filters {
        for column in filter.column_refs() {
            if let Ok(idx) = schema.index_of(column.name.as_str())
                && !scan_indices.contains(&idx)
            {
                scan_indices.push(idx);
            }
        }
    }
    scan_indices.sort_unstable();

    EqualityDeletePlan {
        equality_ids,
        scan_indices,
    }
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
    /// Pre-computed equality delete field IDs — the delete file's key.
    equality_ids: Vec<i32>,
    /// Field IDs the input scan actually carries. A superset of `equality_ids`:
    /// the scan also reads whatever the predicate references, so the writer's
    /// projector must map from these columns, not from the key alone.
    scan_field_ids: Vec<i32>,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergDeleteExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        equality_ids: Vec<i32>,
        scan_field_ids: Vec<i32>,
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
            scan_field_ids,
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
            self.scan_field_ids.clone(),
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
        let scan_field_ids = self.scan_field_ids.clone();

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

            // Describe the batches the scan actually produces, which is the key
            // columns plus whatever the predicate referenced. The projector maps
            // this schema down to `equality_ids`; handing it the key alone would
            // index past the end of a wider batch. Columns it cannot key on
            // (floats, nested) are skipped by the projector rather than rejected,
            // so carrying them here is safe.
            let scan_id_set: std::collections::HashSet<i32> =
                scan_field_ids.iter().copied().collect();
            let equality_fields: Vec<_> = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|f| scan_id_set.contains(&f.id))
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

        let identifier_field_ids: std::collections::HashSet<i32> =
            iceberg_schema.identifier_field_ids().collect();
        let EqualityDeletePlan {
            equality_ids,
            scan_indices,
        } = plan_equality_delete(&arrow_schema, &identifier_field_ids, filters);

        // The Parquet field IDs of the columns the scan will carry, in the same
        // order, so the delete writer can describe its own input.
        let scan_field_ids: Vec<i32> = scan_indices
            .iter()
            .filter_map(|idx| {
                arrow_schema
                    .field(*idx)
                    .metadata()
                    .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                    .and_then(|v| v.parse::<i32>().ok())
            })
            .collect();

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
            .scan(state, Some(&scan_indices), filters, None)
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
            scan_field_ids,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{EqualityDeletePlan, equality_delete_columns, plan_equality_delete};
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
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

    fn ids(values: &[i32]) -> std::collections::HashSet<i32> {
        values.iter().copied().collect()
    }

    /// A schema whose only distinguishing column is a float: `id` and `label`
    /// repeat, `price` does not.
    fn float_keyed_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            field("id", DataType::Int64, Some("1")),
            field("label", DataType::Utf8, Some("2")),
            field("price", DataType::Float64, Some("3")),
        ])
    }

    /// A predicate on a float column has to reach the scan even though the
    /// delete cannot key on it. Projecting only the key columns left
    /// `DELETE ... WHERE price = 1.5` with no `price` to bind to, so the
    /// statement could not be planned at all.
    #[test]
    fn a_predicate_column_reaches_the_scan_even_when_it_cannot_be_keyed_on() {
        let schema = float_keyed_schema();
        let plan = plan_equality_delete(&schema, &ids(&[]), &[col("price").eq(lit(1.5))]);

        assert_eq!(plan.scan_indices, vec![0, 1, 2], "price must be scanned");
        assert_eq!(
            plan.equality_ids,
            vec![1, 2],
            "but it must not become part of the delete key"
        );
    }

    /// The scan is a superset of the key, never a replacement for it: widening
    /// for the predicate must not drop a key column.
    #[test]
    fn the_scan_always_covers_every_key_column() {
        let schema = float_keyed_schema();
        for filters in [
            vec![],
            vec![col("price").gt(lit(0.0))],
            vec![col("id").eq(lit(1_i64))],
            vec![col("id").eq(lit(1_i64)), col("price").lt(lit(9.0))],
        ] {
            let plan = plan_equality_delete(&schema, &ids(&[]), &filters);
            let (key_ids, key_indices) = equality_delete_columns(&schema);
            assert_eq!(plan.equality_ids, key_ids);
            for idx in key_indices {
                assert!(
                    plan.scan_indices.contains(&idx),
                    "key column {idx} missing from the scan for {filters:?}"
                );
            }
        }
    }

    /// Identifier fields are the row identity, so the delete keys on those and
    /// removes exactly the rows the predicate matched. Keying on every eligible
    /// column instead looks stricter but is not: two rows agreeing on all of
    /// them are one row to the delete file.
    #[test]
    fn a_declared_identity_becomes_the_key_while_the_scan_stays_wide() {
        let schema = float_keyed_schema();
        let plan = plan_equality_delete(&schema, &ids(&[1]), &[col("price").eq(lit(1.5))]);

        assert_eq!(plan.equality_ids, vec![1], "keyed on the declared identity");
        assert_eq!(
            plan.scan_indices,
            vec![0, 2],
            "identity column plus the predicate's column"
        );
    }

    #[test]
    fn a_composite_identity_keeps_every_part_in_schema_order() {
        let schema = ArrowSchema::new(vec![
            field("tenant", DataType::Int32, Some("7")),
            field("payload", DataType::Utf8, Some("8")),
            field("id", DataType::Int64, Some("9")),
        ]);
        let plan = plan_equality_delete(&schema, &ids(&[9, 7]), &[]);

        assert_eq!(plan.equality_ids, vec![7, 9]);
        assert_eq!(plan.scan_indices, vec![0, 2]);
    }

    /// No declared identity leaves no better key than every eligible column.
    #[test]
    fn without_an_identity_the_key_is_every_eligible_column() {
        let schema = float_keyed_schema();
        let plan = plan_equality_delete(&schema, &ids(&[]), &[]);

        assert_eq!(
            plan,
            EqualityDeletePlan {
                equality_ids: vec![1, 2],
                scan_indices: vec![0, 1],
            }
        );
    }

    /// A partial identity is not an identity. If one identifier field cannot be
    /// keyed on, keying on the rest would match rows the predicate never chose.
    #[test]
    fn an_identity_with_an_unusable_field_falls_back_to_every_eligible_column() {
        let schema = float_keyed_schema();

        // Field 3 is the float, so the identity can never be honoured.
        let plan = plan_equality_delete(&schema, &ids(&[1, 3]), &[]);
        assert_eq!(plan.equality_ids, vec![1, 2]);

        // Same for an identity naming a field the schema does not carry.
        let plan = plan_equality_delete(&schema, &ids(&[99]), &[]);
        assert_eq!(plan.equality_ids, vec![1, 2]);
    }

    /// A predicate naming a column that is not in the schema must not silently
    /// widen the scan with a bogus index; planning fails later on its own terms.
    #[test]
    fn an_unknown_predicate_column_does_not_widen_the_scan() {
        let schema = float_keyed_schema();
        let plan = plan_equality_delete(&schema, &ids(&[]), &[col("nope").eq(lit(1_i64))]);

        assert_eq!(plan.scan_indices, vec![0, 1]);
    }

    /// The scan projection is handed to `TableProvider::scan`, which requires
    /// ascending indices, and duplicates would double-project a column.
    #[test]
    fn the_scan_projection_is_sorted_and_free_of_duplicates() {
        let schema = float_keyed_schema();
        let plan = plan_equality_delete(
            &schema,
            &ids(&[]),
            &[
                col("price").eq(lit(1.5)),
                col("label").eq(lit("x")),
                col("price").gt(lit(0.0)),
            ],
        );

        assert_eq!(plan.scan_indices, vec![0, 1, 2]);
        let mut deduped = plan.scan_indices.clone();
        deduped.dedup();
        assert_eq!(deduped, plan.scan_indices);
    }

    #[test]
    fn an_empty_schema_yields_an_empty_key() {
        let (ids, indices) = equality_delete_columns(&ArrowSchema::empty());
        assert!(ids.is_empty());
        assert!(indices.is_empty());
    }
}
