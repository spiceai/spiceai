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

use arrow::array::{Array, make_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_tools::type_rewrite::relabel_array_data;
use async_trait::async_trait;
use aws_sdk_credential_bridge;
use chrono::TimeZone;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DFSchema, exec_err};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{ColumnarValue, Expr, Operator, TableProviderFilterPushDown, lit};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::expressions::{BinaryExpressionOp, DecimalData, Expression, Scalar};
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::{DecimalType, PrimitiveType};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::{ExpressionRef, Predicate, SnapshotRef};
use indexmap::IndexMap;
use object_store::ObjectMeta;
use pruning::{can_be_evaluted_for_partition_pruning, prune_partitions};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio::runtime::Handle;
use url::Url;
use util::format_datafusion_error;

use crate::Read;

mod pruning;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to the Delta Lake Table. Verify the Delta Lake Table configuration is valid, and try again. Received the following error while connecting: {source}"
    ))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display(
        "Delta Lake Table checkpoint files are missing or incorrect. Recreate the checkpoint for the Delta Lake Table and try again. {source}"
    ))]
    DeltaCheckpointError { source: delta_kernel::Error },

    #[snafu(display(
        "Failed to plan or execute a Delta Lake table due to the following error: {}",
        format_datafusion_error(source)
    ))]
    DeltaTableExecutionError { source: DataFusionError },

    #[snafu(display(
        "Invalid Delta Lake Table partition value count. The PartitionedFile has a different number of partition values than the number of partition columns."
    ))]
    InvalidPartitionValueCount,

    #[snafu(display(
        "An error has occurred trying to read or update the current snapshot: {source}"
    ))]
    SnapshotLockError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to create object store for Delta Lake table {table_url}: {source}"))]
    ObjectStore {
        table_url: String,
        source: object_store::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DeltaTableFactory {
    params: HashMap<String, SecretString>,
    io_runtime: Handle,
    table_parquet_options: TableParquetOptions,
}

impl std::fmt::Debug for DeltaTableFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableFactory")
            .field("params", &self.params.keys())
            .finish_non_exhaustive()
    }
}

impl DeltaTableFactory {
    #[must_use]
    pub fn new(params: HashMap<String, SecretString>, io_runtime: Handle) -> Self {
        Self {
            params,
            io_runtime,
            table_parquet_options: TableParquetOptions::default(),
        }
    }

    #[must_use]
    pub fn with_table_parquet_options(mut self, opts: TableParquetOptions) -> Self {
        self.table_parquet_options = opts;
        self
    }
}

#[async_trait]
impl Read for DeltaTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let delta_path = table_reference.table().to_string();
        let delta: DeltaTable = DeltaTable::from(delta_path, self.params.clone(), &self.io_runtime)
            .boxed()?
            .with_table_parquet_options(self.table_parquet_options.clone());
        Ok(Arc::new(delta))
    }
}

#[derive(Debug)]
pub struct DeltaTable {
    table_url: Url,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    parquet_object_store: Arc<dyn object_store::ObjectStore>,
    /// User-facing Arrow schema with logical column names. When the Delta table
    /// uses column mapping (`Name` or `Id` mode), parquet files store data under
    /// physical column names that differ from these logical names.
    arrow_schema: SchemaRef,
    delta_schema: delta_kernel::schema::SchemaRef,
    snapshot: RwLock<SnapshotRef>,
    /// Pre-computed physical schema mapping for column mapping modes (`Name`/`Id`).
    /// `None` when the table uses no column mapping (physical names == logical names).
    physical_schema_mapping: Option<PhysicalSchemaMapping>,
    table_parquet_options: TableParquetOptions,
}

impl DeltaTable {
    pub fn from(
        table_location: String,
        options: HashMap<String, SecretString>,
        io_runtime: &Handle,
    ) -> Result<Self> {
        let table_url = delta_kernel::try_parse_uri(ensure_folder_location(table_location))
            .map_err(handle_delta_error)?;

        let mut storage_options: HashMap<String, String> = HashMap::new();
        for (key, value) in options {
            match key.as_ref() {
                "token" | "endpoint" | "credential_vending" => {}
                "client_timeout" => {
                    storage_options.insert("timeout".into(), value.expose_secret().to_string());
                }
                _ => {
                    storage_options.insert(key.clone(), value.expose_secret().to_string());
                }
            }
        }

        // For S3 tables without explicit credentials, use the AWS SDK credential bridge so that
        // IAM roles, environment-variable chains, and other SDK-managed auth sources are available
        // for both the delta-kernel engine (log reads) and the parquet reader (data reads).
        let (parquet_object_store, engine) = if table_url.scheme() == "s3" {
            let region = storage_options
                .get("delta_lake_aws_region")
                .or_else(|| storage_options.get("aws_region"))
                .map(ToString::to_string);

            if let Some(sdk_config) = aws_sdk_credential_bridge::should_use_sdk_credentials(
                &storage_options,
                "aws_access_key_id",
                "aws_secret_access_key",
            ) {
                match aws_sdk_credential_bridge::from_s3_url_and_config(
                    &table_url,
                    region,
                    sdk_config.as_ref(),
                    io_runtime.clone(),
                ) {
                    Ok(sdk_store) => {
                        tracing::trace!(
                            "Using AWS SDK credentials provider for Delta Lake table at {table_url}"
                        );
                        let sdk_store: Arc<dyn object_store::ObjectStore> = sdk_store.into();
                        let engine =
                            Arc::new(DefaultEngine::builder(Arc::clone(&sdk_store)).build());
                        (sdk_store, engine)
                    }
                    Err(err) => {
                        tracing::debug!(
                            "Unable to build AWS SDK object store for Delta Lake table at {table_url}: {err}; falling back to delta_kernel credential resolution"
                        );
                        Self::build_default_stores(&table_url, storage_options)?
                    }
                }
            } else {
                // Explicit credentials present — pass them through delta_kernel's built-in resolution.
                Self::build_default_stores(&table_url, storage_options)?
            }
        } else {
            Self::build_default_stores(&table_url, storage_options)?
        };

        Self::with_engine(table_url, engine, parquet_object_store)
    }

    /// Builds the default (non-SDK) object store and delta-kernel engine from `storage_options`.
    #[expect(clippy::type_complexity)]
    fn build_default_stores(
        table_url: &Url,
        storage_options: HashMap<String, String>,
    ) -> Result<(
        Arc<dyn object_store::ObjectStore>,
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
    )> {
        let (parquet_store, _) = object_store::parse_url_opts(
            table_url,
            storage_options
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .context(ObjectStoreSnafu {
            table_url: table_url.to_string(),
        })?;
        let parquet_store: Arc<dyn object_store::ObjectStore> = Arc::from(parquet_store);

        let engine = Arc::new(
            DefaultEngine::builder(
                store_from_url_opts(table_url, storage_options).map_err(handle_delta_error)?,
            )
            .build(),
        );

        Ok((parquet_store, engine))
    }

    /// Creates a `DeltaTable` backed by a pre-built object store, bypassing
    /// the credential resolution in [`DeltaTable::from`].
    ///
    /// Used by Unity Catalog credential vending, where the store
    /// authenticates with vended, refresh-aware credentials.
    pub fn from_object_store(
        table_location: String,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Self> {
        let table_url = delta_kernel::try_parse_uri(ensure_folder_location(table_location))
            .map_err(handle_delta_error)?;
        // delta-kernel 0.23 removed `DefaultEngine::new`; construct via the
        // builder (mirrors the `DefaultEngine::builder(..).build()` call above).
        let engine = Arc::new(DefaultEngine::builder(Arc::clone(&object_store)).build());
        Self::with_engine(table_url, engine, object_store)
    }

    fn with_engine(
        table_url: Url,
        engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
        parquet_object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Self> {
        let snapshot = Snapshot::builder_for(table_url.clone())
            .build(engine.as_ref())
            .map_err(handle_delta_error)?;

        let delta_schema = snapshot.schema();
        let column_mapping_mode = snapshot.table_configuration().column_mapping_mode();

        tracing::debug!(
            version = snapshot.version(),
            column_mapping = ?column_mapping_mode,
            "Initializing Delta Lake table at '{table_url}'",
        );

        let arrow_schema = Self::get_logical_schema(&snapshot);

        let physical_schema_mapping = if column_mapping_mode == ColumnMappingMode::None {
            None
        } else {
            Some(build_physical_schema_mapping(
                &delta_schema,
                column_mapping_mode,
            ))
        };

        Ok(Self {
            table_url,
            engine,
            parquet_object_store,
            arrow_schema: Arc::new(arrow_schema),
            delta_schema,
            snapshot: RwLock::new(snapshot),
            physical_schema_mapping,
            table_parquet_options: TableParquetOptions::default(),
        })
    }

    #[must_use]
    pub fn with_table_parquet_options(mut self, opts: TableParquetOptions) -> Self {
        self.table_parquet_options = opts;
        self
    }

    /// Gets the latest snapshot by paginating object storage. It uses version hints from the currently
    /// bound snapshot to prune the log scan.
    ///
    /// At the start of a scan, you can get the latest snapshot then reuse it via `DeltaTable::bound_snapshot`
    /// without polling object storage.
    fn get_and_update_snapshot(&self) -> Result<SnapshotRef> {
        let mut current_snapshot = self
            .snapshot
            .write()
            .map_err(|e| Error::SnapshotLockError {
                source: format!("Unable to update snapshot {e}").into(),
            })?;

        let new_snapshot = Snapshot::builder_from(Arc::clone(&*current_snapshot))
            .build(self.engine.as_ref())
            .context(DeltaTableSnafu)?;

        if new_snapshot != *current_snapshot {
            *current_snapshot = new_snapshot;
        }

        Ok(Arc::clone(&*current_snapshot))
    }

    /// Builds the logical (user-facing) Arrow schema from the snapshot's delta schema.
    fn get_logical_schema(snapshot: &Snapshot) -> Schema {
        let schema = snapshot.schema();

        let mut fields: Vec<Field> = vec![];
        for field in schema.fields() {
            fields.push(Field::new(
                field.name(),
                // `ColumnMappingMode::None` to always return logical names
                map_delta_data_type_to_arrow_data_type(&field.data_type, ColumnMappingMode::None),
                field.nullable,
            ));
        }

        Schema::new(fields)
    }

    #[expect(clippy::too_many_arguments)]
    fn create_parquet_exec(
        &self,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        schema: &Arc<Schema>,
        partition_cols: &[Field],
        parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
        partitioned_files: &[PartitionedFile],
        physical_expr: &Arc<dyn PhysicalExpr>,
        logical_to_physical: &HashMap<String, String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // this is needed to pass the plan_extension
        let projection = Some(
            projection
                .cloned()
                .unwrap_or((0..self.arrow_schema.fields().len()).collect::<Vec<_>>()),
        );

        let new_projections = projection.map(|projection| {
            projection
                .iter()
                .map(|&x| {
                    let field = self.arrow_schema.field(x);
                    let name_in_schema = logical_to_physical
                        .get(field.name())
                        .unwrap_or(field.name());

                    if let Ok(i) = schema.index_of(name_in_schema) {
                        return i;
                    }

                    if let Some(i) = partition_cols.iter().position(|r| r == field) {
                        return schema.fields.len() + i;
                    }

                    unreachable!("all projected fields should be mapped to new projected position");
                })
                .collect::<Vec<_>>()
        });
        let table_schema = datafusion_datasource::TableSchema::new(
            Arc::clone(schema),
            partition_cols.iter().map(|f| Arc::new(f.clone())).collect(),
        );
        tracing::trace!(
            table_parquet_options = ?self.table_parquet_options,
            "Creating Delta Lake ParquetSource"
        );
        let parquet_source = ParquetSource::new(table_schema)
            .with_table_parquet_options(self.table_parquet_options.clone())
            .with_parquet_file_reader_factory(Arc::clone(parquet_file_reader_factory))
            .with_predicate(Arc::clone(physical_expr));

        // Matches keying used by `ObjectStoreRegistry::get_url_key`
        // Use BeforeUsername to preserve userinfo (e.g., container name in abfss://container@account.dfs.core.windows.net/)
        let object_store_url = ObjectStoreUrl::parse(format!(
            "{}://{}",
            self.table_url.scheme(),
            &self.table_url[url::Position::BeforeUsername..url::Position::AfterPort]
        ))
        .context(DeltaTableExecutionSnafu)?;

        let file_scan_config_builder =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_limit(limit)
                .with_projection_indices(new_projections)
                .context(DeltaTableExecutionSnafu)?
                .with_file_group(FileGroup::new(partitioned_files.to_vec()));

        Ok(DataSourceExec::from_data_source(
            file_scan_config_builder.build(),
        ))
    }
}

fn ensure_folder_location(table_location: String) -> String {
    if table_location.ends_with('/') {
        table_location
    } else {
        format!("{table_location}/")
    }
}

/// Builds a [`PhysicalSchemaMapping`] for the given Delta schema and column mapping mode.
///
/// The result contains the physical Arrow schema (with physical column names used in parquet
/// files) and bidirectional name mappings between physical and logical column names.
fn build_physical_schema_mapping(
    delta_schema: &delta_kernel::schema::Schema,
    column_mapping_mode: ColumnMappingMode,
) -> PhysicalSchemaMapping {
    let mut fields = vec![];
    let mut physical_to_logical = HashMap::new();
    let mut logical_to_physical = HashMap::new();

    for field in delta_schema.fields() {
        let physical_name = field.physical_name(column_mapping_mode).to_string();
        let logical_name = field.name().clone();
        physical_to_logical.insert(physical_name.clone(), logical_name.clone());
        logical_to_physical.insert(logical_name, physical_name.clone());
        fields.push(Field::new(
            physical_name,
            map_delta_data_type_to_arrow_data_type(field.data_type(), column_mapping_mode),
            field.nullable,
        ));
    }

    PhysicalSchemaMapping {
        schema: Schema::new(fields),
        physical_to_logical,
        logical_to_physical,
    }
}

/// Result of [`build_physical_schema_mapping`]: contains the physical Arrow schema and
/// bidirectional name mappings between physical and logical column names.
#[derive(Debug)]
struct PhysicalSchemaMapping {
    schema: Schema,
    physical_to_logical: HashMap<String, String>,
    logical_to_physical: HashMap<String, String>,
}

/// Rewrites column references in a `DataFusion` [`Expr`] from logical to physical names.
///
/// This is needed so that predicates pushed down to [`ParquetExec`] reference the physical
/// column names actually present in the parquet files.
fn rewrite_column_names(
    expr: Expr,
    logical_to_physical: &HashMap<String, String>,
) -> Result<Expr, DataFusionError> {
    Ok(expr
        .transform(|e| {
            if let Expr::Column(col) = &e
                && let Some(physical_name) = logical_to_physical.get(col.name())
            {
                return Ok(datafusion::common::tree_node::Transformed::yes(
                    Expr::Column(datafusion::common::Column::new(
                        col.relation.clone(),
                        physical_name,
                    )),
                ));
            }
            Ok(datafusion::common::tree_node::Transformed::no(e))
        })?
        .data)
}

/// How much of one refusal's rendered text is kept before it is truncated. Long enough to carry
/// any real column name and the clause around it; short enough that a malformed schema naming a
/// column with a megabyte of text cannot turn a refusal into a megabyte of message.
const MAX_RENDERED_CHARS: usize = 512;

/// Renders text the table chose so it stays on one line and still names exactly one column.
///
/// Column and nested field names come out of the Delta schema, so a name holding a newline would
/// split a refusal that [`unmatched_nested_field`] documents — and `refusals_stay_on_one_line`
/// asserts — as a single line, and each fragment would read like an independent failure.
///
/// Control characters are *escaped* rather than replaced, because the operator reads this message
/// to find the column in their own schema: collapsing them to spaces would render the distinct
/// columns `a\tb` and `a b` identically, and name the wrong one half the time. Everything else is
/// passed through, so the quotes and punctuation the refusals put around these names survive.
fn as_one_line(text: &str) -> String {
    let mut rendered = String::with_capacity(text.len());
    for character in text.chars().take(MAX_RENDERED_CHARS) {
        if character.is_control() {
            rendered.extend(character.escape_debug());
        } else {
            rendered.push(character);
        }
    }
    // `nth` rather than `count`: asking how long the whole text is would walk all of it, which is
    // the work the cap exists to avoid.
    if text.chars().nth(MAX_RENDERED_CHARS).is_some() {
        rendered.push('\u{2026}');
    }
    rendered
}

/// The refusal [`logical_target_in_source_order`] returns, in the shape this connector's other
/// messages take: the table and column that cannot be read, the disagreement, what it costs, and
/// the one action that re-reads the schema. `disagreement` completes "column '<name>' ...".
///
/// Kept on one line in the rendered value — the `\` continuations below strip the newline and the
/// indentation that follows it, which `refusals_stay_on_one_line` holds to. The names the table
/// supplies are rendered through [`as_one_line`] for the same reason.
fn unmatched_nested_field(table_url: &Url, column: &str, disagreement: &str) -> DataFusionError {
    let column = as_one_line(column);
    let disagreement = as_one_line(disagreement);
    DataFusionError::Plan(format!(
        "Failed to read Delta Lake table '{table_url}': column '{column}' {disagreement}, so its \
         fields cannot be matched to their names and the column would be read with its values \
         under the wrong ones. Re-register the dataset so its schema is read from the current \
         table version. See: https://spiceai.org/docs/components/data-connectors/delta-lake"
    ))
}

/// Rebuilds `logical` in `source`'s field order, so it describes the layout `source` already has.
///
/// [`relabel_array_data`] pairs children positionally while permitting renames, so a target whose
/// same-typed sibling fields are ordered differently from the array's is accepted and every child
/// keeps the values it already held, published under another field's name. The Delta column
/// mapping cannot give the rename up — its physical field names are opaque column-mapping ids that
/// never match the logical ones — so the ordering is re-established here instead, which is what
/// lets the positional pairing downstream be sound.
///
/// `physical` supplies the column identity the rename destroys. It and `logical` are two
/// renderings of one walk over the same Delta schema (see
/// [`map_delta_data_type_to_arrow_data_type`], which varies only the name it takes from each
/// field), so the field at a given index in each is the same column. A source field is matched to
/// the physical field of the same name, and the logical field at that index supplies the name,
/// nullability and metadata it is relabelled to. When the two orders already agree — which is
/// every table whose files were written in the order its schema declares — this reproduces
/// `logical` exactly.
///
/// Only `Struct`, `List` and `Map` are walked, because those are the only child-bearing types
/// [`map_delta_data_type_to_arrow_data_type`] builds. Anything else is taken from `logical` whole,
/// but only once `source` and `physical` agree — an unwalked node whose names already differ is
/// refused rather than paired positionally.
///
/// # Errors
///
/// Returns a `DataFusionError` when a source field has no physical field of that name, when two
/// source fields claim the same physical field, when a struct's source and physical field counts
/// disagree, or when an unwalked node's source and physical types differ. Each of those would
/// otherwise be resolved by a positional pairing that nothing checked.
fn logical_target_in_source_order(
    source: &DataType,
    physical: &DataType,
    logical: &DataType,
    table_url: &Url,
    column: &str,
) -> std::result::Result<DataType, DataFusionError> {
    match (source, physical, logical) {
        (
            DataType::Struct(source_fields),
            DataType::Struct(physical_fields),
            DataType::Struct(logical_fields),
        ) => {
            if source_fields.len() != physical_fields.len()
                || physical_fields.len() != logical_fields.len()
            {
                return Err(unmatched_nested_field(
                    table_url,
                    column,
                    &format!(
                        "holds a nested value with {} field(s) where the table's column mapping names {}",
                        source_fields.len(),
                        physical_fields.len(),
                    ),
                ));
            }

            let mut claimed = vec![false; physical_fields.len()];
            let mut fields = Vec::with_capacity(source_fields.len());
            for source_field in source_fields {
                let Some(index) = physical_fields
                    .iter()
                    .position(|physical_field| physical_field.name() == source_field.name())
                else {
                    return Err(unmatched_nested_field(
                        table_url,
                        column,
                        &format!(
                            "holds a nested field '{}' that the table's column mapping does not name",
                            source_field.name(),
                        ),
                    ));
                };

                // `position` yields the first match, so two source fields sharing a name would
                // both take the same logical name and one column's values would be published
                // twice while the other's were dropped.
                if std::mem::replace(&mut claimed[index], true) {
                    return Err(unmatched_nested_field(
                        table_url,
                        column,
                        &format!("holds two nested fields named '{}'", source_field.name()),
                    ));
                }

                fields.push(logical_field_in_source_order(
                    source_field,
                    &physical_fields[index],
                    &logical_fields[index],
                    table_url,
                    column,
                )?);
            }

            Ok(DataType::Struct(fields.into()))
        }
        (
            DataType::List(source_item),
            DataType::List(physical_item),
            DataType::List(logical_item),
        ) => Ok(DataType::List(Arc::new(logical_field_in_source_order(
            source_item,
            physical_item,
            logical_item,
            table_url,
            column,
        )?))),
        (
            DataType::Map(source_entries, _),
            DataType::Map(physical_entries, _),
            DataType::Map(logical_entries, logical_sorted),
        ) => Ok(DataType::Map(
            Arc::new(logical_field_in_source_order(
                source_entries,
                physical_entries,
                logical_entries,
                table_url,
                column,
            )?),
            // The `sorted` flag is not a field name, so it is carried as `logical` declares it
            // exactly as before; a disagreement over it is `relabel_array_data`'s to refuse.
            *logical_sorted,
        )),
        // A node this does not walk carries no nested field names to reorder, so `logical` can
        // only be taken whole once `source` is known to spell its own names the way `physical`
        // does. That equality is what makes the pairing sound, and it holds for every type the
        // Delta mapper builds: it renders leaves identically in both modes, and the three
        // child-bearing types it can produce are walked above.
        _ if source == physical => Ok(logical.clone()),
        // Refused rather than relabelled on a pairing nothing checked: a nested name that
        // reaches here is one this walk cannot match to a logical name, and guessing it
        // positionally is how values end up under the wrong column.
        _ => Err(unmatched_nested_field(
            table_url,
            column,
            &format!("holds a {source} the table's column mapping describes as a {physical}"),
        )),
    }
}

/// One field of [`logical_target_in_source_order`]'s result: everything but the child type is
/// taken from `logical`, so a source and logical field that already agree rebuild identically.
fn logical_field_in_source_order(
    source: &Field,
    physical: &Field,
    logical: &Field,
    table_url: &Url,
    column: &str,
) -> std::result::Result<Field, DataFusionError> {
    Ok(Field::new(
        logical.name(),
        logical_target_in_source_order(
            source.data_type(),
            physical.data_type(),
            logical.data_type(),
            table_url,
            column,
        )?,
        logical.is_nullable(),
    )
    .with_metadata(logical.metadata().clone()))
}

/// Builds a [`ProjectionExec`] that renames columns from physical names back to logical names.
///
/// For columns with nested types (Struct, List, Map) where the physical and logical data types
/// differ (because nested field names are also physical), wraps the column in a
/// [`RelabelFieldsExpr`] to recursively rename nested struct/list/map field names. A
/// `CastExpr` can no longer be used for this: `DataFusion` 53 rejects a struct-to-struct
/// cast whose source and target fields share no names (the physical names are opaque
/// column-mapping ids), so the rename must be done as a metadata-only relabel instead.
fn build_column_mapping_projection(
    exec: Arc<dyn ExecutionPlan>,
    mapping: &PhysicalSchemaMapping,
    logical_schema: &SchemaRef,
    table_url: &Url,
) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let exec_schema = exec.schema();
    let projection_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = exec_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let logical_name = mapping
                .physical_to_logical
                .get(field.name())
                .cloned()
                .unwrap_or_else(|| field.name().clone());

            // If the logical field has a different data type (nested field names differ),
            // relabel the nested struct/list/map field names from physical to logical.
            let expr: Arc<dyn PhysicalExpr> = match logical_schema.field_with_name(&logical_name) {
                Ok(logical_field) if field.data_type() != logical_field.data_type() => {
                    // Only a column the mapping named can get here: a partition column is absent
                    // from `physical_to_logical`, so it keeps its own name and compares equal to
                    // the logical field of that name.
                    let physical_field =
                        mapping.schema.field_with_name(field.name()).map_err(|_| {
                            unmatched_nested_field(
                                table_url,
                                &logical_name,
                                &format!(
                                    "is read from a file field '{}' the table's column mapping does not name",
                                    field.name(),
                                ),
                            )
                        })?;

                    // The relabel pairs children positionally, so the target has to be in the
                    // order the scan produces rather than the order the schema declares.
                    let target = logical_target_in_source_order(
                        field.data_type(),
                        physical_field.data_type(),
                        logical_field.data_type(),
                        table_url,
                        &logical_name,
                    )?;

                    Arc::new(RelabelFieldsExpr::new(
                        Arc::new(Column::new(field.name(), i)),
                        target,
                    ))
                }
                _ => Arc::new(Column::new(field.name(), i)),
            };

            Ok((expr, logical_name))
        })
        .collect::<std::result::Result<Vec<_>, DataFusionError>>()?;

    Ok(Arc::new(ProjectionExec::try_new(projection_expr, exec)?))
}

/// Physical expression that renames the (possibly nested) field names of its
/// input array to `target_type` without changing any values. Used by Delta
/// column mapping to map physical field names back to logical names; replaces a
/// `CastExpr`, which `DataFusion` 53 rejects for structs whose physical and
/// logical field names don't overlap.
#[derive(Debug, Clone, Eq)]
struct RelabelFieldsExpr {
    arg: Arc<dyn PhysicalExpr>,
    target_type: DataType,
}

impl RelabelFieldsExpr {
    fn new(arg: Arc<dyn PhysicalExpr>, target_type: DataType) -> Self {
        Self { arg, target_type }
    }
}

impl PartialEq for RelabelFieldsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.target_type.eq(&other.target_type)
    }
}

impl std::hash::Hash for RelabelFieldsExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
        self.target_type.hash(state);
    }
}

impl std::fmt::Display for RelabelFieldsExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RELABEL({} AS {})", self.arg, self.target_type)
    }
}

impl PhysicalExpr for RelabelFieldsExpr {
    fn data_type(&self, _input_schema: &Schema) -> std::result::Result<DataType, DataFusionError> {
        Ok(self.target_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> std::result::Result<bool, DataFusionError> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> std::result::Result<ColumnarValue, DataFusionError> {
        let array = self.arg.evaluate(batch)?.into_array(batch.num_rows())?;
        let relabeled = make_array(
            relabel_array_data(array.to_data(), &self.target_type)
                .map_err(DataFusionError::from)?,
        );
        Ok(ColumnarValue::Array(relabeled))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> std::result::Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        Ok(Arc::new(RelabelFieldsExpr::new(
            Arc::clone(&children[0]),
            self.target_type.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
#[expect(clippy::cast_possible_wrap)]
fn map_delta_data_type_to_arrow_data_type(
    delta_data_type: &delta_kernel::schema::DataType,
    column_mapping_mode: ColumnMappingMode,
) -> DataType {
    match delta_data_type {
        delta_kernel::schema::DataType::Primitive(primitive_type) => match primitive_type {
            delta_kernel::schema::PrimitiveType::String => DataType::Utf8,
            delta_kernel::schema::PrimitiveType::Long => DataType::Int64,
            delta_kernel::schema::PrimitiveType::Integer => DataType::Int32,
            delta_kernel::schema::PrimitiveType::Short => DataType::Int16,
            delta_kernel::schema::PrimitiveType::Byte => DataType::Int8,
            delta_kernel::schema::PrimitiveType::Float => DataType::Float32,
            delta_kernel::schema::PrimitiveType::Double => DataType::Float64,
            delta_kernel::schema::PrimitiveType::Boolean => DataType::Boolean,
            delta_kernel::schema::PrimitiveType::Binary => DataType::Binary,
            delta_kernel::schema::PrimitiveType::Date => DataType::Date32,
            delta_kernel::schema::PrimitiveType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            delta_kernel::schema::PrimitiveType::TimestampNtz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            delta_kernel::schema::PrimitiveType::Decimal(d) => {
                DataType::Decimal128(d.precision(), d.scale() as i8)
            }
        },
        delta_kernel::schema::DataType::Array(array_type) => DataType::List(Arc::new(Field::new(
            "item",
            map_delta_data_type_to_arrow_data_type(array_type.element_type(), column_mapping_mode),
            array_type.contains_null(),
        ))),
        delta_kernel::schema::DataType::Struct(struct_type)
        | delta_kernel::schema::DataType::Variant(struct_type) => {
            let mut fields: Vec<Field> = vec![];
            for field in struct_type.fields() {
                fields.push(Field::new(
                    field.physical_name(column_mapping_mode),
                    map_delta_data_type_to_arrow_data_type(field.data_type(), column_mapping_mode),
                    field.nullable,
                ));
            }
            DataType::Struct(fields.into())
        }
        delta_kernel::schema::DataType::Map(map_type) => {
            let key_type =
                map_delta_data_type_to_arrow_data_type(map_type.key_type(), column_mapping_mode);
            let value_type =
                map_delta_data_type_to_arrow_data_type(map_type.value_type(), column_mapping_mode);
            DataType::Map(
                Arc::new(Field::new_struct(
                    map_type.type_name.clone(),
                    vec![
                        Arc::new(Field::new("key", key_type, false)),
                        Arc::new(Field::new(
                            "value",
                            value_type,
                            map_type.value_contains_null(),
                        )),
                    ],
                    false,
                )),
                false,
            )
        }
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        let Ok(snapshot) = self.get_and_update_snapshot() else {
            return exec_err!("Unable to get latest Delta table snapshot");
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.arrow_schema))?;

        let parquet_file_reader_factory = Arc::new(DefaultParquetFileReaderFactory::new(
            Arc::clone(&self.parquet_object_store),
        )) as Arc<dyn ParquetFileReaderFactory>;
        let projected_delta_schema = project_delta_schema(
            &self.arrow_schema,
            Arc::clone(&self.delta_schema),
            projection,
        )?;
        let engine = Arc::clone(&self.engine);

        // Clone the filters since we need to move them into the spawn_blocking closure
        let filters_clone = filters.to_vec();

        // The following Delta Lake scan is blocking - run it in a separate blocking task to prevent the Tokio runtime from starving
        let (scan_context, parquet_file_reader_factory, df_schema) =
            tokio::task::spawn_blocking(move || {
                // We'll convert all filters for delta_kernel predicates since
                // partition pruning is already handled separately later in the code

                let mut scan_builder =
                    ScanBuilder::new(snapshot).with_schema(projected_delta_schema);

                // Convert and apply predicate if possible
                if let Some(predicate) = filters_to_delta_kernel_predicate(&filters_clone) {
                    tracing::debug!(
                        "Using delta_kernel predicate for filter pushdown: {predicate:?}"
                    );
                    scan_builder = scan_builder.with_predicate(Some(Arc::new(predicate)));
                }

                let scan = scan_builder
                    .build()
                    .map_err(map_delta_error_to_datafusion_err)?;

                let table_root = scan.table_root();
                let mut scan_context = ScanContext::new(Arc::clone(&engine), table_root.clone());

                let scan_iter = scan
                    .scan_metadata(engine.as_ref())
                    .map_err(map_delta_error_to_datafusion_err)?;

                for scan_result in scan_iter {
                    let scan = scan_result.map_err(map_delta_error_to_datafusion_err)?;
                    scan_context = scan
                        .visit_scan_files(scan_context, handle_scan_file)
                        .map_err(map_delta_error_to_datafusion_err)?;
                }

                Ok::<_, datafusion::error::DataFusionError>((
                    scan_context,
                    parquet_file_reader_factory,
                    df_schema,
                ))
            })
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("Delta Scan panicked: {e}"))
            })??;

        if let Some(err) = scan_context.errs.into_iter().next() {
            return Err(err);
        }

        // In Delta Lake, all files must have the same partition columns,
        // but Delta allows NULL values for the partition columns, represented in the filesystem as `__HIVE_DEFAULT_PARTITION__`.
        //
        // user_id=__HIVE_DEFAULT_PARTITION__/
        //   day=2024-01-01/
        //     part-00000.parquet
        // user_id=123/
        //   day=2024-01-01/
        //     part-00001.parquet
        //
        // In the above example, the partition columns are `user_id` and `day`.
        // The `user_id` column has a NULL value for the first file and a value of `123` for the second file.
        //
        // The `delta_kernel` library skips returning the partition columns for files that have a NULL value for the partition columns.
        // Which means that the partition columns will not be returned in the `partition_values` field of the `PartitionedFile` object.
        // We handle this by keeping track of all the partition columns we find in the `all_partition_columns` variable and if one
        // doesn't have a value, we add a NULL value for that field to the `partition_values` field of the `PartitionedFile` object.
        let mut partitioned_files: Vec<PartitionedFile> = vec![];
        let physical_to_logical = self
            .physical_schema_mapping
            .as_ref()
            .map(|m| &m.physical_to_logical);
        let all_partition_columns = scan_context
            .files
            .iter()
            .flat_map(|file| {
                file.partition_values.keys().filter_map(|k| {
                    let schema = self.schema();
                    // With column mapping, partition value keys use physical names.
                    // Translate to logical name for schema lookup.
                    let logical_key = physical_to_logical
                        .and_then(|m| m.get(k))
                        .map_or(k.as_str(), String::as_str);
                    schema.field_with_name(logical_key).ok().cloned()
                })
            })
            // Use an IndexMap to preserve insertion order
            .fold(IndexMap::new(), |mut acc, field| {
                acc.insert(field, ());
                acc
            });
        for file in scan_context.files {
            let mut partitioned_file = file.partitioned_file;
            partitioned_file.partition_values = all_partition_columns
                .iter()
                .map(|(field, ())| {
                    if let Some((_, value)) = file.partition_values.iter().find(|(k, _)| {
                        // With column mapping, k is a physical name; translate before comparing.
                        let logical_key = physical_to_logical
                            .and_then(|m| m.get(k.as_str()))
                            .map_or(k.as_str(), String::as_str);
                        logical_key == field.name()
                    }) {
                        ScalarValue::try_from_string(value.clone(), field.data_type())
                    } else {
                        // This will create a null value typed for the field
                        Ok(ScalarValue::try_from(field.data_type())?)
                    }
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;

            // If there is a selection vector, create a ParquetAccessPlan that will be used to skip rows based on the selection vector
            if let Some(selection_vector) = file.selection_vector {
                let access_plan = get_parquet_access_plan(
                    &parquet_file_reader_factory,
                    &partitioned_file,
                    selection_vector,
                )
                .await?;
                partitioned_file = partitioned_file.with_extension(access_plan);
            }

            partitioned_files.push(partitioned_file);
        }

        let partition_cols = all_partition_columns
            .into_iter()
            .map(|(field, ())| field)
            .collect::<Vec<_>>();

        let table_partition_col_names = partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        // Split the filters into partition filters and the rest
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });
        tracing::trace!("partition_filters: {partition_filters:?}");
        tracing::trace!("filters: {filters:?}");

        let num_partition_files = partitioned_files.len();
        let filtered_partitioned_files =
            prune_partitions(partitioned_files, &partition_filters, &partition_cols)?;

        tracing::debug!(
            "Partition pruning yielded {} files (out of {num_partition_files})",
            filtered_partitioned_files.len(),
        );

        let filter = conjunction(filters).unwrap_or_else(|| lit(true));

        let non_partition_indices = self
            .arrow_schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(i, f)| (!partition_cols.contains(f)).then_some(i))
            .collect::<Vec<_>>();

        if let Some(mapping) = &self.physical_schema_mapping {
            // Rewrite filter column references from logical to physical names
            // so ParquetExec can match them against the physical parquet schema.
            let physical_filter = rewrite_column_names(filter, &mapping.logical_to_physical)?;
            let physical_df_schema = DFSchema::try_from(Arc::new(mapping.schema.clone()))?;
            let physical_expr = state.create_physical_expr(physical_filter, &physical_df_schema)?;

            let physical_non_partition_schema =
                Arc::new(mapping.schema.project(&non_partition_indices)?);

            let exec = self
                .create_parquet_exec(
                    projection,
                    limit,
                    &physical_non_partition_schema,
                    &partition_cols,
                    &parquet_file_reader_factory,
                    &filtered_partitioned_files,
                    &physical_expr,
                    &mapping.logical_to_physical,
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            build_column_mapping_projection(exec, mapping, &self.arrow_schema, &self.table_url)
        } else {
            let physical_expr = state.create_physical_expr(filter, &df_schema)?;
            let schema = self.arrow_schema.project(&non_partition_indices)?;

            Ok(self
                .create_parquet_exec(
                    projection,
                    limit,
                    &Arc::new(schema),
                    &partition_cols,
                    &parquet_file_reader_factory,
                    &filtered_partitioned_files,
                    &physical_expr,
                    &HashMap::new(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?)
        }
    }
}

struct ScanContext {
    pub errs: Vec<datafusion::error::DataFusionError>,
    engine: Arc<DefaultEngine<TokioBackgroundExecutor>>,
    pub files: Vec<PartitionFileContext>,
    table_root: Url,
}

impl ScanContext {
    fn new(engine: Arc<DefaultEngine<TokioBackgroundExecutor>>, table_root: Url) -> Self {
        Self {
            engine,
            errs: Vec::new(),
            files: Vec::new(),
            table_root,
        }
    }
}

fn project_delta_schema(
    arrow_logical_schema: &SchemaRef,
    schema: delta_kernel::schema::SchemaRef,
    projections: Option<&Vec<usize>>,
) -> Result<delta_kernel::schema::SchemaRef, DataFusionError> {
    if let Some(projections) = projections {
        let projected_fields = projections
            .iter()
            .filter_map(|i| schema.field(arrow_logical_schema.field(*i).name()))
            .cloned()
            .collect::<Vec<_>>();
        Ok(Arc::new(
            delta_kernel::schema::Schema::try_new(projected_fields)
                .map_err(map_delta_error_to_datafusion_err)?,
        ))
    } else {
        Ok(schema)
    }
}

struct PartitionFileContext {
    partitioned_file: PartitionedFile,
    selection_vector: Option<Vec<bool>>,
    partition_values: HashMap<String, String>,

    /// These are transforms that Delta wants to apply to the physical data read from the Parquet files.
    /// Currently this is only used for adding partition columns and mapping the columns read from the Parquet files
    /// into the correct place in the output schema.
    ///
    /// Both of these functions are already handled for us by the `DataFusion` `ParquetExec`. However, we may need to
    /// revisit this if more complex transformations are required.
    ///
    /// See: <https://github.com/delta-io/delta-kernel-rs/blob/7e62d12def00f248eccef23e7672fd4db553274f/kernel/src/scan/mod.rs#L444>
    _transform: Option<ExpressionRef>,
}

#[expect(clippy::cast_sign_loss)]
fn handle_scan_file(scan_context: &mut ScanContext, scan_file: ScanFile) {
    let ScanFile {
        path,
        size,
        dv_info,
        transform,
        partition_values,
        ..
    } = scan_file;

    let root_url = &scan_context.table_root;

    let path = if root_url.path().ends_with('/') {
        format!("{}{}", root_url.path(), path)
    } else {
        format!("{}/{}", root_url.path(), path)
    };

    let partitioned_file_path = match object_store::path::Path::from_url_path(&path) {
        Ok(path) => path,
        Err(e) => {
            scan_context
                .errs
                .push(datafusion::error::DataFusionError::Execution(format!(
                    "Error parsing file path: {e}",
                )));
            return;
        }
    };

    tracing::trace!("partitioned_file_path: {partitioned_file_path:?}");

    let partitioned_file_object_meta = ObjectMeta {
        location: partitioned_file_path,
        last_modified: chrono::Utc.timestamp_nanos(0),
        size: size as u64,
        e_tag: None,
        version: None,
    };

    let partitioned_file = PartitionedFile::from(partitioned_file_object_meta);

    // Get the selection vector (i.e. inverse deletion vector)
    let selection_vector =
        match dv_info.get_selection_vector(scan_context.engine.as_ref(), root_url) {
            Ok(selection_vector) => selection_vector,
            Err(e) => {
                scan_context
                    .errs
                    .push(datafusion::error::DataFusionError::Execution(format!(
                        "Error getting selection vector: {e}",
                    )));
                return;
            }
        };

    scan_context.files.push(PartitionFileContext {
        partitioned_file,
        selection_vector,
        partition_values,
        _transform: transform,
    });
}

fn map_delta_error_to_datafusion_err(e: delta_kernel::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

fn get_row_group_access(
    selection_vector: &[bool],
    row_group_row_start: usize,
    row_group_num_rows: usize,
) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| !x)
    {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows]
        .iter()
        .all(|&x| x)
    {
        return RowGroupAccess::Scan;
    }

    let mask =
        selection_vector[row_group_row_start..row_group_row_start + row_group_num_rows].to_vec();

    // If some rows are deleted, get a row selection that skips the deleted rows
    let row_selection = RowSelection::from_filters(&[mask.into()]);
    RowGroupAccess::Selection(row_selection)
}

fn get_full_selection_vector(selection_vector: &[bool], total_rows: usize) -> Vec<bool> {
    let mut new_selection_vector = vec![true; total_rows];
    let copy_len = std::cmp::min(selection_vector.len(), total_rows);
    new_selection_vector[..copy_len].copy_from_slice(&selection_vector[..copy_len]);
    new_selection_vector
}

#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::cast_sign_loss)]
async fn get_parquet_access_plan(
    parquet_file_reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: &PartitionedFile,
    selection_vector: Vec<bool>,
) -> Result<ParquetAccessPlan, datafusion::error::DataFusionError> {
    let mut parquet_file_reader = parquet_file_reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        &ExecutionPlanMetricsSet::new(),
    )?;

    let parquet_metadata = parquet_file_reader.get_metadata(None).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error getting parquet metadata: {e}"
        ))
    })?;

    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for (i, row_group) in parquet_metadata.row_groups().iter().enumerate() {
        // If all rows in the row group are deleted, skip the row group
        tracing::debug!(
            "Row group {i} num_rows={} row_group_row_start={row_group_row_start}",
            row_group.num_rows()
        );
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    tracing::debug!("Created ParquetAccessPlan with {row_groups:?}");
    Ok(ParquetAccessPlan::new(row_groups))
}

/// Convert a `DataFusion` filter expression to a `delta_kernel` expression
#[expect(
    deprecated,
    reason = "Needed to exhaustively match on all expression types"
)]
fn to_delta_kernel_expr(expr: &Expr) -> Option<Expression> {
    match expr {
        Expr::HigherOrderFunction(_) | Expr::Lambda(_) | Expr::LambdaVariable(_) => None,
        Expr::BinaryExpr(binary) => {
            let left = to_delta_kernel_expr(&binary.left)?;
            let right = to_delta_kernel_expr(&binary.right)?;

            Some(to_delta_kernel_binary_expression(binary.op, left, right)?)
        }
        Expr::Column(col) => {
            let field_names = vec![col.name.as_str()];
            Some(Expression::column(field_names))
        }
        Expr::Literal(value, _) => {
            Some(Expression::literal(to_delta_kernel_scalar(value.clone())?))
        }
        Expr::IsNull(expr) => {
            let expr = to_delta_kernel_expr(expr)?;
            Some(Expression::is_null(expr).into())
        }
        Expr::IsNotNull(expr) => {
            let expr = to_delta_kernel_expr(expr)?;
            Some(Expression::is_not_null(expr).into())
        }
        Expr::Not(expr) => {
            let expr = into_predicate(to_delta_kernel_expr(expr)?)?;
            Some(Predicate::not(expr).into())
        }
        Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::ScalarFunction(_)
        | Expr::Alias(_)
        | Expr::ScalarVariable(_, _)
        | Expr::ScalarSubquery(_)
        | Expr::InSubquery(_)
        | Expr::Exists(_)
        | Expr::Wildcard { .. }
        | Expr::Unnest { .. }
        | Expr::SetComparison(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::AggregateFunction { .. }
        | Expr::WindowFunction { .. }
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::GroupingSet(_)
        | Expr::Placeholder(_) => {
            // Other expression types are not supported for Delta kernel predicates
            None
        }
    }
}

fn into_predicate(expr: Expression) -> Option<Predicate> {
    match expr {
        Expression::Predicate(predicate) => Some(*predicate),
        _ => None,
    }
}

fn to_delta_kernel_binary_expression(
    op: Operator,
    left: Expression,
    right: Expression,
) -> Option<Expression> {
    match op {
        Operator::Plus => Some(Expression::binary(BinaryExpressionOp::Plus, left, right)),
        Operator::Minus => Some(Expression::binary(BinaryExpressionOp::Minus, left, right)),
        Operator::Multiply => Some(Expression::binary(
            BinaryExpressionOp::Multiply,
            left,
            right,
        )),
        Operator::Divide => Some(Expression::binary(BinaryExpressionOp::Divide, left, right)),
        Operator::Lt => Some(Predicate::lt(left, right).into()),
        Operator::LtEq => Some(Predicate::le(left, right).into()),
        Operator::Gt => Some(Predicate::gt(left, right).into()),
        Operator::GtEq => Some(Predicate::ge(left, right).into()),
        Operator::Eq => Some(Predicate::eq(left, right).into()),
        Operator::NotEq => Some(Predicate::ne(left, right).into()),
        Operator::And => Some(Predicate::and(into_predicate(left)?, into_predicate(right)?).into()),
        Operator::Or => Some(Predicate::or(into_predicate(left)?, into_predicate(right)?).into()),
        Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch
        | Operator::LikeMatch
        | Operator::ILikeMatch
        | Operator::NotLikeMatch
        | Operator::NotILikeMatch
        | Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft
        | Operator::StringConcat
        | Operator::AtArrow
        | Operator::ArrowAt
        | Operator::Arrow
        | Operator::LongArrow
        | Operator::HashArrow
        | Operator::Modulo
        | Operator::HashLongArrow
        | Operator::AtAt
        | Operator::IntegerDivide
        | Operator::Colon
        | Operator::HashMinus
        | Operator::AtQuestion
        | Operator::Question
        | Operator::QuestionAnd
        | Operator::QuestionPipe => None,
    }
}

#[expect(clippy::cast_sign_loss)]
fn to_delta_kernel_scalar(scalar: ScalarValue) -> Option<Scalar> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(Scalar::Byte(v)),
        ScalarValue::Int8(None) => Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
            PrimitiveType::Byte,
        ))),
        ScalarValue::UInt8(Some(v)) => Some(Scalar::Short(i16::from(v))),
        ScalarValue::Int16(Some(v)) => Some(Scalar::Short(v)),
        ScalarValue::UInt8(None) | ScalarValue::Int16(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Short),
        )),
        ScalarValue::Int32(Some(v)) => Some(Scalar::Integer(v)),
        ScalarValue::UInt16(Some(v)) => Some(Scalar::Integer(i32::from(v))),
        ScalarValue::UInt16(None) | ScalarValue::Int32(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer),
        )),
        ScalarValue::Int64(Some(v)) => Some(Scalar::Long(v)),
        ScalarValue::UInt32(Some(v)) => Some(Scalar::Long(i64::from(v))),
        ScalarValue::UInt64(Some(v)) => {
            if let Ok(v) = i64::try_from(v) {
                Some(Scalar::Long(v))
            } else {
                None // Cannot represent u64 > i64::MAX in delta_kernel
            }
        }
        ScalarValue::UInt64(None) | ScalarValue::UInt32(None) | ScalarValue::Int64(None) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::Long,
            )))
        }
        ScalarValue::Boolean(Some(v)) => Some(Scalar::Boolean(v)),
        ScalarValue::Boolean(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Boolean),
        )),
        ScalarValue::Float16(Some(v)) => Some(Scalar::Float(f32::from(v))),
        ScalarValue::Float32(Some(v)) => Some(Scalar::Float(v)),
        ScalarValue::Float16(None) | ScalarValue::Float32(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Float),
        )),
        ScalarValue::Float64(Some(v)) => Some(Scalar::Double(v)),
        ScalarValue::Float64(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Double),
        )),
        ScalarValue::Decimal128(Some(v), p, s) => Some(Scalar::Decimal(
            DecimalData::try_new(v, DecimalType::try_new(p, s as u8).ok()?).ok()?,
        )),
        ScalarValue::Decimal128(None, p, s) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::Decimal(DecimalType::try_new(p, s as u8).ok()?),
            )))
        }
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Some(Scalar::String(v)),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Some(Scalar::Null(delta_kernel::schema::DataType::Primitive(
                PrimitiveType::String,
            )))
        }
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Some(Scalar::Binary(v)),
        ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::FixedSizeBinary(_, None)
        | ScalarValue::LargeBinary(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Binary),
        )),
        ScalarValue::Date32(Some(v)) => Some(Scalar::Date(v)),
        ScalarValue::Date32(None) | ScalarValue::Date64(None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Date),
        )),
        ScalarValue::Date64(Some(v)) => {
            // Convert milliseconds to days since epoch
            let days = v / (24 * 60 * 60 * 1000);
            if let Ok(days) = i32::try_from(days) {
                Some(Scalar::Date(days))
            } else {
                None
            }
        }
        ScalarValue::TimestampSecond(Some(v), Some(_)) => {
            v.checked_mul(1_000_000).map(Scalar::Timestamp)
        }
        ScalarValue::TimestampSecond(Some(v), None) => {
            v.checked_mul(1_000_000).map(Scalar::TimestampNtz)
        }
        ScalarValue::TimestampMillisecond(Some(v), Some(_)) => {
            v.checked_mul(1000).map(Scalar::Timestamp)
        }
        ScalarValue::TimestampMillisecond(Some(v), None) => {
            v.checked_mul(1000).map(Scalar::TimestampNtz)
        }
        ScalarValue::TimestampMicrosecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v)),
        ScalarValue::TimestampMicrosecond(Some(v), None) => Some(Scalar::TimestampNtz(v)),
        ScalarValue::TimestampNanosecond(Some(v), Some(_)) => Some(Scalar::Timestamp(v / 1000)), // Convert to microseconds
        ScalarValue::TimestampNanosecond(Some(v), None) => Some(Scalar::TimestampNtz(v / 1000)), // Convert to microseconds
        ScalarValue::TimestampSecond(None, Some(_))
        | ScalarValue::TimestampMillisecond(None, Some(_))
        | ScalarValue::TimestampMicrosecond(None, Some(_))
        | ScalarValue::TimestampNanosecond(None, Some(_)) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::Timestamp),
        )),
        ScalarValue::TimestampSecond(None, None)
        | ScalarValue::TimestampMillisecond(None, None)
        | ScalarValue::TimestampMicrosecond(None, None)
        | ScalarValue::TimestampNanosecond(None, None) => Some(Scalar::Null(
            delta_kernel::schema::DataType::Primitive(PrimitiveType::TimestampNtz),
        )),
        ScalarValue::Null
        | ScalarValue::Decimal256(_, _, _)
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::ListView(_)
        | ScalarValue::LargeListView(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Time32Second(_)
        | ScalarValue::Time32Millisecond(_)
        | ScalarValue::Time64Microsecond(_)
        | ScalarValue::Time64Nanosecond(_)
        | ScalarValue::IntervalYearMonth(_)
        | ScalarValue::IntervalDayTime(_)
        | ScalarValue::IntervalMonthDayNano(_)
        | ScalarValue::DurationSecond(_)
        | ScalarValue::DurationMillisecond(_)
        | ScalarValue::DurationMicrosecond(_)
        | ScalarValue::DurationNanosecond(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::Dictionary(_, _)
        | ScalarValue::RunEndEncoded(_, _, _)
        | ScalarValue::Decimal32(_, _, _)
        | ScalarValue::Decimal64(_, _, _) => None,
    }
}

/// Convert a list of `DataFusion` filter expressions to a single `delta_kernel` expression
///
/// This function processes multiple `DataFusion` expressions and returns a predicate for `delta_kernel`.
fn filters_to_delta_kernel_predicate(filters: &[Expr]) -> Option<Predicate> {
    if filters.is_empty() {
        return None;
    }

    let mut predicates = Vec::new();
    for filter in filters {
        if let Some(expr) = to_delta_kernel_expr(filter) {
            predicates.push(expr);
        }
    }

    if predicates.is_empty() {
        None
    } else if predicates.len() == 1 {
        let expr = predicates.pop()?;
        Some(into_predicate(expr)?)
    } else {
        // Multiple predicates are present, so we need to combine them using an AND operation
        let predicates = predicates
            .into_iter()
            .filter_map(into_predicate)
            .collect::<Vec<_>>();
        Some(Predicate::and_from(predicates))
    }
}

fn handle_delta_error(delta_error: delta_kernel::Error) -> Error {
    match delta_error {
        delta_kernel::Error::InvalidCheckpoint(_) => Error::DeltaCheckpointError {
            source: delta_error,
        },
        _ => Error::DeltaTableError {
            source: delta_error,
        },
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::Fields;
    use datafusion::logical_expr::{Operator, col, lit, not};
    use datafusion::parquet::arrow::arrow_reader::RowSelector;

    use super::*;

    /// The Delta column mapping's physical/logical pair for a struct column, as
    /// [`map_delta_data_type_to_arrow_data_type`] would render one Delta schema in both modes:
    /// same field order, physical ids in one and logical names in the other.
    fn struct_column_mapping() -> (DataType, DataType) {
        let physical = DataType::Struct(Fields::from(vec![
            Field::new("col-1", DataType::Int32, true),
            Field::new("col-2", DataType::Int32, true),
        ]));
        let logical = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        (physical, logical)
    }

    fn test_table_url() -> Url {
        Url::parse("s3://bucket/table/").expect("test table url should parse")
    }

    /// A struct whose children are `col-2` then `col-1`, holding `b`'s values then `a`'s — the
    /// scan output whose order disagrees with the order the Delta schema declares.
    fn reordered_source() -> (DataType, StructArray) {
        let fields = Fields::from(vec![
            Field::new("col-2", DataType::Int32, true),
            Field::new("col-1", DataType::Int32, true),
        ]);
        let array = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![20, 21])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 11])) as ArrayRef,
            ],
            None,
        );
        (DataType::Struct(fields), array)
    }

    fn int32_column(array: &StructArray, name: &str) -> Vec<i32> {
        array
            .column_by_name(name)
            .unwrap_or_else(|| panic!("relabelled struct should expose a field named '{name}'"))
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("field should still be Int32")
            .values()
            .to_vec()
    }

    #[test]
    fn column_mapping_target_reproduces_the_logical_type_when_the_orders_agree() {
        let (physical, logical) = struct_column_mapping();

        let target =
            logical_target_in_source_order(&physical, &physical, &logical, &test_table_url(), "s")
                .expect("a scan in the declared order should need no reordering");

        assert_eq!(
            target, logical,
            "a source already in the declared order must rebuild the logical type exactly, so \
             every table whose files match its schema is unaffected"
        );
    }

    #[test]
    fn column_mapping_target_follows_the_scan_order_for_a_reordered_struct() {
        let (physical, logical) = struct_column_mapping();
        let (source, _) = reordered_source();

        let target =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect("a same-typed reorder should be resolved, not refused");

        let expected = DataType::Struct(Fields::from(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]));
        assert_eq!(
            target, expected,
            "the target must name the scan's first child 'b' — the logical name of the physical \
             field 'col-2' it actually holds — rather than the schema's first name 'a'"
        );
    }

    /// The regression this fix exists for, exercised through the production relabel: the target
    /// built the old way (the logical type verbatim) publishes each child under its sibling's
    /// name, and the target built the new way does not.
    #[test]
    fn column_mapping_relabel_keeps_values_with_their_own_names_across_a_reorder() {
        let (physical, logical) = struct_column_mapping();
        let (source, array) = reordered_source();

        // The old target: the logical type as declared. `relabel_array_data` pairs children
        // positionally, so it accepts this and transposes the two columns.
        let transposed = make_array(
            relabel_array_data(array.to_data(), &logical)
                .expect("a same-typed reorder is accepted, which is the defect"),
        );
        let transposed = transposed
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("relabelled value should still be a struct");
        assert_eq!(
            int32_column(transposed, "a"),
            vec![20, 21],
            "guards the premise: relabelling to the declared type publishes b's values as 'a'"
        );

        // The new target, built in the scan's own order.
        let target =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect("a same-typed reorder should be resolved, not refused");
        let relabelled = make_array(
            relabel_array_data(array.to_data(), &target).expect("the reordered target should hold"),
        );
        let relabelled = relabelled
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("relabelled value should still be a struct");

        assert_eq!(
            int32_column(relabelled, "a"),
            vec![10, 11],
            "column 'a' must carry the values written for 'a'"
        );
        assert_eq!(
            int32_column(relabelled, "b"),
            vec![20, 21],
            "column 'b' must carry the values written for 'b'"
        );
    }

    #[test]
    fn column_mapping_target_refuses_a_nested_field_the_mapping_does_not_name() {
        let (physical, logical) = struct_column_mapping();
        let source = DataType::Struct(Fields::from(vec![
            Field::new("col-1", DataType::Int32, true),
            Field::new("col-9", DataType::Int32, true),
        ]));

        let err =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect_err("an unmapped nested field must not be paired positionally");

        let message = err.to_string();
        assert!(
            message.contains("'col-9'") && message.contains("column 's'"),
            "the refusal must name the unmapped field and its column: {message}"
        );
        assert!(
            message.contains("s3://bucket/table/"),
            "the refusal must name the table: {message}"
        );
    }

    #[test]
    fn column_mapping_target_refuses_two_nested_fields_of_the_same_name() {
        let (physical, logical) = struct_column_mapping();
        let source = DataType::Struct(Fields::from(vec![
            Field::new("col-1", DataType::Int32, true),
            Field::new("col-1", DataType::Int32, true),
        ]));

        let err =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect_err("two fields claiming one logical name must be refused");

        assert!(
            err.to_string().contains("two nested fields named 'col-1'"),
            "the refusal must say which name is duplicated: {err}"
        );
    }

    #[test]
    fn column_mapping_target_refuses_a_struct_whose_field_count_disagrees() {
        let (physical, logical) = struct_column_mapping();
        let source = DataType::Struct(Fields::from(vec![Field::new(
            "col-1",
            DataType::Int32,
            true,
        )]));

        let err =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect_err("a struct the mapping does not describe must be refused");

        assert!(
            err.to_string().contains("1 field(s)") && err.to_string().contains("names 2"),
            "the refusal must give both counts: {err}"
        );
    }

    #[test]
    fn column_mapping_target_reorders_inside_a_list() {
        let (physical_item, logical_item) = struct_column_mapping();
        let (source_item, _) = reordered_source();

        let source = DataType::List(Arc::new(Field::new("item", source_item, true)));
        let physical = DataType::List(Arc::new(Field::new("item", physical_item, true)));
        let logical = DataType::List(Arc::new(Field::new("item", logical_item, true)));

        let target =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect("a reorder under a list should be resolved");

        let expected = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ])),
            true,
        )));
        assert_eq!(
            target, expected,
            "a struct nested under a list must be reordered too — the relabel pairs positionally \
             at every level, not just the top"
        );
    }

    #[test]
    fn column_mapping_target_reorders_inside_a_map_value() {
        let (physical_value, logical_value) = struct_column_mapping();
        let (source_value, _) = reordered_source();

        let entries = |value: DataType| {
            Arc::new(Field::new_struct(
                "key_value",
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", value, true)),
                ],
                false,
            ))
        };
        let source = DataType::Map(entries(source_value), false);
        let physical = DataType::Map(entries(physical_value), false);
        let logical = DataType::Map(entries(logical_value), false);

        let target =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect("a reorder under a map value should be resolved");

        let expected = DataType::Map(
            entries(DataType::Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ]))),
            false,
        );
        assert_eq!(
            target, expected,
            "a struct nested under a map value must be reordered too"
        );
    }

    /// A `LargeList` stands in for any child-bearing type the Delta mapper cannot build, so this
    /// walk does not descend into it. Taking `logical` whole is only sound when `source` already
    /// spells its names the way `physical` does.
    #[test]
    fn column_mapping_target_takes_an_unwalked_type_whole_when_its_names_already_agree() {
        let physical = DataType::LargeList(Arc::new(Field::new("col-1", DataType::Int32, true)));
        let logical = DataType::LargeList(Arc::new(Field::new("a", DataType::Int32, true)));

        let target =
            logical_target_in_source_order(&physical, &physical, &logical, &test_table_url(), "s")
                .expect("an unwalked node whose names agree carries no reordering question");

        assert_eq!(
            target, logical,
            "a node this walk does not descend into must still be relabelled as it was before"
        );
    }

    #[test]
    fn column_mapping_target_refuses_an_unwalked_type_whose_names_differ() {
        let source = DataType::LargeList(Arc::new(Field::new("col-2", DataType::Int32, true)));
        let physical = DataType::LargeList(Arc::new(Field::new("col-1", DataType::Int32, true)));
        let logical = DataType::LargeList(Arc::new(Field::new("a", DataType::Int32, true)));

        let err =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect_err("an unwalked node holding an unmatched name must not be relabelled");

        assert!(
            err.to_string().contains("column 's'"),
            "the refusal must name the column: {err}"
        );
    }

    /// The `sorted` flag is a different question from field order, so a disagreement over it must
    /// not cost the reordering of the value beneath it.
    #[test]
    fn column_mapping_target_reorders_a_map_value_whose_sorted_flag_disagrees() {
        let (physical_value, logical_value) = struct_column_mapping();
        let (source_value, _) = reordered_source();

        let entries = |value: DataType| {
            Arc::new(Field::new_struct(
                "key_value",
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", value, true)),
                ],
                false,
            ))
        };
        let source = DataType::Map(entries(source_value), true);
        let physical = DataType::Map(entries(physical_value), false);
        let logical = DataType::Map(entries(logical_value), false);

        let target =
            logical_target_in_source_order(&source, &physical, &logical, &test_table_url(), "s")
                .expect("a sorted-flag disagreement is not this function's to refuse");

        let expected = DataType::Map(
            entries(DataType::Struct(Fields::from(vec![
                Field::new("b", DataType::Int32, true),
                Field::new("a", DataType::Int32, true),
            ]))),
            false,
        );
        assert_eq!(
            target, expected,
            "the value must still be reordered, and the flag left as the table declares it for \
             `relabel_array_data` to rule on"
        );
    }

    /// Every refusal is a user-facing message, so it stays on one line and the `\` continuations
    /// that keep the source readable must join with single spaces rather than swallow one.
    #[test]
    fn refusals_stay_on_one_line() {
        let message = unmatched_nested_field(&test_table_url(), "s", "holds something unmappable")
            .to_string();

        assert!(
            !message.contains('\n'),
            "a user-facing message must not embed a newline: {message:?}"
        );
        assert!(
            message.contains(
                "column 's' holds something unmappable, so its fields cannot be matched to their \
                 names and the column would be read with its values under the wrong ones."
            ),
            "the continuations must join with single spaces: {message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/components/data-connectors/delta-lake"),
            "the refusal must carry the docs link: {message}"
        );
    }

    /// The names in a refusal come out of the table, so the one-line guarantee above has to hold
    /// for a schema that names a column with a newline in it rather than only for the names a
    /// test picks.
    #[test]
    fn a_name_the_table_chose_cannot_split_a_refusal() {
        let message = unmatched_nested_field(
            &test_table_url(),
            "two\nlines",
            "holds a nested field 'also\rsplit' that the table's column mapping does not name",
        )
        .to_string();

        assert!(
            !message.contains('\n') && !message.contains('\r'),
            "a name the table chose must not split the refusal: {message:?}"
        );
        assert!(
            message.contains(r"column 'two\nlines'"),
            "the column must still be named, with the break escaped in place: {message}"
        );
        assert!(
            message.contains(r"nested field 'also\rsplit'"),
            "the nested name must still be named, with the break escaped in place: {message}"
        );
    }

    /// Escaping rather than replacing is what makes the rendered name identify one column: an
    /// operator reads this message to find the column in their own schema, so two columns that
    /// differ only in a control character must not render the same way.
    #[test]
    fn two_names_differing_only_in_a_control_character_render_differently() {
        let tabbed =
            unmatched_nested_field(&test_table_url(), "a\tb", "holds something unmappable")
                .to_string();
        let spaced = unmatched_nested_field(&test_table_url(), "a b", "holds something unmappable")
            .to_string();

        assert_ne!(
            tabbed, spaced,
            "`a\\tb` and `a b` are different columns and must not produce the same refusal"
        );
        assert!(
            tabbed.contains(r"column 'a\tb'"),
            "the tab must survive as an escape rather than becoming a space: {tabbed}"
        );
    }

    /// A schema is free to name a column with a megabyte of text. One refusal must stay a message
    /// someone can read, not a copy of that name.
    #[test]
    fn a_name_longer_than_the_cap_is_truncated() {
        let message = unmatched_nested_field(
            &test_table_url(),
            &"x".repeat(MAX_RENDERED_CHARS * 4),
            "holds something unmappable",
        )
        .to_string();

        assert!(
            message.chars().count() < MAX_RENDERED_CHARS * 3,
            "an oversized name must not be copied into the refusal whole: {} chars",
            message.chars().count()
        );
        assert!(
            message.contains('\u{2026}'),
            "a truncated name must say it was truncated: {message}"
        );
    }

    #[test]
    #[expect(clippy::similar_names)]
    fn test_to_delta_kernel_expr() {
        // Test basic column reference
        let col_expr = col("name");
        let dk_expr = to_delta_kernel_expr(&col_expr);
        assert!(dk_expr.is_some(), "Column expression should be supported");

        // Test basic literal
        let lit_expr = lit("value");
        let dk_expr = to_delta_kernel_expr(&lit_expr);
        assert!(dk_expr.is_some(), "Literal expression should be supported");

        // Test comparison operators
        // Equality
        let eq_expr = col("age").eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&eq_expr);
        assert!(dk_expr.is_some(), "Equality expression should be supported");

        // Less than
        let lt_expr = col("age").lt(lit(30));
        let dk_expr = to_delta_kernel_expr(&lt_expr);
        assert!(
            dk_expr.is_some(),
            "Less than expression should be supported"
        );

        // Greater than
        let gt_expr = col("age").gt(lit(30));
        let dk_expr = to_delta_kernel_expr(&gt_expr);
        assert!(
            dk_expr.is_some(),
            "Greater than expression should be supported"
        );

        // Less than or equal
        let lte_expr = col("age").lt_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&lte_expr);
        assert!(
            dk_expr.is_some(),
            "Less than or equal expression should be supported"
        );

        // Greater than or equal
        let gte_expr = col("age").gt_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&gte_expr);
        assert!(
            dk_expr.is_some(),
            "Greater than or equal expression should be supported"
        );

        // Not equal
        let neq_expr = col("age").not_eq(lit(30));
        let dk_expr = to_delta_kernel_expr(&neq_expr);
        assert!(
            dk_expr.is_some(),
            "Not equal expression should be supported"
        );

        // Test arithmetic operators using binary expressions directly
        // Addition
        let add_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Plus,
            Box::new(lit(5)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(add_expr));
        assert!(dk_expr.is_some(), "Addition expression should be supported");

        // Subtraction
        let sub_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Minus,
            Box::new(lit(5)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(sub_expr));
        assert!(
            dk_expr.is_some(),
            "Subtraction expression should be supported"
        );

        // Multiplication
        let mul_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Multiply,
            Box::new(lit(2)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(mul_expr));
        assert!(
            dk_expr.is_some(),
            "Multiplication expression should be supported"
        );

        // Division
        let div_expr = datafusion::logical_expr::BinaryExpr::new(
            Box::new(col("age")),
            Operator::Divide,
            Box::new(lit(2)),
        );
        let dk_expr = to_delta_kernel_expr(&Expr::BinaryExpr(div_expr));
        assert!(dk_expr.is_some(), "Division expression should be supported");

        // Test null check
        let is_null_expr = col("optional_field").is_null();
        let dk_expr = to_delta_kernel_expr(&is_null_expr);
        assert!(dk_expr.is_some(), "IsNull expression should be supported");

        // Test is_not_null
        let is_not_null_expr = col("required_field").is_not_null();
        let dk_expr = to_delta_kernel_expr(&is_not_null_expr);
        assert!(
            dk_expr.is_some(),
            "IsNotNull expression should be supported"
        );

        // Test NOT expression
        let not_expr = not(col("active").eq(lit(false)));
        let dk_expr = to_delta_kernel_expr(&not_expr);
        assert!(dk_expr.is_some(), "Not expression should be supported");

        // Test unsupported expressions
        let case_expr = datafusion::logical_expr::case(col("status"))
            .when(lit("active"), lit(1))
            .otherwise(lit(0))
            .expect("Failed to create case expression");
        let dk_expr = to_delta_kernel_expr(&case_expr);
        assert!(
            dk_expr.is_none(),
            "CASE expressions should not be supported"
        );

        let in_list_expr = datafusion::logical_expr::in_list(
            col("status"),
            vec![lit("active"), lit("pending")],
            false,
        );
        let dk_expr = to_delta_kernel_expr(&in_list_expr);
        assert!(
            dk_expr.is_none(),
            "IN LIST expressions should not be supported"
        );

        let alias_expr = col("age").alias("years");
        let dk_expr = to_delta_kernel_expr(&alias_expr);
        assert!(
            dk_expr.is_none(),
            "ALIAS expressions should not be supported"
        );
    }

    #[test]
    fn test_to_delta_kernel_scalar() {
        // Test string scalar
        let scalar = ScalarValue::Utf8(Some("test".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert string scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "test"));

        // Test other string types
        let scalar = ScalarValue::Utf8View(Some("test_view".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Utf8View scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "test_view"));

        let scalar = ScalarValue::LargeUtf8(Some("large_test".to_string()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert LargeUtf8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::String(s) if s == "large_test"));

        // Test integer scalars
        let scalar = ScalarValue::Int8(Some(8));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Byte(v) if v == 8));

        let scalar = ScalarValue::Int16(Some(16));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int16 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Short(v) if v == 16));

        let scalar = ScalarValue::Int32(Some(32));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Integer(v) if v == 32));

        let scalar = ScalarValue::Int64(Some(64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 64));

        // Test unsigned integer conversion
        let scalar = ScalarValue::UInt8(Some(8));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt8 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Short(v) if v == 8));

        let scalar = ScalarValue::UInt16(Some(16));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt16 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Integer(v) if v == 16));

        let scalar = ScalarValue::UInt32(Some(32));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 32));

        let scalar = ScalarValue::UInt64(Some(64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert UInt64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == 64));

        // Test large UInt64 conversion (edge case)
        let max_i64 = i64::MAX as u64;
        let scalar = ScalarValue::UInt64(Some(max_i64));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert max UInt64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Long(v) if v == i64::MAX));

        // Test UInt64 that's too large to fit in i64 (should return None)
        let too_large = (i64::MAX as u64) + 1;
        let scalar = ScalarValue::UInt64(Some(too_large));
        let dk_scalar = to_delta_kernel_scalar(scalar);
        assert!(dk_scalar.is_none());

        // Test float scalars without Float16 (not available in this crate)
        let scalar = ScalarValue::Float32(Some(32.5));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Float32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Float(v) if (v - 32.5).abs() < f32::EPSILON));

        let scalar = ScalarValue::Float64(Some(64.5));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Float64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Double(v) if (v - 64.5).abs() < f64::EPSILON));

        // Test boolean scalar
        let scalar = ScalarValue::Boolean(Some(true));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Boolean scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Boolean(v) if v));

        // Test null scalars
        let scalar = ScalarValue::Int32(None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Int32 null scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Null(dt) if matches!(dt, delta_kernel::schema::DataType::Primitive(PrimitiveType::Integer)))
        );

        let scalar = ScalarValue::Utf8(None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Utf8 null scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Null(dt) if matches!(dt, delta_kernel::schema::DataType::Primitive(PrimitiveType::String)))
        );

        // Test timestamp scalar with different time units
        let scalar = ScalarValue::TimestampSecond(Some(10), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampSecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 10_000_000)); // Converted to microseconds

        let scalar = ScalarValue::TimestampMillisecond(Some(10_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampMillisecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 10_000_000)); // Converted to microseconds

        let scalar = ScalarValue::TimestampMicrosecond(Some(1_000_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampMicrosecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 1_000_000));

        let scalar = ScalarValue::TimestampNanosecond(Some(1_000_000_000), None);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert TimestampNanosecond scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::TimestampNtz(v) if v == 1_000_000)); // Converted to microseconds

        // Test timestamp with timezone
        let scalar = ScalarValue::TimestampMicrosecond(Some(1_000_000), Some("UTC".into()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Timestamp with timezone scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Timestamp(v) if v == 1_000_000));

        // Test decimal scalar
        let scalar = ScalarValue::Decimal128(Some(1234), 10, 2);
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Decimal128 scalar to delta kernel scalar");
        assert!(
            matches!(dk_scalar, Scalar::Decimal(v) if v == DecimalData::try_new(1234, DecimalType::try_new(10, 2).expect("valid decimal")).expect("valid decimal"))
        );

        // Test binary data
        let binary_data = vec![1, 2, 3, 4];
        let scalar = ScalarValue::Binary(Some(binary_data.clone()));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Binary scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Binary(v) if v == binary_data));

        // Test Date32
        let scalar = ScalarValue::Date32(Some(18000)); // Some number of days since epoch
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Date32 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Date(v) if v == 18000));

        // Test Date64
        let days = 100;
        let millis = i64::from(days) * 24 * 60 * 60 * 1000;
        let scalar = ScalarValue::Date64(Some(millis));
        let dk_scalar = to_delta_kernel_scalar(scalar)
            .expect("Failed to convert Date64 scalar to delta kernel scalar");
        assert!(matches!(dk_scalar, Scalar::Date(v) if v == days));

        // Test unsupported types (we don't need to test the exact construction since we only care about the return value)
        let dk_scalar = to_delta_kernel_scalar(ScalarValue::Null);
        assert!(dk_scalar.is_none());
    }

    #[test]
    fn test_filters_to_delta_kernel_expr() {
        // Test empty filters
        let filters: Vec<Expr> = vec![];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_none(), "Empty filters should return None");

        // Test single filter (equality)
        let filters = vec![col("age").eq(lit(30))];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_some(), "Single filter should be converted");

        // Test multiple filters
        let filters = vec![col("age").gt(lit(20)), col("name").eq(lit("John"))];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Multiple filters should be converted to a single expression"
        );

        // Test filters with unsupported expressions
        let case_expr = datafusion::logical_expr::case(col("status"))
            .when(lit("active"), lit(1))
            .otherwise(lit(0))
            .expect("Failed to create case expression for unsupported expressions test");

        let filters = vec![col("age").gt(lit(20)), case_expr.clone()];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Mix of supported and unsupported filters should return the supported ones"
        );

        // Test filters with only unsupported expressions
        let filters = vec![case_expr.clone()];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_none(),
            "Only unsupported filters should return None"
        );

        // Test with multiple unsupported expressions
        let filters = vec![
            case_expr,
            datafusion::logical_expr::in_list(
                col("status"),
                vec![lit("active"), lit("pending")],
                false,
            ),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_none(),
            "Multiple unsupported filters should return None"
        );

        // Test AND variadic operator
        let filters = vec![
            col("age").gt(lit(20)).and(col("name").eq(lit("John"))),
            col("active").eq(lit(true)),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "AND variadic operator should be supported"
        );

        // Test OR variadic operator
        let filters = vec![
            col("age").gt(lit(20)).or(col("name").eq(lit("John"))),
            col("active").eq(lit(true)),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "OR variadic operator should be supported"
        );

        // Test nested variadic operators
        let filters = vec![
            col("age")
                .gt(lit(20))
                .and(col("name").eq(lit("John")))
                .or(col("active").eq(lit(true))),
        ];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Nested variadic operators should be supported"
        );
    }

    #[test]
    fn test_complex_filters_to_delta_kernel_expr() {
        // Test simple comparison expressions
        let filter = col("category").eq(lit("electronics"));
        let filters = vec![filter];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(
            dk_expr.is_some(),
            "Simple equality expression should be supported"
        );

        // Test NOT expressions
        let filter = not(col("deleted").eq(lit(true)));
        let filters = vec![filter];
        let dk_expr = filters_to_delta_kernel_predicate(&filters);
        assert!(dk_expr.is_some(), "NOT expression should be supported");
    }

    #[test]
    fn test_get_row_group_access() {
        // Test case where all rows are selected (should use Scan)
        let selection_vector = &[true, true, true, true, true];
        let row_group_row_start = 0;
        let row_group_num_rows = 5;
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);
        assert_eq!(row_group_access, RowGroupAccess::Scan);

        // Test case where all rows are deleted (should use Skip)
        let selection_vector = &[false, false, false, false, false];
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);
        assert_eq!(row_group_access, RowGroupAccess::Skip);

        // Test case with mixed selection (should use Selection)
        let selection_vector = &[true, true, true, false, true];
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        // Expected selection should have:
        // - Select first 3 rows
        // - Skip 1 row
        // - Select 1 row
        let selectors = vec![
            RowSelector::select(3),
            RowSelector::skip(1),
            RowSelector::select(1),
        ];
        assert_eq!(
            row_group_access,
            RowGroupAccess::Selection(selectors.into())
        );
    }

    #[test]
    fn test_get_row_group_access_with_offset() {
        // Test with offset starting row
        // Full selection vector: [true, true, true, true, true, false, false, false, true, true]
        let selection_vector = &[
            true, true, true, true, true, false, false, false, true, true,
        ];
        let row_group_row_start = 5; // Start at index 5
        let row_group_num_rows = 5; // Take 5 rows (5-9)

        // The selection should consider rows 5-9: [false, false, false, true, true]
        let row_group_access =
            get_row_group_access(selection_vector, row_group_row_start, row_group_num_rows);

        // Expected selectors:
        // - Skip first 3 rows (false, false, false)
        // - Select last 2 rows (true, true)
        let selectors = vec![RowSelector::skip(3), RowSelector::select(2)];
        assert_eq!(
            row_group_access,
            RowGroupAccess::Selection(selectors.into())
        );
    }

    #[test]
    fn test_get_full_selection_vector() {
        // Test expanding a shorter selection vector to a longer one
        let selection_vector = &[true, false, true];
        let total_rows = 5;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should copy the provided values and fill the rest with true
        assert_eq!(full_vector, vec![true, false, true, true, true]);

        // Test truncating a longer selection vector to a shorter one
        let selection_vector = &[true, false, true, false, true];
        let total_rows = 3;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should only copy the first 3 values
        assert_eq!(full_vector, vec![true, false, true]);

        // Test with empty selection vector
        let selection_vector = &[];
        let total_rows = 3;
        let full_vector = get_full_selection_vector(selection_vector, total_rows);

        // Should create a vector of all true values
        assert_eq!(full_vector, vec![true, true, true]);
    }

    #[test]
    fn test_get_table_location() {
        // Test path with trailing slash (should remain unchanged)
        assert_eq!(
            ensure_folder_location("s3://my_bucket/".to_string()),
            "s3://my_bucket/"
        );

        // Test path without trailing slash (should add slash)
        assert_eq!(
            ensure_folder_location("s3://my_bucket".to_string()),
            "s3://my_bucket/"
        );

        // Test path with nested folders
        assert_eq!(
            ensure_folder_location("s3://my_bucket/data/table".to_string()),
            "s3://my_bucket/data/table/"
        );
    }
}
