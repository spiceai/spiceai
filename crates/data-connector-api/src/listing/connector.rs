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

use crate::parameters::ConnectorContext;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_tools::schema::expand_views_schema;
use async_trait::async_trait;
use dataformat_json::{Format, SpiceJsonFormat};
use dataformat_orc::OrcFormat;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DFSchema, GetExt, Result as DFResult, ScalarValue};
use datafusion::config::{ConfigField, TableParquetOptions};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::{
    FileFormat, csv::CsvFormat, file_compression_type::FileCompressionType, json::JsonFormat,
    parquet::ParquetFormat,
};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::cache::file_statistics_cache::DefaultFileStatisticsCache;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{FileExtensions, PartitionedFile, TableSchema};
use futures::TryStreamExt;
use object_store::client::{HttpError, HttpErrorKind};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use snafu::prelude::*;
use std::time::Duration;
use url::Url;
#[cfg(not(windows))]
use {
    datafusion_datasource::file_format::FileFormatFactory, vortex_datafusion::VortexFormatFactory,
};

use crate::accelerated::RegisteredAcceleratedTable;
use crate::{
    ConnectorComponent, DataConnector, DataConnectorError, DataConnectorResult,
    listing::infer::{infer_partitions_with_types_from_files, infer_partitions_with_types_prefix},
};
use app::App;
use data_components::object::{
    metadata::{MetadataColumn, ObjectStoreMetadataTable},
    text::ObjectStoreTextTable,
};
use runtime_component::dataset::DatasetSpec;
use runtime_parameters::{ExposedParamLookup, Parameters};

use super::{
    DelimitedFormat, ParsedFileExtension, detect_file_extension_from_path,
    detect_file_extension_from_url_or_path, parse_file_extension_param,
};
use crate::DataConnectorError::SchemaMismatch;
use runtime_datafusion::session_config::get_df_default_config;
use runtime_object_store::registry::default_runtime_env;

/// Maximum number of files to scan when validating that the schema source path contains objects with the expected extension.
const SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT: usize = 10_000;

/// Maximum matching `ORC` objects whose footers are merged when inferring a
/// collection schema. The default listing path otherwise passes only the
/// newest object to [`FileFormat::infer_schema`], which drops columns that
/// appear only in older files. Scan-time NULL backfill can restore those
/// columns only when the merged schema already lists them. Exceeding this
/// cap is an error rather than a silent truncation, because a later scan
/// reads every matching object. A narrower inference-only prefix would
/// publish an incomplete schema for that full scan.
const ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT: usize = 10_000;

#[derive(Clone, Debug)]
/// Wraps a `ListingTable` to short-circuit broad object-store listings when a
/// query filters on listing metadata, and to apply format-selected Hive
/// listing (`*.orc` / `*.parquet`) so extensionless data objects are scanned
/// without picking up job-marker files.
///
/// Two metadata fast-paths avoid opening files that a predicate already
/// excludes from the object-store listing alone:
/// - a `_location` predicate heads only the named objects (see
///   [`extract_location_predicates`]);
/// - a `_last_modified` bound (e.g. an `append` refresh's
///   `_last_modified > <watermark>`) keeps only the objects whose
///   [`ObjectMeta::last_modified`] satisfies the bound (see
///   [`extract_last_modified_predicate`]), so a refresh with no new data does
///   work proportional to new files rather than re-reading every object.
struct MetadataPruningListingTable {
    inner: Arc<ListingTable>,
    object_store: Arc<dyn ObjectStore>,
    table_path: ListingTableUrl,
    /// The original file schema from the `ListingTable`, containing only columns
    /// physically stored in data files. This must be stored separately because
    /// `ListingTable` doesn't expose its `file_schema` field publicly, and we cannot
    /// reliably reconstruct it from `table_schema` when partition columns also
    /// appear in the file (causing duplicates in `table_schema`).
    file_schema: SchemaRef,
    /// Listing extension from [`ListingTableConnector::get_file_format_and_extension`].
    /// Format-selected values (`*.orc`, `*.parquet`) list through
    /// [`file_matches_extension`] instead of `DataFusion`'s suffix filter.
    listing_extension: String,
}

impl MetadataPruningListingTable {
    fn new(
        inner: Arc<ListingTable>,
        object_store: Arc<dyn ObjectStore>,
        table_path: ListingTableUrl,
        file_schema: SchemaRef,
        listing_extension: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            object_store,
            table_path,
            file_schema,
            listing_extension: listing_extension.into(),
        }
    }

    fn uses_format_selected_listing(&self) -> bool {
        format_selected_data_suffix(&self.listing_extension).is_some()
    }

    fn partition_column_types(&self) -> &[(String, datafusion::arrow::datatypes::DataType)] {
        &self.inner.options().table_partition_cols
    }

    fn object_store_url(&self) -> ObjectStoreUrl {
        // Safe: Listing tables share object store across paths. Should always have at least one path.
        self.inner.table_paths().first().map_or_else(
            || unreachable!("ListingTable should always contain at least one path"),
            ListingTableUrl::object_store,
        )
    }

    fn file_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.file_schema)
    }

    fn collect_partition_values(&self, meta: &ObjectMeta) -> DFResult<Vec<ScalarValue>> {
        let parts = parse_partition_values(
            &self.table_path,
            &meta.location,
            self.partition_column_types(),
        )?;

        let mut values = Vec::with_capacity(self.partition_column_types().len());
        for (value, (name, dtype)) in parts.into_iter().zip(self.partition_column_types()) {
            match ScalarValue::try_from_string(value.clone(), dtype) {
                Ok(scalar) => values.push(scalar),
                Err(_) => {
                    return Err(DataFusionError::Configuration(
                        hive_partition_value_type_error(
                            meta.location.as_ref(),
                            name,
                            &value,
                            dtype,
                        ),
                    ));
                }
            }
        }
        Ok(values)
    }

    fn partitioned_file_for_meta(&self, meta: ObjectMeta) -> DFResult<PartitionedFile> {
        let partition_values = if self.partition_column_types().is_empty() {
            Vec::new()
        } else {
            self.collect_partition_values(&meta)?
        };
        Ok(PartitionedFile {
            object_meta: meta,
            partition_values,
            range: None,
            statistics: None,
            extensions: FileExtensions::new(),
            metadata_size_hint: None,
            ordering: None,
            table_reference: None,
        })
    }

    async fn format_selected_listing_files(
        &self,
        state: &dyn Session,
    ) -> DFResult<Vec<PartitionedFile>> {
        let mut file_stream = self
            .table_path
            .list_all_files(state, self.object_store.as_ref(), "")
            .await?;

        let mut files: Vec<PartitionedFile> = Vec::new();
        while let Some(meta) = file_stream.try_next().await? {
            if !file_matches_extension(&meta.location, &self.listing_extension) {
                continue;
            }
            files.push(self.partitioned_file_for_meta(meta)?);
        }
        Ok(files)
    }

    async fn scan_format_selected_listing(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        state.runtime_env().register_object_store(
            self.object_store_url().as_ref(),
            Arc::clone(&self.object_store),
        );

        let files = self.format_selected_listing_files(state).await?;
        self.scan_partitioned_files(state, files, projection, limit)
            .await
    }

    /// Lists the objects under the table path and keeps only those whose
    /// [`ObjectMeta::last_modified`] satisfies every `_last_modified` bound,
    /// then scans the survivors.
    ///
    /// Each object's `_last_modified` value is a single timestamp known from
    /// the listing (`meta.last_modified.timestamp_micros()`), so a bound prunes
    /// the file exactly — an excluded object cannot contain a row that passes
    /// the residual `FilterExec`. The comparison is done in nanoseconds to match
    /// how `DataFusion` materializes the metadata column
    /// (`Timestamp(Microsecond, "UTC")`, cast to nanoseconds by the refresh
    /// predicate); scaling both sides by 1000 preserves the ordering.
    async fn scan_last_modified_pruned(
        &self,
        state: &dyn Session,
        bounds: &[LastModifiedBound],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        state.runtime_env().register_object_store(
            self.object_store_url().as_ref(),
            Arc::clone(&self.object_store),
        );

        let mut file_stream = self
            .table_path
            .list_all_files(state, self.object_store.as_ref(), "")
            .await?;

        let mut files: Vec<PartitionedFile> = Vec::new();
        while let Some(meta) = file_stream.try_next().await? {
            if !file_matches_extension(&meta.location, &self.listing_extension) {
                continue;
            }
            if !last_modified_meta_passes(&meta, bounds) {
                continue;
            }
            files.push(self.partitioned_file_for_meta(meta)?);
        }

        self.scan_partitioned_files(state, files, projection, limit)
            .await
    }

    async fn scan_partitioned_files(
        &self,
        state: &dyn Session,
        files: Vec<PartitionedFile>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        if files.is_empty() {
            let schema = if let Some(proj) = projection {
                Arc::new(self.schema().project(proj)?)
            } else {
                self.schema()
            };
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        let file_groups = vec![FileGroup::new(files)];
        let partition_fields: Vec<Field> = self
            .partition_column_types()
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();

        let table_schema = TableSchema::new(
            self.file_schema(),
            partition_fields
                .iter()
                .map(|f| Arc::new(f.clone()))
                .collect(),
        );
        let file_source = self.inner.options().format.file_source(table_schema);

        let mut builder = FileScanConfigBuilder::new(self.object_store_url(), file_source)
            .with_file_groups(file_groups)
            .with_limit(limit)
            .with_metadata_cols(self.inner.options().metadata_cols.clone())
            .map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to apply metadata columns: {e}"
                ))
            })?
            .with_projection_indices(projection.cloned())
            .map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to apply projection indices: {e}"
                ))
            })?
            .with_object_versioning_type(self.inner.options().object_versioning_type.clone());

        if let Some(constraints) = self.inner.constraints() {
            builder = builder.with_constraints(constraints.clone());
        }

        let config = builder.build();

        self.inner
            .options()
            .format
            .create_physical_plan(state, config)
            .await
    }
}

fn parse_partition_values(
    table_path: &ListingTableUrl,
    file_path: &Path,
    table_partition_cols: &[(String, datafusion::arrow::datatypes::DataType)],
) -> DFResult<Vec<String>> {
    // Extract hive-style partition values (e.g., year=2023/month=2) from the
    // file path relative to the table path, validating the expected partition
    // column names. A matching object that lacks those segments is an error:
    // omitting it would return incomplete query results.
    let location = file_path.as_ref();
    let expected_columns: Vec<&str> = table_partition_cols
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    let Some(subpath) = table_path.strip_prefix(file_path) else {
        return Err(DataFusionError::Configuration(hive_partition_prefix_error(
            location,
            table_path.prefix().as_ref(),
        )));
    };

    let mut part_values = Vec::with_capacity(table_partition_cols.len());
    for (part, (expected_partition, _)) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == expected_partition => part_values.push(val.to_string()),
            _ => {
                return Err(DataFusionError::Configuration(hive_partition_parse_error(
                    location,
                    &expected_columns,
                )));
            }
        }
    }
    if part_values.len() != table_partition_cols.len() {
        return Err(DataFusionError::Configuration(hive_partition_parse_error(
            location,
            &expected_columns,
        )));
    }
    Ok(part_values)
}

fn hive_partition_parse_error(location: &str, expected_columns: &[&str]) -> String {
    let columns = expected_columns
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let example = expected_columns
        .iter()
        .map(|name| format!("{name}=value"))
        .collect::<Vec<_>>()
        .join("/");
    format!(
        "Object '{location}' does not contain Hive partition segments for columns {columns}, so this scan would omit matching files and return incomplete results. Every matching object must include `key=value` path segments for those columns (for example `{example}/file.orc`). See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
}

fn hive_partition_prefix_error(location: &str, table_prefix: &str) -> String {
    format!(
        "Object '{location}' is not under table path '{table_prefix}', so Hive partition values cannot be parsed and this scan would omit matching files. Keep matching objects under the dataset path. See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
}

fn hive_partition_value_type_error(
    location: &str,
    column: &str,
    value: &str,
    dtype: &DataType,
) -> String {
    format!(
        "Object '{location}' has Hive partition value '{value}' for column '{column}' that cannot be converted to {dtype}, so this scan would omit matching files and return incomplete results. Use a `key=value` path segment whose value matches the partition column type. See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for MetadataPruningListingTable {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion_expr::Expr],
    ) -> DFResult<Vec<datafusion_expr::TableProviderFilterPushDown>> {
        // Format-selected listing builds its own file list, so partition
        // predicates are applied as residual filters rather than pruned here.
        if self.uses_format_selected_listing() {
            return Ok(vec![
                datafusion_expr::TableProviderFilterPushDown::Inexact;
                filters.len()
            ]);
        }
        // When `scan` will prune the listing by `_last_modified`, every
        // predicate stays a residual `FilterExec` above the pruned scan:
        // `Inexact` keeps the row-level filter (so precision and any
        // partition/data-column predicate are always re-enforced) while the
        // prune itself removes the files that cannot match. The `_location`
        // fast-path takes precedence, so this only applies when it is absent.
        // `scan` receives `&[Expr]`, so mirror the same predicate detection here
        // over the borrowed slice this method is given.
        let owned_filters: Vec<datafusion_expr::Expr> = filters.iter().copied().cloned().collect();
        if extract_location_predicates(&owned_filters).is_none()
            && extract_last_modified_predicate(&owned_filters).is_some()
        {
            return Ok(vec![
                datafusion_expr::TableProviderFilterPushDown::Inexact;
                filters.len()
            ]);
        }
        self.inner.supports_filters_pushdown(filters)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let Some(locations) = extract_location_predicates(filters) else {
            // No `_location` predicate. Prune the listing by any `_last_modified`
            // bound before opening files, so an `append` refresh that added no
            // new objects never re-reads (and, for `jsonl.gz`, re-decompresses)
            // the whole source. Correctness is preserved by the residual
            // `FilterExec` this table reports via `supports_filters_pushdown`.
            if let Some(bounds) = extract_last_modified_predicate(filters) {
                return self
                    .scan_last_modified_pruned(state, &bounds, projection, limit)
                    .await;
            }
            if self.uses_format_selected_listing() {
                return self
                    .scan_format_selected_listing(state, projection, limit)
                    .await;
            }
            return self.inner.scan(state, projection, filters, limit).await;
        };

        // Ensure the query runtime uses the same object store configuration (endpoint/region)
        // as the listing table, even when we bypass listing.
        state.runtime_env().register_object_store(
            self.object_store_url().as_ref(),
            Arc::clone(&self.object_store),
        );

        // A combined `_location = x AND _last_modified > w` heads each named
        // object anyway, so drop the ones the mtime bound excludes before they
        // are opened.
        let last_modified_bounds = extract_last_modified_predicate(filters);

        let mut files: Vec<PartitionedFile> = Vec::with_capacity(locations.len());

        for loc in locations {
            let Ok(url) = Url::parse(&loc) else {
                tracing::warn!(location = loc, "Ignoring invalid location predicate URL");
                continue;
            };

            // Enforce that the requested location stays within the configured object store/prefix.
            let location_listing = match ListingTableUrl::parse(&loc) {
                Ok(l) => l,
                Err(err) => {
                    tracing::warn!(%err, location = loc, "Ignoring location predicate outside table prefix");
                    continue;
                }
            };
            if location_listing.object_store() != self.object_store_url()
                || !self.table_path.contains(location_listing.prefix(), false)
            {
                tracing::warn!(
                    location = loc,
                    "Ignoring location predicate outside table prefix/object store"
                );
                continue;
            }

            let path = Path::from(url.path().trim_start_matches('/'));

            let meta = match self.object_store.head(&path).await {
                Ok(m) => m,
                Err(err) => {
                    tracing::warn!(%err, location = loc, "Failed to head object for location predicate");
                    continue;
                }
            };

            if self.uses_format_selected_listing()
                && !file_matches_extension(&meta.location, &self.listing_extension)
            {
                continue;
            }

            if let Some(bounds) = last_modified_bounds.as_deref()
                && !last_modified_meta_passes(&meta, bounds)
            {
                continue;
            }

            files.push(self.partitioned_file_for_meta(meta)?);
        }

        self.scan_partitioned_files(state, files, projection, limit)
            .await
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&datafusion_expr::Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn datafusion::catalog::Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> datafusion::error::Result<datafusion::catalog::ScanResult> {
        let plan = self
            .scan(
                state,
                args.projection().map(<[usize]>::to_vec).as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
            )
            .await?;
        Ok(plan.into())
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        if self.uses_format_selected_listing() {
            // The inner `ListingTable` suffixes with an empty extension and
            // would either miss extensionless objects or treat marker-only
            // listings as an exact empty table.
            return None;
        }
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn datafusion::catalog::Session,
        input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn datafusion::catalog::Session,
        filters: Vec<datafusion_expr::Expr>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn datafusion::catalog::Session,
        assignments: Vec<(String, datafusion_expr::Expr)>,
        filters: Vec<datafusion_expr::Expr>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(
        &self,
        state: &dyn datafusion::catalog::Session,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.truncate(state).await
    }
}

/// Extracts literal locations from `location = 'literal'` and `location IN (...)`
/// predicates when they appear in a purely conjunctive context. If a location
/// predicate appears under `NOT` or `OR`, return `None` to force the caller to
/// fall back to full listing (to avoid incorrect pruning).
fn extract_location_predicates(filters: &[datafusion_expr::Expr]) -> Option<Vec<String>> {
    use datafusion_expr::{Expr, Operator};

    // Recursively walks filter expressions to collect string literals from:
    // - location = 'literal' and 'literal' = location
    // - location IN ('a', 'b', ...)
    // Only safe when predicates are in a purely conjunctive form (no OR/NOT).
    fn literal_str(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn collect_locations(expr: &Expr) -> (Vec<String>, bool) {
        match expr {
            Expr::BinaryExpr(binary) => match binary.op {
                Operator::Eq => {
                    let left_is_location =
                        matches!(*binary.left, Expr::Column(ref c) if c.name == "_location");
                    let right_is_location =
                        matches!(*binary.right, Expr::Column(ref c) if c.name == "_location");

                    let mut values = Vec::new();
                    if left_is_location && let Some(value) = literal_str(&binary.right) {
                        values.push(value);
                    }
                    if right_is_location && let Some(value) = literal_str(&binary.left) {
                        values.push(value);
                    }
                    (values, true)
                }
                Operator::And => {
                    let (mut lvals, lsafe) = collect_locations(&binary.left);
                    let (rvals, rsafe) = collect_locations(&binary.right);
                    lvals.extend(rvals);
                    (lvals, lsafe && rsafe)
                }
                Operator::Or => {
                    let (lvals, lsafe) = collect_locations(&binary.left);
                    let (rvals, rsafe) = collect_locations(&binary.right);
                    if !lvals.is_empty() || !rvals.is_empty() {
                        (Vec::new(), false)
                    } else {
                        (Vec::new(), lsafe && rsafe)
                    }
                }
                _ => (Vec::new(), true),
            },
            Expr::InList(in_list) if matches!(*in_list.expr, Expr::Column(ref c) if c.name == "_location") => {
                if in_list.negated {
                    (Vec::new(), false)
                } else {
                    let mut values = Vec::new();
                    for v in &in_list.list {
                        if let Some(s) = literal_str(v) {
                            values.push(s);
                        }
                    }
                    (values, true)
                }
            }
            Expr::Not(inner) => {
                let (vals, _safe_inner) = collect_locations(inner);
                if vals.is_empty() {
                    (Vec::new(), true)
                } else {
                    (Vec::new(), false)
                }
            }
            _ => (Vec::new(), true),
        }
    }

    let mut values = Vec::new();
    let mut safe = true;
    for filter in filters {
        let (vals, is_safe) = collect_locations(filter);
        values.extend(vals);
        safe &= is_safe;
    }

    if !safe {
        return None;
    }

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

/// A single comparison against the `_last_modified` metadata column, with the
/// threshold held as a [`Duration`] since the Unix epoch. An object is kept when
/// its last-modified time satisfies the comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LastModifiedBound {
    op: datafusion_expr::Operator,
    threshold: Duration,
}

/// True when a cast target keeps the `_last_modified` value exactly.
///
/// The metadata column is `Timestamp(Microsecond, "UTC")`, and pruning compares
/// in nanoseconds. A cast to microseconds or nanoseconds preserves that value
/// (identity or an exact `* 1000`), so pruning stays equivalent to the row
/// predicate. A coarser target (`Second`, `Millisecond`) or a non-timestamp
/// target truncates the value, which would let, for example,
/// `CAST(_last_modified AS Timestamp(Second)) = 200s` prune an object at 200.5s
/// whose row still matches — so such casts are not prunable.
fn cast_preserves_last_modified_precision(data_type: &DataType) -> bool {
    use arrow_schema::TimeUnit;
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, _)
    )
}

/// True when `expr` is the `_last_modified` column, possibly wrapped in a
/// precision-preserving cast. An `append` refresh emits
/// `CAST(_last_modified AS Timestamp(ns, tz)) > …`; `DataFusion`'s
/// `unwrap_cast_in_comparison` rule may instead present the bare column with a
/// cast literal, so both forms are accepted. A coarsening cast is rejected (see
/// [`cast_preserves_last_modified_precision`]) so it cannot build a prunable
/// bound; the residual filter still enforces it.
fn is_last_modified_ref(expr: &Expr) -> bool {
    match expr {
        Expr::Column(column) => column.name == "_last_modified",
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            cast_preserves_last_modified_precision(field.data_type()) && is_last_modified_ref(expr)
        }
        _ => false,
    }
}

/// True when any subexpression references the `_last_modified` column. Used to
/// refuse pruning when the column appears under `OR`/`NOT`, where a single
/// disjunct is not a necessary condition and pruning on it would drop objects
/// whose rows must still be scanned.
fn references_last_modified(expr: &datafusion_expr::Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    // `apply` visits `expr` and every descendant; `is_last_modified_ref` also
    // matches a cast wrapping the column, so either the cast or the bare column
    // node trips the flag.
    let _ = expr.apply(|node| {
        if is_last_modified_ref(node) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

// Flip a comparison so the `_last_modified` reference is on the left, e.g.
// `<literal> > _last_modified` becomes `_last_modified < <literal>`.
fn flip(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

use datafusion_expr::{Between, Cast, Expr, Operator, TryCast};

/// Reads a timestamp literal as a [`Duration`] since the Unix epoch, using the
/// constructor for its precision so the value is exact.
///
/// Returns `None` for a NULL, a non-timestamp literal, or a pre-epoch (negative)
/// value that a `Duration` cannot represent — in each case the caller declines
/// to prune rather than risk dropping matching objects.
fn literal_duration(expr: &Expr) -> Option<Duration> {
    match expr {
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => {
            Some(Duration::from_nanos(u64::try_from(*v).ok()?))
        }
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(v), _), _) => {
            Some(Duration::from_micros(u64::try_from(*v).ok()?))
        }
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(v), _), _) => {
            Some(Duration::from_millis(u64::try_from(*v).ok()?))
        }
        Expr::Literal(ScalarValue::TimestampSecond(Some(v), _), _) => {
            Some(Duration::from_secs(u64::try_from(*v).ok()?))
        }
        _ => None,
    }
}

/// Returns the bounds contributed by `expr` and whether pruning stays safe.
fn collect_last_modified_bounds(expr: &Expr) -> (Vec<LastModifiedBound>, bool) {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq => {
                if is_last_modified_ref(&binary.left) {
                    return match literal_duration(&binary.right) {
                        Some(threshold) => (
                            vec![LastModifiedBound {
                                op: binary.op,
                                threshold,
                            }],
                            true,
                        ),
                        None => (Vec::new(), false),
                    };
                }
                if is_last_modified_ref(&binary.right) {
                    return match literal_duration(&binary.left) {
                        Some(threshold) => (
                            vec![LastModifiedBound {
                                op: flip(binary.op),
                                threshold,
                            }],
                            true,
                        ),
                        None => (Vec::new(), false),
                    };
                }
                (Vec::new(), true)
            }
            Operator::And => {
                let (mut lvals, lsafe) = collect_last_modified_bounds(&binary.left);
                let (rvals, rsafe) = collect_last_modified_bounds(&binary.right);
                lvals.extend(rvals);
                (lvals, lsafe && rsafe)
            }
            Operator::Or => {
                if references_last_modified(&binary.left) || references_last_modified(&binary.right)
                {
                    (Vec::new(), false)
                } else {
                    (Vec::new(), true)
                }
            }
            _ => (Vec::new(), !references_last_modified(expr)),
        },
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) if is_last_modified_ref(expr) => {
            match (*negated, literal_duration(low), literal_duration(high)) {
                (false, Some(low), Some(high)) => (
                    vec![
                        LastModifiedBound {
                            op: Operator::GtEq,
                            threshold: low,
                        },
                        LastModifiedBound {
                            op: Operator::LtEq,
                            threshold: high,
                        },
                    ],
                    true,
                ),
                _ => (Vec::new(), false),
            }
        }
        Expr::Not(inner) => (Vec::new(), !references_last_modified(inner)),
        other => (Vec::new(), !references_last_modified(other)),
    }
}

/// Extracts conjunctive `_last_modified` bounds usable to prune the object-store
/// listing before any file is opened.
///
/// Only a purely conjunctive form is safe: each top-level filter is an implicit
/// `AND`, and pruning on any conjunct is a necessary condition for a row to
/// pass. If `_last_modified` appears under `OR` or `NOT`, or a comparison
/// against it uses a non-timestamp literal, this returns `None` so the caller
/// falls back to a full listing.
fn extract_last_modified_predicate(
    filters: &[datafusion_expr::Expr],
) -> Option<Vec<LastModifiedBound>> {
    let mut bounds = Vec::new();
    let mut safe = true;
    for filter in filters {
        let (vals, is_safe) = collect_last_modified_bounds(filter);
        bounds.extend(vals);
        safe &= is_safe;
    }

    if !safe || bounds.is_empty() {
        None
    } else {
        Some(bounds)
    }
}

/// True when an object's last-modified time satisfies every bound.
///
/// The object's mtime is taken as a [`Duration`] since the Unix epoch and
/// compared against each threshold. Both sides are exact, so the comparison
/// matches the row predicate. A pre-epoch (negative) mtime cannot be a
/// `Duration`, so it is never pruned.
fn last_modified_meta_passes(meta: &ObjectMeta, bounds: &[LastModifiedBound]) -> bool {
    let Ok(micros) = u64::try_from(meta.last_modified.timestamp_micros()) else {
        return true;
    };
    let file = Duration::from_micros(micros);
    bounds.iter().all(|bound| match bound.op {
        Operator::Gt => file > bound.threshold,
        Operator::GtEq => file >= bound.threshold,
        Operator::Lt => file < bound.threshold,
        Operator::LtEq => file <= bound.threshold,
        Operator::Eq => file == bound.threshold,
        // `extract_last_modified_predicate` only produces the operators above.
        _ => true,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectVersionType {
    Version,
}

#[async_trait]
pub trait ListingTableConnector: DataConnector {
    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        None
    }

    /// Whether a single-file dataset from this connector can be served through
    /// the ETag/Version-ID cache in
    /// [`S3SingleFileCached`](data_components::s3_single_file_cached::S3SingleFileCached),
    /// which skips a re-fetch when the object is unchanged.
    ///
    /// Only object stores that return a strong per-object version identifier on
    /// `HEAD` qualify; a store whose `ETag` changes on re-upload of identical
    /// bytes, or which omits one, would serve stale data. Defaults to `false`,
    /// so a connector opts in only after its store has been checked.
    fn supports_single_file_version_cache(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any;

    /// Retrieves the object store URL for a given dataset.
    ///
    /// Determines the URL of the object store associated with the dataset.
    /// If a specific URL is provided as an argument, it uses that; otherwise, it derives
    /// the URL based on the dataset's configuration.
    ///
    /// # Arguments
    ///
    /// * `dataset` - A reference to the [`Dataset`] for which the object store URL is being retrieved.
    /// * `url` - An optional reference to a string representing a specific Path or URL to use.
    ///
    /// # Returns
    ///
    /// A [`DataConnectorResult`] containing the resolved [`Url`] of the object store.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset's `from` (or the supplied `url`) is not a
    /// URL this connector's object store can address.
    fn get_object_store_url(
        &self,
        dataset: &DatasetSpec,
        url: Option<&str>,
    ) -> DataConnectorResult<Url>;

    fn get_params(&self) -> &Parameters;

    #[must_use]
    fn get_session_context(&self) -> SessionContext {
        SessionContext::new_with_config_rt(
            get_df_default_config().set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            ),
            default_runtime_env(self.get_tokio_io_runtime()),
        )
    }

    /// The object store this connector reads `dataset` from.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset's URL cannot be resolved or the store
    /// cannot be constructed from the connector's parameters (bad credentials,
    /// unreachable endpoint).
    fn get_object_store(&self, dataset: &DatasetSpec) -> DataConnectorResult<Arc<dyn ObjectStore>>
    where
        Self: Display,
    {
        let store_url = self.get_object_store_url(dataset, None)?;
        let listing_store_url = ListingTableUrl::parse(store_url).boxed().context(
            crate::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
            },
        )?;
        self.get_session_context()
            .runtime_env()
            .object_store(&listing_store_url)
            .boxed()
            .context(crate::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
            })
    }

    /// The loaded app, for the runtime-level configuration this connector's
    /// reads consult (currently `runtime.params.parquet_page_index`).
    ///
    /// `None` where no runtime is attached — connector unit tests that build the
    /// connector directly.
    fn get_app(&self) -> Option<Arc<App>> {
        None
    }

    /// Returns a handle to the IO runtime that this object store connector should
    /// use for spawning IO tasks.
    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle;

    async fn construct_metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let store_url: Url = self.get_object_store_url(dataset, None)?;
        let store = self.get_object_store(dataset)?;
        let (_, extension) = self.get_file_format_and_extension(dataset).await?;

        let table = ObjectStoreMetadataTable::try_new(store, &store_url, Some(extension.clone()))
            .context(crate::InvalidConfigurationSnafu {
            dataconnector: format!("{self}"),
            message: format!(
                "Invalid file extension ({extension}) for source ({})",
                dataset.name
            ),
            connector_component: ConnectorComponent::from(dataset),
        })?;
        Ok(table as Arc<dyn TableProvider>)
    }

    /// Determines the file format and its corresponding extension for a given dataset.
    ///
    /// If not explicitly specified (via the [`Dataset`]'s `file_format` param key), it attempts
    /// to infer the format from the dataset's file extension. It supports both tabular and
    /// unstructured formats. It supports the following tabular formats:
    ///  - parquet
    ///  - orc
    ///  - vortex (not available on Windows)
    ///  - csv
    ///
    /// For tabular formats, file options can also be specified in the [`Dataset`]'s `param`s.
    ///
    /// For unstructured text formats, the [`Dataset`]'s `file_format` param key must be set. `Ok`
    /// responses, are always of the format `Ok((None, String))`. The data must be UTF8 compatible.
    async fn get_file_format_and_extension(
        &self,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<(Option<Arc<dyn FileFormat>>, String)>
    where
        Self: Display,
    {
        let params = self.get_params();
        let file_extension_param = params.get("file_extension").expose().ok();
        let format_selected_listing =
            file_extension_param.is_some_and(is_format_selected_file_extension_param);
        let configured_extension = if format_selected_listing {
            None
        } else {
            file_extension_param.and_then(parse_file_extension_param)
        };
        let path_extension = detect_file_extension_from_url_or_path(&dataset.from);
        let detected_extension = configured_extension.as_ref().or(path_extension.as_ref());
        let inferred_file_extension =
            detected_extension.and_then(|ext| ext.format_extension.as_deref());
        let file_format_param = params
            .get("file_format")
            .expose()
            .ok()
            .map(str::to_ascii_lowercase);
        let file_compression_type = resolve_file_compression_type(
            &format!("{self}"),
            dataset,
            params,
            detected_extension.and_then(|ext| ext.compression),
        )?;

        // Annotate before `?` so rustc unifies each arm as `Arc<dyn FileFormat>`
        // instead of pinning the match to the first (`CsvFormat`) arm.
        let result: DataConnectorResult<(Option<Arc<dyn FileFormat>>, String)> =
            match (file_format_param.as_deref(), inferred_file_extension) {
            (Some("csv"), _) | (None, Some("csv")) => Ok((
                Some(self.delimiter_separated_format(
                    params,
                    DelimitedFormat::Csv,
                    file_compression_type,
                )?),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".csv",
                    file_compression_type,
                ),
            )),
            (Some("tsv"), _) | (None, Some("tsv")) => Ok((
                Some(self.delimiter_separated_format(
                    params,
                    DelimitedFormat::Tsv,
                    file_compression_type,
                )?),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".tsv",
                    file_compression_type,
                ),
            )),
            (Some("json"), _) => Ok((
                Some(self.get_json_format(dataset, params, Format::Json, file_compression_type)?),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".json",
                    file_compression_type,
                ),
            )),
            (None, Some("json")) => Ok((
                Some(self.get_json_format(dataset, params, Format::Auto, file_compression_type)?),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".json",
                    file_compression_type,
                ),
            )),
            (Some("jsonl" | "ndjson" | "ldjson"), _) | (None, Some("jsonl" | "ndjson" | "ldjson")) => {
                // If json_pointer or json_path is set, route through SpiceJsonFormat
                // so the pointer extraction is applied (DataFusion's JsonFormat doesn't
                // support json_pointer).
                let has_pointer = matches!(
                    params.get("json_pointer").expose(),
                    ExposedParamLookup::Present(v) if !v.is_empty()
                ) || matches!(
                    params.get("json_path").expose(),
                    ExposedParamLookup::Present(v) if !v.is_empty()
                );
                let default_ext = default_jsonl_extension(file_format_param.as_deref(), inferred_file_extension);
                if has_pointer {
                    Ok((
                        Some(self.get_json_format(dataset, params, Format::Auto, file_compression_type)?),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            default_ext,
                            file_compression_type,
                        ),
                    ))
                } else {
                    Ok((
                        Some(self.get_jsonl_format(dataset, params, file_compression_type)?),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            default_ext,
                            file_compression_type,
                        ),
                    ))
                }
            },
            (Some("soda" | "socrata"), _) => Ok((
                Some(self.get_json_format(dataset, params, Format::Soda, file_compression_type)?),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".json",
                    file_compression_type,
                ),
            )),
            #[cfg(not(windows))]
            (Some("vortex"), _) | (None, Some("vortex")) => Ok((
                Some(
                    VortexFormatFactory::new()
                        .with_cache_name(dataset.name.to_string())
                        .default(),
                ),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".vortex",
                    FileCompressionType::UNCOMPRESSED,
                ),
            )),
            (Some("parquet"), _) | (None, Some("parquet"))=> Ok((
                Some(Arc::new(
                    ParquetFormat::default().with_options(self.get_table_parquet_options(dataset).await?),
                )),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".parquet",
                    FileCompressionType::UNCOMPRESSED,
                ),
            )),
            (Some("orc"), _) | (None, Some("orc")) => Ok((
                Some(Arc::new(OrcFormat::new())),
                listing_extension(
                    configured_extension.as_ref(),
                    path_extension.as_ref(),
                    ".orc",
                    FileCompressionType::UNCOMPRESSED,
                ),
            )),
            (Some("auto"), ext) => {
                match ext {
                    Some("csv") => Ok((
                        Some(self.delimiter_separated_format(
                            params,
                            DelimitedFormat::Csv,
                            file_compression_type,
                        )?),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            ".csv",
                            file_compression_type,
                        ),
                    )),
                    Some("tsv") => Ok((
                        Some(self.delimiter_separated_format(
                            params,
                            DelimitedFormat::Tsv,
                            file_compression_type,
                        )?),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            ".tsv",
                            file_compression_type,
                        ),
                    )),
                    Some("jsonl" | "ndjson" | "ldjson") => {
                        let has_pointer = matches!(
                            params.get("json_pointer").expose(),
                            ExposedParamLookup::Present(v) if !v.is_empty()
                        ) || matches!(
                            params.get("json_path").expose(),
                            ExposedParamLookup::Present(v) if !v.is_empty()
                        );
                        let default_ext = default_jsonl_extension(file_format_param.as_deref(), ext);
                        if has_pointer {
                            Ok((
                                Some(self.get_json_format(dataset, params, Format::Auto, file_compression_type)?),
                                listing_extension(
                                    configured_extension.as_ref(),
                                    path_extension.as_ref(),
                                    default_ext,
                                    file_compression_type,
                                ),
                            ))
                        } else {
                            Ok((
                                Some(self.get_jsonl_format(dataset, params, file_compression_type)?),
                                listing_extension(
                                    configured_extension.as_ref(),
                                    path_extension.as_ref(),
                                    default_ext,
                                    file_compression_type,
                                ),
                            ))
                        }
                    },
                    Some("parquet") => Ok((
                        Some(Arc::new(
                            ParquetFormat::default().with_options(self.get_table_parquet_options(dataset).await?),
                        )),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            ".parquet",
                            FileCompressionType::UNCOMPRESSED,
                        ),
                    )),
                    Some("orc") => Ok((
                        Some(Arc::new(OrcFormat::new())),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            ".orc",
                            FileCompressionType::UNCOMPRESSED,
                        ),
                    )),
                    #[cfg(not(windows))]
                    Some("vortex") => Ok((
                        Some(
                            VortexFormatFactory::new()
                                .with_cache_name(dataset.name.to_string())
                                .default(),
                        ),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            ".vortex",
                            FileCompressionType::UNCOMPRESSED,
                        ),
                    )),
                    // For .json or unknown/no extension, use JSON with auto sub-format detection
                    _ => Ok((
                        Some(self.get_json_format(dataset, params, Format::Auto, file_compression_type)?),
                        listing_extension(
                            configured_extension.as_ref(),
                            path_extension.as_ref(),
                            &ext.map_or(".json".to_string(), |e| format!(".{e}")),
                            file_compression_type,
                        ),
                    )),
                }
            },
            (Some(format), _) => Ok((None, format!(".{format}"))),
            (_, _) => Err(
                    crate::DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: "The required 'file_format' parameter is missing. Ensure the parameter is provided, and try again.".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: "Missing file format".into(),
                    },
                ),
        };

        if format_selected_listing {
            let (file_format, default_extension) = result?;
            Ok((
                file_format,
                format_selected_listing_extension(&default_extension),
            ))
        } else {
            result
        }
    }

    /// Returns a [`JsonFormat`] based on the provided [`Datasets`] parameters.
    ///
    /// If the [`Dataset`] has the relevant parameter, return an error if the value is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error if `schema_infer_max_records` is present but not a number.
    fn get_jsonl_format(
        &self,
        dataset: &DatasetSpec,
        params: &Parameters,
        file_compression_type: FileCompressionType,
    ) -> DataConnectorResult<Arc<JsonFormat>>
    where
        Self: Display,
    {
        let mut format = JsonFormat::default().with_file_compression_type(file_compression_type);

        if let ExposedParamLookup::Present(infer_max_rec_str) =
            params.get("schema_infer_max_records").expose()
        {
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSONL parameter 'schema_infer_max_records' must be an integer, not {infer_max_rec_str}"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_schema_infer_max_rec(schema_infer_max_rec);
        }

        Ok(Arc::new(format))
    }

    /// Returns a [`SpiceJsonFormat`] based on the provided [`Datasets`] parameters.
    ///
    /// If the [`Dataset`] has the relevant parameter, return an error if the value is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error if `schema_infer_max_records` is not a number, or
    /// `json_format` is not one of the supported formats.
    fn get_json_format(
        &self,
        dataset: &DatasetSpec,
        params: &Parameters,
        default_format: Format,
        file_compression_type: FileCompressionType,
    ) -> DataConnectorResult<Arc<SpiceJsonFormat>>
    where
        Self: Display,
    {
        let mut format = SpiceJsonFormat::default()
            .with_format(default_format)
            .with_file_compression_type(file_compression_type);

        if let ExposedParamLookup::Present(infer_max_rec_str) =
            params.get("schema_infer_max_records").expose()
        {
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSON parameter 'schema_infer_max_records' must be an integer, not {infer_max_rec_str}"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_schema_infer_max_rec(schema_infer_max_rec);
        }

        if let ExposedParamLookup::Present(json_format_str) = params.get("json_format").expose() {
            let json_format = json_format_str.parse::<Format>().boxed().context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSON format: {json_format_str}, supported formats are: 'json', 'jsonl', 'ndjson', 'ldjson', 'array', 'object', 'soda', 'socrata', 'auto'"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_format(json_format);
        }

        if let ExposedParamLookup::Present(json_pointer) = params.get("json_pointer").expose() {
            format = format.with_json_pointer(json_pointer.to_string());
        } else if let ExposedParamLookup::Present(json_path) = params.get("json_path").expose() {
            format = format.with_json_pointer(json_path.to_string());
        }

        if let ExposedParamLookup::Present(flatten_json) = params.get("flatten_json").expose()
            && flatten_json.eq_ignore_ascii_case("true")
        {
            format = format.with_flatten_json(".".to_string());
        }

        if let ExposedParamLookup::Present(soda_metadata) = params.get("soda_metadata").expose() {
            format = format.with_soda_metadata(soda_metadata.eq_ignore_ascii_case("enabled"));
        }

        // Validate: json_pointer is incompatible with file_format=soda.
        // SODA responses carry their own schema in meta.view.columns and SodaReader
        // handles data extraction internally — json_pointer cannot be applied.
        if format.options().format == Format::Soda && format.options().json_pointer.is_some() {
            return Err(
                crate::DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "'json_pointer' cannot be used with 'file_format: soda'. SODA format extracts data from the response automatically.".to_string(),
                },
            );
        }

        Ok(Arc::new(format))
    }

    /// Returns a [`CsvFormat`] based on the provided [`Datasets`] parameters, and choice of delimiter.
    ///
    /// Uses the appropriate parameters based on the [`DelimitedFormat`] provided.
    ///
    /// # Errors
    ///
    /// Returns an error if the quote or escape parameter is not a single
    /// character, or `schema_infer_max_records` is not a number.
    fn delimiter_separated_format(
        &self,
        params: &Parameters,
        delimiter: DelimitedFormat,
        file_compression_type: FileCompressionType,
    ) -> DataConnectorResult<Arc<CsvFormat>>
    where
        Self: Display,
    {
        let has_header = params
            .get(&format!("{delimiter}_has_header"))
            .expose()
            .ok()
            .is_none_or(|f| f.eq_ignore_ascii_case("true"));
        let quote = params
            .get(&format!("{delimiter}_quote"))
            .expose()
            .ok()
            .map_or(b'"', |f| *f.as_bytes().first().unwrap_or(&b'"'));
        let escape = params
            .get(&format!("{delimiter}_escape"))
            .expose()
            .ok()
            .and_then(|f| f.as_bytes().first().copied());
        let schema_infer_max_rec = params
            .get("schema_infer_max_records")
            .expose()
            .ok()
            .or(params
                .get(&format!("{delimiter}_schema_infer_max_records"))
                .expose()
                .ok()) // For backwards compatibility
            .map_or_else(|| 1000, |f| usize::from_str(f).unwrap_or(1000));
        let delimiter = match delimiter {
            DelimitedFormat::Tsv => delimiter.separator(),
            DelimitedFormat::Csv => params
                .get("csv_delimiter")
                .expose()
                .ok()
                .and_then(|d| d.chars().next().map(|c| c as u8))
                .unwrap_or(delimiter.separator()),
        };

        Ok(Arc::new(
            CsvFormat::default()
                .with_has_header(has_header)
                .with_quote(quote)
                .with_escape(escape)
                .with_schema_infer_max_rec(schema_infer_max_rec)
                .with_delimiter(delimiter)
                .with_file_compression_type(file_compression_type),
        ))
    }

    async fn get_table_parquet_options(
        &self,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<TableParquetOptions>
    where
        Self: Display,
    {
        build_table_parquet_options(self.get_app().as_ref()).map_err(|e| {
            crate::DataConnectorError::UnableToConnectInternal {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            }
        })
    }

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &DatasetSpec,
        _accelerated_table: &mut dyn RegisteredAcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Turn an `object_store` error into the error the user sees.
    ///
    /// An implementation that inspects [`object_store::Error::Generic`] must call
    /// [`object_store_timeout_message`] before classifying it any other way — see that function
    /// for why.
    fn handle_object_store_error(
        &self,
        dataset: &DatasetSpec,
        error: object_store::Error,
    ) -> DataConnectorError
    where
        Self: Display,
    {
        crate::DataConnectorError::UnableToConnectInternal {
            dataconnector: format!("{self}"),
            connector_component: ConnectorComponent::from(dataset),
            source: error.into(),
        }
    }

    async fn create_text_table(
        &self,
        dataset: &DatasetSpec,
        url: &Url,
        extension: &str,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let content_formatter =
            document_parse::get_parser_factory(extension)
                .await
                .map(|factory| {
                    // TODO: add opts.
                    factory.default()
                });

        let metadata_columns = dataset.listing_table_metadata_columns(
            get_url_prefix(url),
            &ObjectStoreTextTable::base_table_schema(),
        );

        Ok(Arc::new(
            ObjectStoreTextTable::try_new(
                self.get_object_store(dataset)?,
                &url.clone(),
                Some(extension.to_string()),
                content_formatter,
                metadata_columns,
            )
            .context(crate::InvalidConfigurationSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "Invalid file extension ({extension}) for source ({})",
                    dataset.name
                ),
            })?,
        ))
    }

    async fn create_listing_table(
        &self,
        dataset: &DatasetSpec,
        url: &Url,
        extension: &str,
        file_format: Arc<dyn FileFormat>,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        // This shouldn't error because we've already validated the URL in `get_object_store_url`.
        let table_path =
            ListingTableUrl::parse(url.clone())
                .boxed()
                .context(crate::InternalSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
                })?;

        let object_store = self.get_object_store(dataset)?;

        let ctx: SessionContext = self.get_session_context();

        let (schema_infer_url, schema_infer_meta) =
            if table_path.is_collection() && listing_extension_is_orc(extension) {
                // Infer from the scan path. `schema_source_path` only replaces
                // `infer_listing_url` and would publish a prefix schema while
                // `ListingTable` still scans `table_path`.
                get_last_modified(
                    format!("{self}"),
                    dataset,
                    extension,
                    table_path.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;
                (
                    SensitiveListingTableUrl::new(table_path.clone(), url.clone()),
                    None,
                )
            } else if let Some(url) = dataset.params.get("schema_source_path") {
                let url = self.get_object_store_url(dataset, Some(url))?;
                let schema_infer_url = ListingTableUrl::parse(&url).boxed().context(
                    crate::UnableToGetSchemaInternalSnafu {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                    },
                )?;
                let schema_infer_meta = verify_schema_source_path(
                    format!("{self}"),
                    dataset,
                    extension,
                    schema_infer_url.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;
                (
                    SensitiveListingTableUrl::new(schema_infer_url, url),
                    schema_infer_meta,
                )
            } else {
                // Get the last modified object for the provided ObjectStore to infer the schema.
                // Report an error if no files matching required extension are found.
                let last_modified_or_added = get_last_modified(
                    format!("{self}"),
                    dataset,
                    extension,
                    table_path.clone(),
                    &ctx,
                    &object_store,
                )
                .await?;

                (
                    to_listing_table_url(
                        url,
                        &last_modified_or_added.location,
                        dataset,
                        &format!("{self}"),
                    )?,
                    None,
                )
            };

        tracing::debug!(
            "Dataset '{name}' schema will be resolved based on {sanitized_url}",
            name = dataset.name,
            sanitized_url = schema_infer_url.sanitized_url(),
        );

        let session_state = ctx.state();
        let mut options = ListingOptions::new(Arc::clone(&file_format))
            .with_file_extension(datafusion_listing_file_extension(extension))
            .with_session_config_options(session_state.config());

        options =
            options.with_object_versioning_type(self.object_versioning_type().map(|v| match v {
                ObjectVersionType::Version => {
                    datafusion::parquet::arrow::async_reader::ObjectVersionType::Version
                }
            }));

        let infer_listing_url = schema_infer_url.expose_sensitive_url();
        let resolved_schema =
            if listing_extension_is_orc(extension) && table_path.is_collection() {
                infer_orc_collection_schema(
                    &ctx.state(),
                    &table_path,
                    &object_store,
                    extension,
                    &file_format,
                )
                .await
            } else {
                options.infer_schema(&ctx.state(), infer_listing_url).await
            }
            .map_err(|e| match e {
                DataFusionError::ObjectStore(object_store_error) => {
                    self.handle_object_store_error(dataset, *object_store_error)
                }
                DataFusionError::Configuration(message) if listing_extension_is_orc(extension) => {
                    crate::DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        message,
                    }
                }
                e => crate::DataConnectorError::UnableToConnectInternal {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e.into(),
                },
            })?;

        let expanded_schema = Arc::new(expand_views_schema(&resolved_schema));

        options = add_metadata_columns_if_required(options, url, &expanded_schema, dataset);

        // If we should infer partitions and the path is a folder, infer the partitions from the folder structure.
        if dataset.get_param("hive_partitioning_enabled", false) && table_path.is_collection() {
            let inferred_partitions = match schema_infer_meta {
                Some(meta) => infer_partitions_with_types_from_files(&table_path, &[meta]),
                None if format_selected_data_suffix(extension).is_some() => {
                    match list_matching_listing_files(
                        &ctx.state(),
                        &table_path,
                        object_store.as_ref(),
                        extension,
                        10,
                    )
                    .await
                    {
                        Ok(files) => infer_partitions_with_types_from_files(&table_path, &files),
                        Err(e) => {
                            tracing::debug!(
                                "Failed to list files for partition inference for {table_path:?}: {e}"
                            );
                            Ok(Vec::new())
                        }
                    }
                }
                None => {
                    infer_partitions_with_types_prefix(&ctx.state(), &table_path, extension).await
                }
            };
            match inferred_partitions {
                Ok(partitions) => {
                    tracing::debug!(
                        "Inferred partitions for {table_path:?}: {:?}",
                        partitions
                            .iter()
                            .map(|(k, _)| k.as_str())
                            .collect::<Vec<_>>()
                    );
                    options = options.with_table_partition_cols(partitions);
                }
                Err(e) => {
                    // This might not be an error, it could be that the table is not partitioned.
                    tracing::debug!("Failed to infer partitions for {table_path:?}: {e}");
                }
            }
        }

        let final_schema = if dataset.get_param("hive_partitioning_enabled", false)
            && table_path.is_collection()
        {
            self.deduplicate_partition_columns_expressed_in_file(
                dataset,
                expanded_schema,
                &options.table_partition_cols,
            )?
        } else {
            expanded_schema
        };

        // Keep a reference to the file schema for MetadataPruningListingTable
        let file_schema = Arc::clone(&final_schema);

        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(options)
            .with_schema(final_schema);

        // This shouldn't error because we're passing the schema and options correctly.
        //
        // Attach a file-statistics cache. With `collect_stat = true` (the
        // DataFusion default), resolving a scan's statistics parses every
        // file's Parquet footer; without a cache, `ListingTable` re-parses all
        // footers on every plan. That makes stat-only queries (e.g. an
        // unfiltered `COUNT(*)`, which the `AggregateStatistics` rule answers
        // purely from statistics) scale linearly with file count on each query.
        // The stock DataFusion `CREATE EXTERNAL TABLE` path wires this same
        // cache; we mirror it so per-file footer stats are reused across
        // queries. The cache invalidates per file on `ObjectMeta` change, so
        // refreshing datasets still pick up new data.
        let table = ListingTable::try_new(config)
            .boxed()
            .context(crate::InternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
            })?
            .with_cache(Some(Arc::new(DefaultFileStatisticsCache::default())));

        // For S3 single-file datasets with acceleration enabled, wrap with a caching layer
        // that checks ETag/Version ID to skip unnecessary re-fetches when file hasn't changed.
        let table_arc = Arc::new(table);
        if self.supports_single_file_version_cache()
            && refresh_skip_enabled(dataset)
            && !table_path.is_collection()
            && dataset.acceleration.is_some()
            && let Some(cached_table) =
                data_components::s3_single_file_cached::S3SingleFileCached::try_new(
                    Arc::clone(&table_arc),
                    Arc::clone(&object_store),
                    dataset.name.to_string(),
                )
        {
            tracing::debug!(
                "Enabled single-file ETag/Version caching for {}",
                dataset.name
            );
            return Ok(Arc::new(cached_table));
        }

        let has_location_metadata = table_arc.options().metadata_cols.iter().any(|c| {
            matches!(
                c,
                datafusion_datasource::metadata::MetadataColumn::Location(_)
            )
        });

        // A `_last_modified` column lets an `append` refresh prune the listing by
        // object mtime, so wrap even when `_location` is not enabled.
        let has_last_modified_metadata = table_arc.options().metadata_cols.iter().any(|c| {
            matches!(
                c,
                datafusion_datasource::metadata::MetadataColumn::LastModified
            )
        });

        if has_location_metadata
            || has_last_modified_metadata
            || format_selected_data_suffix(extension).is_some()
        {
            let wrapped = MetadataPruningListingTable::new(
                table_arc,
                Arc::clone(&object_store),
                table_path,
                file_schema,
                extension,
            );
            Ok(Arc::new(wrapped))
        } else {
            Ok(table_arc)
        }
    }

    /// Drops partition columns from `schema` that the files already carry, so a
    /// hive-partitioned dataset does not expose the same column twice.
    ///
    /// # Errors
    ///
    /// Returns an error if a partition column collides with a file column of a
    /// different type, which cannot be reconciled into one schema.
    fn deduplicate_partition_columns_expressed_in_file(
        &self,
        dataset: &DatasetSpec,
        schema: SchemaRef,
        partition_cols: &[(String, DataType)],
    ) -> DataConnectorResult<SchemaRef> {
        if partition_cols.is_empty() {
            return Ok(schema);
        }

        let mut idents = schema
            .fields
            .iter()
            .map(|f| (f.name().clone(), f.as_ref().clone()))
            .collect::<HashMap<_, _>>();

        for (name, partition_type) in partition_cols {
            if let Some(field) = idents.remove(name) {
                let types_match = match (partition_type, field.data_type()) {
                    (DataType::Utf8, DataType::LargeUtf8 | DataType::Utf8View) => true,
                    (pt, ft) => DFSchema::datatype_is_semantically_equal(pt, ft),
                };

                if !types_match {
                    return Err(SchemaMismatch {
                        dataset_name: dataset.name.to_string(),
                        differences: format!(
                            "Field {name} cannot be deduplicated as its field types differ:\
                            (partition column): {}, (file column): {}",
                            partition_type,
                            field.data_type()
                        ),
                    });
                }
            }
        }

        let new_schema = Schema::new(
            schema
                .fields
                .iter()
                .filter_map(|f| idents.remove(f.name()))
                .collect::<Vec<_>>(),
        )
        .with_metadata(schema.metadata.clone());

        Ok(Arc::new(new_schema))
    }
}

#[async_trait]
impl<T: ListingTableConnector + Display> DataConnector for T {
    fn as_any(&self) -> &dyn Any {
        ListingTableConnector::as_any(self)
    }

    async fn metadata_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }

        Some(self.construct_metadata_provider(dataset).await)
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let url = self.get_object_store_url(dataset, None)?;

        let (file_format_opt, extension) = self.get_file_format_and_extension(dataset).await?;
        match file_format_opt {
            None => {
                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                self.create_text_table(dataset, &url, &extension).await
            }
            Some(file_format) => {
                // Structured tabular data, use a [`ListingTable`].
                self.create_listing_table(dataset, &url, &extension, file_format)
                    .await
            }
        }
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        let url = self.get_object_store_url(dataset, None)?;
        if url.scheme() == "file" {
            tracing::warn!(
                "Dataset {} has a file:// scheme and may not be resolvable on cluster executors without a shared mount.",
                dataset.name
            );
            return Ok(());
        }

        let listing_url =
            ListingTableUrl::parse(url)
                .boxed()
                .context(crate::UnableToConnectInternalSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                })?;

        // Triggers SpiceObjectStoreRegistry::get_store, which builds an object
        // store from the URL fragment params (already secret-expanded by
        // ConnectorParamsBuilder when the connector was created) and registers
        // it on the runtime env keyed by the bare URL.
        runtime_env.object_store(&listing_url).boxed().context(
            crate::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
            },
        )?;

        let mut redacted = <ListingTableUrl as AsRef<url::Url>>::as_ref(&listing_url).clone();
        redacted.set_fragment(None);
        tracing::debug!(
            "Configured object storage for Dataset {} ({redacted})",
            dataset.name,
        );
        Ok(())
    }

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        dataset: &DatasetSpec,
        accelerated_table: &mut dyn RegisteredAcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ListingTableConnector::on_accelerated_table_registration(self, dataset, accelerated_table)
            .await
    }
}

/// Walks an `object_store` error's source chain for the typed `HttpError`.
///
/// `object_store` 0.13 classifies `reqwest`/`hyper`/I/O timeouts into `HttpErrorKind::Timeout`
/// before flattening the error into `object_store::Error::Generic`, so a timeout is detectable by
/// a typed downcast rather than by matching on the error message.
fn object_store_http_error_kind(
    source: &(dyn std::error::Error + 'static),
) -> Option<HttpErrorKind> {
    let mut next = Some(source);
    while let Some(err) = next {
        if let Some(http_error) = err.downcast_ref::<HttpError>() {
            return Some(http_error.kind());
        }
        next = err.source();
    }
    None
}

/// The message for an object-store request that timed out, or `None` when `source` is not a
/// transport timeout.
///
/// Every [`ListingTableConnector::handle_object_store_error`] that inspects
/// [`object_store::Error::Generic`] must consult this **before** classifying the error any other
/// way. `object_store` flattens a timeout into the same `Generic` variant an authentication
/// failure arrives in, so a connector that classifies `Generic` by which credentials are
/// configured reports a network timeout as bad credentials — sending the user to rotate a working
/// secret while the parameter that actually resolves it goes unmentioned (#12793).
///
/// `client_timeout` is the connector's configured value. `None` reports `object_store`'s own
/// default, which every connector inherits by forwarding the parameter unset.
#[must_use]
pub fn object_store_timeout_message(
    source: &(dyn std::error::Error + 'static),
    service: &str,
    client_timeout: Option<&str>,
    docs_url: &str,
) -> Option<String> {
    if object_store_http_error_kind(source) != Some(HttpErrorKind::Timeout) {
        return None;
    }

    let client_timeout = client_timeout.unwrap_or("30s (default)");
    Some(format!(
        "{service} request timed out (client_timeout: {client_timeout}). This often happens when \
         many datasets are loaded concurrently and saturate the network or I/O. Consider \
         increasing the `client_timeout` parameter or reducing the number of concurrent dataset \
         loads. See {docs_url}#params for details."
    ))
}

fn refresh_skip_enabled(dataset: &DatasetSpec) -> bool {
    match dataset.params.get("refresh_skip").map(String::as_str) {
        None | Some("enabled") => true,
        Some("disabled") => false,
        Some(other) => {
            tracing::warn!(
                dataset = %dataset.name,
                value = other,
                "Invalid refresh_skip value; expected 'enabled' or 'disabled'. Defaulting to 'enabled'."
            );
            true
        }
    }
}

fn add_metadata_columns_if_required(
    options: ListingOptions,
    table_url: &Url,
    schema: &Schema,
    dataset: &DatasetSpec,
) -> ListingOptions {
    let url_prefix = get_url_prefix(table_url);
    if let Some(columns) = dataset.listing_table_metadata_columns(url_prefix, schema) {
        tracing::debug!(
            "Enabling metadata columns for '{}': {columns:?}",
            dataset.name,
        );
        let df_columns = columns
            .into_iter()
            .map(|c| match c {
                MetadataColumn::Location(prefix) => {
                    datafusion_datasource::metadata::MetadataColumn::Location(prefix)
                }
                MetadataColumn::LastModified => {
                    datafusion_datasource::metadata::MetadataColumn::LastModified
                }
                MetadataColumn::Size => datafusion_datasource::metadata::MetadataColumn::Size,
            })
            .collect();
        return options.with_metadata_cols(df_columns);
    }

    options
}

// Returns the prefix of the table URL, e.g. for "s3://mybucket/myfolder" it returns "s3://mybucket/"
fn get_url_prefix(table_url: &Url) -> String {
    format!("{}/", &table_url[..url::Position::BeforePath])
}

fn resolve_file_compression_type(
    dataconnector: &str,
    dataset: &DatasetSpec,
    params: &Parameters,
    detected_compression: Option<FileCompressionType>,
) -> DataConnectorResult<FileCompressionType> {
    if let ExposedParamLookup::Present(compression) = params.get("file_compression_type").expose() {
        return compression.parse::<FileCompressionType>().boxed().context(
            crate::InvalidConfigurationSnafu {
                dataconnector: dataconnector.to_string(),
                message: format!(
                    "Invalid file_compression_type: {compression}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"
                ),
                connector_component: ConnectorComponent::from(dataset),
            },
        );
    }

    Ok(detected_compression.unwrap_or(FileCompressionType::UNCOMPRESSED))
}

fn listing_extension(
    configured_extension: Option<&ParsedFileExtension>,
    path_extension: Option<&ParsedFileExtension>,
    default_extension: &str,
    file_compression_type: FileCompressionType,
) -> String {
    if let Some(extension) = configured_extension {
        return extension.file_extension.clone();
    }

    if let Some(extension) = path_extension
        && extension.format_extension.as_deref() == Some(default_extension.trim_start_matches('.'))
    {
        return extension.file_extension.clone();
    }

    format!("{}{}", default_extension, file_compression_type.get_ext())
}

fn default_jsonl_extension(
    file_format_param: Option<&str>,
    inferred_file_extension: Option<&str>,
) -> &'static str {
    match inferred_file_extension.or(file_format_param) {
        Some("ndjson") => ".ndjson",
        Some("ldjson") => ".ldjson",
        _ => ".jsonl",
    }
}

// 1024³
const BYTES_PER_GIB: f64 = 1_073_741_824.0;

/// Identifies the last modified object for a provided ListingTableConnector/ObjectStore
/// Infers if the `file_format` specified is valid, based on the existence of files with the required extension
///
/// # Errors
///
/// - If no files are found at the specified path
/// - If no files with the specified extension are found
async fn get_last_modified(
    dataconnector: String,
    dataset: &DatasetSpec,
    extension: &str,
    table_path: ListingTableUrl,
    ctx: &SessionContext,
    object_store: &Arc<dyn ObjectStore>,
) -> DataConnectorResult<ObjectMeta> {
    tracing::debug!("Detecting the most recently modified object for the path: {table_path}");

    let state = ctx.state();
    let mut file_stream = table_path
        .list_all_files(&state, object_store, "")
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?;

    let mut last_modified_file: Option<ObjectMeta> = None;
    let mut found_extensions = HashSet::new();

    let mut file_count = 0;
    let mut total_size = 0;

    while let Some(file) =
        file_stream
            .try_next()
            .await
            .map_err(|err| DataConnectorError::UnableToConnectInternal {
                dataconnector: dataconnector.clone(),
                connector_component: ConnectorComponent::from(dataset),
                source: err.into(),
            })?
    {
        file_count += 1;
        total_size += file.size;

        #[expect(clippy::cast_precision_loss)]
        if file_count % 1_000_000 == 0 {
            tracing::debug!(
                "Continuing to process {table_path} metadata... {file_count} objects processed so far, representing a total size of: {:.2} GiB",
                total_size as f64 / BYTES_PER_GIB
            );
        }

        if let Some(file_ext) = detect_file_extension_from_path(file.location.as_ref()) {
            found_extensions.insert(file_ext.file_extension.clone());
        } else if !found_extensions.contains(NO_EXTENSION_SENTINEL) {
            // Hive `_committed_*` / `000000_0` objects share one sentinel so
            // the mismatch-error summary stays bounded on large tables.
            found_extensions.insert(NO_EXTENSION_SENTINEL.to_string());
        }

        if file_matches_extension(&file.location, extension) {
            if let Some(ref current) = last_modified_file {
                if current.last_modified < file.last_modified {
                    last_modified_file = Some(file);
                }
            } else {
                last_modified_file = Some(file);
            }
        }
    }

    if found_extensions.is_empty() {
        // No files at all at the path. This is treated as a transient
        // (retriable) condition rather than a permanent configuration error:
        // the source data may not have been written yet (e.g. the object store
        // is still being populated at startup), so the dataset load keeps
        // retrying until files appear. Restores pre-#10246 eventual-readiness.
        return Err(DataConnectorError::ObjectStoreNoFilesAvailable {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Spice could not find any files matching the extension '{extension}' at the specified path."
            ),
        });
    }

    if let Some(best) = last_modified_file {
        Ok(best)
    } else {
        let display_extensions = found_extensions
            .iter()
            .map(|e| format!("'{e}'"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Failed to find any files matching the extension '{extension}'. Is your `file_format` parameter correct? Spice found the following file extensions: {display_extensions}. For details, visit: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
        })
    }
}

async fn verify_schema_source_path(
    dataconnector: String,
    dataset: &DatasetSpec,
    extension: &str,
    schema_source_path: ListingTableUrl,
    ctx: &SessionContext,
    object_store: &Arc<dyn ObjectStore>,
) -> DataConnectorResult<Option<ObjectMeta>> {
    tracing::debug!(
        "Verifying dataset {table_name} schema source path is valid: {schema_source_path}",
        table_name = dataset.name
    );

    let state = ctx.state();
    // Intentionally not passing the `file_extension` parameter to `list_all_files` because we want to
    // short-circuit the listing process if we need to iterate over too many files.
    let mut file_stream = schema_source_path
        .list_all_files(&state, object_store, "")
        .await
        .map_err(|err| DataConnectorError::UnableToConnectInternal {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            source: err.into(),
        })?;

    let mut scanned_files = 0;

    while let Some(file) =
        file_stream
            .try_next()
            .await
            .map_err(|err| DataConnectorError::UnableToConnectInternal {
                dataconnector: dataconnector.clone(),
                connector_component: ConnectorComponent::from(dataset),
                source: err.into(),
            })?
    {
        if file_matches_extension(&file.location, extension) {
            return Ok(Some(file));
        }

        scanned_files += 1;
        if scanned_files > SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT {
            // We've reached the limit of files to scan, but have not found any with the expected extension.
            // We do warning, not an error, as the dataset might have a large number of files.
            tracing::warn!(
                "Failed to find any files matching the extension '{extension}' at the specified path `{schema_source_path}` after scanning {SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT} files. Ensure the `schema_source_path` is correct."
            );
            return Ok(None);
        }
    }

    Err(DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.clone(),
        connector_component: ConnectorComponent::from(dataset),
        message: format!(
            "Failed to find any files matching the extension '{extension}' at the specified path `{schema_source_path}`. Verify that `schema_source_path` is correct and try again."
        ),
    })
}

/// Glue sets `file_extension` to `*` when `InputFormat` already selected
/// Parquet or ORC. Listing then accepts the format suffix or extensionless
/// Hive data objects, and skips job-marker files.
fn is_format_selected_file_extension_param(value: &str) -> bool {
    value.trim() == "*"
}

fn format_selected_listing_extension(default_extension: &str) -> String {
    format!("*{default_extension}")
}

/// True when listing should take the ORC collection schema-merge path.
/// `file_extension` keeps the user's casing (`.ORC`), so the suffix is
/// compared case-insensitively — same rule as [`file_name_has_extension`].
fn listing_extension_is_orc(extension: &str) -> bool {
    format_selected_data_suffix(extension)
        .unwrap_or(extension)
        .eq_ignore_ascii_case(".orc")
}

fn format_selected_data_suffix(extension: &str) -> Option<&str> {
    extension
        .strip_prefix('*')
        .filter(|rest| rest.is_empty() || rest.starts_with('.'))
}

fn datafusion_listing_file_extension(extension: &str) -> &str {
    if format_selected_data_suffix(extension).is_some() {
        ""
    } else {
        extension
    }
}

/// Shown in the "no matching extension" error for objects with no suffix.
/// One entry covers every such object so the summary stays bounded.
const NO_EXTENSION_SENTINEL: &str = "(no extension)";

fn object_file_name(location: &Path) -> &str {
    location
        .as_ref()
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(location.as_ref())
}

fn file_name_has_extension(name: &str, extension: &str) -> bool {
    name.rsplit_once('.')
        .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(extension))
}

fn is_hive_listing_marker_name(name: &str) -> bool {
    name.is_empty()
        || name.starts_with('_')
        || name.starts_with('.')
        || name == "$folder$"
        || name.ends_with(".$folder$")
        || file_name_has_extension(name, "crc")
}

fn listing_path_is_hive_staging(location: &Path) -> bool {
    location
        .as_ref()
        .split('/')
        .any(|segment| segment == "_temporary")
}

fn listing_object_matches_format_selected(location: &Path, format_ext: &str) -> bool {
    let name = object_file_name(location);
    if is_hive_listing_marker_name(name) || listing_path_is_hive_staging(location) {
        return false;
    }

    if format_ext.is_empty() {
        return !name.contains('.')
            || file_name_has_extension(name, "orc")
            || file_name_has_extension(name, "parquet");
    }

    file_name_has_extension(name, format_ext.trim_start_matches('.')) || !name.contains('.')
}

fn file_matches_extension(location: &Path, extension: &str) -> bool {
    if let Some(format_ext) = format_selected_data_suffix(extension) {
        return listing_object_matches_format_selected(location, format_ext);
    }
    location.as_ref().ends_with(extension)
}

/// List matching `ORC` objects and merge their footers. Used instead of
/// [`ListingOptions::infer_schema`] on a collection so format-selected
/// listings (`*.orc`) skip job-marker files and so a last-modified-only
/// URL cannot hide columns that exist only in other objects.
///
/// Lists one object past [`ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT`] so a
/// collection larger than the cap errors instead of publishing a truncated
/// schema. Footer merge for `limit` or fewer files is unchanged.
async fn infer_orc_collection_schema(
    state: &dyn Session,
    table_path: &ListingTableUrl,
    object_store: &Arc<dyn ObjectStore>,
    extension: &str,
    file_format: &Arc<dyn FileFormat>,
) -> Result<SchemaRef, DataFusionError> {
    let files = list_matching_listing_files(
        state,
        table_path,
        object_store.as_ref(),
        extension,
        ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT.saturating_add(1),
    )
    .await?;
    let files = orc_collection_schema_infer_files_within_limit(
        files,
        ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT,
    )?;
    file_format.infer_schema(state, object_store, &files).await
}

/// Accepts objects collected with `limit + 1` so "exactly `limit`" can be
/// distinguished from overflow. More than `limit` matching objects is an
/// error: a later scan reads every match, so a silently truncated schema
/// would omit columns or incompatible types that first appear after the cap.
fn orc_collection_schema_infer_files_within_limit(
    files: Vec<ObjectMeta>,
    limit: usize,
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    if files.len() > limit {
        return Err(DataFusionError::Configuration(
            orc_collection_schema_infer_limit_error(limit),
        ));
    }
    Ok(files)
}

fn orc_collection_schema_infer_limit_error(limit: usize) -> String {
    format!(
        "ORC schema inference found more than {limit} matching objects, so the collection is too large for automatic ORC footer merge. A later scan reads every matching object, and a truncated inferred schema would omit columns or incompatible types that first appear after the cap. See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
}

/// Lists objects under `table_path` whose names match `extension`, stopping
/// after `limit` matches. Callers that must distinguish "exactly `limit`"
/// from "more than `limit`" should pass `limit + 1`.
async fn list_matching_listing_files(
    state: &dyn Session,
    table_path: &ListingTableUrl,
    object_store: &dyn ObjectStore,
    extension: &str,
    limit: usize,
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let mut file_stream = table_path.list_all_files(state, object_store, "").await?;
    let mut files = Vec::new();
    while let Some(file) = file_stream.try_next().await? {
        if file_matches_extension(&file.location, extension) {
            files.push(file);
            if files.len() >= limit {
                break;
            }
        }
    }
    Ok(files)
}

fn to_listing_table_url(
    original_url: &Url,
    path: &Path,
    dataset: &DatasetSpec,
    dataconnector: &str,
) -> DataConnectorResult<SensitiveListingTableUrl> {
    let mut new_url = original_url.clone();
    new_url.set_path(&format!("/{path}"));

    let sensitive_url = ListingTableUrl::parse(&new_url).boxed().context(
        crate::UnableToGetSchemaInternalSnafu {
            dataconnector: dataconnector.to_string(),
            connector_component: ConnectorComponent::from(dataset),
        },
    )?;

    Ok(SensitiveListingTableUrl::new(sensitive_url, new_url))
}

fn sanitize_url(mut url: Url) -> Url {
    url.set_fragment(None);
    url
}

/// Wrapper struct that contains a potentially sensitive URL with fragments containing secrets,
/// and a sanitized URL without the fragments that can be used for logging and error messages.
struct SensitiveListingTableUrl {
    sensitive_url: ListingTableUrl,
    sanitized_url: Url,
}

impl SensitiveListingTableUrl {
    fn new(sensitive_url: ListingTableUrl, url: Url) -> Self {
        Self {
            sensitive_url,
            sanitized_url: sanitize_url(url),
        }
    }

    fn expose_sensitive_url(&self) -> &ListingTableUrl {
        &self.sensitive_url
    }

    fn sanitized_url(&self) -> &Url {
        &self.sanitized_url
    }
}

/// Builds [`TableParquetOptions`] from the runtime configuration.
///
/// Always sets `pushdown_filters = true`. When a runtime is provided, reads
/// `runtime.params.parquet_page_index` (`required` | `auto` | `skip`) and
/// sets `enable_page_index` accordingly. When no runtime is available,
/// `enable_page_index` retains the `DataFusion` default (`true`).
///
/// # Errors
///
/// Returns an error if `runtime.params.parquet_page_index` is not one of the
/// accepted values, or a resulting option is rejected by `DataFusion`.
pub fn build_table_parquet_options(
    app: Option<&Arc<App>>,
) -> std::result::Result<TableParquetOptions, DataFusionError> {
    let mut opts = TableParquetOptions::new();
    opts.set("pushdown_filters", "true")?;

    if let Some(app) = app {
        let page_index_options = parquet_page_index_options(app);
        opts.set(
            "enable_page_index",
            &page_index_options.enable_page_index.to_string(),
        )?;
    }

    Ok(opts)
}

struct ParquetPageIndexOptions {
    enable_page_index: bool,
}

impl Default for ParquetPageIndexOptions {
    fn default() -> Self {
        Self {
            enable_page_index: true,
        }
    }
}

/// Returns the parquet page index options to use when reading Parquet files
///
/// Expects the user to configure the spicepod runtime params:
///
/// ```yaml
/// runtime:
///   params:
///     parquet_page_index: required # skip, auto
/// ```
fn parquet_page_index_options(app: &Arc<App>) -> ParquetPageIndexOptions {
    // `App::get_runtime_param` reads the `Option<Arc<App>>` the runtime stores.
    let app = Some(Arc::clone(app));
    let parquet_page_index_param =
        App::get_runtime_param(&app, "parquet_page_index", "required".to_string());

    match parquet_page_index_param.as_str() {
        // Note: "auto" and "required" both enable page index now. The difference was that "auto"
        // set tolerate_missing_page_index=true, but that option was removed in DataFusion v51.
        // Page index reading now handles missing indexes gracefully by default.
        "auto" | "required" => ParquetPageIndexOptions::default(),
        "skip" => ParquetPageIndexOptions {
            enable_page_index: false,
        },
        _ => {
            tracing::warn!(
                "Invalid value '{parquet_page_index_param}' for runtime.params.parquet_page_index, valid options are: 'auto', 'skip', 'required'. Using 'required'.",
            );
            ParquetPageIndexOptions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, RecordBatch};
    use chrono::{TimeZone, Utc};
    use datafusion::sql::TableReference;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::runtime::Handle;
    use url::Url;

    use crate::listing::LISTING_TABLE_PARAMETERS;
    use crate::{ConnectorParams, DataConnectorFactory};
    use runtime_parameters::ParameterSpec;

    use super::*;

    #[derive(Debug)]
    struct TestConnector {
        params: Parameters,
    }

    impl std::fmt::Display for TestConnector {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestConnector")
        }
    }

    impl DataConnectorFactory for TestConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn create<'a>(
            &'a self,
            params: ConnectorParams,
            _context: &'a dyn ConnectorContext,
        ) -> Pin<Box<dyn Future<Output = crate::NewDataConnectorResult> + Send + 'a>> {
            Box::pin(async move {
                let connector = Self {
                    params: params.parameters,
                };
                Ok(Arc::new(connector) as Arc<dyn DataConnector>)
            })
        }

        fn prefix(&self) -> &'static str {
            "test"
        }

        fn parameters(&self) -> &'static [ParameterSpec] {
            &[]
        }
    }

    impl ListingTableConnector for TestConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_params(&self) -> &Parameters {
            &self.params
        }

        fn get_tokio_io_runtime(&self) -> Handle {
            Handle::current()
        }

        fn get_object_store_url(
            &self,
            dataset: &DatasetSpec,
            url: Option<&str>,
        ) -> DataConnectorResult<Url> {
            let raw = url.unwrap_or(dataset.from.as_str());
            if let Ok(parsed) = Url::parse(raw)
                && parsed.scheme() == "file"
            {
                return Ok(parsed);
            }
            Url::parse("test")
                .boxed()
                .context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "Invalid URL".to_string(),
                })
        }
    }

    const TEST_PARAMETERS: &[ParameterSpec] = LISTING_TABLE_PARAMETERS;

    fn setup_connector(
        path: String,
        params: HashMap<String, String>,
    ) -> (TestConnector, DatasetSpec) {
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "test",
                TEST_PARAMETERS,
            ),
        };

        (
            connector,
            DatasetSpec::new(path, TableReference::bare("test")),
        )
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_require_file_format() {
        let (connector, dataset) = setup_connector("test:test/".to_string(), HashMap::new());

        match connector.get_file_format_and_extension(&dataset).await {
            Ok(_) => panic!("Unexpected success"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Cannot setup the dataset test (TestConnector) with an invalid configuration. The required 'file_format' parameter is missing. Ensure the parameter is provided, and try again."
            ),
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_csv_extension() {
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), HashMap::new());

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_parquet_extension() {
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), HashMap::new());

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_vortex_extension() {
        let (connector, dataset) = setup_connector("test:test.vortex".to_string(), HashMap::new());

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".vortex");
        } else {
            panic!("Unexpected error");
        }
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_get_file_format_and_extension_auto_detects_vortex() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "auto".to_string());
        let (connector, dataset) = setup_connector("test:test.vortex".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".vortex");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_csv_gzip_extension() {
        let (connector, dataset) = setup_connector("test:test.csv.gz".to_string(), HashMap::new());

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".csv.gz");
            assert_eq!(
                file_format.compression_type(),
                Some(FileCompressionType::GZIP)
            );
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_jsonl_zstd_extension() {
        let (connector, dataset) =
            setup_connector("test:test.ndjson.zst".to_string(), HashMap::new());

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".ndjson.zst");
            assert_eq!(
                file_format.compression_type(),
                Some(FileCompressionType::ZSTD)
            );
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_file_extension_param_compression() {
        let mut params = HashMap::new();
        params.insert("file_extension".to_string(), ".jsonl.gz".to_string());
        let (connector, dataset) = setup_connector("test:test/".to_string(), params);

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".jsonl.gz");
            assert_eq!(
                file_format.compression_type(),
                Some(FileCompressionType::GZIP)
            );
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_file_compression_type_sets_extension() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        params.insert("file_compression_type".to_string(), "GZIP".to_string());
        let (connector, dataset) = setup_connector("test:test/".to_string(), params);

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".csv.gz");
            assert_eq!(
                file_format.compression_type(),
                Some(FileCompressionType::GZIP)
            );
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_preserves_explicit_file_extension_with_compression_type()
     {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        params.insert("file_compression_type".to_string(), "GZIP".to_string());
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params);

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".csv");
            assert_eq!(
                file_format.compression_type(),
                Some(FileCompressionType::GZIP)
            );
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_csv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".csv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_tsv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "tsv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".tsv");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_parquet_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params);

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_detect_orc_extension() {
        let (connector, dataset) = setup_connector("test:test.orc".to_string(), HashMap::new());

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".orc");
            assert_eq!(file_format.get_ext(), "orc");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_orc_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params);

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".orc");
            assert_eq!(file_format.get_ext(), "orc");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_auto_detects_orc() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "auto".to_string());
        let (connector, dataset) = setup_connector("test:test.orc".to_string(), params);

        if let Ok((Some(file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".orc");
            assert_eq!(file_format.get_ext(), "orc");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_format_selected_orc() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        params.insert("file_extension".to_string(), "*".to_string());
        let (connector, dataset) = setup_connector("test:test/".to_string(), params);

        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("format-selected ORC listing")
        else {
            panic!("expected an ORC file format from file_format=orc");
        };
        assert_eq!(extension, "*.orc");
        assert_eq!(file_format.get_ext(), "orc");
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_format_selected_parquet() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        params.insert("file_extension".to_string(), "*".to_string());
        let (connector, dataset) = setup_connector("test:test/".to_string(), params);

        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("format-selected Parquet listing")
        else {
            panic!("expected a Parquet file format from file_format=parquet");
        };
        assert_eq!(extension, "*.parquet");
        assert_eq!(file_format.get_ext(), "parquet");
    }

    /// Listing `file_format: orc` must scan an ORC file that was not written by
    /// the production `orc-rust` encoder. The fixture is Apache ORC's Java-produced
    /// `TestOrcFile.test1.orc`. Glue `OrcInputFormat` tables are registered through
    /// this same listing path (`InputFormat::Orc.file_format()` is `"orc"`).
    #[tokio::test]
    async fn listing_connector_scans_an_independently_produced_orc_file() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        let (connector, dataset) = setup_connector("file://unused.orc".to_string(), params);

        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("listing connector selects ORC")
        else {
            panic!("expected an ORC file format from file_format=orc");
        };
        assert_eq!(extension, ".orc");
        assert_eq!(file_format.get_ext(), "orc");

        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/orc/TestOrcFile.test1.orc"
        ));
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test1.orc");
        std::fs::write(&path, bytes).expect("write independently produced ORC fixture");

        let ctx = SessionContext::new();
        let table_url =
            ListingTableUrl::parse(format!("file://{}", path.display())).expect("listing url");
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(ListingOptions::new(file_format).with_file_extension(".orc"))
            .infer_schema(&ctx.state())
            .await
            .expect("infer schema from the independently produced ORC file");
        let table = ListingTable::try_new(config).expect("listing table");
        ctx.register_table("orc_ext", Arc::new(table))
            .expect("register listing table");

        let batches = ctx
            .sql("SELECT COUNT(*) AS n FROM orc_ext")
            .await
            .expect("count sql")
            .collect()
            .await
            .expect("collect count");
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count column");
        assert_eq!(
            counts.value(0),
            2,
            "Apache ORC TestOrcFile.test1.orc contains two rows"
        );

        // `OrcFormat::infer_stats` publishes an exact footer row count, and
        // `AggregateStatistics` can answer unfiltered `COUNT(*)` from those
        // stats alone. Decode a known Java-writer value so this test cannot
        // pass without reading stripe data.
        let decoded = ctx
            .sql("SELECT CAST(string1 AS VARCHAR) AS s FROM orc_ext ORDER BY s")
            .await
            .expect("decode sql")
            .collect()
            .await
            .expect("collect decoded strings");
        let mut strings = Vec::new();
        for batch in &decoded {
            let col = batch.column(0);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..batch.num_rows() {
                    strings.push(arr.value(i).to_string());
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                for i in 0..batch.num_rows() {
                    strings.push(arr.value(i).to_string());
                }
            } else {
                panic!(
                    "string1 decoded as {}, expected Utf8 or Utf8View",
                    col.data_type()
                );
            }
        }
        assert_eq!(
            strings,
            vec!["bye".to_string(), "hi".to_string()],
            "Apache ORC TestOrcFile.test1.orc rows are string1='hi' then 'bye'"
        );
    }

    fn write_orc_fixture(path: &std::path::Path, batch: &RecordBatch) {
        let mut out = Vec::new();
        let mut writer = orc_rust::arrow_writer::ArrowWriterBuilder::new(&mut out, batch.schema())
            .try_build()
            .expect("construct ORC writer");
        writer.write(batch).expect("write ORC batch");
        writer.close().expect("close ORC writer");
        std::fs::write(path, out).expect("write ORC fixture");
    }

    /// Regression: `create_listing_table` used the newest object as
    /// `schema_infer_url`, so `OrcFormat::infer_schema` saw a one-file
    /// slice and dropped columns that exist only in older files.
    #[tokio::test]
    async fn create_listing_table_merges_orc_collection_schemas_not_only_the_newest_file() {
        let dir = tempfile::tempdir().expect("tempdir");

        let id_only_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_only = RecordBatch::try_new(
            Arc::clone(&id_only_schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .expect("id-only batch");

        let both_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let both = RecordBatch::try_new(
            Arc::clone(&both_schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![2])),
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
            ],
        )
        .expect("id+extra batch");

        let older = dir.path().join("older_with_extra.orc");
        let newer = dir.path().join("newer_id_only.orc");
        write_orc_fixture(&older, &both);
        write_orc_fixture(&newer, &id_only);
        let older_mtime = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1);
        let newer_mtime = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
        std::fs::File::options()
            .write(true)
            .open(&older)
            .expect("open older ORC")
            .set_modified(older_mtime)
            .expect("set older mtime");
        std::fs::File::options()
            .write(true)
            .open(&newer)
            .expect("open newer ORC")
            .set_modified(newer_mtime)
            .expect("set newer mtime");

        let table_url = format!("file://{}/", dir.path().display());
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        let (connector, dataset) = setup_connector(table_url.clone(), params);

        let url = Url::parse(&table_url).expect("collection url");
        let table_path = ListingTableUrl::parse(&table_url).expect("listing url");
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(&url, Arc::new(object_store::local::LocalFileSystem::new()));
        let store = ctx
            .runtime_env()
            .object_store(&table_path)
            .expect("local object store");
        let last_modified = get_last_modified(
            "TestConnector".to_string(),
            &dataset,
            ".orc",
            table_path,
            &ctx,
            &store,
        )
        .await
        .expect("newest matching ORC object");
        assert!(
            last_modified
                .location
                .as_ref()
                .ends_with("newer_id_only.orc"),
            "newest object must be the id-only file so last-modified-only infer would drop extra: {}",
            last_modified.location
        );

        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("ORC listing format")
        else {
            panic!("expected an ORC file format");
        };
        assert_eq!(extension, ".orc");

        let newest_only = file_format
            .infer_schema(&ctx.state(), &store, std::slice::from_ref(&last_modified))
            .await
            .expect("infer from the newest object alone");
        assert!(
            newest_only.field_with_name("extra").is_err(),
            "last-modified-only infer must omit extra so this test still detects the production-path bug: {newest_only:?}"
        );

        let provider = connector
            .create_listing_table(&dataset, &url, &extension, file_format)
            .await
            .expect("create_listing_table production path");

        let extra = provider
            .schema()
            .field_with_name("extra")
            .expect("merged schema must include extra from the older file")
            .clone();
        assert!(
            extra.is_nullable(),
            "a column present in only some ORC files is nullable so scan can NULL-fill"
        );

        let query_ctx = SessionContext::new();
        query_ctx
            .register_table("merged", provider)
            .expect("register listing table");
        let batches = query_ctx
            .sql("SELECT id, extra FROM merged ORDER BY id")
            .await
            .expect("select merged columns")
            .collect()
            .await
            .expect("collect merged rows");

        let mut pairs = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("id column");
            let extra_col = batch.column(1);
            for i in 0..batch.num_rows() {
                let extra = if let Some(arr) = extra_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                } else if let Some(arr) = extra_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringViewArray>()
                {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_string())
                    }
                } else {
                    panic!(
                        "extra decoded as {}, expected Utf8 or Utf8View",
                        extra_col.data_type()
                    );
                };
                pairs.push((ids.value(i), extra));
            }
        }
        assert_eq!(
            pairs,
            vec![(1, None), (2, Some("x".to_string()))],
            "production listing path must expose extra and NULL-fill the newer id-only file"
        );
    }

    /// `schema_source_path` only replaces the inference URL. A later scan
    /// still lists `from`, so inferring from a narrower id-only prefix must
    /// not unpublish `extra` from objects outside that prefix.
    #[tokio::test]
    async fn create_listing_table_does_not_infer_orc_schema_from_schema_source_path_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let schema_dir = dir.path().join("schema");
        let data_dir = dir.path().join("data");
        std::fs::create_dir(&schema_dir).expect("schema prefix");
        std::fs::create_dir(&data_dir).expect("data prefix");

        let id_only_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let id_only = RecordBatch::try_new(
            Arc::clone(&id_only_schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .expect("id-only batch");

        let both_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let both = RecordBatch::try_new(
            Arc::clone(&both_schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![2])),
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
            ],
        )
        .expect("id+extra batch");

        write_orc_fixture(&schema_dir.join("id.orc"), &id_only);
        write_orc_fixture(&data_dir.join("id_extra.orc"), &both);

        let table_url = format!("file://{}/", dir.path().display());
        let schema_source_path = format!("file://{}/", schema_dir.display());
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        let (connector, mut dataset) = setup_connector(table_url.clone(), params);
        dataset
            .params
            .insert("schema_source_path".to_string(), schema_source_path);

        let url = Url::parse(&table_url).expect("collection url");
        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("ORC listing format")
        else {
            panic!("expected an ORC file format");
        };
        assert_eq!(extension, ".orc");

        let provider = connector
            .create_listing_table(&dataset, &url, &extension, file_format)
            .await
            .expect("create_listing_table must not publish a schema_source_path prefix schema");

        provider
            .schema()
            .field_with_name("extra")
            .expect("scan-path merge must keep extra from table/data/id_extra.orc");
    }

    #[test]
    fn orc_collection_schema_infer_limit_error_names_the_cap_and_docs() {
        let message =
            orc_collection_schema_infer_limit_error(ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT);
        assert!(
            message.contains(&ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT.to_string()),
            "cap must appear in: {message}"
        );
        assert!(
            !message.contains("schema_source_path"),
            "must not recommend an inference-only prefix: {message}"
        );
        assert!(
            message.contains(
                "https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
            "must link listing connector docs: {message}"
        );
        assert!(
            message.contains("later scan reads every matching object"),
            "must explain why truncated inference is rejected: {message}"
        );
    }

    #[test]
    fn orc_collection_schema_infer_files_within_limit_errors_instead_of_truncating() {
        let within = vec![
            create_meta("table/a.orc", 1, 10),
            create_meta("table/b.orc", 2, 10),
        ];
        let accepted = orc_collection_schema_infer_files_within_limit(within, 2)
            .expect("exactly the cap must still merge footers");
        assert_eq!(accepted.len(), 2);

        let overflow = vec![
            create_meta("table/a.orc", 1, 10),
            create_meta("table/b.orc", 2, 10),
            create_meta("table/c.orc", 3, 10),
        ];
        let err = orc_collection_schema_infer_files_within_limit(overflow, 2)
            .expect_err("one extra matching object must not be silently dropped");
        let DataFusionError::Configuration(message) = err else {
            panic!("cap overflow must be a configuration error, got: {err}");
        };
        assert_eq!(message, orc_collection_schema_infer_limit_error(2));
    }

    /// Regression: listing used to stop at
    /// [`ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT`] and publish a truncated
    /// schema. Scan later reads every matching object, so a column that
    /// first appears after the cap must not be inferred from a silent prefix.
    #[tokio::test]
    async fn infer_orc_collection_schema_errors_when_matching_objects_exceed_the_cap() {
        let url = Url::parse("s3://bucket/table/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url).expect("to parse url");
        let ctx = SessionContext::new();

        let meta_files: Vec<ObjectMeta> = (0..=ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT)
            .map(|i| {
                create_meta(
                    &format!("table/part-{i}.orc"),
                    i64::try_from(i).expect("object index fits i64"),
                    100,
                )
            })
            .collect();
        assert_eq!(
            meta_files.len(),
            ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT + 1,
            "must list one object past the cap so silent truncation would succeed"
        );

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;
        let listed = list_matching_listing_files(
            &ctx.state(),
            &table_path,
            test_store.as_ref(),
            ".orc",
            ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT.saturating_add(1),
        )
        .await
        .expect("list one past the infer cap");
        assert_eq!(
            listed.len(),
            ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT + 1,
            "must fetch one extra matching object so the overflow is visible"
        );

        let file_format: Arc<dyn FileFormat> = Arc::new(OrcFormat::new());
        let err = infer_orc_collection_schema(
            &ctx.state(),
            &table_path,
            &test_store,
            ".orc",
            &file_format,
        )
        .await
        .expect_err("exceeding the infer cap must not silently truncate");

        let DataFusionError::Configuration(message) = err else {
            panic!("cap overflow must be a configuration error, got: {err}");
        };
        assert_eq!(
            message,
            orc_collection_schema_infer_limit_error(ORC_COLLECTION_SCHEMA_INFER_FILE_LIMIT)
        );
    }

    /// Glue `OrcInputFormat` / `MapredParquetInputFormat` tables set
    /// `file_extension=*`, which becomes `*.orc` / `*.parquet`. Hive data
    /// objects are often extensionless (`000000_0`); job markers must not be
    /// opened as ORC.
    #[tokio::test]
    async fn format_selected_listing_scans_extensionless_orc_and_skips_markers() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/orc/TestOrcFile.test1.orc"
        ));
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("000000_0"), bytes)
            .expect("write extensionless Hive ORC object");
        std::fs::write(dir.path().join("_SUCCESS"), b"ok").expect("write _SUCCESS marker");
        std::fs::write(dir.path().join("_committed_000"), b"x").expect("write _committed marker");
        std::fs::write(dir.path().join("_started_000"), b"y").expect("write _started marker");
        std::fs::write(dir.path().join(".$folder$"), b"").expect("write folder placeholder");
        std::fs::write(dir.path().join("notes.txt"), b"not orc").expect("write unrelated file");

        let ctx = SessionContext::new();
        let table_url = format!("file://{}/", dir.path().display());
        let store_url = Url::parse(&table_url).expect("store url");
        let table_path = ListingTableUrl::parse(&table_url).expect("listing url");
        ctx.runtime_env().register_object_store(
            &store_url,
            Arc::new(object_store::local::LocalFileSystem::new()),
        );

        let infer_path =
            ListingTableUrl::parse(format!("file://{}/000000_0", dir.path().display()))
                .expect("infer url");
        let file_format = Arc::new(OrcFormat::new());
        let infer_options = ListingOptions::new(Arc::clone(&file_format) as Arc<dyn FileFormat>)
            .with_file_extension("");
        let schema = infer_options
            .infer_schema(&ctx.state(), &infer_path)
            .await
            .expect("infer schema from the extensionless ORC object");

        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(
                    ListingOptions::new(Arc::clone(&file_format) as Arc<dyn FileFormat>)
                        .with_file_extension(""),
                )
                .with_schema(Arc::clone(&schema)),
        )
        .expect("listing table");

        let provider = MetadataPruningListingTable::new(
            Arc::new(listing),
            ctx.runtime_env()
                .object_store(&table_path)
                .expect("object store"),
            table_path,
            schema,
            "*.orc",
        );
        ctx.register_table("hive_orc", Arc::new(provider))
            .expect("register format-selected listing");

        let decoded = ctx
            .sql("SELECT CAST(string1 AS VARCHAR) AS s FROM hive_orc ORDER BY s")
            .await
            .expect("decode sql")
            .collect()
            .await
            .expect("collect decoded strings");
        let mut strings = Vec::new();
        for batch in &decoded {
            let col = batch.column(0);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..batch.num_rows() {
                    strings.push(arr.value(i).to_string());
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                for i in 0..batch.num_rows() {
                    strings.push(arr.value(i).to_string());
                }
            } else {
                panic!(
                    "string1 decoded as {}, expected Utf8 or Utf8View",
                    col.data_type()
                );
            }
        }
        assert_eq!(
            strings,
            vec!["bye".to_string(), "hi".to_string()],
            "extensionless Hive ORC object must be scanned and markers must be skipped"
        );
    }

    #[derive(Debug)]
    struct TestObjectStore {
        meta: Vec<ObjectMeta>,
    }

    impl TestObjectStore {
        fn new(meta: Vec<ObjectMeta>) -> Self {
            Self { meta }
        }
    }

    impl std::fmt::Display for TestObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestObjectStore")
        }
    }

    #[async_trait]
    impl ObjectStore for TestObjectStore {
        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            stream::iter(self.meta.clone().into_iter().map(Ok)).boxed()
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        async fn get_opts(
            &self,
            _location: &Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }
        fn delete_stream(
            &self,
            _locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }
        async fn copy_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    fn create_meta(location: &str, last_modified_secs: i64, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: Utc
                .timestamp_opt(last_modified_secs, 0)
                .single()
                .expect("valid timestamp"),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[tokio::test]
    async fn test_get_last_modified_returns_latest() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("file_old.parquet", 100, 100),
            create_meta("file_new.parquet", 200, 200),
            create_meta("file_other.csv", 300, 300),
            create_meta("file_other.parquet", 150, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let last_modified = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect("to get last modified");

        assert_eq!(last_modified.location.as_ref(), "file_new.parquet");
    }

    #[test]
    fn format_selected_listing_accepts_extensionless_hive_objects_and_skips_markers() {
        let cases = [
            ("warehouse/table/000000_0", "*.orc", true),
            ("warehouse/table/part-00000.orc", "*.orc", true),
            ("warehouse/table/_SUCCESS", "*.orc", false),
            ("warehouse/table/_committed_000", "*.orc", false),
            ("warehouse/table/_started_000", "*.orc", false),
            ("warehouse/table/.$folder$", "*.orc", false),
            ("warehouse/table/$folder$", "*.orc", false),
            ("warehouse/table/000000_0.crc", "*.orc", false),
            ("warehouse/table/000000_0.CRC", "*.orc", false),
            ("warehouse/table/part-00000.ORC", "*.orc", true),
            ("warehouse/table/notes.txt", "*.orc", false),
            ("warehouse/table/part-00000.parquet", "*.orc", false),
            ("warehouse/table/_temporary/000000_0", "*.orc", false),
            (
                "warehouse/table/dt=__HIVE_DEFAULT_PARTITION__/000000_0",
                "*.orc",
                true,
            ),
            ("warehouse/table/part-00000.parquet", "*.parquet", true),
            ("warehouse/table/000000_0", "*.parquet", true),
            ("warehouse/table/_SUCCESS", "*.parquet", false),
            ("warehouse/table/file.parquet", ".parquet", true),
            ("warehouse/table/000000_0", ".parquet", false),
        ];
        for (path, extension, expected) in cases {
            assert_eq!(
                file_matches_extension(&Path::from(path), extension),
                expected,
                "path={path} extension={extension}"
            );
        }
    }

    fn hive_dt_partition_cols() -> Vec<(String, DataType)> {
        vec![("dt".to_string(), DataType::Utf8)]
    }

    fn format_selected_hive_listing_table(
        ctx: &SessionContext,
        store: Arc<dyn ObjectStore>,
        partition_cols: Vec<(String, DataType)>,
    ) -> MetadataPruningListingTable {
        let table_path = ListingTableUrl::parse("s3://bucket/table/").expect("listing url");
        ctx.runtime_env()
            .register_object_store(table_path.object_store().as_ref(), Arc::clone(&store));

        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let options = ListingOptions::new(Arc::new(OrcFormat::new()) as Arc<dyn FileFormat>)
            .with_file_extension("")
            .with_table_partition_cols(partition_cols);
        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("listing table");
        MetadataPruningListingTable::new(Arc::new(listing), store, table_path, file_schema, "*.orc")
    }

    #[test]
    fn hive_partition_parse_error_names_the_object_and_expected_columns() {
        let message = hive_partition_parse_error("table/late.orc", &["dt"]);
        assert!(
            message.contains("'table/late.orc'"),
            "object location must appear in: {message}"
        );
        assert!(
            message.contains("'dt'"),
            "expected partition columns must appear in: {message}"
        );
        assert!(
            message.contains("`key=value`"),
            "must tell the user how to fix the path: {message}"
        );
        assert!(
            message.contains(
                "https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
            "must link listing connector docs: {message}"
        );
    }

    #[test]
    fn parse_partition_values_reads_hive_key_value_segments() {
        let table_path = ListingTableUrl::parse("s3://bucket/table/").expect("listing url");
        let file = Path::from("table/dt=2024-01-01/good.orc");
        let values = parse_partition_values(&table_path, &file, &hive_dt_partition_cols())
            .expect("well-formed hive path");
        assert_eq!(values, vec!["2024-01-01".to_string()]);
    }

    #[test]
    fn parse_partition_values_errors_when_matching_object_lacks_hive_segments() {
        let table_path = ListingTableUrl::parse("s3://bucket/table/").expect("listing url");
        let file = Path::from("table/late.orc");
        let err = parse_partition_values(&table_path, &file, &hive_dt_partition_cols())
            .expect_err("late.orc has no dt= segment");
        let DataFusionError::Configuration(message) = err else {
            panic!("must be a configuration error, got: {err}");
        };
        assert_eq!(
            message,
            hive_partition_parse_error("table/late.orc", &["dt"])
        );
    }

    /// Regression: format-selected listing used to skip matching objects whose
    /// Hive path lacked the inferred `key=value` segments, so a mixed layout
    /// (`dt=2024-01-01/good.orc` plus sibling `late.orc`) returned only the
    /// well-formed file.
    #[tokio::test]
    async fn format_selected_listing_scan_errors_when_a_matching_object_lacks_hive_partition_segments()
     {
        let ctx = SessionContext::new();
        let store = Arc::new(TestObjectStore::new(vec![
            create_meta("table/dt=2024-01-01/good.orc", 1, 10),
            create_meta("table/late.orc", 2, 10),
            create_meta("table/_SUCCESS", 3, 0),
        ])) as Arc<dyn ObjectStore>;
        let provider = format_selected_hive_listing_table(&ctx, store, hive_dt_partition_cols());

        let err = provider
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect_err("late.orc must fail the scan, not be omitted");
        let DataFusionError::Configuration(message) = err else {
            panic!("must be a configuration error, got: {err}");
        };
        assert_eq!(
            message,
            hive_partition_parse_error("table/late.orc", &["dt"])
        );
    }

    #[tokio::test]
    async fn format_selected_listing_files_include_well_formed_hive_objects_and_skip_markers() {
        let ctx = SessionContext::new();
        let store = Arc::new(TestObjectStore::new(vec![
            create_meta("table/dt=2024-01-01/good.orc", 1, 10),
            create_meta("table/_SUCCESS", 2, 0),
            create_meta("table/notes.txt", 3, 10),
        ])) as Arc<dyn ObjectStore>;
        let provider = format_selected_hive_listing_table(&ctx, store, hive_dt_partition_cols());

        let files = provider
            .format_selected_listing_files(&ctx.state())
            .await
            .expect("well-formed hive object must be listed");
        assert_eq!(
            files.len(),
            1,
            "markers and unmatched extensions must not be treated as partition errors"
        );
        assert_eq!(
            files[0].object_meta.location.as_ref(),
            "table/dt=2024-01-01/good.orc"
        );
        assert_eq!(
            files[0].partition_values,
            vec![ScalarValue::Utf8(Some("2024-01-01".to_string()))]
        );
    }

    #[test]
    fn listing_extension_is_orc_matches_suffix_and_format_selected() {
        assert!(listing_extension_is_orc(".orc"));
        assert!(listing_extension_is_orc(".ORC"));
        assert!(listing_extension_is_orc(".Orc"));
        assert!(listing_extension_is_orc("*.orc"));
        assert!(listing_extension_is_orc("*.ORC"));
        assert!(!listing_extension_is_orc(".parquet"));
        assert!(!listing_extension_is_orc("*.parquet"));
        assert!(!listing_extension_is_orc(".PARQUET"));
        assert!(!listing_extension_is_orc(".csv"));
        assert!(!listing_extension_is_orc("*"));
    }

    #[tokio::test]
    async fn uppercase_file_extension_orc_selects_collection_schema_merge() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "orc".to_string());
        params.insert("file_extension".to_string(), "ORC".to_string());
        let (connector, dataset) = setup_connector("test:test/".to_string(), params);

        let (Some(_file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("ORC listing with file_extension=ORC")
        else {
            panic!("expected an ORC file format");
        };
        assert_eq!(extension, ".ORC", "file_extension keeps the user's casing");
        assert!(
            listing_extension_is_orc(&extension),
            "`.ORC` must take the collection schema-merge path, not newest-object-only"
        );
    }

    #[tokio::test]
    async fn get_last_modified_format_selected_selects_extensionless_hive_object() {
        let url = Url::parse("s3://bucket/table/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/table/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("table/_SUCCESS", 400, 0),
            create_meta("table/_committed_1", 350, 0),
            create_meta("table/.$folder$", 300, 0),
            create_meta("table/notes.txt", 500, 10),
            create_meta("table/000000_0", 200, 100),
        ];
        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let last_modified = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            "*.orc",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect("to select the extensionless Hive object");

        assert_eq!(last_modified.location.as_ref(), "table/000000_0");
    }

    #[tokio::test]
    async fn get_last_modified_format_selected_errors_when_only_markers_exist() {
        let url = Url::parse("s3://bucket/table/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/table/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("table/_SUCCESS", 400, 0),
            create_meta("table/.$folder$", 300, 0),
        ];
        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let err = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            "*.orc",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect_err("markers alone are not a readable listing");

        let DataConnectorError::InvalidConfigurationNoSource { message, .. } = err else {
            panic!("only-markers should be a configuration error, got: {err:?}");
        };
        assert!(
            message.contains(&format!("'{NO_EXTENSION_SENTINEL}'")),
            "extensionless markers must keep the set non-empty via the sentinel: {message}"
        );
        assert!(
            !message.contains("_SUCCESS"),
            "extensionless object names must not appear in the error: {message}"
        );
    }

    #[tokio::test]
    async fn get_last_modified_extensionless_objects_use_one_no_extension_sentinel() {
        // Regression: a large empty Hive table can have tens of thousands of
        // `_committed_*` / `000000_0` objects. Those share one sentinel in the
        // mismatch-error summary instead of one HashSet entry per basename.
        let url = Url::parse("s3://bucket/table/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/table/", TableReference::bare("test"));

        let mut meta_files = vec![
            create_meta("table/_SUCCESS", 400, 0),
            create_meta("table/000000_0", 200, 100),
            create_meta("table/notes.txt", 500, 10),
        ];
        for i in 0_i64..64 {
            meta_files.push(create_meta(&format!("table/_committed_{i}"), 300 + i, 0));
        }
        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let err = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect_err("no object matches .parquet");

        let DataConnectorError::InvalidConfigurationNoSource { message, .. } = err else {
            panic!("extensionless objects must not look like an empty path, got: {err:?}");
        };
        assert!(
            message.contains(&format!("'{NO_EXTENSION_SENTINEL}'")),
            "expected one no-extension sentinel in: {message}"
        );
        assert_eq!(
            message.matches(NO_EXTENSION_SENTINEL).count(),
            1,
            "sentinel must appear once, got: {message}"
        );
        assert!(
            message.contains("'.txt'"),
            "real extensions must still be listed: {message}"
        );
        assert!(
            !message.contains("_committed_")
                && !message.contains("000000_0")
                && !message.contains("_SUCCESS"),
            "extensionless basenames must not appear in the error: {message}"
        );
    }

    #[tokio::test]
    async fn test_get_last_modified_returns_latest_compressed_extension() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("file_old.csv.gz", 100, 100),
            create_meta("file_new.csv.gz", 200, 200),
            create_meta("file_plain.csv", 300, 300),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let last_modified = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".csv.gz",
            table_path,
            &ctx,
            &test_store,
        )
        .await
        .expect("to get last modified");

        assert_eq!(last_modified.location.as_ref(), "file_new.csv.gz");
    }

    #[derive(Debug)]
    struct NoListObjectStore {
        meta: ObjectMeta,
        list_called: std::sync::atomic::AtomicBool,
    }

    impl std::fmt::Display for NoListObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "NoListObjectStore")
        }
    }

    impl NoListObjectStore {
        fn new(meta: ObjectMeta) -> Self {
            Self {
                meta,
                list_called: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for NoListObjectStore {
        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.list_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            panic!("list should not be called for location-pruned scans");
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if !options.head {
                return Err(object_store::Error::NotImplemented {
                    operation: "get without head option".to_string(),
                    implementer: "NoListObjectStore".to_string(),
                });
            }
            Ok(object_store::GetResult {
                payload: object_store::GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                attributes: object_store::Attributes::default(),
                range: 0..0,
                meta: self.meta.clone(),
            })
        }

        fn delete_stream(
            &self,
            _locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.list_called
                .store(true, std::sync::atomic::Ordering::SeqCst);
            panic!("list_with_delimiter should not be called for location-pruned scans");
        }

        async fn copy_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_location_pruning_skips_listing() {
        let ctx = SessionContext::new();
        let no_list_store = Arc::new(NoListObjectStore::new(create_meta(
            "prefix/day=2025-01-01/file.parquet",
            100,
            128,
        )));
        let store_url = Url::parse("s3://bucket").expect("store url");
        ctx.runtime_env().register_object_store(
            &store_url,
            Arc::clone(&no_list_store) as Arc<dyn ObjectStore>,
        );

        let table_path =
            ListingTableUrl::parse("s3://bucket/prefix/").expect("to parse listing table url");
        let file_format = Arc::new(ParquetFormat::default());
        let options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![]);

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow_schema::DataType::Utf8, true),
            MetadataColumn::Location(Some("s3://bucket/".into())).field(),
        ]));

        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("create listing table");

        let provider = MetadataPruningListingTable::new(
            Arc::new(listing),
            ctx.runtime_env()
                .object_store(&table_path)
                .expect("object store"),
            table_path.clone(),
            file_schema,
            ".parquet",
        );

        ctx.register_table("test_table", Arc::new(provider))
            .expect("register table");

        let df = ctx
            .sql("SELECT value FROM test_table WHERE _location = 's3://bucket/prefix/day=2025-01-01/file.parquet'")
            .await
            .expect("execute query");

        // Use create_physical_plan instead of collect — this triggers
        // the scan/listing logic without actually reading parquet data
        let plan = df
            .create_physical_plan()
            .await
            .expect("create physical plan");

        assert_eq!(
            plan.schema().fields().len(),
            1,
            "should project only the 'value' column"
        );
        assert_eq!(
            plan.schema().field(0).name(),
            "value",
            "column should be 'value'"
        );

        assert!(
            !no_list_store
                .list_called
                .load(std::sync::atomic::Ordering::SeqCst),
            "Listing should not be invoked when location predicates are present"
        );
    }

    /// 200 seconds after the Unix epoch, expressed in nanoseconds, as an
    /// `append`-refresh watermark literal carries.
    fn watermark_ts_ns(secs: i64) -> datafusion_expr::Expr {
        datafusion_expr::lit(ScalarValue::TimestampNanosecond(
            Some(secs * 1_000_000_000),
            Some("UTC".into()),
        ))
    }

    fn cast_last_modified_to_ns() -> datafusion_expr::Expr {
        datafusion_expr::Expr::Cast(datafusion_expr::Cast::new(
            Box::new(datafusion_expr::col("_last_modified")),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
        ))
    }

    #[test]
    fn extract_last_modified_predicate_matches_bare_and_cast_forms() {
        use datafusion_expr::Operator;

        // Bare column, as `unwrap_cast_in_comparison` may leave it.
        let bare = extract_last_modified_predicate(&[
            datafusion_expr::col("_last_modified").gt(watermark_ts_ns(200))
        ])
        .expect("bare `_last_modified >` is prunable");
        assert_eq!(bare.len(), 1);
        assert_eq!(bare[0].op, Operator::Gt);
        assert_eq!(bare[0].threshold, Duration::from_secs(200));

        // Cast form, as the refresh predicate emits it verbatim.
        let cast =
            extract_last_modified_predicate(&[cast_last_modified_to_ns().gt(watermark_ts_ns(200))])
                .expect("cast-wrapped `_last_modified >` is prunable");
        assert_eq!(cast[0].op, Operator::Gt);
        assert_eq!(cast[0].threshold, Duration::from_secs(200));

        // Literal on the left flips the operator.
        let flipped = extract_last_modified_predicate(&[
            watermark_ts_ns(200).lt(datafusion_expr::col("_last_modified"))
        ])
        .expect("`literal < _last_modified` is prunable");
        assert_eq!(flipped[0].op, Operator::Gt);

        // `>=`, as a day-granular high-water mark uses.
        let gte = extract_last_modified_predicate(&[
            datafusion_expr::col("_last_modified").gt_eq(watermark_ts_ns(200))
        ])
        .expect("`_last_modified >=` is prunable");
        assert_eq!(gte[0].op, Operator::GtEq);
    }

    #[test]
    fn extract_last_modified_predicate_refuses_disjunction_and_negation() {
        // A single disjunct of an `OR` is not a necessary condition, so pruning
        // on it would drop objects whose rows must still be scanned.
        assert!(
            extract_last_modified_predicate(&[datafusion_expr::col("_last_modified")
                .gt(watermark_ts_ns(200))
                .or(datafusion_expr::col("value").eq(datafusion_expr::lit(1)))])
            .is_none(),
            "`_last_modified` under OR must not prune"
        );

        assert!(
            extract_last_modified_predicate(&[datafusion_expr::Expr::Not(Box::new(
                datafusion_expr::col("_last_modified").gt(watermark_ts_ns(200))
            ))])
            .is_none(),
            "`_last_modified` under NOT must not prune"
        );

        // A comparison against something other than a timestamp literal cannot
        // be evaluated from the listing alone.
        assert!(
            extract_last_modified_predicate(&[
                datafusion_expr::col("_last_modified").gt(datafusion_expr::col("other"))
            ])
            .is_none(),
            "`_last_modified` vs a non-literal must not prune"
        );

        // No `_last_modified` predicate at all: nothing to prune on.
        assert!(
            extract_last_modified_predicate(&[
                datafusion_expr::col("value").eq(datafusion_expr::lit(1))
            ])
            .is_none()
        );
    }

    #[test]
    fn extract_last_modified_predicate_keeps_other_conjuncts_prunable() {
        // Pruning on any conjunct of a top-level AND is safe; the other conjunct
        // is left to the residual `FilterExec`.
        let bounds = extract_last_modified_predicate(&[
            datafusion_expr::col("_last_modified").gt(watermark_ts_ns(200)),
            datafusion_expr::col("value").eq(datafusion_expr::lit(1)),
        ])
        .expect("the `_last_modified` conjunct is still prunable");
        assert_eq!(bounds.len(), 1);
    }

    #[test]
    fn last_modified_meta_passes_compares_as_duration() {
        use datafusion_expr::Operator;

        // 200s watermark; an object at 200s does not pass strict `>` but a later
        // one does. The mtime is taken as a Duration since the epoch.
        let bound = LastModifiedBound {
            op: Operator::Gt,
            threshold: Duration::from_secs(200),
        };
        assert!(!last_modified_meta_passes(
            &create_meta("f", 200, 1),
            &[bound]
        ));
        assert!(last_modified_meta_passes(
            &create_meta("f", 201, 1),
            &[bound]
        ));

        // A microsecond above the watermark passes even though the second-level
        // mtime rounds down — proving the comparison is not truncated to seconds.
        let sub_second = ObjectMeta {
            location: Path::from("f"),
            last_modified: Utc
                .timestamp_opt(200, 1_000)
                .single()
                .expect("valid timestamp"),
            size: 1,
            e_tag: None,
            version: None,
        };
        assert!(last_modified_meta_passes(&sub_second, &[bound]));
    }

    #[test]
    fn extract_last_modified_predicate_rejects_coarsening_cast() {
        fn cast_last_modified_to(unit: arrow_schema::TimeUnit) -> datafusion_expr::Expr {
            datafusion_expr::Expr::Cast(datafusion_expr::Cast::new(
                Box::new(datafusion_expr::col("_last_modified")),
                DataType::Timestamp(unit, Some("UTC".into())),
            ))
        }

        // A coarsening cast (`Second`) truncates the microsecond metadata value,
        // so `CAST(_last_modified AS Timestamp(Second)) = 200s` matches an object
        // at 200.5s that a nanosecond prune would drop. It must not build a
        // bound — the residual filter enforces it instead.
        assert!(
            extract_last_modified_predicate(&[cast_last_modified_to(
                arrow_schema::TimeUnit::Second
            )
            .eq(datafusion_expr::lit(ScalarValue::TimestampSecond(
                Some(200),
                Some("UTC".into())
            )))])
            .is_none(),
            "a Second-precision cast must not prune"
        );
        assert!(
            extract_last_modified_predicate(&[cast_last_modified_to(
                arrow_schema::TimeUnit::Millisecond
            )
            .gt(watermark_ts_ns(200))])
            .is_none(),
            "a Millisecond-precision cast must not prune"
        );

        // Microsecond/nanosecond casts keep the value exactly and stay prunable.
        assert!(
            extract_last_modified_predicate(&[cast_last_modified_to(
                arrow_schema::TimeUnit::Microsecond
            )
            .gt(watermark_ts_ns(200))])
            .is_some(),
            "a Microsecond-precision cast preserves the value and is prunable"
        );
        assert!(
            extract_last_modified_predicate(&[cast_last_modified_to(
                arrow_schema::TimeUnit::Nanosecond
            )
            .gt(watermark_ts_ns(200))])
            .is_some(),
            "a Nanosecond-precision cast preserves the value and is prunable"
        );
    }

    /// Reproduces issue #14264: an `append` refresh over an object-store source
    /// filters on `_last_modified > <watermark>`. An object whose mtime is below
    /// the watermark must be pruned from the listing and never opened, so a
    /// stale corrupt object cannot fail the refresh. On the pre-fix path every
    /// object is listed and opened before the row filter runs, so the corrupt
    /// object errors the scan.
    #[tokio::test]
    async fn append_refresh_prunes_stale_objects_without_opening_them() {
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");

        // Valid, newest object (mtime 300s): the only row a `> 200s` filter keeps.
        let file_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![Arc::new(arrow::array::StringArray::from(vec!["new"]))],
        )
        .expect("valid batch");
        let valid_path = dir.path().join("new.parquet");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&valid_path).expect("create parquet"),
            Arc::clone(&file_schema),
            None,
        )
        .expect("parquet writer");
        writer.write(&batch).expect("write parquet");
        writer.close().expect("close parquet");
        std::fs::File::options()
            .write(true)
            .open(&valid_path)
            .expect("open new.parquet")
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(300))
            .expect("set new mtime");

        // Corrupt, stale object (mtime 100s, below the watermark). A `.parquet`
        // name so it is listed, but not valid Parquet, so opening it errors.
        let corrupt_path = dir.path().join("old_corrupt.parquet");
        std::fs::write(&corrupt_path, b"NOT A PARQUET FILE").expect("write corrupt object");
        std::fs::File::options()
            .write(true)
            .open(&corrupt_path)
            .expect("open old_corrupt.parquet")
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100))
            .expect("set corrupt mtime");

        let table_url = format!("file://{}/", dir.path().display());
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, mut dataset) = setup_connector(table_url.clone(), params);
        dataset.metadata = HashMap::from([(
            MetadataColumn::LastModified.name().to_string(),
            "enabled".to_string(),
        )]);

        let url = Url::parse(&table_url).expect("table url");
        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("parquet listing format")
        else {
            panic!("expected a parquet file format");
        };

        let provider = connector
            .create_listing_table(&dataset, &url, &extension, file_format)
            .await
            .expect("create_listing_table production path");

        let query_ctx = SessionContext::new();
        query_ctx
            .register_table("appended", provider)
            .expect("register listing table");

        // The corrupt object really does break an unpruned read, so the filtered
        // success below can only be the prune skipping it.
        let unfiltered = query_ctx
            .sql("SELECT value FROM appended")
            .await
            .expect("plan full read")
            .collect()
            .await;
        assert!(
            unfiltered.is_err(),
            "the corrupt object must break a full read, else this test cannot prove pruning"
        );

        // 200s watermark, at the column's exact type — the corrupt 100s object
        // is below it and must be pruned.
        let watermark = datafusion_expr::lit(ScalarValue::TimestampMicrosecond(
            Some(200_000_000),
            Some("UTC".into()),
        ));
        let batches = query_ctx
            .table("appended")
            .await
            .expect("open table")
            .filter(datafusion_expr::col("_last_modified").gt(watermark))
            .expect("apply `_last_modified >` filter")
            .collect()
            .await
            .expect("prune must skip the corrupt object below the watermark");

        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 1,
            "only the newest object's row is above the watermark"
        );
        let value = batches[0]
            .column_by_name("value")
            .expect("value column")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("value is Utf8")
            .value(0)
            .to_string();
        assert_eq!(value, "new");
    }

    /// Location predicates used to warn and skip a matching object whose Hive
    /// path could not be parsed, which is the same silent-omit as the
    /// format-selected listing scan.
    #[tokio::test]
    async fn location_predicate_scan_errors_when_hive_partition_segments_are_missing() {
        let ctx = SessionContext::new();
        let no_list_store = Arc::new(NoListObjectStore::new(create_meta(
            "prefix/late.orc",
            100,
            128,
        )));
        let store_url = Url::parse("s3://bucket").expect("store url");
        ctx.runtime_env().register_object_store(
            &store_url,
            Arc::clone(&no_list_store) as Arc<dyn ObjectStore>,
        );

        let table_path =
            ListingTableUrl::parse("s3://bucket/prefix/").expect("to parse listing table url");
        let file_format = Arc::new(ParquetFormat::default());
        let options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(hive_dt_partition_cols());

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("value", arrow_schema::DataType::Utf8, true),
            MetadataColumn::Location(Some("s3://bucket/".into())).field(),
        ]));

        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("create listing table");

        let provider = MetadataPruningListingTable::new(
            Arc::new(listing),
            ctx.runtime_env()
                .object_store(&table_path)
                .expect("object store"),
            table_path,
            file_schema,
            ".parquet",
        );

        ctx.register_table("test_table", Arc::new(provider))
            .expect("register table");

        let df = ctx
            .sql("SELECT value FROM test_table WHERE _location = 's3://bucket/prefix/late.orc'")
            .await
            .expect("execute query");
        let err = df
            .create_physical_plan()
            .await
            .expect_err("late.orc must fail the location-predicate scan, not be omitted");
        let message = err.to_string();
        assert!(
            message.contains(&hive_partition_parse_error("prefix/late.orc", &["dt"])),
            "location-predicate scan must surface the hive parse error, got: {message}"
        );
    }

    /// Regression test for issue where SELECT with metadata columns (like `location`)
    /// appearing before partition columns in the projection would fail with
    /// "column types must match schema types" error.
    ///
    /// The root cause was in `DataFusion`'s `ExtendedColumnProjector::project()` which
    /// inserted partition columns first, then metadata columns, without accounting
    /// for index position shifts when metadata columns had lower schema indices
    /// than partition columns.
    ///
    /// For example, with schema [compression, day, location] where:
    /// - compression is a file column (index 0)
    /// - day is a partition column (index 1)
    /// - location is a metadata column (index 2)
    ///
    /// `SELECT location, day, compression` would request projection [2, 1, 0] which
    /// maps to output positions [0, 1, 2]. The old code would:
    /// 1. Insert partition column `day` at position 1 → [compression, day]
    /// 2. Insert metadata column `location` at position 0 → [location, compression, day]
    ///
    /// But the correct output should be [location, day, compression].
    #[tokio::test]
    async fn test_location_metadata_column_projection_order() {
        use datafusion::parquet::arrow::ArrowWriter;
        use tempfile::TempDir;

        // Create temp directory with hive-partitioned parquet files
        let temp_dir = TempDir::new().expect("create temp dir");
        let partition_dir = temp_dir.path().join("day=2025-01-01");
        std::fs::create_dir_all(&partition_dir).expect("create partition dir");

        // Create a simple parquet file with one column (the partition column comes from path)
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "compression",
            arrow_schema::DataType::Utf8,
            true,
        )]));

        let compression_array = arrow::array::StringArray::from(vec!["gzip"]);
        let batch =
            RecordBatch::try_new(Arc::clone(&file_schema), vec![Arc::new(compression_array)])
                .expect("create batch");

        let parquet_path = partition_dir.join("data.parquet");
        let file = std::fs::File::create(&parquet_path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&file_schema), None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        // Set up DataFusion with the listing table
        let ctx = SessionContext::new();
        let table_url = format!("file://{}/", temp_dir.path().display());
        let store_url = Url::parse(&table_url).expect("parse url");
        let table_path = ListingTableUrl::parse(&table_url).expect("parse listing url");

        // Register the local filesystem object store
        let object_store = object_store::local::LocalFileSystem::new();
        ctx.runtime_env()
            .register_object_store(&store_url, Arc::new(object_store));

        // Create listing options with partition columns and location metadata
        let file_format = Arc::new(ParquetFormat::default());
        let options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("day".to_string(), arrow_schema::DataType::Utf8)])
            .with_metadata_cols(vec![
                datafusion_datasource::metadata::MetadataColumn::Location(Some(
                    table_url.clone().into(),
                )),
            ]);

        // Note: We only provide the file schema here. The ListingTable automatically
        // adds partition columns (day) and metadata columns (location) to form the
        // full table schema.
        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("create listing table");

        let provider = MetadataPruningListingTable::new(
            Arc::new(listing),
            ctx.runtime_env()
                .object_store(&table_path)
                .expect("object store"),
            table_path,
            file_schema,
            ".parquet",
        );

        ctx.register_table("test_table", Arc::new(provider))
            .expect("register table");

        // Test 1: SELECT with location first (this was failing before the fix)
        let df = ctx
            .sql("SELECT _location, day, compression FROM test_table")
            .await
            .expect("execute query");

        let batches: Vec<RecordBatch> = df.collect().await.expect("collect results");
        assert_eq!(batches.len(), 1, "should have one batch");

        let result = &batches[0];
        assert_eq!(result.num_columns(), 3, "should have 3 columns");
        assert_eq!(result.num_rows(), 1, "should have 1 row");

        // Verify column order is correct
        assert_eq!(
            result.schema().field(0).name(),
            "_location",
            "first column should be location"
        );
        assert_eq!(
            result.schema().field(1).name(),
            "day",
            "second column should be day"
        );
        assert_eq!(
            result.schema().field(2).name(),
            "compression",
            "third column should be compression"
        );

        // Verify data types are correct
        let location_col = result
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("location should be string array");
        let day_col = result
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("day should be string array");
        let compression_col = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("compression should be string array");

        // Verify values
        assert!(
            location_col
                .value(0)
                .contains("day=2025-01-01/data.parquet"),
            "location should contain file path, got: {}",
            location_col.value(0)
        );
        assert_eq!(day_col.value(0), "2025-01-01", "day should be 2025-01-01");
        assert_eq!(
            compression_col.value(0),
            "gzip",
            "compression should be gzip"
        );

        // Test 2: SELECT with just location and day (was causing panic before fix)
        let df = ctx
            .sql("SELECT _location, day FROM test_table")
            .await
            .expect("execute query");

        let batches: Vec<RecordBatch> = df.collect().await.expect("collect results");
        assert_eq!(batches.len(), 1, "should have one batch");
        assert_eq!(batches[0].num_columns(), 2, "should have 2 columns");
        assert_eq!(
            batches[0].schema().field(0).name(),
            "_location",
            "first column should be location"
        );
        assert_eq!(
            batches[0].schema().field(1).name(),
            "day",
            "second column should be day"
        );

        // Test 3: SELECT with reversed order (day, location) - should also work
        let df = ctx
            .sql("SELECT day, _location FROM test_table")
            .await
            .expect("execute query");

        let batches: Vec<RecordBatch> = df.collect().await.expect("collect results");
        assert_eq!(batches.len(), 1, "should have one batch");
        assert_eq!(batches[0].num_columns(), 2, "should have 2 columns");
        assert_eq!(
            batches[0].schema().field(0).name(),
            "day",
            "first column should be day"
        );
        assert_eq!(
            batches[0].schema().field(1).name(),
            "_location",
            "second column should be location"
        );
    }

    #[tokio::test]
    async fn test_listing_table_metadata_columns_are_applied() {
        let mut dataset = DatasetSpec::new("s3://bucket/prefix/", TableReference::bare("test"));
        dataset.metadata = HashMap::from([(
            MetadataColumn::Location(None).name().to_string(),
            "enabled".to_string(),
        )]);

        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let schema = Schema::new(vec![Field::new(
            "compression",
            arrow_schema::DataType::Utf8,
            true,
        )]);

        let result = add_metadata_columns_if_required(
            options,
            &Url::parse("s3://bucket/prefix/").expect("parse table url"),
            &schema,
            &dataset,
        );

        assert!(
            !result.metadata_cols.is_empty(),
            "metadata columns should be set on listing options"
        );
    }

    #[tokio::test]
    async fn test_get_last_modified_no_matching_extension() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("file_old.parquet", 100, 100),
            create_meta("file_new.parquet", 200, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".csv",
            table_path,
            &ctx,
            &test_store,
        )
        .await;

        result.expect_err("should error on no matching extension");
    }

    #[tokio::test]
    async fn test_get_last_modified_no_files_is_retriable() {
        // Regression test for the v2.0.0 dataset-init regression: when an
        // object-store path has no files yet (e.g. the source has not been
        // written at startup), get_last_modified must return a RETRIABLE error
        // so the dataset load keeps retrying until the data appears, rather than
        // permanently failing and never retrying. (Restores pre-#10246
        // eventual-readiness for object-store sources.)
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/", TableReference::bare("test"));

        // Empty store: no files at all at the path.
        let test_store = Arc::new(TestObjectStore::new(vec![])) as Arc<dyn ObjectStore>;

        let result = get_last_modified(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            table_path,
            &ctx,
            &test_store,
        )
        .await;

        let err = result.expect_err("should error when no files are present at the path");
        assert!(
            matches!(err, DataConnectorError::ObjectStoreNoFilesAvailable { .. }),
            "no-files-at-path should map to ObjectStoreNoFilesAvailable, got: {err:?}"
        );
        assert!(
            err.is_retriable(),
            "no-files-at-path must be retriable so the dataset load keeps retrying until data appears"
        );
    }

    #[tokio::test]
    async fn test_verify_schema_source_path_valid() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/schema/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("schema/file1.parquet", 100, 100),
            create_meta("schema/file2.csv", 200, 200),
            create_meta("schema/file3.parquet", 300, 300),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path,
            &ctx,
            &test_store,
        )
        .await;

        result.expect("should succeed with matching files");
    }

    #[tokio::test]
    async fn test_verify_schema_source_path_compressed_extension_valid() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/schema/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("schema/file1.csv", 100, 100),
            create_meta("schema/file2.csv.gz", 200, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".csv.gz",
            schema_source_path,
            &ctx,
            &test_store,
        )
        .await;

        result.expect("should succeed with matching compressed files");
    }

    #[tokio::test]
    async fn test_verify_schema_source_path_no_matching_files() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/schema/", TableReference::bare("test"));

        let meta_files = vec![
            create_meta("schema/file1.csv", 100, 100),
            create_meta("schema/file2.csv", 200, 200),
        ];

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path.clone(),
            &ctx,
            &test_store,
        )
        .await;

        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string(),
                format!(
                    "Cannot setup the dataset test (TestListingConnector) with an invalid configuration. Failed to find any files matching the extension '.parquet' at the specified path `{schema_source_path}`. Verify that `schema_source_path` is correct and try again."
                )
            );
        }
    }

    #[tokio::test]
    #[expect(clippy::cast_possible_wrap)]
    async fn test_verify_schema_source_path_file_limit() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let dataset = DatasetSpec::new("s3://bucket/schema/", TableReference::bare("test"));

        // Create more files than SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT
        let meta_files: Vec<ObjectMeta> = (0..SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT + 100)
            .map(|i| create_meta(&format!("schema/file{i}.csv"), 100 + i as i64, 100))
            .collect();

        let test_store = Arc::new(TestObjectStore::new(meta_files)) as Arc<dyn ObjectStore>;

        let result = verify_schema_source_path(
            "TestListingConnector".to_string(),
            &dataset,
            ".parquet",
            schema_source_path,
            &ctx,
            &test_store,
        )
        .await;

        // Should return Ok even though no matching files were found,
        // because we hit the scan limit
        assert!(result.is_ok(), "Expected Ok, got {result:?}");
    }

    #[test]
    fn test_get_url_prefix_basic() {
        let url = Url::parse("s3://mybucket/").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_path() {
        let url = Url::parse("s3://mybucket/folder/file.txt").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_query() {
        let url = Url::parse("s3://mybucket/file.txt?version=1").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_fragment() {
        let url = Url::parse("s3://mybucket/file.txt#section1").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "s3://mybucket/");
    }

    #[test]
    fn test_get_url_prefix_with_port() {
        let url = Url::parse("http://localhost:8080/path").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "http://localhost:8080/");
    }

    #[test]
    fn test_get_url_prefix_without_host() {
        let url = Url::parse("file:///absolute/path").expect("to parse url");
        assert_eq!(get_url_prefix(&url), "file:///");
    }

    #[test]
    fn test_parquet_page_index_options_default() {
        let app = Arc::new(app::AppBuilder::new("test").build());

        let options = parquet_page_index_options(&app);
        assert!(options.enable_page_index);
    }

    #[test]
    fn test_parquet_page_index_options_auto() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "auto".to_string());
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_runtime_params(params)
                .build(),
        );

        // "auto" and "required" now behave the same since tolerate_missing_page_index
        // was removed in DataFusion v51. Page index reading handles missing indexes gracefully.
        let options = parquet_page_index_options(&app);
        assert!(options.enable_page_index);
    }

    #[test]
    fn test_extract_location_predicates_equality() {
        use datafusion_expr::{col, lit};

        let filters = vec![col("_location").eq(lit("s3://bucket/path/file.parquet"))];
        let values = extract_location_predicates(&filters);
        assert_eq!(
            values,
            Some(vec!["s3://bucket/path/file.parquet".to_string()])
        );
    }

    #[test]
    fn test_extract_location_predicates_in_list() {
        use datafusion_expr::{col, lit};

        let filters = vec![col("_location").in_list(
            vec![lit("s3://bucket/a.parquet"), lit("s3://bucket/b.parquet")],
            false,
        )];
        let mut values = extract_location_predicates(&filters).expect("some values");
        values.sort();
        assert_eq!(
            Some(vec![
                "s3://bucket/a.parquet".to_string(),
                "s3://bucket/b.parquet".to_string()
            ]),
            Some(values)
        );
    }

    #[test]
    fn test_extract_location_predicates_reversed_equality() {
        use datafusion_expr::{col, lit};

        let filters = vec![lit("s3://bucket/reversed.parquet").eq(col("_location"))];
        let values = extract_location_predicates(&filters);
        assert_eq!(
            values,
            Some(vec!["s3://bucket/reversed.parquet".to_string()])
        );
    }

    #[test]
    fn test_extract_location_predicates_nested_and_or() {
        use datafusion_expr::{col, lit};

        let filters = vec![
            col("_location")
                .eq(lit("s3://bucket/a.parquet"))
                .and(col("id").gt(lit(1)))
                .or(col("_location").eq(lit("s3://bucket/b.parquet"))),
        ];
        let values = extract_location_predicates(&filters);
        assert!(values.is_none(), "Location under OR should disable pruning");
    }

    #[test]
    fn test_extract_location_predicates_not_wrapped() {
        use datafusion_expr::{col, lit};

        let filters = vec![datafusion_expr::not(
            col("_location").eq(lit("s3://bucket/negated.parquet")),
        )];
        let values = extract_location_predicates(&filters);
        assert!(
            values.is_none(),
            "Location under NOT should disable pruning"
        );
    }

    #[test]
    fn test_extract_location_predicates_ignores_non_location() {
        use datafusion_expr::{col, lit};

        let filters = vec![
            col("id")
                .eq(lit(5))
                .and(col("_location").eq(lit("s3://bucket/only_location.parquet"))),
        ];
        let values = extract_location_predicates(&filters);
        assert_eq!(
            values,
            Some(vec!["s3://bucket/only_location.parquet".to_string()])
        );
    }

    #[test]
    fn test_extract_location_predicates_not_in_list() {
        use datafusion_expr::{col, lit};

        let filters = vec![col("_location").in_list(
            vec![lit("s3://bucket/a.parquet"), lit("s3://bucket/b.parquet")],
            true,
        )];
        let values = extract_location_predicates(&filters);
        assert!(
            values.is_none(),
            "Negated IN should disable location pruning"
        );
    }

    #[test]
    fn test_parquet_page_index_options_skip() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "skip".to_string());
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_runtime_params(params)
                .build(),
        );

        let options = parquet_page_index_options(&app);
        assert!(!options.enable_page_index);
    }

    #[test]
    fn test_parquet_page_index_options_required() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "required".to_string());
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_runtime_params(params)
                .build(),
        );

        let options = parquet_page_index_options(&app);
        assert!(options.enable_page_index);
    }

    #[test]
    fn test_parquet_page_index_options_invalid() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "invalid".to_string());
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_runtime_params(params)
                .build(),
        );

        let options = parquet_page_index_options(&app);
        assert!(
            options.enable_page_index,
            "an invalid value falls back to the default"
        );
    }

    /// An `InMemory` store that records the [`object_store::GetOptions`] of every
    /// read, and reports a version for the object it holds so a reader has something
    /// to pin to.
    ///
    /// `InMemory` itself reports no version, and a reader cannot pin what the store
    /// does not give it — which would make the guard below pass whether or not the
    /// pinning worked.
    #[derive(Debug)]
    struct VersionRecordingStore {
        inner: object_store::memory::InMemory,
        version: String,
        reads: std::sync::Mutex<Vec<object_store::GetOptions>>,
    }

    impl VersionRecordingStore {
        fn new(version: &str) -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                version: version.to_string(),
                reads: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn reads(&self) -> Vec<object_store::GetOptions> {
            self.reads.lock().expect("reads lock").clone()
        }

        /// Forget what has been recorded, so a test can set itself up through the
        /// same store it is about to assert on.
        fn forget_reads(&self) {
            self.reads.lock().expect("reads lock").clear();
        }
    }

    impl std::fmt::Display for VersionRecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "VersionRecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for VersionRecordingStore {
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.reads.lock().expect("reads lock").push(options.clone());
            let mut result = self.inner.get_opts(location, options).await?;
            result.meta.version = Some(self.version.clone());
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Every read a versioned Parquet scan makes has to name the version it started
    /// from, and none of them may be a suffix range.
    ///
    /// The connectors that set `object_versioning_type` — S3, ABFS, GCS, `SharePoint` —
    /// do so because the object under a dataset can be replaced while a scan is in
    /// flight. Pinning makes the reads a consistent view of one version; without it
    /// the footer comes from the old object and the pages from the new one, and the
    /// scan returns rows that were never in either.
    ///
    /// Both halves of that live on forks. `ListingOptions::with_object_versioning_type`
    /// is a `spiceai/datafusion` patch, and the `if_match`/`version` it turns into on
    /// every metadata, byte-range and suffix fetch is a `spiceai/arrow-rs` patch to
    /// `ParquetObjectReader`. The `arrow-rs` half is the dangerous one: a re-cut that
    /// keeps the constructor and drops the pinning still compiles, still scans, and
    /// stops pinning.
    ///
    /// The suffix-range assertion guards the other reason `new_with_meta` exists:
    /// Azure Blob Storage does not serve suffix ranges, so a reader that falls back to
    /// one cannot read Parquet from ABFS at all.
    #[tokio::test]
    async fn a_versioned_parquet_read_pins_every_request_to_one_object_version() {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::arrow::async_reader::{
            ObjectVersionType, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use futures::TryStreamExt;

        const VERSION: &str = "the-version-the-scan-started-from";

        // Two columns, so the data reads are plural and go through
        // `get_byte_ranges` — the override that coalesces them — rather than a
        // single `get_bytes`.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("builds a batch");

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None).expect("parquet writer");
        writer.write(&batch).expect("writes the batch");
        writer.close().expect("closes the file");

        let store = Arc::new(VersionRecordingStore::new(VERSION));
        let location = Path::from("versioned.parquet");
        store
            .put(&location, buffer.into())
            .await
            .expect("stores the file");
        let meta = store.head(&location).await.expect("heads the file");
        // The write and the `head` are the test's own setup, and the `head` reaches
        // `get_opts` too. Only what the reader asks for is under assertion.
        store.forget_reads();
        let store_handle = Arc::clone(&store);

        let reader = ParquetObjectReader::new_with_meta(store as Arc<dyn ObjectStore>, meta)
            .with_object_versioning_type(Some(ObjectVersionType::Version));
        let rows: usize = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .expect("reads the Parquet metadata")
            .build()
            .expect("builds the record-batch stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("reads the data")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            rows, 3,
            "the read has to reach the data, not just the footer"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "the read issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.version.as_deref(),
                Some(VERSION),
                "a read did not pin the object version, so a replacement mid-scan is read as a \
                 mixture of both versions: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(object_store::GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }

    /// A second reader built for the *same* file has to stay on the generation the
    /// first one pinned.
    ///
    /// This is not a hypothetical second reader. A predicate scan reads the
    /// bloom filters through the reader it already has and then builds a fresh one
    /// to decode with, from the same `PartitionedFile` the listing produced — and a
    /// listing from `ListObjectsV2` carries an `ETag` and no version id. The first
    /// reader `HEAD`s to promote a version id from that `ETag` (so page reads pin a
    /// generation rather than a size); the replacement starts from the listing
    /// again and knows nothing of it, so without this patch it falls back to
    /// `If-Match` on the listed `ETag`. Against an object that has since been
    /// replaced the two disagree: the pinned reader keeps reading the generation it
    /// started on and the `If-Match` reader gets a `412`, so the query retries or
    /// fails where it should have completed.
    ///
    /// The whole mechanism is a `spiceai/datafusion` patch to
    /// `CachedParquetFileReaderFactory` — the map from `(location, ETag)` to the
    /// promoted version id, and the two calls that fill it and read it back. It has
    /// no API of its own, so losing it compiles, scans, and silently stops sharing
    /// the pin.
    #[tokio::test]
    async fn a_second_reader_for_the_same_file_keeps_the_version_the_first_one_pinned() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
        use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
        use datafusion::execution::context::SessionContext;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::arrow::async_reader::ObjectVersionType;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

        const VERSION: &str = "the-version-the-head-promoted";

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("builds a batch");

        // Bloom filters written because that is what makes a real scan build the
        // second reader at all; nothing here reads them, so the guard does not
        // depend on the optimiser choosing to.
        let properties = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .build();
        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), Some(properties))
            .expect("parquet writer");
        writer.write(&batch).expect("writes the batch");
        writer.close().expect("closes the file");

        let store = Arc::new(VersionRecordingStore::new(VERSION));
        let location = Path::from("listing/data.parquet");
        store
            .put(&location, buffer.into())
            .await
            .expect("stores the file");

        // The listing a scan actually starts from: an `ETag`, no version id. Taken
        // from the inner store because the wrapper is what adds the version, and
        // adding it here would pin the reader without the patch doing anything.
        let listed = store
            .inner
            .head(&location)
            .await
            .expect("heads the file through the unversioned inner store");
        assert!(
            listed.version.is_none(),
            "this guard needs a listing with no version id"
        );
        assert!(
            listed.e_tag.is_some(),
            "this guard needs a listing that carries an ETag for the HEAD to match against"
        );

        let factory = CachedParquetFileReaderFactory::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            SessionContext::new()
                .runtime_env()
                .cache_manager
                .get_file_metadata_cache(),
        )
        .with_object_versioning_type(Some(ObjectVersionType::Version));
        let file = PartitionedFile::new_from_meta(listed);
        let metrics = ExecutionPlanMetricsSet::new();

        let mut first = factory
            .create_reader(0, file.clone(), None, &metrics)
            .expect("builds the metadata reader");
        first
            .get_metadata(None)
            .await
            .expect("the metadata load must HEAD and promote the listed generation");
        drop(first);
        store.forget_reads();

        let mut replacement = factory
            .create_reader(0, file, None, &metrics)
            .expect("builds the replacement reader");
        replacement
            .get_bytes(0..8)
            .await
            .expect("the replacement reader must be able to read a range");

        let reads = store.reads();
        assert!(
            !reads.is_empty(),
            "the replacement reader issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.version.as_deref(),
                Some(VERSION),
                "the replacement reader did not carry the version the first reader promoted, so \
                 a replaced object answers it with 412 while the first reader reads on: \
                 {options:?}"
            );
            assert!(
                options.if_match.is_none(),
                "a read pinned to a version id must not also send If-Match: {options:?}"
            );
        }
    }

    /// Unversioned buckets still report an `ETag` and never a version id. A
    /// `Version` pin that only sends `version=` is then a no-op; every request
    /// has to carry `If-Match` instead, or a replacement is read as a mixture.
    #[tokio::test]
    async fn a_versioned_parquet_read_pins_by_etag_when_the_listing_has_no_version_id() {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::arrow::async_reader::{
            ObjectVersionType, ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use futures::TryStreamExt;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("builds a batch");

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None).expect("parquet writer");
        writer.write(&batch).expect("writes the batch");
        writer.close().expect("closes the file");

        let store = Arc::new(EtagRecordingStore::new());
        let location = Path::from("unversioned.parquet");
        store
            .put(&location, buffer.into())
            .await
            .expect("stores the file");
        let meta = store.head(&location).await.expect("heads the file");
        let etag = meta
            .e_tag
            .clone()
            .expect("an unversioned listing still carries an ETag");
        assert!(
            meta.version.is_none(),
            "this test needs a listing with no version id"
        );
        store.forget_reads();
        let store_handle = Arc::clone(&store);

        let reader = ParquetObjectReader::new_with_meta(store as Arc<dyn ObjectStore>, meta)
            .with_object_versioning_type(Some(ObjectVersionType::Version));
        let rows: usize = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .expect("reads the Parquet metadata")
            .build()
            .expect("builds the record-batch stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("reads the data")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            rows, 3,
            "the read has to reach the data, not just the footer"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "the read issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.if_match.as_deref(),
                Some(etag.as_str()),
                "a Version pin with no version id must send If-Match, or a replacement mid-scan \
                 is read as a mixture of both generations: {options:?}"
            );
            assert!(
                options.version.is_none(),
                "must not invent a version id the listing did not have: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(object_store::GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }

    /// Like [`VersionRecordingStore`], but the listing has an `ETag` and no version
    /// id — the unversioned-bucket shape.
    #[derive(Debug)]
    struct EtagRecordingStore {
        inner: object_store::memory::InMemory,
        reads: std::sync::Mutex<Vec<object_store::GetOptions>>,
    }

    impl EtagRecordingStore {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                reads: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn reads(&self) -> Vec<object_store::GetOptions> {
            self.reads.lock().expect("reads lock").clone()
        }

        fn forget_reads(&self) {
            self.reads.lock().expect("reads lock").clear();
        }
    }

    impl std::fmt::Display for EtagRecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "EtagRecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for EtagRecordingStore {
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.reads.lock().expect("reads lock").push(options.clone());
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}
