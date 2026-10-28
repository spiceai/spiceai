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

use crate::federated::FederatedTableProvider;
use crate::parameters::ConnectorContext;
use data_components::cdc::{AccelerationContents, ChangesStream};
use data_components::file_schema_drift::{FileSchemaDriftGuard, FileSchemaDriftLayer};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::ops::Deref;
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
#[cfg(test)]
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::{
    FileFormat, csv::CsvFormat, file_compression_type::FileCompressionType, json::JsonFormat,
};
use datafusion::datasource::listing::{
    ListingFileFilter, ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadata, DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
};
use datafusion::execution::cache::default_cache::DefaultCache;
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
use util::levenshtein::closest_match;

use super::parquet_nan::listing_parquet_format;
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

/// Where [`ListingSuffix`] was resolved from. Display and secret-masking
/// follow this origin rather than comparing the suffix string to a parameter:
/// a secret `file_format: csv` still lists as the hardcoded `.csv`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingSuffixOrigin {
    /// Configured `file_extension`.
    FileExtension,
    /// Fallback `.{file_format}` for document and unknown formats.
    FileFormat,
    /// Path suffix or a structured format's hardcoded default (for example `.csv`).
    Default,
}

/// The listing suffix [`ListingTableConnector::get_file_format_and_extension`]
/// resolved, plus the parameter that produced it when one did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListingSuffix {
    value: String,
    origin: ListingSuffixOrigin,
}

impl ListingSuffix {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// How this suffix should appear in a user-facing error or warning.
    #[must_use]
    fn display(&self, params: &Parameters) -> String {
        match self.origin {
            ListingSuffixOrigin::FileExtension => {
                params.display_value("file_extension", &self.value)
            }
            ListingSuffixOrigin::FileFormat => params.display_value("file_format", &self.value),
            ListingSuffixOrigin::Default => self.value.clone(),
        }
    }

    #[must_use]
    fn secret_source<'a>(&self, params: &'a Parameters) -> Option<&'a str> {
        match self.origin {
            ListingSuffixOrigin::FileExtension => params.secret_source("file_extension"),
            ListingSuffixOrigin::FileFormat => params.secret_source("file_format"),
            ListingSuffixOrigin::Default => None,
        }
    }
}

impl Deref for ListingSuffix {
    type Target = str;

    fn deref(&self) -> &str {
        &self.value
    }
}

impl AsRef<str> for ListingSuffix {
    fn as_ref(&self) -> &str {
        &self.value
    }
}

impl Display for ListingSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl PartialEq<str> for ListingSuffix {
    fn eq(&self, other: &str) -> bool {
        self.value == other
    }
}

impl PartialEq<&str> for ListingSuffix {
    fn eq(&self, other: &&str) -> bool {
        self.value == *other
    }
}

impl PartialEq<String> for ListingSuffix {
    fn eq(&self, other: &String) -> bool {
        self.value == *other
    }
}

/// The listing options and file schema a dataset's listing table is built from, as
/// [`ListingTableConnector::listing_table_template`] resolves them.
///
/// [`Self::build`] builds the table over any path from them, so a connector that reads a
/// dataset from more than one location over its lifetime exposes the same schema at each.
#[derive(Clone, Debug)]
pub struct ListingTableTemplate {
    options: ListingOptions,
    /// The columns physically stored in the data files, without partition or metadata
    /// columns.
    schema: SchemaRef,
    object_store: Arc<dyn ObjectStore>,
    /// Listing extension from [`ListingTableConnector::get_file_format_and_extension`].
    extension: String,
    /// The dataset name, for schema-drift messages.
    dataset_name: String,
    /// Whether scans are checked for registered columns missing from every scanned file
    /// (see [`data_components::file_schema_drift`]). Off when `schema_source_path` names the
    /// schema: its columns need not appear in the data files.
    check_schema_drift: bool,
}

impl ListingTableTemplate {
    /// The columns physically stored in the data files, without partition or metadata
    /// columns.
    #[must_use]
    pub fn file_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Builds the dataset's table over `table_path`: a [`ListingTable`] with a
    /// file-statistics cache that selects its data files with [`FormatSelectedDataFiles`] for a
    /// format-selected listing, wrapped for `location` pruning when the dataset has a
    /// `_location` column.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing options do not fit the file schema, such as a metadata
    /// column whose name a file column already uses.
    pub fn build(&self, table_path: ListingTableUrl) -> DFResult<Arc<dyn TableProvider>> {
        let table = Arc::new(self.listing_table(table_path.clone())?);
        Ok(self.wrap(table, table_path))
    }

    fn listing_table(&self, table_path: ListingTableUrl) -> DFResult<ListingTable> {
        let options =
            with_format_selected_file_filter(self.options.clone(), &table_path, &self.extension);
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(Arc::clone(&self.schema));

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
        Ok(ListingTable::try_new(config)?.with_cache(Some(Arc::new(
            DefaultCache::<TableScopedPath, CachedFileMetadata>::new(
                DEFAULT_FILE_STATISTICS_MEMORY_LIMIT,
            )
            .with_name("DefaultFileStatisticsCache"),
        ))))
    }

    fn wrap(
        &self,
        table: Arc<ListingTable>,
        table_path: ListingTableUrl,
    ) -> Arc<dyn TableProvider> {
        let table: Arc<dyn TableProvider> = if has_location_metadata(table.options()) {
            Arc::new(LocationPruningListingTable::new(
                table,
                Arc::clone(&self.object_store),
                table_path,
                Arc::clone(&self.schema),
                self.extension.as_str(),
            ))
        } else {
            table
        };
        match self.schema_drift_guard() {
            Some(guard) => FileSchemaDriftLayer::over(table, guard),
            None => table,
        }
    }

    /// The guard that checks this dataset's scans for registered columns missing from every
    /// scanned file, or `None` when the check is off or the file format cannot be checked.
    fn schema_drift_guard(&self) -> Option<FileSchemaDriftGuard> {
        (self.check_schema_drift
            && FileSchemaDriftGuard::supports_format(self.options.format.as_ref()))
        .then(|| {
            FileSchemaDriftGuard::new(
                self.dataset_name.clone(),
                Arc::clone(&self.schema),
                Arc::clone(&self.options.format),
            )
        })
    }
}

#[derive(Clone, Debug)]
/// Wraps a `ListingTable` to short-circuit broad object-store listings when
/// queries include `location` predicates.
struct LocationPruningListingTable {
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
    /// A `location` predicate names objects without listing them, so the objects it names
    /// are checked against it with [`file_matches_extension`].
    listing_extension: String,
}

impl LocationPruningListingTable {
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

    /// Whether a query can name objects through `_location`, the column
    /// [`extract_location_predicates`] reads. Without it the wrapper passes every call
    /// through to `inner`.
    fn has_location_column(&self) -> bool {
        self.inner.schema().field_with_name("_location").is_ok()
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
            arrow_schema: None,
        })
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

        let table_schema = TableSchema::builder(self.file_schema())
            .with_table_partition_cols(
                partition_fields
                    .iter()
                    .map(|f| Arc::new(f.clone()))
                    .collect::<Vec<_>>(),
            )
            .build();
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
impl TableProvider for LocationPruningListingTable {
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
        let inner_results = self.inner.supports_filters_pushdown(filters)?;
        if !self.has_location_column() {
            return Ok(inner_results);
        }

        // Names of the configured metadata columns other than `_location` (i.e.
        // `_last_modified`, `_size`) — the ones the head()-based fast path prunes on.
        let options = self.inner.options();
        let non_location_metadata: Vec<&str> = options
            .metadata_cols
            .iter()
            .map(datafusion_datasource::metadata::MetadataColumn::name)
            .filter(|name| *name != "_location")
            .collect();

        // `scan` applies exactly two kinds of metadata predicate before opening a file, so
        // only those may be reported `Exact` (which drops the `FilterExec` above the scan):
        //   - `_location`: the fast path head()s exactly those keys;
        //   - `_last_modified`/`_size`: the fast path evaluates them against each head()ed
        //     `ObjectMeta`, and the fall-through path prunes on `inner`'s listing.
        // Everything else (partition and data columns, or a single predicate mixing
        // `_location` with another metadata column) is forced `Inexact` so DataFusion keeps
        // re-applying it above the scan.
        Ok(filters
            .iter()
            .zip(inner_results)
            .map(|(filter, inner_result)| {
                let refs = filter.column_refs();
                let location_only = refs.iter().all(|c| c.name == "_location");
                let non_location_metadata_only = !refs.is_empty()
                    && refs
                        .iter()
                        .all(|c| non_location_metadata.contains(&c.name.as_str()));
                if location_only || non_location_metadata_only {
                    inner_result
                } else {
                    datafusion_expr::TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
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
        // The planner hands a pushed-down conjunction it has proven unsatisfiable to
        // `scan` as a `false` (or NULL) literal: `_location = 'a' AND _location = 'b'`
        // arrives as `[false]`. No row passes it, so answer with an empty scan instead
        // of listing the whole prefix only to prune every file.
        if filters.iter().any(|filter| {
            matches!(
                filter,
                datafusion_expr::Expr::Literal(ScalarValue::Boolean(None | Some(false)), _)
            )
        }) {
            return self
                .scan_partitioned_files(state, Vec::new(), projection, limit)
                .await;
        }

        let locations = if self.has_location_column() {
            extract_location_predicates(filters)
        } else {
            None
        };
        let Some(locations) = locations else {
            return self.inner.scan(state, projection, filters, limit).await;
        };

        // Ensure the query runtime uses the same object store configuration (endpoint/region)
        // as the listing table, even when we bypass listing.
        state.runtime_env().register_object_store(
            self.object_store_url().as_ref(),
            Arc::clone(&self.object_store),
        );

        // Metadata predicates (`_location`, `_last_modified`, `_size`) are computable from each
        // object's `ObjectMeta`, so evaluate them against the head()ed metadata and skip a file
        // before opening it (e.g. `_location = X AND _last_modified > W` head()s X and never
        // GETs it when its mtime fails the bound). `supports_filters_pushdown` reports these
        // `Exact`, so every one must be enforced here.
        //
        // `locations` is only a candidate set: it unions the values of every conjunct, which
        // is a superset of the true matches (`_location = 'A' AND _location = 'B'` heads both
        // and keeps neither). `_location` predicates that the extractor does not understand,
        // such as `LIKE`, are likewise enforced by this check.
        let metadata_cols = &self.inner.options().metadata_cols;
        let metadata_filters: Vec<datafusion_expr::Expr> = filters
            .iter()
            .filter(|f| {
                let refs = f.column_refs();
                !refs.is_empty()
                    && refs
                        .iter()
                        .all(|c| metadata_cols.iter().any(|m| m.name() == c.name))
            })
            .cloned()
            .collect();

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

            // `ListingTable` never reads a zero-byte object (see
            // [`listed_object_is_data_file`]), so a named one holds no rows.
            if meta.size == 0
                || (self.uses_format_selected_listing()
                    && !file_matches_extension(&meta.location, &self.listing_extension))
            {
                continue;
            }

            // Prune by the metadata predicates before opening the object; a file that
            // fails one is never GETed.
            let Some(meta) = datafusion::datasource::listing::helpers::filter_by_metadata(
                meta,
                &metadata_filters,
                metadata_cols,
                state.execution_props(),
            )?
            else {
                continue;
            };

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

    async fn merge_into(
        &self,
        state: &dyn datafusion::catalog::Session,
        source: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        merge_schema: datafusion::common::DFSchemaRef,
        on: datafusion_expr::Expr,
        clauses: Vec<datafusion::logical_expr::dml::MergeIntoClause>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner
            .merge_into(state, source, merge_schema, on, clauses)
            .await
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

    // Drop repeated literals (order preserved) so a file is never scanned twice.
    let mut seen = std::collections::HashSet::new();
    values.retain(|v| seen.insert(v.clone()));

    if values.is_empty() {
        None
    } else {
        Some(values)
    }
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

        let table =
            ObjectStoreMetadataTable::try_new(store, &store_url, Some(extension.to_string()))
                .map_err(|source| {
                    invalid_file_extension_error(
                        format!("{self}"),
                        dataset,
                        self.get_params(),
                        &extension,
                        source,
                    )
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
    /// responses are always of the format `Ok((None, ListingSuffix))`. The data must be UTF8 compatible.
    async fn get_file_format_and_extension(
        &self,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<(Option<Arc<dyn FileFormat>>, ListingSuffix)>
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
        let result: DataConnectorResult<(Option<Arc<dyn FileFormat>>, ListingSuffix)> =
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
                Some(listing_parquet_format(self.get_table_parquet_options(dataset).await?)),
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
                        Some(listing_parquet_format(self.get_table_parquet_options(dataset).await?)),
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
            (Some(format), _) if is_document_file_format(format) => {
                Ok((
                    None,
                    file_format_listing_extension(
                        configured_extension.as_ref(),
                        path_extension.as_ref(),
                        &format!(".{format}"),
                        file_compression_type,
                    ),
                ))
            }
            (Some(format), _) => {
                match classify_unknown_file_format(format) {
                    UnknownFileFormatAction::ConfirmStructuredTypo { suggested } => {
                        if listing_confirms_structured_typo(self, dataset, format, &suggested).await
                        {
                            Err(crate::DataConnectorError::InvalidConfiguration {
                                dataconnector: format!("{self}"),
                                message: unknown_file_format_message(dataset, params, format),
                                connector_component: ConnectorComponent::from(dataset),
                                source: "Unknown file format".into(),
                            })
                        } else {
                            // No listing evidence (or listing unavailable):
                            // keep trunk's text-table fallthrough.
                            tracing::warn!(
                                "{}",
                                unknown_file_format_fallback_warning(
                                    dataset,
                                    params,
                                    format,
                                    Some(suggested.as_str()),
                                )
                            );
                            Ok((
                                None,
                                file_format_listing_extension(
                                    configured_extension.as_ref(),
                                    path_extension.as_ref(),
                                    &format!(".{format}"),
                                    file_compression_type,
                                ),
                            ))
                        }
                    }
                    UnknownFileFormatAction::LoadAsText { hint } => {
                        // Trunk treated unknown formats such as `log`, `xml`,
                        // and `yaml` as a plain-text table. Keep that
                        // fallthrough unless listing evidence confirms a
                        // structured-format typo (the arm above).
                        tracing::warn!(
                            "{}",
                            unknown_file_format_fallback_warning(
                                dataset,
                                params,
                                format,
                                hint.as_deref(),
                            )
                        );
                        Ok((
                            None,
                            file_format_listing_extension(
                                configured_extension.as_ref(),
                                path_extension.as_ref(),
                                &format!(".{format}"),
                                file_compression_type,
                            ),
                        ))
                    }
                }
            }
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
            let (file_format, suffix) = result?;
            Ok((
                file_format,
                ListingSuffix {
                    value: format_selected_listing_extension(suffix.as_str()),
                    origin: suffix.origin,
                },
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
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(
                crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSONL parameter 'schema_infer_max_records' must be an integer, not {}",
                        params.display_value("schema_infer_max_records", infer_max_rec_str)
                    ),
                    connector_component: ConnectorComponent::from(dataset),
                },
            )?;
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
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(
                crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSON parameter 'schema_infer_max_records' must be an integer, not {}",
                        params.display_value("schema_infer_max_records", infer_max_rec_str)
                    ),
                    connector_component: ConnectorComponent::from(dataset),
                },
            )?;
            format = format.with_schema_infer_max_rec(schema_infer_max_rec);
        }

        if let ExposedParamLookup::Present(json_format_str) = params.get("json_format").expose() {
            let json_format = json_format_str.parse::<Format>().boxed().context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSON format: {}, supported formats are: 'json', 'jsonl', 'ndjson', 'ldjson', 'array', 'object', 'soda', 'socrata', 'auto'",
                        params.display_value("json_format", json_format_str)
                    ),
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

    /// Whether this listing connector can produce a [`ChangesStream`].
    ///
    /// Defaults to `false`. The blanket [`DataConnector`] impl forwards this, so
    /// a listing connector that supports CDC (S3 via SQS) must override it —
    /// inheriting the default would report no change stream and refuse
    /// `refresh_mode: changes`.
    fn supports_changes_stream(&self) -> bool {
        false
    }

    /// Fail closed on connector-specific dataset configuration that the default
    /// listing `read_provider` would otherwise accept.
    ///
    /// Called from the blanket [`DataConnector::read_provider`] before the
    /// listing table is built. Defaults to `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns the connector's own configuration error when `dataset` names a
    /// combination it cannot serve, so registration fails with that message
    /// instead of a generic one. The default implementation accepts every
    /// dataset.
    fn validate_dataset(&self, _dataset: &DatasetSpec) -> DataConnectorResult<()> {
        Ok(())
    }

    /// The CDC stream for `dataset`, if this listing connector produces one.
    ///
    /// Wrappers must not inherit a defaulted no-op: the blanket [`DataConnector`]
    /// impl forwards this to the listing connector. See
    /// [`DataConnector::changes_stream`].
    async fn changes_stream(
        &self,
        _context: &dyn ConnectorContext,
        _federated_table: Arc<dyn FederatedTableProvider>,
        _dataset: &DatasetSpec,
        _acceleration: AccelerationContents,
    ) -> Option<ChangesStream> {
        None
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

    /// Build an [`ObjectStoreTextTable`]. `extension` is the listing suffix
    /// (`file_extension` or the path suffix). `parser_key` is the document
    /// parser registry key — typically the lowercased `file_format` — and is
    /// distinct from the listing suffix so `file_format: pdf` plus
    /// `file_extension: .bin` still extracts PDF text.
    async fn create_text_table(
        &self,
        dataset: &DatasetSpec,
        url: &Url,
        extension: &ListingSuffix,
        parser_key: Option<&str>,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let lookup = parser_key.unwrap_or(extension.as_str());
        let content_formatter = document_parse::get_parser_factory(lookup)
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
            .map_err(|source| {
                invalid_file_extension_error(
                    format!("{self}"),
                    dataset,
                    self.get_params(),
                    extension,
                    source,
                )
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
        let template = self
            .listing_table_template(dataset, url, extension, file_format)
            .await?;

        // This shouldn't error because `listing_table_template` already parsed the same URL.
        let table_path =
            ListingTableUrl::parse(url.clone())
                .boxed()
                .context(crate::InternalSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
                })?;

        // This shouldn't error because we're passing the schema and options correctly.
        let table_arc = Arc::new(template.listing_table(table_path.clone()).boxed().context(
            crate::InternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
            },
        )?);

        // For S3 single-file datasets with acceleration enabled, wrap with a caching layer
        // that checks ETag/Version ID to skip unnecessary re-fetches when file hasn't changed.
        if self.supports_single_file_version_cache()
            && refresh_skip_enabled(dataset)
            && !table_path.is_collection()
            && dataset.acceleration.is_some()
            && let Some(cached_table) =
                data_components::s3_single_file_cached::S3SingleFileCached::try_new(
                    Arc::clone(&table_arc),
                    Arc::clone(&template.object_store),
                    dataset.name.to_string(),
                )
        {
            tracing::debug!(
                "Enabled single-file ETag/Version caching for {}",
                dataset.name
            );
            let cached_table = match template.schema_drift_guard() {
                Some(guard) => cached_table.with_schema_drift_guard(guard),
                None => cached_table,
            };
            return Ok(Arc::new(cached_table));
        }

        Ok(template.wrap(table_arc, table_path))
    }

    /// Resolves the listing options and schema [`Self::create_listing_table`] builds the
    /// dataset's table from, without building it.
    ///
    /// A connector whose files are read from a location that changes over the dataset's
    /// lifetime — a path pinned to a source revision — resolves this once and builds the table
    /// at each location with [`ListingTableTemplate::build`], so the dataset keeps the schema
    /// it registered with.
    async fn listing_table_template(
        &self,
        dataset: &DatasetSpec,
        url: &Url,
        extension: &str,
        file_format: Arc<dyn FileFormat>,
    ) -> DataConnectorResult<ListingTableTemplate>
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
        let params = self.get_params();
        let displayed_extension = if params.secret_source("file_extension").is_some() {
            params.display_value("file_extension", extension)
        } else {
            extension.to_string()
        };

        let (schema_infer_url, schema_infer_meta) =
            if table_path.is_collection() && listing_extension_is_orc(extension) {
                // Infer from the scan path. `schema_source_path` only replaces
                // `infer_listing_url` and would publish a prefix schema while
                // `ListingTable` still scans `table_path`.
                get_last_modified(
                    format!("{self}"),
                    dataset,
                    extension,
                    &displayed_extension,
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
                    &displayed_extension,
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
                    &displayed_extension,
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

        // `target_partitions` and `collect_statistics` are read from the scanning
        // session's config at scan time, so they are no longer copied onto the options.
        let mut options = ListingOptions::new(Arc::clone(&file_format))
            .with_file_extension(datafusion_listing_file_extension(extension));

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

        Ok(ListingTableTemplate {
            options,
            schema: final_schema,
            object_store,
            extension: extension.to_string(),
            dataset_name: dataset.name.to_string(),
            // An `ORC` collection infers from the scan path and ignores `schema_source_path`.
            check_schema_drift: (table_path.is_collection() && listing_extension_is_orc(extension))
                || !dataset.params.contains_key("schema_source_path"),
        })
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

    fn supports_changes_stream(&self) -> bool {
        ListingTableConnector::supports_changes_stream(self)
    }

    async fn changes_stream(
        &self,
        context: &dyn ConnectorContext,
        federated_table: Arc<dyn FederatedTableProvider>,
        dataset: &DatasetSpec,
        acceleration: AccelerationContents,
    ) -> Option<ChangesStream> {
        ListingTableConnector::changes_stream(self, context, federated_table, dataset, acceleration)
            .await
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        ListingTableConnector::validate_dataset(self, dataset)?;
        let url = self.get_object_store_url(dataset, None)?;

        let (file_format_opt, extension) = self.get_file_format_and_extension(dataset).await?;
        match file_format_opt {
            None => {
                // Assume its unstructured text data. Use a [`ObjectStoreTextTable`].
                let parser_key = listing_document_parser_key(self.get_params());
                self.create_text_table(dataset, &url, &extension, parser_key.as_deref())
                    .await
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
                    "Invalid file_compression_type: {}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED",
                    params.display_value("file_compression_type", compression)
                ),
                connector_component: ConnectorComponent::from(dataset),
            },
        );
    }

    Ok(detected_compression.unwrap_or(FileCompressionType::UNCOMPRESSED))
}

/// Configuration error for an unusable listing suffix.
///
/// Masking follows [`ListingSuffix`] origin: a secret `file_format` that
/// became `.{format}` is shown as its `${ store:key }` reference, and the
/// regex compiler error is replaced so the resolved pattern cannot leak
/// through [`std::error::Error::source`].
fn invalid_file_extension_error(
    dataconnector: String,
    dataset: &DatasetSpec,
    params: &Parameters,
    suffix: &ListingSuffix,
    source: Box<dyn std::error::Error + Send + Sync>,
) -> crate::DataConnectorError {
    let displayed = suffix.display(params);
    let source = if suffix.secret_source(params).is_some() {
        "invalid regex".into()
    } else {
        source
    };
    crate::DataConnectorError::InvalidConfiguration {
        dataconnector,
        connector_component: ConnectorComponent::from(dataset),
        message: format!(
            "Invalid file extension ({displayed}) for source ({})",
            dataset.name
        ),
        source,
    }
}

/// Document parser registry key for a listing dataset. Distinct from the
/// listing suffix: `file_format: pdf` plus `file_extension: .bin` must still
/// look up the PDF parser.
fn listing_document_parser_key(params: &Parameters) -> Option<String> {
    params
        .get("file_format")
        .expose()
        .ok()
        .map(str::to_ascii_lowercase)
}

fn listing_extension(
    configured_extension: Option<&ParsedFileExtension>,
    path_extension: Option<&ParsedFileExtension>,
    default_extension: &str,
    file_compression_type: FileCompressionType,
) -> ListingSuffix {
    listing_suffix(
        configured_extension,
        path_extension,
        default_extension,
        file_compression_type,
        ListingSuffixOrigin::Default,
    )
}

fn file_format_listing_extension(
    configured_extension: Option<&ParsedFileExtension>,
    path_extension: Option<&ParsedFileExtension>,
    default_extension: &str,
    file_compression_type: FileCompressionType,
) -> ListingSuffix {
    listing_suffix(
        configured_extension,
        path_extension,
        default_extension,
        file_compression_type,
        ListingSuffixOrigin::FileFormat,
    )
}

fn listing_suffix(
    configured_extension: Option<&ParsedFileExtension>,
    path_extension: Option<&ParsedFileExtension>,
    default_extension: &str,
    file_compression_type: FileCompressionType,
    fallback_origin: ListingSuffixOrigin,
) -> ListingSuffix {
    if let Some(extension) = configured_extension {
        return ListingSuffix {
            value: extension.file_extension.clone(),
            origin: ListingSuffixOrigin::FileExtension,
        };
    }

    if let Some(extension) = path_extension
        && extension.format_extension.as_deref() == Some(default_extension.trim_start_matches('.'))
    {
        return ListingSuffix {
            value: extension.file_extension.clone(),
            origin: ListingSuffixOrigin::Default,
        };
    }

    ListingSuffix {
        value: format!("{}{}", default_extension, file_compression_type.get_ext()),
        origin: fallback_origin,
    }
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

const DOCUMENT_FILE_FORMATS: &[&str] = &[
    "docx", "htm", "html", "markdown", "md", "pdf", "pptx", "rst", "text", "txt", "xlsx",
];

/// Common real text and source extensions that `closest_match` can treat as
/// typos of a structured or document format (`org`→`orc`, `css`→`csv`,
/// `tex`→`tsv`/`text`). Fast path: load as text and skip the `Did you mean`
/// hint. The list is not exhaustive — unlisted near-misses still load as
/// text unless listing evidence confirms a typo.
const TEXT_TABLE_FILE_EXTENSIONS: &[&str] = &[
    "css", "doc", "htmlx", "json5", "jsonc", "mdx", "mkd", "org", "ppt", "psv", "qmd", "rmd", "rs",
    "srt", "tex", "ts", "tsx", "xhtml", "xls",
];

fn is_document_file_format(format: &str) -> bool {
    DOCUMENT_FILE_FORMATS.contains(&format)
}

fn is_text_table_file_extension(format: &str) -> bool {
    TEXT_TABLE_FILE_EXTENSIONS.contains(&format)
}

fn structured_file_formats() -> Vec<&'static str> {
    let mut formats = vec![
        "auto", "csv", "json", "jsonl", "ldjson", "ndjson", "orc", "parquet", "soda", "socrata",
        "tsv",
    ];
    #[cfg(not(windows))]
    formats.push("vortex");
    formats
}

fn valid_listing_file_formats() -> Vec<&'static str> {
    let mut formats = structured_file_formats();
    formats.extend_from_slice(DOCUMENT_FILE_FORMATS);
    formats.sort_unstable();
    formats
}

/// What to do with a `file_format` that is neither a structured listing
/// format nor a document format.
#[derive(Debug, PartialEq, Eq)]
enum UnknownFileFormatAction {
    /// Near-miss of a format the listing connector parses natively. Hard-error
    /// only after listing evidence confirms it is a typo.
    ConfirmStructuredTypo { suggested: String },
    /// Trunk-compatible text-table fallthrough. `hint` is a suggested format
    /// to mention in the WARN, if any (document near-miss, or a structured
    /// near-miss that listing has not confirmed).
    LoadAsText { hint: Option<String> },
}

fn classify_unknown_file_format(format: &str) -> UnknownFileFormatAction {
    if is_text_table_file_extension(format) {
        return UnknownFileFormatAction::LoadAsText { hint: None };
    }
    if let Some(suggested) = closest_file_format(format, &structured_file_formats()) {
        return UnknownFileFormatAction::ConfirmStructuredTypo { suggested };
    }
    UnknownFileFormatAction::LoadAsText {
        hint: closest_file_format(format, &valid_listing_file_formats()),
    }
}

/// Whether a listed object's format suffix matches `format` (`csv` matches
/// `orders.csv` and `orders.csv.gz`).
fn object_matches_file_format(location: &str, format: &str) -> bool {
    if detect_file_extension_from_path(location)
        .and_then(|ext| ext.format_extension)
        .is_some_and(|fmt| fmt.eq_ignore_ascii_case(format))
    {
        return true;
    }
    location.rsplit(['/', '\\']).next().is_some_and(|name| {
        name.to_ascii_lowercase()
            .ends_with(&format!(".{}", format.to_ascii_lowercase()))
    })
}

/// True only when a complete listing confirms a typo: no object uses
/// `claimed` as its format suffix, and at least one uses `suggested`.
/// Listing failure, an incomplete scan, or any `claimed` object is not
/// evidence — the caller must WARN and load as text.
async fn listing_confirms_structured_typo<T>(
    connector: &T,
    dataset: &DatasetSpec,
    claimed: &str,
    suggested: &str,
) -> bool
where
    T: ListingTableConnector + Display + ?Sized,
{
    let Ok(url) = connector.get_object_store_url(dataset, None) else {
        return false;
    };
    let Ok(store) = connector.get_object_store(dataset) else {
        return false;
    };
    let Ok(table_path) = ListingTableUrl::parse(url) else {
        return false;
    };
    let ctx = connector.get_session_context();
    let state = ctx.state();
    let Ok(mut file_stream) = table_path.list_all_files(&state, &store, "").await else {
        return false;
    };

    let mut saw_claimed = false;
    let mut saw_suggested = false;
    let mut scanned = 0usize;

    loop {
        match file_stream.try_next().await {
            Ok(Some(file)) => {
                scanned += 1;
                if scanned > SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT {
                    return false;
                }
                let location = file.location.as_ref();
                if object_matches_file_format(location, claimed) {
                    saw_claimed = true;
                    break;
                }
                if object_matches_file_format(location, suggested) {
                    saw_suggested = true;
                }
            }
            Ok(None) => break,
            Err(_) => return false,
        }
    }

    !saw_claimed && saw_suggested
}

/// Closest valid format for a typo.
///
/// [`closest_match`] allows one edit per three characters (minimum 1), so
/// a three-letter adjacent transposition such as `cvs` → `csv` is rejected.
/// Those are the typos this error exists to catch, so an adjacent swap that
/// lands on a valid format is accepted as a fallback.
fn closest_file_format(format: &str, valid: &[&str]) -> Option<String> {
    if let Some(hint) = closest_match(format, valid) {
        return Some(hint);
    }
    adjacent_transposition_match(format, valid)
}

fn adjacent_transposition_match(typo: &str, valid: &[&str]) -> Option<String> {
    let mut chars: Vec<char> = typo.chars().collect();
    for i in 0..chars.len().saturating_sub(1) {
        chars.swap(i, i + 1);
        let candidate: String = chars.iter().collect();
        chars.swap(i, i + 1);
        if let Some(hit) = valid
            .iter()
            .find(|option| option.eq_ignore_ascii_case(&candidate))
        {
            return Some((*hit).to_string());
        }
    }
    None
}

fn unknown_file_format_message(dataset: &DatasetSpec, params: &Parameters, format: &str) -> String {
    let valid = valid_listing_file_formats();
    let valid_list = valid.join(", ");
    let dataset_name = dataset.name.to_string();
    let (format_clause, suggest) = match params.secret_source("file_format") {
        Some(source) => (format!(" from {source}"), false),
        None if params.origins_tracked() => (format!(" '{format}'"), true),
        None => (String::new(), false),
    };
    let suggestion = if suggest {
        closest_file_format(format, &structured_file_formats())
            .map(|hint| format!(" Did you mean `{hint}`?"))
            .unwrap_or_default()
    } else {
        String::new()
    };
    format!(
        "Unknown `file_format`{format_clause} for dataset '{dataset_name}'. Valid formats: {valid_list}.{suggestion} Update `file_format` and retry. See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
}

/// Warning when an unknown `file_format` falls through to the plain-text
/// table (trunk-compatible). `hint` is a suggested format when one exists.
fn unknown_file_format_fallback_warning(
    dataset: &DatasetSpec,
    params: &Parameters,
    format: &str,
    hint: Option<&str>,
) -> String {
    let dataset_name = dataset.name.to_string();
    let (format_clause, suggest) = match params.secret_source("file_format") {
        Some(source) => (format!(" from {source}"), false),
        None if params.origins_tracked() => (format!(" '{format}'"), true),
        None => (String::new(), false),
    };
    let suggestion = if suggest {
        hint.map(|hint| format!(" Did you mean `{hint}`?"))
            .unwrap_or_default()
    } else {
        String::new()
    };
    format!(
        "Unknown `file_format`{format_clause} for dataset '{dataset_name}' is not a recognized structured or document format, so the dataset is loaded as a plain-text table.{suggestion} Set `file_format` to a known format if that is not intended. See: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
    )
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
    displayed_extension: &str,
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

        if listed_object_is_data_file(&file, extension) {
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
                "Spice could not find any files matching the extension '{displayed_extension}' at the specified path."
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
                "Failed to find any files matching the extension '{displayed_extension}'. Is your `file_format` parameter correct? Spice found the following file extensions: {display_extensions}. For details, visit: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
        })
    }
}

async fn verify_schema_source_path(
    dataconnector: String,
    dataset: &DatasetSpec,
    extension: &str,
    displayed_extension: &str,
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
        if listed_object_is_data_file(&file, extension) {
            return Ok(Some(file));
        }

        scanned_files += 1;
        if scanned_files > SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT {
            // We've reached the limit of files to scan, but have not found any with the expected extension.
            // We do warning, not an error, as the dataset might have a large number of files.
            tracing::warn!(
                "Failed to find any files matching the extension '{displayed_extension}' at the specified path `{schema_source_path}` after scanning {SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT} files. Ensure the `schema_source_path` is correct."
            );
            return Ok(None);
        }
    }

    Err(DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: dataconnector.clone(),
        connector_component: ConnectorComponent::from(dataset),
        message: format!(
            "Failed to find any files matching the extension '{displayed_extension}' at the specified path `{schema_source_path}`. Verify that `schema_source_path` is correct and try again."
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

/// Whether the listing table reads the object at `location`, for an `extension`
/// from [`ListingTableConnector::get_file_format_and_extension`].
///
/// A plain extension (`.parquet`, `.csv.gz`) is `DataFusion`'s suffix match. A
/// format-selected one (`*.orc`, `*.parquet`) also accepts extensionless Hive
/// data objects and skips job markers (`_SUCCESS`, `_committed_*`, `.crc`) and
/// `_temporary` staging paths.
#[must_use]
pub fn file_matches_extension(location: &Path, extension: &str) -> bool {
    if let Some(format_ext) = format_selected_data_suffix(extension) {
        return listing_object_matches_format_selected(location, format_ext);
    }
    location.as_ref().ends_with(extension)
}

/// Whether a listed object is a data file the listing table reads for
/// `extension`: non-empty, and named as [`file_matches_extension`] accepts.
///
/// `DataFusion`'s `ListingTable` skips zero-byte objects, so every listing Spice
/// builds itself has to skip them too. An S3 folder marker (key `table/`, 0
/// bytes) lists as `table`, an extensionless name a format-selected listing
/// (`*.parquet`) would otherwise read as a Hive data object and fail on.
fn listed_object_is_data_file(meta: &ObjectMeta, extension: &str) -> bool {
    meta.size > 0 && file_matches_extension(&meta.location, extension)
}

fn has_location_metadata(options: &ListingOptions) -> bool {
    options.metadata_cols.iter().any(|c| {
        matches!(
            c,
            datafusion_datasource::metadata::MetadataColumn::Location(_)
        )
    })
}

/// Attaches [`FormatSelectedDataFiles`] to `options` when `extension` is
/// format-selected (`*.orc`, `*.parquet`). The `ListingTable` built from them
/// then lists exactly the objects [`listed_object_is_data_file`] accepts and
/// scans them on its regular path, with per-file statistics, partition pruning
/// and file splitting.
fn with_format_selected_file_filter(
    options: ListingOptions,
    table_path: &ListingTableUrl,
    extension: &str,
) -> ListingOptions {
    if format_selected_data_suffix(extension).is_none() {
        return options;
    }
    let file_filter = FormatSelectedDataFiles {
        table_path: table_path.clone(),
        extension: extension.to_string(),
        partition_cols: options.table_partition_cols.clone(),
    };
    options.with_file_filter(Some(Arc::new(file_filter)))
}

/// The data files of a format-selected listing: non-empty objects that
/// [`file_matches_extension`] accepts. A selected object outside the table's
/// Hive `key=value` directories fails the listing, where `DataFusion`'s
/// partition parsing would skip it and the scan would return incomplete
/// results.
#[derive(Debug)]
struct FormatSelectedDataFiles {
    table_path: ListingTableUrl,
    extension: String,
    partition_cols: Vec<(String, DataType)>,
}

impl ListingFileFilter for FormatSelectedDataFiles {
    fn is_data_file(&self, object: &ObjectMeta) -> DFResult<bool> {
        if !listed_object_is_data_file(object, &self.extension) {
            return Ok(false);
        }
        if !self.partition_cols.is_empty() {
            parse_partition_values(&self.table_path, &object.location, &self.partition_cols)?;
        }
        Ok(true)
    }

    fn accepts_inserted_files(&self, file_extension: &str) -> bool {
        // An insert names its files `<16 alphanumeric write id>_<n>.<file_extension>`.
        file_matches_extension(
            &Path::from(format!("0000000000000000_0.{file_extension}")),
            &self.extension,
        )
    }
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
        if listed_object_is_data_file(&file, extension) {
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
    use datafusion::common::TableReference;
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
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// Listing connector whose [`ObjectStore`] is injected, so tests can
    /// exercise `list_all_files` without writing thousands of files to disk.
    #[derive(Debug)]
    struct ListingStoreConnector {
        params: Parameters,
        store: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for ListingStoreConnector {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ListingStoreConnector")
        }
    }

    impl ListingTableConnector for ListingStoreConnector {
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
            Url::parse(url.unwrap_or(dataset.from.as_str()))
                .boxed()
                .context(crate::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "Invalid URL".to_string(),
                })
        }

        fn get_object_store(
            &self,
            _dataset: &DatasetSpec,
        ) -> DataConnectorResult<Arc<dyn ObjectStore>> {
            Ok(Arc::clone(&self.store))
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
            )
            .with_tracked_origins(),
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

    fn setup_listed_connector(
        filenames: &[&str],
        format: &str,
    ) -> (TestConnector, DatasetSpec, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in filenames {
            std::fs::write(dir.path().join(name), "hello").expect("write listed object");
        }
        let (connector, dataset) = setup_connector(
            format!("file://{}/", dir.path().display()),
            HashMap::from([("file_format".to_string(), format.to_string())]),
        );
        (connector, dataset, dir)
    }

    async fn assert_text_table_format(connector: &TestConnector, dataset: &DatasetSpec, ext: &str) {
        let (file_format, extension) = connector
            .get_file_format_and_extension(dataset)
            .await
            .unwrap_or_else(|err| panic!("{ext} must keep the text-table path: {err}"));
        assert!(file_format.is_none(), "{ext} must use the text-table path");
        assert_eq!(extension, format!(".{ext}"));
    }

    #[tokio::test]
    async fn test_unknown_file_format_cvs_over_csv_is_rejected_with_suggestion() {
        let (connector, dataset, _dir) = setup_listed_connector(&["orders.csv"], "cvs");

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("cvs over .csv files must fail dataset setup");
        let message = err.to_string();
        assert!(
            message.contains("Unknown `file_format` 'cvs'"),
            "error must name the invalid format: {message}"
        );
        assert!(
            message.contains("Did you mean `csv`?"),
            "error must suggest the closest valid format: {message}"
        );
        assert!(
            message.contains("Valid formats:"),
            "error must list valid formats: {message}"
        );
        assert!(
            message.contains(
                "https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
            "error must link to the file-format docs: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_parqet_over_parquet_is_rejected_with_suggestion() {
        let (connector, dataset, _dir) = setup_listed_connector(&["data.parquet"], "parqet");

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("parqet over .parquet files must fail dataset setup");
        let message = err.to_string();
        assert!(
            message.contains("Unknown `file_format` 'parqet'"),
            "error must name the invalid format: {message}"
        );
        assert!(
            message.contains("Did you mean `parquet`?"),
            "error must suggest parquet: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_jsno_over_json_is_rejected_with_suggestion() {
        let (connector, dataset, _dir) = setup_listed_connector(&["data.json"], "jsno");

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("jsno over .json files must fail dataset setup");
        let message = err.to_string();
        assert!(
            message.contains("Unknown `file_format` 'jsno'"),
            "error must name the invalid format: {message}"
        );
        assert!(
            message.contains("Did you mean `json`?"),
            "error must suggest json: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_parquett_over_parquet_is_rejected_with_suggestion() {
        let (connector, dataset, _dir) = setup_listed_connector(&["data.parquet"], "parquett");

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("parquett over .parquet files must fail dataset setup");
        let message = err.to_string();
        assert!(
            message.contains("Unknown `file_format` 'parquett'"),
            "error must name the invalid format: {message}"
        );
        assert!(
            message.contains("Did you mean `parquet`?"),
            "error must suggest parquet: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_real_text_near_misses_load_over_matching_files() {
        for format in ["cs", "csx", "rc", "jsonld"] {
            let (connector, dataset, _dir) =
                setup_listed_connector(&[format!("notes.{format}").as_str()], format);
            assert_text_table_format(&connector, &dataset, format).await;
            let warning = unknown_file_format_fallback_warning(
                &dataset,
                connector.get_params(),
                format,
                closest_file_format(format, &structured_file_formats()).as_deref(),
            );
            assert!(
                warning.contains("Did you mean"),
                "{format} is an unlisted near-miss and must WARN with a hint: {warning}"
            );
            assert!(
                warning.contains("plain-text table"),
                "{format} must stay on the text fallthrough: {warning}"
            );
        }
    }

    #[tokio::test]
    async fn test_unknown_file_format_cvs_over_only_cvs_files_loads_as_text() {
        let (connector, dataset, _dir) = setup_listed_connector(&["notes.cvs"], "cvs");
        assert_text_table_format(&connector, &dataset, "cvs").await;
        let warning = unknown_file_format_fallback_warning(
            &dataset,
            connector.get_params(),
            "cvs",
            Some("csv"),
        );
        assert!(
            warning.contains("Did you mean `csv`?"),
            "unconfirmed cvs must WARN with the hint: {warning}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_cvs_over_mixed_claimed_and_suggested_loads_as_text() {
        let (connector, dataset, _dir) =
            setup_listed_connector(&["notes.cvs", "orders.csv"], "cvs");
        assert_text_table_format(&connector, &dataset, "cvs").await;
    }

    #[tokio::test]
    async fn test_unknown_file_format_cvs_over_csv_gz_is_rejected_with_suggestion() {
        let (connector, dataset, _dir) = setup_listed_connector(&["orders.csv.gz"], "cvs");
        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("cvs over .csv.gz must fail dataset setup");
        let message = err.to_string();
        assert!(
            message.contains("Unknown `file_format` 'cvs'"),
            "error must name the invalid format: {message}"
        );
        assert!(
            message.contains("Did you mean `csv`?"),
            "error must suggest csv: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_cvs_without_listing_degrades_to_text() {
        let (connector, dataset) = setup_connector(
            "test:test/".to_string(),
            HashMap::from([("file_format".to_string(), "cvs".to_string())]),
        );
        assert_text_table_format(&connector, &dataset, "cvs").await;
    }

    #[tokio::test]
    async fn test_unknown_file_format_incomplete_scan_loads_as_text() {
        let meta_files: Vec<ObjectMeta> = (0..=SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT)
            .map(|i| create_meta(&format!("file{i}.csv"), 100, 100))
            .collect();
        let store = Arc::new(TestObjectStore::new(meta_files));
        let connector = ListingStoreConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([(
                    "file_format".to_string(),
                    "cvs".to_string(),
                )]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_tracked_origins(),
            store: Arc::clone(&store) as Arc<dyn ObjectStore>,
        };
        let dataset = DatasetSpec::new("file:///unused/".to_string(), TableReference::bare("test"));

        assert!(
            !listing_confirms_structured_typo(&connector, &dataset, "cvs", "csv").await,
            "hitting the scan cap is not listing confirmation"
        );
        assert!(
            store.list_calls() >= 1,
            "incomplete-scan coverage must list objects, not degrade on a store error"
        );

        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .unwrap_or_else(|err| panic!("cvs must keep the text-table path: {err}"));
        assert!(file_format.is_none(), "cvs must use the text-table path");
        assert_eq!(extension, ".cvs");
        let warning = unknown_file_format_fallback_warning(
            &dataset,
            connector.get_params(),
            "cvs",
            Some("csv"),
        );
        assert!(
            warning.contains("plain-text table"),
            "incomplete scan must stay on the text fallthrough: {warning}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_text_extensions_still_load() {
        for format in ["tex", "org", "json5", "jsonc", "mdx", "css", "ts", "psv"] {
            let (connector, dataset) = setup_connector(
                "test:test/".to_string(),
                HashMap::from([("file_format".to_string(), format.to_string())]),
            );
            let (file_format, extension) = connector
                .get_file_format_and_extension(&dataset)
                .await
                .unwrap_or_else(|err| panic!("{format} must keep the text-table path: {err}"));
            assert!(
                file_format.is_none(),
                "{format} must use the text-table path"
            );
            assert_eq!(extension, format!(".{format}"));
            let warning =
                unknown_file_format_fallback_warning(&dataset, &connector.params, format, None);
            assert!(
                !warning.contains("Did you mean"),
                "{format} is a real text extension and must not suggest a structured format: {warning}"
            );
        }
    }

    #[tokio::test]
    async fn test_text_file_format_honors_file_extension_tex() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("notes.tex"), "hello").expect("write tex");
        std::fs::write(dir.path().join("notes.md"), "skip").expect("write md");

        let (connector, dataset) = setup_connector(
            format!("test:{}/", dir.path().display()),
            HashMap::from([
                ("file_format".to_string(), "text".to_string()),
                ("file_extension".to_string(), ".tex".to_string()),
            ]),
        );
        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("file_format text plus file_extension .tex must succeed");
        assert!(file_format.is_none(), "text stays on the text-table path");
        assert_eq!(extension, ".tex");

        let mut matched = Vec::new();
        for entry in std::fs::read_dir(dir.path()).expect("read_dir") {
            let name = entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned();
            if file_matches_extension(&Path::from(name.as_str()), &extension) {
                matched.push(name);
            }
        }
        matched.sort_unstable();
        assert_eq!(
            matched,
            vec!["notes.tex".to_string()],
            "file_extension .tex must list only *.tex files"
        );
    }

    #[test]
    fn classify_unknown_file_format_allowlist_loads_as_text() {
        for format in TEXT_TABLE_FILE_EXTENSIONS {
            assert_eq!(
                classify_unknown_file_format(format),
                UnknownFileFormatAction::LoadAsText { hint: None },
                "{format} is a real text extension"
            );
        }
    }

    #[test]
    fn classify_unknown_file_format_structured_near_misses_need_listing() {
        for (format, suggested) in [
            ("cvs", "csv"),
            ("parqet", "parquet"),
            ("jsno", "json"),
            ("parquett", "parquet"),
            ("cs", "csv"),
            ("csx", "csv"),
            ("rc", "orc"),
            ("jsonld", "jsonl"),
        ] {
            assert_eq!(
                classify_unknown_file_format(format),
                UnknownFileFormatAction::ConfirmStructuredTypo {
                    suggested: suggested.to_string()
                },
                "{format} is a structured near-miss; listing decides"
            );
        }
    }

    #[test]
    fn object_matches_file_format_suffix_and_gzip() {
        assert!(object_matches_file_format("orders.csv", "csv"));
        assert!(object_matches_file_format("orders.CSV", "csv"));
        assert!(object_matches_file_format("/data/orders.csv.gz", "csv"));
        assert!(object_matches_file_format("notes.jsonld", "jsonld"));
        assert!(!object_matches_file_format("notes.jsonld", "jsonl"));
        assert!(!object_matches_file_format("notes.cs", "csv"));
        assert!(!object_matches_file_format("orders.csv", "cs"));
        assert!(!object_matches_file_format("notes.cvs", "csv"));
    }

    #[test]
    fn classify_unknown_file_format_document_near_miss_warns_with_hint() {
        assert_eq!(
            classify_unknown_file_format("mardown"),
            UnknownFileFormatAction::LoadAsText {
                hint: Some("markdown".to_string())
            }
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_mardown_loads_with_did_you_mean() {
        let (connector, dataset) = setup_connector(
            "test:test/".to_string(),
            HashMap::from([("file_format".to_string(), "mardown".to_string())]),
        );
        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("document near-miss must keep the text-table path");
        assert!(file_format.is_none());
        assert_eq!(extension, ".mardown");
        let warning = unknown_file_format_fallback_warning(
            &dataset,
            &connector.params,
            "mardown",
            Some("markdown"),
        );
        assert!(
            warning.contains("Did you mean `markdown`?"),
            "document near-miss WARN must include the hint: {warning}"
        );
        assert!(
            warning.contains("plain-text table"),
            "document near-miss WARN must keep the text fallthrough: {warning}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_log_falls_through_to_text_table() {
        let (connector, dataset) = setup_connector(
            "test:test/".to_string(),
            HashMap::from([("file_format".to_string(), "log".to_string())]),
        );

        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("unknown non-near-miss file_format must keep the text-table path");
        assert!(
            file_format.is_none(),
            "log is not a near-miss of a known format and must use the text-table path"
        );
        assert_eq!(extension, ".log");
        let warning =
            unknown_file_format_fallback_warning(&dataset, &connector.params, "log", None);
        assert!(
            warning.contains("Unknown `file_format` 'log'"),
            "fallback warning must name the format: {warning}"
        );
        assert!(
            warning.contains("plain-text table"),
            "fallback warning must say the dataset is loaded as a plain-text table: {warning}"
        );
        assert!(
            warning.contains(
                "https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
            ),
            "fallback warning must link to the file-format docs: {warning}"
        );
    }

    #[tokio::test]
    async fn test_document_file_format_md_still_uses_text_table() {
        let (connector, dataset) = setup_connector(
            "test:test/".to_string(),
            HashMap::from([("file_format".to_string(), "md".to_string())]),
        );

        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("md is a valid document format");
        assert!(
            file_format.is_none(),
            "document formats must keep the text-table path"
        );
        assert_eq!(extension, ".md");
    }

    #[tokio::test]
    async fn test_document_file_format_pdf_still_uses_text_table() {
        let (connector, dataset) = setup_connector(
            "test:test/".to_string(),
            HashMap::from([("file_format".to_string(), "pdf".to_string())]),
        );

        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("pdf is a valid document format");
        assert!(file_format.is_none());
        assert_eq!(extension, ".pdf");
    }

    /// A minimal, well-formed single-page PDF whose content stream draws the
    /// text "Spice Hello". Same fixture as `document_parse::pdf` tests.
    const SAMPLE_PDF: &[u8] = include_bytes!("../../../document_parse/tests/fixtures/hello.pdf");

    async fn create_document_text_table(
        connector: &TestConnector,
        dataset: &DatasetSpec,
        expected_extension: &str,
    ) -> Arc<dyn TableProvider> {
        document_parse::register_all().await;
        let url = connector
            .get_object_store_url(dataset, None)
            .expect("dataset url");
        let (file_format, extension) = connector
            .get_file_format_and_extension(dataset)
            .await
            .expect("document file_format must stay on the text-table path");
        assert!(
            file_format.is_none(),
            "document formats must keep the text-table path"
        );
        assert_eq!(extension, expected_extension);
        let parser_key = listing_document_parser_key(connector.get_params());
        connector
            .create_text_table(dataset, &url, &extension, parser_key.as_deref())
            .await
            .expect("create text table")
    }

    async fn scan_text_table_content(table: Arc<dyn TableProvider>) -> String {
        let ctx = SessionContext::new();
        ctx.register_table("docs", table)
            .expect("register text table");
        let batches = ctx
            .sql("SELECT content FROM docs")
            .await
            .expect("plan content scan")
            .collect()
            .await
            .expect("collect content");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "expected one document row, got {batches:?}");
        let col = batches[0]
            .column_by_name("content")
            .expect("content column");
        let values = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("content is utf8");
        values.value(0).to_string()
    }

    #[tokio::test]
    async fn test_document_file_format_pdf_with_bin_extension_extracts_text() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("hello.bin"), SAMPLE_PDF).expect("write pdf as bin");

        let (connector, dataset) = setup_connector(
            format!("file://{}/", dir.path().display()),
            HashMap::from([
                ("file_format".to_string(), "pdf".to_string()),
                ("file_extension".to_string(), ".bin".to_string()),
            ]),
        );

        let table = create_document_text_table(&connector, &dataset, ".bin").await;
        let content = scan_text_table_content(table).await;
        assert!(
            content.contains("Spice"),
            "pdf + .bin must extract text, got: {content:?}"
        );
        assert!(
            !content.contains("%PDF"),
            "pdf + .bin must not UTF-8-decode raw PDF bytes, got: {content:?}"
        );
    }

    #[tokio::test]
    async fn test_document_file_format_pdf_uppercase_path_is_parsed() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a.PDF"), SAMPLE_PDF).expect("write uppercase PDF");

        let (connector, dataset) = setup_connector(
            format!("file://{}/a.PDF", dir.path().display()),
            HashMap::from([("file_format".to_string(), "pdf".to_string())]),
        );

        let table = create_document_text_table(&connector, &dataset, ".PDF").await;
        let content = scan_text_table_content(table).await;
        assert!(
            content.contains("Spice"),
            "uppercase .PDF path must be parsed, got: {content:?}"
        );
        assert!(
            !content.contains("%PDF"),
            "uppercase .PDF path must not UTF-8-decode raw PDF bytes, got: {content:?}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_from_secret_does_not_echo_value() {
        const CANARY: &str = "cvs";
        const REFERENCE: &str = "${ env:FILE_FMT }";
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("orders.csv"), "hello").expect("write csv");
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([(
                    "file_format".to_string(),
                    CANARY.to_string(),
                )]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_format".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new(
            format!("file://{}/", dir.path().display()),
            TableReference::bare("test"),
        );

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("listing-confirmed secret-derived file_format must fail");
        let message = err.to_string();
        assert!(
            !message.contains(CANARY),
            "unknown file_format errors must not echo a resolved secret: {message}"
        );
        assert!(
            message.contains(REFERENCE),
            "unknown file_format errors must name the secret reference: {message}"
        );
        assert!(
            !message.contains("Did you mean"),
            "do not suggest a correction from a secret-derived value: {message}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_from_secret_without_listing_does_not_echo_value() {
        const CANARY: &str = "cvs";
        const REFERENCE: &str = "${ env:FILE_FMT }";
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([(
                    "file_format".to_string(),
                    CANARY.to_string(),
                )]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_format".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new("test:test/".to_string(), TableReference::bare("test"));

        assert_text_table_format(&connector, &dataset, CANARY).await;
        let warning =
            unknown_file_format_fallback_warning(&dataset, &connector.params, CANARY, Some("csv"));
        assert!(
            !warning.contains(CANARY),
            "unconfirmed secret-derived fallthrough must not echo a resolved secret: {warning}"
        );
        assert!(
            warning.contains(REFERENCE),
            "unconfirmed secret-derived fallthrough must name the secret reference: {warning}"
        );
        assert!(
            !warning.contains("Did you mean"),
            "do not suggest a correction from a secret-derived value: {warning}"
        );
    }

    #[tokio::test]
    async fn test_unknown_file_format_secret_non_near_miss_falls_through_without_echo() {
        const CANARY: &str = "CANARYPGPW7731";
        const REFERENCE: &str = "${ env:FILE_FMT }";
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([(
                    "file_format".to_string(),
                    CANARY.to_string(),
                )]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_format".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new("test:test/".to_string(), TableReference::bare("test"));

        let (file_format, _extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("non-near-miss secret-derived file_format must fall through");
        assert!(file_format.is_none());
        let warning =
            unknown_file_format_fallback_warning(&dataset, &connector.params, CANARY, None);
        assert!(
            !warning.contains(CANARY),
            "fallback warning must not echo a resolved secret: {warning}"
        );
        assert!(
            warning.contains(REFERENCE),
            "fallback warning must name the secret reference: {warning}"
        );
    }

    #[tokio::test]
    async fn test_invalid_file_compression_type_from_secret_does_not_echo_value() {
        const CANARY: &str = "CANARYPGPW7731";
        const REFERENCE: &str = "${ env:PG_PW }";
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([
                    ("file_format".to_string(), "csv".to_string()),
                    ("file_compression_type".to_string(), CANARY.to_string()),
                ]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_compression_type".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new("test:test/".to_string(), TableReference::bare("test"));

        let err = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect_err("secret-derived compression must fail");
        let message = err.to_string();
        assert!(
            !message.contains(CANARY),
            "file_compression_type errors must not echo a resolved secret: {message}"
        );
        assert!(
            message.contains(REFERENCE),
            "file_compression_type errors must name the secret reference: {message}"
        );
    }

    fn error_chain_text(err: &dyn std::error::Error) -> String {
        let mut out = err.to_string();
        let mut cur = err.source();
        while let Some(inner) = cur {
            out.push('\n');
            out.push_str(&inner.to_string());
            cur = inner.source();
        }
        out
    }

    fn secret_invalid_file_extension_connector(
        format: &str,
    ) -> (TestConnector, DatasetSpec, tempfile::TempDir) {
        const CANARY: &str = ".CANARYPGPW7731[";
        const REFERENCE: &str = "${ env:SECRET }";
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("notes.txt"), "hello").expect("write listed object");
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([
                    ("file_format".to_string(), format.to_string()),
                    ("file_extension".to_string(), CANARY.to_string()),
                ]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_extension".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new(
            format!("file://{}/", dir.path().display()),
            TableReference::bare("test"),
        );
        (connector, dataset, dir)
    }

    fn assert_invalid_file_extension_hides_secret(err: &DataConnectorError) {
        const CANARY: &str = "CANARYPGPW7731";
        const REFERENCE: &str = "${ env:SECRET }";
        assert_error_hides_canary(err, CANARY, REFERENCE);
    }

    fn assert_error_hides_canary(err: &DataConnectorError, canary: &str, reference: &str) {
        let chain = error_chain_text(err);
        let debug = format!("{err:?}");
        assert!(
            chain.contains(reference),
            "error must name the secret reference: {chain}"
        );
        assert!(
            !chain.contains(canary),
            "error Display/source must not echo a resolved secret: {chain}"
        );
        assert!(
            !debug.contains(canary),
            "error Debug must not echo a resolved secret: {debug}"
        );
    }

    #[tokio::test]
    async fn test_secret_file_extension_text_does_not_echo_value() {
        let (connector, dataset, _dir) = secret_invalid_file_extension_connector("text");
        let url = connector
            .get_object_store_url(&dataset, None)
            .expect("directory url");
        let (_file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("text plus file_extension must resolve");
        let err = connector
            .create_text_table(&dataset, &url, &extension, Some("text"))
            .await
            .expect_err("invalid secret-derived file_extension must fail text table setup");
        assert_invalid_file_extension_hides_secret(&err);
    }

    #[tokio::test]
    async fn test_secret_file_extension_csv_does_not_echo_value() {
        let (connector, dataset, _dir) = secret_invalid_file_extension_connector("csv");
        let err = connector
            .construct_metadata_provider(&dataset)
            .await
            .expect_err("invalid secret-derived file_extension must fail metadata setup");
        assert_invalid_file_extension_hides_secret(&err);
    }

    #[tokio::test]
    async fn test_secret_file_format_text_fallthrough_does_not_echo_value() {
        const CANARY: &str = "secret123[";
        const REFERENCE: &str = "${ env:SECRET }";
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("notes.txt"), "hello").expect("write listed object");
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([(
                    "file_format".to_string(),
                    CANARY.to_string(),
                )]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_format".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new(
            format!("file://{}/", dir.path().display()),
            TableReference::bare("test"),
        );
        let url = connector
            .get_object_store_url(&dataset, None)
            .expect("directory url");
        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("secret-derived unknown file_format must fall through to text");
        assert!(
            file_format.is_none(),
            "unknown file_format must use the text-table path"
        );
        let err = connector
            .create_text_table(&dataset, &url, &extension, None)
            .await
            .expect_err("invalid secret-derived file_format suffix must fail text table setup");
        assert_error_hides_canary(&err, "secret123", REFERENCE);
    }

    #[tokio::test]
    async fn test_secret_file_extension_missing_files_does_not_echo_value() {
        const CANARY: &str = ".CANARYPGPW7731[";
        const REFERENCE: &str = "${ env:SECRET }";
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("notes.txt"), "hello").expect("write non-matching object");
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(HashMap::from([
                    ("file_format".to_string(), "csv".to_string()),
                    ("file_extension".to_string(), CANARY.to_string()),
                ]))
                .into_iter()
                .collect(),
                "test",
                TEST_PARAMETERS,
            )
            .with_secret_sources(HashMap::from([(
                "file_extension".to_string(),
                REFERENCE.to_string(),
            )])),
        };
        let dataset = DatasetSpec::new(
            format!("file://{}/", dir.path().display()),
            TableReference::bare("test"),
        );
        let url = connector
            .get_object_store_url(&dataset, None)
            .expect("directory url");
        let (file_format, extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("csv plus file_extension must resolve");
        let file_format = file_format.expect("csv must use the listing-table path");
        let err = connector
            .create_listing_table(&dataset, &url, &extension, file_format)
            .await
            .expect_err("secret-derived file_extension with no matching files must fail");
        assert_error_hides_canary(&err, "CANARYPGPW7731", REFERENCE);
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
                .with_listing_options(with_format_selected_file_filter(
                    ListingOptions::new(Arc::clone(&file_format) as Arc<dyn FileFormat>)
                        .with_file_extension(""),
                    &table_path,
                    "*.orc",
                ))
                .with_schema(Arc::clone(&schema)),
        )
        .expect("listing table");

        let provider = LocationPruningListingTable::new(
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
        list_calls: AtomicUsize,
    }

    impl TestObjectStore {
        fn new(meta: Vec<ObjectMeta>) -> Self {
            Self {
                meta,
                list_calls: AtomicUsize::new(0),
            }
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
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
            self.list_calls.fetch_add(1, Ordering::SeqCst);
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

    /// A session that plans without reading file footers, for tests whose
    /// [`TestObjectStore`] only lists.
    fn listing_only_session() -> SessionContext {
        SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_collect_statistics(false),
        )
    }

    fn format_selected_hive_listing_table(
        ctx: &SessionContext,
        store: Arc<dyn ObjectStore>,
        partition_cols: Vec<(String, DataType)>,
    ) -> LocationPruningListingTable {
        let table_path = ListingTableUrl::parse("s3://bucket/table/").expect("listing url");
        ctx.runtime_env()
            .register_object_store(table_path.object_store().as_ref(), Arc::clone(&store));

        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let options = with_format_selected_file_filter(
            ListingOptions::new(Arc::new(OrcFormat::new()) as Arc<dyn FileFormat>)
                .with_file_extension("")
                .with_table_partition_cols(partition_cols),
            &table_path,
            "*.orc",
        );
        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("listing table");
        LocationPruningListingTable::new(Arc::new(listing), store, table_path, file_schema, "*.orc")
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
        let ctx = listing_only_session();
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
        let ctx = listing_only_session();
        let store = Arc::new(TestObjectStore::new(vec![
            create_meta("table/dt=2024-01-01/good.orc", 1, 10),
            create_meta("table/_SUCCESS", 2, 0),
            create_meta("table/_committed_000", 3, 10),
            create_meta("table/notes.txt", 4, 10),
        ])) as Arc<dyn ObjectStore>;
        let provider = format_selected_hive_listing_table(&ctx, store, hive_dt_partition_cols());

        let files: Vec<PartitionedFile> = provider
            .inner
            .list_files_for_scan(&ctx.state(), &[], None)
            .await
            .expect("well-formed hive object must be listed")
            .file_groups
            .into_iter()
            .flat_map(FileGroup::into_inner)
            .collect();
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

    /// Registers the hive-partitioned Parquet dataset at `table_url` under `name`
    /// through the production `create_listing_table` path.
    async fn register_parquet_listing(
        ctx: &SessionContext,
        name: &str,
        table_url: &str,
        file_extension: Option<&str>,
    ) {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        if let Some(file_extension) = file_extension {
            params.insert("file_extension".to_string(), file_extension.to_string());
        }
        let (connector, mut dataset) = setup_connector(table_url.to_string(), params);
        dataset
            .params
            .insert("hive_partitioning_enabled".to_string(), "true".to_string());
        let url = Url::parse(table_url).expect("table url");
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
        ctx.register_table(name, provider)
            .expect("register listing table");
    }

    async fn physical_plan_with_statistics(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan sql")
            .create_physical_plan()
            .await
            .expect("physical plan");
        datafusion::physical_plan::displayable(plan.as_ref())
            .set_show_statistics(true)
            .indent(true)
            .to_string()
    }

    async fn pretty_rows(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan sql")
            .collect()
            .await
            .expect("collect rows");
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format rows")
            .to_string()
    }

    async fn float_cell(ctx: &SessionContext, sql: &str) -> f64 {
        use arrow::array::Float64Array;

        let batches = ctx
            .sql(sql)
            .await
            .expect("plan sql")
            .collect()
            .await
            .expect("collect rows");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column")
            .value(0)
    }

    /// Same `id`/`f` layout as the reporter's `tricky.parquet`: NaN at 2 and 12,
    /// +inf at 3, -inf at 4, NULL at 7 and 16.
    fn write_tricky_parquet(path: &std::path::Path) {
        use arrow::array::{Decimal128Array, Float64Array, Int64Array};
        use datafusion::parquet::arrow::ArrowWriter;

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k", DataType::Int64, true),
            Field::new("amt", DataType::Decimal128(10, 2), true),
            Field::new("f", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![
                Arc::new(Int64Array::from_iter_values(1..=20)),
                Arc::new(Int64Array::from_iter_values(0..20)),
                Arc::new(
                    Decimal128Array::from_iter_values((0..20).map(|i| i * 100))
                        .with_precision_and_scale(10, 2)
                        .expect("decimal scale"),
                ),
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(-0.0),
                    Some(0.0),
                    None,
                    Some(1e308),
                    Some(-1e-308),
                    Some(2.5),
                    Some(3.5),
                    Some(f64::NAN),
                    Some(0.1),
                    Some(0.2),
                    Some(0.300_000_000_000_000_04),
                    None,
                    Some(1.0),
                    Some(1.0),
                    Some(-2.5),
                    Some(100.0),
                ])),
            ],
        )
        .expect("valid batch");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).expect("create parquet"),
            file_schema,
            None,
        )
        .expect("parquet writer");
        writer.write(&batch).expect("write parquet");
        writer.close().expect("close parquet");
    }

    async fn register_tricky_listing(table_url: &str) -> SessionContext {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, dataset) = setup_connector(table_url.to_string(), params);
        let url = Url::parse(table_url).expect("table url");
        let (Some(file_format), extension) = connector
            .get_file_format_and_extension(&dataset)
            .await
            .expect("parquet listing format")
        else {
            panic!("expected a parquet file format");
        };
        assert!(
            file_format
                .as_ref()
                .is::<crate::listing::parquet_nan::NanSafeParquetFormat>(),
            "the production listing path must wrap Parquet so float stats cannot be trusted"
        );
        let provider = connector
            .create_listing_table(&dataset, &url, &extension, file_format)
            .await
            .expect("create_listing_table production path");
        let ctx = SessionContext::new();
        ctx.register_table("t", provider)
            .expect("register listing table");
        ctx
    }

    async fn assert_tricky_float_answers(ctx: &SessionContext) {
        let max_float = float_cell(ctx, "SELECT MAX(f) AS mx FROM t").await;
        assert!(
            max_float.is_nan(),
            "MAX(f) must be NaN, not the parquet stat (inf); got {max_float}"
        );
        let max_non_null = float_cell(ctx, "SELECT MAX(f) AS mx FROM t WHERE f IS NOT NULL").await;
        assert!(
            max_non_null.is_nan(),
            "MAX(f) WHERE f IS NOT NULL must be NaN; got {max_non_null}"
        );
        assert_eq!(
            pretty_rows(
                ctx,
                "SELECT COUNT(*) AS n FROM t WHERE f = CAST('NaN' AS DOUBLE)"
            )
            .await,
            ["+---+", "| n |", "+---+", "| 2 |", "+---+"].join("\n"),
            "f = NaN must not prune the file"
        );
        assert_eq!(
            pretty_rows(
                ctx,
                "SELECT id FROM t WHERE f > CAST('inf' AS DOUBLE) ORDER BY id"
            )
            .await,
            ["+----+", "| id |", "+----+", "| 2  |", "| 12 |", "+----+",].join("\n"),
            "f > inf matches NaN under DataFusion total order"
        );
        let min_float = float_cell(ctx, "SELECT MIN(f) AS mn FROM t").await;
        assert!(
            min_float.is_infinite() && min_float.is_sign_negative(),
            "MIN(f) must stay -inf; got {min_float}"
        );
    }

    async fn assert_non_float_max_keeps_stats_shortcut(ctx: &SessionContext) {
        let int_max_plan = physical_plan_with_statistics(ctx, "SELECT MAX(k) FROM t").await;
        assert!(
            int_max_plan.contains("PlaceholderRowExec"),
            "integer MAX must keep the stats shortcut:\n{int_max_plan}"
        );
        let decimal_max_plan = physical_plan_with_statistics(ctx, "SELECT MAX(amt) FROM t").await;
        assert!(
            decimal_max_plan.contains("PlaceholderRowExec"),
            "decimal MAX must keep the stats shortcut:\n{decimal_max_plan}"
        );
        let float_max_plan = physical_plan_with_statistics(ctx, "SELECT MAX(f) FROM t").await;
        assert!(
            !float_max_plan.contains("PlaceholderRowExec"),
            "float MAX must not fold parquet min/max that omitted NaN:\n{float_max_plan}"
        );
        assert_eq!(
            pretty_rows(ctx, "SELECT MAX(k) AS mx FROM t").await,
            ["+----+", "| mx |", "+----+", "| 19 |", "+----+"].join("\n")
        );
    }

    /// Parquet writers omit `NaN` from float min/max. The listing `FileFormat`
    /// must not answer `MAX(f)` from those stats or prune `f = NaN` / `f > inf`.
    /// Integer and decimal `MAX` still use the statistics shortcut.
    ///
    /// Regression test for the federated parquet path of the same class as
    /// spiceai/spiceai#14719.
    #[tokio::test]
    async fn listing_parquet_nan_stats_do_not_answer_max_or_prune() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_tricky_parquet(&dir.path().join("tricky.parquet"));
        let ctx = register_tricky_listing(&format!("file://{}/", dir.path().display())).await;
        assert_tricky_float_answers(&ctx).await;
        assert_non_float_max_keeps_stats_shortcut(&ctx).await;
    }

    /// Two row groups: RG0 is finite + `inf` (footer max `inf`), RG1 holds `NaN`
    /// (footer max omits it). `TopK` `ORDER BY f DESC LIMIT 1` attaches a dynamic
    /// `f > threshold` after RG0; `DataFusion`'s runtime `RowGroupPruner` then
    /// evaluates that against footer min/max and can skip RG1. The listing
    /// wrapper must not let that happen.
    fn write_multi_rg_nan_after_inf_parquet(path: &std::path::Path) {
        use arrow::array::{Float64Array, Int64Array};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use datafusion::parquet::file::statistics::Statistics as ParquetStatistics;

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(f64::INFINITY),
                    Some(f64::NAN),
                    Some(0.5),
                    Some(1.5),
                ])),
            ],
        )
        .expect("valid batch");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).expect("create parquet"),
            file_schema,
            Some(properties),
        )
        .expect("parquet writer");
        writer.write(&batch).expect("write parquet");
        writer.close().expect("close parquet");

        let reader = SerializedFileReader::new(std::fs::File::open(path).expect("open parquet"))
            .expect("read footer");
        assert_eq!(
            reader.num_row_groups(),
            2,
            "TopK row-group pruning needs NaN in a later row group"
        );
        let double_max = |rg: usize| -> f64 {
            match reader.metadata().row_group(rg).column(1).statistics() {
                Some(ParquetStatistics::Double(stats)) => {
                    *stats.max_opt().expect("row group must publish a float max")
                }
                other => panic!("expected double stats on f, got {other:?}"),
            }
        };
        assert!(
            double_max(0).is_infinite() && double_max(0).is_sign_positive(),
            "RG0 footer max must be inf so TopK's first threshold is inf"
        );
        let rg1_max = double_max(1);
        assert!(
            (rg1_max - 1.5).abs() < f64::EPSILON,
            "RG1 footer max must omit NaN (the hole the runtime pruner would use); got {rg1_max}"
        );
    }

    /// Regression for the Codex finding on spiceai/spiceai#15510: `pruning=false`
    /// does not stop `DataFusion`'s runtime dynamic-filter `RowGroupPruner`.
    #[tokio::test]
    async fn listing_parquet_topk_dynamic_filter_does_not_skip_nan_row_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_multi_rg_nan_after_inf_parquet(&dir.path().join("multi_rg.parquet"));
        let ctx = register_tricky_listing(&format!("file://{}/", dir.path().display())).await;

        let topk_plan =
            physical_plan_with_statistics(&ctx, "SELECT f FROM t ORDER BY f DESC LIMIT 1").await;
        assert!(
            topk_plan.contains("TopK"),
            "the query must take the TopK path that attaches a dynamic filter:\n{topk_plan}"
        );
        assert!(
            !topk_plan.contains("predicate=DynamicFilter"),
            "float TopK must not attach a dynamic filter the runtime pruner would evaluate against footer min/max:\n{topk_plan}"
        );

        let top = float_cell(&ctx, "SELECT f FROM t ORDER BY f DESC LIMIT 1").await;
        assert!(
            top.is_nan(),
            "ORDER BY f DESC LIMIT 1 must be NaN, not the inf from RG0; got {top}\nplan:\n{topk_plan}"
        );

        let max_float = float_cell(&ctx, "SELECT MAX(f) AS mx FROM t").await;
        assert!(
            max_float.is_nan(),
            "MAX(f) must be NaN on the multi-row-group file; got {max_float}"
        );

        let int_topk_plan =
            physical_plan_with_statistics(&ctx, "SELECT k FROM t ORDER BY k DESC LIMIT 1").await;
        assert!(
            int_topk_plan.contains("DynamicFilter"),
            "integer TopK must still attach a dynamic filter so trusted stats can prune:\n{int_topk_plan}"
        );
        assert_eq!(
            pretty_rows(&ctx, "SELECT k FROM t ORDER BY k DESC LIMIT 1").await,
            ["+---+", "| k |", "+---+", "| 6 |", "+---+"].join("\n")
        );
    }

    /// Cluster executors must serialize listing parquet and restore the
    /// NaN-safe `FileSource` on decode. Emitting a bare `ParquetScan` would
    /// encode, then lose float-filter refusal when `DataFusion` rebuilds
    /// `ParquetSource`.
    #[tokio::test]
    async fn listing_parquet_scan_round_trips_through_physical_plan_proto() {
        use crate::listing::{NanSafeListingPhysicalCodec, is_nan_safe_listing_parquet_scan};
        use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
        use datafusion_proto::protobuf::PhysicalPlanNode;

        let dir = tempfile::tempdir().expect("tempdir");
        write_multi_rg_nan_after_inf_parquet(&dir.path().join("multi_rg.parquet"));
        let ctx = register_tricky_listing(&format!("file://{}/", dir.path().display())).await;
        let planned = ctx
            .sql("SELECT f FROM t ORDER BY f DESC LIMIT 1")
            .await
            .expect("plan sql")
            .create_physical_plan()
            .await
            .expect("physical plan");
        assert!(
            is_nan_safe_listing_parquet_scan(&planned),
            "the scheduler plan must wrap parquet scans before encode"
        );

        // Encode before any collect. TopK mutates its DynamicFilter as it
        // runs; serializing after that would snapshot `f > NaN` and the
        // decoded heap would drop every row.
        let default_codec = DefaultPhysicalExtensionCodec {};
        let proto = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&planned), &default_codec)
            .expect("listing parquet scan must serialize for distributed execution");
        assert!(
            listing_scan_is_extension(&proto),
            "encode must tag the scan so decode can restore the NaN-safe source:\n{proto:?}"
        );
        let default_decode = proto.try_into_physical_plan(ctx.task_ctx().as_ref(), &default_codec);
        assert!(
            default_decode.is_err(),
            "DefaultPhysicalExtensionCodec must not decode the tagged scan as a bare ParquetSource"
        );

        let codec = NanSafeListingPhysicalCodec;
        let decoded = proto
            .try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)
            .expect("tagged listing parquet scan must decode");
        assert!(
            is_nan_safe_listing_parquet_scan(&decoded),
            "decode must restore the NaN-safe source"
        );
        let decoded_plan = datafusion::physical_plan::displayable(decoded.as_ref())
            .indent(true)
            .to_string();
        assert!(
            !decoded_plan.contains("predicate=DynamicFilter"),
            "decoded float TopK must not carry a dynamic filter on the scan:\n{decoded_plan}"
        );

        let planned_top = collect_float_cell(Arc::clone(&planned), ctx.task_ctx()).await;
        assert!(
            planned_top.is_nan(),
            "the live plan must return NaN; got {planned_top}"
        );
        // Collect the restored TopK once. A second collect of this same tree
        // (the scan child, or TopK again) can come back empty: the decoded
        // `ParquetSource` shares execution state with its parent.
        let decoded_top = collect_float_cell(Arc::clone(&decoded), ctx.task_ctx()).await;
        assert!(
            decoded_top.is_nan(),
            "the restored plan must return NaN; got {decoded_top}"
        );

        let scan_tree = proto
            .try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)
            .expect("second decode of the tagged scan");
        let scan = scan_tree
            .children()
            .into_iter()
            .find(|child| {
                child
                    .as_ref()
                    .downcast_ref::<datafusion::datasource::source::DataSourceExec>()
                    .is_some()
            })
            .map(Arc::clone)
            .expect("decoded TopK must still have a DataSourceExec");
        let scan_rows = datafusion::physical_plan::collect(scan, ctx.task_ctx())
            .await
            .expect("collect freshly decoded scan");
        let scan_n: usize = scan_rows.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            scan_n, 6,
            "freshly decoded scan must read every row; batches={scan_rows:?}"
        );

        let int_plan = ctx
            .sql("SELECT k FROM t ORDER BY k DESC LIMIT 1")
            .await
            .expect("plan int sql")
            .create_physical_plan()
            .await
            .expect("int physical plan");
        let int_proto = PhysicalPlanNode::try_from_physical_plan(int_plan, &codec)
            .expect("integer listing parquet scan must serialize");
        assert!(
            listing_scan_is_extension(&int_proto),
            "integer-only TopK must still tag the scan so decode keeps the wrapper:\n{int_proto:?}"
        );
        let int_decoded = int_proto
            .try_into_physical_plan(ctx.task_ctx().as_ref(), &codec)
            .expect("integer listing parquet scan must decode");
        assert!(
            is_nan_safe_listing_parquet_scan(&int_decoded),
            "integer listing parquet decode must restore the NaN-safe source"
        );
    }

    /// Cluster executors use a default function registry. A pushed Spice UDF
    /// on the parquet scan must still decode via the active codec's
    /// `try_decode_udf`, not `DefaultPhysicalExtensionCodec`.
    #[tokio::test]
    async fn listing_parquet_spice_udf_predicate_round_trips_on_executor_task_ctx() {
        use crate::listing::{
            decode_nan_safe_listing_parquet_scan, is_nan_safe_listing_parquet_scan,
        };
        use datafusion::common::{Result, internal_datafusion_err};
        use datafusion::execution::{FunctionRegistry, TaskContext};
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion_expr::ScalarUDF;
        use datafusion_proto::physical_plan::{
            AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
            PhysicalProtoConverterExtension,
        };
        use datafusion_proto::protobuf::PhysicalPlanNode;
        use runtime_datafusion::udf::register_core_scalar_udfs;

        struct SchedulerUdfCodec {
            functions: SessionContext,
        }

        impl std::fmt::Debug for SchedulerUdfCodec {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SchedulerUdfCodec").finish_non_exhaustive()
            }
        }

        impl PhysicalExtensionCodec for SchedulerUdfCodec {
            fn try_decode(
                &self,
                buf: &[u8],
                inputs: &[Arc<dyn ExecutionPlan>],
                ctx: &TaskContext,
                proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                if !inputs.is_empty() {
                    return Err(internal_datafusion_err!(
                        "NaN-safe listing parquet scan is a leaf and must not have inputs"
                    ));
                }
                decode_nan_safe_listing_parquet_scan(buf, ctx, self, proto_converter)
            }

            fn try_encode(
                &self,
                _node: Arc<dyn ExecutionPlan>,
                _buf: &mut Vec<u8>,
                _proto_converter: &dyn PhysicalProtoConverterExtension,
            ) -> Result<()> {
                Err(internal_datafusion_err!(
                    "NaN-safe listing parquet scans serialize via FileSource::try_to_proto"
                ))
            }

            fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
                self.functions.udf(name)
            }
        }

        let dir = tempfile::tempdir().expect("tempdir");
        write_tricky_parquet(&dir.path().join("tricky.parquet"));
        let scheduler = register_tricky_listing(&format!("file://{}/", dir.path().display())).await;
        register_core_scalar_udfs(&scheduler);

        let sql = "SELECT k FROM t WHERE bucket(10, k) = 0 ORDER BY k";
        let planned = scheduler
            .sql(sql)
            .await
            .expect("plan sql")
            .create_physical_plan()
            .await
            .expect("physical plan");
        assert!(
            is_nan_safe_listing_parquet_scan(&planned),
            "the scheduler plan must wrap parquet scans before encode"
        );
        let planned_text = datafusion::physical_plan::displayable(planned.as_ref())
            .indent(true)
            .to_string();
        assert!(
            planned_text.contains("bucket"),
            "the plan must use the Spice bucket UDF:\n{planned_text}"
        );
        assert!(
            planned_text.contains("predicate="),
            "bucket must be pushed onto the parquet scan so inner ParquetScan decode sees it:\n{planned_text}"
        );

        let proto = PhysicalPlanNode::try_from_physical_plan(
            Arc::clone(&planned),
            &DefaultPhysicalExtensionCodec {},
        )
        .expect("listing parquet scan must serialize");
        assert!(
            listing_scan_is_extension(&proto),
            "encode must tag the scan:\n{proto:?}"
        );

        let executor = SessionContext::new();
        let executor_task_ctx = executor.task_ctx();
        assert!(
            executor_task_ctx.udf("bucket").is_err(),
            "executor TaskContext must not preload Spice UDFs"
        );

        let codec = SchedulerUdfCodec {
            functions: scheduler.clone(),
        };
        let decoded = proto
            .try_into_physical_plan(executor_task_ctx.as_ref(), &codec)
            .expect("pushed Spice UDF predicate must decode on an executor TaskContext");
        assert!(
            is_nan_safe_listing_parquet_scan(&decoded),
            "decode must restore the NaN-safe source"
        );

        let expected = pretty_rows(&scheduler, sql).await;
        let decoded_batches = datafusion::physical_plan::collect(
            Arc::clone(&decoded),
            Arc::clone(&executor_task_ctx),
        )
        .await
        .expect("execute decoded plan");
        let got = arrow::util::pretty::pretty_format_batches(&decoded_batches)
            .expect("format decoded rows")
            .to_string();
        assert_eq!(
            got, expected,
            "decoded executor plan must return the scheduler rows"
        );
    }

    async fn collect_float_cell(
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        task_ctx: Arc<datafusion::execution::TaskContext>,
    ) -> f64 {
        use arrow::array::Float64Array;

        let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let batches = datafusion::physical_plan::collect(plan, task_ctx)
            .await
            .unwrap_or_else(|err| panic!("collect plan: {err}\n{plan_text}"));
        assert!(
            !batches.is_empty() && batches[0].num_rows() > 0,
            "plan returned no rows:\n{plan_text}"
        );
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column")
            .value(0)
    }

    fn listing_scan_is_extension(proto: &datafusion_proto::protobuf::PhysicalPlanNode) -> bool {
        use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
        match proto.physical_plan_type.as_ref() {
            Some(PhysicalPlanType::Extension(_)) => true,
            Some(PhysicalPlanType::Sort(sort)) => sort
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::Projection(projection)) => projection
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::Merge(coalesce)) => coalesce
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::CoalesceBatches(coalesce)) => coalesce
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::GlobalLimit(limit)) => limit
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::LocalLimit(limit)) => limit
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            Some(PhysicalPlanType::Filter(filter)) => filter
                .input
                .as_ref()
                .is_some_and(|input| listing_scan_is_extension(input)),
            _ => false,
        }
    }

    /// Regression test for Glue TPC-H plans that switched every `HashJoinExec`
    /// from `CollectLeft` to `Partitioned`. Glue lists Parquet and ORC tables
    /// with `file_extension: '*'`, and that format-selected listing scanned its
    /// files without statistics, in one file group, and without partition
    /// pruning, so join selection had no sizes to choose a broadcast build side
    /// from. Over the same data files it must plan exactly as the suffix
    /// listing does, statistics included, and return the same rows.
    #[tokio::test]
    async fn format_selected_parquet_plans_and_answers_like_the_suffix_listing() {
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        for (partition, first_id) in [("1", 0_i64), ("2", 100), ("3", 200)] {
            let partition_dir = dir.path().join(format!("p={partition}"));
            std::fs::create_dir_all(&partition_dir).expect("create partition directory");
            let batch = RecordBatch::try_new(
                Arc::clone(&file_schema),
                vec![
                    Arc::new(arrow::array::Int64Array::from_iter_values(
                        first_id..first_id + 10,
                    )),
                    Arc::new(arrow::array::Int64Array::from_iter_values(
                        (0..10).map(|i| i * 2),
                    )),
                ],
            )
            .expect("valid batch");
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(partition_dir.join("part-0.parquet"))
                    .expect("create parquet"),
                Arc::clone(&file_schema),
                None,
            )
            .expect("parquet writer");
            writer.write(&batch).expect("write parquet");
            writer.close().expect("close parquet");
            // A checksum a Hadoop writer leaves beside its output. Neither listing reads it.
            std::fs::write(partition_dir.join(".part-0.parquet.crc"), b"crc")
                .expect("write checksum");
        }
        std::fs::write(dir.path().join("_SUCCESS"), b"ok").expect("write _SUCCESS marker");

        let table_url = format!("file://{}/", dir.path().display());
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(4),
        );
        register_parquet_listing(&ctx, "suffix", &table_url, None).await;
        register_parquet_listing(&ctx, "format_selected", &table_url, Some("*")).await;

        let join = "SELECT a.p, count(*) AS n, sum(b.v) AS s FROM {t} a JOIN {t} b ON a.id = b.id GROUP BY a.p ORDER BY a.p";
        let pruned = "SELECT count(*) AS n, sum(t.v) AS s FROM {t} AS t WHERE t.p = 2";
        for query in [join, pruned] {
            let suffix_plan =
                physical_plan_with_statistics(&ctx, &query.replace("{t}", "suffix")).await;
            let format_selected_plan =
                physical_plan_with_statistics(&ctx, &query.replace("{t}", "format_selected")).await;
            assert_eq!(
                format_selected_plan, suffix_plan,
                "the format-selected listing must plan like the suffix listing for {query}"
            );
            assert_eq!(
                pretty_rows(&ctx, &query.replace("{t}", "format_selected")).await,
                pretty_rows(&ctx, &query.replace("{t}", "suffix")).await,
                "the format-selected listing must return the suffix listing's rows for {query}"
            );
        }

        // The comparison above is only as strong as what the suffix plan shows.
        let join_plan = physical_plan_with_statistics(&ctx, &join.replace("{t}", "suffix")).await;
        assert!(
            join_plan.contains("HashJoinExec: mode=CollectLeft"),
            "join selection must broadcast from the scans' statistics:\n{join_plan}"
        );
        assert!(
            join_plan.contains("statistics=[Rows=Exact(30)"),
            "the scan must carry the files' exact row count:\n{join_plan}"
        );
        let pruned_plan =
            physical_plan_with_statistics(&ctx, &pruned.replace("{t}", "suffix")).await;
        assert!(
            pruned_plan.contains("p=2/part-0.parquet")
                && !pruned_plan.contains("p=1/")
                && !pruned_plan.contains("p=3/"),
            "the partition filter must prune the listing to `p=2`:\n{pruned_plan}"
        );
        assert_eq!(
            pretty_rows(&ctx, &join.replace("{t}", "format_selected")).await,
            [
                "+---+----+----+",
                "| p | n  | s  |",
                "+---+----+----+",
                "| 1 | 10 | 90 |",
                "| 2 | 10 | 90 |",
                "| 3 | 10 | 90 |",
                "+---+----+----+",
            ]
            .join("\n")
        );
        assert_eq!(
            pretty_rows(&ctx, &pruned.replace("{t}", "format_selected")).await,
            [
                "+----+----+",
                "| n  | s  |",
                "+----+----+",
                "| 10 | 90 |",
                "+----+----+"
            ]
            .join("\n")
        );
    }

    /// An insert into a format-selected listing writes `<write id>_<n>.parquet`
    /// files, which its later scans read back exactly as the suffix listing's do.
    #[tokio::test]
    async fn format_selected_parquet_listing_reads_back_what_an_insert_writes() {
        use datafusion::parquet::arrow::ArrowWriter;

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let ctx = SessionContext::new();
        let mut dirs = Vec::new();
        for (name, file_extension) in [("suffix", None), ("format_selected", Some("*"))] {
            let dir = tempfile::tempdir().expect("tempdir");
            let batch = RecordBatch::try_new(
                Arc::clone(&file_schema),
                vec![
                    Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                    Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                ],
            )
            .expect("valid batch");
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(dir.path().join("part-0.parquet")).expect("create parquet"),
                Arc::clone(&file_schema),
                None,
            )
            .expect("parquet writer");
            writer.write(&batch).expect("write parquet");
            writer.close().expect("close parquet");
            std::fs::write(dir.path().join("_SUCCESS"), b"ok").expect("write _SUCCESS marker");
            let table_url = format!("file://{}/", dir.path().display());
            register_parquet_listing(&ctx, name, &table_url, file_extension).await;
            dirs.push(dir);
        }

        for table in ["suffix", "format_selected"] {
            assert_eq!(
                pretty_rows(
                    &ctx,
                    &format!("INSERT INTO {table} VALUES (4, 40), (5, 50)")
                )
                .await,
                [
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 2     |",
                    "+-------+"
                ]
                .join("\n"),
                "the insert into {table} writes two rows"
            );
        }
        let read_back = "SELECT count(*) AS n, sum(t.id) AS ids, sum(t.v) AS vs FROM {t} AS t";
        assert_eq!(
            pretty_rows(&ctx, &read_back.replace("{t}", "format_selected")).await,
            pretty_rows(&ctx, &read_back.replace("{t}", "suffix")).await,
        );
        assert_eq!(
            pretty_rows(&ctx, &read_back.replace("{t}", "format_selected")).await,
            [
                "+---+-----+-----+",
                "| n | ids | vs  |",
                "+---+-----+-----+",
                "| 5 | 15  | 150 |",
                "+---+-----+-----+",
            ]
            .join("\n")
        );
        let written: Vec<String> = std::fs::read_dir(dirs[1].path())
            .expect("list the format-selected table")
            .map(|entry| {
                entry
                    .expect("directory entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name != "part-0.parquet" && name != "_SUCCESS")
            .collect();
        assert!(
            written.len() == 1 && written[0].ends_with("_0.parquet"),
            "the insert writes one `<write id>_0.parquet` file: {written:?}"
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

        let provider = LocationPruningListingTable::new(
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

    #[tokio::test]
    async fn test_location_pushdown_stays_inexact_for_other_metadata_filters() {
        use datafusion_expr::{col, lit};
        // `scan`'s head()-based fast path only ever applies the `_location`
        // predicates it can extract — it never evaluates any other filter. A
        // `_last_modified` predicate combined with a `_location` predicate must
        // stay `Inexact` so DataFusion keeps re-applying it above the scan;
        // otherwise the fast path silently drops it (regression test for
        // spiceai#14264: mixed `_location` + metadata-column predicates
        // returning wrong rows once the metadata predicate is reported `Exact`
        // with no residual filter re-applying it).
        let ctx = SessionContext::new();
        let no_list_store = Arc::new(NoListObjectStore::new(create_meta(
            "prefix/file.parquet",
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
            MetadataColumn::LastModified.field(),
        ]));

        let listing = ListingTable::try_new(
            ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(Arc::clone(&file_schema)),
        )
        .expect("create listing table");

        let provider = LocationPruningListingTable::new(
            Arc::new(listing),
            ctx.runtime_env()
                .object_store(&table_path)
                .expect("object store"),
            table_path,
            file_schema,
            ".parquet",
        );

        let location_filter = col("_location").eq(lit("s3://bucket/prefix/file.parquet"));
        let last_modified_filter = col("_last_modified").gt(lit(
            ScalarValue::TimestampMicrosecond(Some(0), Some("UTC".into())),
        ));

        let pushdown = provider
            .supports_filters_pushdown(&[&location_filter, &last_modified_filter])
            .expect("supports_filters_pushdown");

        assert_eq!(
            pushdown[1],
            datafusion_expr::TableProviderFilterPushDown::Inexact,
            "a filter on any column other than _location must stay Inexact, since \
             the location fast path never evaluates it"
        );
    }

    /// A partition directory holding only an empty file contributes no row, so
    /// `MAX` of the partition column must not be that partition's value. With
    /// exact row counts collected, `MIN`/`MAX` of a partition column is answered
    /// from the listing's statistics, and the `spiceai/datafusion` fork gives a
    /// file that holds no rows no bounds for its partition columns
    /// (spiceai/datafusion#251).
    #[tokio::test]
    async fn max_of_a_partition_column_skips_a_partition_holding_only_an_empty_file() {
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (partition, rows) in [("1", 5_i64), ("2", 5), ("3", 5), ("99", 0)] {
            let partition_dir = dir.path().join(format!("p={partition}"));
            std::fs::create_dir_all(&partition_dir).expect("create partition directory");
            let batch = RecordBatch::try_new(
                Arc::clone(&file_schema),
                vec![Arc::new(arrow::array::Int64Array::from_iter_values(
                    0..rows,
                ))],
            )
            .expect("valid batch");
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(partition_dir.join("f.parquet")).expect("create parquet"),
                Arc::clone(&file_schema),
                None,
            )
            .expect("parquet writer");
            writer.write(&batch).expect("write parquet");
            writer.close().expect("close parquet");
        }

        let table_url = format!("file://{}/", dir.path().display());
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, mut dataset) = setup_connector(table_url.clone(), params);
        dataset
            .params
            .insert("hive_partitioning_enabled".to_string(), "true".to_string());

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

        let ctx = SessionContext::new();
        ctx.register_table("hive", provider)
            .expect("register listing table");

        // The listing carries exact row counts, so an aggregate the statistics
        // can answer is answered from them; otherwise this test would pass
        // without ever reaching the bounds it is about.
        let count_plan = ctx
            .sql("SELECT count(*) FROM hive")
            .await
            .expect("plan count")
            .create_physical_plan()
            .await
            .expect("physical count plan");
        let count_plan = datafusion::physical_plan::displayable(count_plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            count_plan.contains("PlaceholderRowExec"),
            "precondition: count(*) must be answered from exact statistics:\n{count_plan}"
        );

        let batches = ctx
            .sql("SELECT min(p) AS lo, max(p) AS hi FROM hive")
            .await
            .expect("plan min/max")
            .collect()
            .await
            .expect("run min/max");
        let rendered: Vec<String> = ["lo", "hi"]
            .iter()
            .map(|name| {
                let column = batches[0].column_by_name(name).expect("min/max column");
                arrow::util::display::array_value_to_string(column, 0).expect("value renders")
            })
            .collect();
        assert_eq!(
            rendered,
            ["1", "3"],
            "partition 99 holds no rows, so it is neither the minimum nor the maximum"
        );
    }

    /// `ListingTable` prunes the listing by a predicate on a metadata column
    /// before it opens a file, and reports that predicate `Exact`
    /// (spiceai/datafusion#240): with `_size < 50` only the small files are
    /// scanned.
    #[tokio::test]
    async fn listing_table_prunes_files_by_a_metadata_column_predicate() {
        use datafusion::datasource::file_format::json::JsonFormat;
        use datafusion::datasource::physical_plan::FileScanConfig;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::logical_expr::TableProviderFilterPushDown;
        use datafusion::physical_plan::ExecutionPlan;
        use datafusion_expr::{col, lit};
        use std::fmt::Write;

        fn scanned_files(plan: &Arc<dyn ExecutionPlan>) -> usize {
            if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
                && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
            {
                return config
                    .file_groups
                    .iter()
                    .map(datafusion_datasource::file_groups::FileGroup::len)
                    .sum();
            }
            plan.children()
                .iter()
                .map(|child| scanned_files(child))
                .sum()
        }

        let dir = tempfile::tempdir().expect("tempdir");
        // Three one-row files and one fifty-row file, told apart by `_size`.
        for (name, rows) in [("a", 1), ("b", 1), ("c", 1), ("d", 50)] {
            let content = (0..rows).fold(String::new(), |mut content, n| {
                writeln!(content, "{{\"id\": {n}}}").expect("write to a String");
                content
            });
            std::fs::write(dir.path().join(format!("{name}.json")), content)
                .expect("write json file");
        }

        let ctx = SessionContext::new();
        let options = ListingOptions::new(Arc::new(JsonFormat::default()))
            .with_file_extension(".json")
            .with_metadata_cols(vec![datafusion_datasource::metadata::MetadataColumn::Size]);
        let config = ListingTableConfig::new(
            ListingTableUrl::parse(format!("file://{}/", dir.path().display()))
                .expect("listing url"),
        )
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .expect("infer schema");
        let table = Arc::new(ListingTable::try_new(config).expect("listing table"));

        let filter = col("_size").lt(lit(50_u64));
        assert_eq!(
            table
                .supports_filters_pushdown(&[&filter])
                .expect("filter pushdown"),
            vec![TableProviderFilterPushDown::Exact],
            "a predicate on a metadata column is decided per file, so the listing answers it exactly"
        );

        ctx.register_table("t", table)
            .expect("register listing table");
        let plan = ctx
            .table("t")
            .await
            .expect("open table")
            .filter(filter)
            .expect("apply `_size <` filter")
            .select_columns(&["id"])
            .expect("project id")
            .create_physical_plan()
            .await
            .expect("physical plan");
        assert_eq!(
            scanned_files(&plan),
            3,
            "the fifty-row file must be pruned from the listing:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
    }

    /// A `_location` predicate sends `scan` down its fast path, which opens the
    /// named objects and applies no other predicate, while the inner listing
    /// reports a metadata-column predicate `Exact` (spiceai/datafusion#240). A
    /// `_size` predicate beside `_location` must still remove the rows of the
    /// file it excludes, through the table `create_listing_table` builds.
    #[tokio::test]
    async fn location_fast_path_keeps_a_size_predicate_beside_it() {
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = tempfile::tempdir().expect("tempdir");
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("valid batch");
        let parquet_path = dir.path().join("data.parquet");
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&parquet_path).expect("create parquet"),
            Arc::clone(&file_schema),
            None,
        )
        .expect("parquet writer");
        writer.write(&batch).expect("write parquet");
        writer.close().expect("close parquet");
        let file_size = std::fs::metadata(&parquet_path)
            .expect("parquet file metadata")
            .len();
        assert!(
            file_size > 50,
            "precondition: the file must be larger than the `_size` bound, got {file_size} bytes"
        );

        let table_url = Url::from_directory_path(dir.path())
            .expect("directory url")
            .to_string();
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "parquet".to_string());
        let (connector, mut dataset) = setup_connector(table_url.clone(), params);
        dataset.metadata = HashMap::from([
            (
                MetadataColumn::Location(None).name().to_string(),
                "enabled".to_string(),
            ),
            (
                MetadataColumn::Size.name().to_string(),
                "enabled".to_string(),
            ),
        ]);

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

        let ctx = SessionContext::new();
        ctx.register_table("t", provider)
            .expect("register listing table");

        let location = Url::from_file_path(&parquet_path)
            .expect("file url")
            .to_string();
        let rows = async |predicate: &str| -> usize {
            let sql = format!("SELECT id FROM t WHERE _location = '{location}'{predicate}");
            ctx.sql(&sql)
                .await
                .expect("plan query")
                .collect()
                .await
                .expect("run query")
                .iter()
                .map(RecordBatch::num_rows)
                .sum()
        };

        assert_eq!(
            rows("").await,
            3,
            "precondition: the `_location` predicate alone selects the file"
        );
        assert_eq!(rows(" AND _size > 50").await, 3);
        assert_eq!(
            rows(" AND _size < 50").await,
            0,
            "a `_size` predicate the file does not satisfy must remove its rows"
        );
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

        let provider = LocationPruningListingTable::new(
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

        let provider = LocationPruningListingTable::new(
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
        use datafusion_datasource::metadata::MetadataColumn as ListingMetadataColumn;

        let table_url = Url::parse("s3://bucket/prefix/").expect("parse table url");
        let schema = Schema::new(vec![Field::new(
            "compression",
            arrow_schema::DataType::Utf8,
            true,
        )]);
        let listing_options = || {
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet")
        };

        let mut dataset = DatasetSpec::new("s3://bucket/prefix/", TableReference::bare("test"));
        dataset.metadata = HashMap::from([(
            MetadataColumn::Location(None).name().to_string(),
            "enabled".to_string(),
        )]);
        let result =
            add_metadata_columns_if_required(listing_options(), &table_url, &schema, &dataset);
        // `_location` values are built from the bucket-level prefix, not the
        // table path.
        assert_eq!(
            result.metadata_cols,
            vec![ListingMetadataColumn::Location(Some(Arc::from(
                "s3://bucket/"
            )))]
        );

        // Every enabled column is forwarded as its DataFusion counterpart, in
        // a fixed order.
        dataset.metadata = HashMap::from([
            (
                MetadataColumn::LastModified.name().to_string(),
                "enabled".to_string(),
            ),
            (
                MetadataColumn::Location(None).name().to_string(),
                "enabled".to_string(),
            ),
            (
                MetadataColumn::Size.name().to_string(),
                "enabled".to_string(),
            ),
        ]);
        let result =
            add_metadata_columns_if_required(listing_options(), &table_url, &schema, &dataset);
        assert_eq!(
            result.metadata_cols,
            vec![
                ListingMetadataColumn::LastModified,
                ListingMetadataColumn::Location(Some(Arc::from("s3://bucket/"))),
                ListingMetadataColumn::Size,
            ]
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
            ".csv",
            table_path,
            &ctx,
            &test_store,
        )
        .await;

        let err = result.expect_err("should error on no matching extension");
        let DataConnectorError::InvalidConfigurationNoSource {
            dataconnector,
            message,
            ..
        } = &err
        else {
            panic!("an extension mismatch must be a configuration error, got: {err:?}");
        };
        assert_eq!(dataconnector, "TestListingConnector");
        assert_eq!(
            message,
            "Failed to find any files matching the extension '.csv'. Is your `file_format` parameter correct? Spice found the following file extensions: '.parquet'. For details, visit: https://spiceai.org/docs/components/data-connectors#object-store-file-formats"
        );
        assert!(
            !err.is_retriable(),
            "files that are present but none match `file_format` is a permanent configuration error, not a wait-for-data retry"
        );
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
    #[expect(
        deprecated,
        reason = "guards the Spice patches to arrow-rs's `ParquetObjectReader`, which is deprecated upstream but still carries them (docs/dev/fork_patches.md)"
    )]
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

    /// A truncated range body has to come back as an error the reader can act on,
    /// not take the thread down.
    ///
    /// The pinned readers above ask for byte ranges, and an object store is
    /// entitled to answer a range request with fewer bytes than were asked for —
    /// notably when the object shrinks in place between the `HEAD` that sized it
    /// and the `GET` that reads it, which is the same replace-mid-scan the pinning
    /// exists for. Upstream `PushBuffers::push_range` asserts that the buffer
    /// matches the range, so that answer aborts the thread doing the decode
    /// (`Range length must match buffer length`) instead of surfacing a decode
    /// error the scan could retry or report. A Spice patch to the
    /// `spiceai/arrow-rs` fork returns `ParquetError` instead
    /// ([apache/arrow-rs#10564](https://github.com/apache/arrow-rs/pull/10564)).
    ///
    /// Driven through `ParquetMetaDataPushDecoder`, which is the public surface
    /// `push_range` sits behind; the panic and the error are the same two outcomes
    /// there as on the prefetch path, and a panicking guard fails as loudly as an
    /// assertion does.
    #[test]
    fn a_short_range_body_is_a_parquet_error_and_not_a_panic() {
        use datafusion::parquet::file::metadata::ParquetMetaDataPushDecoder;

        const FILE_LEN: u64 = 4096;

        let mut decoder =
            ParquetMetaDataPushDecoder::try_new(FILE_LEN).expect("builds a metadata decoder");

        // Eight bytes asked for, three delivered — a short read, not a malformed
        // one, so nothing but the length check can tell it apart from a good body.
        let error = decoder
            .push_range(0..8, bytes::Bytes::from_static(b"abc"))
            .expect_err(
                "a range answered with fewer bytes than were asked for must be reported, not \
                 asserted on: a footer prefetch racing an in-place shrink takes the decoding \
                 thread down instead of failing the scan",
            );
        let message = error.to_string();
        assert!(
            message.contains('3') && message.contains('8'),
            "the error has to say what was asked for and what arrived, or a short read is \
             indistinguishable from a corrupt file: {message}"
        );

        // The decoder is still usable afterwards, which is what makes the error
        // retriable rather than merely non-fatal.
        decoder
            .push_range(0..3, bytes::Bytes::from_static(b"abc"))
            .expect("a well-formed range is still accepted after a rejected one");
    }

    /// A footer whose Thrift list header declares more elements than the file could hold is
    /// refused before anything is reserved for that count.
    ///
    /// Every footer list (`schema`, `row_groups`, `key_value_metadata`, `column_orders`) and
    /// the offset index's `page_locations` decodes into a `Vec` sized from the element count
    /// in that header, so an unreadable file of a few bytes can ask for room for `i32::MAX`
    /// elements before a single one is read. That happens during schema inference at dataset
    /// initialization, and again on every refresh of the dataset, and the process is killed
    /// for it. A Spice patch to the `spiceai/arrow-rs` fork bounds the count by the bytes left
    /// in the input ([spiceai/arrow-rs#32](https://github.com/spiceai/arrow-rs/pull/32)).
    ///
    /// Driven through `ParquetMetaDataReader::decode_metadata`, the public surface the footer
    /// decode sits behind. The error has to name the declared count: an oversized footer is a
    /// malformed file, and an error that does not say so reads like a truncated download.
    #[test]
    fn an_oversized_footer_list_length_is_a_parquet_error_and_not_an_allocation() {
        use datafusion::parquet::file::metadata::ParquetMetaDataReader;

        // Struct header, then field 2 (`schema`): a `list<SchemaElement>` declaring
        // i32::MAX elements, with no element following it.
        const FOOTER: &[u8] = &[0x29, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00];

        let error = ParquetMetaDataReader::decode_metadata(FOOTER)
            .expect_err("no file holds as many schema elements as this footer declares");
        let message = error.to_string();
        assert!(
            message.contains("Thrift list declares 2147483647 elements"),
            "the error has to name the declared count, or an oversized footer is \
             indistinguishable from a truncated one: {message}"
        );
    }

    /// A Parquet footer whose schema nests deeper than 100 levels is refused, and one
    /// at exactly 100 levels still opens.
    ///
    /// The footer's schema is a flat element list that the reader rebuilds into a tree by
    /// recursing once per nesting level, so a file of a few KiB can declare enough levels to
    /// exhaust the stack during schema inference at dataset initialization. A Spice patch to
    /// the `spiceai/arrow-rs` fork bounds that depth.
    #[test]
    fn a_footer_schema_nested_past_the_depth_limit_is_a_parquet_error() {
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

        const MAX_DEPTH: usize = 100;

        // A column whose leaf sits `depth` levels below the schema root.
        fn file_with_leaf_at(depth: usize) -> bytes::Bytes {
            let mut field =
                arrow::datatypes::Field::new("leaf", arrow::datatypes::DataType::Int64, true);
            for _ in 1..depth {
                field = arrow::datatypes::Field::new_struct("s", vec![field], true);
            }
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
            let mut buffer = Vec::new();
            ArrowWriter::try_new_with_options(
                &mut buffer,
                schema,
                ArrowWriterOptions::new().with_skip_arrow_metadata(true),
            )
            .expect("parquet writer")
            .close()
            .expect("writes the footer");
            bytes::Bytes::from(buffer)
        }

        let reader =
            SerializedFileReader::new(file_with_leaf_at(MAX_DEPTH)).expect("reads the footer");
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .column(0)
                .path()
                .parts()
                .len(),
            MAX_DEPTH,
            "the accepted file has to sit exactly at the limit"
        );

        let Err(error) = SerializedFileReader::new(file_with_leaf_at(MAX_DEPTH + 1)) else {
            panic!(
                "a footer schema nested {} levels deep was accepted",
                MAX_DEPTH + 1
            );
        };
        assert!(
            error
                .to_string()
                .contains("Parquet schema nesting depth exceeds the maximum of 100"),
            "the footer was refused, but not on the depth limit: {error}"
        );
    }

    /// A Parquet page whose header declares more uncompressed bytes than its
    /// compressed body can back is a scan error, raised before the reader allocates
    /// the declared size.
    ///
    /// A Spice patch to `decode_page` in the `spiceai/arrow-rs` fork requires a page
    /// above 64 MiB to carry at least 1/1000 of its declared size in compressed bytes
    /// ([apache/arrow-rs#10734](https://github.com/apache/arrow-rs/issues/10734)).
    /// Without it, a file of a few KiB whose pages each declare hundreds of MiB makes
    /// every scan of it allocate that much per page, and concurrent scans of it can
    /// exhaust memory. Driven through a `DataFusion` Parquet scan, the reader path a
    /// file or object-store dataset takes.
    #[tokio::test]
    async fn a_page_declaring_more_than_its_compressed_bytes_can_back_is_an_error() {
        use datafusion::execution::context::SessionContext;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::basic::{Compression, ZstdLevel};
        use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use datafusion::prelude::ParquetReadOptions;

        // One value just past the 64 MiB that any page is allowed; zero bytes
        // compress to a few KiB.
        const VALUE_LEN: usize = 65 * 1024 * 1024;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("payload", arrow::datatypes::DataType::Binary, false),
        ]));
        let value = vec![0u8; VALUE_LEN];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::BinaryArray::from_vec(vec![
                value.as_slice(),
            ]))],
        )
        .expect("builds a batch");
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let dir = tempfile::tempdir().expect("creates a temporary directory");
        let path = dir.path().join("page.parquet");
        let file = std::fs::File::create(&path).expect("creates the file");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("parquet writer");
        writer.write(&batch).expect("writes the batch");
        writer.close().expect("closes the file");

        let reader = SerializedFileReader::new(std::fs::File::open(&path).expect("opens the file"))
            .expect("reads the footer");
        let column = reader.metadata().row_group(0).column(0);
        assert!(
            column.uncompressed_size() > 64 * 1024 * 1024
                && column.compressed_size() * 1000 < column.uncompressed_size(),
            "the page has to be past the bound for this guard to assert anything: \
             {} compressed bytes for {} uncompressed",
            column.compressed_size(),
            column.uncompressed_size()
        );

        let scan = SessionContext::new()
            .read_parquet(
                path.to_str().expect("a UTF-8 path"),
                ParquetReadOptions::default(),
            )
            .await
            .expect("plans the scan")
            .collect()
            .await;
        let Err(error) = scan else {
            panic!("a page declaring {VALUE_LEN} bytes behind a few KiB of ZSTD was decoded");
        };
        assert!(
            error.to_string().contains("exceeds the maximum allowed"),
            "the scan failed, but not on the page-size bound: {error}"
        );
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
    #[expect(
        deprecated,
        reason = "guards the Spice patches to arrow-rs's `ParquetObjectReader`, which is deprecated upstream but still carries them (docs/dev/fork_patches.md)"
    )]
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

    /// Regression matrix for the object-store request pattern of the `_location`
    /// fast path and the metadata-column prune (spiceai/spiceai#14264):
    ///
    /// | # | predicate                                  | LIST | opens the file? | residual `FilterExec` |
    /// |---|--------------------------------------------|------|-----------------|---------------------|
    /// | 1 | `_location = X`                            | no   | yes             | no                  |
    /// | 2 | `_location = X AND _last_modified > W`     | no   | only if it passes W | no              |
    /// | 3 | `_location = X AND other_col = 'foo'`      | no   | yes             | yes (`other_col`)   |
    /// | 4 | `_last_modified > W`                       | yes  | survivors only  | no                  |
    /// | 5 | `_last_modified > W AND other_col = 'foo'` | yes  | survivors only  | yes (`other_col`)   |
    ///
    /// Asserted at plan time (`create_physical_plan`, as the other fast-path tests do):
    /// a pruned scan becomes an `EmptyExec` (the object is never opened), and a `_size`/
    /// `_last_modified`/`_location` predicate carries no residual `FilterExec` while a data
    /// column does. `MatrixStore` panics if the fast path ever lists.
    mod metadata_prune_matrix {
        use super::*;
        use datafusion::physical_plan::displayable;
        use datafusion_datasource::metadata::MetadataColumn as DfMeta;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Serves a fixed set of controlled `ObjectMeta` and counts list/head requests.
        /// With `forbid_list`, any listing panics — proving the `_location` fast path
        /// never lists. Only `HEAD`s occur during `create_physical_plan`; an actual
        /// object read (a non-head GET) is an execution-time step the plan-level tests
        /// never trigger, so it is refused here.
        #[derive(Debug)]
        struct MatrixStore {
            metas: Vec<ObjectMeta>,
            forbid_list: bool,
            list_calls: AtomicUsize,
            head_calls: AtomicUsize,
        }

        impl MatrixStore {
            fn new(metas: Vec<ObjectMeta>, forbid_list: bool) -> Arc<Self> {
                Arc::new(Self {
                    metas,
                    forbid_list,
                    list_calls: AtomicUsize::new(0),
                    head_calls: AtomicUsize::new(0),
                })
            }
            fn lists(&self) -> usize {
                self.list_calls.load(Ordering::SeqCst)
            }
            fn heads(&self) -> usize {
                self.head_calls.load(Ordering::SeqCst)
            }
        }

        impl std::fmt::Display for MatrixStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "MatrixStore")
            }
        }

        #[async_trait]
        impl ObjectStore for MatrixStore {
            fn list(
                &self,
                prefix: Option<&Path>,
            ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                assert!(
                    !self.forbid_list,
                    "list must not be called on the _location fast path"
                );
                self.list_calls.fetch_add(1, Ordering::SeqCst);
                let prefix = prefix.cloned();
                #[expect(
                    clippy::needless_collect,
                    reason = "the returned stream is 'static and cannot borrow self"
                )]
                let metas: Vec<_> = self
                    .metas
                    .iter()
                    .filter(|m| {
                        prefix
                            .as_ref()
                            .is_none_or(|p| m.location.as_ref().starts_with(p.as_ref()))
                    })
                    .cloned()
                    .collect();
                stream::iter(metas.into_iter().map(Ok)).boxed()
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
                location: &Path,
                options: object_store::GetOptions,
            ) -> object_store::Result<object_store::GetResult> {
                if !options.head {
                    return Err(object_store::Error::NotImplemented {
                        operation: "non-head get".to_string(),
                        implementer: "MatrixStore".to_string(),
                    });
                }
                self.head_calls.fetch_add(1, Ordering::SeqCst);
                let meta = self
                    .metas
                    .iter()
                    .find(|m| &m.location == location)
                    .cloned()
                    .expect("head() for a controlled object");
                Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(
                        futures::stream::empty(),
                    )),
                    attributes: object_store::Attributes::default(),
                    range: 0..0,
                    meta,
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

        const OLD_LOC: &str = "s3://bucket/prefix/old.csv";

        // `old.csv` mtime = 2001-09-09; `new.csv` mtime = 2033-05-18.
        fn old_meta() -> ObjectMeta {
            create_meta("prefix/old.csv", 1_000_000_000, 10)
        }
        fn new_meta() -> ObjectMeta {
            create_meta("prefix/new.csv", 2_000_000_000, 20)
        }

        fn provider(ctx: &SessionContext, store: &Arc<MatrixStore>) -> LocationPruningListingTable {
            let store_url = Url::parse("s3://bucket").expect("store url");
            ctx.runtime_env()
                .register_object_store(&store_url, Arc::clone(store) as Arc<dyn ObjectStore>);

            let table_path = ListingTableUrl::parse("s3://bucket/prefix/").expect("listing url");
            let file_schema = Arc::new(Schema::new(vec![Field::new(
                "other_col",
                DataType::Utf8,
                true,
            )]));
            let options =
                ListingOptions::new(Arc::new(CsvFormat::default()) as Arc<dyn FileFormat>)
                    .with_file_extension(".csv")
                    .with_table_partition_cols(vec![])
                    .with_metadata_cols(vec![
                        DfMeta::Location(Some("s3://bucket/".into())),
                        DfMeta::LastModified,
                        DfMeta::Size,
                    ]);
            let listing = ListingTable::try_new(
                ListingTableConfig::new(table_path.clone())
                    .with_listing_options(options)
                    .with_schema(Arc::clone(&file_schema)),
            )
            .expect("listing table");
            LocationPruningListingTable::new(
                Arc::new(listing),
                Arc::clone(store) as Arc<dyn ObjectStore>,
                table_path,
                file_schema,
                ".csv",
            )
        }

        /// Plan `sql` against a freshly registered provider and return the indented
        /// physical plan string.
        async fn plan_of(store: Arc<MatrixStore>, sql: &str) -> String {
            let ctx = SessionContext::new_with_config(
                datafusion::prelude::SessionConfig::new().with_collect_statistics(false),
            );
            ctx.register_table("t", Arc::new(provider(&ctx, &store)))
                .expect("register table");
            let plan = ctx
                .sql(sql)
                .await
                .expect("build logical plan")
                .create_physical_plan()
                .await
                .expect("build physical plan");
            displayable(plan.as_ref()).indent(true).to_string()
        }

        // Case 1: `_location = X` — no LIST, opens the object, no residual filter.
        #[tokio::test]
        async fn case1_location_only_skips_listing_and_scans() {
            let store = MatrixStore::new(vec![old_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!("SELECT other_col FROM t WHERE _location = '{OLD_LOC}'"),
            )
            .await;
            assert_eq!(store.lists(), 0, "fast path must not LIST");
            assert!(store.heads() >= 1, "fast path HEADs the object");
            assert!(
                !plan.contains("EmptyExec"),
                "the object is scanned, not pruned"
            );
            assert!(
                !plan.contains("FilterExec"),
                "a pure _location scan needs no residual filter"
            );
        }

        // Case 2: `_location = X AND _last_modified > W` — no LIST; the object is pruned
        // on its HEADed ObjectMeta (EmptyExec) when it fails W, and scanned when it passes.
        #[tokio::test]
        async fn case2_location_and_last_modified_prunes_before_opening() {
            // old.csv (2001) fails `> 2020` → pruned to EmptyExec, never opened.
            let store = MatrixStore::new(vec![old_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t \
                     WHERE _location = '{OLD_LOC}' AND _last_modified > TIMESTAMP '2020-01-01T00:00:00Z'"
                ),
            )
            .await;
            assert_eq!(store.lists(), 0, "fast path must not LIST");
            assert!(
                store.heads() >= 1,
                "the object is HEADed before it is pruned"
            );
            assert!(
                plan.contains("EmptyExec"),
                "a stale object is pruned before it is opened"
            );
            assert!(
                !plan.contains("FilterExec"),
                "_last_modified is applied by the prune, not a residual filter"
            );
        }

        #[tokio::test]
        async fn case2_location_and_last_modified_scans_when_it_passes() {
            // old.csv (2001) passes `> 1990` → kept and scanned.
            let store = MatrixStore::new(vec![old_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t \
                     WHERE _location = '{OLD_LOC}' AND _last_modified > TIMESTAMP '1990-01-01T00:00:00Z'"
                ),
            )
            .await;
            assert_eq!(store.lists(), 0, "fast path must not LIST");
            assert!(!plan.contains("EmptyExec"), "a passing object is scanned");
            assert!(
                !plan.contains("FilterExec"),
                "_last_modified is applied by the prune, not a residual filter"
            );
        }

        const NEW_LOC: &str = "s3://bucket/prefix/new.csv";

        // Conjoined `_location` equalities naming different objects match nothing. The
        // planner folds them to `false` before `scan`, which answers without listing.
        #[tokio::test]
        async fn case2b_conflicting_location_equalities_match_nothing() {
            let store = MatrixStore::new(vec![old_meta(), new_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t WHERE _location = '{OLD_LOC}' AND _location = '{NEW_LOC}'"
                ),
            )
            .await;
            assert_eq!(store.lists(), 0, "fast path must not LIST");
            assert_eq!(
                store.heads(),
                0,
                "an unsatisfiable predicate opens no object"
            );
            assert!(
                plan.contains("EmptyExec"),
                "no object satisfies both equalities: {plan}"
            );
            assert!(!plan.contains("FilterExec"), "{plan}");
        }

        // An equality plus a `_location` predicate the extractor ignores (`LIKE`) must
        // still enforce the `LIKE`.
        #[tokio::test]
        async fn case2c_location_equality_and_like_enforces_the_like() {
            let store = MatrixStore::new(vec![old_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t WHERE _location = '{OLD_LOC}' AND _location LIKE '%/new.csv'"
                ),
            )
            .await;
            assert!(
                plan.contains("EmptyExec"),
                "the LIKE excludes old.csv: {plan}"
            );
            assert!(!plan.contains("FilterExec"), "{plan}");
        }

        // Overlapping `IN` lists keep only the shared object. The planner intersects the
        // lists before `scan`, so only that object is head()ed.
        #[tokio::test]
        async fn case2d_overlapping_location_in_lists_keep_the_intersection() {
            let store = MatrixStore::new(vec![old_meta(), new_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t WHERE _location IN ('{OLD_LOC}', '{NEW_LOC}') \
                     AND _location IN ('{NEW_LOC}')"
                ),
            )
            .await;
            assert!(!plan.contains("EmptyExec"), "new.csv matches: {plan}");
            assert!(plan.contains("new.csv"), "{plan}");
            assert!(!plan.contains("old.csv"), "old.csv is pruned: {plan}");
            assert_eq!(store.heads(), 1, "only the shared candidate is head()ed");
        }

        // Repeated literals must not scan the same object twice.
        #[tokio::test]
        async fn case2e_repeated_location_literals_head_the_object_once() {
            for predicate in [
                format!("_location = '{NEW_LOC}' AND _location = '{NEW_LOC}'"),
                format!("_location IN ('{NEW_LOC}', '{NEW_LOC}')"),
            ] {
                let store = MatrixStore::new(vec![new_meta()], true);
                let plan = plan_of(
                    Arc::clone(&store),
                    &format!("SELECT other_col FROM t WHERE {predicate}"),
                )
                .await;
                assert!(!plan.contains("EmptyExec"), "new.csv matches: {plan}");
                assert_eq!(store.heads(), 1, "`{predicate}` heads new.csv once");
            }
        }

        // Case 3: `_location = X AND other_col = 'foo'` — no LIST, scans, and the data
        // column stays a residual FilterExec above the scan.
        #[tokio::test]
        async fn case3_location_and_data_column_post_filters() {
            let store = MatrixStore::new(vec![old_meta()], true);
            let plan = plan_of(
                Arc::clone(&store),
                &format!(
                    "SELECT other_col FROM t WHERE _location = '{OLD_LOC}' AND other_col = 'foo'"
                ),
            )
            .await;
            assert_eq!(store.lists(), 0, "fast path must not LIST");
            assert!(!plan.contains("EmptyExec"), "the object is scanned");
            assert!(
                plan.contains("FilterExec"),
                "a data column is applied as a residual filter"
            );
        }

        // Case 4: `_last_modified > W` (no _location) — LISTs, prunes the listing on
        // ObjectMeta, no residual filter.
        #[tokio::test]
        async fn case4_last_modified_only_lists_and_prunes() {
            let store = MatrixStore::new(vec![old_meta(), new_meta()], false);
            let plan = plan_of(
                Arc::clone(&store),
                "SELECT other_col FROM t WHERE _last_modified > TIMESTAMP '2020-01-01T00:00:00Z'",
            )
            .await;
            assert!(store.lists() >= 1, "no _location predicate → must LIST");
            // new.csv (2033) survives, old.csv (2001) is pruned; the scan is non-empty.
            assert!(!plan.contains("EmptyExec"), "a surviving object is scanned");
            assert!(
                !plan.contains("FilterExec"),
                "_last_modified is applied by the listing prune"
            );
        }

        #[tokio::test]
        async fn case4_last_modified_prunes_all_to_empty() {
            let store = MatrixStore::new(vec![old_meta(), new_meta()], false);
            let plan = plan_of(
                Arc::clone(&store),
                "SELECT other_col FROM t WHERE _last_modified > TIMESTAMP '2099-01-01T00:00:00Z'",
            )
            .await;
            assert!(store.lists() >= 1, "no _location predicate → must LIST");
            assert!(
                plan.contains("EmptyExec"),
                "both objects are pruned by the listing prune"
            );
        }

        // Case 5: `_last_modified > W AND other_col = 'foo'` — LISTs, prunes the listing,
        // and the data column stays a residual FilterExec.
        #[tokio::test]
        async fn case5_last_modified_and_data_column_lists_prunes_and_post_filters() {
            let store = MatrixStore::new(vec![old_meta(), new_meta()], false);
            let plan = plan_of(
                Arc::clone(&store),
                "SELECT other_col FROM t \
                 WHERE _last_modified > TIMESTAMP '2020-01-01T00:00:00Z' AND other_col = 'foo'",
            )
            .await;
            assert!(store.lists() >= 1, "no _location predicate → must LIST");
            assert!(!plan.contains("EmptyExec"), "a surviving object is scanned");
            assert!(
                plan.contains("FilterExec"),
                "a data column is applied as a residual filter"
            );
        }
    }
}
