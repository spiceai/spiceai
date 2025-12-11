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

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_tools::schema::expand_views_schema;
use async_trait::async_trait;
use dataformat_json::{Format, SpiceJsonFormat};
use datafusion::catalog::Session;
use datafusion::common::{Constraints, DFSchema, Result as DFResult, ScalarValue};
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
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use datafusion::physical_plan::empty::EmptyExec;
#[cfg(feature = "vortex")]
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{PartitionedFile, metadata::MetadataColumn};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path};
use snafu::prelude::*;
use url::Url;
#[cfg(feature = "vortex")]
use vortex_datafusion::VortexFormatFactory;

use crate::Runtime;
use crate::accelerated_table::AcceleratedTable;
use crate::component::dataset::Dataset;
use crate::dataconnector::{
    ConnectorComponent, DataConnector, DataConnectorError, DataConnectorResult,
    listing::infer::{infer_partitions_with_types_from_files, infer_partitions_with_types_prefix},
};
use crate::parameters::{ExposedParamLookup, Parameters};
use data_components::object::{metadata::ObjectStoreMetadataTable, text::ObjectStoreTextTable};

use super::DelimitedFormat;
use crate::dataconnector::DataConnectorError::SchemaMismatch;
use crate::datafusion::builder::get_df_default_config;
use runtime_object_store::registry::default_runtime_env;

/// Maximum number of files to scan when validating that the schema source path contains objects with the expected extension.
const SCHEMA_SOURCE_PATH_FILE_SCAN_LIMIT: usize = 10_000;

#[derive(Clone, Debug)]
/// Wraps a `ListingTable` to short-circuit broad object-store listings when
/// queries include `location` predicates. Instead of listing a large prefix, it
/// directly fetches the specific objects referenced in the predicate, which
/// significantly reduces LIST calls on large buckets.
struct LocationPruningListingTable {
    inner: Arc<ListingTable>,
    object_store: Arc<dyn ObjectStore>,
    table_path: ListingTableUrl,
}

impl LocationPruningListingTable {
    fn new(
        inner: Arc<ListingTable>,
        object_store: Arc<dyn ObjectStore>,
        table_path: ListingTableUrl,
    ) -> Self {
        Self {
            inner,
            object_store,
            table_path,
        }
    }

    fn partition_column_types(&self) -> &[(String, datafusion::arrow::datatypes::DataType)] {
        &self.inner.options().table_partition_cols
    }

    fn metadata_columns(&self) -> &Vec<MetadataColumn> {
        &self.inner.options().metadata_cols
    }

    fn object_store_url(&self) -> ObjectStoreUrl {
        // Safe: Listing tables share object store across paths. Should always have at least one path.
        self.inner.table_paths().first().map_or_else(
            || unreachable!("ListingTable should always contain at least one path"),
            ListingTableUrl::object_store,
        )
    }

    fn file_schema(&self) -> Arc<Schema> {
        let table_schema = self.inner.schema();
        let partition_cols: HashSet<&str> = self
            .partition_column_types()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let metadata_cols: HashSet<&str> = self
            .metadata_columns()
            .iter()
            .map(MetadataColumn::name)
            .collect();

        let fields: Vec<_> = table_schema
            .fields()
            .iter()
            .filter(|field| {
                let name = field.name().as_str();
                !partition_cols.contains(name) && !metadata_cols.contains(name)
            })
            .cloned()
            .collect();

        Arc::new(Schema::new(fields))
    }

    fn collect_partition_values(&self, meta: &ObjectMeta) -> Option<Vec<ScalarValue>> {
        let parts = parse_partition_values(
            &self.table_path,
            &meta.location,
            self.partition_column_types(),
        )?;

        let mut values = Vec::with_capacity(self.partition_column_types().len());
        for (value, (_, dtype)) in parts.into_iter().zip(self.partition_column_types()) {
            let scalar = ScalarValue::try_from_string(value.to_string(), dtype).ok()?;
            values.push(scalar);
        }
        Some(values)
    }
}

fn parse_partition_values(
    table_path: &ListingTableUrl,
    file_path: &Path,
    table_partition_cols: &[(String, datafusion::arrow::datatypes::DataType)],
) -> Option<Vec<String>> {
    // Extract hive-style partition values (e.g., year=2023/month=2) from the
    // file path relative to the table path, validating the expected partition
    // column names.
    let subpath = table_path.strip_prefix(file_path)?;

    let mut part_values = Vec::with_capacity(table_partition_cols.len());
    for (part, (expected_partition, _)) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == expected_partition => part_values.push(val.to_string()),
            _ => return None,
        }
    }
    Some(part_values)
}

#[async_trait]
impl TableProvider for LocationPruningListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            return self.inner.scan(state, projection, filters, limit).await;
        };

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

            let Some(partition_values) = self.collect_partition_values(&meta) else {
                tracing::warn!(
                    location = loc,
                    "Unable to parse partition values for location predicate; skipping file"
                );
                continue;
            };

            files.push(PartitionedFile {
                object_meta: meta,
                partition_values,
                range: None,
                statistics: None,
                extensions: None,
                metadata_size_hint: None,
            });
        }

        if files.is_empty() {
            return Ok(Arc::new(EmptyExec::new(self.schema())));
        }

        let file_groups = vec![FileGroup::new(files)];
        let partition_fields: Vec<Field> = self
            .partition_column_types()
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), true))
            .collect();

        let file_source = self.inner.options().format.file_source();

        let mut builder =
            FileScanConfigBuilder::new(self.object_store_url(), self.file_schema(), file_source)
                .with_file_groups(file_groups)
                .with_table_partition_cols(partition_fields)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_metadata_cols(self.metadata_columns().clone())
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
                        matches!(*binary.left, Expr::Column(ref c) if c.name == "location");
                    let right_is_location =
                        matches!(*binary.right, Expr::Column(ref c) if c.name == "location");

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
            Expr::InList(in_list) if matches!(*in_list.expr, Expr::Column(ref c) if c.name == "location") => {
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

#[async_trait]
pub trait ListingTableConnector: DataConnector {
    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        None
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
    fn get_object_store_url(
        &self,
        dataset: &Dataset,
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

    fn get_object_store(&self, dataset: &Dataset) -> DataConnectorResult<Arc<dyn ObjectStore>>
    where
        Self: Display,
    {
        let store_url = self.get_object_store_url(dataset, None)?;
        let listing_store_url = ListingTableUrl::parse(store_url).boxed().context(
            crate::dataconnector::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
            },
        )?;
        self.get_session_context()
            .runtime_env()
            .object_store(&listing_store_url)
            .boxed()
            .context(crate::dataconnector::UnableToConnectInternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
            })
    }

    fn get_runtime(&self) -> Option<Runtime> {
        None
    }

    /// Returns a handle to the IO runtime that this object store connector should
    /// use for spawning IO tasks.
    fn get_tokio_io_runtime(&self) -> tokio::runtime::Handle;

    async fn construct_metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        let store_url: Url = self.get_object_store_url(dataset, None)?;
        let store = self.get_object_store(dataset)?;
        let (_, extension) = self.get_file_format_and_extension(dataset).await?;

        let table = ObjectStoreMetadataTable::try_new(store, &store_url, Some(extension.clone()))
            .context(crate::dataconnector::InvalidConfigurationSnafu {
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
    ///  - csv
    ///
    /// For tabular formats, file options can also be specified in the [`Dataset`]'s `param`s.
    ///
    /// For unstructured text formats, the [`Dataset`]'s `file_format` param key must be set. `Ok`
    /// responses, are always of the format `Ok((None, String))`. The data must be UTF8 compatible.
    async fn get_file_format_and_extension(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<(Option<Arc<dyn FileFormat>>, String)>
    where
        Self: Display,
    {
        let params = self.get_params();
        let extension = params
            .get("file_extension")
            .expose()
            .ok()
            .map(str::to_string);
        let file_extension = std::path::Path::new(dataset.path())
            .extension()
            .map(|ext| ext.to_ascii_lowercase().to_string_lossy().to_string());
        let file_format_param = params.get("file_format").expose().ok();

        match (file_format_param, file_extension.as_deref()) {
            (Some("csv"), _) | (None, Some("csv")) => Ok((
                Some(self.delimiter_separated_format(dataset, params, DelimitedFormat::Csv)?),
                extension.unwrap_or(".csv".to_string()),
            )),
            (Some("tsv"), _) | (None, Some("tsv")) => Ok((
                Some(self.delimiter_separated_format(dataset, params, DelimitedFormat::Tsv)?),
                extension.unwrap_or(".tsv".to_string()),
            )),
            (Some("json"), _) | (None, Some("json")) => Ok((
                Some(self.get_json_format(dataset, params)?),
                extension.unwrap_or(".json".to_string()),
            )),
            (Some("jsonl"), _) | (None, Some("jsonl"))=> Ok((
                Some(self.get_jsonl_format(dataset, params)?),
                extension.unwrap_or(".jsonl".to_string()),
            )),
            #[cfg(feature = "vortex")]
            (Some("vortex"), _) | (None, Some("vortex")) => Ok((
                Some(VortexFormatFactory::new().default()),
                extension.unwrap_or(".vortex".to_string()),
            )),
            (Some("parquet"), _) | (None, Some("parquet"))=> Ok((
                Some(Arc::new(
                    ParquetFormat::default().with_options(self.get_table_parquet_options(dataset).await?),
                )),
                extension.unwrap_or(".parquet".to_string()),
            )),
            (Some(format), _) => Ok((None, format!(".{format}"))),
            (_, _) => Err(
                    crate::dataconnector::DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: "The required 'file_format' parameter is missing. Ensure the parameter is provided, and try again.".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: "Missing file format".into(),
                    },
                ),
        }
    }

    /// Returns a [`JsonFormat`] based on the provided [`Datasets`] parameters.
    ///
    /// If the [`Dataset`] has the relevant parameter, return an error if the value is invalid.
    fn get_jsonl_format(
        &self,
        dataset: &Dataset,
        params: &Parameters,
    ) -> DataConnectorResult<Arc<JsonFormat>>
    where
        Self: Display,
    {
        let mut format = JsonFormat::default();

        if let ExposedParamLookup::Present(comp_as_str) =
            params.get("file_compression_type").expose()
        {
            let compression = comp_as_str.parse::<FileCompressionType>().boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSONL compression_type: {comp_as_str}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_file_compression_type(compression);
        }

        if let ExposedParamLookup::Present(infer_max_rec_str) =
            params.get("schema_infer_max_records").expose()
        {
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
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
    fn get_json_format(
        &self,
        dataset: &Dataset,
        params: &Parameters,
    ) -> DataConnectorResult<Arc<SpiceJsonFormat>>
    where
        Self: Display,
    {
        let mut format = SpiceJsonFormat::default();

        if let ExposedParamLookup::Present(comp_as_str) =
            params.get("file_compression_type").expose()
        {
            let compression = comp_as_str.parse::<FileCompressionType>().boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSON compression_type: {comp_as_str}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_file_compression_type(compression);
        }

        if let ExposedParamLookup::Present(infer_max_rec_str) =
            params.get("schema_infer_max_records").expose()
        {
            let schema_infer_max_rec = usize::from_str(infer_max_rec_str).boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "JSON parameter 'schema_infer_max_records' must be an integer, not {infer_max_rec_str}"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_schema_infer_max_rec(schema_infer_max_rec);
        }

        if let ExposedParamLookup::Present(json_format_str) = params.get("json_format").expose() {
            let json_format = json_format_str.parse::<Format>().boxed().context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!(
                        "Invalid JSON format: {json_format_str}, supported formats are: 'jsonl', 'ndjson', 'array'"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;
            format = format.with_format(json_format);
        }

        if let ExposedParamLookup::Present(flatten_json) = params.get("flatten_json").expose()
            && flatten_json.eq_ignore_ascii_case("true")
        {
            format = format.with_flatten_json(".".to_string());
        }

        Ok(Arc::new(format))
    }

    /// Returns a [`CsvFormat`] based on the provided [`Datasets`] parameters, and choice of delimiter.
    ///
    /// Uses the appropriate parameters based on the [`DelimitedFormat`] provided.
    fn delimiter_separated_format(
        &self,
        dataset: &Dataset,
        params: &Parameters,
        delimiter: DelimitedFormat,
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
        let compression_type = params
            .get("file_compression_type")
            .expose()
            .ok()
            .unwrap_or_default();

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
                .with_file_compression_type(
                    FileCompressionType::from_str(compression_type)
                        .boxed()
                        .context(crate::dataconnector::InvalidConfigurationSnafu {
                            dataconnector: format!("{self}"),
                            message: format!(
                                "Invalid {} compression_type: {compression_type}, supported types are: GZIP, BZIP2, XZ, ZSTD, UNCOMPRESSED", delimiter.to_string().to_uppercase()
                            ),
                            connector_component: ConnectorComponent::from(dataset),
                        })?,
                ),
        ))
    }

    async fn get_table_parquet_options(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<TableParquetOptions>
    where
        Self: Display,
    {
        let mut table_parquet_options = TableParquetOptions::new();
        table_parquet_options
            .set("pushdown_filters", "true")
            .map_err(
                |e| crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                },
            )?;

        if let Some(runtime) = self.get_runtime() {
            let page_index_options = parquet_page_index_options(&runtime).await;

            table_parquet_options
                .set(
                    "enable_page_index",
                    &page_index_options.enable_page_index.to_string(),
                )
                .map_err(
                    |e| crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    },
                )?;

            table_parquet_options
                .set(
                    "tolerate_missing_page_index",
                    &page_index_options.tolerate_missing_page_index.to_string(),
                )
                .map_err(
                    |e| crate::dataconnector::DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    },
                )?;
        }

        Ok(table_parquet_options)
    }

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &Dataset,
        _accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError
    where
        Self: Display,
    {
        crate::dataconnector::DataConnectorError::UnableToConnectInternal {
            dataconnector: format!("{self}"),
            connector_component: ConnectorComponent::from(dataset),
            source: error.into(),
        }
    }

    async fn create_text_table(
        &self,
        dataset: &Dataset,
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
            .context(crate::dataconnector::InvalidConfigurationSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                message: format!(
                    "Invalid file extension ({extension}) for source ({})",
                    dataset.name
                ),
            })?,
        ))
    }

    #[expect(clippy::too_many_lines)]
    async fn create_listing_table(
        &self,
        dataset: &Dataset,
        url: &Url,
        extension: &str,
        file_format: Arc<dyn FileFormat>,
    ) -> DataConnectorResult<Arc<dyn TableProvider>>
    where
        Self: Display,
    {
        // This shouldn't error because we've already validated the URL in `get_object_store_url`.
        let table_path = ListingTableUrl::parse(url.clone()).boxed().context(
            crate::dataconnector::InternalSnafu {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                code: "LTC-RP-LTUP".to_string(), // ListingTableConnector-ReadProvider-ListingTableUrlParse
            },
        )?;

        let object_store = self.get_object_store(dataset)?;

        let ctx: SessionContext = self.get_session_context();

        let (schema_infer_url, schema_infer_meta) =
            if let Some(url) = dataset.params.get("schema_source_path") {
                let url = self.get_object_store_url(dataset, Some(url))?;
                let schema_infer_url = ListingTableUrl::parse(&url).boxed().context(
                    crate::dataconnector::UnableToGetSchemaInternalSnafu {
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
        let mut options = ListingOptions::new(file_format)
            .with_file_extension(extension)
            .with_object_versioning_type(self.object_versioning_type())
            .with_session_config_options(session_state.config());

        let resolved_schema = options
            .infer_schema(&ctx.state(), schema_infer_url.expose_sensitive_url())
            .await
            .map_err(|e| match e {
                DataFusionError::ObjectStore(object_store_error) => {
                    self.handle_object_store_error(dataset, *object_store_error)
                }
                e => crate::dataconnector::DataConnectorError::UnableToConnectInternal {
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
                None => {
                    infer_partitions_with_types_prefix(&ctx.state(), &table_path, extension).await
                }
            };
            match inferred_partitions {
                Ok(partitions) => {
                    tracing::debug!(
                        "Inferred partitions for {:?}: {:?}",
                        table_path,
                        partitions
                            .iter()
                            .map(|(k, _)| k.as_str())
                            .collect::<Vec<_>>()
                    );
                    options = options.with_table_partition_cols(partitions);
                }
                Err(e) => {
                    // This might not be an error, it could be that the table is not partitioned.
                    tracing::debug!("Failed to infer partitions for {:?}: {e}", table_path);
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

        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(options)
            .with_schema(final_schema);

        // This shouldn't error because we're passing the schema and options correctly.
        let table =
            ListingTable::try_new(config)
                .boxed()
                .context(crate::dataconnector::InternalSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    code: "LTC-RP-LTTN".to_string(), // ListingTableConnector-ReadProvider-ListingTableTryNew
                })?;

        // For S3 single-file datasets with acceleration enabled, wrap with a caching layer
        // that checks ETag/Version ID to skip unnecessary re-fetches when file hasn't changed.
        let table_arc = Arc::new(table);
        let is_s3_connector = ListingTableConnector::as_any(self)
            .downcast_ref::<crate::dataconnector::s3::S3>()
            .is_some();
        if is_s3_connector
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
                "Enabled S3 single-file ETag/Version caching for {}",
                dataset.name
            );
            return Ok(Arc::new(cached_table));
        }

        let has_location_metadata = table_arc
            .options()
            .metadata_cols
            .iter()
            .any(|c| matches!(c, MetadataColumn::Location(_)));

        if has_location_metadata {
            let wrapped =
                LocationPruningListingTable::new(table_arc, Arc::clone(&object_store), table_path);
            Ok(Arc::new(wrapped))
        } else {
            Ok(table_arc)
        }
    }

    fn deduplicate_partition_columns_expressed_in_file(
        &self,
        dataset: &Dataset,
        schema: SchemaRef,
        partition_cols: &[(String, DataType)],
    ) -> DataConnectorResult<SchemaRef> {
        if partition_cols.is_empty() {
            return Ok(schema);
        }

        let mut idents = schema
            .fields
            .iter()
            .map(|f| (f.name().to_string(), f.as_ref().clone()))
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
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }

        Some(self.construct_metadata_provider(dataset).await)
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
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

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ListingTableConnector::on_accelerated_table_registration(self, dataset, accelerated_table)
            .await
    }
}

fn refresh_skip_enabled(dataset: &Dataset) -> bool {
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
    mut options: ListingOptions,
    table_url: &Url,
    schema: &Schema,
    dataset: &Dataset,
) -> ListingOptions {
    let url_prefix = get_url_prefix(table_url);
    if let Some(columns) = dataset.listing_table_metadata_columns(url_prefix, schema) {
        tracing::debug!(
            "Enabling metadata columns for '{}': {:?}",
            dataset.name,
            columns
        );
        options = options.with_metadata_cols(columns);
    }

    options
}

// Returns the prefix of the table URL, e.g. for "s3://mybucket/myfolder" it returns "s3://mybucket/"
fn get_url_prefix(table_url: &Url) -> String {
    format!("{}/", &table_url[..url::Position::BeforePath])
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
    dataset: &Dataset,
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
                "Continuing to process {table_path} metadata... {} objects processed so far, representing a total size of: {:.2} GiB",
                file_count,
                total_size as f64 / BYTES_PER_GIB
            );
        }

        if let Some(ext) = file.location.extension() {
            let file_ext = format!(".{ext}");
            found_extensions.insert(file_ext.clone());
            if file_ext == extension {
                if let Some(ref current) = last_modified_file {
                    if current.last_modified < file.last_modified {
                        last_modified_file = Some(file);
                    }
                } else {
                    last_modified_file = Some(file);
                }
            }
        }
    }

    if found_extensions.is_empty() {
        return Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: dataconnector.clone(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "Failed to find any files matching the extension '{extension}'. Spice could not find any files with extensions at the specified path. Check the path and try again."
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
    dataset: &Dataset,
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
        if let Some(ext) = file.location.extension()
            && format!(".{ext}") == extension
        {
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

fn to_listing_table_url(
    original_url: &Url,
    path: &Path,
    dataset: &Dataset,
    dataconnector: &str,
) -> DataConnectorResult<SensitiveListingTableUrl> {
    let mut new_url = original_url.clone();
    new_url.set_path(&format!("/{path}"));

    let sensitive_url = ListingTableUrl::parse(&new_url).boxed().context(
        crate::dataconnector::UnableToGetSchemaInternalSnafu {
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

struct ParquetPageIndexOptions {
    enable_page_index: bool,
    tolerate_missing_page_index: bool,
}

impl Default for ParquetPageIndexOptions {
    fn default() -> Self {
        Self {
            enable_page_index: true,
            tolerate_missing_page_index: false,
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
async fn parquet_page_index_options(runtime: &Runtime) -> ParquetPageIndexOptions {
    let runtime_app = runtime.app();
    let app = runtime_app.read().await;
    let parquet_page_index_param =
        app::App::get_runtime_param(&app, "parquet_page_index", "required".to_string());

    match parquet_page_index_param.as_str() {
        "auto" => ParquetPageIndexOptions {
            enable_page_index: true,
            tolerate_missing_page_index: true,
        },
        "skip" => ParquetPageIndexOptions {
            enable_page_index: false,
            tolerate_missing_page_index: false,
        },
        "required" => ParquetPageIndexOptions::default(),
        _ => {
            tracing::warn!(
                "Invalid value '{}' for runtime.params.parquet_page_index, valid options are: 'auto', 'skip', 'required'. Using 'required'.",
                parquet_page_index_param
            );
            ParquetPageIndexOptions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use datafusion_table_providers::util::secrets::to_secret_map;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use tokio::runtime::Handle;
    use url::Url;

    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
    use crate::dataconnector::{ConnectorParams, DataConnectorFactory};
    use crate::parameters::ParameterSpec;

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

        fn create(
            &self,
            params: ConnectorParams,
        ) -> Pin<Box<dyn Future<Output = crate::dataconnector::NewDataConnectorResult> + Send>>
        {
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
            dataset: &Dataset,
            _url: Option<&str>,
        ) -> DataConnectorResult<Url> {
            Url::parse("test")
                .boxed()
                .context(crate::dataconnector::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "Invalid URL".to_string(),
                })
        }
    }

    const TEST_PARAMETERS: &[ParameterSpec] = LISTING_TABLE_PARAMETERS;

    async fn setup_connector(
        path: String,
        params: HashMap<String, String>,
    ) -> (TestConnector, Dataset) {
        let connector = TestConnector {
            params: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "test",
                TEST_PARAMETERS,
            ),
        };
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;

        let dataset = DatasetBuilder::try_new(path, "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

        (connector, dataset)
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_require_file_format() {
        let (connector, dataset) = setup_connector("test:test/".to_string(), HashMap::new()).await;

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
        let (connector, dataset) =
            setup_connector("test:test.csv".to_string(), HashMap::new()).await;

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
        let (connector, dataset) =
            setup_connector("test:test.parquet".to_string(), HashMap::new()).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
    }

    #[tokio::test]
    async fn test_get_file_format_and_extension_csv_from_params() {
        let mut params = HashMap::new();
        params.insert("file_format".to_string(), "csv".to_string());
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params).await;

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
        let (connector, dataset) = setup_connector("test:test.parquet".to_string(), params).await;

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
        let (connector, dataset) = setup_connector("test:test.csv".to_string(), params).await;

        if let Ok((Some(_file_format), extension)) =
            connector.get_file_format_and_extension(&dataset).await
        {
            assert_eq!(extension, ".parquet");
        } else {
            panic!("Unexpected error");
        }
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

        async fn put(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        async fn get(&self, _location: &Path) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }
        async fn get_opts(
            &self,
            _location: &Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }
        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        fn delete_stream<'a>(
            &'a self,
            _locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }
        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
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
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

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

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Ok(self.meta.clone())
        }

        async fn put(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, _location: &Path) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
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

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
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
        let mut options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_metadata_cols(vec![MetadataColumn::Location(Some("s3://bucket/".into()))]);
        options = options.with_table_partition_cols(vec![]);

        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow_schema::DataType::Utf8,
            true,
        )]));

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
        );

        let filters = vec![datafusion_expr::col("location").eq(datafusion_expr::lit(
            "s3://bucket/prefix/day=2025-01-01/file.parquet",
        ))];

        let plan = provider
            .scan(&ctx.state(), None, &filters, None)
            .await
            .expect("scan with location predicate");

        assert_eq!(plan.schema().fields().len(), 1);

        assert!(
            !no_list_store
                .list_called
                .load(std::sync::atomic::Ordering::SeqCst),
            "Listing should not be invoked when location predicates are present"
        );
    }

    #[tokio::test]
    async fn test_get_last_modified_no_matching_extension() {
        let url = Url::parse("s3://bucket/").expect("to parse url");
        let table_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

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
    async fn test_verify_schema_source_path_valid() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

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
    async fn test_verify_schema_source_path_no_matching_files() {
        let url = Url::parse("s3://bucket/schema/").expect("to parse url");
        let schema_source_path = ListingTableUrl::parse(url.clone()).expect("to parse url");
        let ctx = SessionContext::new();
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

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
        let app = app::AppBuilder::new("test").build();
        let rt = crate::Runtime::builder().build().await;
        let dataset = DatasetBuilder::try_new("s3://bucket/schema/".to_string(), "test")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");

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

    #[tokio::test]
    async fn test_parquet_page_index_options_default() {
        let app = app::AppBuilder::new("test").build();
        let runtime = crate::Runtime::builder()
            .with_app_opt(Some(Arc::new(app)))
            .build()
            .await;

        let options = parquet_page_index_options(&runtime).await;
        assert!(options.enable_page_index);
        assert!(!options.tolerate_missing_page_index);
    }

    #[tokio::test]
    async fn test_parquet_page_index_options_auto() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "auto".to_string());
        let app = app::AppBuilder::new("test")
            .with_runtime_params(params)
            .build();
        let runtime = crate::Runtime::builder()
            .with_app_opt(Some(Arc::new(app)))
            .build()
            .await;

        let options = parquet_page_index_options(&runtime).await;
        assert!(options.enable_page_index);
        assert!(options.tolerate_missing_page_index);
    }

    #[test]
    fn test_extract_location_predicates_equality() {
        use datafusion_expr::{col, lit};

        let filters = vec![col("location").eq(lit("s3://bucket/path/file.parquet"))];
        let values = extract_location_predicates(&filters);
        assert_eq!(
            values,
            Some(vec!["s3://bucket/path/file.parquet".to_string()])
        );
    }

    #[test]
    fn test_extract_location_predicates_in_list() {
        use datafusion_expr::{col, lit};

        let filters = vec![col("location").in_list(
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

        let filters = vec![lit("s3://bucket/reversed.parquet").eq(col("location"))];
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
            col("location")
                .eq(lit("s3://bucket/a.parquet"))
                .and(col("id").gt(lit(1)))
                .or(col("location").eq(lit("s3://bucket/b.parquet"))),
        ];
        let values = extract_location_predicates(&filters);
        assert!(values.is_none(), "Location under OR should disable pruning");
    }

    #[test]
    fn test_extract_location_predicates_not_wrapped() {
        use datafusion_expr::{col, lit};

        let filters = vec![datafusion_expr::not(
            col("location").eq(lit("s3://bucket/negated.parquet")),
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
                .and(col("location").eq(lit("s3://bucket/only_location.parquet"))),
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

        let filters = vec![col("location").in_list(
            vec![lit("s3://bucket/a.parquet"), lit("s3://bucket/b.parquet")],
            true,
        )];
        let values = extract_location_predicates(&filters);
        assert!(
            values.is_none(),
            "Negated IN should disable location pruning"
        );
    }

    #[tokio::test]
    async fn test_parquet_page_index_options_skip() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "skip".to_string());
        let app = app::AppBuilder::new("test")
            .with_runtime_params(params)
            .build();
        let runtime = crate::Runtime::builder()
            .with_app_opt(Some(Arc::new(app)))
            .build()
            .await;

        let options = parquet_page_index_options(&runtime).await;
        assert!(!options.enable_page_index);
        assert!(!options.tolerate_missing_page_index);
    }

    #[tokio::test]
    async fn test_parquet_page_index_options_required() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "required".to_string());
        let app = app::AppBuilder::new("test")
            .with_runtime_params(params)
            .build();
        let runtime = crate::Runtime::builder()
            .with_app_opt(Some(Arc::new(app)))
            .build()
            .await;

        let options = parquet_page_index_options(&runtime).await;
        assert!(options.enable_page_index);
        assert!(!options.tolerate_missing_page_index);
    }

    #[tokio::test]
    async fn test_parquet_page_index_options_invalid() {
        let mut params = std::collections::HashMap::new();
        params.insert("parquet_page_index".to_string(), "invalid".to_string());
        let app = app::AppBuilder::new("test")
            .with_runtime_params(params)
            .build();
        let runtime = crate::Runtime::builder()
            .with_app_opt(Some(Arc::new(app)))
            .build()
            .await;

        let options = parquet_page_index_options(&runtime).await;
        // Should fall back to default
        assert!(options.enable_page_index);
        assert!(!options.tolerate_missing_page_index);
    }
}
