// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`SpiceJsonFormat`]: Line delimited JSON [`FileFormat`] abstractions

use std::collections::{BTreeSet, VecDeque};
use std::fmt::{self, Debug};
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::sync::Arc;

use crate::source::SpiceJsonSource;
use crate::{
    ArrayToNdjson, ArrayToNdjsonPush, JsonPointerReader, ReadResult, SodaReader,
    body_opens_a_json_array, extract_flattened_from_nested, flatten_json_obj, is_soda_response,
    unnest_struct_schema,
};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::json;
use arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};

use async_trait::async_trait;
use bytes::Buf;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::{DisplayFormatType, Partitioning};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{DEFAULT_JSON_EXTENSION, GetExt, Statistics, not_impl_err},
    datasource::{
        file_format::{
            DEFAULT_SCHEMA_INFER_MAX_RECORD, FileFormat, file_compression_type::FileCompressionType,
        },
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
    },
    error::Result,
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::decoder::Decoder;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::source::DataSource;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};
use snafu::prelude::*;

#[derive(Debug, Clone)]
pub struct SpiceJsonOptions {
    pub compression: CompressionTypeVariant,
    pub schema_infer_max_rec: Option<usize>,
    pub format: Format,
    /// If set, flatten nested structs with the given separator
    pub flatten_json: Option<String>,
    /// If set, extract data at the specified [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)
    /// path within a JSON object.
    /// E.g. `"/data"` for `{"data": [...]}` or `"/response/items"` for nested objects.
    /// A leading `/` is added automatically if missing.
    pub json_pointer: Option<String>,
    /// When `true`, include Socrata `meta_data` columns (sid, id, position, etc.)
    /// in the schema for SODA format responses. Disabled by default.
    pub soda_metadata: bool,
}

#[derive(Debug, Snafu)]
pub enum FormatParseError {
    #[snafu(display(
        "Invalid JSON format '{s}'. Valid formats are: 'auto', 'json', 'jsonl', 'ndjson', 'ldjson', 'array', 'object', 'soda', 'socrata'",
    ))]
    InvalidFormat { s: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// Line-delimited JSON format (JSONL/NDJSON/LDJSON).
    /// Each line contains a valid JSON value separated by newlines.
    /// Supports both `\n` and `\r\n` line endings.
    Jsonl,
    /// JSON array format where the entire file is a single JSON array.
    Array,
    /// Single JSON object format. The file contains exactly one JSON object
    /// (e.g. `{"name": "Alice", "age": 30}`) which produces a single row.
    Object,
    /// Plain JSON: auto-detect array vs JSONL by peeking at the first byte,
    /// but do NOT perform SODA auto-detection.
    /// Use this when the user explicitly sets `file_format: json`.
    Json,
    /// Auto-detect format including SODA. Peeks at the first non-whitespace byte
    /// (`[` → Array, `{` → check for SODA, otherwise → Jsonl).
    Auto,
    /// SODA (Socrata Open Data API) format.
    /// The response is a JSON object with `meta.view.columns` for schema
    /// and `data` for the row data (array of arrays).
    /// Schema is derived from `meta.view.columns` instead of row-based inference.
    Soda,
}

impl FromStr for Format {
    type Err = FormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            // JSONL (JSON Lines), NDJSON (Newline Delimited JSON), and LDJSON (Line Delimited JSON)
            // are all essentially the same format - one JSON value per line.
            "jsonl" | "ndjson" | "ldjson" => Ok(Format::Jsonl),
            "array" => Ok(Format::Array),
            "object" => Ok(Format::Object),
            "json" => Ok(Format::Json),
            "auto" => Ok(Format::Auto),
            "soda" | "socrata" => Ok(Format::Soda),
            _ => InvalidFormatSnafu { s: s.to_string() }.fail(),
        }
    }
}

impl Default for SpiceJsonOptions {
    fn default() -> Self {
        Self {
            compression: CompressionTypeVariant::UNCOMPRESSED,
            schema_infer_max_rec: None,
            format: Format::Auto,
            flatten_json: None,
            json_pointer: None,
            soda_metadata: false,
        }
    }
}

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug, Default, Clone)]
pub struct SpiceJsonFormat {
    options: SpiceJsonOptions,
}

impl SpiceJsonFormat {
    /// Set JSON options
    #[must_use]
    pub fn with_options(mut self, options: SpiceJsonOptions) -> Self {
        self.options = options;
        self
    }

    /// Retrieve JSON options
    #[must_use]
    pub fn options(&self) -> &SpiceJsonOptions {
        &self.options
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    #[must_use]
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.options.schema_infer_max_rec = Some(max_rec);
        self
    }

    /// Set a [`FileCompressionType`] of JSON
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    #[must_use]
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.options.compression = file_compression_type.into();
        self
    }

    /// Set the `format` option
    #[must_use]
    pub fn with_format(mut self, format: Format) -> Self {
        self.options.format = format;
        self
    }

    /// Set the `flatten_json` option with the given separator.
    #[must_use]
    pub fn with_flatten_json(mut self, flatten_json_separator: String) -> Self {
        self.options.flatten_json = Some(flatten_json_separator);
        self
    }

    /// Set the `json_pointer` option for extracting data from a specific path in a JSON object.
    #[must_use]
    pub fn with_json_pointer(mut self, json_pointer: String) -> Self {
        self.options.json_pointer = if json_pointer.is_empty() {
            None
        } else {
            Some(json_pointer)
        };
        self
    }

    /// When `true`, include Socrata `meta_data` columns in the SODA schema.
    #[must_use]
    pub fn with_soda_metadata(mut self, enabled: bool) -> Self {
        self.options.soda_metadata = enabled;
        self
    }

    /// Infer the schema of `objects` and count how many JSON records were
    /// inspected. The count is `0` for an empty array, an empty pointer
    /// target, or blank JSONL. An empty object (`{}` / `[{}]`) is one record
    /// with no keys, which is not the same as zero records.
    ///
    /// # Errors
    ///
    /// Returns the error reading a file, decompressing it, extracting a JSON
    /// pointer, or inferring a schema from the records.
    pub async fn probe_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<JsonSchemaProbe> {
        let mut schemas = Vec::new();
        let mut records_to_read = self
            .options
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let mut record_count = 0;
        let file_compression_type = FileCompressionType::from(self.options.compression);
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                    record_count += 1;
                }
                should_take
            };

            let r = store.as_ref().get(&object.location).await?;
            let raw_buf = match r.payload {
                GetResultPayload::File(file, _) => {
                    let mut decoder = file_compression_type.convert_read(file)?;
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;
                    raw_buf
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let mut decoder = file_compression_type.convert_read(data.reader())?;
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;
                    raw_buf
                }
            };
            schemas.push(self.schema_from_raw_buf(&raw_buf, &mut take_while)?);
            if records_to_read == 0 {
                break;
            }
        }

        Ok(JsonSchemaProbe {
            schema: Arc::new(Schema::try_merge(schemas)?),
            record_count,
        })
    }

    /// Collect top-level object keys and a record count from `objects`.
    /// Values are not type-merged: an unregistered key that is a number in one
    /// record and an object in another is still one key.
    ///
    /// When `flatten_json` is set, a null-valued key that is the parent path of
    /// a name in `registered` is skipped so `{"user": null}` does not look like
    /// a rename of `user.name`.
    ///
    /// The count is `0` for an empty array, an empty pointer target, or blank
    /// JSONL. An empty object (`{}` / `[{}]`) is one record with no keys.
    ///
    /// # Errors
    ///
    /// Returns the error reading a file, decompressing it, extracting a JSON
    /// pointer, or parsing a record. Type conflicts between records are not
    /// an error.
    pub async fn probe_keys(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
        registered: &BTreeSet<String>,
    ) -> Result<JsonKeyProbe> {
        let mut keys = BTreeSet::new();
        let mut records_to_read = self
            .options
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let mut record_count = 0;
        let file_compression_type = FileCompressionType::from(self.options.compression);
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                    record_count += 1;
                }
                should_take
            };

            let r = store.as_ref().get(&object.location).await?;
            let raw_buf = match r.payload {
                GetResultPayload::File(file, _) => {
                    let mut decoder = file_compression_type.convert_read(file)?;
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;
                    raw_buf
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let mut decoder = file_compression_type.convert_read(data.reader())?;
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;
                    raw_buf
                }
            };
            self.collect_keys_from_raw_buf(&raw_buf, &mut take_while, registered, &mut keys)?;
            if records_to_read == 0 {
                break;
            }
        }

        Ok(JsonKeyProbe { keys, record_count })
    }

    fn collect_keys_from_raw_buf(
        &self,
        raw_buf: &[u8],
        take_while: &mut dyn FnMut() -> bool,
        registered: &BTreeSet<String>,
        keys: &mut BTreeSet<String>,
    ) -> Result<()> {
        let is_soda = self.options.format == Format::Soda
            || (self.options.format == Format::Auto && is_soda_response(raw_buf));

        if is_soda && self.options.json_pointer.is_some() {
            return Err(DataFusionError::Configuration(
                "'json_pointer' cannot be used with SODA format data. \
                 SODA responses contain their own schema in 'meta.view.columns' \
                 and data is extracted automatically. \
                 Remove 'json_pointer' or set 'file_format: json' to treat the file as regular JSON."
                    .to_string(),
            ));
        }

        if is_soda {
            let soda = SodaReader::from_vec(raw_buf, self.options.soda_metadata)?;
            take_soda_rows(&soda, take_while);
            let schema = if let Some(separator) = &self.options.flatten_json {
                unnest_struct_schema(soda.schema(), separator)
            } else {
                soda.schema().clone()
            };
            keys.extend(schema.fields().iter().map(|f| f.name().clone()));
            return Ok(());
        }

        let extracted = if let Some(path) = &self.options.json_pointer {
            let reader = JsonPointerReader::from_vec(raw_buf, path)?;
            let mut out = Vec::new();
            std::io::BufReader::new(reader).read_to_end(&mut out)?;
            Some(out)
        } else {
            None
        };
        let buf = extracted.as_deref().unwrap_or(raw_buf);
        collect_json_keys_for_format(
            buf,
            self.options.format,
            take_while,
            self.options.flatten_json.as_deref(),
            registered,
            keys,
        )
    }

    fn schema_from_raw_buf(
        &self,
        raw_buf: &[u8],
        take_while: &mut dyn FnMut() -> bool,
    ) -> Result<Schema> {
        let is_soda = self.options.format == Format::Soda
            || (self.options.format == Format::Auto && is_soda_response(raw_buf));

        if is_soda && self.options.json_pointer.is_some() {
            return Err(DataFusionError::Configuration(
                "'json_pointer' cannot be used with SODA format data. \
                 SODA responses contain their own schema in 'meta.view.columns' \
                 and data is extracted automatically. \
                 Remove 'json_pointer' or set 'file_format: json' to treat the file as regular JSON."
                    .to_string(),
            ));
        }

        if is_soda {
            let soda = SodaReader::from_vec(raw_buf, self.options.soda_metadata)?;
            take_soda_rows(&soda, take_while);
            let schema = soda.schema().clone();
            return Ok(if let Some(separator) = &self.options.flatten_json {
                unnest_struct_schema(&schema, separator)
            } else {
                schema
            });
        }

        let extracted = if let Some(path) = &self.options.json_pointer {
            let reader = JsonPointerReader::from_vec(raw_buf, path)?;
            let mut out = Vec::new();
            std::io::BufReader::new(reader).read_to_end(&mut out)?;
            Some(out)
        } else {
            None
        };
        let buf = extracted.as_deref().unwrap_or(raw_buf);
        let schema = infer_json_schema_for_format(buf, self.options.format, take_while)?;
        Ok(if let Some(separator) = &self.options.flatten_json {
            unnest_struct_schema(&schema, separator)
        } else {
            schema
        })
    }
}

/// Schema inferred from a JSON source plus how many records were inspected.
#[derive(Debug, Clone)]
pub struct JsonSchemaProbe {
    /// Union of object keys seen while probing `objects`.
    pub schema: SchemaRef,
    /// Records `take_while` accepted while inferring. `0` means the files
    /// contained no JSON values (`[]`, `{"data":[]}` after a pointer, blank
    /// JSONL). An empty object is one record with no keys.
    pub record_count: usize,
}

/// Top-level object keys from a JSON source plus how many records were inspected.
///
/// Unlike [`JsonSchemaProbe`], this does not merge value types. It is the
/// probe the file schema-drift guard uses so an unregistered key that
/// changes type across records cannot fail a refresh.
#[derive(Debug, Clone)]
pub struct JsonKeyProbe {
    /// Top-level keys (flattened when `flatten_json` is set) seen in any record.
    pub keys: BTreeSet<String>,
    /// Records `take_while` accepted. `0` means the files contained no JSON
    /// values (`[]`, `{"data":[]}` after a pointer, blank JSONL). An empty
    /// object is one record with no keys.
    pub record_count: usize,
}

#[async_trait]
impl FileFormat for SpiceJsonFormat {
    fn get_ext(&self) -> String {
        DEFAULT_JSON_EXTENSION[1..].to_string()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(self.options.compression.into())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        Ok(self.probe_schema(state, store, objects).await?.schema)
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let needs_non_repartitioned = matches!(
            self.options.format,
            Format::Array | Format::Object | Format::Auto | Format::Json | Format::Soda
        ) || self.options.json_pointer.is_some();

        if needs_non_repartitioned {
            tracing::debug!(
                "Creating non-repartitioned DataSource for JSON for url: {}",
                conf.object_store_url
            );
            // Use NonRepartitionedFileScanConfig to prevent repartitioning for JSON array/auto/json_pointer files,
            // as splitting would break parsing due to incomplete JSON fragments.
            // In order to still allow parallel read for individual files, we wrap them into separate files groups
            let individual_file_groups: Vec<_> = conf
                .file_groups
                .iter()
                .flat_map(|group| {
                    group
                        .iter()
                        .map(|file_meta| FileGroup::new(vec![file_meta.clone()]))
                })
                .collect();

            let conf = FileScanConfigBuilder::from(conf)
                .with_file_groups(individual_file_groups)
                .with_file_compression_type(FileCompressionType::from(self.options.compression))
                .build();

            return Ok(DataSourceExec::from_data_source(
                NonRepartitionedFileScanConfig::new(conf),
            ));
        }

        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(self.options.compression))
            .build();

        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Inserts are not implemented yet for Json")
    }

    fn file_source(&self, table_schema: datafusion_datasource::TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            SpiceJsonSource::new(table_schema.clone())
                .with_format(self.options.format)
                .with_json_pointer(self.options.json_pointer.clone())
                .with_unnest_struct(self.options.flatten_json.clone())
                .with_soda_metadata(self.options.soda_metadata)
                .with_table_schema(table_schema),
        )
    }
}

#[derive(Debug)]
pub struct SpiceJsonDecoder {
    inner: json::reader::Decoder,
    unnest_struct: Option<String>,
    array_to_ndjson_push: Option<ArrayToNdjsonPush>,
    pending: VecDeque<u8>,
    projected_schema: SchemaRef,
}

impl SpiceJsonDecoder {
    #[must_use]
    pub fn new(
        decoder: json::reader::Decoder,
        array_to_ndjson: bool,
        unnest_struct: Option<String>,
        projected_schema: SchemaRef,
    ) -> Self {
        let array_to_ndjson_push = if array_to_ndjson {
            Some(ArrayToNdjsonPush::new())
        } else {
            None
        };
        Self {
            inner: decoder,
            array_to_ndjson_push,
            unnest_struct,
            pending: VecDeque::new(),
            projected_schema,
        }
    }
}

impl Decoder for SpiceJsonDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        let buf_len = buf.len();
        if let Some(push) = &mut self.array_to_ndjson_push {
            push.push_bytes(buf)?;
            while let ReadResult::Ready(data) = push.try_read() {
                self.pending.extend(data);
            }
        } else {
            self.pending.extend(buf);
        }

        let pending_contiguous = self.pending.make_contiguous();
        let n = self.inner.decode(pending_contiguous)?;
        self.pending.drain(..n);

        // We always consume the entire buffer, so return the length of the buffer
        Ok(buf_len)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        // `can_flush_early` is false, so the deserializer only flushes once the
        // input stream is exhausted. That makes this the one place the adapter
        // can be told the file has ended, and the only place a still-open array
        // can be reported instead of silently returning the rows read so far.
        if let Some(push) = &self.array_to_ndjson_push {
            push.finish()?;
        }

        let projected_schema = Arc::clone(&self.projected_schema);
        self.inner.flush().map(move |batch| {
            batch.map(|batch| {
                if let Some(separator) = &self.unnest_struct {
                    return extract_flattened_from_nested(&batch, &projected_schema, separator)
                        .unwrap_or(batch);
                }
                batch
            })
        })
    }

    fn can_flush_early(&self) -> bool {
        false
    }
}

/// A wrapper around `FileScanConfig` that prevents `DataFusion` from automatically
/// repartitioning files into multiple partitions/chunks.
///
/// This is specifically useful for JSON array files where splitting the file
/// into multiple partitions would break the JSON parsing, as each partition
/// would contain incomplete JSON fragments.
///
/// Note: A single partition is applied to the entire file group. To read files in parallel,
/// organize files into multiple `file_groups`.
///
/// The inner config is public so scan-time file discovery (schema-drift checks)
/// can see the same files a plain [`FileScanConfig`] scan would list. Array,
/// object, auto, JSON, SODA, and `json_pointer` scans all wrap here.
#[derive(Debug)]
pub struct NonRepartitionedFileScanConfig {
    inner: FileScanConfig,
}

impl NonRepartitionedFileScanConfig {
    fn new(inner: FileScanConfig) -> Self {
        Self { inner }
    }

    /// The file groups and object-store URL this wrapper scans.
    #[must_use]
    pub fn file_scan_config(&self) -> &FileScanConfig {
        &self.inner
    }
}

impl DataSource for NonRepartitionedFileScanConfig {
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None) // Return None to prevent repartitioning
    }
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        tracing::trace!(
            "NonRepartitionedFileScanConfig: opening partition {partition} (task_id: {:?}, session_id: {:?})",
            context.task_id(),
            context.session_id()
        );
        self.inner.open(partition, context)
    }
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }
    fn eq_properties(&self) -> EquivalenceProperties {
        self.inner.eq_properties()
    }
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        self.inner.with_fetch(limit)
    }
    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.inner.metrics()
    }
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        match self.inner.try_swapping_with_projection(projection)? {
            Some(new_inner) => {
                let new_config = new_inner
                    .downcast_ref::<FileScanConfig>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "NonRepartitionedFileScanConfig inner must be FileScanConfig"
                                .to_string(),
                        )
                    })?
                    .clone();
                Ok(Some(Arc::new(NonRepartitionedFileScanConfig::new(
                    new_config,
                ))))
            }
            None => Ok(None),
        }
    }
}

/// UTF-8 BOM prefix (`\xEF\xBB\xBF`).
const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// Offers each SODA `data` row to `take_while`, so a SODA source counts its
/// records like other JSON input and an empty `data` array counts as none.
fn take_soda_rows(soda: &SodaReader, take_while: &mut dyn FnMut() -> bool) {
    for _ in 0..soda.row_count() {
        if !take_while() {
            break;
        }
    }
}

/// Infer a JSON schema from all bytes in `buf`, dispatching by [`Format`].
///
/// For [`Format::Object`] the buffer is parsed as a single JSON value (handles
/// multi-line / pretty-printed objects).
///
/// For [`Format::Auto`] and [`Format::Json`], when the content starts with `{`
/// the function first tries to parse the entire buffer as a single JSON object.
/// If that succeeds (no trailing content) the schema is inferred from that one
/// value. If it fails (e.g. because there are multiple NDJSON lines) it falls
/// back to Arrow's line-based `ValueIter`.
fn infer_json_schema_for_format(
    buf: &[u8],
    format: Format,
    take_while: &mut dyn FnMut() -> bool,
) -> Result<Schema> {
    // Strip a leading UTF-8 BOM so serde_json can parse the input.
    let buf = buf.strip_prefix(&UTF8_BOM).unwrap_or(buf);

    if format == Format::Object {
        let value: serde_json::Value =
            serde_json::from_slice(buf).map_err(|e| DataFusionError::External(Box::new(e)))?;
        // Decrement schema_infer_max_rec consistently for single-object inputs.
        take_while();
        return infer_json_schema_from_iterator(std::iter::once(Ok::<_, ArrowError>(value)))
            .map_err(DataFusionError::from);
    }

    let mut reader = BufReader::new(std::io::Cursor::new(buf));

    let is_array = match format {
        Format::Array => true,
        Format::Jsonl | Format::Object => false,
        Format::Soda => {
            return Err(DataFusionError::Internal(
                "Format::Soda should be handled before calling infer_json_schema_for_format"
                    .to_string(),
            ));
        }
        Format::Auto | Format::Json => body_opens_a_json_array(&mut reader)?,
    };

    if is_array {
        let mut adapter = ArrayToNdjson::try_new(reader)?;
        let iter = ValueIter::new(&mut adapter, None);
        Ok(infer_json_schema_from_iterator(
            iter.take_while(|_| take_while()),
        )?)
    } else {
        // For Auto/Json: try parsing as a single JSON object first.
        // This handles multi-line/pretty-printed objects that the line-based
        // ValueIter cannot parse. serde_json::from_slice rejects input with
        // trailing non-whitespace content, so NDJSON files will fail here and
        // fall through to ValueIter.
        if matches!(format, Format::Auto | Format::Json)
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(buf)
            && value.is_object()
        {
            take_while();
            return Ok(infer_json_schema_from_iterator(std::iter::once(Ok::<
                _,
                ArrowError,
            >(
                value
            )))?);
        }

        // NDJSON: use line-based ValueIter
        let mut reader = BufReader::new(std::io::Cursor::new(buf));
        let iter = ValueIter::new(&mut reader, None);
        Ok(infer_json_schema_from_iterator(
            iter.take_while(|_| take_while()),
        )?)
    }
}

fn collect_record_keys(
    value: &serde_json::Value,
    flatten_separator: Option<&str>,
    registered: &BTreeSet<String>,
    keys: &mut BTreeSet<String>,
) {
    let flattened = flatten_separator.map(|sep| flatten_json_obj(value, sep));
    let object = flattened.as_ref().unwrap_or(value).as_object();
    if let Some(object) = object {
        for (key, val) in object {
            if is_null_parent_of_registered(key, val, flatten_separator, registered) {
                continue;
            }
            keys.insert(key.clone());
        }
    }
}

/// `{"user": null}` flattens to key `user`. A registered `user.name` then
/// looks missing while `user` looks new, which the drift guard reads as a
/// rename. Skip that null parent so the missing child is a sparse nullable.
fn is_null_parent_of_registered(
    key: &str,
    value: &serde_json::Value,
    flatten_separator: Option<&str>,
    registered: &BTreeSet<String>,
) -> bool {
    let Some(separator) = flatten_separator else {
        return false;
    };
    if !value.is_null() {
        return false;
    }
    registered.iter().any(|name| {
        name.starts_with(key)
            && name
                .get(key.len()..)
                .is_some_and(|rest| rest.starts_with(separator))
    })
}

/// Collect top-level object keys from `buf` using the same array / object /
/// JSONL extraction as [`infer_json_schema_for_format`], without merging
/// value types.
fn collect_json_keys_for_format(
    buf: &[u8],
    format: Format,
    take_while: &mut dyn FnMut() -> bool,
    flatten_separator: Option<&str>,
    registered: &BTreeSet<String>,
    keys: &mut BTreeSet<String>,
) -> Result<()> {
    let buf = buf.strip_prefix(&UTF8_BOM).unwrap_or(buf);

    if format == Format::Object {
        let value: serde_json::Value =
            serde_json::from_slice(buf).map_err(|e| DataFusionError::External(Box::new(e)))?;
        take_while();
        collect_record_keys(&value, flatten_separator, registered, keys);
        return Ok(());
    }

    let mut reader = BufReader::new(std::io::Cursor::new(buf));

    let is_array = match format {
        Format::Array => true,
        Format::Jsonl | Format::Object => false,
        Format::Soda => {
            return Err(DataFusionError::Internal(
                "Format::Soda should be handled before calling collect_json_keys_for_format"
                    .to_string(),
            ));
        }
        Format::Auto | Format::Json => body_opens_a_json_array(&mut reader)?,
    };

    if is_array {
        let mut adapter = ArrayToNdjson::try_new(reader)?;
        let iter = ValueIter::new(&mut adapter, None);
        for value in iter.take_while(|_| take_while()) {
            collect_record_keys(&value?, flatten_separator, registered, keys);
        }
        return Ok(());
    }

    if matches!(format, Format::Auto | Format::Json)
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(buf)
        && value.is_object()
    {
        take_while();
        collect_record_keys(&value, flatten_separator, registered, keys);
        return Ok(());
    }

    let mut reader = BufReader::new(std::io::Cursor::new(buf));
    let iter = ValueIter::new(&mut reader, None);
    for value in iter.take_while(|_| take_while()) {
        collect_record_keys(&value?, flatten_separator, registered, keys);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_parse_jsonl() {
        assert!(matches!("jsonl".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("JSONL".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("Jsonl".parse::<Format>(), Ok(Format::Jsonl)));
    }

    #[test]
    fn test_format_parse_ndjson() {
        // NDJSON (Newline Delimited JSON) is treated the same as JSONL
        assert!(matches!("ndjson".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("NDJSON".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("Ndjson".parse::<Format>(), Ok(Format::Jsonl)));
    }

    #[test]
    fn test_format_parse_ldjson() {
        // LDJSON (Line Delimited JSON) is treated the same as JSONL
        assert!(matches!("ldjson".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("LDJSON".parse::<Format>(), Ok(Format::Jsonl)));
        assert!(matches!("Ldjson".parse::<Format>(), Ok(Format::Jsonl)));
    }

    #[test]
    fn test_format_parse_array() {
        assert!(matches!("array".parse::<Format>(), Ok(Format::Array)));
        assert!(matches!("ARRAY".parse::<Format>(), Ok(Format::Array)));
        assert!(matches!("Array".parse::<Format>(), Ok(Format::Array)));
    }

    #[test]
    fn test_format_parse_auto() {
        assert!(matches!("auto".parse::<Format>(), Ok(Format::Auto)));
        assert!(matches!("AUTO".parse::<Format>(), Ok(Format::Auto)));
        assert!(matches!("Auto".parse::<Format>(), Ok(Format::Auto)));
    }

    #[test]
    fn test_format_parse_object() {
        assert!(matches!("object".parse::<Format>(), Ok(Format::Object)));
        assert!(matches!("OBJECT".parse::<Format>(), Ok(Format::Object)));
        assert!(matches!("Object".parse::<Format>(), Ok(Format::Object)));
    }

    #[test]
    fn test_format_parse_json() {
        // "json" maps to Auto (auto-detect sub-format)
        assert!(matches!("json".parse::<Format>(), Ok(Format::Json)));
        assert!(matches!("JSON".parse::<Format>(), Ok(Format::Json)));
        assert!(matches!("Json".parse::<Format>(), Ok(Format::Json)));
    }

    #[test]
    fn test_format_parse_soda() {
        assert!(matches!("soda".parse::<Format>(), Ok(Format::Soda)));
        assert!(matches!("SODA".parse::<Format>(), Ok(Format::Soda)));
        assert!(matches!("Soda".parse::<Format>(), Ok(Format::Soda)));
    }

    #[test]
    fn test_format_parse_socrata() {
        // "socrata" is an alias for Soda
        assert!(matches!("socrata".parse::<Format>(), Ok(Format::Soda)));
        assert!(matches!("SOCRATA".parse::<Format>(), Ok(Format::Soda)));
        assert!(matches!("Socrata".parse::<Format>(), Ok(Format::Soda)));
    }

    #[test]
    fn test_format_parse_invalid() {
        // The message echoes the value exactly as written — not lowercased — and
        // lists every accepted spelling.
        for input in ["", "invalid", "Parquet"] {
            let err = input
                .parse::<Format>()
                .expect_err("an unknown format must be rejected");
            assert!(
                matches!(&err, FormatParseError::InvalidFormat { s } if s == input),
                "the error must carry {input:?}, got {err:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "Invalid JSON format '{input}'. Valid formats are: 'auto', 'json', 'jsonl', 'ndjson', 'ldjson', 'array', 'object', 'soda', 'socrata'"
                )
            );
        }
    }

    #[test]
    fn empty_json_array_inspects_zero_records() {
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let schema = infer_json_schema_for_format(b"[]", Format::Array, &mut take)
            .expect("empty array is a valid schema");
        assert_eq!(schema.fields().len(), 0);
        assert_eq!(n, 0, "[] is zero records, not a missing-column list");
    }

    #[test]
    fn empty_json_object_array_inspects_one_record() {
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let schema = infer_json_schema_for_format(b"[{}]", Format::Array, &mut take)
            .expect("empty object is a valid record");
        assert_eq!(schema.fields().len(), 0);
        assert_eq!(n, 1, "[{{}}] is one record with no keys");
    }

    #[test]
    fn blank_jsonl_inspects_zero_records() {
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let schema = infer_json_schema_for_format(b"\n\n  \n", Format::Jsonl, &mut take)
            .expect("blank JSONL is a valid schema");
        assert_eq!(schema.fields().len(), 0);
        assert_eq!(n, 0);
    }

    #[test]
    fn infer_json_schema_fails_when_an_unregistered_key_changes_type() {
        let buf = concat!(
            r#"{"id":1,"extra":1}"#,
            "\n",
            r#"{"id":2,"extra":{}}"#,
            "\n"
        );
        let mut take = || true;
        let err = infer_json_schema_for_format(buf.as_bytes(), Format::Jsonl, &mut take)
            .expect_err("arrow-json type merge fails when extra changes type");
        let message = err.to_string();
        assert!(
            message.contains("Expected scalar or scalar array JSON type")
                || message.contains("Expected object json type"),
            "{message}"
        );
    }

    #[test]
    fn collect_json_keys_ignores_value_types() {
        let buf = concat!(
            r#"{"id":1,"extra":1}"#,
            "\n",
            r#"{"id":2,"extra":{}}"#,
            "\n"
        );
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            buf.as_bytes(),
            Format::Jsonl,
            &mut take,
            None,
            &BTreeSet::new(),
            &mut keys,
        )
        .expect("key collection does not merge types");
        assert_eq!(n, 2);
        assert_eq!(
            keys,
            BTreeSet::from(["id".to_string(), "extra".to_string()])
        );
    }

    #[test]
    fn collect_json_keys_empty_array_is_zero_records() {
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            b"[]",
            Format::Array,
            &mut take,
            None,
            &BTreeSet::new(),
            &mut keys,
        )
        .expect("empty array");
        assert_eq!(n, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn collect_json_keys_empty_object_array_is_one_record() {
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            b"[{}]",
            Format::Array,
            &mut take,
            None,
            &BTreeSet::new(),
            &mut keys,
        )
        .expect("empty object is one record");
        assert_eq!(n, 1);
        assert!(keys.is_empty());
    }

    #[test]
    fn collect_json_keys_skips_a_null_flatten_parent_of_a_registered_column() {
        let registered = BTreeSet::from(["user.name".to_string()]);
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            br#"{"id":1,"user":null}"#,
            Format::Jsonl,
            &mut take,
            Some("."),
            &registered,
            &mut keys,
        )
        .expect("null parent of a registered child");
        assert_eq!(n, 1);
        assert_eq!(
            keys,
            BTreeSet::from(["id".to_string()]),
            "null parent of user.name must not appear as an extra key"
        );
    }

    #[test]
    fn collect_json_keys_keeps_a_null_flatten_key_that_is_not_a_registered_parent() {
        let registered = BTreeSet::from(["id".to_string()]);
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            br#"{"id":1,"user":null}"#,
            Format::Jsonl,
            &mut take,
            Some("."),
            &registered,
            &mut keys,
        )
        .expect("unrelated null key");
        assert_eq!(n, 1);
        assert_eq!(keys, BTreeSet::from(["id".to_string(), "user".to_string()]));
    }

    #[test]
    fn collect_json_keys_keeps_a_non_null_flatten_parent() {
        let registered = BTreeSet::from(["user.name".to_string()]);
        let mut n = 0;
        let mut take = || {
            n += 1;
            true
        };
        let mut keys = BTreeSet::new();
        collect_json_keys_for_format(
            br#"{"user":"alice"}"#,
            Format::Jsonl,
            &mut take,
            Some("."),
            &registered,
            &mut keys,
        )
        .expect("scalar parent");
        assert_eq!(n, 1);
        assert_eq!(keys, BTreeSet::from(["user".to_string()]));
    }

    #[test]
    fn test_infer_schema_object_single_line() {
        let buf = br#"{"name": "Alice", "age": 30}"#;
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect("should infer schema from single-line object");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    /// Format detection runs before the array reader and *consumes* the
    /// prefix it skips, so whichever predicate it uses decides what the
    /// reader's own guards ever get to see. With the wider
    /// `is_ascii_whitespace`, a leading form feed — which `serde_json` rejects
    /// — was eaten here and the body reached `ArrayToNdjson` already looking
    /// well formed.
    ///
    /// This goes through `infer_json_schema_for_format`, the wiring the
    /// default `Format::Auto` actually uses; a test that constructs the
    /// adapter itself passes whether or not detection is correct.
    ///
    /// Each rejection is matched to the guard that raised it, because every
    /// one of these bodies fails *somewhere*: an `is_err` check passes just as
    /// well for a body that slipped past detection and failed later for an
    /// unrelated reason.
    #[test]
    fn a_leading_form_feed_is_not_eaten_by_format_detection() {
        /// The rejection, tagged with the reader that raised it.
        fn rejection(body: &[u8], format: Format) -> String {
            let mut take = || true;
            match infer_json_schema_for_format(body, format, &mut take) {
                Ok(schema) => panic!(
                    "{format:?} accepted {:?}, which serde_json rejects, as {schema:?}",
                    String::from_utf8_lossy(body)
                ),
                Err(DataFusionError::IoError(e)) => format!("io {:?}: {e}", e.kind()),
                Err(DataFusionError::ArrowError(e, _)) => format!("arrow: {e}"),
                Err(other) => format!("other: {other}"),
            }
        }
        fn partial_bom(seen: usize) -> String {
            format!(
                "io InvalidData: Failed to read JSON: the body starts with {seen} of the 3 bytes of a UTF-8 byte-order mark and then something else, so it is neither a marked nor an unmarked JSON document. Check the file for a truncated or re-encoded header, or re-export it as UTF-8."
            )
        }

        for format in [Format::Auto, Format::Json] {
            // Detection stops at the form feed and answers "not an array", so
            // the body never reaches the array reader (which would have accepted
            // it had the byte been eaten). The line reader gets it instead,
            // trims the byte as Unicode whitespace, and refuses the array behind
            // it as a record.
            for body in [&b"\x0c[{\"a\":1}]"[..], &b"\x0b[{\"a\":1}]"[..]] {
                assert_eq!(
                    rejection(body, format),
                    "arrow: Json error: Expected JSON record to be an object, found Array [Object {\"a\": Number(1)}]",
                    "{format:?} on {:?}",
                    String::from_utf8_lossy(body)
                );
            }
            // A prefix that starts like a BOM and is not one is refused by
            // detection itself, which has already consumed it. What the *scan*
            // paths do with the same bodies is
            // `array_detection_propagates_an_error_that_consumed_bytes`.
            assert_eq!(
                rejection(b"\xEF{\"a\":1}", format),
                partial_bom(1),
                "{format:?}"
            );
            assert_eq!(
                rejection(b"\xEF\xBB{\"a\":1}", format),
                partial_bom(2),
                "{format:?}"
            );
        }

        // `Format::Array` skips detection, so the array reader's own prologue
        // guard is what sees each of these bytes.
        assert_eq!(
            rejection(b"\x0c[{\"a\":1}]", Format::Array),
            "io InvalidData: expected '[' but found '\u{c}'"
        );
        assert_eq!(
            rejection(b"\x0b[{\"a\":1}]", Format::Array),
            "io InvalidData: expected '[' but found '\u{b}'"
        );
        for body in [&b"\xEF{\"a\":1}"[..], &b"\xEF\xBB{\"a\":1}"[..]] {
            assert_eq!(
                rejection(body, Format::Array),
                "io InvalidData: expected '[' but found non-ASCII byte while looking for array start",
                "{:?}",
                String::from_utf8_lossy(body)
            );
        }

        // The four bytes JSON does admit still reach the array reader.
        let mut take = || true;
        let schema = infer_json_schema_for_format(b" \t\r\n[{\"a\":1}]", Format::Auto, &mut take)
            .expect("JSON whitespace before the array must remain acceptable");
        schema
            .field_with_name("a")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_object_pretty_printed() {
        let buf = b"{\n  \"airports\": 23\n}";
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect("should infer schema from pretty-printed object");
        assert_eq!(schema.fields().len(), 1);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_object_nested_pretty_printed() {
        let buf = br#"{
  "airports": [
    {"code": "ATL", "name": "Hartsfield"},
    {"code": "LAX", "name": "Los Angeles"}
  ],
  "count": 2
}"#;
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect("should infer schema from nested pretty-printed object");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
        schema
            .field_with_name("count")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_object_invalid_json() {
        let buf = b"not json";
        let mut take = || true;
        let err = infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect_err("should fail on invalid JSON");
        let DataFusionError::External(source) = &err else {
            panic!("expected serde_json's parse error, got {err:?}");
        };
        let parse_error = source
            .downcast_ref::<serde_json::Error>()
            .expect("the external error is serde_json's");
        // `n` opens `null`; the `o` after it is where the parse fails.
        assert_eq!(parse_error.to_string(), "expected ident at line 1 column 2");
    }

    #[test]
    fn test_infer_schema_auto_pretty_printed_object() {
        // Auto format should detect a multi-line JSON object without json_format: object
        let buf = b"{\n  \"airports\": 23\n}";
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Auto, &mut take)
            .expect("Auto should infer schema from pretty-printed object");
        assert_eq!(schema.fields().len(), 1);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_json_pretty_printed_object() {
        // Json format should also detect a multi-line JSON object
        let buf = b"{\n  \"name\": \"Alice\",\n  \"age\": 30\n}";
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Json, &mut take)
            .expect("Json should infer schema from pretty-printed object");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_auto_ndjson_still_works() {
        use arrow::datatypes::{DataType, Field};

        // NDJSON with multiple lines should still work via ValueIter fallback
        let buf = b"{\"a\": 1}\n{\"a\": 2, \"b\": 3}\n";
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Auto, &mut take)
            .expect("Auto should still handle NDJSON");
        // `b` appears only on the second line, so it is there only if every
        // line was read; both columns are integers in every row that has them.
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ])
        );
    }

    #[test]
    fn test_infer_schema_auto_object_with_nested_array() {
        // A single JSON object containing an array field (common API response shape)
        let buf = br#"{
  "airports": [
    {"code": "ATL", "name": "Hartsfield-Jackson Atlanta International"},
    {"code": "PEK", "name": "Beijing Capital International"}
  ]
}"#;
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Auto, &mut take)
            .expect("Auto should infer schema from object with nested array");
        assert_eq!(schema.fields().len(), 1);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_infer_schema_object_with_nested_array() {
        let buf = br#"{
  "airports": [
    {"code": "ATL", "name": "Hartsfield-Jackson Atlanta International"},
    {"code": "PEK", "name": "Beijing Capital International"}
  ]
}"#;
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect("Object should infer schema from object with nested array");
        assert_eq!(schema.fields().len(), 1);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
    }

    // ----------------------------------------------------------------
    // File-based schema inference tests using committed JSON fixtures
    // ----------------------------------------------------------------

    const TEST_DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/");

    fn load_fixture(name: &str) -> Vec<u8> {
        let path = format!("{TEST_DATA_DIR}{name}");
        std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read fixture {path}: {e}"))
    }

    // ---- JSONL fixtures ----

    #[test]
    fn test_fixture_jsonl_standard_schema() {
        let data = load_fixture("jsonl_standard.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Jsonl, &mut take)
            .expect("Jsonl schema from jsonl_standard.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("active")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_jsonl_standard_auto_schema() {
        let data = load_fixture("jsonl_standard.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from jsonl_standard.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("active")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_jsonl_crlf_schema() {
        let data = load_fixture("jsonl_crlf.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Jsonl, &mut take)
            .expect("Jsonl schema from jsonl_crlf.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("id")
            .expect("field should exist in schema");
        schema
            .field_with_name("status")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_jsonl_nested_schema() {
        let data = load_fixture("jsonl_nested.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Jsonl, &mut take)
            .expect("Jsonl schema from jsonl_nested.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("user")
            .expect("field should exist in schema");
        schema
            .field_with_name("scores")
            .expect("field should exist in schema");
    }

    // ---- Array fixtures ----

    #[test]
    fn test_fixture_array_standard_schema() {
        let data = load_fixture("array_standard.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_standard.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_standard_auto_schema() {
        let data = load_fixture("array_standard.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from array_standard.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_pretty_schema() {
        let data = load_fixture("array_pretty.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_pretty.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("city")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_single_schema() {
        let data = load_fixture("array_single.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_single.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("only")
            .expect("field should exist in schema");
        schema
            .field_with_name("value")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_nested_schema() {
        let data = load_fixture("array_nested.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_nested.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("tags")
            .expect("field should exist in schema");
        schema
            .field_with_name("meta")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_mixed_types_schema() {
        let data = load_fixture("array_mixed_types.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_mixed_types.json");
        assert_eq!(schema.fields().len(), 7);
        schema
            .field_with_name("str")
            .expect("field should exist in schema");
        schema
            .field_with_name("int")
            .expect("field should exist in schema");
        schema
            .field_with_name("float")
            .expect("field should exist in schema");
        schema
            .field_with_name("bool")
            .expect("field should exist in schema");
        schema
            .field_with_name("null_val")
            .expect("field should exist in schema");
        schema
            .field_with_name("nested")
            .expect("field should exist in schema");
        schema
            .field_with_name("arr")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_array_empty_schema() {
        let data = load_fixture("array_empty.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Array, &mut take)
            .expect("Array schema from array_empty.json");
        assert_eq!(schema.fields().len(), 0);
    }

    // ---- Object fixtures ----

    #[test]
    fn test_fixture_object_single_schema() {
        let data = load_fixture("object_single.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from object_single.json");
        assert_eq!(schema.fields().len(), 4);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("active")
            .expect("field should exist in schema");
        schema
            .field_with_name("scores")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_object_pretty_schema() {
        let data = load_fixture("object_pretty.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from object_pretty.json");
        assert_eq!(schema.fields().len(), 4);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("address")
            .expect("field should exist in schema");
        schema
            .field_with_name("tags")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_object_pretty_auto_schema() {
        // Key test: Auto should handle pretty-printed multi-line object
        let data = load_fixture("object_pretty.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from object_pretty.json");
        assert_eq!(schema.fields().len(), 4);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("address")
            .expect("field should exist in schema");
        schema
            .field_with_name("tags")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_object_nulls_schema() {
        let data = load_fixture("object_nulls.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from object_nulls.json");
        assert_eq!(schema.fields().len(), 4);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("middle_name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
        schema
            .field_with_name("nickname")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_object_empty_schema() {
        let data = load_fixture("object_empty.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from object_empty.json");
        assert_eq!(schema.fields().len(), 0);
    }

    // ---- Airports fixture (pretty-printed multi-line object with nested array) ----

    #[test]
    fn test_fixture_airports_object_schema() {
        let data = load_fixture("object_airports.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from object_airports.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
        schema
            .field_with_name("count")
            .expect("field should exist in schema");
        schema
            .field_with_name("source")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_airports_auto_schema() {
        let data = load_fixture("object_airports.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from object_airports.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
        schema
            .field_with_name("count")
            .expect("field should exist in schema");
        schema
            .field_with_name("source")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_airports_json_schema() {
        let data = load_fixture("object_airports.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Json, &mut take)
            .expect("Json schema from object_airports.json");
        assert_eq!(schema.fields().len(), 3);
        schema
            .field_with_name("airports")
            .expect("field should exist in schema");
        schema
            .field_with_name("count")
            .expect("field should exist in schema");
        schema
            .field_with_name("source")
            .expect("field should exist in schema");
    }

    // ---- BOM-prefixed fixtures ----

    #[test]
    fn test_fixture_bom_object_schema() {
        let data = load_fixture("bom_object.json");
        // Verify BOM is actually present
        assert_eq!(
            &data[..3],
            &[0xEF, 0xBB, 0xBF],
            "fixture should start with UTF-8 BOM"
        );
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Object, &mut take)
            .expect("Object schema from bom_object.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_bom_object_auto_schema() {
        let data = load_fixture("bom_object.json");
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from bom_object.json");
        assert_eq!(schema.fields().len(), 2);
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_bom_jsonl_auto_schema() {
        let data = load_fixture("bom_jsonl.json");
        assert_eq!(
            &data[..3],
            &[0xEF, 0xBB, 0xBF],
            "fixture should start with UTF-8 BOM"
        );
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from bom_jsonl.json");
        schema
            .field_with_name("name")
            .expect("field should exist in schema");
        schema
            .field_with_name("age")
            .expect("field should exist in schema");
    }

    #[test]
    fn test_fixture_bom_array_auto_schema() {
        let data = load_fixture("bom_array.json");
        assert_eq!(
            &data[..3],
            &[0xEF, 0xBB, 0xBF],
            "fixture should start with UTF-8 BOM"
        );
        let mut take = || true;
        let schema = infer_json_schema_for_format(&data, Format::Auto, &mut take)
            .expect("Auto schema from bom_array.json");
        schema
            .field_with_name("id")
            .expect("field should exist in schema");
        schema
            .field_with_name("val")
            .expect("field should exist in schema");
    }

    mod truncated_array {
        use super::*;
        use arrow::datatypes::{DataType, Field};
        use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
        use futures::{StreamExt, executor::block_on};

        fn schema() -> SchemaRef {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
        }

        fn decoder(schema: &SchemaRef) -> SpiceJsonDecoder {
            let inner = json::reader::ReaderBuilder::new(Arc::clone(schema))
                .build_decoder()
                .expect("build arrow json decoder");
            SpiceJsonDecoder::new(inner, true, None, Arc::clone(schema))
        }

        /// Rows read before the input ran out must not be handed back as if
        /// they were the whole file.
        #[test]
        fn flush_rejects_an_array_that_never_closed() {
            for body in [
                &br#"[{"a":1},{"a":2}"#[..],
                &br#"[{"a":1},"#[..],
                &br"[1,2,3"[..],
                &br"["[..],
            ] {
                let schema = schema();
                let mut dec = decoder(&schema);
                dec.decode(body).expect("decode should accept the prefix");

                let err = dec.flush().expect_err(
                    "an array with no closing bracket must be reported, not silently truncated",
                );
                assert!(
                    err.to_string().contains("closing ']'"),
                    "unexpected error for {:?}: {err}",
                    String::from_utf8_lossy(body)
                );
            }
        }

        #[test]
        fn flush_rejects_a_body_that_is_not_an_array() {
            for body in [&b""[..], &b"   \n\t "[..]] {
                let schema = schema();
                let mut dec = decoder(&schema);
                dec.decode(body).expect("decode should accept the prefix");

                let err = dec
                    .flush()
                    .expect_err("a body with no array at all must be reported");
                assert!(
                    err.to_string().contains("opening '['"),
                    "unexpected error for {:?}: {err}",
                    String::from_utf8_lossy(body)
                );
            }
        }

        #[test]
        fn flush_accepts_a_closed_array() {
            for body in [&br#"[{"a":1},{"a":2}]"#[..], &br"[]"[..], &b"  [ ]  "[..]] {
                let schema = schema();
                let mut dec = decoder(&schema);
                dec.decode(body).expect("decode should accept the body");

                // Flush is called repeatedly until it yields no more batches;
                // a closed array must stay clean across every call.
                for _ in 0..3 {
                    dec.flush().unwrap_or_else(|e| {
                        panic!(
                            "closed array {:?} must flush clean: {e}",
                            String::from_utf8_lossy(body)
                        )
                    });
                }
            }
        }

        /// The whole point of `finish` is that the deserializer reaches it, so
        /// drive the real `deserialize_stream` wiring rather than `flush` alone.
        #[test]
        fn the_stream_fails_instead_of_returning_a_short_table() {
            let schema = schema();
            let chunks: Vec<Result<bytes::Bytes>> = vec![
                Ok(bytes::Bytes::from_static(br#"[{"a":1},"#)),
                Ok(bytes::Bytes::from_static(br#"{"a":2}"#)),
            ];

            // Bounded: `deserialize_stream` re-polls the exhausted input and so
            // repeats any decoder error indefinitely. Consumers stop at the
            // first error; the test only needs to see one arrive.
            let batches: Vec<_> = block_on(
                deserialize_stream(
                    futures::stream::iter(chunks),
                    DecoderDeserializer::new(decoder(&schema)),
                )
                .take(8)
                .collect(),
            );

            let rows: usize = batches
                .iter()
                .filter_map(|b| b.as_ref().ok())
                .map(RecordBatch::num_rows)
                .sum();
            assert!(
                batches.iter().any(Result::is_err),
                "a truncated array yielded {rows} rows and no error"
            );
        }

        /// The buffered reader already rejects these bodies. The streaming
        /// adapter reading the same file must not disagree with it.
        #[test]
        fn both_readers_reject_the_same_truncated_body() {
            let body = br#"[{"a":1},{"a":2}"#;

            // Both readers classify the failure as the input ending early.
            let mut pulled = Vec::new();
            let pull = ArrayToNdjson::try_new(std::io::Cursor::new(body.to_vec()))
                .expect("array start is present")
                .read_to_end(&mut pulled)
                .expect_err("buffered reader accepted a truncated array");
            assert_eq!(pull.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(pull.to_string(), "EOF while peeking next byte");

            let schema = schema();
            let mut dec = decoder(&schema);
            dec.decode(body).expect("decode should accept the prefix");
            let push = dec.flush().expect_err(
                "streaming adapter accepted a truncated array the buffered reader rejects",
            );
            let ArrowError::IoError(message, source) = &push else {
                panic!("expected the adapter's end-of-input verdict, got {push:?}");
            };
            assert_eq!(
                source.kind(),
                std::io::ErrorKind::UnexpectedEof,
                "{message}"
            );
            assert_eq!(
                message,
                "Failed to read JSON array: the input ended before the closing ']' — the file is truncated. Rows read so far are only part of the file. Re-fetch the complete file, or set the dataset's 'format' to match its contents."
            );
        }

        /// A body carrying more than the array it opens with is the other way
        /// a file can be read short, and the two readers have to agree about
        /// it as well: whichever one a dataset happens to be scanned through,
        /// the same file has to reach the same verdict.
        #[test]
        fn both_readers_reject_the_same_trailing_content() {
            // Both readers raise the shared `trailing_content_error`, so the
            // verdict is word for word the same whichever one scans the file.
            const TRAILING_CONTENT: &str = "Failed to read JSON array: found '{' after the closing ']'. The body holds more than the single JSON array it was read as, so the rows returned are only its first array. Check the file for concatenated or trailing content, or set the dataset's 'format' to match its contents.";
            let body = br#"[{"a":1}]{"a":2}"#;

            let mut pulled = Vec::new();
            let pull = ArrayToNdjson::try_new(std::io::Cursor::new(body.to_vec()))
                .expect("array start is present")
                .read_to_end(&mut pulled)
                .expect_err("buffered reader read a second array as part of the first");
            assert_eq!(pull.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(pull.to_string(), TRAILING_CONTENT);
            assert!(
                pulled.is_empty(),
                "the first array's row was handed out before the verdict: {:?}",
                String::from_utf8_lossy(&pulled)
            );

            let schema = schema();
            let mut dec = decoder(&schema);
            let push = dec.decode(body).expect_err(
                "streaming adapter accepted trailing content the buffered reader rejects",
            );
            let ArrowError::IoError(message, source) = &push else {
                panic!("expected the adapter's trailing-content verdict, got {push:?}");
            };
            assert_eq!(source.kind(), std::io::ErrorKind::InvalidData, "{message}");
            assert_eq!(message, TRAILING_CONTENT);
        }
    }
}
