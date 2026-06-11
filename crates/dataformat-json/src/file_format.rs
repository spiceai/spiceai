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

use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::sync::Arc;

use crate::source::SpiceJsonSource;
use crate::{
    ArrayToNdjson, ArrayToNdjsonPush, JsonPointerReader, ReadResult, SodaReader,
    extract_flattened_from_nested, is_soda_response, peek_first_non_ws_byte, unnest_struct_schema,
};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::json;
use arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};

use async_trait::async_trait;
use bytes::Buf;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
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

#[derive(Debug)]
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
#[derive(Debug, Default)]
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
}

#[async_trait]
impl FileFormat for SpiceJsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self
            .options
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let file_compression_type = FileCompressionType::from(self.options.compression);
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                }
                should_take
            };

            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(file, _) => {
                    let mut decoder = file_compression_type.convert_read(file)?;

                    // Buffer everything so we can probe for SODA when Auto.
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;

                    // Check SODA on raw data before json_pointer extraction.
                    let is_soda = self.options.format == Format::Soda
                        || (self.options.format == Format::Auto && is_soda_response(&raw_buf));

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
                        let soda = SodaReader::from_vec(&raw_buf, self.options.soda_metadata)?;
                        let schema = soda.schema().clone();
                        if let Some(separator) = &self.options.flatten_json {
                            unnest_struct_schema(&schema, separator)
                        } else {
                            schema
                        }
                    } else {
                        let buf = if let Some(path) = &self.options.json_pointer {
                            let extracted = JsonPointerReader::from_vec(&raw_buf, path)?;
                            let mut out = Vec::new();
                            std::io::BufReader::new(extracted).read_to_end(&mut out)?;
                            out
                        } else {
                            raw_buf
                        };

                        let schema = infer_json_schema_for_format(
                            &buf,
                            self.options.format,
                            &mut take_while,
                        )?;

                        if let Some(separator) = &self.options.flatten_json {
                            unnest_struct_schema(&schema, separator)
                        } else {
                            schema
                        }
                    }
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let mut decoder = file_compression_type.convert_read(data.reader())?;
                    let mut raw_buf = Vec::new();
                    decoder.read_to_end(&mut raw_buf)?;

                    let is_soda = self.options.format == Format::Soda
                        || (self.options.format == Format::Auto && is_soda_response(&raw_buf));

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
                        let soda = SodaReader::from_vec(&raw_buf, self.options.soda_metadata)?;
                        let schema = soda.schema().clone();
                        if let Some(separator) = &self.options.flatten_json {
                            unnest_struct_schema(&schema, separator)
                        } else {
                            schema
                        }
                    } else {
                        let buf = if let Some(path) = &self.options.json_pointer {
                            let extracted = JsonPointerReader::from_vec(&raw_buf, path)?;
                            let mut out = Vec::new();
                            std::io::BufReader::new(extracted).read_to_end(&mut out)?;
                            out
                        } else {
                            raw_buf
                        };

                        let schema = infer_json_schema_for_format(
                            &buf,
                            self.options.format,
                            &mut take_while,
                        )?;

                        if let Some(separator) = &self.options.flatten_json {
                            unnest_struct_schema(&schema, separator)
                        } else {
                            schema
                        }
                    }
                }
            };

            schemas.push(schema);
            if records_to_read == 0 {
                break;
            }
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
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
#[derive(Debug)]
struct NonRepartitionedFileScanConfig {
    inner: FileScanConfig,
}

impl NonRepartitionedFileScanConfig {
    fn new(inner: FileScanConfig) -> Self {
        Self { inner }
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

    fn as_any(&self) -> &dyn Any {
        &self.inner
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
    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.inner.partition_statistics(partition)
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
                    .as_any()
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
        Format::Auto | Format::Json => peek_first_non_ws_byte(&mut reader).is_ok_and(|b| b == b'['),
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
        let _ = ""
            .parse::<Format>()
            .expect_err("empty format should be rejected");
        let _ = "invalid"
            .parse::<Format>()
            .expect_err("invalid format should be rejected");
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
        infer_json_schema_for_format(buf, Format::Object, &mut take)
            .expect_err("should fail on invalid JSON");
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
        // NDJSON with multiple lines should still work via ValueIter fallback
        let buf = b"{\"a\": 1}\n{\"a\": 2, \"b\": 3}\n";
        let mut take = || true;
        let schema = infer_json_schema_for_format(buf, Format::Auto, &mut take)
            .expect("Auto should still handle NDJSON");
        schema
            .field_with_name("a")
            .expect("field should exist in schema");
        schema
            .field_with_name("b")
            .expect("field should exist in schema");
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
}
