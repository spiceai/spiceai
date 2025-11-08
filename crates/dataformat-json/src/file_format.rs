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
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;

use crate::source::SpiceJsonSource;
use crate::{
    ArrayToNdjson, ArrayToNdjsonPush, ReadResult, extract_flattened_from_nested,
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
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExpr;
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
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};
use snafu::prelude::*;

#[derive(Debug)]
pub struct SpiceJsonOptions {
    pub compression: CompressionTypeVariant,
    pub schema_infer_max_rec: Option<usize>,
    pub format: Format,
    /// If set, flatten nested structs with the given separator
    pub flatten_json: Option<String>,
}

#[derive(Debug, Snafu)]
pub enum FormatParseError {
    #[snafu(display("Invalid JSON format '{s}'. Valid formats are: 'jsonl', 'ndjson', 'array'",))]
    InvalidFormat { s: String },
}

#[derive(Debug)]
pub enum Format {
    Jsonl,
    Array,
}

impl FromStr for Format {
    type Err = FormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "jsonl" | "ndjson" => Ok(Format::Jsonl),
            "array" => Ok(Format::Array),
            _ => InvalidFormatSnafu { s: s.to_string() }.fail(),
        }
    }
}

impl Default for SpiceJsonOptions {
    fn default() -> Self {
        Self {
            compression: CompressionTypeVariant::UNCOMPRESSED,
            schema_infer_max_rec: None,
            format: Format::Jsonl,
            flatten_json: None,
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
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);

                    let schema = if matches!(self.options.format, Format::Array) {
                        let mut adapter = ArrayToNdjson::try_new(reader)?;
                        let iter = ValueIter::new(&mut adapter, None);
                        infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                    } else {
                        let iter = ValueIter::new(&mut reader, None);
                        infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                    };

                    if let Some(separator) = &self.options.flatten_json {
                        unnest_struct_schema(&schema, separator)
                    } else {
                        schema
                    }
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let mut reader = BufReader::new(decoder);

                    let schema = if matches!(self.options.format, Format::Array) {
                        let mut adapter = ArrayToNdjson::try_new(reader)?;
                        let iter = ValueIter::new(&mut adapter, None);
                        infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                    } else {
                        let iter = ValueIter::new(&mut reader, None);
                        infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                    };

                    if let Some(separator) = &self.options.flatten_json {
                        unnest_struct_schema(&schema, separator)
                    } else {
                        schema
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
        mut conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = Arc::new(
            SpiceJsonSource::new()
                .with_array_to_ndjson(matches!(self.options.format, Format::Array))
                .with_unnest_struct(self.options.flatten_json.clone()),
        );

        if matches!(self.options.format, Format::Array) {
            tracing::debug!(
                "Creating non-repartitioned DataSource for JSON arrays for url: {}",
                conf.object_store_url
            );
            // Use NonRepartitionedFileScanConfig to prevent repartitioning for JSON array files,
            // as splitting would break parsing due to incomplete JSON fragments.
            // In order to still allow parallel read for individual files, we wrap them into separate files groups
            let individual_file_groups: Vec<_> = conf
                .file_groups
                .into_iter()
                .flat_map(|group| {
                    group
                        .into_inner()
                        .into_iter()
                        .map(|file_meta| FileGroup::new(vec![file_meta]))
                })
                .collect();

            conf.file_groups = individual_file_groups;

            let conf = FileScanConfigBuilder::from(conf)
                .with_file_compression_type(FileCompressionType::from(self.options.compression))
                .with_source(source)
                .build();

            return Ok(DataSourceExec::from_data_source(
                NonRepartitionedFileScanConfig::new(conf),
            ));
        }

        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(self.options.compression))
            .with_source(source)
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

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(
            SpiceJsonSource::new()
                .with_array_to_ndjson(matches!(self.options.format, Format::Array))
                .with_unnest_struct(self.options.flatten_json.clone()),
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
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
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
        projection: &[ProjectionExpr],
    ) -> Result<Option<Arc<dyn DataSource>>> {
        self.inner.try_swapping_with_projection(projection)
    }
}
