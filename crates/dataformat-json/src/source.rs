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

//! Execution plan for reading JSON files

use std::any::Any;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use crate::file_format::{Format, SpiceJsonDecoder};
use crate::{
    ArrayToNdjson, JsonPointerReader, SodaReader, is_soda_response, nest_struct_schema,
    peek_first_non_ws_byte,
};
use crate::{extract_flattened_from_nested, project_nested_schema};

use datafusion::error::{DataFusionError, Result};

use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, RangeCalculation, TableSchema, calculate_range};

use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_physical_expr::projection::ProjectionExprs;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct SpiceJsonOpener {
    batch_size: usize,
    base_flattened_schema: SchemaRef,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    format: Format,
    json_pointer: Option<String>,
    unnest_struct: Option<String>,
    soda_metadata: bool,
}

/// `SpiceJsonSource` holds the extra configuration that is necessary for [`SpiceJsonOpener`]
#[derive(Clone)]
pub struct SpiceJsonSource {
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    format: Format,
    json_pointer: Option<String>,
    unnest_struct: Option<String>,
    soda_metadata: bool,
    table_schema: TableSchema,
    projection: SplitProjection,
}

impl SpiceJsonSource {
    /// Initialize a [`SpiceJsonSource`] with the given `table_schema`.
    #[must_use]
    pub fn new(table_schema: TableSchema) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            format: Format::Jsonl,
            json_pointer: None,
            unnest_struct: None,
            soda_metadata: false,
            table_schema,
            projection,
        }
    }

    #[must_use]
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }

    #[must_use]
    pub fn with_json_pointer(mut self, json_pointer: Option<String>) -> Self {
        self.json_pointer = json_pointer;
        self
    }

    #[must_use]
    pub fn with_unnest_struct(mut self, unnest_struct: Option<String>) -> Self {
        self.unnest_struct = unnest_struct;
        self
    }

    #[must_use]
    pub fn with_soda_metadata(mut self, enabled: bool) -> Self {
        self.soda_metadata = enabled;
        self
    }

    #[must_use]
    pub fn with_table_schema(mut self, table_schema: TableSchema) -> Self {
        self.projection = SplitProjection::unprojected(&table_schema);
        self.table_schema = table_schema;
        self
    }
}

impl FileSource for SpiceJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let projected_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let opener: Arc<dyn FileOpener> = Arc::new(SpiceJsonOpener {
            batch_size: self.batch_size.or(base_config.batch_size).unwrap_or(8192),
            base_flattened_schema: Arc::clone(file_schema),
            projected_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
            format: self.format,
            json_pointer: self.json_pointer.clone(),
            unnest_struct: self.unnest_struct.clone(),
            soda_metadata: self.soda_metadata,
        });

        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &'static str {
        "json"
    }
}

impl FileOpener for SpiceJsonOpener {
    /// Open a partitioned JSON file.
    ///
    /// Supports multiple JSON formats (JSONL, Array, Auto-detect) and optional
    /// `json_pointer` extraction from JSON objects.
    ///
    /// If `unnest_struct` is set, the struct is unnested with the given separator.
    ///
    /// If `partitioned_file.range` is `None`, the entire file is opened.
    /// Else `partitioned_file.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    #[expect(clippy::too_many_lines)]
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let base_flattened_schema = Arc::clone(&self.base_flattened_schema);
        let original_nested_schema = self
            .unnest_struct
            .as_ref()
            .map(|separator| nest_struct_schema(&base_flattened_schema, separator));
        let projected_flattened_schema = Arc::clone(&self.projected_schema);
        let projected_nested_schema = original_nested_schema.map(|schema| {
            if let Some(separator) = &self.unnest_struct {
                Arc::new(project_nested_schema(
                    &projected_flattened_schema,
                    &schema,
                    separator,
                ))
            } else {
                Arc::clone(&projected_flattened_schema)
            }
        });
        let projected_schema =
            projected_nested_schema.unwrap_or(Arc::clone(&projected_flattened_schema));
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type;
        let format = self.format;
        let json_pointer = self.json_pointer.clone();
        let unnest_struct_separator = self.unnest_struct.clone();
        let soda_metadata = self.soda_metadata;
        let file_range = partitioned_file.range.clone();

        tracing::trace!(
            "FileOpener::open called for file: file_path={}, file_size={}, range={:?}, thread_id={:?}",
            partitioned_file.object_meta.location.as_ref(),
            partitioned_file.object_meta.size,
            partitioned_file.range,
            std::thread::current().id()
        );

        Ok(Box::pin(async move {
            let calculated_range = calculate_range(&partitioned_file, &store, None).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed());
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes = if file_range.is_none() {
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        let limit = result.range.end - result.range.start;
                        file_compression_type.convert_read(file.take(limit as u64))?
                    };

                    // Buffer the content for SODA auto-detection when Auto
                    let mut raw_buf = Vec::new();
                    let mut bytes = bytes;
                    bytes
                        .read_to_end(&mut raw_buf)
                        .map_err(DataFusionError::from)?;

                    // Check SODA on raw data before json_pointer extraction.
                    let is_soda = format == Format::Soda
                        || (format == Format::Auto && is_soda_response(&raw_buf));

                    if is_soda && json_pointer.is_some() {
                        return Err(DataFusionError::Configuration(
                            "'json_pointer' cannot be used with SODA format data. \
                             SODA responses contain their own schema in 'meta.view.columns' \
                             and data is extracted automatically. \
                             Remove 'json_pointer' or set 'file_format: json' to treat the file as regular JSON."
                                .to_string(),
                        ));
                    }

                    // SODA format: convert to NDJSON via SodaReader
                    if is_soda {
                        let soda = SodaReader::from_vec(&raw_buf, soda_metadata)
                            .map_err(DataFusionError::from)?;
                        let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                            .with_batch_size(batch_size)
                            .build(BufReader::new(soda))?;
                        let stream = futures::stream::iter(reader).boxed();
                        // Apply flatten if configured (don't early-return to skip it)
                        if let Some(separator) = &unnest_struct_separator {
                            let separator = separator.clone();
                            return Ok(stream
                                .map(move |batch| {
                                    batch
                                        .map(|batch| {
                                            extract_flattened_from_nested(
                                                &batch,
                                                &projected_flattened_schema,
                                                &separator,
                                            )
                                            .unwrap_or(batch)
                                        })
                                        .map_err(DataFusionError::from)
                                })
                                .boxed());
                        }
                        return Ok(stream.map(|b| b.map_err(DataFusionError::from)).boxed());
                    }

                    // Not SODA — apply json_pointer if configured
                    let buf = if let Some(path) = &json_pointer {
                        let ptr = JsonPointerReader::from_vec(&raw_buf, path)
                            .map_err(DataFusionError::from)?;
                        let mut out = Vec::new();
                        BufReader::new(ptr)
                            .read_to_end(&mut out)
                            .map_err(DataFusionError::from)?;
                        out
                    } else {
                        raw_buf
                    };

                    let mut buf_reader = BufReader::new(std::io::Cursor::new(buf));

                    // Determine if the content is a JSON array
                    let is_array = match format {
                        Format::Array => true,
                        Format::Jsonl | Format::Object => false,
                        Format::Soda => unreachable!("handled above"),
                        Format::Auto | Format::Json => {
                            peek_first_non_ws_byte(&mut buf_reader).is_ok_and(|b| b == b'[')
                        }
                    };

                    let stream = if is_array {
                        let adapter = ArrayToNdjson::try_new(buf_reader)?;
                        let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                            .with_batch_size(batch_size)
                            .build(adapter)?;
                        futures::stream::iter(reader).boxed()
                    } else {
                        let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                            .with_batch_size(batch_size)
                            .build(buf_reader)?;
                        futures::stream::iter(reader).boxed()
                    };

                    if let Some(separator) = &unnest_struct_separator {
                        let separator = separator.clone();
                        Ok(stream
                            .map(move |batch| {
                                batch
                                    .map(|batch| {
                                        extract_flattened_from_nested(
                                            &batch,
                                            &projected_flattened_schema,
                                            &separator,
                                        )
                                        .unwrap_or(batch)
                                    })
                                    .map_err(DataFusionError::from)
                            })
                            .boxed())
                    } else {
                        Ok(stream.map(|b| b.map_err(DataFusionError::from)).boxed())
                    }
                }
                GetResultPayload::Stream(s) => {
                    // SODA and json_pointer require buffering the full response
                    // because they must parse the entire JSON document.
                    // Auto needs buffering to peek the first byte then decode.
                    // Object needs buffering because the single JSON value may span multiple chunks.
                    if json_pointer.is_some()
                        || matches!(
                            format,
                            Format::Auto | Format::Json | Format::Soda | Format::Object
                        )
                    {
                        let s = s.map_err(DataFusionError::from);
                        let decompressed = file_compression_type.convert_stream(s.boxed())?;
                        let chunks: Vec<bytes::Bytes> = decompressed.try_collect().await?;

                        let total_len: usize = chunks.iter().map(bytes::Bytes::len).sum();
                        let mut all_bytes = Vec::with_capacity(total_len);
                        for chunk in &chunks {
                            all_bytes.extend_from_slice(chunk);
                        }

                        // Check SODA on raw data before json_pointer extraction
                        let is_soda = format == Format::Soda
                            || (format == Format::Auto && is_soda_response(&all_bytes));

                        if is_soda && json_pointer.is_some() {
                            return Err(DataFusionError::Configuration(
                                "'json_pointer' cannot be used with SODA format data. \
                                 SODA responses contain their own schema in 'meta.view.columns' \
                                 and data is extracted automatically. \
                                 Remove 'json_pointer' or set 'file_format: json' to treat the file as regular JSON."
                                    .to_string(),
                            ));
                        }

                        if is_soda {
                            let soda = SodaReader::from_vec(&all_bytes, soda_metadata)
                                .map_err(DataFusionError::from)?;
                            let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                                .with_batch_size(batch_size)
                                .build(BufReader::new(soda))?;
                            let stream = futures::stream::iter(reader).boxed();
                            // Apply flatten if configured
                            if let Some(separator) = &unnest_struct_separator {
                                let separator = separator.clone();
                                return Ok(stream
                                    .map(move |batch| {
                                        batch
                                            .map(|batch| {
                                                extract_flattened_from_nested(
                                                    &batch,
                                                    &projected_flattened_schema,
                                                    &separator,
                                                )
                                                .unwrap_or(batch)
                                            })
                                            .map_err(DataFusionError::from)
                                    })
                                    .boxed());
                            }
                            return Ok(stream.map(|b| b.map_err(DataFusionError::from)).boxed());
                        }

                        // Not SODA — apply json_pointer if configured
                        let processed_bytes = if let Some(path) = &json_pointer {
                            let ptr = JsonPointerReader::from_vec(&all_bytes, path)
                                .map_err(DataFusionError::from)?;
                            let mut out = Vec::new();
                            BufReader::new(ptr)
                                .read_to_end(&mut out)
                                .map_err(DataFusionError::from)?;
                            out
                        } else {
                            all_bytes
                        };

                        let mut buf_reader = BufReader::new(std::io::Cursor::new(processed_bytes));

                        let is_array = match format {
                            Format::Array => true,
                            Format::Jsonl | Format::Object => false,
                            Format::Soda => unreachable!("handled above"),
                            Format::Auto | Format::Json => {
                                peek_first_non_ws_byte(&mut buf_reader).is_ok_and(|b| b == b'[')
                            }
                        };

                        let stream = if is_array {
                            let adapter = ArrayToNdjson::try_new(buf_reader)?;
                            let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                                .with_batch_size(batch_size)
                                .build(adapter)?;
                            futures::stream::iter(reader).boxed()
                        } else {
                            let reader = ReaderBuilder::new(Arc::clone(&projected_schema))
                                .with_batch_size(batch_size)
                                .build(buf_reader)?;
                            futures::stream::iter(reader).boxed()
                        };

                        if let Some(separator) = &unnest_struct_separator {
                            let separator = separator.clone();
                            Ok(stream
                                .map(move |batch| {
                                    batch
                                        .map(|batch| {
                                            extract_flattened_from_nested(
                                                &batch,
                                                &projected_flattened_schema,
                                                &separator,
                                            )
                                            .unwrap_or(batch)
                                        })
                                        .map_err(DataFusionError::from)
                                })
                                .boxed())
                        } else {
                            Ok(stream.map(|b| b.map_err(DataFusionError::from)).boxed())
                        }
                    } else {
                        // JSONL, Array, and Object can stream without buffering
                        let s = s.map_err(DataFusionError::from);

                        let decoder = ReaderBuilder::new(Arc::clone(&projected_schema))
                            .with_batch_size(batch_size)
                            .build_decoder()?;
                        let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                        Ok(deserialize_stream(
                            input,
                            DecoderDeserializer::new(SpiceJsonDecoder::new(
                                decoder,
                                matches!(format, Format::Array),
                                unnest_struct_separator,
                                projected_flattened_schema,
                            )),
                        )
                        .map(|b| b.map_err(DataFusionError::from))
                        .boxed())
                    }
                }
            }
        }))
    }
}
