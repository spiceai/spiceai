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

use crate::file_format::SpiceJsonDecoder;
use crate::{ArrayToNdjson, nest_struct_schema};
use crate::{extract_flattened_from_nested, project_nested_schema};

use datafusion::error::{DataFusionError, Result};

use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{PartitionedFile, RangeCalculation, calculate_range};

use arrow::datatypes::SchemaRef;
use arrow::json::ReaderBuilder;
use datafusion::common::Statistics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use futures::{StreamExt, TryStreamExt};
use object_store::{GetOptions, GetResultPayload, ObjectStore};

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct SpiceJsonOpener {
    batch_size: usize,
    base_flattened_schema: SchemaRef,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    array_to_ndjson: bool,
    unnest_struct: Option<String>,
}

impl SpiceJsonOpener {
    /// Returns a  [`SpiceJsonOpener`]
    pub fn new(
        batch_size: usize,
        base_flattened_schema: SchemaRef,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
        array_to_ndjson: bool,
        unnest_struct: Option<String>,
    ) -> Self {
        Self {
            batch_size,
            base_flattened_schema,
            projected_schema,
            file_compression_type,
            object_store,
            array_to_ndjson,
            unnest_struct,
        }
    }
}

/// `SpiceJsonSource` holds the extra configuration that is necessary for [`SpiceJsonOpener`]
#[derive(Clone, Default)]
pub struct SpiceJsonSource {
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    array_to_ndjson: bool,
    unnest_struct: Option<String>,
}

impl SpiceJsonSource {
    /// Initialize a [`SpiceJsonSource`] with default values
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_array_to_ndjson(mut self, array_to_ndjson: bool) -> Self {
        self.array_to_ndjson = array_to_ndjson;
        self
    }

    #[must_use]
    pub fn with_unnest_struct(mut self, unnest_struct: Option<String>) -> Self {
        self.unnest_struct = unnest_struct;
        self
    }
}

impl FileSource for SpiceJsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(SpiceJsonOpener {
            batch_size: self.batch_size.or(base_config.batch_size).unwrap_or(8192),
            base_flattened_schema: Arc::clone(&base_config.file_schema),
            projected_schema: base_config.projected_file_schema(),
            file_compression_type: base_config.file_compression_type,
            object_store,
            array_to_ndjson: self.array_to_ndjson,
            unnest_struct: self.unnest_struct.clone(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        statistics.clone().ok_or_else(|| {
            DataFusionError::Internal("projected_statistics must be set to call".to_string())
        })
    }

    fn file_type(&self) -> &'static str {
        "json"
    }
}

impl FileOpener for SpiceJsonOpener {
    /// Open a partitioned NDJSON file. If `array_to_ndjson` is true, the file is converted to NDJSON from an array.
    ///
    /// If `unnest_struct` is set, the struct is unnested with the given separator.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// Else `file_meta.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    #[expect(clippy::too_many_lines)]
    fn open(&self, file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {
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
        let array_to_ndjson = self.array_to_ndjson;
        let unnest_struct_separator = self.unnest_struct.clone();

        tracing::trace!(
            "FileOpener::open called for file: file_path={}, file_size={}, range={:?}, thread_id={:?}",
            file_meta.location().to_string(),
            file_meta.object_meta.size,
            file_meta.range,
            std::thread::current().id()
        );

        Ok(Box::pin(async move {
            let calculated_range = calculate_range(&file_meta, &store, None).await?;

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

            let result = store.get_opts(file_meta.location(), options).await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes = if file_meta.range.is_none() {
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        let limit = result.range.end - result.range.start;
                        file_compression_type.convert_read(file.take(limit as u64))?
                    };

                    let buf_reader = BufReader::new(bytes);
                    let stream = if array_to_ndjson {
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
                    let s = s.map_err(DataFusionError::from);

                    let decoder = ReaderBuilder::new(Arc::clone(&projected_schema))
                        .with_batch_size(batch_size)
                        .build_decoder()?;
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    Ok(deserialize_stream(
                        input,
                        DecoderDeserializer::new(SpiceJsonDecoder::new(
                            decoder,
                            array_to_ndjson,
                            unnest_struct_separator,
                            projected_flattened_schema,
                        )),
                    )
                    .map(|b| b.map_err(DataFusionError::from))
                    .boxed())
                }
            }
        }))
    }
}
