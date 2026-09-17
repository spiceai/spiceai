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

//! [`FirstRecordProbeSource`] — a [`FileSource`] decorator that reads at most the
//! first record of each file.
//!
//! It answers a partition-only `GROUP BY`/`DISTINCT` for formats that carry no
//! exact per-file row count (JSON, CSV, and any file without collected
//! statistics), where [`super::partition_only_scan`]'s statistics fast path
//! cannot apply. Those formats have no footer to read a row count from, so the
//! only way to learn whether a partition's file holds any rows is to decode a
//! record from it — but decoding *one* record is enough:
//!
//! - a partition value is constant across every row of its file, so one row
//!   carries the same partition tuple as the whole file; and
//! - a `DISTINCT`/`GROUP BY` over partition columns needs only the *set* of
//!   tuples, which the downstream aggregate recovers by collapsing the one row
//!   per file this source emits.
//!
//! Correctness rests on reusing the wrapped source's own opener, so "does this
//! file yield a row?" is decided by the identical decode path the full scan
//! would use:
//!
//! - an empty file (or a whitespace-only / header-only file) yields no record,
//!   so the source emits no row and the partition drops out of the result —
//!   exactly as a full scan would exclude it; and
//! - a non-empty file yields exactly one row carrying its partition values.
//!
//! Setting the scan's batch size to one and stopping after the first row bounds
//! the read to a single record's worth of bytes per file (a small object-store
//! prefix for compressed formats), instead of decoding every row of every file.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::table_schema::TableSchema;
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;

/// A [`FileSource`] decorator that emits at most one row per file — the first
/// record the wrapped source decodes. See the module documentation for the
/// correctness argument.
pub struct FirstRecordProbeSource {
    inner: Arc<dyn FileSource>,
}

impl FirstRecordProbeSource {
    /// Wrap `inner` so each file it scans yields at most its first record.
    #[must_use]
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }
}

impl FileSource for FirstRecordProbeSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // A batch size of one makes the first decoded batch carry a single row,
        // so stopping after it reads only that record's bytes rather than a full
        // 8192-row batch.
        let inner = self.inner.with_batch_size(1);
        let inner_opener = inner.create_file_opener(object_store, base_config, partition)?;
        Ok(Arc::new(FirstRecordOpener {
            inner: inner_opener,
        }))
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        // The probe forces a batch size of one when it opens a file; carry the
        // requested size on the wrapped source so the rest of its configuration
        // is preserved.
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
        })
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        // A filter pushed into the wrapped scan must still run: it decides which
        // rows count, so it decides whether a file yields a probe row at all.
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        // The projected schema (partition columns only) must be preserved, or
        // the rewritten plan's output schema would not match the original scan.
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)?;
        // Mark the probe in `EXPLAIN` output so a partition-only scan answered by
        // reading one record per file is distinguishable from a full file scan.
        if matches!(t, DisplayFormatType::Default | DisplayFormatType::Verbose) {
            write!(f, ", first_record_probe=true")?;
        }
        Ok(())
    }

    fn supports_repartitioning(&self) -> bool {
        // The probe collapses each file to one row, so splitting a file into
        // byte-range partitions would read the same file's first record more
        // than once. One file, one probe.
        false
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            inner: self.inner.with_schema_adapter_factory(factory)?,
        }))
    }
}

/// A [`FileOpener`] that truncates the wrapped opener's per-file stream to its
/// first row.
struct FirstRecordOpener {
    inner: Arc<dyn FileOpener>,
}

impl FileOpener for FirstRecordOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let inner = self.inner.open(partitioned_file)?;
        Ok(async move {
            let stream = inner.await?;
            let first_row = stream
                .filter_map(|batch| async move {
                    match batch {
                        // Skip empty batches: an empty file must contribute no
                        // row so its partition drops out of a `DISTINCT`.
                        Ok(batch) if batch.num_rows() == 0 => None,
                        // Keep one row; a partition value is constant across the
                        // file, so any row carries the same partition tuple.
                        Ok(batch) => Some(Ok(batch.slice(0, 1))),
                        Err(err) => Some(Err(err)),
                    }
                })
                .take(1)
                .boxed();
            Ok(first_row)
        }
        .boxed())
    }
}
