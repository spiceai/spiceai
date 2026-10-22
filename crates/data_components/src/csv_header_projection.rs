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

//! Projects a headered CSV onto the registered schema by column name.
//!
//! `DataFusion`'s [`CsvFormat`] binds a provided schema by position and only uses
//! the header to skip the first row. A file whose columns are reordered then
//! returns another column's values under the registered names. This wrapper
//! reads each file's header and maps those names onto the registered schema so
//! a reorder keeps the values with their header, and a renamed or dropped
//! column is `NULL` instead of another field's values.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow::compute::cast;
use arrow::csv::reader::Format as ArrowCsvFormat;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::CsvOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileMeta};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, apply_expression_roots};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::metadata::MetadataColumn;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_csv::source::{CsvOpener, CsvSource};
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

/// Initial object prefix read when parsing the header. The probe grows from
/// byte 0 until a quote-aware header record is available, or the whole object
/// has been read.
const HEADER_PROBE_BYTES: u64 = 64 * 1024;

/// [`CsvFormat`] that maps each file's header names onto the registered schema.
#[derive(Debug)]
pub struct HeaderProjectedCsvFormat {
    inner: CsvFormat,
}

impl HeaderProjectedCsvFormat {
    /// Wraps `format` so a headered CSV is read by name. Other formats, and CSV
    /// with `has_header` false, are returned unchanged.
    #[must_use]
    pub fn wrap(format: Arc<dyn FileFormat>) -> Arc<dyn FileFormat> {
        let Some(csv) = format.as_ref().downcast_ref::<CsvFormat>() else {
            return format;
        };
        if !csv.has_header().unwrap_or(true) {
            return format;
        }
        Arc::new(Self {
            inner: CsvFormat::default().with_options(csv.options().clone()),
        })
    }

    #[must_use]
    pub fn options(&self) -> &CsvOptions {
        self.inner.options()
    }
}

#[async_trait]
impl FileFormat for HeaderProjectedCsvFormat {
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<datafusion::common::Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        self.inner
            .infer_ordering(state, store, table_schema, object)
            .await
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<FileMeta> {
        self.inner
            .infer_stats_and_ordering(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let has_header = self
            .inner
            .has_header()
            .unwrap_or_else(|| state.config_options().catalog.has_header);
        let newlines_in_values = self
            .inner
            .options()
            .newlines_in_values
            .unwrap_or_else(|| state.config_options().catalog.newlines_in_values);
        let mut options = self.inner.options().clone();
        options.has_header = Some(has_header);
        options.newlines_in_values = Some(newlines_in_values);

        let table_schema = conf.file_source().table_schema().clone();
        let mut source = HeaderProjectedCsvSource::new(table_schema, options);
        if let Some(existing) = conf
            .file_source()
            .as_ref()
            .downcast_ref::<HeaderProjectedCsvSource>()
        {
            source.projection = existing.projection.clone();
            source.batch_size = existing.batch_size;
        }
        let config = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(self.inner.options().compression.into())
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<datafusion::physical_expr_common::sort_expr::LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let mut options = self.inner.options().clone();
        if options.has_header.is_none() {
            options.has_header = Some(true);
        }
        Arc::new(HeaderProjectedCsvSource::new(table_schema, options))
    }
}

/// [`FileSource`] that remaps each file's header onto the registered file schema.
#[derive(Clone)]
struct HeaderProjectedCsvSource {
    options: CsvOptions,
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl HeaderProjectedCsvSource {
    fn new(table_schema: TableSchema, options: CsvOptions) -> Self {
        Self {
            options,
            batch_size: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }

    fn newlines_in_values(&self) -> bool {
        self.options.newlines_in_values.unwrap_or(false)
    }
}

impl FileSource for HeaderProjectedCsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let opener = HeaderMappedOpener {
            options: self.options.clone(),
            batch_size: self.batch_size.unwrap_or(8192),
            registered_file_schema: Arc::clone(self.table_schema.file_schema()),
            file_indices: self.projection.file_indices.clone(),
            file_compression_type: base_config.file_compression_type,
            object_store,
        };
        ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::new(opener),
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_metadata_cols(
        &self,
        metadata_cols: Vec<MetadataColumn>,
    ) -> Option<Arc<dyn FileSource>> {
        let mut source = self.clone();
        source.table_schema = source.table_schema.with_metadata_cols(metadata_cols);
        source.projection = SplitProjection::unprojected(&source.table_schema);
        Some(Arc::new(source))
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &'static str {
        "csv"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    ", has_header={}, header_projection=true",
                    self.has_header()
                )
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn supports_repartitioning(&self) -> bool {
        !self.newlines_in_values()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        source.projection =
            SplitProjection::new_with_table_schema(&self.table_schema, &new_projection);
        Ok(Some(Arc::new(source)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }
}

/// Reads one CSV with the file's header schema, then projects onto the
/// registered file columns `file_indices` selects, by name.
struct HeaderMappedOpener {
    options: CsvOptions,
    batch_size: usize,
    registered_file_schema: SchemaRef,
    file_indices: Vec<usize>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for HeaderMappedOpener {
    fn open(
        &self,
        partitioned_file: datafusion_datasource::PartitionedFile,
    ) -> Result<FileOpenFuture> {
        let options = self.options.clone();
        let batch_size = self.batch_size;
        let registered = Arc::clone(&self.registered_file_schema);
        let file_indices = self.file_indices.clone();
        let compression = self.file_compression_type;
        let store = Arc::clone(&self.object_store);

        Ok(Box::pin(async move {
            let header =
                read_csv_header(&store, &partitioned_file.object_meta, &options, compression)
                    .await?;
            let physical = Arc::new(physical_schema(&header, &registered, &file_indices));
            let target = projected_registered_schema(&registered, &file_indices)?;

            let physical_source = CsvSource::new(physical)
                .with_csv_options(options)
                .with_batch_size(batch_size);
            let Some(physical_source) = physical_source.as_ref().downcast_ref::<CsvSource>() else {
                return Err(DataFusionError::Internal(
                    "header-projected CSV reader did not build a CSV source".to_string(),
                ));
            };
            let opener = CsvOpener::new(Arc::new(physical_source.clone()), compression, store);
            let stream = opener.open(partitioned_file)?.await?;
            Ok(stream
                .map(move |batch| batch.and_then(|b| project_record_batch(&b, &target)))
                .boxed())
        }))
    }
}

fn physical_schema(header: &[String], registered: &Schema, file_indices: &[usize]) -> Schema {
    Schema::new(
        header
            .iter()
            .map(|name| {
                let projected = file_indices
                    .iter()
                    .any(|&i| registered.field(i).name() == name);
                if projected {
                    registered
                        .field_with_name(name)
                        .ok()
                        .cloned()
                        .unwrap_or_else(|| Field::new(name, DataType::Utf8, true))
                } else {
                    // Unselected columns stay Utf8 so an invalid value in a
                    // column the query does not read cannot fail the scan.
                    Field::new(name, DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    )
}

fn projected_registered_schema(registered: &Schema, file_indices: &[usize]) -> Result<SchemaRef> {
    Ok(Arc::new(registered.project(file_indices)?))
}

fn project_record_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let columns: Vec<ArrayRef> = target
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => {
                let column = Arc::clone(batch.column(idx));
                if column.data_type() == field.data_type() {
                    Ok(column)
                } else {
                    cast(column.as_ref(), field.data_type()).map_err(DataFusionError::from)
                }
            }
            Err(_) => Ok(new_null_array(field.data_type(), rows)),
        })
        .collect::<Result<Vec<_>>>()?;
    // `count(*)` projects no file columns; the empty schema still needs the
    // file's row count so the aggregate is not zero.
    RecordBatch::try_new_with_options(
        Arc::clone(target),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(DataFusionError::from)
}

async fn read_csv_header(
    store: &Arc<dyn ObjectStore>,
    meta: &ObjectMeta,
    options: &CsvOptions,
    compression: FileCompressionType,
) -> Result<Vec<String>> {
    // A 64 KiB prefix is enough for an ordinary header, but it is not a
    // complete object: a UTF-8 character or a bzip2 block can straddle that
    // cut. Grow from byte 0 until a quote-aware header record is available,
    // and only then treat a parse error as a malformed file.
    let mut end = meta.size.min(HEADER_PROBE_BYTES);
    loop {
        let whole_object = end >= meta.size;
        let prefix = store
            .get_range(&meta.location, 0..end)
            .await
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
        match header_from_prefix(prefix, options, compression, whole_object).await {
            Ok(Some(names)) => return Ok(names),
            Ok(None) if whole_object => return Ok(Vec::new()),
            Err(e) if whole_object => return Err(e),
            Ok(None) | Err(_) => {
                let next = end
                    .saturating_mul(2)
                    .max(end.saturating_add(1))
                    .min(meta.size);
                if next == end {
                    return Ok(Vec::new());
                }
                end = next;
            }
        }
    }
}

/// A prefix that decoded far enough to parse, that needs more bytes, or that
/// failed in a way only a complete object can settle.
async fn header_from_prefix(
    prefix: bytes::Bytes,
    options: &CsvOptions,
    compression: FileCompressionType,
    whole_object: bool,
) -> Result<Option<Vec<String>>> {
    let Some(decoded) = decode_prefix(prefix, compression, options).await? else {
        return Ok(None);
    };
    if !whole_object
        && !header_record_complete(
            &decoded,
            options.delimiter,
            options.quote,
            options.escape,
            options.comment,
        )
    {
        return Ok(None);
    }
    parse_header_names(&decoded, options).map(Some)
}

/// Decompresses `prefix`. `None` means the prefix is not a complete compressed
/// stream (grow and retry). An uncompressed prefix is always `Some`.
async fn decode_prefix(
    prefix: bytes::Bytes,
    compression: FileCompressionType,
    options: &CsvOptions,
) -> Result<Option<bytes::Bytes>> {
    if !compression.is_compressed() {
        return Ok(Some(prefix));
    }
    let stream = futures::stream::once(async move { Ok(prefix) }).boxed();
    let Ok(mut decoded) = compression.convert_stream(stream) else {
        return Ok(None);
    };
    let mut acc = Vec::new();
    loop {
        if let Some(Ok(chunk)) = decoded.next().await {
            acc.extend_from_slice(&chunk);
            if header_record_complete(
                &acc,
                options.delimiter,
                options.quote,
                options.escape,
                options.comment,
            ) {
                break;
            }
        } else if acc.is_empty() {
            return Ok(None);
        } else {
            break;
        }
    }
    Ok(Some(bytes::Bytes::from(acc)))
}

/// Whether `bytes` contains a quote-aware CSV header record terminator, so a
/// header parse will not see a field split across the prefix cut.
///
/// Leading empty records (and comment records, when `comment` is set) are
/// skipped first. A prefix that is only blank lines would otherwise look
/// complete and a later header that straddles the cut would be parsed as a
/// truncated name list.
fn header_record_complete(
    bytes: &[u8],
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    comment: Option<u8>,
) -> bool {
    let mut i = skip_leading_empty_and_comment_records(bytes, comment);
    if i >= bytes.len() {
        return false;
    }
    let mut in_quotes = false;
    let mut at_field_start = true;
    while i < bytes.len() {
        let b = bytes[i];
        if in_quotes {
            if let Some(esc) = escape
                && b == esc
                && i + 1 < bytes.len()
            {
                i += 2;
                continue;
            }
            if b == quote {
                if i + 1 < bytes.len() && bytes[i + 1] == quote {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
        } else if b == quote && at_field_start {
            // A quote only opens a field at the start of that field. A quote
            // mid-field is literal data, not a multiline-field delimiter.
            in_quotes = true;
            at_field_start = false;
        } else if b == delimiter {
            at_field_start = true;
        } else if b == b'\n' || b == b'\r' {
            return true;
        } else {
            at_field_start = false;
        }
        i += 1;
    }
    false
}

fn skip_leading_empty_and_comment_records(bytes: &[u8], comment: Option<u8>) -> usize {
    let mut i = 0;
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        i = 3;
    }
    loop {
        while i < bytes.len() && (bytes[i] == b'\n' || bytes[i] == b'\r') {
            i += 1;
        }
        match comment {
            Some(marker) if i < bytes.len() && bytes[i] == marker => {
                while i < bytes.len() && bytes[i] != b'\n' && bytes[i] != b'\r' {
                    i += 1;
                }
            }
            _ => return i,
        }
    }
}

fn parse_header_names(bytes: &[u8], options: &CsvOptions) -> Result<Vec<String>> {
    let mut format = ArrowCsvFormat::default()
        .with_header(true)
        .with_delimiter(options.delimiter)
        .with_quote(options.quote)
        .with_truncated_rows(true);
    if let Some(escape) = options.escape {
        format = format.with_escape(escape);
    }
    if let Some(comment) = options.comment {
        format = format.with_comment(comment);
    }
    let (schema, _) = format
        .infer_schema(std::io::Cursor::new(bytes), Some(0))
        .map_err(DataFusionError::from)?;
    Ok(schema
        .fields()
        .iter()
        .map(|field| field.name().trim_start_matches('\u{feff}').to_string())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;
    use std::fmt::Write;
    use std::path::Path;

    fn header_probe_len() -> usize {
        usize::try_from(HEADER_PROBE_BYTES).expect("header probe fits usize")
    }

    fn write_csv(path: &Path, header: &str, rows: &[&str]) {
        let mut text = String::from(header);
        text.push('\n');
        for row in rows {
            text.push_str(row);
            text.push('\n');
        }
        std::fs::write(path, text).expect("write csv");
    }

    fn registered_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, true),
            Field::new("user_id", DataType::Int64, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("amount_cents", DataType::Int64, true),
            Field::new("ts", DataType::Utf8, true),
        ]))
    }

    fn listing_table(
        dir: &Path,
        format: Arc<dyn FileFormat>,
    ) -> Arc<dyn datafusion::datasource::TableProvider> {
        listing_table_with_extension(dir, format, ".csv")
    }

    fn listing_table_with_extension(
        dir: &Path,
        format: Arc<dyn FileFormat>,
        extension: &str,
    ) -> Arc<dyn datafusion::datasource::TableProvider> {
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let config = ListingTableConfig::new(url)
            .with_listing_options(ListingOptions::new(format).with_file_extension(extension))
            .with_schema(registered_schema());
        Arc::new(ListingTable::try_new(config).expect("listing table"))
    }

    async fn query_sums(
        table: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Result<(Option<i64>, Option<i64>)> {
        let ctx = SessionContext::new();
        ctx.register_table("base", table)?;
        let batches = ctx
            .sql("SELECT sum(amount_cents), sum(user_id) FROM base")
            .await?
            .collect()
            .await?;
        let batch = &batches[0];
        let amounts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount_cents");
        let users = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("user_id");
        Ok((
            (!amounts.is_null(0)).then(|| amounts.value(0)),
            (!users.is_null(0)).then(|| users.value(0)),
        ))
    }

    fn header_format() -> Arc<dyn FileFormat> {
        HeaderProjectedCsvFormat::wrap(Arc::new(CsvFormat::default().with_has_header(true)))
    }

    fn header_tsv_format() -> Arc<dyn FileFormat> {
        HeaderProjectedCsvFormat::wrap(Arc::new(
            CsvFormat::default()
                .with_has_header(true)
                .with_delimiter(b'\t'),
        ))
    }

    fn write_tsv(path: &Path, header: &str, rows: &[&str]) {
        write_csv(path, header, rows);
    }

    #[tokio::test]
    async fn reordered_csv_columns_follow_header_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(
            &dir.path().join("base.csv"),
            "event_id,user_id,region,amount_cents,ts",
            &["1,10,us,100,t0", "2,20,eu,200,t1"],
        );
        let table = listing_table(dir.path(), header_format());
        let before = query_sums(Arc::clone(&table))
            .await
            .expect("query before reorder");
        assert_eq!(before, (Some(300), Some(30)));

        write_csv(
            &dir.path().join("base.csv"),
            "event_id,amount_cents,region,user_id,ts",
            &["1,100,us,10,t0", "2,200,eu,20,t1"],
        );
        let after = query_sums(table).await.expect("query after reorder");
        assert_eq!(
            after,
            (Some(300), Some(30)),
            "values must follow the header names, not column position"
        );
    }

    #[tokio::test]
    async fn unprojected_invalid_int_does_not_fail_selected_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(
            &dir.path().join("base.csv"),
            "event_id,user_id,region,amount_cents,ts",
            &["1,10,us,not-an-int,t0", "2,20,eu,also-not-an-int,t1"],
        );
        let table = listing_table(dir.path(), header_format());
        let ctx = SessionContext::new();
        ctx.register_table("base", table).expect("register");
        let batches = ctx
            .sql("SELECT sum(event_id) FROM base")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("an unselected invalid int must not fail the query");
        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("event_id");
        assert_eq!((!column.is_null(0)).then(|| column.value(0)), Some(3));
    }

    #[tokio::test]
    async fn header_probe_grows_when_a_literal_quote_precedes_a_multiline_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        // `dummy"tag` is an unquoted field with a literal quote. The next field
        // is a quoted multiline value whose closer sits past the 64 KiB cut.
        let mut text = String::from("event_id,user_id,region,dummy\"tag,\"note\n");
        text.push_str(&"x".repeat(65_536));
        text.push_str("\",amount_cents,ts\n1,10,us,x,note,100,t0\n2,20,eu,x,note,200,t1\n");
        let prefix = text.as_bytes().get(..header_probe_len()).expect("prefix");
        assert!(
            !prefix
                .windows(b"amount_cents".len())
                .any(|w| w == b"amount_cents"),
            "the 64 KiB probe must cut before amount_cents"
        );
        assert!(
            !header_record_complete(prefix, b',', b'"', None, None),
            "a quote mid-field must not end the header at the next newline"
        );
        std::fs::write(dir.path().join("base.csv"), text).expect("write csv");
        let sums = query_sums(listing_table(dir.path(), header_format()))
            .await
            .expect("query after growing past a literal-quote multiline header");
        assert_eq!(sums, (Some(300), Some(30)));
    }

    #[tokio::test]
    async fn reordered_tsv_columns_follow_header_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_tsv(
            &dir.path().join("base.tsv"),
            "event_id\tuser_id\tregion\tamount_cents\tts",
            &["1\t10\tus\t100\tt0", "2\t20\teu\t200\tt1"],
        );
        let table = listing_table_with_extension(dir.path(), header_tsv_format(), ".tsv");
        assert_eq!(
            query_sums(Arc::clone(&table))
                .await
                .expect("query before tsv reorder"),
            (Some(300), Some(30))
        );

        write_tsv(
            &dir.path().join("base.tsv"),
            "event_id\tamount_cents\tregion\tuser_id\tts",
            &["1\t100\tus\t10\tt0", "2\t200\teu\t20\tt1"],
        );
        assert_eq!(
            query_sums(table).await.expect("query after tsv reorder"),
            (Some(300), Some(30)),
            "TSV is the same headered delimiter format as CSV"
        );
    }

    #[tokio::test]
    async fn extra_leading_column_does_not_shift_registered_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(
            &dir.path().join("base.csv"),
            "note,event_id,user_id,region,amount_cents,ts",
            &["x,1,10,us,100,t0", "y,2,20,eu,200,t1"],
        );
        let table = listing_table(dir.path(), header_format());
        let sums = query_sums(table)
            .await
            .expect("query with an extra leading column");
        assert_eq!(sums, (Some(300), Some(30)));
    }

    #[tokio::test]
    async fn renamed_header_returns_null_for_the_missing_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(
            &dir.path().join("base.csv"),
            "event_id,user_id,region,amt,ts",
            &["1,10,us,100,t0", "2,20,eu,200,t1"],
        );
        let table = listing_table(dir.path(), header_format());
        let (amount, user) = query_sums(table)
            .await
            .expect("a federated read still answers");
        assert_eq!(amount, None, "amount_cents is gone from the header");
        assert_eq!(user, Some(30), "present columns stay bound by name");
    }

    #[tokio::test]
    async fn csv_without_a_header_is_not_wrapped() {
        let format =
            HeaderProjectedCsvFormat::wrap(Arc::new(CsvFormat::default().with_has_header(false)));
        assert!(
            format.as_ref().downcast_ref::<CsvFormat>().is_some(),
            "positional CSV must keep DataFusion's reader"
        );
        assert!(
            format
                .as_ref()
                .downcast_ref::<HeaderProjectedCsvFormat>()
                .is_none()
        );
    }

    #[test]
    fn project_record_batch_fills_missing_columns_with_null() {
        let physical = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new("amount_cents", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .expect("batch");
        let target = Arc::new(Schema::new(vec![
            Field::new("amount_cents", DataType::Int64, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("user_id", DataType::Int64, true),
        ]));
        let projected = project_record_batch(&batch, &target).expect("project");
        let amounts = projected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount");
        assert_eq!(amounts.values(), &[100, 200]);
        let missing = projected
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("missing");
        assert!(missing.is_null(0) && missing.is_null(1));
        let users = projected
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("user");
        assert_eq!(users.values(), &[10, 20]);
    }

    #[test]
    fn project_record_batch_keeps_row_count_when_no_file_column_is_projected() {
        let physical = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .expect("batch");
        let target = Arc::new(Schema::empty());
        let projected = project_record_batch(&batch, &target).expect("project");
        assert_eq!(projected.num_rows(), 2);
        assert_eq!(projected.num_columns(), 0);
    }

    #[test]
    fn parse_header_names_strips_bom_and_quoted_commas() {
        let options = CsvOptions {
            delimiter: b',',
            quote: b'"',
            ..CsvOptions::default()
        };
        let names = parse_header_names(
            "\u{feff}event_id,\"city, state\",amount_cents\n1,\"a, b\",2\n".as_bytes(),
            &options,
        )
        .expect("header");
        assert_eq!(
            names,
            vec![
                "event_id".to_string(),
                "city, state".to_string(),
                "amount_cents".to_string()
            ]
        );
    }

    #[test]
    fn header_record_complete_is_quote_aware() {
        assert!(!header_record_complete(b"a,b", b',', b'"', None, None));
        assert!(header_record_complete(
            b"a,b\n1,2\n",
            b',',
            b'"',
            None,
            None
        ));
        assert!(!header_record_complete(
            b"\"a\nb\",c",
            b',',
            b'"',
            None,
            None
        ));
        assert!(header_record_complete(
            b"\"a\nb\",c\n",
            b',',
            b'"',
            None,
            None
        ));
        assert!(header_record_complete(
            b"\"a\"\"b\",c\n",
            b',',
            b'"',
            None,
            None
        ));
        assert!(!header_record_complete(b"\n\n", b',', b'"', None, None));
        assert!(!header_record_complete(
            b"\n\nid,amo",
            b',',
            b'"',
            None,
            None
        ));
        assert!(header_record_complete(
            b"\n\nid,amount\n",
            b',',
            b'"',
            None,
            None
        ));
        assert!(!header_record_complete(
            b"# note\nid,amo",
            b',',
            b'"',
            None,
            Some(b'#')
        ));
        assert!(header_record_complete(
            b"# note\nid,amount\n",
            b',',
            b'"',
            None,
            Some(b'#')
        ));
        // A quote mid-field is literal. The newline after `multi` is inside the
        // following quoted field, so the prefix is not a finished header.
        let literal_quote_prefix = b"event_id,dummy\"tag,\"multi\nxxxx";
        let literal_quote_full = b"event_id,dummy\"tag,\"multi\nxxxx\",amount_cents\n";
        assert!(!header_record_complete(
            literal_quote_prefix,
            b',',
            b'"',
            None,
            None
        ));
        assert!(header_record_complete(
            literal_quote_full,
            b',',
            b'"',
            None,
            None
        ));
    }

    #[tokio::test]
    async fn header_probe_grows_when_a_utf8_character_straddles_the_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        // 65_533 ASCII bytes + ',' + a 3-byte '€' puts the character across
        // the 64 KiB probe. The header record is only complete after the grow.
        let header = format!(
            "{}{}",
            "n".repeat(65_533),
            ",€,event_id,user_id,region,amount_cents,ts"
        );
        let prefix = header.as_bytes().get(..header_probe_len()).expect("prefix");
        assert!(
            std::str::from_utf8(prefix).is_err(),
            "the 64 KiB probe must split the euro sign"
        );
        assert!(!header_record_complete(prefix, b',', b'"', None, None));
        write_csv(
            &dir.path().join("base.csv"),
            &header,
            &["x,y,1,10,us,100,t0", "x,y,2,20,eu,200,t1"],
        );
        let sums = query_sums(listing_table(dir.path(), header_format()))
            .await
            .expect("query after growing the header probe");
        assert_eq!(sums, (Some(300), Some(30)));
    }

    async fn compress_bzip2(plain: &[u8]) -> Vec<u8> {
        let owned = bytes::Bytes::copy_from_slice(plain);
        let stream = futures::stream::once(async move { Ok(owned) }).boxed();
        let mut encoded = FileCompressionType::BZIP2
            .convert_to_compress_stream(stream)
            .expect("bzip2 encoder");
        let mut acc = Vec::new();
        while let Some(chunk) = encoded.next().await {
            acc.extend_from_slice(&chunk.expect("compress"));
        }
        acc
    }

    #[tokio::test]
    async fn header_probe_grows_when_bzip2_prefix_does_not_decompress() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut csv = String::from("event_id,user_id,region,amount_cents,ts\n");
        let mut rng = 0x1234_5678_u64;
        for i in 0..5_000 {
            rng = rng.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            writeln!(
                csv,
                "{i},10,us,100,{:016x}{:016x}",
                rng,
                rng.wrapping_add(1)
            )
            .expect("write row");
        }
        let compressed = compress_bzip2(csv.as_bytes()).await;
        assert!(
            compressed.len() as u64 > HEADER_PROBE_BYTES,
            "the compressed file must be larger than the first probe"
        );
        let prefix = bytes::Bytes::copy_from_slice(&compressed[..header_probe_len()]);
        let options = CsvOptions::default();
        let decoded = decode_prefix(prefix, FileCompressionType::BZIP2, &options)
            .await
            .expect("incomplete prefix is not an error");
        assert!(
            decoded
                .as_ref()
                .is_none_or(|bytes| !header_record_complete(bytes, b',', b'"', None, None)),
            "a 64 KiB bzip2 prefix must not be treated as a complete header"
        );

        std::fs::write(dir.path().join("base.csv.bz2"), compressed).expect("write bz2");
        let format = HeaderProjectedCsvFormat::wrap(Arc::new(
            CsvFormat::default()
                .with_has_header(true)
                .with_file_compression_type(FileCompressionType::BZIP2),
        ));
        let sums = query_sums(listing_table_with_extension(dir.path(), format, ".csv.bz2"))
            .await
            .expect("query after growing the compressed header probe");
        assert_eq!(sums, (Some(500_000), Some(50_000)));
    }

    #[tokio::test]
    async fn header_probe_grows_when_leading_blank_lines_push_the_header_across_the_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        // 65_530 leading newlines put the real header across the 64 KiB cut.
        // Treating the first blank line as a terminator would parse a truncated
        // name list (`event_`) instead of growing.
        let mut text = "\n".repeat(65_530);
        text.push_str("event_id,user_id,region,amount_cents,ts\n");
        text.push_str("1,10,us,100,t0\n2,20,eu,200,t1\n");
        let prefix = text.as_bytes().get(..header_probe_len()).expect("prefix");
        assert!(
            !prefix
                .windows(b"amount_cents".len())
                .any(|w| w == b"amount_cents"),
            "the 64 KiB probe must cut through the header name"
        );
        assert!(
            !header_record_complete(prefix, b',', b'"', None, None),
            "leading blank lines are not a complete header"
        );
        std::fs::write(dir.path().join("base.csv"), text).expect("write csv");
        let sums = query_sums(listing_table(dir.path(), header_format()))
            .await
            .expect("query after growing past leading blank records");
        assert_eq!(sums, (Some(300), Some(30)));
    }
}
