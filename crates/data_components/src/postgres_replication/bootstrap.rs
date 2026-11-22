/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Initial-snapshot loader.
//!
//! Emits `ChangeBatch`es with op="c" for every row of the source table, using a
//! `REPEATABLE READ` transaction to provide a stable view for the snapshot
//! scan. This does NOT imply exported-snapshot or slot-creation-LSN-consistent
//! semantics across the snapshot/WAL boundary — see the module-level doc
//! comment on [`super::start_replication_stream`]. Readiness is lag-based and
//! comes from the WAL stream once it catches up to the source head; the
//! snapshot's final envelope is a not-ready boundary marker.

use std::sync::{Arc, atomic::AtomicU64};

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder,
        ListArray, RecordBatch, StringArray, StringBuilder, StructArray, Time64NanosecondBuilder,
        TimestampMicrosecondBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, SchemaRef, TimeUnit},
};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use snafu::{OptionExt, ResultExt};
use tokio_postgres::NoTls;
use tokio_postgres::types::Type;

use super::{
    BootstrapSnafu, PgOutputDecodeSnafu, ReplicationMetricsCollector, Result, SetupConnectSnafu,
    TlsConfigSnafu, changes::envelope_with_lsn, config::ReplicationParams,
};
use crate::cdc::{ChangeBatch, ChangeEnvelope, StreamError, changes_schema};

/// Input for [`snapshot_stream`]. Grouped into a struct to keep the function
/// signature below clippy's `too_many_arguments` threshold and to make the
/// callers easier to read.
pub struct SnapshotInput {
    pub params: ReplicationParams,
    pub schema_name: String,
    pub table_name: String,
    pub dataset_schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub dataset_name: String,
    pub metrics: Arc<ReplicationMetricsCollector>,
}

/// Roughly 1 GiB per reader at PostgreSQL's usual 8 KiB page size amortizes
/// connection/snapshot setup and avoids fanning out small-table bootstraps.
const PAGES_PER_READER: u32 = 131_072;
/// At most two queued batches per reader; producers await downstream capacity.
const BATCHES_PER_READER: usize = 2;

/// Build a `ChangesStream`-compatible stream that emits all rows of the source
/// table as op="c" change envelopes.
pub fn snapshot_stream(
    input: SnapshotInput,
) -> Result<impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<>> {
    Ok(snapshot_stream_inner(input, PAGES_PER_READER, None))
}

/// Internal integration-test entry point: exercise heap splitting without a
/// multi-GiB fixture and observe the actual fan-out without adding a metric.
#[doc(hidden)]
pub fn snapshot_stream_for_test(
    input: SnapshotInput,
    pages_per_reader: std::num::NonZeroU32,
    readers: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<>> {
    Ok(snapshot_stream_inner(
        input,
        pages_per_reader.get(),
        Some(readers),
    ))
}

/// Own the connection driver as well as the client: aborting a reader or
/// dropping the output stream closes its socket and rolls back its transaction.
struct BootstrapConnection {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for BootstrapConnection {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

async fn connect(input: &SnapshotInput) -> Result<BootstrapConnection> {
    let tls = input
        .params
        .native_tls_connector()
        .await
        .context(TlsConfigSnafu)?;
    let cfg = input.params.pg_config(&format!(
        "spice-replication-bootstrap/{}",
        input.dataset_name
    ));
    let (client, driver) = if let Some(connector) = tls {
        let (client, connection) = cfg.connect(connector).await.context(SetupConnectSnafu)?;
        (
            client,
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres bootstrap connection terminated: {e}");
                }
            }),
        )
    } else {
        let (client, connection) = cfg.connect(NoTls).await.context(SetupConnectSnafu)?;
        (
            client,
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres bootstrap connection terminated: {e}");
                }
            }),
        )
    };
    Ok(BootstrapConnection { client, driver })
}

fn snapshot_stream_inner(
    input: SnapshotInput,
    pages_per_reader: u32,
    readers_observer: Option<Arc<std::sync::atomic::AtomicUsize>>,
) -> impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<> {
    // Keep the existing try_stream envelope/error contract at the WAL boundary.
    try_stream! {
        let input = Arc::new(input);
        let holder = connect(&input).await.map_err(super::err_to_stream)?;
        let relation = format!("{}.{}", quote_ident(&input.schema_name), quote_ident(&input.table_name));
        // Probe in autocommit so catalog access cannot establish the scan's snapshot.
        let probe = async {
            let metadata = holder.client.query_one(
                "SELECT c.relpages, c.relkind = 'r' AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent = c.oid), current_setting('server_version_num')::int >= 140000 FROM pg_catalog.pg_class c WHERE c.oid = $1::text::regclass",
                &[&relation],
            ).await?;
            let pages: i32 = metadata.try_get(0)?;
            let ordinary_heap: bool = metadata.try_get(1)?;
            let supports_tid_ranges: bool = metadata.try_get(2)?;
            Ok::<_, tokio_postgres::Error>((pages, ordinary_heap, supports_tid_ranges))
        }.await;
        let (pages, readers) = match probe {
            Ok((pages, ordinary_heap, supports_tid_ranges)) => {
                let pages = u32::try_from(pages).unwrap_or_default();
                let readers = if ordinary_heap && supports_tid_ranges {
                    cpu_budget::cpu_budget().postgres_snapshot_readers(pages, pages_per_reader)
                } else { 1 };
                (pages, readers)
            }
            Err(error) => {
                tracing::debug!(dataset = %input.dataset_name, table = %relation, error = %super::pg_error_detail(&error), "initial snapshot bootstrap probe failed; using one reader");
                (0, 1)
            }
        };
        if let Some(observer) = readers_observer {
            observer.store(readers, std::sync::atomic::Ordering::Release);
        }
        tracing::debug!(dataset = %input.dataset_name, readers, pages, "initial snapshot bootstrap readers");
        holder.client.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await.context(BootstrapSnafu).map_err(super::err_to_stream)?;
        let snapshot: Option<String> = if readers > 1 {
            // TRUNCATE and rewriting ALTER are not MVCC-safe without a table lock
            // held before the snapshot: https://www.postgresql.org/docs/current/mvcc-caveats.html.
            holder.client.simple_query(&format!("LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE"))
                .await.context(BootstrapSnafu).map_err(super::err_to_stream)?;
            Some(holder.client.query_one("SELECT pg_export_snapshot()", &[])
                .await.context(BootstrapSnafu).map_err(super::err_to_stream)?
                .try_get(0).context(BootstrapSnafu).map_err(super::err_to_stream)?)
        } else { None };
        // Stream rows out of the snapshot transaction. Select exactly the
        // dataset's columns (not `*`): columns outside the dataset schema —
        // whatever their types — are never fetched. Columns whose Arrow type
        // is text-derived are cast to `::text` so the wire type matches what
        // the builders read:
        //   - Utf8/LargeUtf8: the provider maps uuid/json(b)/inet/citext/enum
        //     and friends to Arrow strings, but tokio-postgres only
        //     deserializes genuine text wire types into String. `::text`
        //     yields each type's canonical text form (a no-op for real text)
        //     — the same representation pgoutput emits on the WAL path.
        //   - Decimal128: read as text and parsed by the shared numeric
        //     parser (NUMERIC has no native String deserialization).
        //   - List/Dictionary: Postgres array literal / enum label, parsed by
        //     the same builder the WAL path uses.
        let select_list = input.dataset_schema
            .fields()
            .iter()
            .map(|f| {
                let ident = quote_ident(f.name());
                if matches!(
                    f.data_type(),
                    DataType::List(_)
                        | DataType::Dictionary(_, _)
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Decimal128(_, _)
                ) {
                    format!("{ident}::text")
                } else {
                    ident
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut tasks = tokio::task::JoinSet::new();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(readers * BATCHES_PER_READER);
        let mut holder = Some(holder);
        for reader in 0..readers {
            let sql = if readers == 1 {
                // Preserve the single-reader SELECT, including inheritance semantics.
                format!("SELECT {select_list} FROM {relation}")
            } else {
                let lo = u64::from(pages) * reader as u64 / readers as u64;
                // ONLY keeps concurrent INHERIT from changing the scanned relation set.
                // The last range is open-ended so pages added after `relpages`
                // was sampled are still read.
                let upper = if reader + 1 < readers {
                    let hi = u64::from(pages) * (reader + 1) as u64 / readers as u64;
                    format!(" AND ctid < '({hi},0)'::tid")
                } else {
                    String::new()
                };
                format!("SELECT {select_list} FROM ONLY {relation} WHERE ctid >= '({lo},0)'::tid{upper}")
            };
            let holder_connection = if reader == 0 { holder.take() } else { None };
            let input = Arc::clone(&input);
            let sender = sender.clone();
            let snapshot = snapshot.clone();
            tasks.spawn(async move {
                let is_holder = holder_connection.is_some();
                let connection = if let Some(connection) = holder_connection {
                    connection
                } else {
                    let connection = connect(&input).await?;
                    connection.client.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
                        .await.context(BootstrapSnafu)?;
                    // Import is the first statement after BEGIN, before any query.
                    let snapshot = snapshot.as_ref().context(super::BootstrapReaderMissingSnafu)?;
                    let import = format!("SET TRANSACTION SNAPSHOT '{}'", snapshot.replace('\'', "''"));
                    connection.client.simple_query(&import).await.context(BootstrapSnafu)?;
                    connection
                };
                let result = scan_reader(&connection.client, &input, &sql, &sender).await?;
                if !is_holder {
                    connection.client.simple_query("COMMIT").await.context(BootstrapSnafu)?;
                }
                Ok::<_, super::Error>((result, if is_holder { Some(connection) } else { None }))
            });
        }
        drop(sender);
        let confirmed_flush = Arc::new(AtomicU64::new(0));
        let mut total_rows = 0;
        let mut holder_result = None;
        // `select!` arms run in their own async blocks, where `try_stream!` cannot
        // rewrite `?` or `yield`, so each arm only reports what happened.
        enum ReaderEvent<J> {
            Joined(Option<J>),
            Batch(ChangeBatch),
        }
        while !tasks.is_empty() {
            let event = tokio::select! {
                biased;
                joined = tasks.join_next() => ReaderEvent::Joined(joined),
                Some(batch) = receiver.recv() => ReaderEvent::Batch(batch),
            };
            match event {
                ReaderEvent::Joined(joined) => {
                    let result = joined.context(super::BootstrapReaderMissingSnafu)
                        .and_then(|joined| joined.context(super::BootstrapReaderJoinSnafu))
                        .and_then(std::convert::identity);
                    if result.is_err() {
                        // Tear down every transaction before yielding the error, even
                        // if the caller leaves the stream suspended at that error.
                        tasks.shutdown().await;
                        drop(holder_result.take());
                    }
                    let ((final_batch, rows), connection) = result.map_err(super::err_to_stream)?;
                    total_rows += rows;
                    if let Some(connection) = connection {
                        holder_result = Some((connection, final_batch));
                    } else {
                        yield envelope_with_lsn(final_batch, Arc::clone(&confirmed_flush), 0, false, input.dataset_name.clone());
                    }
                }
                ReaderEvent::Batch(batch) => {
                    yield envelope_with_lsn(batch, Arc::clone(&confirmed_flush), 0, false, input.dataset_name.clone());
                }
            }
        }
        // All producers finished; drain the bounded queue before the final boundary.
        while let Some(batch) = receiver.recv().await {
            yield envelope_with_lsn(batch, Arc::clone(&confirmed_flush), 0, false, input.dataset_name.clone());
        }
        let (holder, final_batch) = holder_result.context(super::BootstrapReaderMissingSnafu)
            .map_err(super::err_to_stream)?;
        let commit = holder.client.simple_query("COMMIT").await.context(BootstrapSnafu);
        drop(holder);
        commit.map_err(super::err_to_stream)?;
        input.metrics.mark_bootstrap_complete();
        tracing::info!(
            dataset = %input.dataset_name,
            rows = total_rows,
            expected = ?input.metrics.bootstrap_rows_expected(),
            "initial snapshot bootstrap complete"
        );
        // Readiness remains lag-based; shared.rs appends the watermark only on success.
        yield envelope_with_lsn(final_batch, confirmed_flush, 0, false, input.dataset_name.clone());
    }
}

/// Scan one transaction using exactly the bootstrap's existing Arrow conversion
/// and batch boundaries. The final batch is returned for commit-before-boundary.
async fn scan_reader(
    client: &tokio_postgres::Client,
    input: &SnapshotInput,
    select_sql: &str,
    sender: &tokio::sync::mpsc::Sender<ChangeBatch>,
) -> Result<(ChangeBatch, u64)> {
    let SnapshotInput {
        params,
        dataset_schema,
        primary_keys,
        dataset_name,
        metrics,
        ..
    } = input;
    let bootstrap_batch_size = params.bootstrap_batch_size;
    let portal_stream = client
        .query_raw(
            select_sql,
            std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
        )
        .await
        .context(BootstrapSnafu)?;
    tokio::pin!(portal_stream);

    // Prepare accumulators.
    let mut builders = dataset_schema
        .fields()
        .iter()
        .map(|f| BootstrapBuilder::new(f.data_type()))
        .collect::<Result<Vec<_>>>()?;
    let mut rows_in_batch: usize = 0;
    let mut total_rows: u64 = 0;

    // Cache dataset_field_idx → pg_column_idx once (computed from the first
    // row's column metadata) so the hot per-row loop is O(fields) instead of
    // O(fields²). All subsequent rows in a SELECT stream share the same
    // column layout.
    let mut column_map: Option<Vec<usize>> = None;

    while let Some(row_result) = portal_stream.next().await {
        // Buffered rows can otherwise keep conversion running without yielding.
        tokio::task::consume_budget().await;
        let row = row_result.context(BootstrapSnafu)?;

        if column_map.is_none() {
            let map = dataset_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        row.columns()
                            .iter()
                            .position(|c| c.name() == field.name())
                            .ok_or_else(|| super::Error::SchemaMismatch {
                                message: format!(
                                    "dataset column `{}` not in source table `{schema_name}.{table_name}`",
                                    field.name(),
                                    schema_name = input.schema_name,
                                    table_name = input.table_name,
                                ),
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
            column_map = Some(map);
        }
        let Some(column_map_ref) = column_map.as_ref() else {
            unreachable!("column_map is initialized above on first iteration")
        };
        for (col_idx, &pg_idx) in column_map_ref.iter().enumerate() {
            builders[col_idx].append_from_row(&row, pg_idx)?;
        }
        rows_in_batch += 1;
        total_rows += 1;
        metrics.add_bootstrap_rows(1);

        if rows_in_batch >= bootstrap_batch_size {
            let batch = finish_batch(dataset_schema, &mut builders, rows_in_batch, primary_keys)?;
            rows_in_batch = 0;
            builders = dataset_schema
                .fields()
                .iter()
                .map(|f| BootstrapBuilder::new(f.data_type()))
                .collect::<Result<Vec<_>>>()?;
            snafu::ensure!(
                sender.send(batch).await.is_ok(),
                super::BootstrapReaderMissingSnafu
            );
            if let Some(percent) = metrics.bootstrap_progress_percent() {
                tracing::debug!(
                    dataset = %dataset_name,
                    rows = total_rows,
                    expected = ?metrics.bootstrap_rows_expected(),
                    progress_percent = percent,
                    "initial snapshot bootstrap progress"
                );
            }
        }
    }

    // The coordinator withholds the holder's final batch until every commit succeeds.
    let final_batch = if rows_in_batch > 0 {
        finish_batch(dataset_schema, &mut builders, rows_in_batch, primary_keys)?
    } else {
        // Empty table: still emit a zero-row snapshot-boundary envelope so
        // the WAL stream has a clean schema baseline to chain from. The
        // batch has zero rows.
        finish_batch(dataset_schema, &mut builders, 0, primary_keys)?
    };

    Ok((final_batch, total_rows))
}

fn finish_batch(
    dataset_schema: &SchemaRef,
    builders: &mut Vec<BootstrapBuilder>,
    num_rows: usize,
    primary_keys: &[String],
) -> Result<ChangeBatch> {
    let data_arrays: Vec<ArrayRef> = std::mem::take(builders)
        .into_iter()
        .map(BootstrapBuilder::finish)
        .collect();

    // op column: all "c"
    let mut op_builder = StringBuilder::with_capacity(num_rows, num_rows * 2);
    for _ in 0..num_rows {
        op_builder.append_value("c");
    }

    // primary_keys column: list of pk names per row (same for every row).
    let mut pk_offsets = Vec::<i32>::with_capacity(num_rows + 1);
    let mut pk_values: Vec<String> = Vec::with_capacity(num_rows * primary_keys.len());
    pk_offsets.push(0);
    for _ in 0..num_rows {
        for pk in primary_keys {
            pk_values.push(pk.clone());
        }
        pk_offsets.push(i32::try_from(pk_values.len()).map_err(|e| {
            super::Error::SchemaMismatch {
                message: format!("pk list overflow: {e}"),
            }
        })?);
    }

    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_list = ListArray::new(
        Arc::clone(&pk_field),
        OffsetBuffer::new(pk_offsets.into()),
        Arc::new(StringArray::from(pk_values)),
        None,
    );

    // Mirror the nullability decision in `changes::build_change_batch`: the
    // ChangeBatch's internal data struct is always nullable so it can hold
    // null-padded rows (not relevant for bootstrap, but keeps the schema
    // identical to the WAL path so downstream code sees a consistent shape).
    let nullable_schema = super::changes::nullable_clone_for_bootstrap(dataset_schema);
    let data_struct = StructArray::new(nullable_schema.fields().clone(), data_arrays, None);
    let wrapper_schema = Arc::new(changes_schema(&nullable_schema));
    let record = RecordBatch::try_new(
        wrapper_schema,
        vec![
            Arc::new(op_builder.finish()) as ArrayRef,
            Arc::new(pk_list) as ArrayRef,
            Arc::new(data_struct) as ArrayRef,
        ],
    )
    .map_err(|e| super::Error::SchemaMismatch {
        message: format!("bootstrap record batch build failed: {e}"),
    })?;
    ChangeBatch::try_new(record).map_err(|e| super::Error::SchemaMismatch {
        message: format!("bootstrap ChangeBatch validation failed: {e}"),
    })
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Row-driven builder that pulls typed values out of `tokio_postgres::Row`.
enum BootstrapBuilder {
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    Time64Nanos(Time64NanosecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder, Option<Arc<str>>),
    TimestampNanos(TimestampNanosecondBuilder, Option<Arc<str>>),
    Decimal128(Decimal128Builder, u8, i8),
    /// Array or enum (dictionary) column. Fetched as `::text` — the same
    /// representation pgoutput emits on the WAL path — and parsed by the same
    /// builder the WAL path uses, so both paths share one implementation.
    TextCast(super::changes::FieldBuilder),
}

impl BootstrapBuilder {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int8 => Self::Int8(Int8Builder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::UInt32 => Self::UInt32(UInt32Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64Nanos(Time64NanosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::new(), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Self::TimestampNanos(TimestampNanosecondBuilder::new(), tz.clone())
            }
            DataType::Decimal128(precision, scale) => Self::Decimal128(
                Decimal128Builder::new().with_data_type(data_type.clone()),
                *precision,
                *scale,
            ),
            DataType::List(_) | DataType::Dictionary(_, _) => {
                // Bootstrap builders accumulate up to `bootstrap_batch_size`
                // rows before finishing; start at Arrow's default capacity (1024)
                // and let amortized growth cover the rest.
                Self::TextCast(super::changes::FieldBuilder::with_capacity(
                    data_type, 1024,
                )?)
            }
            DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                return PgOutputDecodeSnafu {
                    message: format!(
                        "bootstrap: list type {data_type} is not supported yet. \
                         Cast the column to a scalar type on the source, or exclude it."
                    ),
                }
                .fail();
            }
            DataType::Interval(_) => {
                return PgOutputDecodeSnafu {
                    message: "bootstrap: INTERVAL columns are not supported yet. \
                              Cast to text or numeric seconds on the source."
                        .to_string(),
                }
                .fail();
            }
            other => {
                return PgOutputDecodeSnafu {
                    message: format!(
                        "bootstrap: unsupported Arrow data type in dataset schema: {other}"
                    ),
                }
                .fail();
            }
        })
    }

    fn append_from_row(&mut self, row: &tokio_postgres::Row, idx: usize) -> Result<()> {
        let pg_type = row.columns()[idx].type_().clone();
        match self {
            Self::Utf8(b) => {
                let v: Option<String> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read utf8: {e}"),
                    })?;
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Self::LargeUtf8(b) => {
                let v: Option<String> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read large utf8: {e}"),
                    })?;
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Self::Binary(b) => {
                let v: Option<Vec<u8>> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read bytea: {e}"),
                    })?;
                match v {
                    Some(bytes) => b.append_value(bytes),
                    None => b.append_null(),
                }
            }
            Self::Bool(b) => {
                let v: Option<bool> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read bool: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Int8(b) => {
                let v: Option<i8> = row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                    message: format!("bootstrap read int8: {e}"),
                })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Int16(b) => {
                let v: Option<i16> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read int16: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Int32(b) => {
                let v: Option<i32> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read int32: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Int64(b) => {
                let v: Option<i64> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read int64: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::UInt32(b) => {
                let v: Option<u32> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read uint32: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Float32(b) => {
                let v: Option<f32> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read f32: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Float64(b) => {
                let v: Option<f64> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read f64: {e}"),
                    })?;
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            Self::Date32(b) => {
                let v: Option<chrono::NaiveDate> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read date: {e}"),
                    })?;
                match v {
                    Some(d) => {
                        let Some(epoch) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1) else {
                            unreachable!("1970-01-01 is a valid NaiveDate")
                        };
                        let days = (d - epoch).num_days();
                        b.append_value(i32::try_from(days).map_err(|e| {
                            super::Error::SchemaMismatch {
                                message: format!("date overflow: {e}"),
                            }
                        })?);
                    }
                    None => b.append_null(),
                }
            }
            Self::Time64Nanos(b) => {
                use chrono::Timelike;
                let v: Option<chrono::NaiveTime> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read time: {e}"),
                    })?;
                match v {
                    Some(t) => {
                        let nanos = i64::from(t.num_seconds_from_midnight()) * 1_000_000_000
                            + i64::from(t.nanosecond());
                        b.append_value(nanos);
                    }
                    None => b.append_null(),
                }
            }
            Self::TimestampMicros(b, _tz) => {
                if pg_type == Type::TIMESTAMPTZ {
                    let v: Option<chrono::DateTime<chrono::Utc>> =
                        row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                            message: format!("bootstrap read timestamptz: {e}"),
                        })?;
                    match v {
                        Some(dt) => b.append_value(dt.timestamp_micros()),
                        None => b.append_null(),
                    }
                } else {
                    let v: Option<chrono::NaiveDateTime> =
                        row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                            message: format!("bootstrap read timestamp: {e}"),
                        })?;
                    match v {
                        Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                        None => b.append_null(),
                    }
                }
            }
            Self::TimestampNanos(b, _tz) => {
                if pg_type == Type::TIMESTAMPTZ {
                    let v: Option<chrono::DateTime<chrono::Utc>> =
                        row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                            message: format!("bootstrap read timestamptz: {e}"),
                        })?;
                    match v {
                        Some(dt) => {
                            let nanos = dt.timestamp_nanos_opt().ok_or_else(|| {
                                super::Error::SchemaMismatch {
                                    message: format!("timestamptz '{dt}' out of nanosecond range"),
                                }
                            })?;
                            b.append_value(nanos);
                        }
                        None => b.append_null(),
                    }
                } else {
                    let v: Option<chrono::NaiveDateTime> =
                        row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                            message: format!("bootstrap read timestamp: {e}"),
                        })?;
                    match v {
                        Some(dt) => {
                            let nanos = dt.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                                super::Error::SchemaMismatch {
                                    message: format!("timestamp '{dt}' out of nanosecond range"),
                                }
                            })?;
                            b.append_value(nanos);
                        }
                        None => b.append_null(),
                    }
                }
            }
            Self::TextCast(fb) => {
                // Fetched as `::text` — array literal or enum label.
                let v: Option<String> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read column (as text): {e}"),
                    })?;
                match v {
                    // Bootstrap fetches these columns as `::text`, so the value
                    // is always text — the `type_oid` is irrelevant here (only
                    // the binary branch consults it), hence `0` (unknown).
                    Some(s) => fb.append(
                        Some(&super::pgoutput::Value::Text(bytes::Bytes::from(s))),
                        super::changes::ChangeOp::Create,
                        0,
                    )?,
                    None => fb.append_null(),
                }
            }
            Self::Decimal128(b, precision, scale) => {
                // Read NUMERIC as text from Postgres; parse to i128 with the
                // dataset's declared precision and scale, through the same
                // routine as the WAL path. Both must be passed: a snapshot that
                // widened precision would admit rows the replication stream
                // rejects, so the same source value would land differently
                // depending on which path carried it.
                let v: Option<String> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read numeric (as text): {e}"),
                    })?;
                match v {
                    Some(s) => {
                        let value = super::changes::parse_pg_numeric_public(&s, *precision, *scale)
                            .map_err(|e| super::Error::SchemaMismatch {
                                message: format!("bootstrap numeric parse '{s}': {e}"),
                            })?;
                        b.append_value(value);
                    }
                    None => b.append_null(),
                }
            }
        }
        Ok(())
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int8(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Time64Nanos(b) => Arc::new(b.finish()),
            Self::TimestampMicros(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(Arc::clone(tz)),
                    None => arr,
                })
            }
            Self::TimestampNanos(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(Arc::clone(tz)),
                    None => arr,
                })
            }
            Self::Decimal128(b, _, _) => Arc::new(b.finish()),
            Self::TextCast(fb) => {
                let placeholder = super::changes::FieldBuilder::Utf8(StringBuilder::new());
                std::mem::replace(fb, placeholder).finish()
            }
        }
    }
}
