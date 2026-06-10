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
//! comment on [`super::start_replication_stream`]. The last envelope flips
//! `is_dataset_ready=true`.

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
use futures::Stream;
use snafu::ResultExt;
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

/// Build a `ChangesStream`-compatible stream that emits all rows of the source
/// table as op="c" change envelopes.
pub fn snapshot_stream(
    input: SnapshotInput,
) -> Result<impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<>> {
    let SnapshotInput {
        params,
        schema_name,
        table_name,
        dataset_schema,
        primary_keys,
        dataset_name,
        metrics,
    } = input;
    let dataset_name_clone = dataset_name.clone();
    let bootstrap_batch_size = params.bootstrap_batch_size;

    Ok(try_stream! {
        // Build the TLS connector inside the stream because it's an async
        // operation (reads sslrootcert via tokio::fs).
        let tls_connector = params
            .native_tls_connector()
            .await
            .context(TlsConfigSnafu)
            .map_err(super::err_to_stream)?;
        let cfg = params.pg_config(
            &format!("spice-replication-bootstrap/{dataset_name_clone}")
        );

        let (client, conn_task) = if let Some(connector) = tls_connector {
            let (c, connection) = cfg
                .connect(connector)
                .await
                .context(SetupConnectSnafu)
                .map_err(super::err_to_stream)?;
            let t = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres bootstrap connection terminated: {e}");
                }
            });
            (c, t)
        } else {
            let (c, connection) = cfg
                .connect(NoTls)
                .await
                .context(SetupConnectSnafu)
                .map_err(super::err_to_stream)?;
            let t = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres bootstrap connection terminated: {e}");
                }
            });
            (c, t)
        };

        // REPEATABLE READ to lock in a consistent view.
        client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .context(BootstrapSnafu)
            .map_err(super::err_to_stream)?;

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
        let select_list = dataset_schema
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
        let select_sql = format!(
            "SELECT {select_list} FROM {schema}.{table}",
            schema = quote_ident(&schema_name),
            table = quote_ident(&table_name),
        );
        let portal_stream = client
            .query_raw(&select_sql, std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>())
            .await
            .context(BootstrapSnafu)
            .map_err(super::err_to_stream)?;
        tokio::pin!(portal_stream);

        // Prepare accumulators.
        let mut builders = dataset_schema
            .fields()
            .iter()
            .map(|f| BootstrapBuilder::new(f.data_type()))
            .collect::<Result<Vec<_>>>()
            .map_err(super::err_to_stream)?;
        let mut rows_in_batch: usize = 0;
        let mut total_rows: u64 = 0;
        let confirmed_flush = Arc::new(AtomicU64::new(0)); // unused for bootstrap, but needed by envelope helper

        // Cache dataset_field_idx → pg_column_idx once (computed from the first
        // row's column metadata) so the hot per-row loop is O(fields) instead of
        // O(fields²). All subsequent rows in a `SELECT *` stream share the same
        // column layout.
        let mut column_map: Option<Vec<usize>> = None;

        use futures::StreamExt;
        while let Some(row_result) = portal_stream.next().await {
            let row = row_result
                .context(BootstrapSnafu)
                .map_err(super::err_to_stream)?;

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
                                    field.name()
                                ),
                            })
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(super::err_to_stream)?;
                column_map = Some(map);
            }
            let Some(column_map_ref) = column_map.as_ref() else {
                unreachable!("column_map is initialized above on first iteration")
            };
            for (col_idx, &pg_idx) in column_map_ref.iter().enumerate() {
                builders[col_idx]
                    .append_from_row(&row, pg_idx)
                    .map_err(super::err_to_stream)?;
            }
            rows_in_batch += 1;
            total_rows += 1;
            metrics.add_bootstrap_rows(1);

            if rows_in_batch >= bootstrap_batch_size {
                let batch = finish_batch(
                    &dataset_schema,
                    &mut builders,
                    rows_in_batch,
                    &primary_keys,
                )
                .map_err(super::err_to_stream)?;
                rows_in_batch = 0;
                builders = dataset_schema
                    .fields()
                    .iter()
                    .map(|f| BootstrapBuilder::new(f.data_type()))
                    .collect::<Result<Vec<_>>>()
                    .map_err(super::err_to_stream)?;
                yield envelope_with_lsn(batch, Arc::clone(&confirmed_flush), 0, false);
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

        // Build the final batch but do NOT yield it yet. We first commit the
        // REPEATABLE READ transaction; only if that succeeds do we emit the
        // ready-signalling envelope. If COMMIT fails we error out and the
        // runtime never sees `is_dataset_ready=true`, matching the durable
        // state of the bootstrap.
        let final_batch = if rows_in_batch > 0 {
            finish_batch(
                &dataset_schema,
                &mut builders,
                rows_in_batch,
                &primary_keys,
            )
            .map_err(super::err_to_stream)?
        } else {
            // Empty table: still need an envelope to flip the ready flag. The
            // batch has zero rows.
            finish_batch(&dataset_schema, &mut builders, 0, &primary_keys)
                .map_err(super::err_to_stream)?
        };

        client
            .simple_query("COMMIT")
            .await
            .context(BootstrapSnafu)
            .map_err(super::err_to_stream)?;
        drop(client);
        let _ = conn_task.await;

        metrics.mark_bootstrap_complete();

        tracing::info!(
            dataset = %dataset_name,
            rows = total_rows,
            expected = ?metrics.bootstrap_rows_expected(),
            "initial snapshot bootstrap complete"
        );

        // Yield AFTER the commit has succeeded so readiness matches durability.
        yield envelope_with_lsn(final_batch, Arc::clone(&confirmed_flush), 0, true);
    })
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
                Self::TextCast(super::changes::FieldBuilder::new(data_type)?)
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
                    Some(s) => fb.append(
                        Some(&super::pgoutput::Value::Text(s)),
                        super::changes::ChangeOp::Create,
                    )?,
                    None => fb.append_null(),
                }
            }
            Self::Decimal128(b, _precision, scale) => {
                // Read NUMERIC as text from Postgres; parse to i128 with the
                // dataset's declared scale. Uses the same routine as the WAL
                // path so behavior is consistent.
                let v: Option<String> =
                    row.try_get(idx).map_err(|e| super::Error::SchemaMismatch {
                        message: format!("bootstrap read numeric (as text): {e}"),
                    })?;
                match v {
                    Some(s) => {
                        let value =
                            super::changes::parse_pg_numeric_public(&s, *scale).map_err(|e| {
                                super::Error::SchemaMismatch {
                                    message: format!("bootstrap numeric parse '{s}': {e}"),
                                }
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
