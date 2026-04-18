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
        ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, LargeStringBuilder, ListArray, RecordBatch, StringArray,
        StringBuilder, StructArray, TimestampMicrosecondBuilder,
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

/// Rows-per-batch when COPY-streaming the initial snapshot.
const BOOTSTRAP_BATCH_SIZE: usize = 1024;

/// Input for [`snapshot_stream`]. Grouped into a struct to keep the function
/// signature below clippy's `too_many_arguments` threshold and to make the
/// callers easier to read.
pub struct SnapshotInput {
    pub params: ReplicationParams,
    /// Reserved for a future `SET TRANSACTION SNAPSHOT` implementation; unused
    /// today because the bootstrap uses a fresh REPEATABLE READ tx.
    pub _snapshot_name: Option<String>,
    pub schema_name: String,
    pub table_name: String,
    pub dataset_schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub dataset_name: String,
    pub metrics: Arc<ReplicationMetricsCollector>,
}

/// Build a `ChangesStream`-compatible stream that emits all rows of the source
/// table as op="c" change envelopes.
pub async fn snapshot_stream(
    input: SnapshotInput,
) -> Result<impl Stream<Item = std::result::Result<ChangeEnvelope, StreamError>> + Send + use<>> {
    let SnapshotInput {
        params,
        _snapshot_name,
        schema_name,
        table_name,
        dataset_schema,
        primary_keys,
        dataset_name,
        metrics,
    } = input;
    // Eagerly build the TLS connector — avoids doing it inside `try_stream!`
    // where error conversion is awkward.
    let tls_connector = params.native_tls_connector().context(TlsConfigSnafu)?;
    let params_for_stream = params.clone();
    let dataset_name_clone = dataset_name.clone();

    Ok(try_stream! {
        let cfg = params_for_stream.pg_config(
            &format!("spice-replication-bootstrap/{dataset_name_clone}")
        );

        let (client, conn_task) = match tls_connector {
            Some(connector) => {
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
            }
            None => {
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
            }
        };

        // REPEATABLE READ to lock in a consistent view.
        client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .context(BootstrapSnafu)
            .map_err(super::err_to_stream)?;

        // Stream rows out of the snapshot transaction.
        let select_sql = format!(
            "SELECT * FROM {schema}.{table}",
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
            let column_map_ref = column_map.as_ref().expect("column_map set above");
            for (col_idx, &pg_idx) in column_map_ref.iter().enumerate() {
                builders[col_idx]
                    .append_from_row(&row, pg_idx)
                    .map_err(super::err_to_stream)?;
            }
            rows_in_batch += 1;
            total_rows += 1;
            metrics.add_bootstrap_rows(1);

            if rows_in_batch >= BOOTSTRAP_BATCH_SIZE {
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
            }
        }

        // Final batch: mark dataset ready.
        if rows_in_batch > 0 {
            let batch = finish_batch(
                &dataset_schema,
                &mut builders,
                rows_in_batch,
                &primary_keys,
            )
            .map_err(super::err_to_stream)?;
            yield envelope_with_lsn(batch, Arc::clone(&confirmed_flush), 0, true);
        } else {
            // Empty table: still need to flip the ready flag. Emit an empty batch.
            let batch = finish_batch(&dataset_schema, &mut builders, 0, &primary_keys)
                .map_err(super::err_to_stream)?;
            yield envelope_with_lsn(batch, Arc::clone(&confirmed_flush), 0, true);
        }

        // Wait for a successful COMMIT before marking bootstrap complete —
        // if the commit fails (network drop, server restart), we don't want
        // the `replication_bootstrap_complete` metric to flip, because it's
        // watched as a readiness signal.
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
            "initial snapshot bootstrap complete"
        );
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

    let data_struct = StructArray::new(dataset_schema.fields().clone(), data_arrays, None);
    let wrapper_schema = Arc::new(changes_schema(dataset_schema));
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
    Bool(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date32(Date32Builder),
    TimestampMicros(TimestampMicrosecondBuilder, Option<Arc<str>>),
}

impl BootstrapBuilder {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new()),
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int16 => Self::Int16(Int16Builder::new()),
            DataType::Int32 => Self::Int32(Int32Builder::new()),
            DataType::Int64 => Self::Int64(Int64Builder::new()),
            DataType::Float32 => Self::Float32(Float32Builder::new()),
            DataType::Float64 => Self::Float64(Float64Builder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TimestampMicros(TimestampMicrosecondBuilder::new(), tz.clone())
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
                        let epoch = match chrono::NaiveDate::from_ymd_opt(1970, 1, 1) {
                            Some(epoch) => epoch,
                            None => unreachable!("1970-01-01 is a valid NaiveDate"),
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
            Self::TimestampMicros(b, _tz) => {
                // Differentiate timestamptz vs timestamp.
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
        }
        Ok(())
    }

    fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::LargeUtf8(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::TimestampMicros(b, tz) => {
                let arr = b.finish();
                Arc::new(match tz {
                    Some(tz) => arr.with_timezone(tz.clone()),
                    None => arr,
                })
            }
        }
    }
}
