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

//! Initial table snapshot, streamed as `op="c"` change batches.
//!
//! The binlog head position is captured *before* this snapshot begins (see
//! `super::start_replication_stream`), so rows committed while the snapshot
//! runs are delivered at most twice — once by the snapshot's consistent
//! read, once by binlog replay — and converge via the accelerator's PK
//! upsert. This is the same at-least-once contract as the Postgres
//! snapshot/WAL boundary.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use futures::StreamExt;
use mysql_async::Row;
use mysql_async::prelude::Queryable;

use super::config::ReplicationParams;
use super::metrics::MetricsCollector;
use super::rows::{ChangeOp, DecodedChange, build_change_batch};
use super::setup::TableLayout;
use super::{Error, err_to_stream};
use crate::cdc::{ChangeEnvelope, ChangesStream, NoOpCommitter};

pub(super) struct SnapshotInput {
    pub params: ReplicationParams,
    pub layout: TableLayout,
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    /// Dataset field index → source row-image index.
    pub column_map: Vec<usize>,
    pub database: String,
    pub table: String,
    pub dataset_name: String,
    pub metrics: Arc<MetricsCollector>,
}

/// Stream the table's current rows as `op="c"` change envelopes with no-op
/// committers. The caller chains this before the binlog stream and appends
/// the ready/position envelope once the snapshot completes.
pub(super) fn snapshot_stream(input: SnapshotInput) -> ChangesStream {
    let SnapshotInput {
        params,
        layout,
        schema,
        primary_keys,
        column_map,
        database,
        table,
        dataset_name,
        metrics,
    } = input;

    // Select every layout column, in layout order, so the produced rows line
    // up with binlog row images and `column_map` applies to both.
    let column_list = layout
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!(
        "SELECT {column_list} FROM {}.{}",
        quote_ident(&database),
        quote_ident(&table)
    );

    Box::pin(try_stream! {
        tracing::info!(
            dataset = %dataset_name,
            "mysql replication: starting initial snapshot"
        );

        // The MySQL cursor borrows its connection, which a boxed 'static
        // stream can't hold across yields — so a reader task owns the
        // connection and sends owned row batches through a small bounded
        // channel (which also pipelines row fetch with batch conversion).
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<Vec<Row>, mysql_async::Error>>(2);
        let batch_size = params.bootstrap_batch_size;
        let reader_params = params.clone();
        let reader: tokio::task::JoinHandle<Result<(), mysql_async::Error>> =
            tokio::spawn(async move {
                let mut conn = mysql_async::Conn::new(reader_params.opts.clone()).await?;
                // Pin the session to UTC so TIMESTAMP columns render in the
                // same zone the binlog stores them in (binlog TIMESTAMP
                // values are UTC unix seconds) — snapshot and live batches
                // must agree.
                conn.query_drop("SET time_zone = '+00:00'").await?;
                conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .await?;
                conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                    .await?;

                {
                    let mut query_result = conn.exec_iter(select_sql, ()).await?;
                    let row_stream = query_result.stream::<Row>().await?;
                    if let Some(mut row_stream) = row_stream {
                        // Manual accumulation instead of the `chunks()`
                        // combinator: wrapping the multi-lifetime
                        // `ResultSetStream` in a combinator inside this
                        // spawned (Send + 'static) future trips rustc's
                        // higher-ranked lifetime check.
                        let mut pending: Vec<Row> = Vec::with_capacity(batch_size);
                        loop {
                            match row_stream.next().await {
                                Some(Ok(row)) => {
                                    pending.push(row);
                                    if pending.len() >= batch_size
                                        && tx.send(Ok(std::mem::take(&mut pending))).await.is_err()
                                    {
                                        // Receiver dropped — consumer went away.
                                        return Ok(());
                                    }
                                }
                                Some(Err(e)) => {
                                    let _ = tx.send(Err(e)).await;
                                    return Ok(());
                                }
                                None => break,
                            }
                        }
                        if !pending.is_empty() && tx.send(Ok(pending)).await.is_err() {
                            return Ok(());
                        }
                    }
                }

                // Read-only transaction: COMMIT just releases the snapshot.
                conn.query_drop("COMMIT").await?;
                conn.disconnect().await?;
                Ok(())
            });

        let mut total_rows: u64 = 0;
        while let Some(rows) = rx.recv().await {
            let rows = rows.map_err(|e| snapshot_error("snapshot row read", &e))?;
            if rows.is_empty() {
                continue;
            }
            let changes: Vec<DecodedChange> = rows
                .into_iter()
                .map(|row| DecodedChange {
                    op: ChangeOp::Create,
                    // `mysql_async::Row::unwrap` (not `Option::unwrap`) moves
                    // the row's values out; it only panics after a prior
                    // `take()`, which this path never calls.
                    row: row.unwrap(),
                })
                .collect();
            let row_count = changes.len() as u64;
            let batch = build_change_batch(&schema, &primary_keys, &column_map, &changes)
                .map_err(err_to_stream)?;
            total_rows += row_count;
            metrics.add_bootstrap_rows(row_count);
            yield ChangeEnvelope::new(Box::new(NoOpCommitter), batch, false);
        }

        match reader.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => Err(snapshot_error("snapshot", &e))?,
            Err(join_error) => Err(crate::cdc::StreamError::External(format!(
                "mysql snapshot reader task failed: {join_error}"
            )))?,
        }

        metrics.mark_bootstrap_complete();
        tracing::info!(
            dataset = %dataset_name,
            rows = total_rows,
            "mysql replication: initial snapshot complete"
        );
    })
}

fn snapshot_error(context: &str, e: &mysql_async::Error) -> crate::cdc::StreamError {
    err_to_stream(Error::Bootstrap {
        message: format!("{context}: {e}"),
    })
}

/// Backtick-quote a `MySQL` identifier (embedded backticks double).
fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers_with_embedded_backticks() {
        assert_eq!(quote_ident("orders"), "`orders`");
        assert_eq!(quote_ident("odd`name"), "`odd``name`");
    }
}
