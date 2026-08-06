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

//! Forwards writes to the runtime's own tables to a Drasi source.
//!
//! `runtime.task_history` and `runtime.metrics` are not change-data-captured
//! from an external source — the runtime writes them itself — so there is no
//! change stream to decorate. Every such write funnels through
//! [`DataFusion::write_data`](crate::datafusion::DataFusion::write_data), which
//! is where this taps in.
//!
//! Delivery is **queued, never awaited by the writer**. These writes come from
//! the OpenTelemetry batch exporter's single serial export loop, so awaiting a
//! Drasi round-trip there would park telemetry export behind a retry budget that
//! can run to minutes — dropping spans at the exporter's own queue, and stalling
//! shutdown. Each table gets a bounded queue drained by one task: the writer
//! hands over a batch and returns, ordering per table is preserved, and an
//! unreachable Drasi fills the queue and is counted rather than propagating
//! backpressure into the runtime.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::Constraints;
use datafusion::sql::TableReference;
use runtime_drasi::{DrasiChangeRows, DrasiSink, OnDeliveryError};
use runtime_query_engine::query_engine::UpdateType;
use spicepod::drasi::{RuntimeDrasi, RuntimeDrasiTable};
use tokio::sync::mpsc;

use crate::datafusion::SPICE_RUNTIME_SCHEMA;

/// Batches a table may have awaiting delivery before new ones are dropped.
///
/// Bounded on purpose: the alternative to dropping is holding telemetry batches
/// in memory for as long as Drasi is unreachable.
const QUEUE_DEPTH: usize = 64;

/// The forwarders configured by `runtime.drasi`, keyed by table.
#[derive(Debug)]
pub(crate) struct InternalForwarders {
    by_table: HashMap<TableReference, TableForwarder>,
}

#[derive(Debug)]
struct TableForwarder {
    queue: mpsc::Sender<ForwardJob>,
    /// Key columns from the Spicepod. Empty means "use the table's declared
    /// primary key", which is only knowable once the table is registered.
    configured_key: Vec<String>,
    /// Batches dropped because the queue was full or the key was unusable.
    dropped: AtomicU64,
}

/// One batch handed to a table's delivery task.
struct ForwardJob {
    op_code: &'static str,
    key: Vec<String>,
    batch: RecordBatch,
}

impl InternalForwarders {
    /// Builds a forwarder per table named in `spec`, each with its own delivery
    /// task.
    ///
    /// Must be called from within a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if a transport parameter is missing or unusable.
    pub(crate) fn try_new(spec: &RuntimeDrasi) -> runtime_drasi::Result<Self> {
        let mut by_table = HashMap::with_capacity(spec.tables.len());

        for table in &spec.tables {
            let table_ref = TableReference::partial(SPICE_RUNTIME_SCHEMA, table.name.clone());
            let name = table_ref.to_string();
            let sink = super::build_sink(
                name.clone(),
                &spec.source_id,
                labels_for(table, &name),
                spec.transport,
                // A runtime table has no replication position to replay, so
                // there is nothing for blocking to protect — the queue absorbs
                // an outage and counts what it drops.
                OnDeliveryError::Skip,
                spec.params.as_ref(),
            )?;

            let (queue, jobs) = mpsc::channel(QUEUE_DEPTH);
            tokio::spawn(deliver(jobs, sink, name));

            by_table.insert(
                table_ref,
                TableForwarder {
                    queue,
                    configured_key: table.key.clone(),
                    dropped: AtomicU64::new(0),
                },
            );
        }

        Ok(Self { by_table })
    }

    /// Queues a committed write for delivery.
    ///
    /// Returns as soon as the batches are queued — it never awaits Drasi, and
    /// never fails the write. A caller that retried a "failed" write would
    /// duplicate the rows it already wrote.
    pub(crate) fn forward(
        &self,
        table: &TableReference,
        update_type: &UpdateType,
        constraints: Option<&Constraints>,
        schema: &SchemaRef,
        data: &[RecordBatch],
    ) {
        let Some(forwarder) = self.by_table.get(table) else {
            return;
        };

        let op_code = match update_type {
            UpdateType::Append => "c",
            UpdateType::Changes => "u",
            // An overwrite replaces the table wholesale without naming the rows
            // it removed, so it cannot be expressed as a set of Drasi element
            // changes — the same limitation truncate has on the CDC path.
            UpdateType::Overwrite => {
                forwarder.record_drop(
                    table,
                    "an overwrite replaces the table without naming the rows it removes, which has no Drasi equivalent",
                );
                return;
            }
        };

        let key = match forwarder.key(constraints, schema) {
            Ok(key) => key,
            Err(message) => {
                forwarder.record_drop(table, &message);
                return;
            }
        };

        for batch in data {
            if batch.num_rows() == 0 {
                continue;
            }

            // Cloning a `RecordBatch` clones `Arc`s over its arrays, not the
            // data.
            let job = ForwardJob {
                op_code,
                key: key.clone(),
                batch: batch.clone(),
            };

            if forwarder.queue.try_send(job).is_err() {
                forwarder.record_drop(
                    table,
                    "the delivery queue is full, so Drasi is not keeping up or is unreachable",
                );
            }
        }
    }
}

impl TableForwarder {
    /// The key columns for this table.
    ///
    /// Resolved per write rather than cached: `runtime.metrics` evolves its
    /// schema as new metric dimensions appear, so a key cached from the first
    /// write can outlive the column it names.
    fn key(
        &self,
        constraints: Option<&Constraints>,
        schema: &SchemaRef,
    ) -> Result<Vec<String>, String> {
        if self.configured_key.is_empty() {
            return declared_primary_key(constraints, schema).ok_or_else(|| {
                "it declares no primary key, so no stable Drasi element id can be derived. \
                Set 'runtime.drasi.tables[].key' to the columns that identify a row"
                    .to_string()
            });
        }

        let missing: Vec<&str> = self
            .configured_key
            .iter()
            .filter(|column| schema.field_with_name(column).is_err())
            .map(String::as_str)
            .collect();
        if missing.is_empty() {
            Ok(self.configured_key.clone())
        } else {
            Err(format!(
                "the configured key column(s) {} are not columns of the table. \
                Correct 'runtime.drasi.tables[].key'",
                missing.join(", ")
            ))
        }
    }

    /// Counts a dropped batch, logging sparsely.
    ///
    /// Every one of these conditions persists across writes — a table with no
    /// key never gains one, and an unreachable Drasi stays unreachable — so
    /// logging each occurrence would spam the log at telemetry write rate.
    fn record_drop(&self, table: &TableReference, reason: &str) {
        let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if dropped == 1 || dropped.is_multiple_of(1000) {
            tracing::warn!(
                "Not forwarding {table} to Drasi ({dropped} batch(es) dropped so far): {reason}"
            );
        }
    }
}

/// Node labels for a runtime table, defaulting to its qualified name.
fn labels_for(table: &RuntimeDrasiTable, qualified_name: &str) -> Vec<String> {
    if table.labels.is_empty() {
        vec![qualified_name.to_string()]
    } else {
        table.labels.clone()
    }
}

/// Reads the primary-key column names off a table's constraints.
fn declared_primary_key(
    constraints: Option<&Constraints>,
    schema: &SchemaRef,
) -> Option<Vec<String>> {
    let columns = datafusion_ddl::extract_primary_key_columns(constraints?, schema);

    // `extract_primary_key_columns` drops an index it cannot resolve, which
    // would silently shorten the key — and a shortened key is a wrong element
    // id, merging distinct rows onto one Drasi node. Take it only when every
    // index resolved.
    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

/// Drains one table's queue, delivering each batch in order.
async fn deliver(mut jobs: mpsc::Receiver<ForwardJob>, sink: Arc<DrasiSink>, table: String) {
    while let Some(job) = jobs.recv().await {
        let key: Vec<&str> = job.key.iter().map(String::as_str).collect();
        let rows = DrasiChangeRows {
            op_codes: vec![job.op_code; job.batch.num_rows()],
            primary_key_columns: vec![key; job.batch.num_rows()],
            data: &job.batch,
            // The runtime writes these as they happen, so arrival time is the
            // event time. Letting Drasi stamp it keeps one clock.
            source_commit_ts_ms: None,
        };

        if let Err(e) = sink.forward(&rows).await {
            tracing::warn!("Failed to forward {table} to Drasi: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Constraint;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("task", DataType::Utf8, false),
        ]))
    }

    fn spec(tables: Vec<RuntimeDrasiTable>) -> RuntimeDrasi {
        RuntimeDrasi {
            source_id: "spice-runtime".to_string(),
            tables,
            transport: spicepod::drasi::DrasiTransport::Http,
            params: Some(spicepod::param::Params::from_string_map(
                [(
                    "drasi_http_endpoint".to_string(),
                    "http://drasi:9000".to_string(),
                )]
                .into_iter()
                .collect(),
            )),
        }
    }

    fn table(name: &str) -> RuntimeDrasiTable {
        RuntimeDrasiTable {
            name: name.to_string(),
            key: vec![],
            labels: vec![],
        }
    }

    fn table_ref(name: &str) -> TableReference {
        TableReference::partial(SPICE_RUNTIME_SCHEMA, name)
    }

    /// Only the tables an operator names are forwarded — `write_data` also
    /// carries user DML to accelerated tables.
    #[tokio::test]
    async fn only_named_tables_are_forwarded() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")])).expect("builds");

        assert!(forwarders.by_table.contains_key(&table_ref("task_history")));
        assert!(!forwarders.by_table.contains_key(&table_ref("metrics")));
        assert!(!forwarders.by_table.contains_key(&TableReference::bare("orders")));
    }

    #[tokio::test]
    async fn no_configured_table_forwards_nothing() {
        let forwarders = InternalForwarders::try_new(&spec(vec![])).expect("builds");
        assert!(forwarders.by_table.is_empty());
    }

    #[test]
    fn declared_primary_key_is_read_from_constraints() {
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![1])]);
        assert_eq!(
            declared_primary_key(Some(&constraints), &schema()),
            Some(vec!["span_id".to_string()])
        );
    }

    #[test]
    fn composite_primary_key_preserves_column_order() {
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])]);
        assert_eq!(
            declared_primary_key(Some(&constraints), &schema()),
            Some(vec!["trace_id".to_string(), "span_id".to_string()])
        );
    }

    /// A `Unique` constraint is not an identity: two rows can differ on it over
    /// time, so it must not be mistaken for a key.
    #[test]
    fn unique_constraint_is_not_treated_as_a_key() {
        let constraints = Constraints::new_unverified(vec![Constraint::Unique(vec![0])]);
        assert_eq!(declared_primary_key(Some(&constraints), &schema()), None);
    }

    #[test]
    fn absent_constraints_yield_no_key() {
        assert_eq!(declared_primary_key(None, &schema()), None);
    }

    /// `runtime.metrics` declares no primary key. Forwarding it under a
    /// synthesized id would publish a duplicate node on every retry, so it is
    /// refused with an actionable message instead.
    #[tokio::test]
    async fn keyless_table_without_configured_key_is_refused() {
        let forwarders = InternalForwarders::try_new(&spec(vec![table("metrics")])).expect("builds");
        let forwarder = forwarders
            .by_table
            .get(&table_ref("metrics"))
            .expect("configured");

        let message = forwarder
            .key(None, &schema())
            .expect_err("a keyless table has no stable element id");
        assert!(message.contains("declares no primary key"), "{message}");
        assert!(message.contains("runtime.drasi.tables[].key"), "{message}");
    }

    #[tokio::test]
    async fn configured_key_overrides_the_declared_primary_key() {
        let mut metrics = table("metrics");
        metrics.key = vec!["trace_id".to_string()];
        let forwarders = InternalForwarders::try_new(&spec(vec![metrics])).expect("builds");
        let forwarder = forwarders
            .by_table
            .get(&table_ref("metrics"))
            .expect("configured");

        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![1])]);
        assert_eq!(
            forwarder
                .key(Some(&constraints), &schema())
                .expect("configured key wins"),
            vec!["trace_id".to_string()]
        );
    }

    /// A typo'd key column would otherwise surface as a per-row mapping failure
    /// on every write; catch it and name the fix.
    #[tokio::test]
    async fn configured_key_column_must_exist() {
        let mut metrics = table("metrics");
        metrics.key = vec!["nope".to_string()];
        let forwarders = InternalForwarders::try_new(&spec(vec![metrics])).expect("builds");
        let forwarder = forwarders
            .by_table
            .get(&table_ref("metrics"))
            .expect("configured");

        let message = forwarder
            .key(None, &schema())
            .expect_err("a key column that is not in the schema cannot key a row");
        assert!(message.contains("nope"), "{message}");
    }

    #[tokio::test]
    async fn missing_transport_param_is_reported_at_construction() {
        let mut spec = spec(vec![table("task_history")]);
        spec.params = None;

        let err = InternalForwarders::try_new(&spec)
            .expect_err("the http endpoint is required for the http transport");
        assert!(err.to_string().contains("drasi_http_endpoint"));
    }

    #[test]
    fn labels_default_to_the_qualified_table_name() {
        assert_eq!(
            labels_for(&table("task_history"), "runtime.task_history"),
            vec!["runtime.task_history".to_string()]
        );

        let mut labelled = table("task_history");
        labelled.labels = vec!["Task".to_string()];
        assert_eq!(
            labels_for(&labelled, "runtime.task_history"),
            vec!["Task".to_string()]
        );
    }

    /// A write to an unconfigured table must not queue anything, and must not
    /// panic on the missing entry.
    #[tokio::test]
    async fn an_unconfigured_table_is_ignored() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")])).expect("builds");

        forwarders.forward(
            &TableReference::bare("orders"),
            &UpdateType::Append,
            None,
            &schema(),
            &[],
        );
    }

    /// An overwrite cannot be expressed as element changes, so it is counted as
    /// a drop rather than forwarded as inserts.
    #[tokio::test]
    async fn overwrite_is_dropped_and_counted() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")])).expect("builds");

        forwarders.forward(
            &table_ref("task_history"),
            &UpdateType::Overwrite,
            None,
            &schema(),
            &[],
        );

        let forwarder = forwarders
            .by_table
            .get(&table_ref("task_history"))
            .expect("configured");
        assert_eq!(forwarder.dropped.load(Ordering::Relaxed), 1);
    }

    /// The writer must hand off and return; a full queue drops rather than
    /// applying backpressure to the runtime's telemetry writer.
    #[tokio::test]
    async fn a_full_queue_drops_rather_than_blocking_the_writer() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")])).expect("builds");
        let forwarder = forwarders
            .by_table
            .get(&table_ref("task_history"))
            .expect("configured");

        // Fill the queue without letting the delivery task drain it. The task
        // is parked on an unreachable endpoint, so nothing is consumed.
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["t"])),
                Arc::new(arrow::array::StringArray::from(vec!["s"])),
                Arc::new(arrow::array::StringArray::from(vec!["task"])),
            ],
        )
        .expect("valid batch");
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![1])]);

        for _ in 0..(QUEUE_DEPTH * 2) {
            forwarders.forward(
                &table_ref("task_history"),
                &UpdateType::Append,
                Some(&constraints),
                &schema(),
                std::slice::from_ref(&batch),
            );
        }

        assert!(
            forwarder.dropped.load(Ordering::Relaxed) > 0,
            "a full queue must drop and count rather than block"
        );
    }
}
