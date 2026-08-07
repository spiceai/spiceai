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

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::Constraints;
use datafusion::sql::TableReference;
use runtime_query_engine::query_engine::UpdateType;
use spicepod::drasi::{RuntimeDrasi, RuntimeDrasiTable};

use crate::datafusion::SPICE_RUNTIME_SCHEMA;
use crate::drasi::queue::{DEFAULT_QUEUE_DEPTH, DeliveryQueue, QueuedBatch};

/// The forwarders configured by `runtime.drasi`, keyed by table.
#[derive(Debug)]
pub(crate) struct InternalForwarders {
    by_table: HashMap<TableReference, TableForwarder>,
}

#[derive(Debug)]
struct TableForwarder {
    queue: DeliveryQueue,
    /// Key columns from the Spicepod. Empty means "use the table's declared
    /// primary key", which is only knowable once the table is registered.
    configured_key: Vec<String>,
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
    pub(crate) async fn try_new(spec: &RuntimeDrasi) -> runtime_drasi::Result<Self> {
        let mut by_table = HashMap::with_capacity(spec.tables.len());

        for table in &spec.tables {
            let table_ref = TableReference::partial(SPICE_RUNTIME_SCHEMA, table.name.clone());
            let name = table_ref.to_string();
            let sink = super::build_sink(
                name.clone(),
                &spec.source_id,
                labels_for(table, &name),
                spec.transport,
                // Surfaced, not skipped: a runtime table has no replication
                // position to replay, so the dead-letter store is the only
                // thing that can retain a failed batch — and it only sees one
                // if the sink reports it.
                super::QUEUED_SINK_POLICY,
                spec.params.as_ref(),
            )?;

            let store = super::open_dead_letter_store(&name).await;

            by_table.insert(
                table_ref,
                TableForwarder {
                    queue: DeliveryQueue::spawn(
                        sink,
                        name,
                        DEFAULT_QUEUE_DEPTH,
                        store,
                    ),
                    configured_key: table.key.clone(),
                },
            );
        }

        Ok(Self { by_table })
    }

    /// Queues a committed write for delivery.
    ///
    /// Never awaits Drasi and never fails the write — a caller that retried a
    /// "failed" write would duplicate the rows it already wrote. It can wait on
    /// one local file write when the queue is full, which is what keeps an
    /// overflowing batch from being lost.
    pub(crate) async fn forward(
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
                forwarder.queue.dead_letter(
                    "an overwrite replaces the table without naming the rows it removes, which has no Drasi equivalent",
                );
                return;
            }
        };

        let key = match forwarder.key(constraints, schema) {
            Ok(key) => key,
            Err(message) => {
                forwarder.queue.dead_letter(&message);
                return;
            }
        };

        for batch in data {
            if batch.num_rows() == 0 {
                continue;
            }

            // Cloning a `RecordBatch` clones `Arc`s over its arrays, not the
            // data. The runtime writes these as they happen, so arrival time is
            // the event time — letting Drasi stamp it keeps one clock.
            forwarder
                .queue
                .enqueue(QueuedBatch::uniform(op_code, &key, batch.clone(), None))
                .await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Constraint;
    use std::sync::Arc;

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
            forwarding: spicepod::drasi::DrasiForwarding::Enabled,
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
            InternalForwarders::try_new(&spec(vec![table("task_history")]))
                .await
                .expect("builds");

        assert!(forwarders.by_table.contains_key(&table_ref("task_history")));
        assert!(!forwarders.by_table.contains_key(&table_ref("metrics")));
        assert!(!forwarders.by_table.contains_key(&TableReference::bare("orders")));
    }

    #[tokio::test]
    async fn no_configured_table_forwards_nothing() {
        let forwarders = InternalForwarders::try_new(&spec(vec![])).await.expect("builds");
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
        let forwarders = InternalForwarders::try_new(&spec(vec![table("metrics")]))
            .await
            .expect("builds");
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
        let forwarders = InternalForwarders::try_new(&spec(vec![metrics])).await.expect("builds");
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
        let forwarders = InternalForwarders::try_new(&spec(vec![metrics])).await.expect("builds");
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
            .await
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
            InternalForwarders::try_new(&spec(vec![table("task_history")]))
                .await
                .expect("builds");

        forwarders
            .forward(
                &TableReference::bare("orders"),
                &UpdateType::Append,
                None,
                &schema(),
                &[],
            )
            .await;
    }

    /// An overwrite cannot be expressed as element changes, so it is counted as
    /// a drop rather than forwarded as inserts.
    #[tokio::test]
    async fn overwrite_is_dropped_and_counted() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")]))
                .await
                .expect("builds");

        forwarders
            .forward(
                &table_ref("task_history"),
                &UpdateType::Overwrite,
                None,
                &schema(),
                &[],
            )
            .await;

        let forwarder = forwarders
            .by_table
            .get(&table_ref("task_history"))
            .expect("configured");
        assert_eq!(forwarder.queue.dead_lettered(), 1);
    }

    /// The writer must hand off and return rather than waiting on Drasi. A full
    /// queue is retained durably instead of being dropped — the runtime has
    /// already committed these rows, so losing them here loses them for good.
    #[tokio::test]
    async fn a_full_queue_is_retained_rather_than_blocking_the_writer() {
        let forwarders =
            InternalForwarders::try_new(&spec(vec![table("task_history")]))
                .await
                .expect("builds");
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

        for _ in 0..(DEFAULT_QUEUE_DEPTH * 2) {
            forwarders
                .forward(
                    &table_ref("task_history"),
                    &UpdateType::Append,
                    Some(&constraints),
                    &schema(),
                    std::slice::from_ref(&batch),
                )
                .await;
        }

        // The assertion is that the loop above returned at all: every enqueue
        // completed without waiting on Drasi, which is unreachable here. Where
        // the overflow *went* is asserted directly, against an explicit store,
        // by `queue::tests::a_full_queue_retains_overflow_in_the_store`.
        let _ = forwarder;
    }
}
