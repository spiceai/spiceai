/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use opentelemetry::KeyValue;
use tokio::time::Instant;

use runtime_request_context::RequestContext;

use super::error_code::ErrorCode;
use super::single_line;
use runtime_metrics::query as metrics;

#[derive(Clone)]
pub(crate) struct QueryTracker {
    /// Whether this query's detail row is written to `runtime.task_history`.
    /// Metrics are recorded either way; when `false` the tracker skips only the
    /// work that table consumes: the events and the captured-output preview.
    pub(crate) task_history_enabled: bool,
    pub(crate) schema: Option<SchemaRef>,
    pub(crate) query_duration_secs: Option<f32>,
    pub(crate) query_execution_duration_secs: Option<f32>,
    pub(crate) rows_produced: u64,
    pub(crate) results_cache_hit: Option<bool>,
    pub(crate) is_accelerated: Option<bool>,
    pub(crate) error_message: Option<String>,
    pub(crate) error_code: Option<ErrorCode>,
    pub(crate) query_duration_timer: Instant,
    pub(crate) query_execution_duration_timer: Instant,
    pub(crate) datasets: Arc<HashSet<TableReference>>,
}

impl QueryTracker {
    pub fn finish_with_error(
        mut self,
        request_context: &RequestContext,
        error_message: String,
        error_code: ErrorCode,
    ) {
        // The failure is named by `finish` below, which collapses the message to
        // one record and picks the level from `error_code`. Naming it here too
        // would log a memory-pool breakdown across as many lines as it has
        // consumers, which a collector cannot group.
        self.error_message = Some(error_message);
        self.error_code = Some(error_code);
        self.finish(request_context, "");
    }

    pub fn finish(mut self, request_context: &RequestContext, captured_output: &str) {
        let query_duration = self.query_duration_timer.elapsed();
        let query_execution_duration = self.query_execution_duration_timer.elapsed();

        if self.query_duration_secs.is_none() {
            self.query_duration_secs = Some(query_duration.as_secs_f32());
        }

        if self.query_execution_duration_secs.is_none() {
            self.query_execution_duration_secs = Some(query_execution_duration.as_secs_f32());
        }

        let mut tags = vec![];
        match self.results_cache_hit {
            Some(true) => {
                tags.push("cache-hit");
            }
            Some(false) => {
                tags.push("cache-miss");
            }
            None => {}
        }

        if self.error_message.is_some() {
            tags.push("error");
        }

        // Build the datasets label once and reuse it for OTel metrics and
        // task-history logging. Sort so the same set of datasets always
        // produces the same joined string — HashSet iteration order would
        // otherwise split identical workloads across multiple telemetry series
        // (e.g. "a,b" vs "b,a").
        let mut dataset_names: Vec<String> =
            self.datasets.iter().map(ToString::to_string).collect();
        dataset_names.sort();
        let datasets_label = dataset_names.join(",");

        let mut labels = vec![
            KeyValue::new("tags", tags.join(",")),
            KeyValue::new("datasets", datasets_label.clone()),
        ];

        labels.extend(request_context.to_dimensions());

        // Record the execution count here (rather than at query submission) so it
        // shares the same `datasets`/`tags` dimensions as the duration metrics
        // below. `finish` is the single terminal step for every tracked query
        // (normal completion, cache hit, and error paths all route through it),
        // so this counts each execution exactly once.
        runtime_metrics::telemetry::track_query_count(&labels);
        runtime_metrics::telemetry::track_query_duration(query_duration, &labels);
        runtime_metrics::telemetry::track_query_execution_duration(
            query_execution_duration,
            &labels,
        );

        // Push per-table query latency DOWN into the Cayenne adaptive tuner (a
        // no-op for non-Cayenne tables — keyed by table name in a process-global
        // registry). Successful queries only: an errored query's duration is noise
        // for the latency/QPH goals. Total wall `query_duration` is the
        // operator-facing latency. A multi-table query attributes its full latency
        // to every participant — conservative, biasing toward more query-health
        // tuning (the safe direction).
        if self.error_message.is_none() {
            let latency_ms = query_duration.as_secs_f64() * 1000.0;
            let mut touched_cayenne = false;
            for ds in self.datasets.iter() {
                touched_cayenne |= cayenne::record_query_latency(ds.table(), latency_ms);
            }
            // QPH is system-wide: count each Cayenne-touching query exactly ONCE
            // (a join over several datasets is one unit of throughput, not N), so
            // record it globally outside the per-dataset loop. Skipped when the
            // query touched no Cayenne table — those queries can't move QPH that a
            // Cayenne controller could influence.
            if touched_cayenne {
                cayenne::record_global_query(latency_ms);
            }
        }

        if let Some(err) = &self.error_code {
            labels.push(KeyValue::new("err_code", err.to_string()));
            metrics::FAILURES.add(1, &labels);
        }

        // The failure record on the console.
        //
        // Deliberately outside the `task_history_enabled` gate below: the
        // `task_history` target reaches only the task-history table, so with the
        // table switched off a failed query left no record naming its trace id —
        // the very case that gate exists to survive. `finish` is the single
        // terminal step for every tracked query, so a failure is named here
        // exactly once, on every protocol, while the query's trace span is still
        // entered — which is what puts the id on the record.
        //
        // A refusal for want of memory is logged at `warn` and everything else at
        // `debug`. A malformed query is the caller's problem and would only be
        // noise, but a runtime refusing queries for want of memory is an outage
        // its operator cannot see any other way: `/health` is served by a
        // separate tokio runtime and stays green throughout.
        //
        // `single_line` is passed as a macro argument, not bound first, so a
        // multi-KB memory-pool breakdown is only re-collected when the level it
        // is being logged at is actually enabled.
        if let Some(error_message) = &self.error_message {
            match &self.error_code {
                Some(code @ ErrorCode::ResourcesExhausted) => {
                    tracing::warn!(
                        "Query refused, out of memory ({code}): {}",
                        single_line(error_message)
                    );
                }
                Some(code) => {
                    tracing::debug!("Query failed ({code}): {}", single_line(error_message));
                }
                None => tracing::debug!("Query failed: {}", single_line(error_message)),
            }
        }

        if self.task_history_enabled {
            trace_query(request_context, &self, captured_output, &datasets_label);
        }
    }

    #[must_use]
    pub(crate) fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub(crate) fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = rows_produced;
        self
    }

    #[must_use]
    pub(crate) fn results_cache_hit(mut self, cache_hit: bool) -> Self {
        self.results_cache_hit = Some(cache_hit);
        self
    }

    #[must_use]
    pub(crate) fn datasets(mut self, datasets: Arc<HashSet<TableReference>>) -> Self {
        self.datasets = datasets;
        self
    }
}

fn trace_query(
    request_context: &RequestContext,
    query_tracker: &QueryTracker,
    captured_output: &str,
    datasets_label: &str,
) {
    if let Some(error_code) = &query_tracker.error_code {
        tracing::info!(target: "task_history", error_code = %error_code, "labels");
    }

    if let Some(error_message) = &query_tracker.error_message {
        tracing::error!(target: "task_history", "{error_message}");
    }

    if let Some(query_execution_duration_secs) = &query_tracker.query_execution_duration_secs {
        tracing::info!(target: "task_history", query_execution_duration_ms = %query_execution_duration_secs * 1000.0, "labels");
    }

    tracing::info!(target: "task_history", rows_produced = %query_tracker.rows_produced, "labels");

    if query_tracker.results_cache_hit == Some(true) {
        tracing::info!(target: "task_history", results_cache_hit = true, "labels");
    }

    if matches!(&query_tracker.is_accelerated, Some(true)) {
        tracing::info!(target: "task_history", accelerated = true, "labels");
    }

    tracing::info!(target: "task_history", protocol = ?request_context.protocol(), datasets = datasets_label, "labels");
    tracing::info!(target: "task_history", captured_output = %captured_output);
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_request_context::{Protocol, RequestContextBuilder};
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt as _;

    /// Everything a probe subscriber wrote while a tracker finished with
    /// `error`, with the task-history table off — the configuration whose
    /// failures reach no table at all.
    fn tracker_finished_with(error: Option<(&str, ErrorCode)>) -> String {
        let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || WriteTo(Arc::clone(&writer))),
        );

        let request_context = RequestContextBuilder::new(Protocol::Internal).build();
        let tracker = QueryTracker {
            task_history_enabled: false,
            schema: None,
            query_duration_secs: None,
            query_execution_duration_secs: None,
            rows_produced: 0,
            results_cache_hit: None,
            is_accelerated: None,
            error_message: None,
            error_code: None,
            query_duration_timer: Instant::now(),
            query_execution_duration_timer: Instant::now(),
            datasets: Arc::new(HashSet::new()),
        };

        tracing::subscriber::with_default(subscriber, || match error {
            Some((message, code)) => {
                tracker.finish_with_error(&request_context, message.to_string(), code);
            }
            None => tracker.finish(&request_context, ""),
        });

        let captured = buffer.lock().expect("probe buffer poisoned").clone();
        String::from_utf8_lossy(&captured).into_owned()
    }

    /// `Arc<Mutex<Vec<u8>>>` is not itself a `MakeWriter`, so the closure above
    /// hands out this newtype instead.
    struct WriteTo(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for WriteTo {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("probe buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A refusal for want of memory is the runtime's own capacity condition and
    /// the only signal an operator gets, so it has to survive the default
    /// verbosity — and reach the log as one record, or a collector cannot group
    /// it. Everything else stays at `debug`: a malformed query is the caller's
    /// problem, and promoting every one would bury this.
    #[test]
    fn a_memory_refusal_is_named_at_warn_on_one_line() {
        let logged = tracker_finished_with(Some((
            "Resources exhausted: top consumers as:\n  HashJoinInput#12 consumed 1.0 GB\n\
             Error: Failed to allocate 256.0 MB",
            ErrorCode::ResourcesExhausted,
        )));

        let records: Vec<&str> = logged
            .lines()
            .filter(|line| line.contains("Query refused, out of memory"))
            .collect();
        assert_eq!(
            records.len(),
            1,
            "expected exactly one record, got: {logged}"
        );
        assert!(
            records[0].contains("WARN")
                && records[0].contains("HashJoinInput#12 consumed 1.0 GB")
                && records[0].contains("Failed to allocate 256.0 MB"),
            "the whole breakdown must ride on that one WARN record, got: {}",
            records[0]
        );
    }

    #[test]
    fn other_failures_stay_at_debug() {
        let logged = tracker_finished_with(Some((
            "Error during planning: table 'nope' not found",
            ErrorCode::QueryPlanningError,
        )));

        assert!(
            logged.contains("Query failed") && logged.contains("table 'nope' not found"),
            "the record must still name the failure, got: {logged}"
        );
        assert!(
            !logged.contains("Query refused, out of memory"),
            "an ordinary query failure is not a capacity condition, got: {logged}"
        );
    }

    /// The record exists to explain a failure, so a query that had none must
    /// not produce it.
    #[test]
    fn a_successful_query_names_no_failure() {
        let logged = tracker_finished_with(None);

        assert!(
            !logged.contains("Query failed") && !logged.contains("out of memory"),
            "a successful query must log no failure, got: {logged}"
        );
    }
}
