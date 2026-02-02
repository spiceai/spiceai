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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    panic,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Result;
use arrow::array::RecordBatch;
use dashmap::DashMap;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use spiceai::{Client as SpiceClient, SpiceClientError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::constants::{HTTP_BASE_URL, QUERIES_ENDPOINT, SQL_ENDPOINT};
use crate::telemetry::streaming::QueryMetricEvent;

use crate::{
    metrics::QueryStatus,
    queries::{Query, validation, validation::QueryValidationResult},
    snapshot::record_explain_plan,
};

use super::EndCondition;

/// Maximum interval between status polls for distributed queries (caps exponential backoff)
const MAX_POLL_INTERVAL: Duration = Duration::from_millis(5000);
/// Maximum time to wait for a distributed query to complete (1 hour)
const POLL_TIMEOUT: Duration = Duration::from_secs(3600);

#[expect(clippy::struct_excessive_bools)]
pub(crate) struct SpiceTestQueryWorker {
    id: usize,
    query_set: Vec<Query>,
    end_condition: EndCondition,
    explain_plan_snapshot: bool,
    results_snapshot_predicate: Option<fn(&str) -> bool>,
    name: String,
    pub progress_bar: Option<ProgressBar>,
    validate: bool,
    scale_factor: f64,
    spice_client: Option<Arc<SpiceClient>>,
    http_client: Option<reqwest::Client>,
    /// Whether to use distributed query mode via /v1/queries API
    distributed_mode: bool,
    /// Optional custom validation data for scenario queries
    validation_data: Option<HashMap<Arc<str>, Vec<RecordBatch>>>,
    /// Optional reference schema for validating against known good tables
    reference_schema: Option<String>,
    /// Queries to skip row count validation for (e.g., queries that legitimately return 0 rows)
    skip_row_count_validation: HashSet<String>,
    /// Whether to validate row counts between HTTP and Flight endpoints, and check for zero rows
    validate_row_counts: bool,
    shutdown_token: CancellationToken,
    /// Optional sender for streaming query metrics to OTLP
    streaming_metrics_sender: Option<mpsc::Sender<QueryMetricEvent>>,
    /// Duration threshold - queries exceeding this are marked as failed in streaming metrics
    query_duration_threshold: Option<Duration>,
}

pub struct SpiceTestQueryWorkerResult {
    pub query_durations: BTreeMap<Arc<str>, Vec<Duration>>,
    pub query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)>,
    pub query_statuses: BTreeMap<Arc<str>, QueryStatus>,
    pub connection_failed: bool,
    pub row_counts: BTreeMap<Arc<str>, Vec<usize>>,
}

struct QueryRunResult {
    connection_failed: bool,
    query_failure: Option<String>,
}

impl SpiceTestQueryWorkerResult {
    pub fn new(
        query_durations: &Arc<DashMap<Arc<str>, Vec<Duration>>>,
        query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)>,
        query_statuses: BTreeMap<Arc<str>, QueryStatus>,
        connection_failed: bool,
        row_counts: BTreeMap<Arc<str>, Vec<usize>>,
    ) -> Self {
        let query_durations = query_durations
            .iter()
            .map(|mapref| (Arc::clone(mapref.key()), mapref.value().clone()))
            .collect();

        Self {
            query_durations,
            query_iteration_durations,
            query_statuses,
            connection_failed,
            row_counts,
        }
    }
}

impl SpiceTestQueryWorker {
    pub fn new(
        id: usize,
        query_set: Vec<Query>,
        end_condition: EndCondition,
        name: String,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            spice_client: None,
            explain_plan_snapshot: false,
            results_snapshot_predicate: None,
            name,
            progress_bar: None,
            validate: false,
            scale_factor: 1.0,
            http_client: None,
            distributed_mode: false,
            validation_data: None,
            reference_schema: None,
            skip_row_count_validation: default_row_count_validation_skip_queries(),
            validate_row_counts: true,
            shutdown_token: CancellationToken::new(),
            streaming_metrics_sender: None,
            query_duration_threshold: None,
        }
    }

    pub fn with_http_client(mut self, http_client: reqwest::Client) -> Self {
        self.http_client = Some(http_client);
        self
    }

    pub fn with_distributed_mode(mut self, distributed_mode: bool) -> Self {
        self.distributed_mode = distributed_mode;
        self
    }

    pub fn with_flight_client(mut self, spice_client: SpiceClient) -> Self {
        self.spice_client = Some(Arc::new(spice_client));
        self
    }

    pub fn with_scale_factor(mut self, scale_factor: f64) -> Self {
        self.scale_factor = scale_factor;
        self
    }

    pub fn with_shutdown_token(mut self, shutdown_token: CancellationToken) -> Self {
        self.shutdown_token = shutdown_token;
        self
    }

    pub fn with_validate(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    pub fn with_streaming_metrics(mut self, sender: mpsc::Sender<QueryMetricEvent>) -> Self {
        self.streaming_metrics_sender = Some(sender);
        self
    }

    pub fn with_query_duration_threshold(mut self, threshold: Duration) -> Self {
        self.query_duration_threshold = Some(threshold);
        self
    }

    pub fn with_explain_plan_snapshot(mut self, explain_plan_snapshot: bool) -> Self {
        self.explain_plan_snapshot = explain_plan_snapshot;
        self
    }

    pub fn with_results_snapshot(
        mut self,
        results_snapshot_predicate: Option<fn(&str) -> bool>,
    ) -> Self {
        self.results_snapshot_predicate = results_snapshot_predicate;
        self
    }

    pub fn with_progress_bar(mut self, progress_bar: ProgressBar) -> Self {
        self.progress_bar = Some(progress_bar);
        self
    }

    pub fn with_validation_data(
        mut self,
        validation_data: HashMap<Arc<str>, Vec<RecordBatch>>,
    ) -> Self {
        self.validation_data = Some(validation_data);
        self
    }

    pub fn with_reference_schema(mut self, reference_schema: Option<String>) -> Self {
        self.reference_schema = reference_schema;
        self
    }

    pub fn with_validate_row_counts(mut self, validate_row_counts: bool) -> Self {
        self.validate_row_counts = validate_row_counts;
        self
    }

    pub fn with_skip_row_count_validation(
        mut self,
        queries: impl IntoIterator<Item = String>,
    ) -> Self {
        self.skip_row_count_validation = queries.into_iter().collect();
        self
    }

    /// Send a query metric event to the streaming exporter if configured.
    /// If a duration threshold is set and the query exceeds it, it will be marked as a timeout failure.
    fn send_streaming_metric(&self, query_name: &str, duration: Duration, success: bool) {
        let Some(sender) = &self.streaming_metrics_sender else {
            return;
        };

        // Check if duration exceeds threshold - if so, mark as timeout failure
        let exceeded_threshold =
            success && self.query_duration_threshold.is_some_and(|t| duration > t);

        let event = if exceeded_threshold {
            QueryMetricEvent::with_failure(query_name.to_string(), duration, self.id, "timeout")
        } else if success {
            QueryMetricEvent::new(query_name.to_string(), duration, true, self.id)
        } else {
            QueryMetricEvent::with_failure(query_name.to_string(), duration, self.id, "error")
        };

        // Non-blocking send - if channel is full, we drop the metric
        let _ = sender.try_send(event);
    }

    /// Validate query results against expected data
    /// Uses TPCH validation for TPCH queries, custom validation data for scenario queries
    fn validate_query_results(
        &self,
        query: &Query,
        actual_batches: &[RecordBatch],
    ) -> Result<QueryValidationResult> {
        // Check if we have custom validation data for this query
        if let Some(validation_data) = &self.validation_data
            && let Some(expected_batches) = validation_data.get(&query.name)
        {
            return validation::validate_with_expected_batches(
                &query.name,
                actual_batches,
                expected_batches,
            );
        }

        // Fall back to TPCH validation (which handles TPCH, parameterized TPCH, etc.)
        validation::validate_tpch_query(query, actual_batches)
    }

    pub fn start(self) -> JoinHandle<Result<SpiceTestQueryWorkerResult>> {
        tokio::spawn(async move {
            // Load test queries may be generated with multiple parameter sets, resulting in a large
            // set of queries. To respect duration limits, we group queries by name and run one
            // group at a time, cycling through each group's parameter variations.
            // If queries are unique, it will result in a single query set and will be the same as usual
            let query_sets = build_unique_query_sets(&self.query_set)?;

            let query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>> = Arc::new(DashMap::new());

            // Keeps track of the start and end time of each query iteration
            let mut query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)> =
                BTreeMap::new();

            let mut query_statuses: BTreeMap<Arc<str>, QueryStatus> = BTreeMap::new();
            let mut row_counts: BTreeMap<Arc<str>, Vec<usize>> = BTreeMap::new();
            let mut query_set_count = 0;
            let start = Instant::now();

            match self.end_condition {
                EndCondition::Duration(_) | EndCondition::Unlimited => {
                    // For Duration-based or Unlimited end condition, keep running queries in sequence
                    while !self.shutdown_token.is_cancelled()
                        && !self.end_condition.is_met(&start, query_set_count)
                    {
                        if self.progress_bar.is_none() && self.id == 0 {
                            println!(
                                "Worker {} - Query set count: {} - Elapsed time: {:?}",
                                self.id,
                                query_set_count,
                                start.elapsed()
                            );
                        }

                        // Select the query set to use for this iteration
                        let queries_to_run = {
                            let set_index = query_set_count % query_sets.len();
                            &query_sets[set_index]
                        };

                        if !self
                            .run_query_set(
                                Arc::clone(&query_durations),
                                &mut query_statuses,
                                &mut row_counts,
                                queries_to_run,
                            )
                            .await?
                        {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                &query_durations,
                                query_iteration_durations,
                                query_statuses,
                                true,
                                row_counts,
                            ));
                        }
                        query_set_count += 1;
                    }
                }
                EndCondition::QuerySetCompleted(target_count) => {
                    // For QuerySetCompleted, run each query target_count times before moving to next
                    let start = SystemTime::now();
                    for query in &self.query_set {
                        if self.shutdown_token.is_cancelled() {
                            break;
                        }
                        if self.validate && query.name.contains("simple") {
                            continue; // skip validation for simple TPCH queries, because they are not part of the spec
                        }

                        let mut current_query_count = 0;
                        let query_start = SystemTime::now();
                        let mut query_status = QueryStatus::Passed;

                        let snapshot_results = self
                            .results_snapshot_predicate
                            .is_some_and(|predicate| predicate(&query.name))
                            && self.id == 0; // only one worker should snapshot results

                        // Additional round of query run before recording results.
                        // To discard the abnormal results caused by: establishing initial connection / spark cluster startup time

                        let QueryRunResult {
                            connection_failed, ..
                        } = self
                            .run_single_query(
                                query,
                                Arc::new(DashMap::new()),
                                &mut BTreeMap::new(),
                                snapshot_results,
                                false,
                            )
                            .await?;
                        if connection_failed {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                &query_durations,
                                query_iteration_durations,
                                query_statuses,
                                true,
                                row_counts,
                            ));
                        }

                        if self.explain_plan_snapshot
                            && self.id == 0
                            && let Some(client) = &self.spice_client
                        {
                            println!("Worker {} - Query '{}' - Explain plan", self.id, query.name);
                            if let Err(e) = record_explain_plan(
                                Arc::clone(client),
                                self.name.as_str(),
                                query,
                                self.scale_factor,
                            )
                            .await
                            {
                                println!(
                                    "Worker {} - Query '{}' explain plan failed: {}",
                                    self.id, query.name, e
                                );

                                query_status = QueryStatus::Failed(Some(
                                    "Explain plan snapshot assertion failed".into(),
                                ));
                            }
                        }

                        while current_query_count < target_count {
                            if self.progress_bar.is_none()
                                && self.id == 0
                                && (current_query_count % 10 == 0 || target_count <= 5)
                            {
                                println!(
                                    "Worker {} - Query '{}' - {}/{} - Elapsed time: {:?}",
                                    self.id,
                                    query.name,
                                    current_query_count + 1,
                                    target_count,
                                    start.elapsed().unwrap_or_default()
                                );
                            }

                            let QueryRunResult {
                                connection_failed,
                                query_failure,
                            } = self
                                .run_single_query(
                                    query,
                                    Arc::clone(&query_durations),
                                    &mut row_counts,
                                    false, // don't attempt to snapshot results more than once
                                    self.validate,
                                )
                                .await?;

                            if connection_failed {
                                return Ok(SpiceTestQueryWorkerResult::new(
                                    &query_durations,
                                    query_iteration_durations,
                                    query_statuses,
                                    true,
                                    row_counts,
                                ));
                            }

                            if let Some(query_failure) = query_failure {
                                query_status = QueryStatus::Failed(Some(query_failure.into()));
                            }

                            current_query_count += 1;
                        }
                        let end = SystemTime::now();
                        query_iteration_durations
                            .insert(Arc::clone(&query.name), (query_start, end));
                        query_statuses.insert(Arc::clone(&query.name), query_status);
                    }
                }
            }

            Ok(SpiceTestQueryWorkerResult::new(
                &query_durations,
                query_iteration_durations,
                query_statuses,
                false,
                row_counts,
            ))
        })
    }

    // run queries as a duration-based test
    async fn run_query_set(
        &self,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        query_statuses: &mut BTreeMap<Arc<str>, QueryStatus>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        queries: &[Query],
    ) -> Result<bool> {
        for query in queries {
            let QueryRunResult {
                connection_failed,
                query_failure,
            } = self
                .run_single_query(
                    query,
                    Arc::clone(&query_durations),
                    row_counts,
                    false,
                    false,
                )
                .await?;
            if connection_failed {
                return Ok(false);
            }

            let worker_status = if let Some(query_failure) = query_failure {
                QueryStatus::Failed(Some(query_failure.into()))
            } else {
                QueryStatus::Passed
            };

            query_statuses
                .entry(Arc::clone(&query.name))
                .and_modify(|existing_status| {
                    // If the worker reports failure, update the status to Failed
                    if matches!(worker_status, QueryStatus::Failed(_)) {
                        *existing_status = worker_status.clone();
                    }
                })
                .or_insert(worker_status);
        }
        Ok(true)
    }

    // run queries as a set-completion based test
    async fn run_single_query(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<QueryRunResult> {
        match self
            .execute_query(
                query,
                Arc::clone(&query_durations),
                row_counts,
                results_snapshot,
                validate,
            )
            .await
        {
            Ok(()) => Ok(QueryRunResult {
                connection_failed: false,
                query_failure: None,
            }),
            Err(e) => {
                let flight_error = e.downcast_ref::<flight_client::Error>();
                if let Some(
                    flight_client::Error::UnableToConnectToServer { .. }
                    | flight_client::Error::UnableToPerformHandshake { .. },
                ) = flight_error
                {
                    eprintln!(
                        "FAIL - EARLY EXIT - Worker {} - Query '{}' failed: {}",
                        self.id, query.name, e
                    );
                    Ok(QueryRunResult {
                        connection_failed: true,
                        query_failure: None,
                    })
                } else {
                    eprintln!(
                        "{} FAIL - Worker {} - Query '{}' failed: {}",
                        chrono::Utc::now(),
                        self.id,
                        query.name,
                        e
                    );

                    query_durations.entry(Arc::clone(&query.name)).or_default();
                    Ok(QueryRunResult {
                        connection_failed: false,
                        query_failure: Some(format!("{e}")),
                    })
                }
            }
        }
    }

    async fn execute_flight(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<()> {
        let Some(spice_client) = self.spice_client.as_ref() else {
            return Ok(());
        };

        let query_start = Instant::now();

        let mut result_stream = spice_client
            .sql_with_params(&query.sql, query.get_parameters_batch().transpose()?)
            .await?;

        let mut row_count: usize = 0;
        let mut limited_records = vec![];
        let mut validation_records = vec![];

        loop {
            let batch = result_stream.try_next().await;
            match batch {
                Ok(None) => break,
                Err(e) => {
                    if let SpiceClientError::ConnectionReset { .. } = e {
                        row_count = 0;
                        limited_records.clear();
                        validation_records.clear();
                    } else {
                        let duration = query_start.elapsed();
                        // Send streaming metric for failed Flight query
                        self.send_streaming_metric(&query.name, duration, false);
                        eprintln!(
                            "{} FAIL - Worker {} - Query '{}' failed: {}",
                            chrono::Utc::now(),
                            self.id,
                            query.name,
                            e
                        );

                        query_durations.entry(Arc::clone(&query.name)).or_default();
                        return Err(e.into());
                    }
                }
                Ok(Some(batch)) => {
                    if validate {
                        validation_records.push(batch.clone());
                    }
                    if batch.num_rows() == 0 {
                        println!(
                            "Worker {} - Query '{}' returned 0 rows",
                            self.id, query.name
                        );
                    }
                    row_count += batch.num_rows();
                    if limited_records.len() < 10 {
                        let required_rows = 10 - limited_records.len();
                        let end = if batch.num_rows() > required_rows {
                            required_rows
                        } else {
                            batch.num_rows()
                        };
                        for i in 0..end {
                            limited_records.push(batch.slice(i, 1));
                        }
                    }
                }
            }
        }

        if validate {
            // Execute reference query if reference_schema is provided
            let reference_batches = if let Some(ref_schema) = &self.reference_schema {
                let reference_query = query.rewrite_with_reference_schema(ref_schema)?;
                println!(
                    "Worker {} - Query '{}' - Executing reference query against {}.* tables",
                    self.id, query.name, ref_schema
                );

                let mut ref_result_stream = spice_client
                    .sql_with_params(
                        &reference_query.sql,
                        reference_query.get_parameters_batch().transpose()?,
                    )
                    .await?;

                let mut ref_batches = vec![];
                while let Some(batch) = ref_result_stream.try_next().await? {
                    ref_batches.push(batch);
                }
                Some(ref_batches)
            } else {
                None
            };

            // Validate against reference query results if available
            if let Some(ref_batches) = reference_batches {
                let validation_result = validation::validate_with_expected_batches(
                    &query.name,
                    &validation_records,
                    &ref_batches,
                )?;

                if let QueryValidationResult::Fail(validation_reason) = validation_result {
                    eprintln!(
                        "\n{} FAIL - Worker {} - Query '{}' reference validation failed",
                        chrono::Utc::now(),
                        self.id,
                        query.name
                    );
                    eprintln!("Query SQL: {}", query.sql);
                    eprintln!("Validation failure reason: {validation_reason:?}");
                    eprintln!("\nExpected results (from reference schema):");
                    match arrow::util::pretty::pretty_format_batches(&ref_batches) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format expected batches: {e}"),
                    }
                    eprintln!("\nActual results:");
                    match arrow::util::pretty::pretty_format_batches(&validation_records) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format actual batches: {e}"),
                    }
                    eprintln!();
                    return Err(anyhow::anyhow!(
                        "Query reference validation failed: {validation_reason:?}"
                    ));
                }
            }

            // Also validate using existing validation logic (TPCH or custom validation data)
            let validation_result = self.validate_query_results(query, &validation_records)?;

            if let QueryValidationResult::Fail(validation_reason) = validation_result {
                eprintln!(
                    "\n{} FAIL - Worker {} - Query '{}' validation failed",
                    chrono::Utc::now(),
                    self.id,
                    query.name
                );
                eprintln!("Query SQL: {}", query.sql);
                eprintln!("Validation failure reason: {validation_reason:?}");

                // Print expected results based on validation source
                if let Some(validation_data) = &self.validation_data
                    && let Some(expected_batches) = validation_data.get(&query.name)
                {
                    eprintln!("\nExpected results (from custom validation data):");
                    match arrow::util::pretty::pretty_format_batches(expected_batches) {
                        Ok(pretty) => eprintln!("{pretty}"),
                        Err(e) => eprintln!("Failed to format expected batches: {e}"),
                    }
                } else {
                    eprintln!(
                        "\nExpected results: See TPCH specification for query {}",
                        query.name
                    );
                }

                eprintln!("\nActual results:");
                match arrow::util::pretty::pretty_format_batches(&validation_records) {
                    Ok(pretty) => eprintln!("{pretty}"),
                    Err(e) => eprintln!("Failed to format actual batches: {e}"),
                }
                eprintln!();

                return Err(anyhow::anyhow!(
                    "Query validation failed: {validation_reason:?}"
                ));
            }
        }

        if results_snapshot {
            let query_name = Arc::clone(&query.name);
            let name = self.name.clone();
            let snapshot_name = if (self.scale_factor - 1.0).abs() < f64::EPSILON {
                format!("{name}_{query_name}")
            } else {
                format!("{name}_{query_name}_sf{}", self.scale_factor)
            };

            let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)?;
            let result = panic::catch_unwind(|| {
                insta::with_settings!({
                     description => format!("Query: {query_name}"),
                                         omit_expression => true,
                    snapshot_path => "../../snapshot/snapshots/results"
                }, {
                    insta::assert_snapshot!(snapshot_name, records_pretty);
                });
            });
            if result.is_err() {
                let error_str = format!("Query `{name}` `{query_name}` snapshot assertion failed",);
                eprintln!("{error_str}");
                return Err(anyhow::anyhow!(error_str));
            }
        }

        let duration = query_start.elapsed();

        // Send streaming metric for real-time OTLP export
        self.send_streaming_metric(&query.name, duration, true);

        query_durations
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(duration);

        row_counts
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(row_count);

        if let Some(pb) = self.progress_bar.as_ref() {
            pb.inc(1);
        }
        Ok(())
    }

    async fn execute_http(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        http_row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
    ) -> Result<()> {
        if let Some(http_client) = self.http_client.as_ref() {
            let query_start = Instant::now();
            let sql_text = query.to_sql_with_inlined_params();
            let sql_url = format!("{HTTP_BASE_URL}{SQL_ENDPOINT}");
            let http_response = http_client
                .post(&sql_url)
                .header("Accept", "application/vnd.spiceai.sql.v1+json")
                .body(sql_text.to_string())
                .send()
                .await?;

            let status = http_response.status();
            let response_text = http_response.text().await.unwrap_or_default();

            if !status.is_success() {
                eprintln!(
                    "{} FAIL - Worker {} - Query '{}' HTTP request failed: {status} - {response_text}",
                    chrono::Utc::now(),
                    self.id,
                    query.name,
                );
                return Err(anyhow::anyhow!("Query HTTP request failed: {status}",));
            }

            let duration = query_start.elapsed();

            if let Ok(response_json) = serde_json::from_str::<serde_json::Value>(&response_text) {
                if let Some(row_count) = response_json
                    .get("row_count")
                    .and_then(serde_json::Value::as_u64)
                {
                    #[expect(clippy::cast_possible_truncation)]
                    let row_count_usize = row_count as usize;
                    http_row_counts
                        .entry(Arc::clone(&query.name))
                        .or_default()
                        .push(row_count_usize);
                } else {
                    eprintln!(
                        "Warning: No row_count field in HTTP response for query '{}'",
                        query.name
                    );
                }
            } else {
                eprintln!(
                    "Warning: Failed to parse HTTP response as JSON for query '{}'",
                    query.name
                );
            }

            query_durations
                .entry(Arc::clone(&query.name))
                .or_default()
                .push(duration);
        }

        Ok(())
    }

    /// Execute a query using the distributed query API (`/v1/queries`).
    ///
    /// This method:
    /// 1. Submits the query via `POST /v1/queries`
    /// 2. Polls `/v1/queries/{query_id}/status` until completion
    /// 3. Fetches results from `/v1/queries/{query_id}/results`
    ///
    /// The distributed query API is only available when spiced is running in cluster mode
    /// with the scheduler role.
    async fn execute_distributed(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
    ) -> Result<()> {
        let Some(http_client) = self.http_client.as_ref() else {
            return Err(anyhow::anyhow!(
                "Failed to execute distributed query '{}': HTTP client is not configured. Ensure distributed mode is only enabled when an HTTP client is available",
                query.name
            ));
        };

        let query_start = Instant::now();
        let sql_text = query.to_sql_with_inlined_params();
        let queries_url = format!("{HTTP_BASE_URL}{QUERIES_ENDPOINT}");

        // Step 1: Submit the query
        let submit_body = serde_json::json!({
            "sql": sql_text,
        });

        let submit_response = http_client
            .post(&queries_url)
            .header("Content-Type", "application/json")
            .json(&submit_body)
            .send()
            .await?;

        let submit_status = submit_response.status();
        if !submit_status.is_success() {
            let error_text = submit_response.text().await.unwrap_or_default();
            let duration = query_start.elapsed();
            self.send_streaming_metric(&query.name, duration, false);
            eprintln!(
                "{} FAIL - Worker {} - Query '{}' distributed submit failed: {submit_status} - {error_text}",
                chrono::Utc::now(),
                self.id,
                query.name,
            );
            return Err(anyhow::anyhow!(
                "Query distributed submit failed: {submit_status}"
            ));
        }

        let submit_json: serde_json::Value = submit_response.json().await?;
        let query_id = submit_json
            .get("query_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("No query_id in submit response"))?;

        // Step 2: Poll for completion
        let status_url = format!("{queries_url}/{query_id}/status");
        let mut poll_interval = Duration::from_millis(100);

        let poll_start = Instant::now();
        loop {
            if poll_start.elapsed() > POLL_TIMEOUT {
                let duration = query_start.elapsed();
                self.send_streaming_metric(&query.name, duration, false);
                return Err(anyhow::anyhow!(
                    "Query '{}' timed out waiting for distributed execution",
                    query.name
                ));
            }

            let status_response = http_client.get(&status_url).send().await?;

            if !status_response.status().is_success() {
                let error_text = status_response.text().await.unwrap_or_default();
                let duration = query_start.elapsed();
                self.send_streaming_metric(&query.name, duration, false);
                return Err(anyhow::anyhow!(
                    "Query '{}' status check failed: {error_text}",
                    query.name
                ));
            }

            let status_json: serde_json::Value = status_response.json().await?;
            let state = status_json
                .get("state")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");

            match state {
                "succeeded" => break,
                "failed" => {
                    let error_msg = status_json
                        .get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("Unknown error");
                    let duration = query_start.elapsed();
                    self.send_streaming_metric(&query.name, duration, false);
                    eprintln!(
                        "{} FAIL - Worker {} - Query '{}' distributed execution failed: {error_msg}",
                        chrono::Utc::now(),
                        self.id,
                        query.name,
                    );
                    return Err(anyhow::anyhow!(
                        "Query distributed execution failed: {error_msg}"
                    ));
                }
                "cancelled" => {
                    let duration = query_start.elapsed();
                    self.send_streaming_metric(&query.name, duration, false);
                    return Err(anyhow::anyhow!("Query '{}' was cancelled", query.name));
                }
                "closed" => {
                    let duration = query_start.elapsed();
                    self.send_streaming_metric(&query.name, duration, false);
                    return Err(anyhow::anyhow!(
                        "Query '{}' results expired before retrieval",
                        query.name
                    ));
                }
                "pending" | "running" => {
                    // Continue polling with exponential backoff
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = std::cmp::min(poll_interval * 2, MAX_POLL_INTERVAL);
                }
                _ => {
                    // Unknown state, continue polling
                    tokio::time::sleep(poll_interval).await;
                    poll_interval = std::cmp::min(poll_interval * 2, MAX_POLL_INTERVAL);
                }
            }
        }

        // Step 3: Fetch results (first chunk to get row count)
        let results_url = format!("{queries_url}/{query_id}/results");
        let results_response = http_client.get(&results_url).send().await?;

        let results_status = results_response.status();
        if !results_status.is_success() {
            let error_text = results_response.text().await.unwrap_or_default();
            let duration = query_start.elapsed();
            self.send_streaming_metric(&query.name, duration, false);
            return Err(anyhow::anyhow!(
                "Query '{}' results fetch failed: {results_status} - {error_text}",
                query.name
            ));
        }

        let results_json: serde_json::Value = results_response.json().await?;

        // Get total row count from manifest; treat missing or invalid values as errors
        let manifest = results_json.get("manifest").ok_or_else(|| {
            anyhow::anyhow!(
                "Query '{}' results response missing 'manifest' field",
                query.name
            )
        })?;

        let total_row_count_value = manifest.get("total_row_count").ok_or_else(|| {
            anyhow::anyhow!(
                "Query '{}' results manifest missing 'total_row_count' field",
                query.name
            )
        })?;

        let total_row_count_u64 = total_row_count_value.as_u64().ok_or_else(|| {
            anyhow::anyhow!(
                "Query '{}' results manifest 'total_row_count' field is not a valid u64",
                query.name
            )
        })?;

        #[expect(clippy::cast_possible_truncation)]
        let row_count = total_row_count_u64 as usize;

        let duration = query_start.elapsed();

        // Send streaming metric for successful distributed query
        self.send_streaming_metric(&query.name, duration, true);

        query_durations
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(duration);

        row_counts
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(row_count);

        if let Some(pb) = self.progress_bar.as_ref() {
            pb.inc(1);
        }

        Ok(())
    }

    async fn execute_query(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<()> {
        // Use distributed mode if enabled
        if self.distributed_mode {
            return self
                .execute_distributed(query, query_durations, row_counts)
                .await;
        }

        let mut http_row_counts: BTreeMap<Arc<str>, Vec<usize>> = BTreeMap::new();

        futures::future::try_join(
            self.execute_flight(
                query,
                Arc::clone(&query_durations),
                row_counts,
                results_snapshot,
                validate,
            ),
            self.execute_http(query, Arc::clone(&query_durations), &mut http_row_counts),
        )
        .await?;

        // Skip row count validation if disabled or for specific queries that legitimately return 0 rows
        if !self.validate_row_counts
            || self
                .skip_row_count_validation
                .contains(&query.name.to_string())
        {
            return Ok(());
        }

        // Validate row counts if both HTTP and Flight are available
        if let Some(http_counts) = http_row_counts.get(&query.name) {
            if let Some(flight_counts) = row_counts.get(&query.name) {
                // Compare the last row count from each
                if let (Some(&http_count), Some(&flight_count)) =
                    (http_counts.last(), flight_counts.last())
                {
                    // Check for zero row counts (indicates potential query execution issue)
                    if http_count == 0 && flight_count == 0 {
                        eprintln!(
                            "{} FAIL - Worker {} - Query '{}' returned 0 rows in both HTTP and Flight",
                            chrono::Utc::now(),
                            self.id,
                            query.name
                        );
                        return Err(anyhow::anyhow!(
                            "Worker {} - Query '{}' returned 0 rows in both HTTP and Flight",
                            self.id,
                            query.name
                        ));
                    }

                    // Check if row counts match
                    if http_count != flight_count {
                        eprintln!(
                            "{} FAIL - Worker {} - Query '{}' row count mismatch: HTTP={}, Flight={}",
                            chrono::Utc::now(),
                            self.id,
                            query.name,
                            http_count,
                            flight_count
                        );
                        return Err(anyhow::anyhow!(
                            "Worker {} - Query '{}' row count mismatch between HTTP ({}) and Flight ({})",
                            self.id,
                            query.name,
                            http_count,
                            flight_count
                        ));
                    }
                }
            }
        } else if let Some(flight_counts) = row_counts.get(&query.name) {
            // Only Flight available, check for zero rows
            if let Some(&flight_count) = flight_counts.last()
                && flight_count == 0
            {
                eprintln!(
                    "{} FAIL - Worker {} - Query '{}' returned 0 rows via Flight",
                    chrono::Utc::now(),
                    self.id,
                    query.name
                );
                return Err(anyhow::anyhow!(
                    "Worker {} - Query '{}' returned 0 rows via Flight",
                    self.id,
                    query.name
                ));
            }
        }

        Ok(())
    }
}

fn default_row_count_validation_skip_queries() -> HashSet<String> {
    [
        "tpcds_q8",
        "tpcds_q29",
        "tpcds_q37",
        "tpcds_q41",
        "tpcds_q44",
        "tpcds_q54",
        "tpcds_q58",
    ]
    .iter()
    .map(std::string::ToString::to_string)
    .collect()
}

/// Build unique query sets by grouping queries by parameter index.
/// Creates one query set per parameter variation, where each set contains
/// one query of each type with the same parameter index.
fn build_unique_query_sets(queries: &[Query]) -> Result<Vec<Vec<Query>>> {
    use std::collections::HashMap;

    // Group queries by name first
    let mut groups: HashMap<Arc<str>, Vec<&Query>> = HashMap::new();
    for query in queries {
        groups
            .entry(Arc::clone(&query.name))
            .or_default()
            .push(query);
    }

    // Validate that all groups have the same size
    let mut expected_size = None;
    for (name, query_group) in &groups {
        let group_size = query_group.len();
        match expected_size {
            None => expected_size = Some(group_size),
            Some(expected) if expected != group_size => {
                return Err(anyhow::anyhow!(
                    "Uneven parameter groups detected: query '{name}' has {group_size} parameters, expected {expected}"
                ));
            }
            _ => {}
        }
    }

    let num_variations = expected_size.unwrap_or(0);

    // Create query sets by parameter index
    let mut result = Vec::with_capacity(num_variations);

    for param_index in 0..num_variations {
        let mut query_set = Vec::with_capacity(groups.len());

        for query_group in groups.values() {
            if let Some(query) = query_group.get(param_index) {
                query_set.push((*query).clone());
            }
        }

        result.push(query_set);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::queries::parameterized::ParameterValue;

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_build_unique_query_sets_single_group() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("1".into())]),
            },
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("2".into())]),
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(
            result.len(),
            2,
            "Should have two query sets (one per parameter)"
        );
        assert_eq!(result[0].len(), 1, "Each set should have one query");
        assert_eq!(result[1].len(), 1, "Each set should have one query");
    }

    #[test]
    fn test_build_unique_query_sets_multiple_groups() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1 WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("1".into())]),
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2 WHERE id = ?"),
                overridden: false,
                parameters: Some(vec![ParameterValue::String("2".into())]),
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(
            result.len(),
            2,
            "Should have two query sets (one per parameter)"
        );
        for group in &result {
            assert_eq!(
                group.len(),
                2,
                "Each set should have two queries (one per query type)"
            );
        }

        // Verify each set contains one query of each type
        let set1_names: Vec<&str> = result[0].iter().map(|q| q.name.as_ref()).collect();
        let set2_names: Vec<&str> = result[1].iter().map(|q| q.name.as_ref()).collect();
        assert!(set1_names.contains(&"query1") && set1_names.contains(&"query2"));
        assert!(set2_names.contains(&"query1") && set2_names.contains(&"query2"));
    }

    #[test]
    fn test_build_unique_query_sets_unique_names() {
        let queries = vec![
            Query {
                name: Arc::from("query1"),
                sql: Arc::from("SELECT * FROM table1"),
                overridden: false,
                parameters: None,
            },
            Query {
                name: Arc::from("query2"),
                sql: Arc::from("SELECT * FROM table2"),
                overridden: false,
                parameters: None,
            },
        ];

        let result = build_unique_query_sets(&queries).expect("Should succeed");

        assert_eq!(result.len(), 1, "Should have one query set");
        assert_eq!(result[0].len(), 2, "Set should have both queries");

        // Verify we have both query names in the single set
        let names: Vec<&str> = result[0].iter().map(|q| q.name.as_ref()).collect();
        assert!(names.contains(&"query1") && names.contains(&"query2"));
    }
}
