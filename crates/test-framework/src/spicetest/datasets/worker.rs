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
    collections::{BTreeMap, HashMap},
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
use tokio::task::JoinHandle;

use crate::constants::{HTTP_BASE_URL, SQL_ENDPOINT};

use crate::{
    metrics::QueryStatus,
    queries::{Query, validation, validation::QueryValidationResult},
    snapshot::record_explain_plan,
};

use super::EndCondition;

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
    /// Optional custom validation data for scenario queries
    validation_data: Option<HashMap<Arc<str>, Vec<RecordBatch>>>,
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
            validation_data: None,
        }
    }

    pub fn with_http_client(mut self, http_client: reqwest::Client) -> Self {
        self.http_client = Some(http_client);
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

    pub fn with_validate(mut self, validate: bool) -> Self {
        self.validate = validate;
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

    #[allow(clippy::too_many_lines)]
    pub fn start(self) -> JoinHandle<Result<SpiceTestQueryWorkerResult>> {
        tokio::spawn(async move {
            let query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>> = Arc::new(DashMap::new());

            // Keeps track of the start and end time of each query iteration
            let mut query_iteration_durations: BTreeMap<Arc<str>, (SystemTime, SystemTime)> =
                BTreeMap::new();

            let mut query_statuses: BTreeMap<Arc<str>, QueryStatus> = BTreeMap::new();
            let mut row_counts: BTreeMap<Arc<str>, Vec<usize>> = BTreeMap::new();
            let mut query_set_count = 0;
            let start = Instant::now();

            match self.end_condition {
                EndCondition::Duration(_) => {
                    // For Duration-based end condition, keep running queries in sequence
                    while !self.end_condition.is_met(&start, query_set_count) {
                        if self.progress_bar.is_none() && self.id == 0 {
                            println!(
                                "Worker {} - Query set count: {} - Elapsed time: {:?}",
                                self.id,
                                query_set_count,
                                start.elapsed()
                            );
                        }

                        if !self
                            .run_query_set(
                                Arc::clone(&query_durations),
                                &mut query_statuses,
                                &mut row_counts,
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
    ) -> Result<bool> {
        for query in &self.query_set {
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

    #[allow(clippy::too_many_lines)]
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
            .query_with_params(&query.sql, query.get_parameters_batch().transpose()?)
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
            // Validate the query results
            let validation_result = self.validate_query_results(query, &validation_records)?;
            if let QueryValidationResult::Fail(validation_reason) = validation_result {
                eprintln!(
                    "{} FAIL - Worker {} - Query '{}' validation failed: {validation_reason:?}",
                    chrono::Utc::now(),
                    self.id,
                    query.name
                );
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
            let sql_url = format!("{HTTP_BASE_URL}{SQL_ENDPOINT}");
            let http_response = http_client
                .post(&sql_url)
                .header("Accept", "application/vnd.spiceai.sql.v1+json")
                .body(query.sql.to_string())
                .send()
                .await?;

            if !http_response.status().is_success() {
                eprintln!(
                    "{} FAIL - Worker {} - Query '{}' HTTP request failed: {}",
                    chrono::Utc::now(),
                    self.id,
                    query.name,
                    http_response.status()
                );
                return Err(anyhow::anyhow!(
                    "Query HTTP request failed: {}",
                    http_response.status()
                ));
            }

            // Extract row count from response
            let response_text = http_response.text().await?;

            let duration = query_start.elapsed();

            if let Ok(response_json) = serde_json::from_str::<serde_json::Value>(&response_text) {
                if let Some(row_count) = response_json
                    .get("row_count")
                    .and_then(serde_json::Value::as_u64)
                {
                    #[allow(clippy::cast_possible_truncation)]
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

    async fn execute_query(
        &self,
        query: &Query,
        query_durations: Arc<DashMap<Arc<str>, Vec<Duration>>>,
        row_counts: &mut BTreeMap<Arc<str>, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<()> {
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
