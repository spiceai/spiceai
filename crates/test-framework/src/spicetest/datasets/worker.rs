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
    collections::BTreeMap,
    panic,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Result;
use flight_client::FlightClient;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use tokio::task::JoinHandle;

use crate::{
    metrics::QueryStatus,
    queries::{
        validation::{self, QueryValidationResult},
        Query,
    },
    snapshot::record_explain_plan,
};

use super::EndCondition;

pub(crate) struct SpiceTestQueryWorker {
    id: usize,
    query_set: Vec<Query>,
    end_condition: EndCondition,
    flight_client: FlightClient,
    explain_plan_snapshot: bool,
    results_snapshot_predicate: Option<fn(&str) -> bool>,
    name: String,
    pub progress_bar: Option<ProgressBar>,
    validate: bool,
}

pub struct SpiceTestQueryWorkerResult {
    pub query_durations: BTreeMap<String, Vec<Duration>>,
    pub query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)>,
    pub query_statuses: BTreeMap<String, QueryStatus>,
    pub connection_failed: bool,
    pub row_counts: BTreeMap<String, Vec<usize>>,
}

impl SpiceTestQueryWorkerResult {
    pub fn new(
        query_durations: BTreeMap<String, Vec<Duration>>,
        query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)>,
        query_statuses: BTreeMap<String, QueryStatus>,
        connection_failed: bool,
        row_counts: BTreeMap<String, Vec<usize>>,
    ) -> Self {
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
        flight_client: FlightClient,
        name: String,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            flight_client,
            explain_plan_snapshot: false,
            results_snapshot_predicate: None,
            name,
            progress_bar: None,
            validate: false,
        }
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

    #[allow(clippy::too_many_lines)]
    pub fn start(self) -> JoinHandle<Result<SpiceTestQueryWorkerResult>> {
        tokio::spawn(async move {
            let mut query_durations: BTreeMap<String, Vec<Duration>> = BTreeMap::new();

            // Keeps track of the start and end time of each query iteration
            let mut query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)> =
                BTreeMap::new();

            let mut query_statuses: BTreeMap<String, QueryStatus> = BTreeMap::new();
            let mut row_counts: BTreeMap<String, Vec<usize>> = BTreeMap::new();
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
                                &mut query_durations,
                                &mut query_statuses,
                                &mut row_counts,
                            )
                            .await?
                        {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                query_durations,
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

                        println!("Worker {} - Query '{}' - Pre-run", self.id, query.name);
                        let (connection_succeed, _) = self
                            .run_single_query(
                                query,
                                &mut BTreeMap::new(),
                                &mut BTreeMap::new(),
                                snapshot_results,
                                false,
                            )
                            .await?;
                        if !connection_succeed {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                query_durations,
                                query_iteration_durations,
                                query_statuses,
                                true,
                                row_counts,
                            ));
                        }

                        if self.explain_plan_snapshot && self.id == 0 {
                            println!("Worker {} - Query '{}' - Explain plan", self.id, query.name);
                            if let Err(e) = record_explain_plan(
                                &self.flight_client,
                                self.name.as_str(),
                                &query.name,
                                &query.sql,
                            )
                            .await
                            {
                                println!(
                                    "Worker {} - Query '{}' explain plan failed: {}",
                                    self.id, query.name, e
                                );

                                query_status = QueryStatus::Failed;
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

                            let (connection_succeed, query_succeed) = self
                                .run_single_query(
                                    query,
                                    &mut query_durations,
                                    &mut row_counts,
                                    false, // don't attempt to snapshot results more than once
                                    self.validate,
                                )
                                .await?;

                            if !connection_succeed {
                                return Ok(SpiceTestQueryWorkerResult::new(
                                    query_durations,
                                    query_iteration_durations,
                                    query_statuses,
                                    true,
                                    row_counts,
                                ));
                            }

                            if !query_succeed {
                                query_status = QueryStatus::Failed;
                            }

                            current_query_count += 1;
                        }
                        let end = SystemTime::now();
                        query_iteration_durations
                            .insert(query.name.to_string(), (query_start, end));
                        query_statuses.insert(query.name.to_string(), query_status);
                    }
                }
            }

            Ok(SpiceTestQueryWorkerResult::new(
                query_durations,
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
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        query_statuses: &mut BTreeMap<String, QueryStatus>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
    ) -> Result<bool> {
        for query in &self.query_set {
            let (connection_succeed, query_succeed) = self
                .run_single_query(query, query_durations, row_counts, false, false)
                .await?;
            if !connection_succeed {
                return Ok(false);
            }

            let worker_status = if query_succeed {
                QueryStatus::Passed
            } else {
                QueryStatus::Failed
            };

            query_statuses
                .entry(query.name.to_string())
                .and_modify(|existing_status| {
                    // If the worker reports failure, update the status to Failed
                    if worker_status == QueryStatus::Failed {
                        *existing_status = QueryStatus::Failed;
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
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<(bool, bool)> {
        match self
            .execute_query(
                query,
                query_durations,
                row_counts,
                results_snapshot,
                validate,
            )
            .await
        {
            Ok(()) => Ok((true, true)),
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
                    Ok((false, false))
                } else {
                    eprintln!(
                        "FAIL - Worker {} - Query '{}' failed: {}",
                        self.id, query.name, e
                    );
                    query_durations.entry(query.name.to_string()).or_default();
                    Ok((true, false))
                }
            }
        }
    }

    async fn execute_query(
        &self,
        query: &Query,
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
        results_snapshot: bool,
        validate: bool,
    ) -> Result<()> {
        let query_start = Instant::now();
        let mut result_stream = self.flight_client.query(&query.sql).await?;

        let mut row_count: usize = 0;
        let mut limited_records = vec![];
        let mut validation_records = vec![];
        loop {
            let batch = result_stream.try_next().await;
            match batch {
                Ok(None) => break,
                Err(e) => {
                    eprintln!(
                        "FAIL - Worker {} - Query '{}' failed: {}",
                        self.id, query.name, e
                    );
                    query_durations.entry(query.name.to_string()).or_default();
                    return Err(e.into());
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
            let validation_result = validation::validate_tpch_query(query, &validation_records)?;
            if let QueryValidationResult::Fail(validation_reason) = validation_result {
                eprintln!(
                    "FAIL - Worker {} - Query '{}' validation failed: {validation_reason:?}",
                    self.id, query.name
                );
                return Err(anyhow::anyhow!("Query validation failed"));
            }
        }

        if results_snapshot {
            let query_name = Arc::clone(&query.name);
            let name = self.name.clone();

            let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)?;
            let result = panic::catch_unwind(|| {
                insta::with_settings!({
                    description => format!("Query: {query_name}"),
                    omit_expression => true,
                    snapshot_path => "../../snapshot/snapshots/results"
                }, {
                    insta::assert_snapshot!(format!("{name}_{query_name}"), records_pretty);
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
            .entry(query.name.to_string())
            .or_default()
            .push(duration);

        row_counts
            .entry(query.name.to_string())
            .or_default()
            .push(row_count);

        if let Some(pb) = self.progress_bar.as_ref() {
            pb.inc(1);
        }

        Ok(())
    }
}
