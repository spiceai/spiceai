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
    time::{Duration, Instant, SystemTime},
};

use anyhow::Result;
use arrow::array::RecordBatch;
use flight_client::FlightClient;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use tokio::task::JoinHandle;

use crate::snapshot::record_explain_plan;

use super::EndCondition;

pub(crate) struct SpiceTestQueryWorker {
    id: usize,
    query_set: Vec<(&'static str, &'static str)>,
    end_condition: EndCondition,
    flight_client: FlightClient,
    explain_plan_snapshot: bool,
    results_snapshot_predicate: Option<fn(&str) -> bool>,
    component_name: Option<String>,
    pub progress_bar: Option<ProgressBar>,
}

pub struct SpiceTestQueryWorkerResult {
    pub query_durations: BTreeMap<String, Vec<Duration>>,
    pub query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)>,
    pub connection_failed: bool,
    pub row_counts: BTreeMap<String, Vec<usize>>,
}

impl SpiceTestQueryWorkerResult {
    pub fn new(
        query_durations: BTreeMap<String, Vec<Duration>>,
        query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)>,
        connection_failed: bool,
        row_counts: BTreeMap<String, Vec<usize>>,
    ) -> Self {
        Self {
            query_durations,
            query_iteration_durations,
            connection_failed,
            row_counts,
        }
    }
}

impl SpiceTestQueryWorker {
    pub fn new(
        id: usize,
        query_set: Vec<(&'static str, &'static str)>,
        end_condition: EndCondition,
        flight_client: FlightClient,
    ) -> Self {
        Self {
            id,
            query_set,
            end_condition,
            flight_client,
            explain_plan_snapshot: false,
            results_snapshot_predicate: None,
            component_name: None,
            progress_bar: None,
        }
    }

    pub fn with_component_name(mut self, component_name: Option<String>) -> Self {
        self.component_name = component_name;
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
                            .run_query_set(&mut query_durations, &mut row_counts)
                            .await?
                        {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                query_durations,
                                query_iteration_durations,
                                true,
                                row_counts,
                            ));
                        }
                        query_set_count += 1;
                    }
                }
                EndCondition::QuerySetCompleted(target_count) => {
                    // For QuerySetCompleted, run each query target_count times before moving to next
                    for query in &self.query_set {
                        let mut current_query_count = 0;
                        let start = SystemTime::now();

                        let snapshot_results = self
                            .results_snapshot_predicate
                            .is_some_and(|predicate| predicate(query.0));

                        // Additional round of query run before recording results.
                        // To discard the abnormal results caused by: establishing initial connection / spark cluster startup time
                        if !self
                            .run_single_query(
                                query,
                                &mut BTreeMap::new(),
                                &mut BTreeMap::new(),
                                snapshot_results,
                            )
                            .await?
                        {
                            return Ok(SpiceTestQueryWorkerResult::new(
                                query_durations,
                                query_iteration_durations,
                                true,
                                row_counts,
                            ));
                        }

                        if let Some(component_name) = &self.component_name {
                            if self.explain_plan_snapshot {
                                record_explain_plan(
                                    &self.flight_client,
                                    component_name,
                                    query.0,
                                    query.1,
                                )
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!("Failed to record explain plan: {}", e)
                                })?;
                            }
                        }

                        while current_query_count < target_count {
                            if self.progress_bar.is_none() && self.id == 0 {
                                println!(
                                    "Worker {} - Query '{}' count: {}/{} - Elapsed time: {:?}",
                                    self.id,
                                    query.0,
                                    current_query_count + 1,
                                    target_count,
                                    start.elapsed().unwrap_or_default()
                                );
                            }

                            if !self
                                .run_single_query(
                                    query,
                                    &mut query_durations,
                                    &mut row_counts,
                                    false,
                                )
                                .await?
                            {
                                return Ok(SpiceTestQueryWorkerResult::new(
                                    query_durations,
                                    query_iteration_durations,
                                    true,
                                    row_counts,
                                ));
                            }
                            current_query_count += 1;
                        }
                        let end = SystemTime::now();
                        query_iteration_durations.insert(query.0.to_string(), (start, end));
                    }
                }
            }

            Ok(SpiceTestQueryWorkerResult::new(
                query_durations,
                query_iteration_durations,
                false,
                row_counts,
            ))
        })
    }

    async fn run_query_set(
        &self,
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
    ) -> Result<bool> {
        for query in &self.query_set {
            if !self
                .run_single_query(query, query_durations, row_counts, false)
                .await?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn run_single_query(
        &self,
        query: &(&'static str, &'static str),
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
        results_snapshot: bool,
    ) -> Result<bool> {
        match self
            .execute_query(query, query_durations, row_counts, results_snapshot)
            .await
        {
            Ok(()) => Ok(true),
            Err(e) => {
                let flight_error = e.downcast_ref::<flight_client::Error>();

                if let Some(
                    flight_client::Error::UnableToConnectToServer { .. }
                    | flight_client::Error::UnableToPerformHandshake { .. },
                ) = flight_error
                {
                    eprintln!(
                        "FAIL - EARLY EXIT - Worker {} - Query '{}' failed: {}",
                        self.id, query.0, e
                    );
                    Ok(false)
                } else {
                    eprintln!(
                        "FAIL - Worker {} - Query '{}' failed: {}",
                        self.id, query.0, e
                    );
                    query_durations.entry(query.0.to_string()).or_default();
                    Ok(true)
                }
            }
        }
    }

    async fn execute_query(
        &self,
        query: &(&'static str, &'static str),
        query_durations: &mut BTreeMap<String, Vec<Duration>>,
        row_counts: &mut BTreeMap<String, Vec<usize>>,
        results_snapshot: bool,
    ) -> Result<()> {
        let query_start = Instant::now();
        let mut result_stream = self.flight_client.query(query.1).await?;

        let mut row_count: usize = 0;
        let mut limited_records = vec![];
        loop {
            let batch = result_stream.try_next().await;
            match batch {
                Ok(None) => break,
                Err(e) => {
                    eprintln!(
                        "FAIL - Worker {} - Query '{}' failed: {}",
                        self.id, query.0, e
                    );
                    query_durations.entry(query.0.to_string()).or_default();
                    return Err(e.into());
                }
                Ok(Some(batch)) => {
                    row_count += batch.num_rows();

                    if limited_records
                        .iter()
                        .map(|b: &RecordBatch| b.num_rows())
                        .sum::<usize>()
                        < 10
                    {
                        let required_rows = 10 - limited_records.len();
                        let end = if batch.num_rows() > required_rows {
                            required_rows
                        } else {
                            batch.num_rows()
                        };

                        limited_records.push(batch.slice(0, end));
                    }
                }
            }
        }

        if results_snapshot {
            let query_name = query.0;

            let connector = match &self.component_name {
                Some(name) => name.clone(),
                None => {
                    return Err(anyhow::anyhow!(
                        "Connector name is required for results snapshot but was not provided"
                    ))
                }
            };

            let records_pretty = arrow::util::pretty::pretty_format_batches(&limited_records)?;
            let result = panic::catch_unwind(|| {
                insta::with_settings!({
                    description => format!("Query: {query_name}"),
                    omit_expression => true,
                    snapshot_path => "../../snapshot/snapshots/results"
                }, {
                    insta::assert_snapshot!(format!("{connector}_{query_name}"), records_pretty);
                });
            });

            if result.is_err() {
                let error_str =
                    format!("Query `{connector}` `{query_name}` snapshot assertion failed",);
                eprintln!("{error_str}");
                return Err(anyhow::anyhow!(error_str));
            }
        }

        let duration = query_start.elapsed();
        query_durations
            .entry(query.0.to_string())
            .or_default()
            .push(duration);

        row_counts
            .entry(query.0.to_string())
            .or_default()
            .push(row_count);

        if let Some(pb) = self.progress_bar.as_ref() {
            pb.inc(1);
        }

        Ok(())
    }
}
