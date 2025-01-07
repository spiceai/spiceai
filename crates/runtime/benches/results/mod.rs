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

use std::sync::Arc;

use arrow::{
    array::{Int32Builder, Int64Builder, RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use runtime::dataupdate::{DataUpdate, UpdateType};

#[derive(Copy, Clone)]
pub(crate) enum Status {
    Passed,
    Failed,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Status::Passed => write!(f, "passed"),
            Status::Failed => write!(f, "failed"),
        }
    }
}

pub(crate) struct BenchmarkResult {
    start_time: i64,
    end_time: i64,
    connector_name: Arc<str>,
    query_name: Arc<str>,
    pub status: Status,
    min_duration_ms: i64,
    max_duration_ms: i64,
    iterations: i32,
}

impl BenchmarkResult {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        start_time: i64,
        end_time: i64,
        connector_name: &str,
        query_name: &str,
        status: Status,
        min_duration_ms: i64,
        max_duration_ms: i64,
        iterations: i32,
    ) -> Self {
        Self {
            start_time,
            end_time,
            connector_name: connector_name.into(),
            query_name: query_name.into(),
            status,
            min_duration_ms,
            max_duration_ms,
            iterations,
        }
    }
}

pub(crate) struct BenchmarkResultsBuilder {
    this_run_id: String,
    this_commit_sha: String,
    this_branch_name: String,
    this_iterations: i32,

    run_id: StringBuilder,
    started_at: Int64Builder,
    finished_at: Int64Builder,
    connector_name: StringBuilder,
    query_name: StringBuilder,
    status: StringBuilder,
    min_duration_ms: Int64Builder,
    max_duration_ms: Int64Builder,
    iterations: Int32Builder,
    commit_sha: StringBuilder,
    branch_name: StringBuilder,
}

impl BenchmarkResultsBuilder {
    pub(crate) fn new(commit_sha: String, branch_name: String, iterations: i32) -> Self {
        Self {
            this_run_id: uuid::Uuid::new_v4().to_string(),
            this_commit_sha: commit_sha,
            this_branch_name: branch_name,
            this_iterations: iterations,
            run_id: StringBuilder::new(),
            started_at: Int64Builder::new(),
            finished_at: Int64Builder::new(),
            query_name: StringBuilder::new(),
            status: StringBuilder::new(),
            min_duration_ms: Int64Builder::new(),
            max_duration_ms: Int64Builder::new(),
            iterations: Int32Builder::new(),
            commit_sha: StringBuilder::new(),
            branch_name: StringBuilder::new(),
            connector_name: StringBuilder::new(),
        }
    }

    pub(crate) fn record_result(&mut self, result: BenchmarkResult) {
        self.run_id.append_value(&self.this_run_id);
        self.started_at.append_value(result.start_time);
        self.finished_at.append_value(result.end_time);
        self.query_name.append_value(result.query_name);
        self.connector_name.append_value(result.connector_name);
        self.status.append_value(result.status.to_string());
        self.min_duration_ms.append_value(result.min_duration_ms);
        self.max_duration_ms.append_value(result.max_duration_ms);
        self.iterations.append_value(result.iterations);
        self.commit_sha.append_value(&self.this_commit_sha);
        self.branch_name.append_value(&self.this_branch_name);
    }

    pub(crate) fn iterations(&self) -> i32 {
        self.this_iterations
    }

    pub(crate) fn build(mut self) -> RecordBatch {
        let schema = results_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.run_id.finish()),
                Arc::new(self.started_at.finish()),
                Arc::new(self.finished_at.finish()),
                Arc::new(self.query_name.finish()),
                Arc::new(self.status.finish()),
                Arc::new(self.min_duration_ms.finish()),
                Arc::new(self.max_duration_ms.finish()),
                Arc::new(self.iterations.finish()),
                Arc::new(self.commit_sha.finish()),
                Arc::new(self.branch_name.finish()),
                Arc::new(self.connector_name.finish()),
            ],
        );
        match batch {
            Ok(batch) => batch,
            Err(e) => panic!("Error building record batch: {e}"),
        }
    }
}

impl From<BenchmarkResultsBuilder> for DataUpdate {
    fn from(builder: BenchmarkResultsBuilder) -> Self {
        let batch = builder.build();
        DataUpdate {
            schema: batch.schema(),
            data: vec![batch],
            update_type: UpdateType::Append,
        }
    }
}

fn results_schema() -> SchemaRef {
    let fields = vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("started_at", DataType::Int64, false),
        Field::new("finished_at", DataType::Int64, false),
        Field::new("query_name", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("min_duration_ms", DataType::Int64, false),
        Field::new("max_duration_ms", DataType::Int64, false),
        Field::new("iterations", DataType::Int32, false),
        Field::new("commit_sha", DataType::Utf8, false),
        Field::new("branch_name", DataType::Utf8, false),
        Field::new("connector_name", DataType::Utf8, false),
    ];
    Arc::new(Schema::new(fields))
}
