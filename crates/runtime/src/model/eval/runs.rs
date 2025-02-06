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

use crate::{
    datafusion::{DataFusion, SPICE_DEFAULT_CATALOG, SPICE_EVAL_SCHEMA},
    dataupdate::{DataUpdate, UpdateType},
    tracing_util::random_trace_id,
};

use super::FailedToUpdateEvalRunTableSnafu;
use arrow::{
    array::{
        ArrayRef, Float32Builder, ListArray, MapBuilder, RecordBatch, StringArray, StringBuilder,
        TimestampSecondArray,
    },
    buffer::OffsetBuffer,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::TryStreamExt;
use snafu::ResultExt;

use super::Result;
use datafusion::sql::TableReference;

use spicepod::component::eval::Eval;
use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, LazyLock},
};

/// The unique identifier for an evaluation run. Can be used to uniquely identify an eval run within `spice.evals.runs`.
pub type EvalRunId = String;

#[derive(Debug, Clone)]
pub enum EvalRunStatus {
    Waiting,
    Queued,
    Running,
    Completed,
    Failed,
}

impl Display for EvalRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalRunStatus::Waiting => write!(f, "Waiting"),
            EvalRunStatus::Queued => write!(f, "Queued"),
            EvalRunStatus::Running => write!(f, "Running"),
            EvalRunStatus::Completed => write!(f, "Completed"),
            EvalRunStatus::Failed => write!(f, "Failed"),
        }
    }
}

pub static EVAL_RUNS_TABLE_REFERENCE: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::Full {
        catalog: SPICE_DEFAULT_CATALOG.into(),
        schema: SPICE_EVAL_SCHEMA.into(),
        table: "runs".into(),
    });

pub static EVAL_RUNS_TABLE_TIME_COLUMN: &str = "created_at";
pub static EVAL_RUNS_TABLE_TIME_COMPLETED_COLUMN: &str = "completed_at";
pub static EVAL_RUNS_TABLE_PRIMARY_KEY: &str = "id";

/// Represents the response for an evaluation run
#[derive(Debug)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EvalRunResponse {
    /// Unique identifier for the evaluation run
    pub primary_key: String,

    /// Timestamp indicating when the evaluation was created or run
    pub time_column: String,

    /// The name of the dataset used for the evaluation
    pub dataset: String,

    /// The model used for the evaluation
    pub model: String,

    /// The status of the evaluation (e.g., "completed", "failed", etc.)
    pub status: String,

    /// The error message if the evaluation failed, otherwise `None`
    pub error_message: Option<String>,

    /// List of scorers used in the evaluation
    pub scorers: Vec<String>,

    /// A map of metric names to their corresponding values
    pub metrics: HashMap<String, f64>,
}

pub static EVAL_RUNS_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(EVAL_RUNS_TABLE_PRIMARY_KEY, DataType::Utf8, false),
        Field::new(
            EVAL_RUNS_TABLE_TIME_COLUMN,
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new(
            EVAL_RUNS_TABLE_TIME_COMPLETED_COLUMN,
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new("dataset", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, true),
        Field::new("scorers", DataType::new_list(DataType::Utf8, false), false),
        Field::new(
            "metrics",
            DataType::Map(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Float32, true)),
                    ],
                    false,
                )),
                false,
            ),
            false,
        ),
    ]))
});

/// Add aggregate metrics for an eval run in `spice.evals.runs`.
/// `metrics` is a map of scorer name to pairs of (metric name, metrics value).
pub(super) async fn add_metrics_to_eval_run(
    df: Arc<DataFusion>,
    id: &EvalRunId,
    metrics: &HashMap<String, Vec<(String, f32)>>,
) -> Result<()> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Float32Builder::new());
    for (scorer, metric_pair) in metrics {
        for (metric_name, score) in metric_pair {
            builder
                .keys()
                .append_value(format!("{scorer}/{metric_name}"));
            builder.values().append_value(*score);
        }
    }

    builder
        .append(true)
        .boxed()
        .context(FailedToUpdateEvalRunTableSnafu {
            eval_run_id: id.clone(),
        })?;

    let mut updates: HashMap<&str, ArrayRef> = HashMap::new();
    updates.insert("metrics", Arc::new(builder.finish()) as ArrayRef);

    update_eval_run(df, id, updates).await
}

/// Updates a row in [`EVAL_RUNS_TABLE_REFERENCE`] with the provided status and error message.
pub async fn update_eval_run_status(
    df: Arc<DataFusion>,
    id: &EvalRunId,
    status: &EvalRunStatus,
    err_msg: Option<String>,
) -> Result<()> {
    let mut updates: HashMap<&str, ArrayRef> = HashMap::new();
    updates.insert(
        "status",
        Arc::new(StringArray::from(vec![status.to_string()])),
    );

    if matches!(status, EvalRunStatus::Completed) {
        updates.insert(
            EVAL_RUNS_TABLE_TIME_COMPLETED_COLUMN,
            Arc::new(TimestampSecondArray::from(vec![Some(
                chrono::Utc::now().timestamp(),
            )])),
        );
    }

    if let Some(err) = err_msg {
        updates.insert("error_message", Arc::new(StringArray::from(vec![err])));
    };
    update_eval_run(df, id, updates).await
}

/// Writes a new row to `spice.evals.runs` table and returns primary key.
pub async fn start_tracing_eval_run(
    eval: &Eval,
    model_name: &str,
    df: Arc<DataFusion>,
) -> Result<EvalRunId> {
    // Use a traceId for the eval run job.
    let id = random_trace_id().to_string();
    let rb = eval_runs_record(id.as_str(), model_name, eval)
        .boxed()
        .context(FailedToUpdateEvalRunTableSnafu {
            eval_run_id: id.to_string(),
        })?;

    df.write_data(
        &EVAL_RUNS_TABLE_REFERENCE,
        DataUpdate {
            schema: Arc::clone(&EVAL_RUNS_TABLE_SCHEMA),
            data: vec![rb],
            update_type: UpdateType::Append,
        },
    )
    .await
    .boxed()
    .context(FailedToUpdateEvalRunTableSnafu {
        eval_run_id: id.to_string(),
    })?;

    Ok(id)
}
pub fn sql_query_for(id: &EvalRunId) -> String {
    format!(
        "SELECT * FROM {tbl} WHERE id = '{id}';",
        tbl = EVAL_RUNS_TABLE_REFERENCE.to_quoted_string(),
        id = id
    )
}

async fn get_eval_run(
    df: Arc<DataFusion>,
    id: &EvalRunId,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let rb = df
        .query_builder(sql_query_for(id).as_str())
        .build()
        .run()
        .await
        .boxed()?
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .boxed()?
        .into_iter()
        .next()
        .ok_or(format!("No eval run found with id: {id}"))?;

    Ok(rb)
}

/// Updates the record batch with the provided updates.
///
/// Consumes `updates`.
fn update_record_batch(
    record_batch: &RecordBatch,
    mut updates: HashMap<&str, ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    let schema = record_batch.schema();
    let mut cols = record_batch.columns().to_vec();

    for (col, arr) in updates.drain() {
        if let Ok(i) = schema.index_of(col) {
            cols[i] = arr;
        }
    }

    RecordBatch::try_new(schema, cols)
}

async fn update_eval_run(
    df: Arc<DataFusion>,
    id: &EvalRunId,
    updates: HashMap<&str, ArrayRef>,
) -> Result<()> {
    let rb = get_eval_run(Arc::clone(&df), id)
        .await
        .context(FailedToUpdateEvalRunTableSnafu {
            eval_run_id: id.clone(),
        })?;

    let new_rb =
        update_record_batch(&rb, updates)
            .boxed()
            .context(FailedToUpdateEvalRunTableSnafu {
                eval_run_id: id.clone(),
            })?;

    df.write_data(
        &EVAL_RUNS_TABLE_REFERENCE.clone(),
        DataUpdate {
            schema: new_rb.schema(),
            data: vec![new_rb],
            update_type: UpdateType::Changes,
        },
    )
    .await
    .boxed()
    .context(FailedToUpdateEvalRunTableSnafu {
        eval_run_id: id.clone(),
    })?;

    // Invalidate cache so subsequent calls to [`update_eval_run`] get the most up to date table.
    if let Some(cache) = df.cache_provider() {
        cache
            .invalidate_for_table(EVAL_RUNS_TABLE_REFERENCE.clone())
            .await
            .boxed()
            .context(FailedToUpdateEvalRunTableSnafu {
                eval_run_id: id.clone(),
            })?;
    }
    Ok(())
}

fn eval_runs_record(id: &str, model: &str, eval: &Eval) -> Result<RecordBatch, ArrowError> {
    // `metrics` as single null in MapArray.
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Float32Builder::new());
    builder.append(true)?;

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![id.to_string()])),
        Arc::new(TimestampSecondArray::from(vec![
            chrono::Utc::now().timestamp()
        ])),
        Arc::new(TimestampSecondArray::from(vec![None])),
        Arc::new(StringArray::from(vec![eval.dataset.clone()])),
        Arc::new(StringArray::from(vec![model.to_string()])),
        Arc::new(StringArray::from(vec![EvalRunStatus::Waiting.to_string()])),
        Arc::new(StringArray::from(vec![None] as Vec<Option<&str>>)),
        Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            OffsetBuffer::<i32>::from_lengths([eval.scorers.len()]),
            Arc::new(StringArray::from_iter_values(eval.scorers.iter().clone())),
            None,
        )?),
        Arc::new(builder.finish()) as ArrayRef,
    ];
    RecordBatch::try_new(EVAL_RUNS_TABLE_SCHEMA.clone(), arrays)
}
