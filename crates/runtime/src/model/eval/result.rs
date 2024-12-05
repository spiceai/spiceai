/*
Copyright 2024 The Spice.ai OSS Authors

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
    model::EvalWorker,
};

use super::{FailedToUpdateEvalMetadataSnafu, FailedToUpdateEvalRunStatusSnafu};
use arrow::{
    array::{ArrayRef, ListArray, RecordBatch, StringArray, TimestampSecondArray},
    buffer::OffsetBuffer,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use snafu::ResultExt;
use uuid::Uuid;

use super::Result;
use datafusion::sql::TableReference;

use spicepod::component::eval::Eval;
use std::{
    fmt::Display,
    sync::{Arc, LazyLock},
};

pub static EVAL_RESULTS_TABLE_REFERENCE: LazyLock<TableReference> =
    LazyLock::new(|| TableReference::Full {
        catalog: SPICE_DEFAULT_CATALOG.into(),
        schema: SPICE_EVAL_SCHEMA.into(),
        table: "results".into(),
    });

pub static EVAL_RESULTS_TABLE_TIME_COLUMN: &str = "created_at";
pub static EVAL_RESULTS_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new(
            EVAL_RESULTS_TABLE_TIME_COLUMN,
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        // input, output, actual, scorer, value
        Field::new("dataset", DataType::
    ]))
});

/// Writes a new row to `spice.evals.runs` table and returns primary key.
pub async fn start_eval_run(
    eval: &Eval,
    model_name: String,
    df: Arc<DataFusion>,
    ew: Arc<EvalWorker>,
) -> Result<EvalRunId> {
    let id = uuid::Uuid::new_v4();
    let rb = EVAL_RESULTS_record(&id, model_name.as_str(), eval)
        .boxed()
        .context(FailedToUpdateEvalMetadataSnafu {
            eval_run_id: id.to_string(),
        })?;

    df.write_data(
        &EVAL_RESULTS_TABLE_REFERENCE,
        DataUpdate {
            schema: Arc::clone(&EVAL_RESULTS_TABLE_SCHEMA),
            data: vec![rb],
            update_type: UpdateType::Overwrite,
        },
    )
    .await
    .boxed()
    .context(FailedToUpdateEvalMetadataSnafu {
        eval_run_id: id.to_string(),
    })?;

    ew.queue_eval_job(&id.to_string(), eval, model_name.as_str())
        .await?;

    Ok(id.to_string())
}

fn eval_results_record(uuid: &Uuid, model: &str, eval: &Eval) -> Result<RecordBatch, ArrowError> {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![uuid.to_string()])),
        Arc::new(TimestampSecondArray::from(vec![
            chrono::Utc::now().timestamp()
        ])),
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
    ];
    RecordBatch::try_new(EVAL_RESULTS_TABLE_SCHEMA.clone(), arrays)
}
