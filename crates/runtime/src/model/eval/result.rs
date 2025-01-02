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
    model::{DatasetInput, DatasetOutput},
};

use super::{runs::EvalRunId, FailedToWriteEvalResultsSnafu};
use arrow::array::{Float32Builder, RecordBatch, StringBuilder, TimestampSecondBuilder};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use snafu::ResultExt;

use super::Result;
use datafusion::sql::TableReference;

use std::sync::{Arc, LazyLock};

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
        Field::new("input", DataType::Utf8, false),
        Field::new("output", DataType::Utf8, false),
        Field::new("actual", DataType::Utf8, false),
        Field::new("scorer", DataType::Utf8, false),
        Field::new("value", DataType::Float32, false),
    ]))
});

pub(super) async fn write_result_to_table(
    df: Arc<DataFusion>,
    id: &EvalRunId,
    builder: &mut ResultBuilder,
) -> Result<()> {
    let rb = builder
        .finish()
        .boxed()
        .context(FailedToWriteEvalResultsSnafu {
            eval_run_id: id.to_string(),
        })?;

    df.write_data(
        &EVAL_RESULTS_TABLE_REFERENCE,
        DataUpdate {
            schema: Arc::clone(&EVAL_RESULTS_TABLE_SCHEMA),
            data: vec![rb],
            update_type: UpdateType::Append,
        },
    )
    .await
    .boxed()
    .context(FailedToWriteEvalResultsSnafu {
        eval_run_id: id.to_string(),
    })
}

/// Builder for creating a `RecordBatch` for the [`EVAL_RESULTS_TABLE_REFERENCE`] table
pub(super) struct ResultBuilder {
    run_id: StringBuilder,
    created_at: TimestampSecondBuilder,
    input: StringBuilder,
    output: StringBuilder,
    actual: StringBuilder,
    scorer: StringBuilder,
    value: Float32Builder,
}

impl ResultBuilder {
    pub fn new() -> Self {
        Self {
            run_id: StringBuilder::new(),
            created_at: TimestampSecondBuilder::new(),
            input: StringBuilder::new(),
            output: StringBuilder::new(),
            actual: StringBuilder::new(),
            scorer: StringBuilder::new(),
            value: Float32Builder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        id: &EvalRunId,
        created_at: DateTime<Utc>,
        input: &DatasetInput,
        output: &DatasetOutput,
        actual: &DatasetOutput,
        scorer: &str,
        value: f32,
    ) -> Result<()> {
        self.run_id.append_value(id);
        self.created_at.append_value(created_at.timestamp());
        self.input.append_value(input.try_serialize()?);
        self.output.append_value(output.try_serialize()?);
        self.actual.append_value(actual.try_serialize()?);
        self.scorer.append_value(scorer);
        self.value.append_value(value);
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            EVAL_RESULTS_TABLE_SCHEMA.clone(),
            vec![
                Arc::new(self.run_id.finish()),
                Arc::new(self.created_at.finish()),
                Arc::new(self.input.finish()),
                Arc::new(self.output.finish()),
                Arc::new(self.actual.finish()),
                Arc::new(self.scorer.finish()),
                Arc::new(self.value.finish()),
            ],
        )
    }
}
