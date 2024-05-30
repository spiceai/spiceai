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

use std::{sync::Arc, time::SystemTime};

use arrow::datatypes::Schema;
use datafusion::{
    execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter,
};

use crate::{datafusion::DataFusion, query_history};
use async_stream::stream;
use futures::StreamExt;

pub struct QueryContext {
    pub query_id: uuid::Uuid,
    pub schema: Option<Arc<Schema>>,
    pub sql: Option<String>,
    pub nsql: Option<String>,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub execution_time: Option<u64>,
    pub rows_produced: Option<u64>,
    pub results_cache_hit: Option<bool>,
    pub df: Arc<DataFusion>,
}

impl QueryContext {
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self {
            query_id: uuid::Uuid::new_v4(),
            schema: None,
            sql: None,
            nsql: None,
            start_time: SystemTime::now(),
            end_time: None,
            execution_time: None,
            rows_produced: None,
            results_cache_hit: None,
            df,
        }
    }

    #[must_use]
    pub fn schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub fn sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    #[must_use]
    pub fn nsql(mut self, nsql: String) -> Self {
        self.nsql = Some(nsql);
        self
    }

    #[must_use]
    pub fn start_time(mut self, start_time: SystemTime) -> Self {
        self.start_time = start_time;
        self
    }

    #[must_use]
    pub fn end_time(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    #[must_use]
    pub fn execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time = Some(execution_time);
        self
    }

    #[must_use]
    pub fn rows_produced(mut self, rows_produced: u64) -> Self {
        self.rows_produced = Some(rows_produced);
        self
    }

    #[must_use]
    pub fn results_cache_hit(mut self, results_cache_hit: bool) -> Self {
        self.results_cache_hit = Some(results_cache_hit);
        self
    }

    #[must_use]
    pub fn finish(mut self) -> Self {
        if self.end_time.is_none() {
            self.end_time = Some(SystemTime::now());
        }

        if self.execution_time.is_none() {
            if let Some(execution_time) = self
                .end_time
                .and_then(|end_time| end_time.duration_since(self.start_time).ok())
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            {
                self.execution_time = Some(execution_time);
            }
        }
        if self.results_cache_hit.is_none() {
            self.results_cache_hit = Some(false);
        }

        self
    }
}

#[must_use]
pub fn attach_query_context_to_stream(
    ctx: QueryContext,
    mut stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let schema_copy = Arc::clone(&schema);

    let mut num_records = 0u64;

    let updated_stream = stream! {
        while let Some(batch_result) = stream.next().await {
            if let Ok(batch) = &batch_result {
                num_records += batch.num_rows() as u64;
            }
            yield batch_result;
        }

        if let Err(e) = query_history::write(&ctx
            .end_time(SystemTime::now())
            .schema(schema_copy)
            .rows_produced(num_records)
            .finish()).await {
                tracing::error!("Error writing query history: {e}");
            }
    };

    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        Box::pin(updated_stream),
    ))
}
