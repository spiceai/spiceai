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

use std::fmt;
use std::sync::Arc;
use std::{
    fmt::{Debug, Formatter},
    io::Write,
};

use futures::future::BoxFuture;
use opentelemetry::trace::TraceError;
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};

use crate::datafusion::DataFusion;

use super::TaskSpan;

#[derive(Clone)]
pub struct TaskHistoryExporter {
    df: Arc<DataFusion>,
}

impl Debug for TaskHistoryExporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHistoryExporter").finish()
    }
}

impl TaskHistoryExporter {
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }
}

impl SpanExporter for TaskHistoryExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let df = Arc::clone(&self.df);
        Box::pin(async move {
            TaskSpan::write(df, vec![])
                .await
                .map_err(|e| TraceError::Other(Box::new(e)))
        })
    }
}
