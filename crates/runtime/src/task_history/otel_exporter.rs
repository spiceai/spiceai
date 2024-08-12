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

use std::fs::File;
use std::io::Write;

use futures::future::BoxFuture;
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};

#[derive(Debug)]
pub struct TaskHistoryExporter {
    file: File,
}

impl TaskHistoryExporter {
    pub fn new() -> Self {
        let Ok(file) = File::create("task_history_exporter.log") else {
            panic!("Unable to create task history exporter log file");
        };
        Self { file }
    }
}

impl SpanExporter for TaskHistoryExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        for span in batch {
            writeln!(self.file, "{span:#?}");
        }
        Box::pin(async move { Ok(()) })
    }

    fn shutdown(&mut self) {}

    fn force_flush(&mut self) -> BoxFuture<'static, ExportResult> {
        self.file.flush();
        Box::pin(async { Ok(()) })
    }
}

// async fn write(&self) -> Result<(), Error> {
//     if self.end_time.is_none() {
//         return Err(Error::MissingColumnsInRow {
//             columns: "end_time".to_string(),
//         });
//     }

//     let data = self
//         .to_record_batch()
//         .boxed()
//         .context(UnableToWriteToTableSnafu)?;

//     let data_update = DataUpdate {
//         schema: Arc::new(Self::table_schema()),
//         data: vec![data],
//         update_type: crate::dataupdate::UpdateType::Append,
//     };

//     self.df
//         .write_data(
//             TableReference::partial(SPICE_RUNTIME_SCHEMA, DEFAULT_TASK_HISTORY_TABLE),
//             data_update,
//         )
//         .await
//         .boxed()
//         .context(UnableToWriteToTableSnafu)?;

//     Ok(())
// }
