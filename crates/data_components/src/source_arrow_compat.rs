/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::io::Cursor;

use arrow::array::RecordBatch;

pub(crate) fn record_batch_to_arrow(
    record_batch: &source_arrow::array::RecordBatch,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let mut buffer = Vec::new();
    let schema = record_batch.schema();
    {
        let mut writer =
            source_arrow_ipc::writer::StreamWriter::try_new(&mut buffer, schema.as_ref())
                .map_err(source_arrow_error_to_arrow)?;
        writer
            .write(record_batch)
            .map_err(source_arrow_error_to_arrow)?;
        writer.finish().map_err(source_arrow_error_to_arrow)?;
    }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(buffer), None)?;
    let Some(batch) = reader.next() else {
        return Err(arrow::error::ArrowError::ParseError(
            "source Arrow IPC stream did not contain a record batch".to_string(),
        ));
    };

    batch
}

pub(crate) fn batches_to_arrow(
    record_batches: &[source_arrow::array::RecordBatch],
) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    record_batches.iter().map(record_batch_to_arrow).collect()
}

fn source_arrow_error_to_arrow(error: source_arrow::error::ArrowError) -> arrow::error::ArrowError {
    arrow::error::ArrowError::ExternalError(Box::new(error))
}
