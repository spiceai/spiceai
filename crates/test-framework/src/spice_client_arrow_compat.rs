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

use anyhow::{Result, anyhow};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

type SpiceRecordBatch = spice_client_arrow::record_batch::RecordBatch;

pub fn to_spice_record_batch(batch: &RecordBatch) -> Result<SpiceRecordBatch> {
    let mut data = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut data, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }

    let mut reader =
        spice_client_arrow::ipc::reader::StreamReader::try_new(Cursor::new(data), None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("Arrow IPC conversion produced no record batch"))
}

pub fn from_spice_record_batch(batch: &SpiceRecordBatch) -> Result<RecordBatch> {
    let mut data = Vec::new();
    {
        let mut writer = spice_client_arrow::ipc::writer::StreamWriter::try_new(
            &mut data,
            batch.schema().as_ref(),
        )?;
        writer.write(batch)?;
        writer.finish()?;
    }

    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(data), None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("Arrow IPC conversion produced no record batch"))
}

pub fn optional_params_to_spice(params: Option<RecordBatch>) -> Result<Option<SpiceRecordBatch>> {
    params
        .map(|batch| to_spice_record_batch(&batch))
        .transpose()
}

pub async fn query_to_batches(
    spice_client: &spiceai::Client,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let mut stream = spice_client.sql(sql).await?;
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(from_spice_record_batch(&batch?)?);
    }

    Ok(batches)
}

pub async fn query_with_params_to_batches(
    spice_client: &spiceai::Client,
    sql: &str,
    params: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let params = optional_params_to_spice(params)?;
    let mut stream = spice_client.sql_with_params(sql, params).await?;
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        batches.push(from_spice_record_batch(&batch?)?);
    }

    Ok(batches)
}
