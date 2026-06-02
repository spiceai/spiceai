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

use anyhow::{Result, anyhow};
use arrow::{
    array::ArrayRef,
    datatypes::{Field, FieldRef, Schema},
    record_batch::RecordBatch,
};
use flight_client::FlightClient;
use futures::StreamExt;
use spiceai::{Client as SpiceClient, SpiceClientError};

/// Query a flight client and return the result as a vector of record batches
///
/// # Errors
///
/// - If the flight client fails to query
pub async fn query_to_batches(
    spice_client: Arc<SpiceClient>,
    sql: &str,
    params: Option<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = spice_client.sql_with_params(sql, params).await?;

    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => batches.push(batch),
            Err(e) => match e {
                SpiceClientError::ConnectionReset { .. } => {
                    batches.clear();
                }
                _ => {
                    return Err(anyhow!(e.to_string()));
                }
            },
        }
    }
    Ok(batches)
}

pub async fn put_batches(
    client: &mut FlightClient,
    dataset_path: &str,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    Ok(client.publish(dataset_path, batches).await?)
}

pub struct PreparedStatementParamColumn {
    name: String,
    dtype: arrow::datatypes::DataType,
    nullable: bool,
    array: ArrayRef,
}

impl PreparedStatementParamColumn {
    pub fn new(
        name: String,
        dtype: arrow::datatypes::DataType,
        nullable: bool,
        array: ArrayRef,
    ) -> Self {
        Self {
            name,
            dtype,
            nullable,
            array,
        }
    }
}

/// # Usage
///
/// ```rust,ignore
/// create_param_batch(vec![
///   PreparedStatementParamColumn::new(
///     "$1",
///     arrow::datatypes::DataType::Int64,
///     false,
///     Arc::new(Int64Array::from(vec![41])) as Arc<dyn arrow::array::Array>
///   ),
///   PreparedStatementParamColumn::new(
///     "$2",
///     arrow::datatypes::DataType::Utf8,
///     true,
///     Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as Arc<dyn arrow::array::Array>
///   )
/// ])?;
/// ```
pub fn create_param_batch(
    params: Vec<PreparedStatementParamColumn>,
) -> Result<RecordBatch, anyhow::Error> {
    let (fields, columns): (Vec<FieldRef>, Vec<ArrayRef>) = params
        .into_iter()
        .map(|col| {
            let PreparedStatementParamColumn {
                name,
                dtype,
                nullable,
                array,
            } = col;
            (Arc::new(Field::new(name, dtype, nullable)), array)
        })
        .unzip();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(Into::into)
}
