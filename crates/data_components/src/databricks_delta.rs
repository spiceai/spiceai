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

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::{Engine, Table};
use secrecy::{ExposeSecret, Secret, SecretString};
use serde::Deserialize;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

use crate::Read;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("An error occured with Delta Table: {source}"))]
    DeltaTableError { source: delta_kernel::Error },

    #[snafu(display("An error occured with handling Arrow data: {source}"))]
    ArrowError { source: arrow::error::ArrowError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct DatabricksDelta {
    pub params: Arc<HashMap<String, SecretString>>,
}

impl DatabricksDelta {
    #[must_use]
    pub fn new(params: Arc<HashMap<String, SecretString>>) -> Self {
        Self { params }
    }
}

#[async_trait]
impl Read for DatabricksDelta {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        get_delta_table(table_reference, Arc::clone(&self.params)).await
    }
}

async fn get_delta_table(
    table_reference: TableReference,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let table_uri = resolve_table_uri(table_reference, Arc::clone(&params)).await?;

    let mut storage_options = HashMap::new();
    for (key, value) in params.iter() {
        if key == "token" {
            continue;
        }
        storage_options.insert(key.to_string(), value.expose_secret().to_string());
    }
    //storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());

    let delta_table = open_table_with_storage_options(table_uri, storage_options).await?;

    Ok(Arc::new(delta_table) as Arc<dyn TableProvider>)
}

#[derive(Deserialize)]
struct DatabricksTablesApiResponse {
    storage_location: String,
}

#[allow(clippy::implicit_hasher)]
pub async fn resolve_table_uri(
    table_reference: TableReference,
    params: Arc<HashMap<String, SecretString>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let Some(endpoint) = params.get("endpoint").map(Secret::expose_secret) else {
        return Err("Endpoint not found in dataset params".into());
    };

    let table_name = table_reference.table();

    let mut token = "Token not found in auth provider";
    if let Some(token_secret_val) = params.get("token").map(Secret::expose_secret) {
        token = token_secret_val;
    };

    let url = format!(
        "{}/api/2.1/unity-catalog/tables/{}",
        endpoint.trim_end_matches('/'),
        table_name
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).bearer_auth(token).send().await?;

    if response.status().is_success() {
        let api_response: DatabricksTablesApiResponse = response.json().await?;
        Ok(api_response.storage_location)
    } else {
        Err(format!(
            "Failed to retrieve databricks table URI. Status: {}",
            response.status()
        )
        .into())
    }
}

pub struct DeltaTable {
    table: Table,
    engine: Box<dyn Engine>,
}

impl DeltaTable {
    #[must_use]
    pub fn from(table_location: String) -> Result<Self> {
        let table = Table::try_from_uri(table_location).context(DeltaTableSnafu)?;

        let options: HashMap<String, String> = HashMap::new();
        let engine = Box::new(
            DefaultEngine::try_new(
                table.location(),
                options,
                Arc::new(TokioBackgroundExecutor::new()),
            )
            .context(DeltaTableSnafu)?,
        );

        let snapshot = table
            .snapshot(engine.as_ref(), None)
            .context(DeltaTableSnafu)?;

        let scan = ScanBuilder::new(snapshot)
            .build()
            .context(DeltaTableSnafu)?;

        let mut batches = vec![];
        for res in scan
            .execute(engine.as_ref())
            .context(DeltaTableSnafu)?
            .into_iter()
        {
            let data = res.raw_data.context(DeltaTableSnafu)?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))
                .context(DeltaTableSnafu)?
                .into();
            let batch = if let Some(mask) = res.mask {
                filter_record_batch(&record_batch, &mask.into()).context(ArrowSnafu)?
            } else {
                record_batch
            };
            batches.push(batch);
        }

        Ok(Self { table, engine })
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, datafusion::error::DataFusionError> {
        unimplemented!()
    }

    fn table_type(&self) -> TableType {
        todo!()
    }
}
