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

use std::collections::HashMap;
use std::sync::Arc;

use crate::component::dataset::replication::Replication;
use arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use snafu::prelude::*;

use crate::accelerated_table::Retention;
use crate::{
    accelerated_table::{refresh::Refresh, AcceleratedTable},
    dataaccelerator::{self, create_accelerator_table},
    dataconnector::{localhost::LocalhostConnector, DataConnector, DataConnectorError},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create data connector"))]
    NoReadWriteProvider {},

    #[snafu(display("Unable to create data connector"))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Unable to create source table provider"))]
    UnableToCreateSourceTableProvider { source: DataConnectorError },

    #[snafu(display("Unable to create accelerated table provider: {source}"))]
    UnableToCreateAcceleratedTableProvider { source: dataaccelerator::Error },
}

async fn get_local_table_provider(
    name: &str,
    schema: &Arc<Schema>,
) -> Result<Arc<dyn TableProvider>, Error> {
    let mut dataset = Dataset::new("localhost://internal".to_string(), name.to_string());
    dataset.mode = Mode::ReadWrite;

    let data_connector =
        Arc::new(LocalhostConnector::new(Arc::clone(schema))) as Arc<dyn DataConnector>;

    let source_table_provider = data_connector
        .read_write_provider(&dataset)
        .await
        .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        .context(UnableToCreateSourceTableProviderSnafu)?;

    Ok(source_table_provider)
}

pub async fn create_internal_accelerated_table(
    name: &str,
    schema: Arc<Schema>,
    acceleration: Acceleration,
    refresh: Refresh,
    retention: Option<Retention>,
) -> Result<Arc<AcceleratedTable>, Error> {
    let source_table_provider = get_local_table_provider(name, &schema).await?;

    let accelerated_table_provider =
        create_accelerator_table(name, Arc::clone(&schema), &acceleration, None)
            .await
            .context(UnableToCreateAcceleratedTableProviderSnafu)?;

    let mut builder = AcceleratedTable::builder(
        name.to_string(),
        source_table_provider,
        accelerated_table_provider,
        refresh,
    );

    builder.retention(retention);

    let (accelerated_table, _) = builder.build().await;

    Ok(Arc::new(accelerated_table))
}
