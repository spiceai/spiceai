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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Schema};
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::accelerated_table::Retention;
use crate::datafusion::DataFusion;
use crate::datafusion::Error as DataFusionError;
use crate::dataupdate::DataUpdate;
use crate::{
    accelerated_table::{refresh::Refresh, AcceleratedTable},
    dataaccelerator::{self, create_accelerator_table},
    dataconnector::{localhost::LocalhostConnector, DataConnector, DataConnectorError},
};
use datafusion::sql::TableReference;
use spicepod::component::dataset::{acceleration::Acceleration, Dataset, Mode};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create data connector"))]
    NoDataConnector {},

    #[snafu(display("Unable to create data connector"))]
    NoReadWriteProvider {},

    #[snafu(display("Unable to create accelerated table provider"))]
    UnableToCreateSourceTableProvider { source: DataConnectorError },

    #[snafu(display("Unable to create accelerated table provider: {source}"))]
    UnableToCreateAcceleratedTableProvider { source: dataaccelerator::Error },

    #[snafu(display("Error creating record batch: {source}",))]
    UnableToCreateRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Error writing to {name} table: {source}"))]
    UnableToWriteToTable {
        name: String,
        source: DataFusionError,
    },
}

pub struct InternalTable {
    name: String,
    schema: Arc<Schema>,
    accelerated_table: Arc<AcceleratedTable>,
}

impl InternalTable {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn accelerated_table(&self) -> Arc<AcceleratedTable> {
        Arc::clone(&self.accelerated_table)
    }

    pub async fn insert(
        &self,
        data_fusion: &Arc<RwLock<DataFusion>>,
        data: Vec<ArrayRef>,
    ) -> Result<(), Error> {
        let data_update = DataUpdate {
            schema: Arc::clone(&self.schema),
            data: vec![RecordBatch::try_new(Arc::clone(&self.schema), data)
                .context(UnableToCreateRecordBatchSnafu)?],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        let df = data_fusion.write().await;
        df.write_data(
            TableReference::partial("runtime", self.name.to_string()),
            data_update,
        )
        .await
        .context(UnableToWriteToTableSnafu {
            name: self.name().to_string(),
        })?;
        drop(df);

        Ok(())
    }

    pub async fn new(
        name: &str,
        schema: Arc<Schema>,
        acceleration: Acceleration,
        refresh: Refresh,
        retention: Option<Retention>,
    ) -> Result<Self, Error> {
        let mut dataset = Dataset::new("internal".to_string(), name.to_string());
        dataset.mode = Mode::ReadWrite;

        let data_connector =
            Arc::new(LocalhostConnector::new(Arc::clone(&schema))) as Arc<dyn DataConnector>;

        let source_table_provider = data_connector
            .read_write_provider(&dataset)
            .await
            .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
            .context(UnableToCreateSourceTableProviderSnafu)?;

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

        Ok(Self {
            name: name.to_string(),
            schema,
            accelerated_table: Arc::new(accelerated_table),
        })
    }
}
