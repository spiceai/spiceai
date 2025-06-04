/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::{any::Any, pin::Pin, sync::Arc};

use async_trait::async_trait;
use aws_sdk_glue::{Client, types::Table};
use datafusion::catalog::TableProvider;

use crate::{
    component::dataset::Dataset,
    parameters::{ParameterSpec, Parameters},
};

use super::{
    DataConnector, DataConnectorFactory,
    parameters::{
        ConnectorParams,
        aws::{self, load_config},
    },
    s3::S3,
};

static PREFIX: &str = "glue";

#[derive(Clone, Debug)]
pub struct GlueDataConnector {
    params: Parameters,
}

impl GlueDataConnector {
    async fn client(&self) -> Result<Client, aws::Error> {
        let config = load_config(
            "GlueCatalogConnector",
            "region",
            "key",
            "secret",
            "session_token",
            &self.params,
        )
        .await?;

        Ok(Client::new(&config))
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct GlueDataConnectorFactory {}

impl GlueDataConnectorFactory {
    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for GlueDataConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let glue = GlueDataConnector {
                params: params.parameters,
            };
            Ok(Arc::new(glue) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        crate::dataconnector::s3::PARAMETERS.as_ref()
    }
}

#[async_trait]
impl DataConnector for GlueDataConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.parse_path(false, None).unwrap();
        let database = path.schema().unwrap();
        let table = path.table();

        let client = self.client().await.map_err(|e| {
            super::DataConnectorError::UnableToConnectInternal {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                source: e.into(),
            }
        })?;

        let get_table_output = client
            .get_table()
            .database_name(database)
            .name(table)
            .send()
            .await
            .unwrap();

        let table = get_table_output.table.unwrap();

        match InputFormat::try_from(&table).map_err(|_| todo!())? {
            InputFormat::Parquet => {
                create_parquet_provider(dataset.clone(), self.params.clone(), &table).await
            }
            InputFormat::Iceberg => todo!(),
        }
    }
}

enum InputFormat {
    // Avro,
    // Csv,
    // Json,
    // Xml,
    Parquet,
    // Orc,
    Iceberg,
}

impl TryFrom<&Table> for InputFormat {
    type Error = ();

    fn try_from(table: &Table) -> Result<Self, Self::Error> {
        if table
            .parameters
            .as_ref()
            .and_then(|params| params.get("table_type"))
            .is_some_and(|value| value.to_lowercase() == "iceberg")
        {
            return Ok(Self::Iceberg);
        }

        if table
            .storage_descriptor
            .as_ref()
            .and_then(|sd| sd.input_format.as_ref())
            .is_some_and(|input_format| {
                input_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            })
        {
            return Ok(Self::Parquet);
        }

        Err(())
    }
}

// async fn create_iceberg_provider(
//     &self,
//     name: &str,
//     table: &Table,
// ) -> DFResult<Option<Arc<dyn TableProvider>>> {
//     let metadata_location = get_metadata_location(table.parameters.as_ref(), name)
//         .map_err(|e| DataFusionError::External(Box::new(e)))?;

//     let identifier =
//         TableIdent::new(NamespaceIdent::new(self.database.clone()), name.to_string());

//     let config = GlueCatalogConfig::builder()
//         .warehouse(metadata_location)
//         .build();
//     let catalog = GlueCatalog::new(config)
//         .await
//         .map_err(|e| DataFusionError::External(e.into()))?;

//     let table_provider = IcebergTableProvider::try_new(Arc::new(catalog), identifier)
//         .await
//         .map_err(|e| {
//             DataFusionError::External(Box::new(super::Error::CreateIcebergTableProvider {
//                 source: e,
//             }))
//         })?;

//     Ok(Some(Arc::new(table_provider)))
// }

async fn create_parquet_provider(
    mut dataset: Dataset,
    mut params: Parameters,
    table: &Table,
) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
    let Some(storage_descriptor) = table.storage_descriptor() else {
        panic!();
    };

    let Some(mut from) = storage_descriptor.location().map(String::from) else {
        panic!();
    };

    if !from.ends_with('/') {
        from.push('/');
    }

    // Add required file_format parameter for S3
    params.insert("file_format".into(), "parquet".into());
    let s3 = S3 { params };

    // Modify the dataset for S3 parquet
    dataset.from = from;
    dataset
        .params
        .insert("hive_partitioning_enabled".to_string(), "true".to_string());

    s3.read_provider(&dataset).await
}
