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

//! Shared helper functions for creating table providers from AWS Glue table metadata.
//!
//! Used by both the Glue data connector (`connector-glue`) and the Glue catalog connector
//! (`catalogconnector::glue`). Lives in `runtime` because `create_s3_provider` depends on
//! `crate::dataconnector::s3::S3` (which is still in `runtime` until Phase 3).

use crate::{component::dataset::Dataset, dataconnector::s3::S3, parameters::Parameters};
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_glue::types::Table;
use aws_sdk_s3::config::ProvideCredentials;
use data_components::glue::InputFormat;
use datafusion::datasource::TableProvider;
use iceberg::{
    CatalogBuilder, NamespaceIdent, TableIdent,
    io::{S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN},
};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    GLUE_CATALOG_PROP_CATALOG_ID, GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder,
};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use snafu::prelude::*;
use std::{collections::HashMap, path::Path, sync::Arc};

const PREFIX: &str = "glue";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No AWS region specified."))]
    MissingRegion,
    #[snafu(display("Cannot retrieve AWS credentials."))]
    MissingCredentials,
    #[snafu(display("Invalid AWS credentials: {source}"))]
    InvalidCredentials { source: CredentialsError },
    #[snafu(display("Cannot retrieve metadata location for table '{table}': {message}"))]
    MissingMetadataLocation { table: String, message: String },
    #[snafu(display("No storage descriptor found for table '{table}'."))]
    MissingStorageDescriptor { table: String },
    #[snafu(display("No storage location specified for table '{table}'."))]
    MissingStorageLocation { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub async fn create_iceberg_provider(
    dataset: &Dataset,
    config: &SdkConfig,
    database: String,
    table: &Table,
) -> crate::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
    let region = config.region().ok_or_else(|| {
        let e = Error::MissingRegion;
        crate::dataconnector::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: e.to_string(),
            source: Box::new(e),
        }
    })?;

    let credentials = config
        .credentials_provider()
        .ok_or_else(|| {
            let e = Error::MissingCredentials;
            crate::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?
        .provide_credentials()
        .await
        .map_err(|e| {
            let e = Error::InvalidCredentials { source: e };
            crate::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: e.to_string(),
                source: Box::new(e),
            }
        })?;

    let metadata_location = get_metadata_location(table).map_err(|e| {
        crate::dataconnector::DataConnectorError::InvalidConfiguration {
            dataconnector: PREFIX.to_string(),
            connector_component: dataset.into(),
            message: e.to_string(),
            source: Box::new(e),
        }
    })?;

    let mut props = HashMap::from([
        (
            AWS_ACCESS_KEY_ID.to_string(),
            credentials.access_key_id().to_string(),
        ),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            credentials.secret_access_key().to_string(),
        ),
        (AWS_REGION_NAME.to_string(), region.to_string()),
        (
            S3_ACCESS_KEY_ID.to_string(),
            credentials.access_key_id().to_string(),
        ),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            credentials.secret_access_key().to_string(),
        ),
        (S3_REGION.to_string(), region.to_string()),
    ]);

    if let Some(session_token) = credentials.session_token() {
        props.insert(AWS_SESSION_TOKEN.to_string(), session_token.to_string());
        props.insert(S3_SESSION_TOKEN.to_string(), session_token.to_string());
    }

    // Disable OpenDAL's automatic credential loading from environment variables and config files.
    // As we provide explicit credentials, we don't want OpenDAL to pick up AWS_SESSION_TOKEN
    // or other credentials from the environment that may not be valid for this specific connection.
    props.insert("s3.disable-config-load".to_string(), "true".to_string());

    props.insert(
        GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
        metadata_location.clone(),
    );

    if let Some(catalog_id) = table.catalog_id.clone() {
        props.insert(GLUE_CATALOG_PROP_CATALOG_ID.to_string(), catalog_id);
    }

    // Derive the S3 scheme from the metadata location (e.g. "s3://" or "s3a://").
    // The Glue catalog's default StorageFactory uses "s3a" as the configured scheme,
    // but AWS Glue metadata locations typically use "s3://", causing a scheme mismatch.
    let s3_scheme = metadata_location
        .split("://")
        .next()
        .unwrap_or("s3")
        .to_string();

    let storage_factory: Arc<dyn iceberg::io::StorageFactory> =
        Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: s3_scheme,
            customized_credential_load: None,
        });

    let catalog = GlueCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("glue", props)
        .await
        .map_err(|e| {
            crate::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: dataset.into(),
                message: format!("Cannot initialize Glue catalog for dataset '{} (glue)'. Verify your AWS Glue configuration and credentials. For help, visit: https://docs.spiceai.org/components/data-connectors/glue", dataset.name),
                source: e.into(),
            }
    })?;

    let identifier = TableIdent::new(NamespaceIdent::new(database), table.name().to_string());

    let table_provider = IcebergTableProvider::try_new(
        Arc::new(catalog),
        identifier.namespace().clone(),
        identifier.name().to_string(),
    )
    .await
    .map_err(|e| crate::dataconnector::DataConnectorError::InvalidConfiguration {
        dataconnector: PREFIX.to_string(),
        connector_component: dataset.into(),
        message: format!("Cannot create table provider for Iceberg table '{}' for dataset '{} (glue)'. For help, visit: https://docs.spiceai.org/components/data-connectors/glue", table.name(), dataset.name),
        source: e.into(),
    })?;

    Ok(Arc::new(table_provider))
}

pub async fn create_s3_provider(
    input_format: InputFormat,
    mut dataset: Dataset,
    mut params: Parameters,
    table: &Table,
    tokio_io_runtime: tokio::runtime::Handle,
) -> crate::dataconnector::DataConnectorResult<Arc<dyn TableProvider>> {
    let Some(storage_descriptor) = table.storage_descriptor() else {
        let e = Error::MissingStorageDescriptor {
            table: table.name().to_string(),
        };
        return Err(
            crate::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: (&dataset).into(),
                message: e.to_string(),
                source: Box::new(e),
            },
        );
    };

    let Some(from) = storage_descriptor.location().map(String::from) else {
        let e = Error::MissingStorageLocation {
            table: table.name().to_string(),
        };
        return Err(
            crate::dataconnector::DataConnectorError::InvalidConfiguration {
                dataconnector: PREFIX.to_string(),
                connector_component: (&dataset).into(),
                message: e.to_string(),
                source: Box::new(e),
            },
        );
    };

    let from = ensure_s3_trailing_slash(&from);

    match input_format {
        InputFormat::Csv => {
            // If the table specifies a delimiter, pass it down to the data connector
            // as a parameter
            if let Some(delimiter) = table
                .parameters()
                .and_then(|params| params.get("delimiter"))
            {
                params.insert("csv_delimiter".to_string(), delimiter.as_str().into());
            }
        }
        InputFormat::Parquet => {
            dataset
                .params
                .insert("hive_partitioning_enabled".to_string(), "true".to_string());
        }
        InputFormat::Iceberg => {}
    }

    // Add required file_format parameter for S3
    params.insert("file_format".into(), input_format.file_format().into());
    let s3 = S3 {
        params,
        runtime: Some(Arc::unwrap_or_clone(dataset.runtime())),
        tokio_io_runtime,
    };

    dataset.from = from;

    use crate::dataconnector::DataConnector;
    s3.read_provider(&dataset).await
}

pub fn ensure_s3_trailing_slash(s3_location: &str) -> String {
    static PREFIX: &str = "s3://";

    if !s3_location.starts_with(PREFIX) {
        return s3_location.to_string();
    }

    let path_part = &s3_location[PREFIX.len()..];

    if path_part.ends_with('/') {
        return s3_location.to_string();
    }

    let path = Path::new(path_part);
    if path.extension().is_some() {
        return s3_location.to_string();
    }

    format!("{s3_location}/")
}

pub fn get_metadata_location(table: &Table) -> Result<String, Error> {
    const METADATA_LOCATION: &str = "metadata_location";
    match &table.parameters {
        Some(properties) => match properties.get(METADATA_LOCATION) {
            Some(location) => Ok(location.clone()),
            None => Err(Error::MissingMetadataLocation {
                table: table.name().to_string(),
                message: format!("No property '{METADATA_LOCATION}' found"),
            }),
        },
        None => Err(Error::MissingMetadataLocation {
            table: table.name().to_string(),
            message: "No parameters found".to_string(),
        }),
    }
}
