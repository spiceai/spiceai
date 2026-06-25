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

use super::{CatalogConnector, ConnectorComponent, ParameterSpec, Parameters};
use crate::catalogconnector::iceberg::{
    UnableToBuildCatalogClientSnafu, UnableToBuildCatalogSnafu,
};
use crate::component::dataset::builder::DatasetBuilder;
use crate::{
    App, Runtime,
    component::{catalog::Catalog, dataset::Dataset},
    dataconnector::{
        DataConnector, DataConnectorFactory,
        parameters::{ConnectorParams, ConnectorParamsBuilder},
        spiceai::{SpiceAI, SpiceAIDatasetPath, SpiceAIFactory},
    },
    parameters::ExposedParamLookup,
};
use async_trait::async_trait;
use data_components::{
    Read, RefreshableCatalogProvider, iceberg::catalog::rest::RestCatalog,
    spice_cloud::provider::SpiceCloudPlatformCatalogProvider,
};
use iceberg::{CatalogBuilder, NamespaceIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use snafu::prelude::*;
use spice_cloud_client::endpoints::{data_endpoint as spice_cloud_data_endpoint, is_valid_region};
use std::{any::Any, collections::HashMap, sync::Arc};
use tonic::metadata::MetadataValue;

#[derive(Debug, Snafu)]
pub enum Error {
    InvalidPath,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceCloudPlatformCatalog {
    params: Parameters,
}

impl SpiceCloudPlatformCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let (org, app, catalog_name) = Self::parse_and_validate_catalog_id(catalog)?;
        let catalog_client = self.create_rest_catalog_client(catalog).await?;
        let read_provider = self
            .create_read_provider(runtime, catalog, &org, &app, &catalog_name)
            .await?;

        let Ok(namespace_ident) = NamespaceIdent::from_vec(vec![org, app, catalog_name]) else {
            unreachable!("This only panics if the vec is empty");
        };

        let catalog_provider = SpiceCloudPlatformCatalogProvider::try_new(
            Arc::new(catalog_client),
            namespace_ident,
            read_provider,
            catalog.include.clone(),
        )
        .await
        .map_err(|e| super::Error::UnableToGetCatalogProvider {
            connector: "spice.ai".into(),
            connector_component: ConnectorComponent::from(catalog),
            source: Box::new(e),
        })?;

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }

    fn parse_and_validate_catalog_id(catalog: &Catalog) -> super::Result<(String, String, String)> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "spice.ai".into(),
                    message: "A Catalog Path is required for Spice.ai in the format of: <org>/<app>[/<catalog>] where <catalog> is optional. For details, visit: https://spiceai.org/docs/components/catalogs/spiceai#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        match parse_catalog_slug(catalog_id.as_str()) {
            Ok(result) => Ok(result),
            Err(e) => {
                Err(super::Error::InvalidConfiguration {
                    connector: "spice.ai".into(),
                    message: "A Catalog Path is required for Spice.ai in the format of: <org>/<app>[/<catalog>] where <catalog> is optional. For details, visit: https://spiceai.org/docs/components/catalogs/spiceai#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })
            }
        }
    }

    async fn create_rest_catalog_client(&self, catalog: &Catalog) -> super::Result<RestCatalog> {
        let endpoint = self.http_endpoint(catalog)?;
        let mut props = HashMap::new();
        if let Some(api_key) = self.api_key() {
            props.insert("token".to_string(), api_key.to_string());
        }

        let client = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .use_rustls_tls()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context(UnableToBuildCatalogClientSnafu)?;

        props.insert(REST_CATALOG_PROP_URI.to_string(), endpoint);
        let iceberg_rest_catalog = RestCatalogBuilder::default()
            .with_client(client)
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("rest", props)
            .await
            .context(UnableToBuildCatalogSnafu)?;

        Ok(RestCatalog::new(iceberg_rest_catalog))
    }

    async fn create_read_provider(
        &self,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
        org: &str,
        app: &str,
        catalog_name: &str,
    ) -> super::Result<Arc<dyn Read>> {
        let app_ref = runtime.app();
        let app_lock = app_ref.read().await;
        let runtime_app = match app_lock.as_ref() {
            Some(app) => Arc::clone(app),
            None => {
                return Err(super::Error::FailedToGetAppFromRuntime {});
            }
        };

        let connector_factory = self
            .create_data_connector(
                Arc::clone(&runtime),
                catalog,
                self.create_template_dataset(runtime, runtime_app),
            )
            .await?;

        let Some(data_connector) = connector_factory.as_any().downcast_ref::<SpiceAI>() else {
            unreachable!("Spice.ai is the only valid DataConnector");
        };

        let org_metadata = Self::create_metadata_value(org, catalog)?;
        let app_metadata = Self::create_metadata_value(app, catalog)?;

        let (flight_factory, _) = data_connector.flight_factory(SpiceAIDatasetPath::OrgAppPath {
            org: org_metadata,
            app: app_metadata,
            path: catalog_name.into(),
        });

        Ok(Arc::new(flight_factory))
    }

    fn create_template_dataset(&self, runtime: Arc<Runtime>, app: Arc<App>) -> Dataset {
        let Ok(template_dataset_builder) = DatasetBuilder::try_new("spice.ai".into(), "template")
        else {
            unreachable!("'template' is a valid dataset name");
        };

        let Ok(template_dataset) = template_dataset_builder
            .with_app(app)
            .with_runtime(runtime)
            .build()
        else {
            unreachable!("'template' is a valid dataset name");
        };

        let mut params = HashMap::new();
        if let Some(flight_endpoint) = self.flight_endpoint() {
            params.insert("spiceai_endpoint".to_string(), flight_endpoint.to_string());
        }

        if let Some(region) = self.region() {
            params.insert("spiceai_region".to_string(), region.to_string());
        }

        if let Some(api_key) = self.api_key() {
            params.insert("spiceai_api_key".to_string(), api_key.to_string());
        }

        template_dataset.with_params(params)
    }

    fn flight_endpoint(&self) -> Option<&str> {
        if let ExposedParamLookup::Present(endpoint) = self.params.get("endpoint").expose() {
            return Some(endpoint);
        }

        if let ExposedParamLookup::Present(flight_endpoint) =
            self.params.get("flight_endpoint").expose()
        {
            return Some(flight_endpoint);
        }

        None
    }

    fn http_endpoint(&self, catalog: &Catalog) -> super::Result<String> {
        if let ExposedParamLookup::Present(endpoint) = self.params.get("http_endpoint").expose() {
            return Ok(endpoint.to_string());
        }

        if let Some(region) = self.region() {
            if !is_valid_region(region) {
                return Err(super::Error::InvalidConfigurationNoSource {
                    connector: "spice.ai".into(),
                    message: format!(
                        "Invalid Spice Cloud region: {region}. Specify a valid region, for example 'spiceai_region: us-east-1'. To list available regions, run: 'spice cloud regions'"
                    ),
                    connector_component: ConnectorComponent::from(catalog),
                });
            }

            return Ok(spice_cloud_data_endpoint(region));
        }

        Err(super::Error::InvalidConfigurationNoSource {
            connector: "spice.ai".into(),
            message: "Missing Spice Cloud region. Specify a valid region, for example 'spiceai_region: us-east-1'. To list available regions, run: 'spice cloud regions'".to_string(),
            connector_component: ConnectorComponent::from(catalog),
        })
    }

    fn region(&self) -> Option<&str> {
        if let ExposedParamLookup::Present(region) = self.params.get("region").expose() {
            return Some(region);
        }

        None
    }

    fn api_key(&self) -> Option<&str> {
        if let ExposedParamLookup::Present(api_key) = self.params.get("api_key").expose() {
            return Some(api_key);
        }

        if let ExposedParamLookup::Present(token) = self.params.get("token").expose() {
            return Some(token);
        }

        None
    }

    async fn create_data_connector(
        &self,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
        template_dataset: Dataset,
    ) -> super::Result<Arc<dyn DataConnector>> {
        SpiceAIFactory::new()
            .create(
                ConnectorParamsBuilder::new(
                    "spice.ai".into(),
                    ConnectorComponent::Dataset(Arc::new(template_dataset)),
                )
                .build(runtime.secrets(), runtime.tokio_io_runtime())
                .await
                .map_err(|e| super::Error::InvalidConfiguration {
                    connector: "spice.ai".into(),
                    connector_component: ConnectorComponent::from(catalog),
                    message: e.to_string(),
                    source: e,
                })?,
            )
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: "spice.ai".into(),
                connector_component: ConnectorComponent::from(catalog),
                source: e,
            })
    }

    fn create_metadata_value(
        value: &str,
        catalog: &Catalog,
    ) -> super::Result<MetadataValue<tonic::metadata::Ascii>> {
        MetadataValue::try_from(value).map_err(|e| super::Error::InvalidConfiguration {
            connector: "spice.ai".into(),
            connector_component: ConnectorComponent::from(catalog),
            message: e.to_string(),
            source: Box::new(e),
        })
    }
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("api_key").secret(),
    ParameterSpec::component("token").secret(),
    ParameterSpec::component("region"),
    ParameterSpec::component("endpoint"),
    ParameterSpec::component("flight_endpoint"),
    ParameterSpec::component("http_endpoint"),
];

#[async_trait]
impl CatalogConnector for SpiceCloudPlatformCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        self.refreshable_catalog_provider(runtime, catalog).await
    }
}

fn parse_catalog_slug(catalog_slug: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = catalog_slug.split('/').collect();

    if parts.iter().any(|part| part.is_empty()) {
        return Err(Error::InvalidPath);
    }

    match parts.len() {
        2 | 3 => {
            let org = parts[0].to_string();
            let app = parts[1].to_string();
            let catalog = parts.get(2).map_or("spice", |&c| c).to_string();

            Ok((org, app, catalog))
        }
        _ => Err(Error::InvalidPath),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::catalog::CatalogBuilder;
    use secrecy::SecretString;
    use std::sync::Arc;

    use runtime_secrets::Secrets;
    use tokio::sync::RwLock;

    async fn make_test_catalog() -> Catalog {
        let app = app::AppBuilder::new("test").build();
        let runtime = crate::Runtime::builder().build().await;

        CatalogBuilder::try_new("spice.ai:org/app".to_string(), "test_catalog")
            .expect("catalog builder should be valid")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(runtime))
            .build()
            .expect("catalog should build")
    }

    #[test]
    fn test_parse_catalog_slug_org_and_app() {
        let (org, app, catalog) = parse_catalog_slug("myorg/myapp").expect("valid two-part slug");
        assert_eq!(org, "myorg");
        assert_eq!(app, "myapp");
        assert_eq!(catalog, "spice");
    }

    #[test]
    fn test_parse_catalog_slug_org_app_and_catalog() {
        let (org, app, catalog) =
            parse_catalog_slug("myorg/myapp/mycatalog").expect("valid three-part slug");
        assert_eq!(org, "myorg");
        assert_eq!(app, "myapp");
        assert_eq!(catalog, "mycatalog");
    }

    #[test]
    fn test_parse_catalog_slug_default_catalog_name() {
        let (_, _, catalog) = parse_catalog_slug("org/app").expect("valid two-part slug");
        assert_eq!(catalog, "spice", "default catalog should be 'spice'");
    }

    #[test]
    fn test_parse_catalog_slug_single_part_fails() {
        parse_catalog_slug("justorg").expect_err("single-part slug should be invalid");
    }

    #[test]
    fn test_parse_catalog_slug_four_parts_fails() {
        parse_catalog_slug("a/b/c/d").expect_err("four-part slug should be invalid");
    }

    #[test]
    fn test_parse_catalog_slug_empty_fails() {
        parse_catalog_slug("").expect_err("empty slug should be invalid");
    }

    #[test]
    fn test_parse_catalog_slug_trailing_slash() {
        parse_catalog_slug("org/app/").expect_err("trailing slash should produce invalid slug");
    }

    #[test]
    fn test_parse_catalog_slug_preserves_case() {
        let (org, app, catalog) =
            parse_catalog_slug("MyOrg/MyApp/MyCatalog").expect("valid three-part slug");
        assert_eq!(org, "MyOrg");
        assert_eq!(app, "MyApp");
        assert_eq!(catalog, "MyCatalog");
    }

    #[tokio::test]
    async fn test_flight_endpoint_prefers_endpoint_parameter() {
        let params = Parameters::try_new(
            "test",
            vec![
                (
                    "spiceai_endpoint".to_string(),
                    "grpc://new".to_string().into(),
                ),
                (
                    "spiceai_flight_endpoint".to_string(),
                    "grpc://legacy".to_string().into(),
                ),
            ],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(connector.flight_endpoint(), Some("grpc://new"));
    }

    #[tokio::test]
    async fn test_flight_endpoint_uses_legacy_flight_endpoint_parameter() {
        let params = Parameters::try_new(
            "test",
            vec![(
                "spiceai_flight_endpoint".to_string(),
                "grpc://legacy".to_string().into(),
            )],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(connector.flight_endpoint(), Some("grpc://legacy"));
    }

    #[tokio::test]
    async fn test_flight_endpoint_missing_returns_none() {
        let params = Parameters::try_new(
            "test",
            Vec::<(String, SecretString)>::new(),
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(connector.flight_endpoint(), None);
    }

    #[tokio::test]
    async fn test_http_endpoint_builds_regional_endpoint() {
        let params = Parameters::try_new(
            "test",
            vec![("spiceai_region".to_string(), "us-east-1".to_string().into())],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");
        let catalog = make_test_catalog().await;
        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(
            connector
                .http_endpoint(&catalog)
                .expect("region should build endpoint"),
            "https://us-east-1-prod-aws-data.spiceai.io"
        );
    }

    #[tokio::test]
    async fn test_http_endpoint_rejects_invalid_region() {
        let params = Parameters::try_new(
            "test",
            vec![(
                "spiceai_region".to_string(),
                "bad_region".to_string().into(),
            )],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");
        let catalog = make_test_catalog().await;
        let connector = SpiceCloudPlatformCatalog { params };

        assert!(matches!(
            connector.http_endpoint(&catalog),
            Err(super::super::Error::InvalidConfigurationNoSource { message, .. })
            if message.contains("Invalid Spice Cloud region: bad_region")
        ));
    }

    #[tokio::test]
    async fn test_http_endpoint_requires_region() {
        let params = Parameters::try_new(
            "test",
            Vec::<(String, SecretString)>::new(),
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");
        let catalog = make_test_catalog().await;
        let connector = SpiceCloudPlatformCatalog { params };

        assert!(matches!(
            connector.http_endpoint(&catalog),
            Err(super::super::Error::InvalidConfigurationNoSource { message, .. })
            if message.contains("Missing Spice Cloud region")
        ));
    }

    #[tokio::test]
    async fn test_http_endpoint_parameter_bypasses_region_validation() {
        let params = Parameters::try_new(
            "test",
            vec![
                (
                    "spiceai_http_endpoint".to_string(),
                    "https://custom.example.com".to_string().into(),
                ),
                (
                    "spiceai_region".to_string(),
                    "bad_region".to_string().into(),
                ),
            ],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");
        let catalog = make_test_catalog().await;
        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(
            connector
                .http_endpoint(&catalog)
                .expect("explicit endpoint should be used"),
            "https://custom.example.com"
        );
    }

    #[tokio::test]
    async fn test_api_key_prefers_api_key_parameter() {
        let params = Parameters::try_new(
            "test",
            vec![
                ("spiceai_api_key".to_string(), "api-key".to_string().into()),
                (
                    "spiceai_token".to_string(),
                    "legacy-token".to_string().into(),
                ),
            ],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(connector.api_key(), Some("api-key"));
    }

    #[tokio::test]
    async fn test_api_key_uses_legacy_token_parameter() {
        let params = Parameters::try_new(
            "test",
            vec![(
                "spiceai_token".to_string(),
                "legacy-token".to_string().into(),
            )],
            "spiceai",
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS,
        )
        .await
        .expect("parameters should be valid");

        let connector = SpiceCloudPlatformCatalog { params };

        assert_eq!(connector.api_key(), Some("legacy-token"));
    }
}
