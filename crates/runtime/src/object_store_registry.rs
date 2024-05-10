use std::{collections::HashMap, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use url::{form_urlencoded::parse, Url};

#[cfg(feature = "ftp")]
use crate::objectstore::ftp::FTPObjectStore;
#[cfg(feature = "ftp")]
use suppaftp::FtpStream;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Default)]
pub struct SpiceObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
}

impl SpiceObjectStoreRegistry {
    #[must_use]
    pub fn new() -> Self {
        SpiceObjectStoreRegistry::default()
    }

    fn get_feature_store(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        {
            if url.as_str().starts_with("s3://") {
                if let Some(bucket_name) = url.host_str() {
                    let mut s3_builder = AmazonS3Builder::from_env()
                        .with_bucket_name(bucket_name)
                        .with_allow_http(true);

                    let params: HashMap<String, String> =
                        parse(url.fragment().unwrap_or_default().as_bytes())
                            .into_owned()
                            .collect();

                    if let Some(region) = params.get("region") {
                        s3_builder = s3_builder.with_region(region);
                    }
                    if let Some(endpoint) = params.get("endpoint") {
                        s3_builder = s3_builder.with_endpoint(endpoint);
                    }
                    if let (Some(key), Some(secret)) = (params.get("key"), params.get("secret")) {
                        s3_builder = s3_builder.with_access_key_id(key);
                        s3_builder = s3_builder.with_secret_access_key(secret);
                    } else {
                        s3_builder = s3_builder.with_skip_signature(true);
                    };

                    return Ok(Arc::new(s3_builder.build()?));
                }
            }
            #[cfg(feature = "ftp")]
            if url.as_str().starts_with("ftp://") {
                let mut ftp_stream = FtpStream::connect("eu-central-1.sftpcloud.io:21").unwrap();
                let _ = ftp_stream.login("username", "password");
                let _ = ftp_stream.cwd("taxi_trips");
                let ftp_object_store = FTPObjectStore::new(ftp_stream);

                return Ok(Arc::new(ftp_object_store) as Arc<dyn ObjectStore>);
            }
        }

        Err(DataFusionError::Execution(format!(
            "No object store available for: {:?}/{}",
            url.host_str(),
            url.path(),
        )))
    }
}

impl ObjectStoreRegistry for SpiceObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url).or_else(|_| {
            let store = Self::get_feature_store(url)?;
            self.inner.register_store(url, Arc::clone(&store));

            Ok(store)
        })
    }
}

// This method uses unwrap_or_default, however it should never fail on the initialization. See
// RuntimeEnv::default()
pub(crate) fn default_runtime_env() -> Arc<RuntimeEnv> {
    Arc::new(
        RuntimeEnv::new(
            RuntimeConfig::default()
                .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::default())),
        )
        .unwrap_or_default(),
    )
}

pub mod macros {
    macro_rules! impl_listing_data_connector {
        ($connector:ty) => {
            use crate::object_store_registry::default_runtime_env;
            use async_trait::async_trait;
            use datafusion::datasource::listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            };
            use datafusion::datasource::TableProvider;
            use datafusion::execution::config::SessionConfig;
            use datafusion::execution::context::SessionContext;
            use std::any::Any;

            #[async_trait]
            impl DataConnector for $connector {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                async fn read_provider(
                    &self,
                    dataset: &Dataset,
                ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
                    let ctx = SessionContext::new_with_config_rt(
                        SessionConfig::new().set_bool(
                            "datafusion.execution.listing_table_ignore_subdirectory",
                            false,
                        ),
                        default_runtime_env(),
                    );

                    let url = self.get_object_store_url(dataset).context(
                        super::InvalidConfigurationSnafu {
                            dataconnector: stringify!($connector),
                            message: "Unable to parse URL",
                        },
                    )?;

                    let table_path = ListingTableUrl::parse(url).boxed().context(
                        super::InvalidConfigurationSnafu {
                            dataconnector: stringify!($connector).to_string(),
                            message: "Unable to parse URL",
                        },
                    )?;

                    let (file_format, extension) = self.get_file_format_and_extension().context(
                        super::InvalidConfigurationSnafu {
                            dataconnector: stringify!($connector).to_string(),
                            message: "Unable to resolve file_format and file_extension",
                        },
                    )?;
                    let options = ListingOptions::new(file_format).with_file_extension(&extension);

                    let resolved_schema = options
                        .infer_schema(&ctx.state(), &table_path)
                        .await
                        .boxed()
                        .context(super::InvalidConfigurationSnafu {
                            dataconnector: stringify!($connector).to_string(),
                            message: "Unable to infer files schema",
                        })?;

                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(options)
                        .with_schema(resolved_schema);

                    let table = ListingTable::try_new(config).boxed().context(
                        super::InvalidConfigurationSnafu {
                            dataconnector: stringify!($connector).to_string(),
                            message: "Unable to list files in data provider",
                        },
                    )?;

                    Ok(Arc::new(table))
                }
            }
        };
    }

    pub(crate) use impl_listing_data_connector;
}
