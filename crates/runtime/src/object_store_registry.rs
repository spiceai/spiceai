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
                if let Some(host) = url.host() {
                    let params: HashMap<String, String> =
                        parse(url.fragment().unwrap_or_default().as_bytes())
                            .into_owned()
                            .collect();

                    let port = params
                        .get("port")
                        .map_or("21".to_string(), ToOwned::to_owned);
                    let user = params.get("user").map(ToOwned::to_owned).ok_or_else(|| {
                        DataFusionError::Execution("No user provided for FTP".to_string())
                    })?;
                    let password =
                        params
                            .get("password")
                            .map(ToOwned::to_owned)
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "No password provided for FTP".to_string(),
                                )
                            })?;

                    let ftp_object_store =
                        FTPObjectStore::new(user, password, host.to_string(), port);
                    return Ok(Arc::new(ftp_object_store) as Arc<dyn ObjectStore>);
                }
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
