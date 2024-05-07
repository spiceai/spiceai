use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use url::Url;

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

    #[allow(clippy::unused_self)]
    fn get_feature_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        {
            if url.as_str().starts_with("s3://") {
                if let Some(bucket_name) = url.host_str() {
                    let store = Arc::new(
                        AmazonS3Builder::from_env()
                            .with_bucket_name(bucket_name)
                            .with_skip_signature(true)
                            .build()?,
                    );
                    return Ok(store);
                }
            }
        }

        Err(DataFusionError::Execution(format!(
            "No object store available for: {url}"
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
            let store = self.get_feature_store(url)?;
            self.inner.register_store(url, Arc::clone(&store));

            Ok(store)
        })
    }
}

// This method uses `expect` inside as this won't fail in normal case. See RuntimeEnv::default()
// Rather than let all callers handle this, use expect to simplify the return signature.
pub(crate) fn default_runtime_env() -> Arc<RuntimeEnv> {
    Arc::new(
        RuntimeEnv::new(
            RuntimeConfig::default()
                .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::default())),
        )
        .expect("Runtime env should always be created successfully with SpiceObjectStoreRegistry"),
    )
}
