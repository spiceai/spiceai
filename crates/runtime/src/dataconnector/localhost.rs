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

use async_trait::async_trait;

use std::{any::Any, collections::HashMap, pin::Pin, sync::Arc};

use datafusion::datasource::TableProvider;
use futures::Future;
use secrets::Secret;
use spicepod::component::dataset::Dataset;

use super::{DataConnector, DataConnectorFactory};

/// A no-op connector that allows for Spice to act as a "sink" for data.
///
/// Configure an accelerator to store data - the localhost connector itself does nothing.
pub struct LocalhostConnector {}

impl DataConnectorFactory for LocalhostConnector {
    fn create(
        _secret: Option<Secret>,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(LocalhostConnector {}) as Arc<dyn DataConnector>) })
    }
}

#[async_trait]
impl DataConnector for LocalhostConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        unimplemented!("read_provider not yet implemented for locahost provider");
    }
}
