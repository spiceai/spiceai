use async_trait::async_trait;
use data_components::{Read, Write};
use secrets::Secret;
use std::any::Any;
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};
use data_components::databricks::Databricks;

impl DataConnectorFactory for Databricks {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Box::new(Databricks::new(Arc::new(secret), params)) as Box<dyn DataConnector>)
        })
    }
}

pub type Result<T, E = Box<dyn Error + Send + Sync>> = std::result::Result<T, E>;

#[async_trait]
impl DataConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn read_provider(&self) -> &dyn Read {
        self
    }

    fn write_provider(&self) -> Option<&dyn Write> {
        Some(self)
    }
}
