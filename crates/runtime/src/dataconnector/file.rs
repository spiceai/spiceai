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

use crate::component::dataset::Dataset;
use secrets::Secret;
use snafu::prelude::*;
use std::any::Any;
use std::path::{self, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};
use url::{ParseError, Url};

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, InvalidConfigurationSnafu,
    ListingTableConnector,
};

pub struct File {
    params: Arc<HashMap<String, String>>,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "File")
    }
}

impl DataConnectorFactory for File {
    fn create(
        _secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Self { params }) as Arc<dyn DataConnector>) })
    }
}

impl ListingTableConnector for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let clean_from = dataset.from.replace("file://", "file:/");

        Url::parse(&clean_from)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "File".to_string(),
                message: "Invalid URL".to_string(),
            })
    }
}

impl File {
    pub fn url_from_dataset(dataset: &Dataset) -> Result<Url, ()> {
        let from  = dataset.from.to_string();
        let local_path = path::Path::new(&from);
        println!("local_path: {:?}", local_path);
        Url::from_file_path(local_path)
    }
}

#[cfg(test)]
mod test {
    use crate::component::dataset::Dataset;
    use url::Url;
    
    #[test]
    pub fn test_file() {
        println!("1: {:?}", super::File::url_from_dataset(&Dataset::try_new("/Users/jeadie/Github/jalaipeno/data_Q2_2023.parquet".to_string(), "test").unwrap()).unwrap());
        println!("4: {:?}", super::File::url_from_dataset(&Dataset::try_new("Github/jalaipeno/data_Q2_2023.parquet".to_string(), "test").unwrap()).unwrap());
        // println!("1: {:?}", super::File::url_from_dataset(&Dataset::try_new("".to_string(), "test").unwrap()).unwrap());
        // println!("1: {:?}", super::File::url_from_dataset(&Dataset::try_new("".to_string(), "test").unwrap()).unwrap());
        // println!("1: {:?}", super::File::url_from_dataset(&Dataset::try_new("".to_string(), "test").unwrap()).unwrap());

    

        assert!(false);
    }
}