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

use crate::Runtime;
use async_trait::async_trait;
use snafu::prelude::*;
use spicepod::component::extension::Extension as ExtensionComponent;

pub type ExtensionManifest = ExtensionComponent;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to initialize extension: {source}"))]
    UnableToInitializeExtension {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to start extension: {source}"))]
    UnableToStartExtension {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get catalog provider: {source}"))]
    UnableToGetCatalogProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

///
/// Extension trait
///
/// This trait is used to define the interface for extensions to the Spice runtime.
#[async_trait]
pub trait Extension: Send + Sync {
    fn name(&self) -> &'static str;

    async fn initialize(&mut self, runtime: &Runtime) -> Result<()>;

    async fn on_start(&self, runtime: &Runtime) -> Result<()>;
}

pub trait ExtensionFactory: Send + Sync {
    fn create(&self) -> Box<dyn Extension>;
}
