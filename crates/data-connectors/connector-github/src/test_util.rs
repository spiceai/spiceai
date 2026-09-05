/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Shared fixtures for the connector's unit tests.

use app::AppBuilder;
use data_connector_api::ConnectorComponent;
use runtime::builder::RuntimeBuilder;
use runtime::component::dataset::builder::DatasetBuilder;
use std::sync::{Arc, OnceLock};

/// Building a `ConnectorComponent` requires a full runtime + app construction.
/// Cache a single shared runtime so the unit tests don't spin up a tokio runtime
/// per invocation.
///
/// The tokio runtime is cached and never dropped: `RuntimeBuilder::build`
/// defaults `io_runtime` to `Handle::current()`, so dropping the runtime that
/// built it would leave the constructed `Runtime` holding handles to a dead
/// tokio runtime.
fn shared_runtime() -> &'static (tokio::runtime::Runtime, Arc<runtime::Runtime>) {
    static RUNTIME: OnceLock<(tokio::runtime::Runtime, Arc<runtime::Runtime>)> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Runtime::new().expect("to create tokio runtime");
        let spice_runtime = runtime.block_on(async { RuntimeBuilder::new().build().await });
        (runtime, Arc::new(spice_runtime))
    })
}

/// A `ConnectorComponent` for a GitHub dataset named `dataset_name`.
pub(crate) fn shared_component(dataset_name: &str) -> ConnectorComponent {
    let (_, spice_runtime) = shared_runtime();
    let app = AppBuilder::new("test").build();
    let dataset = DatasetBuilder::try_new("github".to_string(), dataset_name)
        .expect("to create dataset builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::clone(spice_runtime))
        .build()
        .expect("to create dataset");

    ConnectorComponent::from(&dataset)
}
