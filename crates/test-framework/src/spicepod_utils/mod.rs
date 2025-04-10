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

use std::{collections::HashMap, path::PathBuf};

use app::App;
use spicepod::{
    component::{
        ComponentOrReference,
        dataset::{Dataset, Mode, replication::Replication},
        runtime::{ApiKey, ApiKeyAuth, Auth, Runtime},
    },
    param::Params,
    spec::{SpicepodDefinition, SpicepodKind, SpicepodVersion},
};

/// Load a spicepod definition from a file
///
/// # Errors
///
/// - If the file fails to be read
/// - If the file fails to be deserialized
pub fn load_spicepod(path: PathBuf) -> anyhow::Result<SpicepodDefinition> {
    let spicepod_str = std::fs::read_to_string(path)?;
    let spicepod: SpicepodDefinition = serde_yaml::from_str(&spicepod_str)?;
    Ok(spicepod)
}

/// Create a spicepod definition from an app
pub fn from_app(app: App) -> SpicepodDefinition {
    SpicepodDefinition {
        name: app.name,
        runtime: app.runtime,
        extensions: app.extensions,
        secrets: app.secrets,
        views: app
            .views
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        models: app
            .models
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        tools: app
            .tools
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        datasets: app
            .datasets
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        evals: app
            .evals
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        catalogs: app
            .catalogs
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        embeddings: app
            .embeddings
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        version: SpicepodVersion::default(),
        kind: SpicepodKind::default(),
        metadata: HashMap::default(),
        dependencies: Vec::default(),
    }
}

#[must_use]
pub fn make_spiceai_rw_dataset(path: &str, name: &str, api_key: Option<String>) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai:{path}"), name.to_string());
    ds.mode = Mode::ReadWrite;
    ds.replication = Some(Replication { enabled: true });
    if let Some(api_key) = api_key {
        ds.params = Some(Params::from_string_map(
            vec![("spiceai_api_key".to_string(), api_key)]
                .into_iter()
                .collect(),
        ));
    }
    ds
}

pub fn set_read_write_api_key(runtime: &mut Runtime, api_key: String) {
    runtime.auth = Some(Auth {
        api_key: Some(ApiKeyAuth {
            enabled: true,
            keys: vec![ApiKey::ReadWrite { key: api_key }],
        }),
    });
}
