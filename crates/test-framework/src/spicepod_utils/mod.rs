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
    component::ComponentOrReference,
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
