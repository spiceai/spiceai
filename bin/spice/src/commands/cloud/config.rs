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

//! Cloud configuration for linking to Spice Cloud apps.

use crate::error::{ConfigIoSnafu, CreateDirectorySnafu, Result};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fs;
use std::path::{Path, PathBuf};

const CLOUD_CONFIG_DIR: &str = ".spice";
const CLOUD_CONFIG_FILE: &str = "cloud.json";

/// Cloud link configuration for a Spice Cloud app.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudLink {
    pub org: String,
    pub app: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linked_at: Option<String>,
}

impl CloudLink {
    /// Get the full app name in org/app format.
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.org, self.app)
    }
}

/// Get the path to the cloud config file.
fn get_cloud_config_path() -> PathBuf {
    Path::new(CLOUD_CONFIG_DIR).join(CLOUD_CONFIG_FILE)
}

/// Load the cloud link configuration from the current directory.
pub fn load_cloud_link() -> Result<Option<CloudLink>> {
    let config_path = get_cloud_config_path();

    if !config_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&config_path).context(ConfigIoSnafu {
        operation: "read",
        path: config_path,
    })?;

    let link: CloudLink =
        serde_json::from_str(&content).map_err(|e| crate::error::Error::ConfigParse {
            message: format!("Failed to parse cloud config: {e}"),
        })?;

    Ok(Some(link))
}

/// Save the cloud link configuration to the current directory.
pub fn save_cloud_link(link: &CloudLink) -> Result<()> {
    let config_dir = Path::new(CLOUD_CONFIG_DIR);
    fs::create_dir_all(config_dir).context(CreateDirectorySnafu { path: config_dir })?;

    let config_path = get_cloud_config_path();
    let content =
        serde_json::to_string_pretty(link).map_err(|e| crate::error::Error::ConfigParse {
            message: format!("Failed to serialize cloud config: {e}"),
        })?;

    fs::write(&config_path, content).context(ConfigIoSnafu {
        operation: "write",
        path: config_path,
    })?;

    Ok(())
}

/// Remove the cloud link configuration from the current directory.
pub fn remove_cloud_link() -> Result<()> {
    let config_path = get_cloud_config_path();

    if config_path.exists() {
        fs::remove_file(&config_path).context(ConfigIoSnafu {
            operation: "delete",
            path: config_path,
        })?;
    }

    // Try to remove the .spice directory if it's empty
    let config_dir = Path::new(CLOUD_CONFIG_DIR);
    if config_dir.exists() {
        let _ = fs::remove_dir(config_dir);
    }

    Ok(())
}

/// Get the linked app name if available.
pub fn get_linked_app() -> Result<Option<String>> {
    let link = load_cloud_link()?;
    Ok(link.map(|l| l.full_name()))
}
