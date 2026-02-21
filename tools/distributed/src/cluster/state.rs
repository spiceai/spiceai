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

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const STATE_FILE_NAME: &str = "cluster.state";
const STATE_VERSION: &str = "1.0";

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterState {
    version: String,
    started_at: DateTime<Utc>,
    project_dir: PathBuf,
    pub scheduler: NodeState,
    pub executors: Vec<NodeState>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeState {
    pub name: String,
    pub pid: u32,
    pub http_port: u16,
    pub flight_port: Option<u16>, // Only for scheduler
    pub node_port: u16,
    pub work_dir: PathBuf,
    pub log_file: PathBuf,
}

impl ClusterState {
    /// Create a new cluster state.
    #[must_use]
    pub fn new(
        project_dir: PathBuf,
        scheduler: NodeState,
        executors: Vec<NodeState>,
    ) -> Self {
        Self {
            version: STATE_VERSION.to_string(),
            started_at: Utc::now(),
            project_dir,
            scheduler,
            executors,
        }
    }

    /// Get the log file path for a given component name.
    #[must_use]
    pub fn get_log_path(&self, component: &str) -> Option<&PathBuf> {
        if component == "scheduler" {
            return Some(&self.scheduler.log_file);
        }
        self.executors
            .iter()
            .find(|e| e.name == component)
            .map(|e| &e.log_file)
    }

    /// List all component names in the cluster.
    #[must_use]
    pub fn list_components(&self) -> Vec<String> {
        let mut components = vec!["scheduler".to_string()];
        components.extend(self.executors.iter().map(|e| e.name.clone()));
        components
    }

    /// Get all nodes (scheduler + executors).
    pub fn all_nodes(&self) -> Vec<&NodeState> {
        let mut nodes = vec![&self.scheduler];
        nodes.extend(&self.executors);
        nodes
    }
}

/// Check if a cluster state file exists.
#[must_use]
pub fn state_exists(work_dir: &Path) -> bool {
    work_dir.join(STATE_FILE_NAME).exists()
}

/// Load cluster state from file.
pub fn load_state(work_dir: &Path) -> Result<ClusterState> {
    let state_path = work_dir.join(STATE_FILE_NAME);
    let contents =
        fs::read_to_string(&state_path).context("Failed to read cluster state file")?;
    serde_json::from_str(&contents).context("Failed to parse cluster state file")
}

/// Save cluster state to file.
pub fn save_state(state: &ClusterState, work_dir: &Path) -> Result<()> {
    fs::create_dir_all(work_dir).context("Failed to create working directory")?;
    let state_path = work_dir.join(STATE_FILE_NAME);
    let contents = serde_json::to_string_pretty(state).context("Failed to serialize state")?;
    fs::write(&state_path, contents).context("Failed to write cluster state file")?;
    Ok(())
}

/// Remove cluster state file.
pub fn remove_state(work_dir: &Path) -> Result<()> {
    let state_path = work_dir.join(STATE_FILE_NAME);
    if state_path.exists() {
        fs::remove_file(&state_path).context("Failed to remove cluster state file")?;
    }
    Ok(())
}
