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
    pub fn new(project_dir: PathBuf, scheduler: NodeState, executors: Vec<NodeState>) -> Self {
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
    #[expect(dead_code)]
    pub fn all_nodes(&self) -> Vec<&NodeState> {
        let mut nodes = vec![&self.scheduler];
        nodes.extend(&self.executors);
        nodes
    }
}

/// Check if a cluster state file exists.
pub async fn state_exists(work_dir: &Path) -> bool {
    tokio::fs::try_exists(work_dir.join(STATE_FILE_NAME))
        .await
        .unwrap_or(false)
}

/// Load cluster state from file.
pub async fn load_state(work_dir: &Path) -> Result<ClusterState> {
    let state_path = work_dir.join(STATE_FILE_NAME);
    let contents = tokio::fs::read_to_string(&state_path)
        .await
        .context("Failed to read cluster state file")?;
    serde_json::from_str(&contents).context("Failed to parse cluster state file")
}

/// Save cluster state to file.
pub async fn save_state(state: &ClusterState, work_dir: &Path) -> Result<()> {
    tokio::fs::create_dir_all(work_dir)
        .await
        .context("Failed to create working directory")?;
    let state_path = work_dir.join(STATE_FILE_NAME);
    let contents = serde_json::to_string_pretty(state).context("Failed to serialize state")?;
    tokio::fs::write(&state_path, contents)
        .await
        .context("Failed to write cluster state file")?;
    Ok(())
}

/// Remove cluster state file.
pub async fn remove_state(work_dir: &Path) -> Result<()> {
    let state_path = work_dir.join(STATE_FILE_NAME);
    if state_path.exists() {
        tokio::fs::remove_file(&state_path)
            .await
            .context("Failed to remove cluster state file")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_state() -> ClusterState {
        let scheduler = NodeState {
            name: "scheduler".to_string(),
            pid: 1000,
            http_port: 8090,
            flight_port: Some(50051),
            node_port: 50052,
            work_dir: PathBuf::from("/tmp/scheduler"),
            log_file: PathBuf::from("/tmp/logs/scheduler.log"),
        };

        let executor = NodeState {
            name: "executor1".to_string(),
            pid: 1001,
            http_port: 9090,
            flight_port: None,
            node_port: 50062,
            work_dir: PathBuf::from("/tmp/executor1"),
            log_file: PathBuf::from("/tmp/logs/executor1.log"),
        };

        ClusterState::new(PathBuf::from("/tmp/project"), scheduler, vec![executor])
    }

    #[tokio::test]
    async fn test_save_and_load_state() {
        let temp_dir = TempDir::new().expect("failed to create temp directory");
        let state = create_test_state();

        // Save state
        save_state(&state, temp_dir.path())
            .await
            .expect("failed to save state");
        assert!(state_exists(temp_dir.path()).await);

        // Load state
        let loaded = load_state(temp_dir.path())
            .await
            .expect("failed to load state");
        assert_eq!(loaded.scheduler.pid, state.scheduler.pid);
        assert_eq!(loaded.executors.len(), 1);
        assert_eq!(loaded.executors[0].name, "executor1");
    }

    #[tokio::test]
    async fn test_state_exists_returns_false_initially() {
        let temp_dir = TempDir::new().expect("failed to create temp directory");
        assert!(!state_exists(temp_dir.path()).await);
    }

    #[tokio::test]
    async fn test_remove_state() {
        let temp_dir = TempDir::new().expect("failed to create temp directory");
        let state = create_test_state();

        save_state(&state, temp_dir.path())
            .await
            .expect("failed to save state");
        assert!(state_exists(temp_dir.path()).await);

        remove_state(temp_dir.path())
            .await
            .expect("failed to remove state");
        assert!(!state_exists(temp_dir.path()).await);
    }

    #[test]
    fn test_get_log_path_for_scheduler() {
        let state = create_test_state();
        let log_path = state.get_log_path("scheduler");
        assert!(log_path.is_some());
        assert_eq!(
            log_path.expect("log path should exist"),
            &state.scheduler.log_file
        );
    }

    #[test]
    fn test_get_log_path_for_executor() {
        let state = create_test_state();
        let log_path = state.get_log_path("executor1");
        assert!(log_path.is_some());
        assert_eq!(
            log_path.expect("log path should exist"),
            &state.executors[0].log_file
        );
    }

    #[test]
    fn test_get_log_path_for_nonexistent_component() {
        let state = create_test_state();
        let log_path = state.get_log_path("nonexistent");
        assert!(log_path.is_none());
    }

    #[test]
    fn test_list_components() {
        let state = create_test_state();
        let components = state.list_components();
        assert_eq!(components.len(), 2);
        assert!(components.contains(&"scheduler".to_string()));
        assert!(components.contains(&"executor1".to_string()));
    }
}
