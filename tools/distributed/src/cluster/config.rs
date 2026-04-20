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

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub num_executors: usize,
    pub scheduler: SchedulerConfig,
    pub executors: ExecutorConfig,
    pub paths: PathConfig,
    pub detach: bool,
    pub skip_tls_init: bool,
    pub skip_health_check: bool,
}

#[derive(Debug, Clone)]
#[expect(clippy::struct_field_names)]
pub struct SchedulerConfig {
    pub http_port: u16,
    pub flight_port: u16,
    pub node_port: u16,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub base_http_port: u16,
    pub base_node_port: u16,
}

#[derive(Debug, Clone)]
pub struct PathConfig {
    pub work_dir: PathBuf,
    pub log_dir: PathBuf,
    pub project_dir: PathBuf,
    pub spiced_path: PathBuf,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let spice_dir = home.join(".spice");

        Self {
            num_executors: 3,
            scheduler: SchedulerConfig {
                http_port: 8090,
                flight_port: 50051,
                node_port: 50052,
            },
            executors: ExecutorConfig {
                base_http_port: 9090,
                base_node_port: 50062,
            },
            paths: PathConfig {
                work_dir: spice_dir.join("distributed"),
                log_dir: spice_dir.join("distributed/logs"),
                project_dir: PathBuf::from("."),
                spiced_path: spice_dir.join("bin/spiced"),
            },
            detach: false,
            skip_tls_init: false,
            skip_health_check: false,
        }
    }
}

impl ClusterConfig {
    /// Get the HTTP port for a specific executor index (0-based).
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn executor_http_port(&self, index: usize) -> u16 {
        self.executors.base_http_port + index as u16
    }

    /// Get the node port for a specific executor index (0-based).
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn executor_node_port(&self, index: usize) -> u16 {
        self.executors.base_node_port + index as u16
    }

    /// Get the executor name for a specific index (0-based).
    #[must_use]
    #[expect(clippy::unused_self)]
    pub fn executor_name(&self, index: usize) -> String {
        format!("executor{}", index + 1)
    }
}
