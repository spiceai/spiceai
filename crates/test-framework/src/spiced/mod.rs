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

use std::{
    fmt::Display,
    path::PathBuf,
    process::{Child, Command},
    time::Duration,
};

use anyhow::Result;
use flight_client::{Credentials, FlightClient};
use spicepod::spec::SpicepodDefinition;
use sysinfo::Pid;
use tempfile::TempDir;

use crate::{process::Process, utils::wait_until_true};

#[derive(Debug, Clone)]
pub struct SpicedVersion(String);
impl SpicedVersion {
    #[must_use]
    pub fn new(version: String) -> Self {
        Self(version)
    }
}

impl Display for SpicedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SpicedInstance {
    child: Child,
    tempdir: TempDir,
    version: SpicedVersion,
}

pub struct StartRequest {
    spiced_path: PathBuf,
    spicepod: SpicepodDefinition,
    tempdir: TempDir,
    data_dir: Option<PathBuf>,
    prepared: bool,
}

impl StartRequest {
    pub fn new(spiced_path: PathBuf, spicepod: SpicepodDefinition) -> Result<Self> {
        Ok(Self {
            spiced_path,
            spicepod,
            tempdir: TempDir::new()?,
            prepared: false,
            data_dir: None,
        })
    }

    #[must_use]
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    #[must_use]
    pub fn get_tempdir_path(&self) -> PathBuf {
        self.tempdir.path().to_path_buf()
    }

    pub fn prepare(&mut self) -> Result<()> {
        // Serialize spicepod to `spicepod.yaml` in the tempdir
        let spicepod_yaml = serde_yaml::to_string(&self.spicepod)?;
        let spicepod_yaml_path = self.tempdir.path().join("spicepod.yaml");
        std::fs::write(spicepod_yaml_path.clone(), spicepod_yaml)?;

        // Create a symlink to the data directory if one is set
        if let Some(data_dir) = &self.data_dir {
            // resolve the data directory path to an absolute path
            let data_dir = data_dir.canonicalize()?;

            let data_dir_symlink = self.tempdir.path().join("data");
            #[cfg(not(target_os = "windows"))]
            {
                std::os::unix::fs::symlink(data_dir, data_dir_symlink)?;
            }
            #[cfg(target_os = "windows")]
            {
                std::os::windows::fs::symlink_dir(data_dir, data_dir_symlink)?;
            }
        }

        self.prepared = true;

        Ok(())
    }
}

impl SpicedInstance {
    /// Start a spiced instance
    ///
    /// # Errors
    ///
    /// - If spiced is already running
    /// - If the spiced instance fails to start
    /// - If the spicepod definition fails to serialize
    pub async fn start(mut start_request: StartRequest) -> Result<Self> {
        // Check if spiced is already running
        let client = reqwest::Client::new();
        let response = client.get("http://localhost:8090/health").send().await;
        if response.is_ok() {
            anyhow::bail!("Spiced instance is already running");
        }

        if !start_request.prepared {
            start_request.prepare()?;
        }

        let tempdir = start_request.tempdir;

        // Get spiced version
        let version_cmd = Command::new(start_request.spiced_path.clone())
            .arg("--version")
            .output()?;

        if !version_cmd.status.success() {
            anyhow::bail!(
                "Failed to get spiced version: {}",
                String::from_utf8_lossy(&version_cmd.stderr)
            );
        }

        let version = String::from_utf8_lossy(&version_cmd.stdout).to_string();
        // take just the v1.0.0 part of the version
        let version = match (version.contains('-'), version.contains('+')) {
            (true, _) => version.split('-').next().unwrap_or(&version).to_string(),
            (false, true) => version.split('+').next().unwrap_or(&version).to_string(),
            (false, false) => version,
        };

        // Start the spiced instance
        let mut cmd = Command::new(start_request.spiced_path);
        cmd.current_dir(tempdir.path());
        cmd.arg("--telemetry-enabled=false");
        let child = cmd.spawn()?;

        Ok(Self {
            child,
            tempdir,
            version: SpicedVersion::new(version),
        })
    }

    #[must_use]
    pub fn version(&self) -> &str {
        self.version.0.as_str()
    }

    #[must_use]
    pub fn get_tempdir_path(&self) -> PathBuf {
        self.tempdir.path().to_path_buf()
    }

    /// Get a flight client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the flight client fails to be created
    pub async fn flight_client(&self, api_key: Option<String>) -> Result<FlightClient> {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("user-agent", "spice-test-framework/1.0".parse()?);

        let credentials = if let Some(api_key) = api_key {
            Credentials::UsernamePassword {
                username: "".into(),
                password: api_key.into(),
            }
        } else {
            Credentials::UsernamePassword {
                username: "".into(),
                password: "".into(),
            }
        };

        Ok(
            FlightClient::try_new("http://localhost:50051".into(), credentials, Some(metadata))
                .await?,
        )
    }

    /// Get an http client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the http client fails to be created
    pub fn http_client(&self) -> Result<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .user_agent("spice-test-framework/1.0")
            .build()?)
    }

    /// Wait for the spiced instance to be ready
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to be ready within the timeout
    pub async fn wait_for_ready(&mut self, timeout: Duration) -> Result<()> {
        // Wait for the spiced instance to be ready by polling the `/v1/ready` endpoint
        let client = self.http_client()?;
        if !wait_until_true(timeout, || async {
            let response = client.get("http://localhost:8090/v1/ready").send().await;
            match response {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            }
        })
        .await
        {
            anyhow::bail!("Spiced instance not ready within {timeout:?}");
        }
        Ok(())
    }

    pub async fn is_ready(&self) -> bool {
        let Ok(client) = self.http_client() else {
            return false;
        };
        let response = client.get("http://localhost:8090/v1/ready").send().await;
        match response {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Stop the spiced instance
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to exit
    pub fn stop(&mut self) -> Result<()> {
        #[cfg(not(target_os = "windows"))]
        {
            // Send a SIGTERM to the spiced instance and wait for it to exit
            let Ok(pid_i32) = self.child.id().try_into() else {
                anyhow::bail!("Failed to convert pid to i32");
            };
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid_i32),
                nix::sys::signal::Signal::SIGTERM,
            )?;
            self.child.wait()?;
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, we can use the built-in process termination
            self.child.kill()?;
            self.child.wait()?;
        }

        Ok(())
    }

    /// Returns an instance of a `Process` for the spiced instance
    /// This allows tracking the spiced process, without owning the spiced instance
    #[must_use]
    pub fn process(&self) -> Process {
        Process::new(Pid::from_u32(self.child.id()))
    }
}

impl Drop for SpicedInstance {
    fn drop(&mut self) {
        match self.child.kill() {
            Ok(()) => (),
            Err(e) => eprintln!("Failed to kill spiced instance: {e}"),
        }
    }
}
