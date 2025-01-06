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
    path::Path,
    process::{Child, Command},
    time::Duration,
};

use anyhow::Result;
use flight_client::{Credentials, FlightClient};
use spicepod::spec::SpicepodDefinition;
use tempfile::TempDir;

use crate::utils::wait_until_true;

pub struct SpicedInstance {
    child: Child,
    _tempdir: TempDir,
}

impl SpicedInstance {
    /// Start a spiced instance
    ///
    /// # Errors
    ///
    /// - If spiced is already running
    /// - If the spiced instance fails to start
    /// - If the spicepod definition fails to serialize
    pub async fn start(spiced_path: &Path, spicepod: SpicepodDefinition) -> Result<Self> {
        // Check if spiced is already running
        let client = reqwest::Client::new();
        let response = client.get("http://localhost:8090/health").send().await;
        if response.is_ok() {
            anyhow::bail!("Spiced instance is already running");
        }

        let tempdir = tempfile::tempdir()?;
        // Serialize spicepod to `spicepod.yaml` in the tempdir
        let spicepod_yaml = serde_yaml::to_string(&spicepod)?;
        let spicepod_yaml_path = tempdir.path().join("spicepod.yaml");
        std::fs::write(spicepod_yaml_path.clone(), spicepod_yaml)?;

        // Start the spiced instance
        let mut cmd = Command::new(spiced_path);
        cmd.current_dir(tempdir.path());
        cmd.arg("--telemetry-enabled=false");
        let child = cmd.spawn()?;

        Ok(Self {
            child,
            _tempdir: tempdir,
        })
    }

    /// Get a flight client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the flight client fails to be created
    pub async fn flight_client(&self) -> Result<FlightClient> {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("user-agent", "spice-test-framework/1.0".parse()?);
        Ok(FlightClient::try_new(
            "http://localhost:50051".into(),
            Credentials::UsernamePassword {
                username: "".into(),
                password: "".into(),
            },
            Some(metadata),
        )
        .await?)
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

    /// Stop the spiced instance
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to exit
    pub fn stop(&mut self) -> Result<()> {
        // Send a SIGTERM to the spiced instance and wait for it to exit
        let Ok(pid_i32) = self.child.id().try_into() else {
            anyhow::bail!("Failed to convert pid to i32");
        };

        #[cfg(not(target_os = "windows"))]
        {
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
}

impl Drop for SpicedInstance {
    fn drop(&mut self) {
        match self.child.kill() {
            Ok(()) => (),
            Err(e) => eprintln!("Failed to kill spiced instance: {e}"),
        }
    }
}
