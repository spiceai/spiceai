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
    path::PathBuf,
    process::{Child, Command},
};

use anyhow::Result;
use nix::unistd::Pid;
use spicepod::spec::SpicepodDefinition;
use tempfile::TempDir;

pub struct SpicedInstance {
    child: Child,
    _tempdir: TempDir,
}

impl SpicedInstance {
    pub fn start(spiced_path: PathBuf, spicepod: SpicepodDefinition) -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        // Serialize spicepod to `spicepod.yaml` in the tempdir
        let spicepod_yaml = serde_yaml::to_string(&spicepod)?;
        let spicepod_yaml_path = tempdir.path().join("spicepod.yaml");
        std::fs::write(spicepod_yaml_path.clone(), spicepod_yaml)?;

        // Start the spiced instance
        let mut cmd = Command::new(spiced_path);
        cmd.current_dir(tempdir.path());
        let child = cmd.spawn()?;

        Ok(Self {
            child,
            _tempdir: tempdir,
        })
    }

    pub fn stop(&mut self) -> Result<()> {
        // Send a SIGTERM to the spiced instance and wait for it to exit
        let Ok(pid_i32) = self.child.id().try_into() else {
            anyhow::bail!("Failed to convert pid to i32");
        };
        nix::sys::signal::kill(Pid::from_raw(pid_i32), nix::sys::signal::Signal::SIGTERM)?;
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for SpicedInstance {
    fn drop(&mut self) {
        match self.child.kill() {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to kill spiced instance: {}", e),
        }
    }
}
