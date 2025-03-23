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

use anyhow::Result;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct MemoryReading {
    _timestamp: Instant,
    pub(crate) memory_usage: f64,
}

pub type MemoryReadingsHandle = JoinHandle<Result<Vec<MemoryReading>>>;

pub struct Process {
    pid: Pid,
}

impl Process {
    #[must_use]
    pub fn new(pid: Pid) -> Self {
        Self { pid }
    }

    #[must_use]
    pub fn watch_memory(
        &self,
        token: &CancellationToken,
    ) -> JoinHandle<Result<Vec<MemoryReading>>> {
        let token = token.clone();
        let pid = self.pid;
        tokio::spawn(async move {
            let mut readings = Vec::new();
            loop {
                if token.is_cancelled() {
                    break;
                }

                let memory_usage = Self::memory_usage(pid)?;
                let memory_usage_gb =
                    f64::from(u32::try_from(memory_usage / 1024 / 1024)?) / 1024.0;
                let reading = MemoryReading {
                    _timestamp: Instant::now(),
                    memory_usage: memory_usage_gb,
                };

                readings.push(reading);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            Ok(readings)
        })
    }

    /// Returns the memory usage in bytes for the process
    pub fn memory_usage(pid: Pid) -> Result<u64> {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);

        let Some(process) = system.process(pid) else {
            return Err(anyhow::anyhow!("Failed to get process"));
        };

        Ok(process.memory())
    }

    /// Show the memory usage of the spiced instance in GB
    /// Also returns the memory usage in GB as a float
    pub fn show_memory_usage(pid: Pid) -> Result<f64> {
        let memory_usage = Self::memory_usage(pid)?;
        // drop memory usage to MB as a u32 before converting to GB as a float
        // we don't really care about the fractional memory usage of KB/MB
        let memory_usage_gb = f64::from(u32::try_from(memory_usage / 1024 / 1024)?) / 1024.0;
        println!("Memory usage: {memory_usage_gb:.2} GB");

        Ok(memory_usage_gb)
    }
}
