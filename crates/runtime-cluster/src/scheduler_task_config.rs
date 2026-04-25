/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Configuration for the periodic partition-management scheduler task.
//!
//! The task orchestrator itself (`PartitionManagementTask`) lives in the
//! `runtime` crate — it's tightly coupled to the runtime `DataFusion` struct,
//! `App`-driven metadata seeding, and `RuntimeStatus` reporting. Only the
//! configuration and `TryFrom` conversion from spicepod live here, so callers
//! that just want to parse config don't need to pull in runtime internals.

use std::time::Duration;

use snafu::prelude::*;

#[derive(Debug, Clone)]
pub struct PartitionManagementConfig {
    /// How often to run the management cycle
    pub interval: Duration,

    /// Maximum partitions to assign per cycle
    pub max_assignments_per_cycle: usize,

    /// Maximum partitions per executor (soft limit)
    pub max_partitions_per_executor: usize,

    /// How long to wait for partition discovery before timing out
    pub discovery_timeout: Duration,
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("Invalid partition management interval '{interval}': {source}"))]
    InvalidInterval {
        interval: String,
        source: fundu::ParseError,
    },

    #[snafu(display("Partition management interval must be greater than zero"))]
    IntervalIsZero,

    #[snafu(display("Invalid partition management discovery timeout '{timeout}': {source}"))]
    InvalidDiscoveryTimeout {
        timeout: String,
        source: fundu::ParseError,
    },

    #[snafu(display("Partition management discovery timeout must be greater than zero"))]
    DiscoveryTimeoutIsZero,
}

impl TryFrom<spicepod::component::runtime::PartitionManagement> for PartitionManagementConfig {
    type Error = ConfigError;

    fn try_from(
        config: spicepod::component::runtime::PartitionManagement,
    ) -> Result<Self, Self::Error> {
        let interval = fundu::parse_duration(&config.interval).context(InvalidIntervalSnafu {
            interval: &config.interval,
        })?;

        if interval.is_zero() {
            return Err(ConfigError::IntervalIsZero);
        }

        let discovery_timeout = fundu::parse_duration(&config.discovery_timeout).context(
            InvalidDiscoveryTimeoutSnafu {
                timeout: &config.discovery_timeout,
            },
        )?;

        if discovery_timeout.is_zero() {
            return Err(ConfigError::DiscoveryTimeoutIsZero);
        }

        Ok(Self {
            interval,
            max_assignments_per_cycle: config.max_assignments_per_cycle,
            max_partitions_per_executor: config.max_partitions_per_executor,
            discovery_timeout,
        })
    }
}

impl Default for PartitionManagementConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            max_assignments_per_cycle: 100,
            max_partitions_per_executor: 1000,
            discovery_timeout: Duration::from_secs(60),
        }
    }
}
