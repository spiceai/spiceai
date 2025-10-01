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

use std::time::SystemTime;

use anyhow::{Context, Result};

use crate::spiced::SpicedInstance;

#[cfg(feature = "file_append")]
pub mod append;
pub mod datasets;
pub mod http;
pub mod search;

pub trait TestState {}
pub trait TestNotStarted: TestState {}
pub trait TestCompleted: TestState {
    fn end_time(&self) -> SystemTime;
}

/// A throughput test is a test that runs a set of queries in a loop until a condition is met
/// The test queries can also be run in parallel, each with the same end condition.
pub struct SpiceTest<S: TestState> {
    name: String,
    spiced_instance: Option<SpicedInstance>,
    start_time: SystemTime,
    use_progress_bars: bool,
    api_key: Option<String>,
    explain_plan_snapshot: bool,
    results_snapshot_predicate: Option<fn(&str) -> bool>,

    state: S,
}

impl<S: TestCompleted> SpiceTest<S> {
    /// Once the test has completed, return ownership of the spiced instance
    pub fn end(self) -> Result<SpicedInstance> {
        self.spiced_instance
            .context("Spiced instance should be present")
    }
}

impl<S: TestState> SpiceTest<S> {
    pub fn get_spiced(&self) -> Result<&SpicedInstance> {
        self.spiced_instance
            .as_ref()
            .context("Spiced instance should be present")
    }
}

impl<S: TestNotStarted> SpiceTest<S> {
    #[must_use]
    pub fn new(name: String, state: S) -> Self {
        Self {
            name,
            spiced_instance: None,
            start_time: SystemTime::now(),
            use_progress_bars: true,
            api_key: None,
            explain_plan_snapshot: false,
            results_snapshot_predicate: None,
            state,
        }
    }

    #[must_use]
    pub fn with_spiced_instance(mut self, spiced_instance: SpicedInstance) -> Self {
        self.spiced_instance = Some(spiced_instance);
        self
    }

    #[must_use]
    pub fn with_results_snapshot(mut self, predicate: fn(&str) -> bool) -> Self {
        self.results_snapshot_predicate = Some(predicate);
        self
    }

    #[must_use]
    pub fn with_explain_plan_snapshot(mut self) -> Self {
        self.explain_plan_snapshot = true;
        self
    }

    #[must_use]
    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key;
        self
    }

    #[must_use]
    pub fn with_progress_bars(mut self, use_progress_bars: bool) -> Self {
        self.use_progress_bars = use_progress_bars;
        self
    }
}
