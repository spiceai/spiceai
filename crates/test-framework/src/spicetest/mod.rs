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

use crate::spiced::SpicedInstance;

pub mod datasets;
pub mod http;

pub trait TestState {}
pub trait TestNotStarted: TestState {}
pub trait TestCompleted: TestState {
    fn end_time(&self) -> SystemTime;
}

/// A throughput test is a test that runs a set of queries in a loop until a condition is met
/// The test queries can also be run in parallel, each with the same end condition.
pub struct SpiceTest<S: TestState> {
    name: String,
    spiced_instance: SpicedInstance,
    start_time: SystemTime,
    use_progress_bars: bool,

    state: S,
}

impl<S: TestCompleted> SpiceTest<S> {
    /// Once the test has completed, return ownership of the spiced instance
    #[must_use]
    pub fn end(self) -> SpicedInstance {
        self.spiced_instance
    }
}

impl<S: TestNotStarted> SpiceTest<S> {
    #[must_use]
    pub fn new(name: String, spice_instance: SpicedInstance, state: S) -> Self {
        Self {
            name,
            spiced_instance: spice_instance,
            start_time: SystemTime::now(),
            use_progress_bars: true,
            state,
        }
    }

    #[must_use]
    pub fn with_progress_bars(mut self, use_progress_bars: bool) -> Self {
        self.use_progress_bars = use_progress_bars;
        self
    }
}
