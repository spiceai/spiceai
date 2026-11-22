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

//! Object store wrappers for tests: injected latency, (in [`fault`]) injected
//! delivery faults, and (in [`crash`]) a process crash at a chosen write.
//! Results measured through [`LatencyStore`] are simulated latency, never an
//! object store's own numbers.

pub mod crash;
pub mod fault;
pub mod latency;

pub use latency::{LatencyProfile, LatencyStore, Op};
