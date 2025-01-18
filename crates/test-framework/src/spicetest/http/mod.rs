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

use std::{sync::Arc, time::Duration};

use component::HttpComponent;

pub mod component;
pub mod consistency;
pub mod overhead;

#[derive(Clone)]
pub struct HttpConfig {
    /// The total duration of the test.
    pub duration: Duration,

    /// The number of individial HTTP clients to make requests in parallel.
    pub concurrency: usize,

    /// The payloads to send to the component, specifically to be used in [`HttpComponent::send_request`].
    pub payloads: Vec<Arc<str>>,

    /// The HTTP component, within the Spiced instance, to test.
    pub component: HttpComponent,

    /// Duration to send requests before starting the test.
    pub warmup: Duration,

    /// If true, do not show a progress bar showing the duration of the test.
    pub disable_progress_bars: bool,
}
