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

//! Readiness traits for the CDC systems that Spice connects to.
//!
//! Unlike full/append refreshes, there is no defined "done" state for a CDC system,
//! by definition the stream never completes. This poses a challenge to Spice on when
//! to consider an acceleration powered by CDC to be "ready".

pub struct Readiness {
    ready_future: Box<dyn Future<Output = ()> + Send + Unpin>,
}

impl Readiness {
    #[must_use]
    pub fn new(ready_future: Box<dyn Future<Output = ()> + Send + Unpin>) -> Self {
        Self { ready_future }
    }

    /// Returns a readiness check that reports it is ready immediately.
    #[must_use]
    pub fn immediate() -> Self {
        Self {
            ready_future: Box::new(Box::pin(async {})),
        }
    }

    pub async fn wait_until_ready(self) {
        self.ready_future.await;
    }
}
