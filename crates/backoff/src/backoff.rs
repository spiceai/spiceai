/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016 Tibor Benke

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

use std::time::Duration;

/// A backoff policy for retrying an operation.
pub trait Backoff {
    /// Resets the internal state to the initial value.
    fn reset(&mut self) {}

    /// Returns how long to wait before the next attempt, or `None` to stop
    /// retrying.
    fn next_backoff(&mut self) -> Option<Duration>;
}

impl<B: Backoff + ?Sized> Backoff for Box<B> {
    fn reset(&mut self) {
        (**self).reset();
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        (**self).next_backoff()
    }
}
