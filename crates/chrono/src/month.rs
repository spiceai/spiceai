/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2014-2026 Kang Seonghoon and contributors

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

/// A duration in calendar months
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Months(pub(crate) u32);

impl Months {
    /// Construct a new `Months` from a number of months
    pub const fn new(num: u32) -> Self {
        Self(num)
    }
}
