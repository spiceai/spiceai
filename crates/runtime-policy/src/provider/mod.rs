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

//! Policy providers fetch Cedar policies from various sources.

pub mod cloud;
pub mod local;
pub mod operator;

use cedar_policy::PolicySet;

use crate::error::Error;

/// A source of Cedar authorization policies.
///
/// Implementations fetch policies from local files, a K8s operator, or Spice Cloud.
#[async_trait::async_trait]
pub trait PolicyProvider: Send + Sync {
    /// Fetch the current policy set from this provider.
    async fn fetch_policies(&self) -> Result<PolicySet, Error>;
}
