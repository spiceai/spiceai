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

//! Spice Cloud policy provider — polls the Spice Cloud Management API for policies.

use cedar_policy::PolicySet;

use crate::engine::parse_policies;
use crate::error::Error;

use super::PolicyProvider;

/// Fetches Cedar policies from the Spice Cloud Management API.
pub struct CloudPolicyProvider {
    // In the future, this will hold a CloudClient reference.
}

impl CloudPolicyProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for CloudPolicyProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PolicyProvider for CloudPolicyProvider {
    async fn fetch_policies(&self) -> Result<PolicySet, Error> {
        // TODO: Implement HTTP fetch from the Spice Cloud API.
        // For now, return an empty policy set.
        tracing::warn!("Cloud policy provider is not yet implemented; returning empty policy set");
        parse_policies("")
    }
}
