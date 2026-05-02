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

//! K8s Operator policy provider — polls the Spice K8s Operator API for policies.

use cedar_policy::PolicySet;

use crate::engine::parse_policies;
use crate::error::Error;

use super::PolicyProvider;

/// Fetches Cedar policies from the Spice K8s Operator API.
pub struct OperatorPolicyProvider {
    endpoint: String,
}

impl OperatorPolicyProvider {
    #[must_use]
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
}

#[async_trait::async_trait]
impl PolicyProvider for OperatorPolicyProvider {
    async fn fetch_policies(&self) -> Result<PolicySet, Error> {
        // TODO: Implement HTTP fetch from the operator endpoint.
        // For now, return an empty policy set.
        tracing::warn!(
            endpoint = %self.endpoint,
            "Operator policy provider is not yet implemented; returning empty policy set"
        );
        parse_policies("")
    }
}
