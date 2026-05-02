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

#[cfg(feature = "aws-secrets-manager")]
pub mod aws_secrets_manager;
#[cfg(feature = "azure-keyvault")]
pub mod azure_keyvault;
pub mod env;
#[cfg(feature = "hashicorp_vault")]
pub mod hashicorp_vault;
#[cfg(feature = "keyring-secret-store")]
pub mod keyring;
pub mod kubernetes;
pub mod scheduler_rpc;
