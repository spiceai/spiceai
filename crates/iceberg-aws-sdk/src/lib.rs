/*
Copyright 2025 The Spice.ai OSS Authors

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

mod credential_provider;
pub use credential_provider::S3CredentialProvider;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Failed to get credentials from environment"))]
    FailedToGetCredentials,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
