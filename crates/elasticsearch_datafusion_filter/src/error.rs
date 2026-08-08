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

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Failed to translate the Elasticsearch filter for column {column}: the predicate was reported as pushable but produced no Elasticsearch query clause. This is an internal inconsistency; report it at https://github.com/spiceai/spiceai/issues"
    ))]
    PushableFilterNotTranslated { column: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
