/*
Copyright 2024 The Spice.ai OSS Authors

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

use spicepod::component::{dataset::acceleration::Mode, params::Params};

#[cfg(feature = "duckdb")]
mod metadata_duckdb;
#[cfg(feature = "postgres")]
mod metadata_postgres;
#[cfg(feature = "sqlite")]
mod metadata_sqlite;
#[cfg(all(feature = "postgres", feature = "duckdb", feature = "sqlite"))]
mod on_conflict;
mod query_push_down;

fn get_params(mode: &Mode, file: Option<String>, engine: &str) -> Option<Params> {
    let param_name = format!("{engine}_file",);
    if mode == &Mode::File {
        return Some(Params::from_string_map(
            vec![(param_name, file.unwrap_or_default())]
                .into_iter()
                .collect(),
        ));
    }
    None
}
