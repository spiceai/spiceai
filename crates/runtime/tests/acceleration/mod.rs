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

use std::sync::LazyLock;

use spicepod::component::{dataset::acceleration::Mode, params::Params};
use tokio::sync::Mutex;

#[cfg(feature = "duckdb")]
mod checkpoint_duckdb;
#[cfg(feature = "postgres")]
mod checkpoint_postgres;
#[cfg(feature = "sqlite")]
mod checkpoint_sqlite;
#[cfg(feature = "sqlite")]
mod file_watcher;
#[cfg(all(feature = "postgres", feature = "duckdb", feature = "sqlite"))]
mod on_conflict;
mod query_push_down;
#[cfg(feature = "duckdb")]
mod single_instance_duckdb;

// Several acceleration tests need to use shared state from the acceleration registry.
// To avoid race conditions, use a mutex to ensure that the acceleration tests are run serially.
pub static ACCELERATION_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
