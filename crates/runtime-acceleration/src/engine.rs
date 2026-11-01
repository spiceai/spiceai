/*
Copyright 2026 The Spice.ai OSS Authors
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
use crate::AcceleratorEngineNotAvailableSnafu;
use std::fmt::Display;

/// The acceleration engines. Every engine but [`Engine::Cayenne`] is deprecated in v2.4
/// and will be removed in v2.5.
///
/// The default is the engine named by [`spicepod::acceleration::DEFAULT_ENGINE`]: the
/// one an acceleration uses when it does not set `engine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Engine {
    Arrow,
    PartitionedArrow,
    DuckDB,
    Sqlite,
    Turso,
    PostgreSQL,
    #[default]
    Cayenne,
}

impl Engine {
    /// Returns the unpartitioned version of the engine. If an engine is not partitioned, it returns itself.
    #[must_use]
    pub fn to_unpartitioned(&self) -> Engine {
        match self {
            Engine::PartitionedArrow => Engine::Arrow,
            other => *other,
        }
    }

    /// Whether the engine keeps every memory-mode dataset in one in-process
    /// instance, whose tables outlive the dataset that loaded them until Spice
    /// stops: `DuckDB`, `SQLite`, and Turso share one connection pool in
    /// memory mode, while Arrow and Cayenne end a memory table with its dataset.
    #[must_use]
    pub fn shares_memory_instance(&self) -> bool {
        matches!(self, Engine::DuckDB | Engine::Sqlite | Engine::Turso)
    }

    /// Whether the engine reads table names that differ only in case as one
    /// table in a store: `SQLite` and Turso then keep two such datasets in one
    /// table, and `DuckDB` refuses to create the second.
    #[must_use]
    pub fn reads_case_variant_names_as_one_table(&self) -> bool {
        matches!(self, Engine::DuckDB | Engine::Sqlite | Engine::Turso)
    }
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Arrow | Engine::PartitionedArrow => write!(f, "arrow"),
            Engine::DuckDB => write!(f, "duckdb"),
            Engine::Sqlite => write!(f, "sqlite"),
            Engine::Turso => write!(f, "turso"),
            Engine::PostgreSQL => write!(f, "postgres"),
            Engine::Cayenne => write!(f, "cayenne"),
        }
    }
}

impl TryFrom<&str> for Engine {
    type Error = crate::Error;

    fn try_from(engine: &str) -> std::result::Result<Self, Self::Error> {
        match engine.to_lowercase().as_str() {
            "arrow" => Ok(Engine::Arrow),
            "duckdb" => Ok(Engine::DuckDB),
            "sqlite" => Ok(Engine::Sqlite),
            "turso" => Ok(Engine::Turso),
            "postgres" | "postgresql" => Ok(Engine::PostgreSQL),
            "cayenne" | "vortex" => Ok(Engine::Cayenne),
            _ => AcceleratorEngineNotAvailableSnafu {
                name: engine.to_string(),
            }
            .fail(),
        }
    }
}

impl TryFrom<String> for Engine {
    type Error = crate::Error;

    fn try_from(engine: String) -> std::result::Result<Self, Self::Error> {
        Engine::try_from(engine.as_str())
    }
}
