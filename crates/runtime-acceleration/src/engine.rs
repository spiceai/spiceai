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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum Engine {
    #[default]
    Arrow,
    DuckDB,
    PartitionedDuckDB,
    TableModePartitionedDuckDB,
    Sqlite,
    Turso,
    PostgreSQL,
    Cayenne,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Arrow => write!(f, "arrow"),
            Engine::DuckDB | Engine::PartitionedDuckDB | Engine::TableModePartitionedDuckDB => {
                write!(f, "duckdb")
            }
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
