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
use crate::{AcceleratorEngineNotAvailableSnafu, AcceleratorEngineRemovedSnafu};
use std::fmt::Display;

/// Engines removed in v2.5. The parser still recognizes their names, so an acceleration
/// that sets one fails with [`removed_engine_message`], which names the fix, rather than
/// with the unknown-engine error. Spice never switches such an acceleration to Cayenne on
/// its own: rebuilding it would discard data its source may not have.
const REMOVED_ENGINES: &[&str] = &["turso"];

/// Where the migration from a removed engine to Cayenne is documented.
pub const CAYENNE_MIGRATION_DOCS: &str =
    "https://spiceai.org/docs/components/data-accelerators/cayenne/migration";

/// Why an acceleration that sets the removed engine `engine` cannot load, worded as the
/// cause that follows "Failed to load dataset '…': ".
#[must_use]
pub fn removed_engine_message(engine: &str) -> String {
    format!(
        "the acceleration engine `{engine}` is no longer supported as of v2.5 and cannot be used. Set `acceleration.engine: cayenne`, or remove `engine` to use the default, which is `cayenne`, then delete the old engine's acceleration files and snapshots. See: {CAYENNE_MIGRATION_DOCS}"
    )
}

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
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Arrow | Engine::PartitionedArrow => write!(f, "arrow"),
            Engine::DuckDB => write!(f, "duckdb"),
            Engine::Sqlite => write!(f, "sqlite"),
            Engine::PostgreSQL => write!(f, "postgres"),
            Engine::Cayenne => write!(f, "cayenne"),
        }
    }
}

impl TryFrom<&str> for Engine {
    type Error = crate::Error;

    fn try_from(engine: &str) -> std::result::Result<Self, Self::Error> {
        let name = engine.to_lowercase();
        if let Some(removed) = REMOVED_ENGINES.iter().find(|removed| **removed == name) {
            return AcceleratorEngineRemovedSnafu { name: *removed }.fail();
        }
        match name.as_str() {
            "arrow" => Ok(Engine::Arrow),
            "duckdb" => Ok(Engine::DuckDB),
            "sqlite" => Ok(Engine::Sqlite),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_removed_engine_fails_with_the_removal_message() {
        for spelling in ["turso", "Turso", "TURSO"] {
            let err = Engine::try_from(spelling).expect_err("turso is removed");
            assert_eq!(
                err.to_string(),
                "the acceleration engine `turso` is no longer supported as of v2.5 and cannot be used. Set `acceleration.engine: cayenne`, or remove `engine` to use the default, which is `cayenne`, then delete the old engine's acceleration files and snapshots. See: https://spiceai.org/docs/components/data-accelerators/cayenne/migration",
                "{spelling}"
            );
        }
    }

    #[test]
    fn the_unknown_engine_error_does_not_list_a_removed_engine() {
        let err = Engine::try_from("nosuchengine").expect_err("unknown engine");
        assert_eq!(
            err.to_string(),
            "Unknown acceleration engine 'nosuchengine'. Valid engines are: arrow, duckdb, sqlite, postgres/postgresql, cayenne/vortex. Docs: https://spiceai.org/docs/components/data-accelerators"
        );
    }
}
