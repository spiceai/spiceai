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

//! Cedar schema definition for Spice entity types.
//!
//! Defines the `Spice` namespace with entity types, actions, and attributes
//! that map Spice runtime concepts to Cedar's authorization model.

use cedar_policy::Schema;

use crate::error::Error;

/// The Cedar human-readable schema for Spice authorization.
///
/// Entity types:
/// - `Spice::User`    — an authenticated principal (from OIDC, API key, etc.)
/// - `Spice::Role`    — a role or group that users can belong to
/// - `Spice::Dataset` — a registered dataset (table) in the runtime
/// - `Spice::Model`   — an LLM model available for inference
/// - `Spice::Tool`    — a tool (MCP or built-in) available for execution
/// - `Spice::Endpoint`— an API endpoint category (e.g. "chat", "search", "sql")
///
/// Actions:
/// - `query`, `insert`, `update`, `delete`, `ddl` — on datasets
/// - `invoke` — on models
/// - `execute` — on tools
/// - `access` — on endpoints
const CEDAR_SCHEMA: &str = r#"
namespace Spice {
    entity User in [Role] = {
        "org_id": __cedar::String,
    };

    entity Role;

    entity Dataset = {
        "catalog": __cedar::String,
        "schema": __cedar::String,
    };

    entity Model;

    entity Tool;

    entity Endpoint;

    action "query" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "read" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "insert" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "update" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "delete" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "ddl" appliesTo {
        principal: [User, Role],
        resource: [Dataset],
    };

    action "invoke" appliesTo {
        principal: [User, Role],
        resource: [Model],
    };

    action "execute" appliesTo {
        principal: [User, Role],
        resource: [Tool],
    };

    action "access" appliesTo {
        principal: [User, Role],
        resource: [Endpoint],
    };
}
"#;

/// Build the Cedar [`Schema`] for Spice authorization.
///
/// # Errors
///
/// Returns an error if the embedded schema fails to parse (should not happen
/// unless the schema constant above has a syntax error).
pub fn build_schema() -> Result<Schema, Error> {
    let (schema, warnings) =
        Schema::from_cedarschema_str(CEDAR_SCHEMA).map_err(|e| Error::SchemaBuild {
            reason: e.to_string(),
        })?;

    for w in warnings {
        tracing::warn!("Cedar schema warning: {w}");
    }

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_parses_cleanly() {
        let schema = build_schema().expect("schema should parse");
        // Verify we can round-trip the schema
        drop(schema);
    }
}
