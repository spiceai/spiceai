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

//! Build Cedar authorization [`Request`]s from Spice runtime context.

use cedar_policy::{Context, EntityId, EntityTypeName, EntityUid, Request, Schema};
use runtime_auth::AuthPrincipalRef;

use crate::entities::{SpiceResource, principal_uid, resource_entity_uid};
use crate::error::Error;

/// Known Spice authorization actions.
pub struct SpiceAction;

impl SpiceAction {
    pub const QUERY: &str = "query";
    pub const READ: &str = "read";
    pub const INSERT: &str = "insert";
    pub const UPDATE: &str = "update";
    pub const DELETE: &str = "delete";
    pub const DDL: &str = "ddl";
    pub const INVOKE: &str = "invoke";
    pub const EXECUTE: &str = "execute";
    pub const ACCESS: &str = "access";
}

/// Build the Cedar action [`EntityUid`] for an action name.
fn action_uid(action: &str) -> EntityUid {
    let Ok(type_name) = "Spice::Action".parse::<EntityTypeName>() else {
        unreachable!("constant type name Spice::Action is always valid");
    };
    EntityUid::from_type_name_and_id(type_name, EntityId::new(action))
}

/// Build a Cedar [`Request`] for an authorization check.
///
/// # Arguments
///
/// * `principal` - The authenticated user making the request
/// * `action` - The action being performed (use [`SpiceAction`] constants)
/// * `resource` - The Spice resource being accessed
/// * `schema` - The Cedar schema for request validation
///
/// # Errors
///
/// Returns an error if the Cedar request cannot be constructed.
pub fn build_request(
    principal: &AuthPrincipalRef,
    action: &str,
    resource: &SpiceResource,
    schema: &Schema,
) -> Result<Request, Error> {
    let principal = principal_uid(principal);
    let action = action_uid(action);
    let resource = resource_entity_uid(resource);
    let context = Context::empty();

    Request::new(principal, action, resource, context, Some(schema)).map_err(|e| {
        Error::EntityBuild {
            reason: format!("Failed to build Cedar request: {e}"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_uid_roundtrip() {
        let uid = action_uid("query");
        assert_eq!(AsRef::<str>::as_ref(uid.id()), "query");
        assert_eq!(uid.type_name().to_string(), "Spice::Action");
    }
}
