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

//! Functions to build Cedar [`Entity`] objects from Spice runtime state.

use std::collections::{HashMap, HashSet};

use cedar_policy::{Entity, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use runtime_auth::AuthPrincipalRef;

use crate::error::Error;

/// Identifier for a Spice resource to be used in Cedar authorization requests.
#[derive(Debug, Clone)]
pub enum SpiceResource {
    Dataset {
        name: String,
        catalog: Option<String>,
        schema: Option<String>,
    },
    Model {
        name: String,
    },
    Tool {
        name: String,
    },
    Endpoint {
        name: String,
    },
}

impl SpiceResource {
    #[must_use]
    pub fn name(&self) -> &str {
        self.entity_id()
    }

    fn entity_type_name(&self) -> &'static str {
        match self {
            SpiceResource::Dataset { .. } => "Spice::Dataset",
            SpiceResource::Model { .. } => "Spice::Model",
            SpiceResource::Tool { .. } => "Spice::Tool",
            SpiceResource::Endpoint { .. } => "Spice::Endpoint",
        }
    }

    fn entity_id(&self) -> &str {
        match self {
            SpiceResource::Dataset { name, .. }
            | SpiceResource::Model { name }
            | SpiceResource::Tool { name }
            | SpiceResource::Endpoint { name } => name,
        }
    }
}

/// Build a Cedar [`EntityUid`] for a `Spice::User`.
fn user_uid(user_id: &str) -> EntityUid {
    let Ok(type_name) = "Spice::User".parse::<EntityTypeName>() else {
        unreachable!("constant type name Spice::User is always valid");
    };
    EntityUid::from_type_name_and_id(type_name, EntityId::new(user_id))
}

/// Build a Cedar [`EntityUid`] for a `Spice::Role`.
fn role_uid(role_name: &str) -> EntityUid {
    let Ok(type_name) = "Spice::Role".parse::<EntityTypeName>() else {
        unreachable!("constant type name Spice::Role is always valid");
    };
    EntityUid::from_type_name_and_id(type_name, EntityId::new(role_name))
}

/// Build a Cedar [`EntityUid`] for a resource.
fn resource_uid(resource: &SpiceResource) -> EntityUid {
    let Ok(type_name) = resource.entity_type_name().parse::<EntityTypeName>() else {
        unreachable!("constant resource type names are always valid");
    };
    EntityUid::from_type_name_and_id(type_name, EntityId::new(resource.entity_id()))
}

/// Build the Cedar entity set for an authorization request.
///
/// Creates:
/// - A `Spice::User` entity from the auth principal with role parents
/// - `Spice::Role` entities for each role/group the user belongs to
/// - A resource entity for the resource being accessed
///
/// # Errors
///
/// Returns an error if entity construction fails.
pub fn build_entities(
    principal: &AuthPrincipalRef,
    resource: &SpiceResource,
) -> Result<cedar_policy::Entities, Error> {
    let mut entities = Vec::new();

    // Build role entities (no parents, no attributes)
    let role_uids: HashSet<EntityUid> = if let Some(identity) = principal.identity_context() {
        identity.roles.iter().map(|r| role_uid(r)).collect()
    } else {
        principal.groups().iter().map(|g| role_uid(g)).collect()
    };

    for uid in &role_uids {
        let role_entity =
            Entity::new(uid.clone(), HashMap::new(), HashSet::new()).map_err(|e| {
                Error::EntityBuild {
                    reason: format!("Failed to build Role entity: {e}"),
                }
            })?;
        entities.push(role_entity);
    }

    // Build user entity with role parents and attributes
    let user_id = if let Some(identity) = principal.identity_context() {
        identity.user_id.clone()
    } else {
        principal.username().to_string()
    };

    let mut user_attrs: HashMap<String, RestrictedExpression> = HashMap::new();
    let org_id = principal
        .identity_context()
        .and_then(|id| id.org_id.clone())
        .unwrap_or_default();
    user_attrs.insert(
        "org_id".to_string(),
        RestrictedExpression::new_string(org_id),
    );

    let user_entity =
        Entity::new(user_uid(&user_id), user_attrs, role_uids).map_err(|e| Error::EntityBuild {
            reason: format!("Failed to build User entity: {e}"),
        })?;
    entities.push(user_entity);

    // Build resource entity
    let mut resource_attrs: HashMap<String, RestrictedExpression> = HashMap::new();
    if let SpiceResource::Dataset {
        catalog, schema, ..
    } = resource
    {
        resource_attrs.insert(
            "catalog".to_string(),
            RestrictedExpression::new_string(catalog.clone().unwrap_or_default()),
        );
        resource_attrs.insert(
            "schema".to_string(),
            RestrictedExpression::new_string(schema.clone().unwrap_or_default()),
        );
    }

    let resource_entity = Entity::new(resource_uid(resource), resource_attrs, HashSet::new())
        .map_err(|e| Error::EntityBuild {
            reason: format!("Failed to build resource entity: {e}"),
        })?;
    entities.push(resource_entity);

    cedar_policy::Entities::from_entities(entities, None).map_err(|e| Error::EntityBuild {
        reason: format!("Failed to build entity set: {e}"),
    })
}

/// Build the Cedar [`EntityUid`] for the principal in a request.
#[must_use]
pub fn principal_uid(principal: &AuthPrincipalRef) -> EntityUid {
    let user_id = if let Some(identity) = principal.identity_context() {
        identity.user_id.clone()
    } else {
        principal.username().to_string()
    };
    user_uid(&user_id)
}

/// Build the Cedar [`EntityUid`] for a resource.
#[must_use]
pub fn resource_entity_uid(resource: &SpiceResource) -> EntityUid {
    resource_uid(resource)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runtime_auth::{AuthPrincipal, identity::IdentityContext};

    use super::*;

    struct TestPrincipal {
        identity: IdentityContext,
    }

    impl AuthPrincipal for TestPrincipal {
        fn username(&self) -> &str {
            &self.identity.user_id
        }
        fn groups(&self) -> &[&str] {
            &[]
        }
        fn identity_context(&self) -> Option<&IdentityContext> {
            Some(&self.identity)
        }
    }

    #[test]
    fn test_build_entities_for_dataset_query() {
        let principal: AuthPrincipalRef = Arc::new(TestPrincipal {
            identity: IdentityContext::new("user-123")
                .with_org_id("org-456")
                .with_roles(vec!["analyst".into(), "viewer".into()]),
        });

        let resource = SpiceResource::Dataset {
            name: "my_table".to_string(),
            catalog: Some("spice".to_string()),
            schema: Some("public".to_string()),
        };

        let entities = build_entities(&principal, &resource).expect("should build entities");
        // 1 user + 2 roles + 1 dataset = 4 entities
        assert_eq!(entities.iter().count(), 4);
    }

    #[test]
    fn test_build_entities_for_model() {
        let principal: AuthPrincipalRef = Arc::new(TestPrincipal {
            identity: IdentityContext::new("user-1"),
        });

        let resource = SpiceResource::Model {
            name: "gpt-4o".to_string(),
        };

        let entities = build_entities(&principal, &resource).expect("should build entities");
        // 1 user + 0 roles + 1 model = 2 entities
        assert_eq!(entities.iter().count(), 2);
    }
}
