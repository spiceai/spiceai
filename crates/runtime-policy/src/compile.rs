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

//! Compile Cedar policy annotations into Spice fine-grained data access plans.

use std::collections::HashMap;

use cedar_policy::{Effect, Policy, PolicyId, PolicySet};

use crate::entities::SpiceResource;
use crate::error::Error;

const ROW_FILTER_KEY: &str = "row_filter";
const ROW_FILTER_PREFIX: &str = "row_filter_";
const TARGET_TABLE_KEY: &str = "target_table";
const COLUMN_MASK_KEY: &str = "column_mask";
const COLUMN_MASK_PREFIX: &str = "column_mask_";
const MASK_PREFIX: &str = "mask_";
const COLUMN_MASK_TAG_KEY: &str = "column_mask_tag";
const COLUMN_MASK_TAG_PREFIX: &str = "column_mask_tag_";
const MASK_TAG_PREFIX: &str = "mask_tag_";

/// A column mask compiled from a Cedar policy annotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMask {
    /// Column to replace.
    pub column: String,
    /// SQL scalar expression used to replace the column value.
    pub expression: String,
}

/// A tag-targeted column mask compiled from a Cedar policy annotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagMask {
    /// Column tag to match.
    pub tag: String,
    /// SQL scalar expression used to replace matching column values.
    pub expression: String,
}

/// Fine-grained access plan for a dataset read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessPlan {
    /// Whether the read action is allowed by Cedar.
    pub allowed: bool,
    /// Cedar policy ids that caused the decision.
    pub policy_ids: Vec<String>,
    /// SQL boolean expressions to apply as row filters. Multiple filters are AND-combined by callers.
    pub row_filters: Vec<String>,
    /// SQL scalar expressions to apply as column masks.
    pub column_masks: Vec<ColumnMask>,
    /// SQL scalar expressions to apply to every column with a matching tag.
    pub tag_masks: Vec<TagMask>,
}

impl AccessPlan {
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.row_filters.is_empty() && self.column_masks.is_empty() && self.tag_masks.is_empty()
    }
}

/// Validate fine-grained policy annotations at load time.
///
/// # Errors
///
/// Returns an error if a policy annotation is malformed or attached to a `forbid` policy.
pub fn validate_policy_annotations(policy_set: &PolicySet) -> Result<(), Error> {
    for policy in policy_set.policies() {
        if policy.effect() == Effect::Forbid && has_fine_grained_annotation(policy) {
            return Err(Error::PolicyAnnotation {
                policy_id: policy.id().to_string(),
                reason: "fine-grained row filters and column masks are only supported on permit policies".to_string(),
            });
        }

        for (key, value) in policy.annotations() {
            validate_annotation(policy.id(), key, value)?;
        }
    }

    Ok(())
}

/// Compile annotations from the policies that contributed to an allow decision.
///
/// # Errors
///
/// Returns an error if the matching policy annotations are malformed or contain conflicting masks.
pub fn compile_access_plan<'a>(
    policy_set: &PolicySet,
    permit_policy_ids: impl IntoIterator<Item = &'a PolicyId>,
    resource: &SpiceResource,
) -> Result<AccessPlan, Error> {
    let mut plan = AccessPlan {
        allowed: true,
        ..AccessPlan::default()
    };
    let mut masks_by_column: HashMap<String, String> = HashMap::new();
    let mut masks_by_tag: HashMap<String, String> = HashMap::new();

    for policy_id in permit_policy_ids {
        let Some(policy) = policy_set.policy(policy_id) else {
            continue;
        };
        if policy.effect() != Effect::Permit
            || !policy_applies_to_resource_annotation(policy, resource)
        {
            continue;
        }

        plan.policy_ids.push(policy.id().to_string());

        for (key, value) in policy.annotations() {
            if is_row_filter_key(key) {
                plan.row_filters
                    .push(non_empty_annotation_value(policy.id(), key, value)?.to_string());
                continue;
            }

            if let Some(mask) = column_mask_from_annotation(policy.id(), key, value)? {
                match masks_by_column.get(&mask.column) {
                    Some(existing) if existing != &mask.expression => {
                        return Err(Error::PolicyAnnotation {
                            policy_id: policy.id().to_string(),
                            reason: format!(
                                "conflicting column masks for column '{}'",
                                mask.column
                            ),
                        });
                    }
                    Some(_) => {}
                    None => {
                        masks_by_column.insert(mask.column.clone(), mask.expression.clone());
                    }
                }
            }

            if let Some(mask) = tag_mask_from_annotation(policy.id(), key, value)? {
                match masks_by_tag.get(&mask.tag) {
                    Some(existing) if existing != &mask.expression => {
                        return Err(Error::PolicyAnnotation {
                            policy_id: policy.id().to_string(),
                            reason: format!("conflicting tag masks for tag '{}'", mask.tag),
                        });
                    }
                    Some(_) => {}
                    None => {
                        masks_by_tag.insert(mask.tag.clone(), mask.expression.clone());
                    }
                }
            }
        }
    }

    plan.column_masks = masks_by_column
        .into_iter()
        .map(|(column, expression)| ColumnMask { column, expression })
        .collect();
    plan.column_masks
        .sort_by(|left, right| left.column.cmp(&right.column));
    plan.tag_masks = masks_by_tag
        .into_iter()
        .map(|(tag, expression)| TagMask { tag, expression })
        .collect();
    plan.tag_masks
        .sort_by(|left, right| left.tag.cmp(&right.tag));

    Ok(plan)
}

fn validate_annotation(policy_id: &PolicyId, key: &str, value: &str) -> Result<(), Error> {
    if key == TARGET_TABLE_KEY {
        non_empty_annotation_value(policy_id, key, value)?;
    }

    if is_row_filter_key(key) {
        non_empty_annotation_value(policy_id, key, value)?;
    }

    let _ = column_mask_from_annotation(policy_id, key, value)?;
    let _ = tag_mask_from_annotation(policy_id, key, value)?;
    Ok(())
}

fn has_fine_grained_annotation(policy: &Policy) -> bool {
    policy.annotations().any(|(key, _)| {
        is_row_filter_key(key)
            || key == COLUMN_MASK_KEY
            || key.starts_with(COLUMN_MASK_PREFIX)
            || key.starts_with(MASK_PREFIX)
            || key == COLUMN_MASK_TAG_KEY
            || key.starts_with(COLUMN_MASK_TAG_PREFIX)
            || key.starts_with(MASK_TAG_PREFIX)
    })
}

fn is_row_filter_key(key: &str) -> bool {
    key == ROW_FILTER_KEY || key.starts_with(ROW_FILTER_PREFIX)
}

fn policy_applies_to_resource_annotation(policy: &Policy, resource: &SpiceResource) -> bool {
    policy
        .annotation(TARGET_TABLE_KEY)
        .is_none_or(|target_table| target_table.trim() == resource.name())
}

fn non_empty_annotation_value<'a>(
    policy_id: &PolicyId,
    key: &str,
    value: &'a str,
) -> Result<&'a str, Error> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::PolicyAnnotation {
            policy_id: policy_id.to_string(),
            reason: format!("annotation '{key}' must have a non-empty value"),
        });
    }
    Ok(trimmed)
}

fn column_mask_from_annotation(
    policy_id: &PolicyId,
    key: &str,
    value: &str,
) -> Result<Option<ColumnMask>, Error> {
    if key == COLUMN_MASK_TAG_KEY
        || key.starts_with(COLUMN_MASK_TAG_PREFIX)
        || key.starts_with(MASK_TAG_PREFIX)
    {
        return Ok(None);
    }

    if key == COLUMN_MASK_KEY {
        let value = non_empty_annotation_value(policy_id, key, value)?;
        let Some((column, expression)) = value.split_once('=') else {
            return Err(Error::PolicyAnnotation {
                policy_id: policy_id.to_string(),
                reason: "annotation 'column_mask' must use the format '<column>=<sql expression>'"
                    .to_string(),
            });
        };
        return Ok(Some(build_column_mask(policy_id, key, column, expression)?));
    }

    if let Some(column) = key.strip_prefix(COLUMN_MASK_PREFIX) {
        return Ok(Some(build_column_mask(policy_id, key, column, value)?));
    }

    if let Some(column) = key.strip_prefix(MASK_PREFIX) {
        return Ok(Some(build_column_mask(policy_id, key, column, value)?));
    }

    Ok(None)
}

fn build_column_mask(
    policy_id: &PolicyId,
    key: &str,
    column: &str,
    expression: &str,
) -> Result<ColumnMask, Error> {
    let column = column.trim();
    if column.is_empty() {
        return Err(Error::PolicyAnnotation {
            policy_id: policy_id.to_string(),
            reason: format!("annotation '{key}' must name a column"),
        });
    }

    Ok(ColumnMask {
        column: column.to_string(),
        expression: non_empty_annotation_value(policy_id, key, expression)?.to_string(),
    })
}

fn tag_mask_from_annotation(
    policy_id: &PolicyId,
    key: &str,
    value: &str,
) -> Result<Option<TagMask>, Error> {
    if key == COLUMN_MASK_TAG_KEY {
        let value = non_empty_annotation_value(policy_id, key, value)?;
        let Some((tag, expression)) = value.split_once('=') else {
            return Err(Error::PolicyAnnotation {
                policy_id: policy_id.to_string(),
                reason: "annotation 'column_mask_tag' must use the format '<tag>=<sql expression>'"
                    .to_string(),
            });
        };
        return Ok(Some(build_tag_mask(policy_id, key, tag, expression)?));
    }

    if let Some(tag) = key.strip_prefix(COLUMN_MASK_TAG_PREFIX) {
        return Ok(Some(build_tag_mask(policy_id, key, tag, value)?));
    }

    if let Some(tag) = key.strip_prefix(MASK_TAG_PREFIX) {
        return Ok(Some(build_tag_mask(policy_id, key, tag, value)?));
    }

    Ok(None)
}

fn build_tag_mask(
    policy_id: &PolicyId,
    key: &str,
    tag: &str,
    expression: &str,
) -> Result<TagMask, Error> {
    let tag = tag.trim();
    if tag.is_empty() {
        return Err(Error::PolicyAnnotation {
            policy_id: policy_id.to_string(),
            reason: format!("annotation '{key}' must name a column tag"),
        });
    }

    Ok(TagMask {
        tag: tag.to_string(),
        expression: non_empty_annotation_value(policy_id, key, expression)?.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(name: &str) -> SpiceResource {
        SpiceResource::Dataset {
            name: name.to_string(),
            catalog: None,
            schema: None,
        }
    }

    #[test]
    fn compiles_row_filter_and_mask_annotations() {
        let policies: PolicySet = r#"
            @id("read_customers")
            @row_filter("tenant_id = current_org_id()")
            @mask_ssn("'***'")
            permit(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");
        "#
        .parse()
        .expect("valid policy set");

        validate_policy_annotations(&policies).expect("annotations should validate");
        let policy_ids = policies.policies().map(Policy::id).collect::<Vec<_>>();
        let plan = compile_access_plan(&policies, policy_ids, &dataset("customers"))
            .expect("access plan should compile");

        assert!(plan.allowed);
        assert_eq!(plan.row_filters, vec!["tenant_id = current_org_id()"]);
        assert_eq!(
            plan.column_masks,
            vec![ColumnMask {
                column: "ssn".to_string(),
                expression: "'***'".to_string(),
            }]
        );
        assert!(plan.tag_masks.is_empty());
    }

    #[test]
    fn compiles_tag_mask_annotations() {
        let policies: PolicySet = r#"
            @id("mask_pii")
            @column_mask_tag("pii='***'")
            @mask_tag_phi("null")
            permit(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");
        "#
        .parse()
        .expect("valid policy set");

        validate_policy_annotations(&policies).expect("annotations should validate");
        let policy_ids = policies.policies().map(Policy::id).collect::<Vec<_>>();
        let plan = compile_access_plan(&policies, policy_ids, &dataset("customers"))
            .expect("access plan should compile");

        assert_eq!(
            plan.tag_masks,
            vec![
                TagMask {
                    tag: "phi".to_string(),
                    expression: "null".to_string(),
                },
                TagMask {
                    tag: "pii".to_string(),
                    expression: "'***'".to_string(),
                },
            ]
        );
        assert!(plan.column_masks.is_empty());
    }

    #[test]
    fn rejects_conflicting_masks() {
        let policies: PolicySet = r#"
            @id("mask_a")
            @mask_ssn("'***'")
            permit(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");

            @id("mask_b")
            @mask_ssn("null")
            permit(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");
        "#
        .parse()
        .expect("valid policy set");

        let policy_ids = policies.policies().map(Policy::id).collect::<Vec<_>>();
        let err = compile_access_plan(&policies, policy_ids, &dataset("customers"))
            .expect_err("conflicting masks should fail");
        assert!(err.to_string().contains("conflicting column masks"));
    }

    #[test]
    fn rejects_fine_grained_forbid_annotations() {
        let policies: PolicySet = r#"
            @id("bad")
            @row_filter("tenant_id = current_org_id()")
            forbid(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");
        "#
        .parse()
        .expect("valid policy set");

        let err =
            validate_policy_annotations(&policies).expect_err("forbid annotations should fail");
        assert!(
            err.to_string()
                .contains("only supported on permit policies")
        );
    }

    #[test]
    fn rejects_empty_target_table_annotation() {
        let policies: PolicySet = r#"
            @id("bad_target")
            @target_table(" ")
            @row_filter("tenant_id = current_org_id()")
            permit(principal, action == Spice::Action::"read", resource == Spice::Dataset::"customers");
        "#
        .parse()
        .expect("valid policy set");

        let err = validate_policy_annotations(&policies)
            .expect_err("empty target_table annotation should fail");
        assert!(
            err.to_string()
                .contains("annotation 'target_table' must have a non-empty value")
        );
    }
}
