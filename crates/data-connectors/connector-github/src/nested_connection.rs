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

//! Turning a connection nested under another object into its own table.
//!
//! Reviews, review threads and release assets exist only underneath a parent in
//! GitHub's GraphQL schema — there is no top-level connection to page over. Each
//! table therefore pages over the parent and fans the parent's nested connection
//! out into one row per child node.
//!
//! GitHub caps a connection page at 100 nodes and a query cannot paginate a
//! nested connection, so a parent with more children than the page size is
//! truncated. Every fan-out reads the connection's `totalCount` and warns by
//! name when that happens, so a short `COUNT(*)` is visible rather than silent.

use crate::identity::insert_identity;
use serde_json::{Map, Value};

/// How to fan one nested connection out into rows.
pub(crate) struct NestedConnection<'a> {
    /// Response key of the connection to fan out, as it appears in the query.
    pub connection_key: &'a str,

    /// Keys copied from the parent object onto every child row, so a child can
    /// be joined back to its parent.
    pub parent_keys: &'a [&'a str],

    /// Parent key whose value names the parent in the truncation warning.
    pub parent_id_key: &'a str,

    /// Names the parent in the truncation warning, e.g. `pull request`.
    pub parent_label: &'a str,

    /// Names the child resource in the truncation warning, e.g. `reviews`.
    pub child_label: &'a str,
}

/// Rewrites `object[key]`, a `{ "login": "..." }` sub-object, to the login
/// string itself. A missing or null sub-object becomes a null.
pub(crate) fn flatten_login(object: &mut Map<String, Value>, key: &str) {
    let login = object
        .get(key)
        .and_then(|value| value.get("login"))
        .and_then(Value::as_str)
        .map_or(Value::Null, |login| Value::String(login.to_string()));

    object.insert(key.to_string(), login);
}

/// Rewrites `object[key]`, a sub-object, to its `field` member, storing the
/// result under `into`. A missing or null sub-object becomes a null.
pub(crate) fn flatten_member(object: &mut Map<String, Value>, key: &str, field: &str, into: &str) {
    let value = object
        .get(key)
        .and_then(|value| value.get(field))
        .cloned()
        .unwrap_or(Value::Null);

    object.remove(key);
    object.insert(into.to_string(), value);
}

/// Fans the nested connection out into one row per child node, stamping each
/// row with its parent keys and repository identity.
///
/// `transform` runs on each child row after those keys are added, and is where
/// a caller flattens its own nested members.
pub(crate) fn fan_out<F>(
    parent: &Value,
    spec: &NestedConnection<'_>,
    owner: &str,
    repo: &str,
    mut transform: F,
) -> Vec<Value>
where
    F: FnMut(&mut Map<String, Value>),
{
    let Some(parent) = parent.as_object() else {
        return Vec::new();
    };

    let Some(connection) = parent.get(spec.connection_key) else {
        return Vec::new();
    };

    let nodes = connection
        .get("nodes")
        .and_then(Value::as_array)
        .map_or_else(Vec::new, Clone::clone);

    warn_if_truncated(
        connection.get("totalCount").and_then(Value::as_i64),
        nodes.len(),
        spec,
        owner,
        repo,
        parent.get(spec.parent_id_key),
    );

    let mut rows = Vec::with_capacity(nodes.len());
    for node in nodes {
        let Value::Object(mut row) = node else {
            continue;
        };

        for key in spec.parent_keys {
            row.insert(
                (*key).to_string(),
                parent.get(*key).cloned().unwrap_or(Value::Null),
            );
        }
        insert_identity(&mut row, owner, Some(repo));
        transform(&mut row);

        rows.push(Value::Object(row));
    }

    rows
}

/// Warns, naming the parent, when GitHub reported more children than the single
/// un-paginated page returned.
fn warn_if_truncated(
    total_count: Option<i64>,
    returned: usize,
    spec: &NestedConnection<'_>,
    owner: &str,
    repo: &str,
    parent_id: Option<&Value>,
) {
    let Some(total_count) = total_count else {
        return;
    };

    let returned_count = i64::try_from(returned).unwrap_or(i64::MAX);
    if total_count <= returned_count {
        return;
    }

    let parent_id = parent_id.map_or_else(
        || "unknown".to_string(),
        |value| match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        },
    );

    let parent_label = spec.parent_label;
    let child_label = spec.child_label;
    tracing::warn!(
        "GitHub returned only {returned_count} of {total_count} {child_label} for {parent_label} '{parent_id}' of '{owner}/{repo}', so rows for it are incomplete and counts over it will be short. GitHub caps a nested connection at one page and cannot paginate it. See: https://spiceai.org/docs/components/data-connectors/github"
    );
}

#[cfg(test)]
mod tests {
    use super::{NestedConnection, fan_out, flatten_login, flatten_member};
    use serde_json::{Map, Value, json};

    const REVIEWS: NestedConnection<'static> = NestedConnection {
        connection_key: "reviews",
        parent_keys: &["pull_request_id", "pull_request_number"],
        parent_id_key: "pull_request_number",
        parent_label: "pull request",
        child_label: "reviews",
    };

    fn pull_request() -> Value {
        json!({
            "pull_request_id": "PR_1",
            "pull_request_number": 42,
            "reviews": {
                "totalCount": 2,
                "nodes": [
                    {"id": "R_1", "state": "APPROVED", "author": {"login": "lukekim"}},
                    {"id": "R_2", "state": "COMMENTED", "author": null},
                ]
            }
        })
    }

    #[test]
    fn fan_out_emits_one_row_per_child_with_full_identity() {
        let rows = fan_out(&pull_request(), &REVIEWS, "spiceai", "spiceai", |row| {
            flatten_login(row, "author");
        });

        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row["pull_request_id"], json!("PR_1"));
            assert_eq!(row["pull_request_number"], json!(42));
            assert_eq!(row["owner"], json!("spiceai"));
            assert_eq!(row["repo"], json!("spiceai"));
        }
        assert_eq!(rows[0]["author"], json!("lukekim"));
        // A ghost (deleted) author is null rather than an unparseable object.
        assert_eq!(rows[1]["author"], Value::Null);
    }

    #[test]
    fn fan_out_returns_no_rows_when_the_connection_is_absent() {
        let rows = fan_out(
            &json!({"pull_request_id": "PR_1", "pull_request_number": 42}),
            &REVIEWS,
            "spiceai",
            "spiceai",
            |_| {},
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn fan_out_keeps_the_rows_it_did_get_when_a_page_is_truncated() {
        // A short page is reported through a warning, never by dropping the
        // rows that did arrive.
        let truncated = json!({
            "pull_request_id": "PR_1",
            "pull_request_number": 42,
            "reviews": {"totalCount": 500, "nodes": [{"id": "R_1"}]}
        });

        let rows = fan_out(&truncated, &REVIEWS, "spiceai", "spiceai", |_| {});

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], json!("R_1"));
    }

    #[test]
    fn fan_out_nulls_a_parent_key_the_parent_does_not_carry() {
        let rows = fan_out(
            &json!({"reviews": {"totalCount": 1, "nodes": [{"id": "R_1"}]}}),
            &REVIEWS,
            "spiceai",
            "spiceai",
            |_| {},
        );

        assert_eq!(rows[0]["pull_request_id"], Value::Null);
        assert_eq!(rows[0]["pull_request_number"], Value::Null);
    }

    #[test]
    fn flatten_member_replaces_the_sub_object_with_its_field() {
        let mut row: Map<String, Value> = json!({"commit": {"oid": "abc123"}})
            .as_object()
            .expect("object")
            .clone();
        flatten_member(&mut row, "commit", "oid", "commit_sha");

        assert_eq!(row["commit_sha"], json!("abc123"));
        assert!(row.get("commit").is_none());
    }

    #[test]
    fn flatten_member_nulls_a_missing_sub_object() {
        let mut row: Map<String, Value> =
            json!({"commit": null}).as_object().expect("object").clone();
        flatten_member(&mut row, "commit", "oid", "commit_sha");

        assert_eq!(row["commit_sha"], Value::Null);
    }
}
