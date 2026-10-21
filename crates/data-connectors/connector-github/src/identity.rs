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

//! The `owner` and `repo` identity columns every GitHub table carries.
//!
//! GitHub scopes a response by owner and repository *above* the row array —
//! `repository(owner:, name:) { pullRequests { nodes } }` — so a row has no way
//! to say which repository it came from. Without these columns a query that
//! unions several repositories cannot tell the rows apart, and every join back
//! to repository metadata has to be written as a literal per dataset.
//!
//! Both columns are constants for the lifetime of a dataset, so they are
//! stamped onto each row after unnesting (GraphQL) or appended as constant
//! arrays (REST) rather than requested from GitHub.

use arrow::array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use connector_graphql::graphql::{
    Result,
    client::{DuplicateBehavior, UnnestBehavior, unnest_json_object_to_depth},
};
use serde_json::{Map, Value};
use std::sync::Arc;

/// The login of the user or organization that owns the row's repository. For an
/// owner-scoped table (`members`, `repos`) this is the owner the dataset names.
pub(crate) const OWNER_COLUMN: &str = "owner";

/// The name of the repository the row came from, without the owner prefix.
pub(crate) const REPO_COLUMN: &str = "repo";

/// Folds an owner or repository name to the one spelling every table will
/// agree on.
///
/// These columns exist to be joined across datasets, and GitHub treats the
/// names as case-insensitive: it accepts `SpiceAI/SpiceAI` in a dataset path
/// and answers about the same repository it calls `spiceai/spiceai`. SQL
/// equality is not case-insensitive, so a table that stamps what the user typed
/// and a table that reports what GitHub returned would disagree on the join key
/// and match nothing — the one failure this column set exists to prevent.
/// Storing a case-insensitive identifier folded, the way a hostname or an email
/// address is, makes the join hold whatever the dataset path looked like.
///
/// The fold is ASCII-only because a GitHub login or repository name is: letters,
/// digits, hyphen, underscore and period. `name_with_owner` on `repos` and
/// `repo` keeps GitHub's own spelling for display.
pub(crate) fn canonical_identity(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Appends the identity fields to a table's field vector. Pass `repo_scoped` as
/// `false` for an owner-scoped table, which carries only `owner`.
pub(crate) fn push_identity_fields(fields: &mut Vec<Field>, repo_scoped: bool) {
    fields.push(Field::new(OWNER_COLUMN, DataType::Utf8, false));
    if repo_scoped {
        fields.push(Field::new(REPO_COLUMN, DataType::Utf8, false));
    }
}

/// Stamps the identity keys onto one unnested row, folded by
/// [`canonical_identity`] so every table agrees on the join key.
pub(crate) fn insert_identity(row: &mut Map<String, Value>, owner: &str, repo: Option<&str>) {
    row.insert(
        OWNER_COLUMN.to_string(),
        Value::String(canonical_identity(owner)),
    );
    if let Some(repo) = repo {
        row.insert(
            REPO_COLUMN.to_string(),
            Value::String(canonical_identity(repo)),
        );
    }
}

/// Stamps the identity keys onto every row of an unnest result.
pub(crate) fn insert_identity_into_rows(rows: &mut [Value], owner: &str, repo: Option<&str>) {
    for row in rows {
        if let Value::Object(row) = row {
            insert_identity(row, owner, repo);
        }
    }
}

/// Wraps the standard depth-based unnest so every row it emits carries its
/// identity. Use this in place of [`UnnestBehavior::Depth`] for any table whose
/// rows would otherwise be indistinguishable after a union across repositories.
pub(crate) fn identity_unnest(depth: usize, owner: String, repo: Option<String>) -> UnnestBehavior {
    UnnestBehavior::Custom(Box::new(move |object: &Value| -> Result<Vec<Value>> {
        let mut rows =
            unnest_json_object_to_depth(object.clone(), depth, &DuplicateBehavior::Error)?;
        insert_identity_into_rows(&mut rows, &owner, repo.as_deref());
        Ok(rows)
    }))
}

/// Appends the constant identity columns to a batch assembled from a REST
/// response. The caller must have added the matching fields with
/// [`push_identity_fields`] and must pass the same `repo_scoped` choice.
pub(crate) fn push_identity_columns(
    columns: &mut Vec<ArrayRef>,
    owner: &str,
    repo: Option<&str>,
    num_rows: usize,
) {
    let owner = canonical_identity(owner);
    columns.push(Arc::new(StringArray::from(vec![owner.as_str(); num_rows])) as ArrayRef);
    if let Some(repo) = repo {
        let repo = canonical_identity(repo);
        columns.push(Arc::new(StringArray::from(vec![repo.as_str(); num_rows])) as ArrayRef);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OWNER_COLUMN, REPO_COLUMN, canonical_identity, identity_unnest, insert_identity,
        push_identity_columns, push_identity_fields,
    };
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow_schema::{DataType, Field};
    use connector_graphql::graphql::client::UnnestBehavior;
    use serde_json::{Map, json};
    use std::sync::Arc;

    fn unnest(behavior: &UnnestBehavior, value: &serde_json::Value) -> Vec<serde_json::Value> {
        match behavior {
            UnnestBehavior::Custom(f) => f(value).expect("unnest to succeed"),
            UnnestBehavior::Depth(_) => panic!("expected a custom unnest behavior"),
        }
    }

    #[test]
    fn identity_unnest_stamps_owner_and_repo_on_every_row() {
        let behavior = identity_unnest(1, "spiceai".to_string(), Some("spiceai".to_string()));
        let rows = unnest(
            &behavior,
            &json!({"author": {"author": "lukekim"}, "number": 1}),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][OWNER_COLUMN], json!("spiceai"));
        assert_eq!(rows[0][REPO_COLUMN], json!("spiceai"));
        // The wrapped depth unnest still runs.
        assert_eq!(rows[0]["author"], json!("lukekim"));
    }

    #[test]
    fn identity_unnest_omits_repo_for_an_owner_scoped_table() {
        let behavior = identity_unnest(0, "spiceai".to_string(), None);
        let rows = unnest(&behavior, &json!({"username": "lukekim"}));

        assert_eq!(rows[0][OWNER_COLUMN], json!("spiceai"));
        assert!(rows[0].get(REPO_COLUMN).is_none());
    }

    #[test]
    fn identity_fields_and_columns_stay_in_the_same_order() {
        let mut fields: Vec<Field> = vec![Field::new("id", DataType::Utf8, false)];
        push_identity_fields(&mut fields, true);
        assert_eq!(
            fields.iter().map(Field::name).collect::<Vec<_>>(),
            vec!["id", OWNER_COLUMN, REPO_COLUMN]
        );

        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef];
        push_identity_columns(&mut columns, "spiceai", Some("spiceai"), 2);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1].len(), 2);
        assert_eq!(columns[2].len(), 2);
    }

    /// The whole point of these columns is a join across datasets, and GitHub
    /// answers about `SpiceAI/SpiceAI` and `spiceai/spiceai` as one repository.
    /// If the two spellings reached SQL intact the join would match nothing.
    #[test]
    fn identity_folds_so_two_spellings_of_one_repository_join() {
        let typed = {
            let mut row = Map::new();
            insert_identity(&mut row, "SpiceAI", Some("SpiceAI"));
            row
        };
        let canonical = {
            let mut row = Map::new();
            insert_identity(&mut row, "spiceai", Some("spiceai"));
            row
        };

        assert_eq!(typed[OWNER_COLUMN], canonical[OWNER_COLUMN]);
        assert_eq!(typed[REPO_COLUMN], canonical[REPO_COLUMN]);
        assert_eq!(typed[OWNER_COLUMN], json!("spiceai"));
    }

    /// A REST-sourced table builds its identity columns as constant arrays
    /// rather than per-row keys, so it has to fold on the same rule.
    #[test]
    fn identity_columns_fold_on_the_same_rule_as_identity_keys() {
        let mut columns: Vec<ArrayRef> = Vec::new();
        push_identity_columns(&mut columns, "SpiceAI", Some("SpiceAI"), 2);

        let expected: ArrayRef = Arc::new(StringArray::from(vec!["spiceai"; 2]));
        assert_eq!(columns.len(), 2);
        assert_eq!(&columns[0], &expected);
        assert_eq!(&columns[1], &expected);
    }

    /// A name GitHub cannot issue must still survive the fold unchanged rather
    /// than being mangled by a locale-aware lowercase.
    #[test]
    fn canonical_identity_touches_only_ascii() {
        assert_eq!(canonical_identity("Spice-AI_v2.0"), "spice-ai_v2.0");
        assert_eq!(canonical_identity("\u{130}"), "\u{130}");
    }
}
