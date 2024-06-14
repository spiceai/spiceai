/*
Copyright 2024 The Spice.ai OSS Authors

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

use itertools::Itertools;
use snafu::prelude::*;
use std::{fmt::Display, hash::Hash};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"The column reference "{column_ref}" is missing a closing parenthensis."#))]
    MissingClosingParenthesisInColumnReference { column_ref: String },
}

#[derive(Debug, Clone, Eq)]
pub struct ColumnReference {
    columns: Vec<String>,
}

impl ColumnReference {
    #[must_use]
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns: columns.into_iter().sorted().collect(),
        }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self { columns: vec![] }
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(String::as_str)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    #[must_use]
    pub fn contains(&self, column: &String) -> bool {
        self.columns.contains(column)
    }
}

impl Default for ColumnReference {
    fn default() -> Self {
        Self::empty()
    }
}

impl PartialEq for ColumnReference {
    fn eq(&self, other: &Self) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }

        self.columns
            .iter()
            .zip(other.columns.iter())
            .all(|(a, b)| a == b)
    }
}

impl Hash for ColumnReference {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.columns.hash(state);
    }
}

/// Parses column references from a string into a vector of each individual column reference.
///
/// "foo" -> vec!["foo"]
/// "(foo, bar)" -> vec!["foo", "bar"]
/// "(foo, bar" -> Err(The column reference "(foo,bar" is missing a closing parenthensis.)
///
/// # Examples
///
/// ```
/// use runtime::component::dataset::acceleration::constraints::ColumnReference;
///
/// let column_ref = ColumnReference::try_from("foo").expect("valid columns");
/// assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo"]);
///
/// let column_ref = ColumnReference::try_from("(foo, bar)").expect("valid columns");
/// assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo", "bar"]);
/// ```
impl TryFrom<&str> for ColumnReference {
    type Error = Error;

    fn try_from(columns: &str) -> Result<Self, Self::Error> {
        // The index/primary key can be either a single column or a compound index
        if columns.starts_with('(') {
            // Compound index
            let end =
                columns
                    .find(')')
                    .context(MissingClosingParenthesisInColumnReferenceSnafu {
                        column_ref: columns.to_string(),
                    })?;
            Ok(Self {
                columns: columns[1..end]
                    .split(',')
                    .map(str::trim)
                    .map(String::from)
                    .sorted()
                    .collect::<Vec<String>>(),
            })
        } else {
            // Single column reference
            Ok(Self {
                columns: vec![columns.to_string()],
            })
        }
    }
}

impl Display for ColumnReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.columns.len() == 1 {
            write!(f, "{}", self.columns[0])
        } else {
            write!(f, "({})", self.columns.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_column_ref() {
        let column_ref = ColumnReference::try_from("foo").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["foo"]);

        let column_ref = ColumnReference::try_from("(foo, bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);

        let column_ref = ColumnReference::try_from("(foo,bar)").expect("valid columns");
        assert_eq!(column_ref.iter().collect::<Vec<_>>(), vec!["bar", "foo"]);
    }
}
