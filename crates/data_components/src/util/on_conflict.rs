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
#![allow(clippy::module_name_repetitions)]
use snafu::prelude::*;
use std::fmt::Display;

use super::column_reference::{self, ColumnReference};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Invalid column reference: {source}"#))]
    InvalidColumnReference { source: column_reference::Error },

    #[snafu(display("Expected do_nothing or upsert, found: {token}"))]
    UnexpectedToken { token: String },

    #[snafu(display("Expected semicolon in: {token}"))]
    ExpectedSemicolon { token: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflict {
    DoNothingAll,
    DoNothing(ColumnReference),
    Upsert(ColumnReference),
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnConflict::DoNothingAll => write!(f, "do_nothing_all"),
            OnConflict::DoNothing(column) => write!(f, "do_nothing:{column}"),
            OnConflict::Upsert(column) => write!(f, "upsert:{column}"),
        }
    }
}

impl TryFrom<&str> for OnConflict {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        if value == "do_nothing_all" {
            return Ok(OnConflict::DoNothingAll);
        }

        let parts: Vec<&str> = value.split(':').collect();
        if parts.len() != 2 {
            return ExpectedSemicolonSnafu {
                token: value.to_string(),
            }
            .fail();
        }

        let column_ref =
            ColumnReference::try_from(parts[1]).context(InvalidColumnReferenceSnafu)?;

        let on_conflict_behavior = parts[0];
        match on_conflict_behavior {
            "do_nothing" => Ok(OnConflict::DoNothing(column_ref)),
            "upsert" => Ok(OnConflict::Upsert(column_ref)),
            _ => UnexpectedTokenSnafu {
                token: parts[0].to_string(),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::util::{column_reference::ColumnReference, on_conflict::OnConflict};

    #[test]
    fn test_on_conflict_from_str() {
        let on_conflict = OnConflict::try_from("do_nothing_all").expect("valid on conflict");
        assert_eq!(on_conflict, OnConflict::DoNothingAll);

        let on_conflict = OnConflict::try_from("do_nothing:col1").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()]))
        );

        let on_conflict = OnConflict::try_from("upsert:col2").expect("valid on conflict");
        assert_eq!(
            on_conflict,
            OnConflict::Upsert(ColumnReference::new(vec!["col2".to_string()]))
        );

        let err = OnConflict::try_from("do_nothing").expect_err("invalid on conflict");
        assert_eq!(
            err.to_string(),
            "Expected semicolon in: do_nothing".to_string()
        );
    }

    #[test]
    fn test_roundtrip() {
        let on_conflict = OnConflict::DoNothingAll.to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::DoNothingAll
        );

        let on_conflict =
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()])).to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::DoNothing(ColumnReference::new(vec!["col1".to_string()]))
        );

        let on_conflict =
            OnConflict::Upsert(ColumnReference::new(vec!["col2".to_string()])).to_string();
        assert_eq!(
            OnConflict::try_from(on_conflict.as_str()).expect("valid on conflict"),
            OnConflict::Upsert(ColumnReference::new(vec!["col2".to_string()]))
        );
    }
}
