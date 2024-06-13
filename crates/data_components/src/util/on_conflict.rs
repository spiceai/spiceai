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
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OnConflictBehavior {
    #[default]
    Drop,
    Upsert,
}

impl From<&str> for OnConflictBehavior {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "upsert" => OnConflictBehavior::Upsert,
            _ => OnConflictBehavior::Drop,
        }
    }
}

impl Display for OnConflictBehavior {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            OnConflictBehavior::Upsert => write!(f, "upsert"),
            OnConflictBehavior::Drop => write!(f, "drop"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::util::hashmap_from_option_string;

    use super::*;

    #[test]
    fn test_index_type_from_str() {
        assert_eq!(
            OnConflictBehavior::from("upsert"),
            OnConflictBehavior::Upsert
        );
        assert_eq!(OnConflictBehavior::from("drop"), OnConflictBehavior::Drop);
        assert_eq!(OnConflictBehavior::from("Drop"), OnConflictBehavior::Drop);
        assert_eq!(OnConflictBehavior::from("DROP"), OnConflictBehavior::Drop);
    }

    #[test]
    fn test_on_conflict_from_option_string() {
        let on_conflict_option_str = "col1:drop;col2:upsert";
        let on_conflict: HashMap<String, OnConflictBehavior> =
            hashmap_from_option_string(on_conflict_option_str);
        assert_eq!(on_conflict.len(), 2);
        assert_eq!(on_conflict.get("col1"), Some(&OnConflictBehavior::Drop));
        assert_eq!(on_conflict.get("col2"), Some(&OnConflictBehavior::Upsert));
    }
}
