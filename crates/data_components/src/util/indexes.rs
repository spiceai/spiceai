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
pub enum IndexType {
    #[default]
    Enabled,
    Unique,
}

impl From<&str> for IndexType {
    fn from(index_type: &str) -> Self {
        match index_type.to_lowercase().as_str() {
            "unique" => IndexType::Unique,
            _ => IndexType::Enabled,
        }
    }
}

impl Display for IndexType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            IndexType::Unique => write!(f, "unique"),
            IndexType::Enabled => write!(f, "enabled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_index_type_from_str() {
        assert_eq!(IndexType::from("unique"), IndexType::Unique);
        assert_eq!(IndexType::from("enabled"), IndexType::Enabled);
        assert_eq!(IndexType::from("Enabled"), IndexType::Enabled);
        assert_eq!(IndexType::from("ENABLED"), IndexType::Enabled);
    }

    #[test]
    fn test_indexes_from_option_string() {
        let indexes_option_str = "index1:unique;index2";
        let indexes: HashMap<String, IndexType> =
            crate::util::hashmap_from_option_string(indexes_option_str);
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes.get("index1"), Some(&IndexType::Unique));
        assert_eq!(indexes.get("index2"), Some(&IndexType::Enabled));
    }
}
