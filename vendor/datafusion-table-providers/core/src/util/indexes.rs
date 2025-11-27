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
