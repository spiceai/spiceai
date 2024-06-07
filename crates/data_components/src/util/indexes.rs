use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

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

#[must_use]
pub fn indexes_from_option_string(indexes_option_str: &str) -> HashMap<String, IndexType> {
    indexes_option_str
        .split(';')
        .map(|index| {
            let parts: Vec<&str> = index.split(':').collect();
            if parts.len() == 2 {
                (parts[0].to_string(), IndexType::from(parts[1]))
            } else {
                (index.to_string(), IndexType::Enabled)
            }
        })
        .collect()
}

pub fn index_columns(indexes_key: &str) -> Vec<&str> {
    // The key to an index can be either a single column or a compound index
    if indexes_key.starts_with('(') {
        // Compound index
        let end = indexes_key.find(')').unwrap_or(indexes_key.len());
        indexes_key[1..end]
            .split(',')
            .map(str::trim)
            .collect::<Vec<&str>>()
    } else {
        // Single column index
        vec![indexes_key]
    }
}

#[cfg(test)]
mod tests {
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
        let indexes = indexes_from_option_string(indexes_option_str);
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes.get("index1"), Some(&IndexType::Unique));
        assert_eq!(indexes.get("index2"), Some(&IndexType::Enabled));
    }

    #[test]
    fn test_get_index_columns() {
        let index_columns_vec = index_columns("foo");
        assert_eq!(index_columns_vec, vec!["foo"]);

        let index_columns_vec = index_columns("(foo, bar)");
        assert_eq!(index_columns_vec, vec!["foo", "bar"]);

        let index_columns_vec = index_columns("(foo,bar)");
        assert_eq!(index_columns_vec, vec!["foo", "bar"]);
    }
}
