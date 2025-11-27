use snafu::prelude::*;
use std::hash::Hash;

use datafusion::common::DataFusionError;
use std::collections::HashMap;

use crate::UnsupportedTypeAction;

pub mod column_reference;
pub mod constraints;
pub mod indexes;
pub mod ns_lookup;
pub mod on_conflict;
pub mod retriable_error;
#[cfg(any(feature = "sqlite", feature = "duckdb", feature = "postgres"))]
pub mod schema;
pub mod secrets;
pub mod supported_functions;
pub mod table_arg_replace;
pub mod test;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL {
        source: datafusion::error::DataFusionError,
    },
}

#[must_use]
pub fn hashmap_from_option_string<K, V>(hashmap_option_str: &str) -> HashMap<K, V>
where
    K: for<'a> From<&'a str> + Eq + Hash,
    V: for<'a> From<&'a str> + Default,
{
    hashmap_option_str
        .split(';')
        .map(|index| {
            let parts: Vec<&str> = index.split(':').collect();
            if parts.len() == 2 {
                (K::from(parts[0]), V::from(parts[1]))
            } else {
                (K::from(index), V::default())
            }
        })
        .collect()
}

#[must_use]
pub fn remove_prefix_from_hashmap_keys<V>(
    hashmap: HashMap<String, V>,
    prefix: &str,
) -> HashMap<String, V> {
    hashmap
        .into_iter()
        .map(|(key, value)| {
            let new_key = key
                .strip_prefix(prefix)
                .map_or(key.clone(), |s| s.to_string());
            (new_key, value)
        })
        .collect()
}

#[must_use]
pub fn to_datafusion_error<E>(error: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(error))
}

/// If the `UnsupportedTypeAction` is `Error` or `String`, the function will return an error.
/// If the `UnsupportedTypeAction` is `Warn`, the function will log a warning.
/// If the `UnsupportedTypeAction` is `Ignore`, the function will do nothing.
///
/// Used in the handling of unsupported schemas to determine if columns are removed or if the function should return an error.
///
/// Components that want to handle `UnsupportedTypeAction::String` via the component's own logic should handle it before calling this function.
///
/// # Errors
///
/// If the `UnsupportedTypeAction` is `Error` or `String` the function will return an error.
pub fn handle_unsupported_type_error<E>(
    unsupported_type_action: UnsupportedTypeAction,
    error: E,
) -> Result<(), E>
where
    E: std::error::Error + Send + Sync,
{
    match unsupported_type_action {
        UnsupportedTypeAction::Error | UnsupportedTypeAction::String => return Err(error),
        UnsupportedTypeAction::Warn => {
            tracing::warn!("{error}");
        }
        UnsupportedTypeAction::Ignore => {}
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_remove_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_key1".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());
        hashmap.insert("key3".to_string(), "value3".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());
        expected.insert("key3".to_string(), "value3".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_no_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("key1".to_string(), "value1".to_string());
        hashmap.insert("key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("key1".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_hashmap() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let expected: HashMap<String, String> = HashMap::new();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_full_prefix() {
        let mut hashmap = HashMap::new();
        hashmap.insert("prefix_".to_string(), "value1".to_string());
        hashmap.insert("prefix_key2".to_string(), "value2".to_string());

        let result = remove_prefix_from_hashmap_keys(hashmap, "prefix_");

        let mut expected = HashMap::new();
        expected.insert("".to_string(), "value1".to_string());
        expected.insert("key2".to_string(), "value2".to_string());

        assert_eq!(result, expected);
    }
}
