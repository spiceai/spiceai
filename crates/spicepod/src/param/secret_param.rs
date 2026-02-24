/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::fmt;
use std::str::FromStr;

use secret_ref_detect::contains_secret_ref;
use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// A parameter value that may contain a secret reference (e.g. `${env:KEY}`).
///
/// When deserialized from YAML/JSON, the raw string is checked for secret references.
/// If it contains one, the value is stored as `Unresolved(raw_string)`.
/// Otherwise, it is parsed into the target type `T` and stored as `Plain(T)`.
#[derive(Debug, Clone, PartialEq)]
pub enum SecretParam<T> {
    /// A fully resolved, typed value.
    Plain(T),
    /// A string containing one or more secret references that must be resolved at runtime.
    Unresolved(String),
}

impl<T> SecretParam<T> {
    /// Returns a reference to the inner value if this is a `Plain` variant.
    #[must_use]
    pub fn expose(&self) -> Option<&T> {
        match self {
            SecretParam::Plain(v) => Some(v),
            SecretParam::Unresolved(_) => None,
        }
    }

    /// Returns the raw unresolved string if this is an `Unresolved` variant.
    #[must_use]
    pub fn raw(&self) -> Option<&str> {
        match self {
            SecretParam::Plain(_) => None,
            SecretParam::Unresolved(s) => Some(s),
        }
    }
}

impl<T: fmt::Display> SecretParam<T> {
    /// Returns the value as a string, whether it's a plain value or an unresolved reference.
    #[must_use]
    pub fn as_string(&self) -> String {
        match self {
            SecretParam::Plain(v) => v.to_string(),
            SecretParam::Unresolved(s) => s.clone(),
        }
    }
}

impl<T: fmt::Display> Serialize for SecretParam<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_string())
    }
}

impl<'de, T: FromStr> Deserialize<'de> for SecretParam<T>
where
    T::Err: fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(SecretParamVisitor::<T>(std::marker::PhantomData))
    }
}

struct SecretParamVisitor<T>(std::marker::PhantomData<T>);

impl<'de, T: FromStr> Visitor<'de> for SecretParamVisitor<T>
where
    T::Err: fmt::Display,
{
    type Value = SecretParam<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string, number, or boolean value")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if contains_secret_ref(value) {
            return Ok(SecretParam::Unresolved(value.to_owned()));
        }
        value
            .parse::<T>()
            .map(SecretParam::Plain)
            .map_err(de::Error::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value
            .to_string()
            .parse::<T>()
            .map(SecretParam::Plain)
            .map_err(de::Error::custom)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value
            .to_string()
            .parse::<T>()
            .map(SecretParam::Plain)
            .map_err(de::Error::custom)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value
            .to_string()
            .parse::<T>()
            .map(SecretParam::Plain)
            .map_err(de::Error::custom)
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value
            .to_string()
            .parse::<T>()
            .map(SecretParam::Plain)
            .map_err(de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl<T> schemars::JsonSchema for SecretParam<T> {
    fn schema_name() -> String {
        "SecretParam".to_string()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        // Present as a string schema since values may be secret references
        generator.subschema_for::<String>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_u16_deser() {
        let val: SecretParam<u16> = serde_json::from_str("\"5432\"").unwrap();
        assert_eq!(val, SecretParam::Plain(5432));
        assert_eq!(val.expose(), Some(&5432));
        assert_eq!(val.raw(), None);
    }

    #[test]
    fn test_plain_u16_from_number() {
        let val: SecretParam<u16> = serde_json::from_str("5432").unwrap();
        assert_eq!(val, SecretParam::Plain(5432));
    }

    #[test]
    fn test_unresolved_deser() {
        let val: SecretParam<u16> = serde_json::from_str("\"${env:PORT}\"").unwrap();
        assert_eq!(val, SecretParam::Unresolved("${env:PORT}".to_string()));
        assert_eq!(val.expose(), None);
        assert_eq!(val.raw(), Some("${env:PORT}"));
    }

    #[test]
    fn test_plain_string_deser() {
        let val: SecretParam<String> = serde_json::from_str("\"localhost\"").unwrap();
        assert_eq!(val, SecretParam::Plain("localhost".to_string()));
    }

    #[test]
    fn test_unresolved_string_deser() {
        let val: SecretParam<String> =
            serde_json::from_str("\"${secret:db_host}\"").unwrap();
        assert_eq!(
            val,
            SecretParam::Unresolved("${secret:db_host}".to_string())
        );
    }

    #[test]
    fn test_plain_bool_deser() {
        let val: SecretParam<bool> = serde_json::from_str("true").unwrap();
        assert_eq!(val, SecretParam::Plain(true));
    }

    #[test]
    fn test_bool_string_deser() {
        let val: SecretParam<bool> = serde_json::from_str("\"false\"").unwrap();
        assert_eq!(val, SecretParam::Plain(false));
    }

    #[test]
    fn test_serialize_plain() {
        let val = SecretParam::Plain(5432u16);
        let s = serde_json::to_string(&val).unwrap();
        assert_eq!(s, "\"5432\"");
    }

    #[test]
    fn test_serialize_unresolved() {
        let val: SecretParam<u16> = SecretParam::Unresolved("${env:PORT}".to_string());
        let s = serde_json::to_string(&val).unwrap();
        assert_eq!(s, "\"${env:PORT}\"");
    }

    #[test]
    fn test_as_string() {
        assert_eq!(SecretParam::Plain(5432u16).as_string(), "5432");
        assert_eq!(
            SecretParam::<u16>::Unresolved("${env:PORT}".to_string()).as_string(),
            "${env:PORT}"
        );
    }

    #[test]
    fn test_mixed_string_with_secret_ref() {
        let val: SecretParam<String> =
            serde_json::from_str("\"host_${env:SUFFIX}\"").unwrap();
        assert_eq!(
            val,
            SecretParam::Unresolved("host_${env:SUFFIX}".to_string())
        );
    }
}
