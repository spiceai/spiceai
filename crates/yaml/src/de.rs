/*
Copyright 2026 The Spice.ai OSS Authors

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

use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::forward_to_deserialize_any;
use yaml_rust2::{Yaml, YamlLoader};

use crate::error::{Error, Result};
use crate::value::{Mapping, Number, Value};

/// The YAML merge key indicator (YAML 1.1 feature, widely supported).
const MERGE_KEY: &str = "<<";

/// Convert yaml-rust2's Yaml type to our Value type.
pub(crate) fn yaml_to_value(yaml: Yaml) -> Value {
    match yaml {
        Yaml::Boolean(b) => Value::Bool(b),
        Yaml::Integer(i) => {
            if i >= 0 {
                #[expect(clippy::cast_sign_loss, reason = "checked non-negative above")]
                Value::Number(Number::PosInt(i as u64))
            } else {
                Value::Number(Number::NegInt(i))
            }
        }
        Yaml::Real(s) => {
            // Parse the real number string
            if let Ok(f) = s.parse::<f64>() {
                Value::Number(Number::Float(f))
            } else {
                // Handle special values
                match s.to_lowercase().as_str() {
                    ".inf" | "inf" => Value::Number(Number::Float(f64::INFINITY)),
                    "-.inf" | "-inf" => Value::Number(Number::Float(f64::NEG_INFINITY)),
                    ".nan" | "nan" => Value::Number(Number::Float(f64::NAN)),
                    _ => Value::String(s),
                }
            }
        }
        Yaml::String(s) => Value::String(s),
        Yaml::Array(arr) => Value::Sequence(arr.into_iter().map(yaml_to_value).collect()),
        Yaml::Hash(hash) => yaml_hash_to_value(hash),
        // Aliases should be resolved by yaml-rust2
        Yaml::Null | Yaml::Alias(_) | Yaml::BadValue => Value::Null,
    }
}

/// Convert a yaml-rust2 Hash to a Value, handling merge keys.
fn yaml_hash_to_value(hash: yaml_rust2::yaml::Hash) -> Value {
    let mut map = Mapping::new();
    let mut merge_values: Vec<Mapping> = Vec::new();

    // First pass: collect merge values and regular entries
    for (k, v) in hash {
        // Check if this is a merge key
        if let Yaml::String(ref s) = k
            && s == MERGE_KEY
        {
            // Handle merge key - value can be a mapping or array of mappings
            collect_merge_values(&v, &mut merge_values);
            continue;
        }
        // Regular key-value pair
        map.insert(yaml_to_value(k), yaml_to_value(v));
    }

    // Apply merge values (earlier ones take precedence for duplicate keys)
    // Merge values should not override keys that already exist
    for merge_map in merge_values {
        for (k, v) in merge_map {
            // Only insert if key doesn't already exist
            if !map.contains_key(&k) {
                map.insert(k, v);
            }
        }
    }

    Value::Mapping(map)
}

/// Collect mappings to merge from a merge key value.
fn collect_merge_values(yaml: &Yaml, merge_values: &mut Vec<Mapping>) {
    match yaml {
        Yaml::Hash(hash) => {
            // Single mapping to merge
            if let Value::Mapping(m) = yaml_hash_to_value(hash.clone()) {
                merge_values.push(m);
            }
        }
        Yaml::Array(arr) => {
            // Array of mappings to merge (in order)
            for item in arr {
                collect_merge_values(item, merge_values);
            }
        }
        _ => {
            // Invalid merge value - ignore (or could be an alias that's already resolved)
        }
    }
}

/// Parse a YAML string into a Value.
///
/// # Errors
///
/// Returns an error if the YAML string contains multiple documents.
/// Multi-document YAML is not supported to avoid silent data loss.
/// Use `parse_yaml_multi` to parse multi-document YAML files.
pub(crate) fn parse_yaml(s: &str) -> Result<Value> {
    let docs = YamlLoader::load_from_str(s)?;
    match docs.len() {
        0 => Ok(Value::Null),
        1 => {
            // Safe to use .into_iter().next() since we verified len == 1
            Ok(yaml_to_value(docs.into_iter().next().unwrap_or(Yaml::Null)))
        }
        n => Err(Error::deserialize(format!(
            "multi-document YAML is not supported (found {n} documents). Use `from_str_multi` for multi-document YAML, or `---` only at the start of a single document."
        ))),
    }
}

/// Parse a YAML string that may contain multiple documents.
///
/// Returns a vector of Values, one for each document in the YAML string.
/// An empty string returns an empty vector.
///
/// # Errors
///
/// Returns an error if the YAML string is invalid.
#[cfg(test)]
pub(crate) fn parse_yaml_multi(s: &str) -> Result<Vec<Value>> {
    let docs = YamlLoader::load_from_str(s)?;
    Ok(docs.into_iter().map(yaml_to_value).collect())
}

/// Implement Deserialize for Value so it can deserialize into itself.
impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("any valid YAML value")
            }

            fn visit_bool<E>(self, v: bool) -> std::result::Result<Self::Value, E> {
                Ok(Value::Bool(v))
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E> {
                Ok(Value::from(v))
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E> {
                Ok(Value::from(v))
            }

            fn visit_f64<E>(self, v: f64) -> std::result::Result<Self::Value, E> {
                Ok(Value::from(v))
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E> {
                Ok(Value::String(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> std::result::Result<Self::Value, E> {
                Ok(Value::String(v))
            }

            fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(Value::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                serde::Deserialize::deserialize(deserializer)
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(Value::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element()? {
                    vec.push(elem);
                }
                Ok(Value::Sequence(vec))
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut mapping = Mapping::new();
                while let Some((key, value)) = map.next_entry()? {
                    mapping.insert(key, value);
                }
                Ok(Value::Mapping(mapping))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

/// A deserializer that owns its Value.
pub struct ValueDeserializer {
    value: Value,
}

impl ValueDeserializer {
    /// Create a new deserializer from an owned Value.
    pub fn new(value: Value) -> Self {
        Self { value }
    }
}

impl<'de> de::Deserializer<'de> for ValueDeserializer {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Null => visitor.visit_unit(),
            Value::Bool(b) => visitor.visit_bool(b),
            Value::Number(n) => match n {
                Number::PosInt(i) => visitor.visit_u64(i),
                Number::NegInt(i) => visitor.visit_i64(i),
                Number::Float(f) => visitor.visit_f64(f),
            },
            Value::String(s) => visitor.visit_string(s),
            Value::Sequence(seq) => {
                let seq_access = OwnedSeqDeserializer::new(seq.into_iter());
                visitor.visit_seq(seq_access)
            }
            Value::Mapping(map) => {
                let map_access = OwnedMapDeserializer::new(map.into_iter());
                visitor.visit_map(map_access)
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Bool(b) => visitor.visit_bool(b),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "yes" | "on" => visitor.visit_bool(true),
                "false" | "no" | "off" => visitor.visit_bool(false),
                _ => Err(Error::deserialize(format!(
                    "expected bool, found string: {s}"
                ))),
            },
            _ => Err(Error::deserialize(format!(
                "expected bool, found {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            // Simple string variant
            Value::String(s) => visitor.visit_enum(OwnedEnumDeserializer::Unit(s)),
            // Mapping variant: { VariantName: value }
            Value::Mapping(mut map) => {
                if map.len() != 1 {
                    return Err(Error::deserialize(
                        "expected enum with single variant as key",
                    ));
                }
                let (key, value) = map.pop().ok_or_else(|| Error::deserialize("empty map"))?;
                let Value::String(variant) = key else {
                    return Err(Error::deserialize("expected string key for enum variant"));
                };
                visitor.visit_enum(OwnedEnumDeserializer::WithValue { variant, value })
            }
            _ => Err(Error::deserialize(format!(
                "expected string or mapping for enum, found {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Null => visitor.visit_unit(),
            _ => Err(Error::deserialize(format!(
                "expected null, found {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Sequence(seq) => {
                let seq_access = OwnedSeqDeserializer::new(seq.into_iter());
                visitor.visit_seq(seq_access)
            }
            _ => Err(Error::deserialize(format!(
                "expected sequence, found {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Mapping(map) => {
                let map_access = OwnedMapDeserializer::new(map.into_iter());
                visitor.visit_map(map_access)
            }
            _ => Err(Error::deserialize(format!(
                "expected mapping, found {:?}",
                self.value
            ))),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // Allow Null to be deserialized as an empty struct (for structs with all default fields)
        match self.value {
            Value::Null => {
                let map_access = OwnedMapDeserializer::new(std::iter::empty());
                visitor.visit_map(map_access)
            }
            Value::Mapping(map) => {
                let map_access = OwnedMapDeserializer::new(map.into_iter());
                visitor.visit_map(map_access)
            }
            _ => Err(Error::deserialize(format!(
                "expected mapping, found {:?}",
                self.value
            ))),
        }
    }

    forward_to_deserialize_any! {
        i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf identifier ignored_any
    }
}

/// Deserializer for owned sequences.
struct OwnedSeqDeserializer<I> {
    iter: I,
}

impl<I> OwnedSeqDeserializer<I> {
    fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<'de, I> SeqAccess<'de> for OwnedSeqDeserializer<I>
where
    I: Iterator<Item = Value>,
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some(value) => seed.deserialize(ValueDeserializer::new(value)).map(Some),
            None => Ok(None),
        }
    }
}

/// Deserializer for owned mappings.
struct OwnedMapDeserializer<I> {
    iter: I,
    current_value: Option<Value>,
}

impl<I> OwnedMapDeserializer<I> {
    fn new(iter: I) -> Self {
        Self {
            iter,
            current_value: None,
        }
    }
}

impl<'de, I> MapAccess<'de> for OwnedMapDeserializer<I>
where
    I: Iterator<Item = (Value, Value)>,
{
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key, value)) => {
                self.current_value = Some(value);
                seed.deserialize(ValueDeserializer::new(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        match self.current_value.take() {
            Some(value) => seed.deserialize(ValueDeserializer::new(value)),
            None => Err(Error::deserialize("expected map value")),
        }
    }
}

/// Owned enum deserializer.
enum OwnedEnumDeserializer {
    /// A unit variant (just a string).
    Unit(String),
    /// A variant with data.
    WithValue { variant: String, value: Value },
}

impl<'de> EnumAccess<'de> for OwnedEnumDeserializer {
    type Error = Error;
    type Variant = OwnedVariantDeserializer;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, <Self as EnumAccess<'de>>::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        match self {
            OwnedEnumDeserializer::Unit(variant) => {
                use serde::de::value::StringDeserializer;
                let deserializer: StringDeserializer<Error> = variant.into_deserializer();
                let variant_value = seed.deserialize(deserializer)?;
                Ok((variant_value, OwnedVariantDeserializer::Unit))
            }
            OwnedEnumDeserializer::WithValue { variant, value } => {
                use serde::de::value::StringDeserializer;
                let deserializer: StringDeserializer<Error> = variant.into_deserializer();
                let variant_value = seed.deserialize(deserializer)?;
                Ok((variant_value, OwnedVariantDeserializer::Value(value)))
            }
        }
    }
}

/// Owned variant deserializer.
enum OwnedVariantDeserializer {
    /// A unit variant.
    Unit,
    /// A variant with a value.
    Value(Value),
}

impl<'de> VariantAccess<'de> for OwnedVariantDeserializer {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        match self {
            OwnedVariantDeserializer::Unit => Ok(()),
            OwnedVariantDeserializer::Value(value) => {
                if matches!(value, Value::Null) {
                    Ok(())
                } else {
                    Err(Error::deserialize(format!(
                        "expected null for unit variant, found {value:?}"
                    )))
                }
            }
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        match self {
            OwnedVariantDeserializer::Unit => Err(Error::deserialize(
                "expected newtype variant, found unit variant",
            )),
            OwnedVariantDeserializer::Value(value) => {
                seed.deserialize(ValueDeserializer::new(value))
            }
        }
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self {
            OwnedVariantDeserializer::Unit => Err(Error::deserialize(
                "expected tuple variant, found unit variant",
            )),
            OwnedVariantDeserializer::Value(value) => {
                de::Deserializer::deserialize_seq(ValueDeserializer::new(value), visitor)
            }
        }
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self {
            OwnedVariantDeserializer::Unit => Err(Error::deserialize(
                "expected struct variant, found unit variant",
            )),
            OwnedVariantDeserializer::Value(value) => {
                de::Deserializer::deserialize_map(ValueDeserializer::new(value), visitor)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn test_deserialize_primitives() {
        let value = Value::Bool(true);
        let result: bool =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert!(result);

        let value = Value::Number(Number::PosInt(42));
        let result: u64 =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, 42);

        let value = Value::Number(Number::NegInt(-10));
        let result: i64 =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, -10);

        let value = Value::Number(Number::Float(3.15));
        let result: f64 =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert!((result - 3.15).abs() < f64::EPSILON);

        let value = Value::String("hello".into());
        let result: String =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_deserialize_option() {
        let value = Value::Null;
        let result: Option<i32> =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, None);

        let value = Value::Number(Number::PosInt(42));
        let result: Option<u64> =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_deserialize_sequence() {
        let value = Value::Sequence(vec![
            Value::Number(Number::PosInt(1)),
            Value::Number(Number::PosInt(2)),
            Value::Number(Number::PosInt(3)),
        ]);
        let result: Vec<u64> =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_deserialize_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Person {
            name: String,
            age: u32,
        }

        let mut map = Mapping::new();
        map.insert(Value::String("name".into()), Value::String("Alice".into()));
        map.insert(
            Value::String("age".into()),
            Value::Number(Number::PosInt(30)),
        );
        let value = Value::Mapping(map);

        let result: Person =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(
            result,
            Person {
                name: "Alice".into(),
                age: 30
            }
        );
    }

    #[test]
    fn test_deserialize_enum() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum Color {
            Red,
            Green,
            Blue,
        }

        let value = Value::String("Red".into());
        let result: Color =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(result, Color::Red);
    }

    #[test]
    fn test_deserialize_nested() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Inner {
            value: i32,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct Outer {
            inner: Inner,
            name: String,
        }

        let mut inner_map = Mapping::new();
        inner_map.insert(
            Value::String("value".into()),
            Value::Number(Number::PosInt(42)),
        );

        let mut outer_map = Mapping::new();
        outer_map.insert(Value::String("inner".into()), Value::Mapping(inner_map));
        outer_map.insert(Value::String("name".into()), Value::String("test".into()));

        let value = Value::Mapping(outer_map);
        let result: Outer =
            serde::Deserialize::deserialize(ValueDeserializer::new(value)).expect("deserialize");
        assert_eq!(
            result,
            Outer {
                inner: Inner { value: 42 },
                name: "test".into()
            }
        );
    }

    #[test]
    fn test_yaml_to_value() {
        let yaml = YamlLoader::load_from_str("key: value").expect("valid YAML");
        let value = yaml_to_value(yaml.into_iter().next().expect("should have one document"));
        assert!(value.is_mapping());
        assert_eq!(value.get("key").and_then(Value::as_str), Some("value"));
    }

    #[test]
    fn test_parse_yaml() {
        let value = parse_yaml("42").expect("valid YAML");
        assert_eq!(value.as_i64(), Some(42));

        let value = parse_yaml("true").expect("valid YAML");
        assert_eq!(value.as_bool(), Some(true));

        let value = parse_yaml("hello").expect("valid YAML");
        assert_eq!(value.as_str(), Some("hello"));

        let value = parse_yaml("[1, 2, 3]").expect("valid YAML");
        assert!(value.is_sequence());

        let value = parse_yaml("key: value").expect("valid YAML");
        assert!(value.is_mapping());
    }

    #[test]
    fn test_parse_yaml_empty() {
        let value = parse_yaml("").expect("empty YAML is valid");
        assert!(value.is_null());
    }

    #[test]
    fn test_parse_yaml_multi_document_error() {
        // Multi-document YAML should return an error for single-doc parse
        let result = parse_yaml("---\nfirst: 1\n---\nsecond: 2");
        assert!(result.is_err());
        let err = result.expect_err("should be an error");
        assert!(
            err.to_string().contains("multi-document"),
            "Error should mention multi-document: {err}"
        );
    }

    #[test]
    fn test_parse_yaml_multi() {
        let values =
            parse_yaml_multi("---\nfirst: 1\n---\nsecond: 2").expect("valid multi-document YAML");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get("first").and_then(Value::as_i64), Some(1));
        assert_eq!(values[1].get("second").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn test_parse_yaml_multi_empty() {
        let values = parse_yaml_multi("").expect("empty YAML is valid");
        assert!(values.is_empty());
    }

    #[test]
    fn test_parse_yaml_multi_single() {
        let values = parse_yaml_multi("key: value").expect("single-doc YAML is valid");
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].get("key").and_then(Value::as_str), Some("value"));
    }

    // ============================================================
    // Additional parse_yaml tests for comprehensive coverage
    // ============================================================

    #[test]
    fn test_parse_yaml_single_with_explicit_start() {
        let value = parse_yaml("---\nkey: value").expect("explicit doc start is valid");
        assert_eq!(value.get("key").and_then(Value::as_str), Some("value"));
    }

    #[test]
    fn test_parse_yaml_single_with_explicit_end() {
        let value = parse_yaml("key: value\n...").expect("explicit doc end is valid");
        assert_eq!(value.get("key").and_then(Value::as_str), Some("value"));
    }

    #[test]
    fn test_parse_yaml_single_scalar() {
        let value = parse_yaml("42").expect("scalar is valid");
        assert_eq!(value.as_i64(), Some(42));

        let value = parse_yaml("hello").expect("string scalar is valid");
        assert_eq!(value.as_str(), Some("hello"));

        let value = parse_yaml("true").expect("bool scalar is valid");
        assert_eq!(value.as_bool(), Some(true));
    }

    #[test]
    fn test_parse_yaml_single_sequence() {
        let value = parse_yaml("- a\n- b\n- c").expect("sequence is valid");
        let seq = value.as_sequence().expect("should be sequence");
        assert_eq!(seq.len(), 3);
        assert_eq!(seq[0].as_str(), Some("a"));
        assert_eq!(seq[1].as_str(), Some("b"));
        assert_eq!(seq[2].as_str(), Some("c"));
    }

    #[test]
    fn test_parse_yaml_single_mapping() {
        let value = parse_yaml("a: 1\nb: 2").expect("mapping is valid");
        assert_eq!(value.get("a").and_then(Value::as_i64), Some(1));
        assert_eq!(value.get("b").and_then(Value::as_i64), Some(2));
    }

    // ============================================================
    // Additional parse_yaml_multi tests for comprehensive coverage
    // ============================================================

    #[test]
    fn test_parse_yaml_multi_with_explicit_end_markers() {
        let values = parse_yaml_multi("---\na: 1\n...\n---\nb: 2\n...")
            .expect("explicit end markers are valid");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get("a").and_then(Value::as_i64), Some(1));
        assert_eq!(values[1].get("b").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn test_parse_yaml_multi_with_null_doc() {
        let values =
            parse_yaml_multi("---\na: 1\n---\nnull\n---\nc: 3").expect("null doc is valid");
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].get("a").and_then(Value::as_i64), Some(1));
        assert!(values[1].is_null());
        assert_eq!(values[2].get("c").and_then(Value::as_i64), Some(3));
    }

    #[test]
    fn test_parse_yaml_multi_scalars_only() {
        let values =
            parse_yaml_multi("---\n42\n---\nhello\n---\ntrue").expect("scalar docs are valid");
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_i64(), Some(42));
        assert_eq!(values[1].as_str(), Some("hello"));
        assert_eq!(values[2].as_bool(), Some(true));
    }

    #[test]
    fn test_parse_yaml_multi_sequences_only() {
        let values =
            parse_yaml_multi("---\n- 1\n- 2\n---\n- a\n- b").expect("sequence docs are valid");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].as_sequence().map(Vec::len), Some(2));
        assert_eq!(values[1].as_sequence().map(Vec::len), Some(2));
    }

    #[test]
    fn test_parse_yaml_multi_with_comments() {
        let values = parse_yaml_multi("---\n# comment\na: 1\n---\n# another\nb: 2")
            .expect("comments are valid");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get("a").and_then(Value::as_i64), Some(1));
        assert_eq!(values[1].get("b").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn test_parse_yaml_multi_with_anchors() {
        // Anchors should be document-local
        let values = parse_yaml_multi("---\nval: &v 1\nref: *v\n---\nval: &v 2\nref: *v")
            .expect("anchors in multi-doc are valid");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get("val").and_then(Value::as_i64), Some(1));
        assert_eq!(values[0].get("ref").and_then(Value::as_i64), Some(1));
        assert_eq!(values[1].get("val").and_then(Value::as_i64), Some(2));
        assert_eq!(values[1].get("ref").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn test_parse_yaml_multi_many_documents() {
        let yaml = (0..20)
            .map(|i| format!("---\nid: {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let values = parse_yaml_multi(&yaml).expect("many documents are valid");
        assert_eq!(values.len(), 20);
        for (i, v) in values.iter().enumerate() {
            assert_eq!(
                v.get("id").and_then(Value::as_i64),
                Some(i64::try_from(i).expect("i fits in i64"))
            );
        }
    }

    #[test]
    fn test_parse_yaml_multi_deeply_nested() {
        let yaml = r"
---
level1:
  level2:
    level3:
      value: deep
---
simple: shallow
";
        let values = parse_yaml_multi(yaml).expect("nested doc is valid");
        assert_eq!(values.len(), 2);
        let deep_value = values[0]
            .get("level1")
            .and_then(|v| v.get("level2"))
            .and_then(|v| v.get("level3"))
            .and_then(|v| v.get("value"))
            .and_then(Value::as_str);
        assert_eq!(deep_value, Some("deep"));
        assert_eq!(
            values[1].get("simple").and_then(Value::as_str),
            Some("shallow")
        );
    }

    #[test]
    fn test_parse_yaml_invalid_syntax() {
        let result = parse_yaml("key: [unclosed");
        let _ = result.expect_err("should fail on invalid YAML");
    }

    #[test]
    fn test_parse_yaml_multi_invalid_syntax() {
        let result = parse_yaml_multi("---\nvalid: ok\n---\nkey: [unclosed");
        let _ = result.expect_err("should fail on invalid YAML");
    }

    #[test]
    fn test_parse_yaml_whitespace_only() {
        let value = parse_yaml("   \n\n  \t  ").expect("whitespace only is valid");
        assert!(value.is_null());
    }

    #[test]
    fn test_parse_yaml_multi_whitespace_between() {
        let values =
            parse_yaml_multi("---\na: 1\n\n\n---\nb: 2").expect("whitespace between docs is valid");
        assert_eq!(values.len(), 2);
    }
}
