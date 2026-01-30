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

use serde::ser::{
    self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use yaml_rust2::{Yaml, YamlEmitter};

use crate::error::{Error, Result};
use crate::value::{Mapping, Number, Value};

/// Convert our Value type to yaml-rust2's Yaml type.
pub(crate) fn value_to_yaml(value: &Value) -> Yaml {
    match value {
        Value::Null => Yaml::Null,
        Value::Bool(b) => Yaml::Boolean(*b),
        Value::Number(n) => match n {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "YAML only supports i64, large u64 values are rare"
            )]
            Number::PosInt(i) => Yaml::Integer(*i as i64),
            Number::NegInt(i) => Yaml::Integer(*i),
            Number::Float(f) => {
                if f.is_nan() {
                    Yaml::Real(".nan".to_string())
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        Yaml::Real(".inf".to_string())
                    } else {
                        Yaml::Real("-.inf".to_string())
                    }
                } else {
                    Yaml::Real(f.to_string())
                }
            }
        },
        Value::String(s) => Yaml::String(s.clone()),
        Value::Sequence(seq) => Yaml::Array(seq.iter().map(value_to_yaml).collect()),
        Value::Mapping(map) => {
            let mut hash = yaml_rust2::yaml::Hash::new();
            for (k, v) in map {
                hash.insert(value_to_yaml(k), value_to_yaml(v));
            }
            Yaml::Hash(hash)
        }
    }
}

/// Emit a Value as a YAML string.
pub(crate) fn emit_yaml(value: &Value) -> Result<String> {
    let yaml = value_to_yaml(value);
    let mut out = String::new();
    let mut emitter = YamlEmitter::new(&mut out);
    emitter.dump(&yaml)?;
    // Remove the leading "---\n" that yaml-rust2 adds
    let result = out
        .trim_start_matches("---")
        .trim_start_matches('\n')
        .to_string();
    Ok(result)
}

/// A serializer that converts Rust types to Value.
pub struct Serializer;

impl ser::Serializer for Serializer {
    type Ok = Value;
    type Error = Error;

    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = MapSerializer;
    type SerializeStructVariant = StructVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Value> {
        Ok(Value::Bool(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Value> {
        Ok(Value::from(v))
    }

    fn serialize_char(self, v: char) -> Result<Value> {
        Ok(Value::String(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Value> {
        Ok(Value::String(v.to_owned()))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value> {
        Ok(Value::Sequence(v.iter().map(|b| Value::from(*b)).collect()))
    }

    fn serialize_none(self) -> Result<Value> {
        Ok(Value::Null)
    }

    fn serialize_some<T: ?Sized + serde::Serialize>(self, value: &T) -> Result<Value> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value> {
        Ok(Value::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Value> {
        Ok(Value::String(variant.to_owned()))
    }

    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value> {
        let mut map = Mapping::new();
        map.insert(
            Value::String(variant.to_owned()),
            value.serialize(Serializer)?,
        );
        Ok(Value::Mapping(map))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SeqSerializer {
            elements: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(TupleVariantSerializer {
            variant: variant.to_owned(),
            elements: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSerializer {
            entries: Mapping::with_capacity(len.unwrap_or(0)),
            current_key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(StructVariantSerializer {
            variant: variant.to_owned(),
            entries: Mapping::with_capacity(len),
        })
    }
}

/// Serializer for sequences.
pub struct SeqSerializer {
    elements: Vec<Value>,
}

impl SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        self.elements.push(value.serialize(Serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Sequence(self.elements))
    }
}

impl SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value> {
        SerializeSeq::end(self)
    }
}

/// Serializer for tuple variants.
pub struct TupleVariantSerializer {
    variant: String,
    elements: Vec<Value>,
}

impl SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        self.elements.push(value.serialize(Serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        let mut map = Mapping::new();
        map.insert(Value::String(self.variant), Value::Sequence(self.elements));
        Ok(Value::Mapping(map))
    }
}

/// Serializer for maps.
pub struct MapSerializer {
    entries: Mapping,
    current_key: Option<Value>,
}

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: ?Sized + serde::Serialize>(&mut self, key: &T) -> Result<()> {
        self.current_key = Some(key.serialize(Serializer)?);
        Ok(())
    }

    fn serialize_value<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        let key = self
            .current_key
            .take()
            .ok_or_else(|| Error::serialize("serialize_value called before serialize_key"))?;
        self.entries.insert(key, value.serialize(Serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Mapping(self.entries))
    }
}

impl SerializeStruct for MapSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.entries
            .insert(Value::String(key.to_owned()), value.serialize(Serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        Ok(Value::Mapping(self.entries))
    }
}

/// Serializer for struct variants.
pub struct StructVariantSerializer {
    variant: String,
    entries: Mapping,
}

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        self.entries
            .insert(Value::String(key.to_owned()), value.serialize(Serializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value> {
        let mut map = Mapping::new();
        map.insert(Value::String(self.variant), Value::Mapping(self.entries));
        Ok(Value::Mapping(map))
    }
}

/// Implement Serialize for Value.
impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Number(n) => match n {
                Number::PosInt(i) => serializer.serialize_u64(*i),
                Number::NegInt(i) => serializer.serialize_i64(*i),
                Number::Float(f) => serializer.serialize_f64(*f),
            },
            Value::String(s) => serializer.serialize_str(s),
            Value::Sequence(seq) => {
                use serde::ser::SerializeSeq;
                let mut seq_ser = serializer.serialize_seq(Some(seq.len()))?;
                for element in seq {
                    seq_ser.serialize_element(element)?;
                }
                seq_ser.end()
            }
            Value::Mapping(map) => {
                use serde::ser::SerializeMap;
                let mut map_ser = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    map_ser.serialize_entry(k, v)?;
                }
                map_ser.end()
            }
        }
    }
}

/// Implement Serialize for Number.
impl serde::Serialize for Number {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Number::PosInt(i) => serializer.serialize_u64(*i),
            Number::NegInt(i) => serializer.serialize_i64(*i),
            Number::Float(f) => serializer.serialize_f64(*f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn test_serialize_primitives() {
        assert_eq!(
            true.serialize(Serializer).expect("serialize bool"),
            Value::Bool(true)
        );
        assert_eq!(
            42u64.serialize(Serializer).expect("serialize u64"),
            Value::Number(Number::PosInt(42))
        );
        assert_eq!(
            (-10i64).serialize(Serializer).expect("serialize i64"),
            Value::Number(Number::NegInt(-10))
        );
        assert_eq!(
            "hello".serialize(Serializer).expect("serialize str"),
            Value::String("hello".into())
        );
    }

    #[test]
    fn test_serialize_option() {
        assert_eq!(
            Option::<i32>::None
                .serialize(Serializer)
                .expect("serialize None"),
            Value::Null
        );
        assert_eq!(
            Some(42i64).serialize(Serializer).expect("serialize Some"),
            Value::Number(Number::PosInt(42))
        );
    }

    #[test]
    fn test_serialize_sequence() {
        let vec = vec![1u64, 2, 3];
        let value = vec.serialize(Serializer).expect("serialize vec");
        assert!(value.is_sequence());
        let seq = value.as_sequence().expect("should be sequence");
        assert_eq!(seq.len(), 3);
        assert_eq!(seq[0].as_u64(), Some(1));
        assert_eq!(seq[1].as_u64(), Some(2));
        assert_eq!(seq[2].as_u64(), Some(3));
    }

    #[test]
    fn test_serialize_struct() {
        #[derive(Serialize)]
        struct Person {
            name: String,
            age: u32,
        }

        let person = Person {
            name: "Alice".into(),
            age: 30,
        };
        let value = person.serialize(Serializer).expect("serialize struct");
        assert!(value.is_mapping());
        assert_eq!(value.get("name").and_then(Value::as_str), Some("Alice"));
        assert_eq!(value.get("age").and_then(Value::as_u64), Some(30));
    }

    #[test]
    fn test_serialize_enum() {
        #[derive(Serialize)]
        #[expect(dead_code)]
        enum Color {
            Red,
            Green,
            Blue,
        }

        let color = Color::Red;
        let value = color.serialize(Serializer).expect("serialize enum");
        assert_eq!(value.as_str(), Some("Red"));
    }

    #[test]
    fn test_serialize_nested() {
        #[derive(Serialize)]
        struct Inner {
            value: i32,
        }

        #[derive(Serialize)]
        struct Outer {
            inner: Inner,
            name: String,
        }

        let outer = Outer {
            inner: Inner { value: 42 },
            name: "test".into(),
        };
        let value = outer.serialize(Serializer).expect("serialize nested");
        assert!(value.is_mapping());
        assert!(value.get("inner").expect("should have inner").is_mapping());
        assert_eq!(
            value
                .get("inner")
                .and_then(|v| v.get("value"))
                .and_then(Value::as_i64),
            Some(42)
        );
        assert_eq!(value.get("name").and_then(Value::as_str), Some("test"));
    }

    #[test]
    fn test_emit_yaml() {
        let mut map = Mapping::new();
        map.insert(Value::String("key".into()), Value::String("value".into()));
        let value = Value::Mapping(map);
        let yaml = emit_yaml(&value).expect("emit yaml");
        assert!(yaml.contains("key:"));
        assert!(yaml.contains("value"));
    }

    #[test]
    fn test_value_to_yaml() {
        let value = Value::Bool(true);
        let yaml = value_to_yaml(&value);
        assert_eq!(yaml, Yaml::Boolean(true));

        let value = Value::Number(Number::PosInt(42));
        let yaml = value_to_yaml(&value);
        assert_eq!(yaml, Yaml::Integer(42));

        let value = Value::String("hello".into());
        let yaml = value_to_yaml(&value);
        assert_eq!(yaml, Yaml::String("hello".into()));
    }

    #[test]
    fn test_serialize_map_with_various_keys() {
        use std::collections::HashMap;
        let mut map: HashMap<String, i32> = HashMap::new();
        map.insert("one".into(), 1);
        map.insert("two".into(), 2);

        let value = map.serialize(Serializer).expect("serialize map");
        assert!(value.is_mapping());
    }

    #[test]
    fn test_serialize_tuple() {
        let tuple = (1u64, "hello", true);
        let value = tuple.serialize(Serializer).expect("serialize tuple");
        assert!(value.is_sequence());
        let seq = value.as_sequence().expect("should be sequence");
        assert_eq!(seq.len(), 3);
    }
}
