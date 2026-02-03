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

use indexmap::IndexMap;
use std::fmt;

/// A YAML mapping (ordered key-value pairs).
pub type Mapping = IndexMap<Value, Value>;

/// Represents a YAML value.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    /// Represents a YAML null value.
    #[default]
    Null,
    /// Represents a YAML boolean.
    Bool(bool),
    /// Represents a YAML number (stored as string to preserve precision).
    Number(Number),
    /// Represents a YAML string.
    String(String),
    /// Represents a YAML sequence (array).
    Sequence(Vec<Value>),
    /// Represents a YAML mapping (object).
    Mapping(Mapping),
}

/// A YAML number that can be either integer or floating point.
#[derive(Debug, Clone)]
pub enum Number {
    /// A positive integer.
    PosInt(u64),
    /// A negative integer.
    NegInt(i64),
    /// A floating point number.
    Float(f64),
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Number::PosInt(a), Number::PosInt(b)) => a == b,
            (Number::NegInt(a), Number::NegInt(b)) => a == b,
            (Number::Float(a), Number::Float(b)) => {
                // Use bitwise comparison for floats to maintain Eq/Hash contract.
                // This ensures that equal values always hash the same.
                // Note: This means NaN values with different bit patterns are not equal,
                // and -0.0 != 0.0, which differs from IEEE 754 but is required for
                // correct HashMap/HashSet behavior.
                a.to_bits() == b.to_bits()
            }
            _ => false,
        }
    }
}

impl Eq for Number {}

impl std::hash::Hash for Number {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Number::PosInt(n) => {
                0u8.hash(state);
                n.hash(state);
            }
            Number::NegInt(n) => {
                1u8.hash(state);
                n.hash(state);
            }
            Number::Float(n) => {
                2u8.hash(state);
                n.to_bits().hash(state);
            }
        }
    }
}

impl Number {
    /// Returns true if this is a positive integer.
    #[must_use]
    pub fn is_u64(&self) -> bool {
        matches!(self, Number::PosInt(_))
    }

    /// Returns true if this is a negative integer.
    #[must_use]
    pub fn is_i64(&self) -> bool {
        matches!(self, Number::NegInt(_) | Number::PosInt(_))
    }

    /// Returns true if this is a floating point number.
    #[must_use]
    pub fn is_f64(&self) -> bool {
        matches!(self, Number::Float(_))
    }

    /// If the number is a u64, returns it. Otherwise returns None.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "explicit conversion with bounds checking"
    )]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Number::PosInt(n) => Some(*n),
            Number::NegInt(n) => (*n).try_into().ok(),
            Number::Float(n) => {
                if n.fract() == 0.0 && *n >= 0.0 && *n <= u64::MAX as f64 {
                    Some(*n as u64)
                } else {
                    None
                }
            }
        }
    }

    /// If the number is representable as i64, returns it. Otherwise returns None.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        reason = "explicit conversion with bounds checking"
    )]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Number::PosInt(n) => (*n).try_into().ok(),
            Number::NegInt(n) => Some(*n),
            Number::Float(n) => {
                if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                    Some(*n as i64)
                } else {
                    None
                }
            }
        }
    }

    /// Returns the number as f64.
    #[must_use]
    #[expect(clippy::cast_precision_loss, reason = "intentional conversion to f64")]
    pub fn as_f64(&self) -> f64 {
        match self {
            Number::PosInt(n) => *n as f64,
            Number::NegInt(n) => *n as f64,
            Number::Float(n) => *n,
        }
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Number::PosInt(n) => write!(f, "{n}"),
            Number::NegInt(n) => write!(f, "{n}"),
            Number::Float(n) => {
                if n.is_nan() {
                    write!(f, ".nan")
                } else if n.is_infinite() {
                    if n.is_sign_positive() {
                        write!(f, ".inf")
                    } else {
                        write!(f, "-.inf")
                    }
                } else {
                    write!(f, "{n}")
                }
            }
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => 0u8.hash(state),
            Value::Bool(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            Value::Number(n) => {
                2u8.hash(state);
                n.hash(state);
            }
            Value::String(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Value::Sequence(seq) => {
                4u8.hash(state);
                seq.hash(state);
            }
            Value::Mapping(map) => {
                5u8.hash(state);
                for (k, v) in map {
                    k.hash(state);
                    v.hash(state);
                }
            }
        }
    }
}

impl Value {
    /// Returns true if the value is null.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns true if the value is a boolean.
    #[must_use]
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Returns true if the value is a number.
    #[must_use]
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Number(_))
    }

    /// Returns true if the value is a string.
    #[must_use]
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Returns true if the value is a sequence.
    #[must_use]
    pub fn is_sequence(&self) -> bool {
        matches!(self, Value::Sequence(_))
    }

    /// Returns true if the value is a mapping.
    #[must_use]
    pub fn is_mapping(&self) -> bool {
        matches!(self, Value::Mapping(_))
    }

    /// If the value is a boolean, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// If the value is a number, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_number(&self) -> Option<&Number> {
        match self {
            Value::Number(n) => Some(n),
            _ => None,
        }
    }

    /// If the value is a u64, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        self.as_number().and_then(Number::as_u64)
    }

    /// If the value is representable as i64, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        self.as_number().and_then(Number::as_i64)
    }

    /// If the value is a number, returns it as f64. Otherwise returns None.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        self.as_number().map(Number::as_f64)
    }

    /// If the value is a string, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// If the value is a sequence, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_sequence(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Sequence(seq) => Some(seq),
            _ => None,
        }
    }

    /// If the value is a sequence, returns a mutable reference. Otherwise returns None.
    #[must_use]
    pub fn as_sequence_mut(&mut self) -> Option<&mut Vec<Value>> {
        match self {
            Value::Sequence(seq) => Some(seq),
            _ => None,
        }
    }

    /// If the value is a mapping, returns it. Otherwise returns None.
    #[must_use]
    pub fn as_mapping(&self) -> Option<&Mapping> {
        match self {
            Value::Mapping(map) => Some(map),
            _ => None,
        }
    }

    /// If the value is a mapping, returns a mutable reference. Otherwise returns None.
    #[must_use]
    pub fn as_mapping_mut(&mut self) -> Option<&mut Mapping> {
        match self {
            Value::Mapping(map) => Some(map),
            _ => None,
        }
    }

    /// Index into a YAML sequence or mapping.
    /// Returns None if the index/key doesn't exist or if the value is not indexable.
    #[must_use]
    pub fn get<I: Index>(&self, index: I) -> Option<&Value> {
        index.index_into(self)
    }

    /// Mutably index into a YAML sequence or mapping.
    #[must_use]
    pub fn get_mut<I: Index>(&mut self, index: I) -> Option<&mut Value> {
        index.index_into_mut(self)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<i8> for Value {
    #[expect(clippy::cast_sign_loss, reason = "checked non-negative")]
    fn from(n: i8) -> Self {
        Value::Number(if n >= 0 {
            Number::PosInt(n as u64)
        } else {
            Number::NegInt(i64::from(n))
        })
    }
}

impl From<i16> for Value {
    #[expect(clippy::cast_sign_loss, reason = "checked non-negative")]
    fn from(n: i16) -> Self {
        Value::Number(if n >= 0 {
            Number::PosInt(n as u64)
        } else {
            Number::NegInt(i64::from(n))
        })
    }
}

impl From<i32> for Value {
    #[expect(clippy::cast_sign_loss, reason = "checked non-negative")]
    fn from(n: i32) -> Self {
        Value::Number(if n >= 0 {
            Number::PosInt(n as u64)
        } else {
            Number::NegInt(i64::from(n))
        })
    }
}

impl From<i64> for Value {
    #[expect(clippy::cast_sign_loss, reason = "checked non-negative")]
    fn from(n: i64) -> Self {
        Value::Number(if n >= 0 {
            Number::PosInt(n as u64)
        } else {
            Number::NegInt(n)
        })
    }
}

impl From<u8> for Value {
    fn from(n: u8) -> Self {
        Value::Number(Number::PosInt(u64::from(n)))
    }
}

impl From<u16> for Value {
    fn from(n: u16) -> Self {
        Value::Number(Number::PosInt(u64::from(n)))
    }
}

impl From<u32> for Value {
    fn from(n: u32) -> Self {
        Value::Number(Number::PosInt(u64::from(n)))
    }
}

impl From<u64> for Value {
    fn from(n: u64) -> Self {
        Value::Number(Number::PosInt(n))
    }
}

impl From<f32> for Value {
    fn from(n: f32) -> Self {
        Value::Number(Number::Float(f64::from(n)))
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Number(Number::Float(n))
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::Sequence(v.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

/// A trait for types that can be used to index into a Value.
pub trait Index: private::Sealed {
    /// Return None if the key is not already in the sequence or mapping.
    #[doc(hidden)]
    fn index_into<'v>(&self, v: &'v Value) -> Option<&'v Value>;

    /// Return None if the key is not already in the sequence or mapping.
    #[doc(hidden)]
    fn index_into_mut<'v>(&self, v: &'v mut Value) -> Option<&'v mut Value>;
}

impl Index for usize {
    fn index_into<'v>(&self, v: &'v Value) -> Option<&'v Value> {
        match v {
            Value::Sequence(seq) => seq.get(*self),
            _ => None,
        }
    }

    fn index_into_mut<'v>(&self, v: &'v mut Value) -> Option<&'v mut Value> {
        match v {
            Value::Sequence(seq) => seq.get_mut(*self),
            _ => None,
        }
    }
}

impl Index for str {
    fn index_into<'v>(&self, v: &'v Value) -> Option<&'v Value> {
        match v {
            Value::Mapping(map) => map.get(&Value::String(self.to_owned())),
            _ => None,
        }
    }

    fn index_into_mut<'v>(&self, v: &'v mut Value) -> Option<&'v mut Value> {
        match v {
            Value::Mapping(map) => map.get_mut(&Value::String(self.to_owned())),
            _ => None,
        }
    }
}

impl Index for String {
    fn index_into<'v>(&self, v: &'v Value) -> Option<&'v Value> {
        self.as_str().index_into(v)
    }

    fn index_into_mut<'v>(&self, v: &'v mut Value) -> Option<&'v mut Value> {
        self.as_str().index_into_mut(v)
    }
}

impl<T: Index + ?Sized> Index for &T {
    fn index_into<'v>(&self, v: &'v Value) -> Option<&'v Value> {
        (*self).index_into(v)
    }

    fn index_into_mut<'v>(&self, v: &'v mut Value) -> Option<&'v mut Value> {
        (*self).index_into_mut(v)
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for str {}
    impl Sealed for String {}
    impl<T: Sealed + ?Sized> Sealed for &T {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_is_methods() {
        assert!(Value::Null.is_null());
        assert!(Value::Bool(true).is_bool());
        assert!(Value::Number(Number::PosInt(42)).is_number());
        assert!(Value::String("hello".into()).is_string());
        assert!(Value::Sequence(vec![]).is_sequence());
        assert!(Value::Mapping(Mapping::new()).is_mapping());
    }

    #[test]
    fn test_value_as_methods() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::Number(Number::PosInt(42)).as_u64(), Some(42));
        assert_eq!(Value::Number(Number::NegInt(-5)).as_i64(), Some(-5));
        let float_val = Value::Number(Number::Float(3.15));
        assert!((float_val.as_f64().expect("float should convert") - 3.15).abs() < f64::EPSILON);
    }

    #[test]
    fn test_value_indexing() {
        let mut seq = Value::Sequence(vec![
            Value::String("first".into()),
            Value::String("second".into()),
        ]);
        assert_eq!(seq.get(0).and_then(Value::as_str), Some("first"));
        assert_eq!(seq.get(1).and_then(Value::as_str), Some("second"));
        assert!(seq.get(2).is_none());

        // Mutate
        if let Some(v) = seq.get_mut(0) {
            *v = Value::String("modified".into());
        }
        assert_eq!(seq.get(0).and_then(Value::as_str), Some("modified"));
    }

    #[test]
    fn test_mapping_indexing() {
        let mut map = Mapping::new();
        map.insert(Value::String("key".into()), Value::String("value".into()));
        let value = Value::Mapping(map);

        assert_eq!(value.get("key").and_then(Value::as_str), Some("value"));
        assert!(value.get("nonexistent").is_none());
    }

    #[test]
    fn test_number_conversions() {
        let pos = Number::PosInt(42);
        assert_eq!(pos.as_u64(), Some(42));
        assert_eq!(pos.as_i64(), Some(42));
        assert!((pos.as_f64() - 42.0).abs() < f64::EPSILON);

        let neg = Number::NegInt(-10);
        assert_eq!(neg.as_u64(), None);
        assert_eq!(neg.as_i64(), Some(-10));
        assert!((neg.as_f64() - (-10.0)).abs() < f64::EPSILON);

        let float = Number::Float(3.5);
        assert_eq!(float.as_u64(), None);
        assert_eq!(float.as_i64(), None);
        assert!((float.as_f64() - 3.5).abs() < f64::EPSILON);

        // Whole number float
        let whole_float = Number::Float(5.0);
        assert_eq!(whole_float.as_u64(), Some(5));
        assert_eq!(whole_float.as_i64(), Some(5));
    }

    #[test]
    fn test_value_from_conversions() {
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from("hello"), Value::String("hello".into()));
        assert_eq!(Value::from(42u64), Value::Number(Number::PosInt(42)));
        assert_eq!(Value::from(-5i64), Value::Number(Number::NegInt(-5)));

        let seq: Value = vec![1i64, 2, 3].into();
        assert!(seq.is_sequence());

        let some: Value = Some(42i64).into();
        assert!(some.is_number());

        let none: Value = Option::<i64>::None.into();
        assert!(none.is_null());
    }

    #[test]
    fn test_number_display() {
        assert_eq!(Number::PosInt(42).to_string(), "42");
        assert_eq!(Number::NegInt(-10).to_string(), "-10");
        assert_eq!(Number::Float(f64::NAN).to_string(), ".nan");
        assert_eq!(Number::Float(f64::INFINITY).to_string(), ".inf");
        assert_eq!(Number::Float(f64::NEG_INFINITY).to_string(), "-.inf");
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Bool(true), Value::Bool(true));
        assert_ne!(Value::Bool(true), Value::Bool(false));
        assert_eq!(Value::String("test".into()), Value::String("test".into()));
        assert_eq!(
            Value::Number(Number::PosInt(42)),
            Value::Number(Number::PosInt(42))
        );
    }

    #[test]
    fn test_value_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Value::String("test".into()));
        set.insert(Value::Number(Number::PosInt(42)));
        set.insert(Value::Bool(true));
        assert_eq!(set.len(), 3);
        assert!(set.contains(&Value::String("test".into())));
    }

    /// Test that float equality uses bitwise comparison to maintain Eq/Hash contract.
    /// This means -0.0 and 0.0 are treated as different values, and NaN values with
    /// the same bit pattern are equal. This differs from IEEE 754 semantics but is
    /// required for correct HashMap/HashSet behavior.
    #[test]
    fn test_float_equality_bitwise_semantics() {
        // -0.0 and 0.0 have different bit patterns, so they are NOT equal
        // This differs from IEEE 754 where -0.0 == 0.0
        let neg_zero = Number::Float(-0.0_f64);
        let pos_zero = Number::Float(0.0_f64);
        assert_ne!(
            neg_zero, pos_zero,
            "-0.0 and 0.0 should be unequal (bitwise comparison)"
        );

        // Verify they have different bit patterns
        assert_ne!((-0.0_f64).to_bits(), 0.0_f64.to_bits());

        // NaN values with the same bit pattern should be equal
        let nan1 = Number::Float(f64::NAN);
        let nan2 = Number::Float(f64::NAN);
        assert_eq!(nan1, nan2, "NaN with same bits should be equal");

        // Regular floats work as expected
        let a = Number::Float(1.5);
        let b = Number::Float(1.5);
        assert_eq!(a, b);
    }

    /// Test that float hashing is consistent with equality for HashMap/HashSet correctness.
    #[test]
    fn test_float_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_number(n: &Number) -> u64 {
            let mut hasher = DefaultHasher::new();
            n.hash(&mut hasher);
            hasher.finish()
        }

        // Equal values must have equal hashes
        let a = Number::Float(1.5);
        let b = Number::Float(1.5);
        assert_eq!(hash_number(&a), hash_number(&b));

        // NaN values with same bit pattern must have equal hashes
        let nan1 = Number::Float(f64::NAN);
        let nan2 = Number::Float(f64::NAN);
        assert_eq!(hash_number(&nan1), hash_number(&nan2));

        // -0.0 and 0.0 should hash differently (since they are unequal)
        let neg_zero = Number::Float(-0.0_f64);
        let pos_zero = Number::Float(0.0_f64);
        // They are unequal, so their hashes don't need to match (but shouldn't cause issues)
        let _ = hash_number(&neg_zero);
        let _ = hash_number(&pos_zero);
    }

    /// Test that floats work correctly as map keys in `HashSet`.
    #[test]
    fn test_float_in_hashset() {
        use std::collections::HashSet;

        let mut set: HashSet<Number> = HashSet::new();
        set.insert(Number::Float(1.0));
        set.insert(Number::Float(2.0));
        set.insert(Number::Float(-0.0));
        set.insert(Number::Float(0.0));

        // -0.0 and 0.0 are treated as different keys
        assert_eq!(set.len(), 4, "Should have 4 distinct entries");
        assert!(set.contains(&Number::Float(1.0)));
        assert!(set.contains(&Number::Float(-0.0)));
        assert!(set.contains(&Number::Float(0.0)));
    }
}
