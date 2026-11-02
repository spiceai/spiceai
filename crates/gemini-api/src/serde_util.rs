/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Reading JSON the way Google's parser reads it.

use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserialize, Deserializer, IntoDeserializer, MapAccess, SeqAccess, Visitor};

/// Reads a repeated field as Google's parser does: an array, or a single value standing for an
/// array of one, as in Google's own examples (`"contents": {"parts": {"text": "..."}}`); `null`
/// for none.
pub(crate) fn one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct OneOrMany<T>(PhantomData<T>);

    impl<'de, T: Deserialize<'de>> Visitor<'de> for OneOrMany<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a value or an array of values")
        }

        fn visit_unit<E: de::Error>(self) -> Result<Vec<T>, E> {
            Ok(Vec::new())
        }

        fn visit_none<E: de::Error>(self) -> Result<Vec<T>, E> {
            Ok(Vec::new())
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
            let mut items = Vec::new();
            while let Some(item) = seq.next_element()? {
                items.push(item);
            }
            Ok(items)
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Vec<T>, A::Error> {
            T::deserialize(de::value::MapAccessDeserializer::new(map)).map(|item| vec![item])
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Vec<T>, E> {
            T::deserialize(value.into_deserializer()).map(|item| vec![item])
        }
    }

    deserializer.deserialize_any(OneOrMany(PhantomData))
}
