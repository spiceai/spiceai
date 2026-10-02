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

//! The key most of the crate's tests index: one non-nullable `Int64`.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;

use crate::{KeyEncoder, KeyField};

pub(crate) fn encoder() -> KeyEncoder {
    KeyEncoder::new(vec![KeyField::new(DataType::Int64, false)]).expect("int64 key")
}

/// One key column holding `keys`.
pub(crate) fn column(keys: &[i64]) -> Vec<ArrayRef> {
    vec![Arc::new(Int64Array::from(keys.to_vec()))]
}

/// The encoded key `key`.
pub(crate) fn encoded(key: i64) -> Vec<u8> {
    let columns = column(&[key]);
    let encoder = encoder();
    let bound = encoder.bind(&columns).expect("bind");
    let mut out = Vec::new();
    bound.encode_row(0, &mut out);
    out
}

/// The word of the key `key`.
pub(crate) fn word(key: i64) -> u64 {
    encoder().key_word(&encoded(key))
}
