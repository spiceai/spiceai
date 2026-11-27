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

//! Exact extraction of the values under a JSON object's top-level keys, the
//! foundation of storing a JSON column's frequent keys as columns of their own.
//!
//! A query such as `json_get_str(attributes, 'url.path')` parses every row's whole
//! document to find one key. If the text of that key's value is stored beside the
//! document, the same query reads `json_get_str(url_path_value)` instead, a short
//! string that is often the same few values. That rewrite is exact only if
//! `f(document, key, rest...)` equals `f(value_text, rest...)` for every function
//! `f` it applies to and every document, which [`top_level_value`] guarantees by
//! walking the document the way those functions do:
//!
//! - the document must be an object; the first key equal to `key` in document
//!   order is the match, so a duplicate key later in the document is ignored;
//! - keys compare after their escapes are decoded, so `"url"` matches `url`;
//! - the walk stops at the match: whatever follows it, malformed or not, is not
//!   read, by the functions or here.
//!
//! The value text runs from the value's first byte to its last when it parses. When
//! it does not, the functions still read a prefix of it (`json_get_int(doc, 'k', 0)`
//! of `{"k": [1, oops]}` is `1`), so the text is then the rest of the document from
//! the value's first byte, from which they read exactly the same prefix.
//!
//! A key that parses as an integer is never extracted: those functions use such a
//! key as an index when the document is an array, which a top-level value cannot
//! reproduce.

use jiter::{Jiter, Peek};

/// The text of the value under top-level `key` in `json`, as the `json_get_*`
/// functions find it; `None` when they find nothing: `json` is not an object, it
/// has no such key, or it fails to parse before the key is reached.
///
/// See the module documentation for why `f(json, key, rest...)` equals
/// `f(value, rest...)`, and why `key` must not parse as an integer.
#[must_use]
pub(crate) fn top_level_value<'j>(json: &'j str, key: &str) -> Option<&'j str> {
    debug_assert!(
        is_shreddable_key(key),
        "an integer key is an array index to json_get_*"
    );
    let mut jiter = Jiter::new(json.as_bytes());
    if jiter.peek().ok()? != Peek::Object {
        return None;
    }
    let mut next_key = jiter.known_object().ok()??;
    while next_key != key {
        jiter.next_skip().ok()?;
        next_key = jiter.next_key().ok()??;
    }
    let peek = jiter.peek().ok()?;
    let start = jiter.current_index();
    match jiter.known_skip(peek) {
        // jiter stops on ASCII bytes, so both ends are on char boundaries.
        Ok(()) => json.get(start..jiter.current_index()),
        Err(_) => json.get(start..),
    }
}

/// Whether `key` can be extracted with [`top_level_value`]: a key that parses as
/// an integer is an array index to the `json_get_*` functions.
#[must_use]
pub(crate) fn is_shreddable_key(key: &str) -> bool {
    key.parse::<isize>().is_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    /// A small seeded generator, so a failure names the seed that reproduces it.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            // xorshift64*
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n).expect("n fits")).expect("fits")
        }

        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            &items[self.below(items.len())]
        }
    }

    /// Key spellings, including escaped forms of the keys the tests extract, so a
    /// match must compare decoded keys.
    const KEYS: [&str; 9] = [
        r#""a""#,
        r#""a""#,
        r#""url.path""#,
        r#""url.path""#,
        r#""dup""#,
        r#""ké""#,
        r#""ké""#,
        r#""b""#,
        r#""0""#,
    ];

    fn value(rng: &mut Rng, depth: u32) -> String {
        let choice = if depth > 2 { rng.below(8) } else { rng.below(11) };
        match choice {
            0 => r#""plain""#.to_string(),
            1 => r#""esc\"aped\\ é \n""#.to_string(),
            2 => (*rng.pick(&["0", "-7", "200", "9223372036854775807", "9223372036854775808"]))
                .to_string(),
            3 => (*rng.pick(&["1.5", "-0.0", "1e5", "2E-3", "1.0"])).to_string(),
            4 => "true".to_string(),
            5 => "false".to_string(),
            6 => "null".to_string(),
            7 => r#""/api/v1/items""#.to_string(),
            8 => {
                let n = rng.below(4);
                let items: Vec<String> = (0..n).map(|_| value(rng, depth + 1)).collect();
                format!("[{}]", items.join(", "))
            }
            _ => object(rng, depth + 1),
        }
    }

    fn object(rng: &mut Rng, depth: u32) -> String {
        let n = rng.below(5);
        let ws = *rng.pick(&["", " ", "\n\t "]);
        let members: Vec<String> = (0..n)
            .map(|_| format!("{ws}{}{ws}:{ws}{}", rng.pick(&KEYS), value(rng, depth)))
            .collect();
        format!("{{{}{ws}}}", members.join(","))
    }

    /// A document: usually an object, sometimes another JSON value, truncated, with
    /// garbage after it, or not JSON at all.
    fn document(rng: &mut Rng) -> Option<String> {
        let doc = match rng.below(20) {
            0 => return None,
            1 => String::new(),
            2 => value(rng, 0),
            3 => "not json".to_string(),
            4 => format!("{} trailing", object(rng, 0)),
            5 | 6 => {
                let full = object(rng, 0);
                let cut = rng.below(full.len() + 1);
                // Cut on a char boundary.
                let cut = (0..=cut).rev().find(|&i| full.is_char_boundary(i)).unwrap_or(0);
                full[..cut].to_string()
            }
            7 => {
                // A malformed value inside an otherwise well-formed object.
                r#"{"a": [1, 2, oops], "url.path": "x"}"#.to_string()
            }
            _ => object(rng, 0),
        };
        Some(doc)
    }

    const EXTRACTED_KEYS: [&str; 5] = ["a", "url.path", "dup", "ké", "missing"];
    const FUNCTIONS: [&str; 9] = [
        "json_get_str",
        "json_get_int",
        "json_get_float",
        "json_get_bool",
        "json_get_json",
        "json_as_text",
        "json_length",
        "json_get",
        "json_get_array",
    ];
    const REST_PATHS: [&str; 5] = ["", ", 'a'", ", 0", ", 'a', 0", ", 'b'"];

    /// `f(document, key, rest...)` equals `f(top_level_value(document, key), rest...)`
    /// for every function, key and remaining path, over generated documents that
    /// include escaped and duplicate keys, nested and malformed values, non-object
    /// documents, truncated documents and trailing garbage.
    #[tokio::test]
    async fn extracting_a_key_leaves_every_json_function_s_answer_unchanged() {
        // Answers that are not NULL, per function: the comparison must not be
        // vacuous, two NULLs agree whatever the extraction did.
        let mut answered = std::collections::BTreeMap::<&str, usize>::new();
        for seed in [1_u64, 0x5eed, 20_261_127] {
            let mut rng = Rng(seed);
            let docs: Vec<Option<String>> = (0..1500).map(|_| document(&mut rng)).collect();
            let ctx = SessionContext::new();
            let mut ctx_state = ctx.state();
            datafusion_functions_json::register_all(&mut ctx_state).expect("register json functions");
            let ctx = SessionContext::new_with_state(ctx_state);
            for key in EXTRACTED_KEYS {
                let values: StringArray = docs
                    .iter()
                    .map(|d| d.as_deref().and_then(|d| top_level_value(d, key)))
                    .collect();
                let docs_array: StringArray = docs.iter().map(Option::as_deref).collect();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("doc", DataType::Utf8, true),
                    Field::new("val", DataType::Utf8, true),
                ]));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(docs_array) as ArrayRef, Arc::new(values) as ArrayRef],
                )
                .expect("batch");
                let table = MemTable::try_new(schema, vec![vec![batch]]).expect("table");
                ctx.deregister_table("t").expect("deregister");
                ctx.register_table("t", Arc::new(table)).expect("register");
                for f in FUNCTIONS {
                    for rest in REST_PATHS {
                        let sql = format!(
                            "SELECT doc, {f}(doc, '{key}'{rest}) AS whole, {f}(val{rest}) AS extracted FROM t"
                        );
                        let batches = ctx
                            .sql(&sql)
                            .await
                            .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
                            .collect()
                            .await
                            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
                        for b in &batches {
                            for row in 0..b.num_rows() {
                                let whole = ScalarValue::try_from_array(b.column(1), row)
                                    .expect("whole");
                                let extracted = ScalarValue::try_from_array(b.column(2), row)
                                    .expect("extracted");
                                if !whole.is_null() {
                                    *answered.entry(f).or_default() += 1;
                                }
                                assert_eq!(
                                    whole,
                                    extracted,
                                    "seed {seed}: {f}(doc, '{key}'{rest}) differs from {f}(value{rest}) for doc {:?}",
                                    b.column(0).as_any().downcast_ref::<StringArray>().map(|a| {
                                        (!a.is_null(row)).then(|| a.value(row).to_string())
                                    })
                                );
                            }
                        }
                    }
                }
            }
        }
        // Every function answered, many times over, so the comparisons were not vacuous.
        for f in FUNCTIONS {
            let n = answered.get(f).copied().unwrap_or(0);
            assert!(n >= 200, "{f} answered only {n} times: the documents exercise too little");
        }
    }

    #[test]
    fn the_value_is_the_first_match_after_decoding_keys() {
        let doc = r#"{"dup": 1, "url.path": "/a", "dup": 2, "x": {"y": [1, 2]}}"#;
        assert_eq!(top_level_value(doc, "dup"), Some("1"));
        assert_eq!(top_level_value(doc, "url.path"), Some(r#""/a""#));
        assert_eq!(top_level_value(doc, "x"), Some(r#"{"y": [1, 2]}"#));
        assert_eq!(top_level_value(doc, "missing"), None);
        assert_eq!(top_level_value(r#"["a"]"#, "a"), None);
        assert_eq!(top_level_value("", "a"), None);
        // A malformed value: the rest of the document, which the functions read a
        // prefix of exactly as they would in place.
        assert_eq!(
            top_level_value(r#"{"a": [1, oops], "b": 2}"#, "a"),
            Some(r#"[1, oops], "b": 2}"#)
        );
        assert!(!is_shreddable_key("0") && !is_shreddable_key("-3"));
        assert!(is_shreddable_key("url.path") && is_shreddable_key("0x1"));
    }
}
