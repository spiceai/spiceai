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

//! Taking known secret values out of text that may repeat them, such as an
//! error a source returned or a recorded result.

use std::borrow::Cow;

/// What a value taken out is replaced with.
pub const REDACTED: &str = "<redacted>";

/// A part of a value shorter than this is not taken out on its own: bare, it
/// would match unrelated text such as a line number.
pub const SHORTEST_PART: usize = 4;

/// `message` without `values`: each value, and each part of it that a source
/// can repeat on its own, as a database client repeats a connection string's
/// `password=...` without the rest of it; and each of those as JSON writes it
/// inside a string, as a recorded result does, where a quote or backslash is
/// escaped.
#[must_use]
pub fn redact_values<'m>(message: &'m str, values: &[String]) -> Cow<'m, str> {
    let mut present: Vec<Cow<'_, str>> = Vec::new();
    let spellings = values.iter().flat_map(|value| {
        std::iter::once(value.as_str()).chain(
            value
                .split(|c: char| c.is_whitespace() || matches!(c, ';' | '&' | '?'))
                .filter(|part| part.chars().count() >= SHORTEST_PART),
        )
    });
    for value in spellings.filter(|value| !value.is_empty()) {
        if message.contains(value) {
            present.push(Cow::Borrowed(value));
        }
        if let Some(escaped) = json_escaped(value)
            && message.contains(escaped.as_str())
        {
            present.push(Cow::Owned(escaped));
        }
    }
    if present.is_empty() {
        return Cow::Borrowed(message);
    }
    // The longest first, so a quoted value goes whole rather than leaving its
    // quotes behind.
    present.sort_by_key(|value| std::cmp::Reverse(value.len()));
    present.dedup();
    let mut message = message.to_string();
    for value in present {
        message = message.replace(value.as_ref(), REDACTED);
    }
    Cow::Owned(message)
}

/// `value` as JSON writes it inside a string, when that differs from `value`.
fn json_escaped(value: &str) -> Option<String> {
    let quoted = serde_json::to_string(value).ok()?;
    let escaped = quoted.get(1..quoted.len() - 1)?;
    (escaped != value).then(|| escaped.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connection string goes whole, and each part of it a source can repeat
    /// alone goes too; a short part stays, so a port or a line number does.
    #[test]
    fn a_value_and_its_parts_are_taken_out() {
        let values = ["dbname=d host=db.internal port=5432 password=hunter2".to_string()];
        assert_eq!(
            redact_values(
                "Unable to connect at \"dbname=d host=db.internal port=5432 password=hunter2\": line 1",
                &values
            ),
            "Unable to connect at \"<redacted>\": line 1"
        );
        assert_eq!(
            redact_values("password=hunter2 refused for d", &values),
            "<redacted> refused for d"
        );
        assert!(matches!(
            redact_values("nothing to take out", &values),
            Cow::Borrowed(_)
        ));
    }
}
