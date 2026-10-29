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

/// How many times over JSON can have escaped a value in text it is taken out
/// of: a recorded result is JSON, a tool's answer in it can be JSON, and that
/// answer can quote what a source sent back.
const JSON_DEPTH: usize = 4;

/// `message` without `values`: each value, and each part of it that a source
/// can repeat on its own, as a database client repeats a connection string's
/// `password=...` without the rest of it. JSON can carry them escaped, by
/// whichever encoder wrote it and once for each level JSON nests in JSON's
/// strings, as a recorded result and a tool's answer in it do: each JSON string
/// in `message` that holds one once decoded is written again without it, and
/// each escaped spelling is taken out as written too, for a string cut short.
#[must_use]
pub fn redact_values<'m>(message: &'m str, values: &[String]) -> Cow<'m, str> {
    let spellings: Vec<&str> = values
        .iter()
        .flat_map(|value| {
            std::iter::once(value.as_str()).chain(
                value
                    .split(|c: char| c.is_whitespace() || matches!(c, ';' | '&' | '?'))
                    .filter(|part| part.chars().count() >= SHORTEST_PART),
            )
        })
        .filter(|spelling| !spelling.is_empty())
        .collect();
    if spellings.is_empty() {
        return Cow::Borrowed(message);
    }
    redact_spellings(message, &spellings, JSON_DEPTH)
}

/// `message` without `spellings`, as written and as JSON escapes them, with
/// each JSON string in it redacted the same way once decoded, down to `depth`
/// levels of JSON inside JSON.
fn redact_spellings<'m>(message: &'m str, spellings: &[&str], depth: usize) -> Cow<'m, str> {
    let replaced = replace_spellings(message, spellings);
    if let Cow::Owned(decoded) = redact_json_strings(&replaced, spellings, depth) {
        return Cow::Owned(decoded);
    }
    replaced
}

/// `message` with each of `spellings` it holds, as written or as JSON escapes
/// it up to [`JSON_DEPTH`] times, replaced with [`REDACTED`].
fn replace_spellings<'m>(message: &'m str, spellings: &[&str]) -> Cow<'m, str> {
    let mut present: Vec<Cow<'_, str>> = Vec::new();
    for &spelling in spellings {
        if message.contains(spelling) {
            present.push(Cow::Borrowed(spelling));
        }
        let mut escaped = json_escaped(spelling);
        for _ in 0..JSON_DEPTH {
            let Some(current) = escaped else {
                break;
            };
            escaped = json_escaped(&current);
            if message.contains(current.as_str()) {
                present.push(Cow::Owned(current));
            }
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

/// `text` with each JSON string in it that, decoded, holds one of `spellings`
/// written again without it: the decoded string is redacted as
/// [`redact_spellings`] redacts, one level less deep, so a value escaped by
/// any encoder, or in JSON inside the string, is found. Unchanged when no
/// JSON string in `text` holds one.
fn redact_json_strings<'t>(text: &'t str, spellings: &[&str], depth: usize) -> Cow<'t, str> {
    if depth == 0 {
        return Cow::Borrowed(text);
    }
    let mut redacted = String::new();
    let mut copied = 0;
    let mut from = 0;
    while let Some(offset) = text[from..].find('"') {
        let start = from + offset;
        let Some(end) = closing_quote(text, start) else {
            break;
        };
        let Ok(decoded) = serde_json::from_str::<String>(&text[start..=end]) else {
            // Not a JSON string; the quote that ended it may open one.
            from = start + 1;
            continue;
        };
        if let Cow::Owned(clean) = redact_spellings(&decoded, spellings, depth - 1) {
            redacted.push_str(&text[copied..start]);
            redacted.push_str(
                &serde_json::to_string(&clean).unwrap_or_else(|_| format!("\"{REDACTED}\"")),
            );
            copied = end + 1;
        }
        from = end + 1;
    }
    if copied == 0 {
        return Cow::Borrowed(text);
    }
    redacted.push_str(&text[copied..]);
    Cow::Owned(redacted)
}

/// The index of the quote that closes the JSON string `text` opens at
/// `start`: the next one no backslash escapes.
fn closing_quote(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i),
            _ => i += 1,
        }
    }
    None
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
        assert!(matches!(
            redact_values(
                r#"[{"type": "text", "text": "nothing \"to\" take out"}]"#,
                &values
            ),
            Cow::Borrowed(_)
        ));
    }

    /// A value JSON escapes twice, as a recorded result escapes a tool's answer
    /// that is JSON itself, is taken out, and both stay JSON.
    #[test]
    fn a_value_in_json_inside_json_is_taken_out() {
        let password = "pa\"ss\\word".to_string();
        let answer = serde_json::json!({"error": format!("refused: pg_pass = '{password}'")});
        let recorded =
            serde_json::json!([{"type": "text", "text": answer.to_string()}]).to_string();
        assert!(recorded.contains(r#"pa\\\"ss\\\\word"#), "{recorded}");
        let redacted = redact_values(&recorded, std::slice::from_ref(&password));
        let redacted_answer =
            serde_json::json!({"error": "refused: pg_pass = '<redacted>'"}).to_string();
        assert_eq!(
            redacted,
            serde_json::json!([{"type": "text", "text": redacted_answer}]).to_string()
        );
    }

    /// A value another encoder escaped differently, as Python's `json.dumps`
    /// writes a letter outside ASCII, is found once its JSON string is decoded.
    #[test]
    fn a_value_any_encoder_escaped_is_taken_out() {
        let password = "päss\"word".to_string();
        assert_eq!(
            redact_values(
                r#"Error calling tool peer__sql: {"error": "refused: pg_pass = 'p\u00e4ss\"word'"}"#,
                std::slice::from_ref(&password)
            ),
            r#"Error calling tool peer__sql: {"error": "refused: pg_pass = '<redacted>'"}"#
        );
    }

    /// A JSON string cut short, as a truncated record cuts one, cannot be
    /// decoded, so each escaped spelling is taken out as written.
    #[test]
    fn a_value_escaped_in_a_string_cut_short_is_taken_out() {
        let password = "pa\"ss".to_string();
        assert_eq!(
            redact_values(
                r#"{"text": "{\"error\": \"pg_pass = 'pa\\\"ss' and more"#,
                std::slice::from_ref(&password)
            ),
            r#"{"text": "{\"error\": \"pg_pass = '<redacted>' and more"#
        );
    }
}
