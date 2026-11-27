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

//! Text a repository or a Hub endpoint decides, as a message prints it.

/// `text` as a message prints text a repository or a Hub endpoint decides, such as a file's
/// path or what a response that could not be read held: printable ASCII as it is, and every
/// other character, a backslash included, escaped as Rust escapes it (`\n`, `\\`, `\u{202e}`),
/// so whoever controls it cannot break the message's line, colour it, or hide or reorder its
/// characters. ASCII punctuation prints as it is, so the text can still read like the text
/// around it.
#[must_use]
pub fn printable(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c != '\\' && (c.is_ascii_graphic() || c == ' ') {
            out.push(c);
        } else {
            out.extend(c.escape_default());
        }
    }
    out
}
