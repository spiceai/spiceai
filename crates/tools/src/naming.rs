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

//! Encoding of catalog-qualified tool names.
//!
//! When Spice exposes a tool that belongs to a non-default catalog, the exposed
//! name has to carry both the catalog and the tool name. MCP clients such as
//! Claude and Cursor reject any tool name that does not match
//! `^[a-zA-Z0-9_-]{1,64}$`, so the previously used `catalog/tool` form (with a
//! `/` separator) left those tools unusable in those harnesses (issue #10894).
//!
//! [`encode_tool_name`] joins the two components with a `__` separator and
//! escapes any literal `__` / `_-_` inside a component so the transform is
//! reversible by [`decode_tool_name`]. `snake_case` names — the overwhelming
//! majority of MCP tool names — contain only single underscores and therefore
//! pass through with no escaping at all.

/// Separator placed between a catalog name and a tool name.
const SEPARATOR: &str = "__";
/// Replacement for a literal `__` inside a single name component.
const ESCAPE: &str = "_-_";
/// Replacement for a literal `_-_` inside a single name component.
const ESCAPED_ESCAPE: &str = "_--_";

/// Escape a single name component so it can never contain a bare `__`
/// separator. The escape sequence is escaped first so decoding is unambiguous.
fn encode_component(component: &str) -> String {
    component
        .replace(ESCAPE, ESCAPED_ESCAPE)
        .replace(SEPARATOR, ESCAPE)
}

/// Reverse of [`encode_component`]. The replacements are undone in the opposite
/// order to the ones applied by [`encode_component`].
fn decode_component(component: &str) -> String {
    component
        .replace(ESCAPE, SEPARATOR)
        .replace(ESCAPED_ESCAPE, ESCAPE)
}

/// Encode a `(catalog, tool)` pair into a single MCP-safe tool name that only
/// uses characters from `[a-zA-Z0-9_-]` (assuming the inputs do).
#[must_use]
pub fn encode_tool_name(catalog_name: &str, tool_name: &str) -> String {
    format!(
        "{}{SEPARATOR}{}",
        encode_component(catalog_name),
        encode_component(tool_name)
    )
}

/// Decode a tool name produced by [`encode_tool_name`] back into its
/// `(catalog, tool)` components.
///
/// Returns `None` when `name` contains no catalog separator — i.e. it refers to
/// a top-level tool that was never catalog-qualified. Because an encoded
/// component never contains a bare `__`, the first `__` in a well-formed name
/// is always the catalog separator.
#[must_use]
pub fn decode_tool_name(name: &str) -> Option<(String, String)> {
    let (catalog, tool) = name.split_once(SEPARATOR)?;
    Some((decode_component(catalog), decode_component(tool)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_the_documented_examples() {
        // (catalog, tool) -> encoded, from the scheme in issue #10894.
        assert_eq!(
            encode_tool_name("antv_charts", "some_tool"),
            "antv_charts__some_tool"
        );
        assert_eq!(
            encode_tool_name("my-catalog", "list_files"),
            "my-catalog__list_files"
        );
        assert_eq!(
            encode_tool_name("my_catalog", "some-tool"),
            "my_catalog__some-tool"
        );
        assert_eq!(encode_tool_name("a--b", "c"), "a--b__c");
        assert_eq!(encode_tool_name("my-_catalog", "foo"), "my-_catalog__foo");
        assert_eq!(encode_tool_name("my__catalog", "foo"), "my_-_catalog__foo");
        assert_eq!(
            encode_tool_name("my_-_catalog", "foo"),
            "my_--_catalog__foo"
        );
    }

    #[test]
    fn encoded_names_only_use_allowed_characters() {
        // Every character in an encoded name must satisfy the MCP client
        // constraint `^[a-zA-Z0-9_-]{1,64}$` when the inputs do.
        for (catalog, tool) in [
            ("antv_charts", "list_chart_types"),
            ("my__catalog", "a_-_b"),
            ("my_-_catalog", "tool__name"),
            ("a-b_c", "d_e-f"),
        ] {
            let encoded = encode_tool_name(catalog, tool);
            assert!(
                encoded
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
                "encoded name `{encoded}` contains a disallowed character"
            );
        }
    }

    #[test]
    fn round_trips_pairs() {
        // Round-trip over the full supported domain: identifier-shaped names
        // (letters/digits/`-` with single `_`), plus the escape paths that a
        // literal `__` and a literal `_-_` exercise. The scheme escapes one
        // level (`__` -> `_-_`, `_-_` -> `_--_`), which covers every realistic
        // catalog/tool name; it does not target pathological inputs such as
        // runs of 3+ underscores or a literal `_--_`, which never occur in MCP
        // tool identifiers.
        let names = [
            "antv_charts",
            "some_tool",
            "list_chart_types",
            "my-catalog",
            "some-tool",
            "a--b",
            "my-_catalog",
            "my__catalog",
            "my_-_catalog",
            "a_-_b",
            "tool__name__x",
            "get-files",
            "x",
        ];
        for catalog in names {
            for tool in names {
                let encoded = encode_tool_name(catalog, tool);
                let (decoded_catalog, decoded_tool) = decode_tool_name(&encoded)
                    .expect("an encoded name always contains the separator");
                assert_eq!(
                    (decoded_catalog.as_str(), decoded_tool.as_str()),
                    (catalog, tool),
                    "round-trip failed for ({catalog:?}, {tool:?}) via {encoded:?}"
                );
            }
        }
    }

    #[test]
    fn decode_returns_none_without_separator() {
        // A `snake_case` top-level tool name has no `__` separator.
        assert_eq!(decode_tool_name("list_files"), None);
        assert_eq!(decode_tool_name("document_similarity"), None);
        assert_eq!(decode_tool_name(""), None);
    }
}
