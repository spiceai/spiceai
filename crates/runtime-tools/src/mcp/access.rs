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

//! Which of an MCP server's tools only read, as its Spicepod `tools:` entry declares.

use spicepod::component::access::AccessMode;
use tools::ToolAccess;

/// The access of an MCP server's tools: `access` for every tool, except the tools
/// `read_only_tools` names, which only read. Spice cannot verify what an MCP server's tool does,
/// so this is the operator's declaration, and a tool it does not cover can write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpToolAccess {
    default: ToolAccess,
    read_only_tools: Vec<String>,
}

impl Default for McpToolAccess {
    fn default() -> Self {
        Self {
            default: ToolAccess::ReadWrite,
            read_only_tools: Vec::new(),
        }
    }
}

impl McpToolAccess {
    /// The access a `tools:` entry declares with `access` and `read_only_tools`. `access` is
    /// `read` or `read_write` here; anything else counts as `read_write`.
    #[must_use]
    pub fn from_spicepod(access: Option<&AccessMode>, read_only_tools: &[String]) -> Self {
        Self {
            default: match access {
                Some(AccessMode::Read) => ToolAccess::ReadOnly,
                Some(AccessMode::ReadWrite | AccessMode::ReadWriteCreate) | None => {
                    ToolAccess::ReadWrite
                }
            },
            read_only_tools: read_only_tools.to_vec(),
        }
    }

    /// The access of the server's tool named `tool` (the name the server reports).
    #[must_use]
    pub fn of(&self, tool: &str) -> ToolAccess {
        if self
            .read_only_tools
            .iter()
            .any(|pattern| matches_glob(pattern, tool))
        {
            ToolAccess::ReadOnly
        } else {
            self.default
        }
    }

    /// The `read_only_tools` entries that match none of `tools`, the names the server reports.
    pub fn unmatched<'a>(&'a self, tools: &'a [String]) -> impl Iterator<Item = &'a str> + 'a {
        self.read_only_tools
            .iter()
            .filter(|pattern| !tools.iter().any(|tool| matches_glob(pattern, tool)))
            .map(String::as_str)
    }
}

/// Whether `name` matches `pattern`, in which `*` matches any run of characters (none
/// included) and every other character matches itself.
fn matches_glob(pattern: &str, name: &str) -> bool {
    let mut parts = pattern.split('*');
    let Some(first) = parts.next() else {
        return pattern == name;
    };
    let Some(mut rest) = name.strip_prefix(first) else {
        return false;
    };
    let middle: Vec<&str> = parts.collect();
    let Some((last, middle)) = middle.split_last() else {
        // No `*`: the whole name is the pattern.
        return rest.is_empty();
    };
    for part in middle {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs() {
        for (pattern, name) in [
            ("get_ticket", "get_ticket"),
            ("search_*", "search_tickets"),
            ("search_*", "search_"),
            ("*", "anything"),
            ("*_ticket", "get_ticket"),
            ("get_*_by_*", "get_ticket_by_id"),
            ("a*b*c", "abc"),
            ("a**c", "ac"),
        ] {
            assert!(matches_glob(pattern, name), "{pattern} matches {name}");
        }
        for (pattern, name) in [
            ("get_ticket", "get_tickets"),
            ("get_ticket", "Get_ticket"),
            ("search_*", "research_tickets"),
            ("*_ticket", "get_tickets"),
            ("get_*_by_*", "get_ticket"),
            ("a*b*c", "acb"),
            ("ab*ba", "aba"),
        ] {
            assert!(
                !matches_glob(pattern, name),
                "{pattern} does not match {name}"
            );
        }
    }

    /// A tool `read_only_tools` matches only reads; every other tool takes `access`, and an
    /// MCP server's tools can write unless the Spicepod says otherwise.
    #[test]
    fn a_tool_takes_its_server_access_unless_listed() {
        assert_eq!(
            McpToolAccess::default().of("create_ticket"),
            ToolAccess::ReadWrite
        );
        let tickets = McpToolAccess::from_spicepod(
            Some(&AccessMode::ReadWrite),
            &["search_*".to_string(), "get_ticket".to_string()],
        );
        assert_eq!(tickets.of("search_tickets"), ToolAccess::ReadOnly);
        assert_eq!(tickets.of("get_ticket"), ToolAccess::ReadOnly);
        assert_eq!(tickets.of("create_ticket"), ToolAccess::ReadWrite);
        assert_eq!(
            McpToolAccess::from_spicepod(Some(&AccessMode::Read), &[]).of("create_ticket"),
            ToolAccess::ReadOnly
        );
        assert_eq!(
            tickets
                .unmatched(&["search_tickets".to_string(), "create_ticket".to_string()])
                .collect::<Vec<_>>(),
            ["get_ticket"]
        );
    }
}
