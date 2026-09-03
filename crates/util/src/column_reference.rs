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

//! Parsing of the column reference strings used by `acceleration.primary_key`,
//! `acceleration.indexes` and `acceleration.on_conflict`: a single column, or a
//! compound `(column_a, column_b)` list.
//!
//! A name may be double-quoted, the way it would be written in SQL, which is how a
//! column whose name contains dots (`"service.instance.id"`) is referenced. The
//! parsed names are always the unquoted names as they appear in the schema.

use snafu::prelude::*;

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display(
        "The column reference '{column_ref}' is empty. Name the column to use, or a compound key as '(column_a, column_b)', and try again."
    ))]
    EmptyColumnReference { column_ref: String },

    #[snafu(display(
        "The compound column reference '{column_ref}' is missing its closing ')'. Write a compound key as '(column_a, column_b)' and try again."
    ))]
    MissingClosingParenthesis { column_ref: String },

    #[snafu(display(
        "The column reference '{column_ref}' has unexpected text after its closing ')'. Write a compound key as '(column_a, column_b)' and try again."
    ))]
    TrailingText { column_ref: String },

    #[snafu(display(
        "The column reference '{column_ref}' contains an empty column name. Remove the extra ',' and try again."
    ))]
    EmptyColumnName { column_ref: String },

    #[snafu(display(
        "The quoted column name in the column reference '{column_ref}' is missing its closing '\"'. Quote a column name as '\"column.name\"' and try again."
    ))]
    UnterminatedQuotedName { column_ref: String },

    #[snafu(display(
        "The column reference '{column_ref}' has a '\"' in the middle of a column name. Quote a whole column name as '\"column.name\"', doubling any '\"' it contains, and try again."
    ))]
    MisplacedQuote { column_ref: String },

    #[snafu(display(
        "The column name '{column}' in the column reference '{column_ref}' contains an unsupported character '{character}'. Reference a column whose name does not contain ',', ';', ':', '(', ')' or '\"' and try again."
    ))]
    UnsupportedCharacterInName {
        column_ref: String,
        column: String,
        character: char,
    },
}

/// Characters that cannot appear in a referenced column name: each one is a separator in
/// the strings a column reference is carried in (`(a, b)`, `index:unique;other:enabled`,
/// `upsert:(a, b)`), so a name containing one could not be read back unambiguously.
const UNSUPPORTED_CHARACTERS: [char; 6] = [',', ';', ':', '(', ')', '"'];

/// Parses a column reference into the column names it references, in the order written.
///
/// ```rust
/// # use util::column_reference::parse;
/// assert_eq!(parse("id").expect("valid"), vec!["id"]);
/// assert_eq!(
///     parse(r#"(time_unix_nano, "service.instance.id")"#).expect("valid"),
///     vec!["time_unix_nano", "service.instance.id"]
/// );
/// ```
///
/// # Errors
///
/// Returns an [`Error`] if the reference is malformed (an unclosed `(` or `"`, an empty
/// column name) or names a column that cannot be referenced (see
/// [`UNSUPPORTED_CHARACTERS`]).
pub fn parse(column_ref: &str) -> Result<Vec<String>, Error> {
    let trimmed = column_ref.trim();
    ensure!(
        !trimmed.is_empty(),
        EmptyColumnReferenceSnafu { column_ref }
    );

    let inner = match trimmed.strip_prefix('(') {
        Some(rest) => {
            let end =
                closing_parenthesis(rest).context(MissingClosingParenthesisSnafu { column_ref })?;
            ensure!(
                rest[end + 1..].trim().is_empty(),
                TrailingTextSnafu { column_ref }
            );
            &rest[..end]
        }
        None => trimmed,
    };

    split_names(inner, column_ref)
}

/// The byte offset of the `)` closing the reference opened by the `(` that precedes
/// `rest`, ignoring any `)` inside a quoted name.
fn closing_parenthesis(rest: &str) -> Option<usize> {
    let mut in_quotes = false;
    for (offset, c) in rest.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ')' if !in_quotes => return Some(offset),
            _ => {}
        }
    }
    None
}

/// Where the scan is within the name it is reading.
enum State {
    /// Before the first character of a name.
    Start,
    /// Reading an unquoted name.
    Bare,
    /// Inside `"`.
    Quoted,
    /// After the `"` that closed a name.
    AfterQuote,
}

fn split_names(inner: &str, column_ref: &str) -> Result<Vec<String>, Error> {
    let mut names: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut state = State::Start;
    let mut chars = inner.chars().peekable();

    while let Some(c) = chars.next() {
        match state {
            State::Start => match c {
                ',' => return EmptyColumnNameSnafu { column_ref }.fail(),
                '"' => state = State::Quoted,
                c if c.is_whitespace() => {}
                c => {
                    current.push(c);
                    state = State::Bare;
                }
            },
            State::Bare => match c {
                ',' => {
                    push_name(&mut names, current.trim().to_string(), column_ref)?;
                    current.clear();
                    state = State::Start;
                }
                // A quoted name has to be the whole name: `a"b"` is ambiguous.
                '"' => return MisplacedQuoteSnafu { column_ref }.fail(),
                c => current.push(c),
            },
            State::Quoted => match c {
                // `""` is an escaped quote within the name.
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    current.push('"');
                }
                '"' => state = State::AfterQuote,
                c => current.push(c),
            },
            State::AfterQuote => match c {
                ',' => {
                    push_name(&mut names, std::mem::take(&mut current), column_ref)?;
                    state = State::Start;
                }
                c if c.is_whitespace() => {}
                _ => return MisplacedQuoteSnafu { column_ref }.fail(),
            },
        }
    }

    match state {
        State::Start if names.is_empty() => {
            return EmptyColumnReferenceSnafu { column_ref }.fail();
        }
        State::Start => return EmptyColumnNameSnafu { column_ref }.fail(),
        State::Bare => push_name(&mut names, current.trim().to_string(), column_ref)?,
        State::Quoted => return UnterminatedQuotedNameSnafu { column_ref }.fail(),
        State::AfterQuote => push_name(&mut names, current, column_ref)?,
    }

    Ok(names)
}

fn push_name(names: &mut Vec<String>, name: String, column_ref: &str) -> Result<(), Error> {
    ensure!(!name.is_empty(), EmptyColumnNameSnafu { column_ref });
    if let Some(character) = name.chars().find(|c| UNSUPPORTED_CHARACTERS.contains(c)) {
        return UnsupportedCharacterInNameSnafu {
            column_ref,
            column: name,
            character,
        }
        .fail();
    }
    names.push(name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(column_ref: &str) -> Vec<String> {
        parse(column_ref).expect("valid column reference")
    }

    #[test]
    fn parses_single_and_compound_references() {
        assert_eq!(names("id"), vec!["id"]);
        assert_eq!(names(" id "), vec!["id"]);
        assert_eq!(names("(foo, bar)"), vec!["foo", "bar"]);
        assert_eq!(names("(foo,bar)"), vec!["foo", "bar"]);
        assert_eq!(names(" ( foo , bar ) "), vec!["foo", "bar"]);
        // The parentheses are optional: a comma-separated list is a compound key too.
        assert_eq!(names("foo, bar"), vec!["foo", "bar"]);
    }

    #[test]
    fn parses_quoted_names() {
        assert_eq!(
            names(r#""service.instance.id""#),
            vec!["service.instance.id"]
        );
        assert_eq!(
            names(r#"(time_unix_nano, "service.instance.id")"#),
            vec!["time_unix_nano", "service.instance.id"]
        );
        assert_eq!(
            names(r#"("service.instance.id" , time_unix_nano)"#),
            vec!["service.instance.id", "time_unix_nano"]
        );
        // Quoting preserves whitespace and case inside the name.
        assert_eq!(names(r#"" Mixed Case ""#), vec![" Mixed Case "]);
    }

    #[test]
    fn rejects_malformed_references() {
        assert_eq!(
            parse("").expect_err("empty").to_string(),
            "The column reference '' is empty. Name the column to use, or a compound key as '(column_a, column_b)', and try again."
        );
        assert_eq!(
            parse("()").expect_err("no columns"),
            Error::EmptyColumnReference {
                column_ref: "()".to_string()
            }
        );
        assert_eq!(
            parse("(foo,bar").expect_err("unclosed paren"),
            Error::MissingClosingParenthesis {
                column_ref: "(foo,bar".to_string()
            }
        );
        assert_eq!(
            parse("(foo, bar) baz").expect_err("trailing text"),
            Error::TrailingText {
                column_ref: "(foo, bar) baz".to_string()
            }
        );
        assert_eq!(
            parse("(foo,,bar)").expect_err("empty name"),
            Error::EmptyColumnName {
                column_ref: "(foo,,bar)".to_string()
            }
        );
        assert_eq!(
            parse("(foo,)").expect_err("trailing comma"),
            Error::EmptyColumnName {
                column_ref: "(foo,)".to_string()
            }
        );
        assert_eq!(
            parse("(,foo)").expect_err("leading comma"),
            Error::EmptyColumnName {
                column_ref: "(,foo)".to_string()
            }
        );
        assert_eq!(
            parse(r#""service.instance.id"#).expect_err("unterminated quote"),
            Error::UnterminatedQuotedName {
                column_ref: r#""service.instance.id"#.to_string()
            }
        );
        assert_eq!(
            parse(r#"(a"b")"#).expect_err("misplaced quote"),
            Error::MisplacedQuote {
                column_ref: r#"(a"b")"#.to_string()
            }
        );
        assert_eq!(
            parse(r#"("a"b)"#).expect_err("misplaced quote"),
            Error::MisplacedQuote {
                column_ref: r#"("a"b)"#.to_string()
            }
        );
    }

    #[test]
    fn rejects_names_that_cannot_round_trip() {
        // A name holding one of the separators the reference is carried in cannot be
        // read back, so it is refused rather than silently split.
        assert_eq!(
            parse(r#"("a,b")"#).expect_err("comma in name"),
            Error::UnsupportedCharacterInName {
                column_ref: r#"("a,b")"#.to_string(),
                column: "a,b".to_string(),
                character: ',',
            }
        );
        assert_eq!(
            parse(r#""a""b""#).expect_err("quote in name"),
            Error::UnsupportedCharacterInName {
                column_ref: r#""a""b""#.to_string(),
                column: r#"a"b"#.to_string(),
                character: '"',
            }
        );
        for character in [';', ':', '(', ')'] {
            let column_ref = format!("a{character}b");
            assert!(
                parse(&column_ref).is_err(),
                "'{column_ref}' should be refused"
            );
        }
    }

    #[test]
    fn quoted_names_may_contain_parentheses_and_dots() {
        // `)` inside quotes does not close the compound reference.
        assert_eq!(names(r#"("a.b", c)"#), vec!["a.b", "c"]);
        assert_eq!(
            parse(r#"("a)b", c)"#).expect_err("parenthesis in name"),
            Error::UnsupportedCharacterInName {
                column_ref: r#"("a)b", c)"#.to_string(),
                column: "a)b".to_string(),
                character: ')',
            }
        );
    }
}
