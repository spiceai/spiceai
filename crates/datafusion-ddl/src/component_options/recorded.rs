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

//! A statement's text as task history and the list of running queries record
//! it. A statement that may define a component can carry credentials, so its
//! text is read token by token, with the dialect it is parsed with, and what is
//! kept is decided positively: an option of a well-formed `WITH (...)` or
//! `SET (...)` list keeps its value when the value is a field's, a parameter's
//! that is known to be non-secret, or a secret reference. Every other literal,
//! every comment, and every part of the statement that is not a well-formed
//! option is taken out, so a mistyped statement records no more than a correct
//! one.

use std::borrow::Cow;
use std::ops::Range;

use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::{Dialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::keywords::{ALL_KEYWORDS, Keyword};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Whitespace};
use serde_json::Value;

use super::{
    ComponentKind, REDACTED, REDACTED_SQL, byte_offset, is_params_map, is_secret_reference,
    line_starts, quote_string, string_literal,
};

/// What is recorded in place of a statement that may define a component and
/// cannot be parsed, not even into tokens: which of its parts hold values
/// cannot be told.
pub const UNREADABLE_STATEMENT: &str = "<redacted: the statement could not be parsed>";

/// A value shorter than this is taken out of an error message only as it is
/// written in the statement, quotes included: bare, it would match unrelated
/// text such as a line number.
const SHORTEST_BARE_VALUE: usize = util::redact::SHORTEST_PART;

/// A statement's text as it may be recorded, and the values taken out of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedStatement<'a> {
    /// The text to record: borrowed when it is the statement's text
    /// unchanged, and owned whenever anything was taken out.
    pub text: Cow<'a, str>,
    /// Each value taken out of the text, as written and unquoted, which
    /// [`redact_values`] takes out of an error the statement fails with.
    pub removed: Vec<String>,
}

impl<'a> RecordedStatement<'a> {
    fn unchanged(sql: &'a str) -> Self {
        Self {
            text: Cow::Borrowed(sql),
            removed: Vec::new(),
        }
    }

    /// The placeholder for `sql`, which cannot be read into tokens, and the
    /// parts of it that may be values ([`unreadable_parts`]): a source that
    /// quotes the statement back repeats them.
    fn unreadable(sql: &str) -> Self {
        Self {
            // Owned, as any changed text is: a caller tells a changed text by it.
            text: Cow::Owned(UNREADABLE_STATEMENT.to_string()),
            removed: unreadable_parts(sql),
        }
    }
}

/// The parts of `text`, which cannot be read into tokens, that may be values:
/// each word of at least [`SHORTEST_BARE_VALUE`] characters that is quoted or
/// is not a keyword, as written and without the quotes around it. A quoted
/// keyword, such as `'secret`, is a literal. A source that quotes the text
/// back loses them, which may take out a name too; that fails closed.
#[must_use]
pub fn unreadable_parts(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    for word in text.split(|c: char| c.is_whitespace() || matches!(c, ',' | '(' | ')' | '=' | ';'))
    {
        let bare = word.trim_matches(|c: char| matches!(c, '\'' | '"' | '`'));
        let quoted = bare.len() != word.len();
        if bare.chars().count() < SHORTEST_BARE_VALUE
            || (!quoted
                && ALL_KEYWORDS
                    .binary_search(&bare.to_ascii_uppercase().as_str())
                    .is_ok())
        {
            continue;
        }
        parts.push(word.to_string());
        if bare != word {
            parts.push(bare.to_string());
        }
    }
    parts
}

/// `message`, an error a statement failed with or a result that repeats it,
/// without the values the statement's recorded text took out: a parser error
/// quotes the token it stopped at, and a connector can repeat a value it could
/// not use.
pub use util::redact::redact_values;

/// The text of `sql` as it may be recorded, read with `dialect`, the dialect
/// the statement is parsed with. Text that cannot define a component is
/// unchanged. In a statement that can, the value of each option is kept only
/// when it is a field's, a parameter's that `shown` shows (it is given the
/// parameter's name), or a `${ store:key }` reference; every other literal and
/// comment, and every part that is not a well-formed option, is taken out. Text
/// that does not tokenize is recorded as [`UNREADABLE_STATEMENT`].
#[must_use]
pub fn recorded_statement<'a>(
    sql: &'a str,
    dialect: &dyn Dialect,
    shown: &dyn Fn(&str) -> bool,
) -> RecordedStatement<'a> {
    // A dialect spells each of the keywords in ASCII in any case: a check
    // cheap enough for every query, so only the text that passes it is
    // tokenized.
    if !DEFINING_KEYWORDS
        .iter()
        .any(|(_, spelling)| mentions(sql, spelling))
    {
        return RecordedStatement::unchanged(sql);
    }
    record(sql, dialect, shown, false)
}

/// The keywords of a statement that may define a component, with their
/// spelling: `CREATE`, `ALTER`, and `ATTACH`, which Spice does not run but other
/// engines connect a database with, so it may carry a credential.
const DEFINING_KEYWORDS: [(Keyword, &[u8]); 3] = [
    (Keyword::CREATE, b"create"),
    (Keyword::ALTER, b"alter"),
    (Keyword::ATTACH, b"attach"),
];

/// Whether `text` may hold a statement that defines a component: one of the
/// [`DEFINING_KEYWORDS`] where it may be a token of its own, or a `\u`
/// escape, which JSON may spell one with.
#[must_use]
pub fn may_define(text: &str) -> bool {
    text.contains("\\u")
        || DEFINING_KEYWORDS
            .iter()
            .any(|(_, spelling)| mentions(text, spelling))
}

/// `text` as the `sql` tool records a statement it runs, with every
/// parameter's value taken out: read as a statement that may define a
/// component when it parses, and as one that failed to parse otherwise.
#[must_use]
pub fn recorded_text(text: &str) -> RecordedStatement<'_> {
    let dialect = PostgreSqlDialect {};
    if DFParser::parse_sql_with_dialect(text, &dialect).is_ok() {
        recorded_statement(text, &dialect, &|_| false)
    } else {
        recorded_unparsed_statement(text, &dialect, &|_| false)
    }
}

/// `arguments`, a call's arguments to a tool of any name, with each statement
/// in them that may define a component recorded as the `sql` tool records one
/// ([`recorded_text`]), and the values that takes out. Each string of JSON
/// arguments is read on its own, and a string that holds JSON, as the
/// arguments `tool_invoke` passes on, as JSON; arguments that are not JSON are
/// read as one statement. `None` when nothing changes.
#[must_use]
pub fn recorded_call_arguments(arguments: &str) -> Option<(String, Vec<String>)> {
    if !may_define(arguments) {
        return None;
    }
    let mut removed = Vec::new();
    let Ok(mut value) = serde_json::from_str::<Value>(arguments) else {
        let recorded = recorded_text(arguments);
        return match recorded.text {
            Cow::Owned(text) => Some((text, recorded.removed)),
            Cow::Borrowed(_) => None,
        };
    };
    record_json_statements(&mut value, &mut removed).then(|| (value.to_string(), removed))
}

/// Record each statement in the strings of `value`, as
/// [`recorded_call_arguments`] reads them. Returns whether anything changed.
fn record_json_statements(value: &mut Value, removed: &mut Vec<String>) -> bool {
    match value {
        Value::String(text) => {
            if !may_define(text) {
                return false;
            }
            if let Ok(mut inner) = serde_json::from_str::<Value>(text)
                && (inner.is_object() || inner.is_array())
            {
                let changed = record_json_statements(&mut inner, removed);
                if changed {
                    *text = inner.to_string();
                }
                return changed;
            }
            let recorded = recorded_text(text);
            match recorded.text {
                Cow::Owned(recorded_text) => {
                    removed.extend(recorded.removed);
                    *text = recorded_text;
                    true
                }
                Cow::Borrowed(_) => false,
            }
        }
        Value::Array(items) => items.iter_mut().fold(false, |changed, item| {
            record_json_statements(item, removed) | changed
        }),
        Value::Object(map) => map.values_mut().fold(false, |changed, item| {
            record_json_statements(item, removed) | changed
        }),
        _ => false,
    }
}

/// Whether `token` is one of the [`DEFINING_KEYWORDS`], unquoted.
fn is_defining_keyword(token: &Token) -> bool {
    matches!(token, Token::Word(word)
        if word.quote_style.is_none()
            && DEFINING_KEYWORDS.iter().any(|(keyword, _)| *keyword == word.keyword))
}

/// The text of `sql`, a statement that failed to parse, as it may be recorded:
/// as [`recorded_statement`] records one that may define a component,
/// whatever its first word. A mistyped statement, `CRATE TABLE` for `CREATE
/// TABLE`, may carry a credential the parser never read.
#[must_use]
pub fn recorded_unparsed_statement<'a>(
    sql: &'a str,
    dialect: &dyn Dialect,
    shown: &dyn Fn(&str) -> bool,
) -> RecordedStatement<'a> {
    record(sql, dialect, shown, true)
}

/// Record `sql` fail-closed: every statement in it when `every_statement`,
/// and otherwise each that may define a component.
fn record<'a>(
    sql: &'a str,
    dialect: &dyn Dialect,
    shown: &dyn Fn(&str) -> bool,
    every_statement: bool,
) -> RecordedStatement<'a> {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize_with_location() else {
        return RecordedStatement::unreadable(sql);
    };
    // Text that names a keyword only inside a word, as a `created_at` column
    // does, defines nothing: it is returned before its tokens are placed.
    if !every_statement && !tokens.iter().any(|token| is_defining_keyword(&token.token)) {
        return RecordedStatement::unchanged(sql);
    }
    let starts = line_starts(sql);
    let mut toks: Vec<Tok<'_>> = Vec::with_capacity(tokens.len());
    for token in &tokens {
        if matches!(token.token, Token::EOF) {
            continue;
        }
        let (Some(start), Some(end)) = (
            byte_offset(sql, &starts, token.span.start),
            byte_offset(sql, &starts, token.span.end),
        ) else {
            return RecordedStatement::unreadable(sql);
        };
        // Each token follows the one before: text that does not read so is
        // not spliced.
        if start > end || toks.last().is_some_and(|last| last.range.end > start) {
            return RecordedStatement::unreadable(sql);
        }
        toks.push(Tok {
            range: start..end,
            token: &token.token,
        });
    }

    let mut recording = Recording {
        sql,
        toks: &toks,
        shown,
        handled: vec![false; toks.len()],
        replacements: Vec::new(),
        removed: Vec::new(),
    };
    for statement in statements(&toks) {
        if every_statement || defines(&toks[statement.clone()]) {
            recording.statement(statement);
        }
    }
    recording.finish()
}

/// Whether `sql` holds `word`, in any ASCII case, where it may be a token of
/// its own. Every dialect reads an ASCII letter or `_` before it, or an ASCII
/// letter, digit, or `_` after it, as part of one longer word (`created_at`),
/// which is not the keyword; next to anything else, it may be.
fn mentions(sql: &str, word: &[u8]) -> bool {
    let bytes = sql.as_bytes();
    bytes.windows(word.len()).enumerate().any(|(at, window)| {
        window.eq_ignore_ascii_case(word)
            && !at
                .checked_sub(1)
                .and_then(|before| bytes.get(before))
                .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
            && !bytes
                .get(at + word.len())
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
    })
}

/// A token, and the bytes of the statement it was read from.
struct Tok<'t> {
    range: Range<usize>,
    token: &'t Token,
}

/// The token ranges of the statements in `toks`, which `;` separates.
fn statements(toks: &[Tok<'_>]) -> Vec<Range<usize>> {
    let mut statements = Vec::new();
    let mut depth = 0_usize;
    let mut start = 0;
    for (i, tok) in toks.iter().enumerate() {
        match tok.token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
            Token::SemiColon if depth == 0 => {
                statements.push(start..i);
                start = i + 1;
            }
            _ => {}
        }
    }
    statements.push(start..toks.len());
    statements
}

/// Whether the statement `toks` may define a component: it has one of the
/// [`DEFINING_KEYWORDS`], wherever it is, as after `EXPLAIN`.
fn defines(toks: &[Tok<'_>]) -> bool {
    toks.iter().any(|tok| is_defining_keyword(tok.token))
}

/// Whether a token that is neither a word nor whitespace can carry a value, as
/// a literal of any kind does, rather than being punctuation or an operator.
fn carries_value(text: &str) -> bool {
    text.chars()
        .any(|c| c.is_alphanumeric() || matches!(c, '\'' | '"' | '`' | '$'))
}

fn is_comment(token: &Token) -> bool {
    matches!(
        token,
        Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_))
    )
}

fn is_keyword(token: &Token, keyword: &str) -> bool {
    matches!(token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(keyword))
}

/// An option's value, in the shapes the option reader takes, as the indices of
/// its tokens.
enum Shape<'v> {
    /// One literal or word, or a signed number.
    Scalar {
        tokens: &'v [usize],
        string: Option<String>,
    },
    /// `JSON '...'`.
    Json { tokens: &'v [usize], text: String },
    /// `ARRAY[...]`.
    Array {
        tokens: &'v [usize],
        elements: Vec<Shape<'v>>,
    },
}

struct Recording<'s, 't> {
    sql: &'s str,
    toks: &'t [Tok<'t>],
    shown: &'t dyn Fn(&str) -> bool,
    /// Whether each token's part of the text is decided.
    handled: Vec<bool>,
    replacements: Vec<(Range<usize>, String)>,
    removed: Vec<String>,
}

impl<'s, 't> Recording<'s, 't> {
    fn text(&self, i: usize) -> &'s str {
        let sql: &'s str = self.sql;
        &sql[self.toks[i].range.clone()]
    }

    fn token(&self, i: usize) -> &'t Token {
        let toks: &'t [Tok<'t>] = self.toks;
        toks[i].token
    }

    /// Record one statement that may define a component.
    fn statement(&mut self, statement: Range<usize>) {
        let significant: Vec<usize> = statement
            .clone()
            .filter(|&i| !matches!(self.token(i), Token::Whitespace(_)))
            .collect();

        // The option lists, at the statement's top level.
        let mut depth = 0_usize;
        let mut k = 0;
        while k < significant.len() {
            match self.token(significant[k]) {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
                Token::Word(word)
                    if depth == 0
                        && word.quote_style.is_none()
                        && matches!(
                            word.keyword,
                            Keyword::WITH | Keyword::SET | Keyword::RESET
                        ) =>
                {
                    if significant
                        .get(k + 1)
                        .is_some_and(|&i| matches!(self.token(i), Token::LParen))
                    {
                        let close = self.closing(&significant, k + 1);
                        self.options(&significant[k + 2..close]);
                        // On after the list's `)`, still at the top level.
                        k = close + 1;
                        continue;
                    }
                    // Not an option list: what follows cannot be read as
                    // options, so none of it is kept.
                    if k + 1 < significant.len() {
                        self.redact(&significant[k + 1..]);
                    }
                    break;
                }
                _ => {}
            }
            k += 1;
        }

        // Everything else, token by token: a comment and a literal are taken
        // out, and so is a word that is a value, after `=`.
        let mut after_eq = false;
        for i in statement {
            let token = self.token(i);
            if let Token::Whitespace(_) = token {
                if !self.handled[i] && is_comment(token) {
                    self.handled[i] = true;
                    let text = self.text(i);
                    let line_end = if text.ends_with('\n') {
                        "\n"
                    } else if text.ends_with('\r') {
                        "\r"
                    } else {
                        " "
                    };
                    self.replacements
                        .push((self.toks[i].range.clone(), line_end.to_string()));
                }
                continue;
            }
            if !self.handled[i] {
                let value = match token {
                    Token::Word(_) => after_eq,
                    _ => carries_value(self.text(i)),
                };
                if value {
                    self.redact(&[i]);
                }
            }
            after_eq = matches!(token, Token::Eq);
        }
    }

    /// The index in `significant` of the `)` that closes the `(` at `open`, or
    /// the end when nothing does.
    fn closing(&self, significant: &[usize], open: usize) -> usize {
        let mut depth = 0_usize;
        for (k, &i) in significant.iter().enumerate().skip(open) {
            match self.token(i) {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return k;
                    }
                }
                _ => {}
            }
        }
        significant.len()
    }

    /// The items of an option list, which `,` separates.
    fn options(&mut self, items: &[usize]) {
        let mut depth = 0_usize;
        let mut start = 0;
        for (n, &i) in items.iter().enumerate() {
            match self.token(i) {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth = depth.saturating_sub(1),
                Token::Comma if depth == 0 => {
                    self.option(&items[start..n]);
                    start = n + 1;
                }
                _ => {}
            }
        }
        self.option(&items[start..]);
    }

    /// One `key = value`, or one key alone, as `RESET` lists them.
    fn option(&mut self, item: &[usize]) {
        if item.is_empty() {
            return;
        }
        // The key: a word or a quoted name, or several joined by `.`.
        let mut parts = Vec::new();
        let mut n = 0;
        while let Some(&i) = item.get(n) {
            match self.token(i) {
                Token::Word(word) => parts.push(word.value.clone()),
                Token::SingleQuotedString(s) | Token::DoubleQuotedString(s) => {
                    parts.push(s.clone());
                }
                _ => break,
            }
            n += 1;
            let dotted = item
                .get(n)
                .is_some_and(|&i| matches!(self.token(i), Token::Period))
                && item
                    .get(n + 1)
                    .is_some_and(|&i| matches!(self.token(i), Token::Word(_)));
            if !dotted {
                break;
            }
            n += 1;
        }
        if parts.is_empty() {
            self.redact(item);
            return;
        }
        self.keep(&item[..n]);
        if n == item.len() {
            return;
        }
        // Anything but `= value` after the key is not an option the statement
        // can be read for.
        if !matches!(self.token(item[n]), Token::Eq) || n + 1 == item.len() {
            self.redact(&item[n..]);
            return;
        }
        self.keep(&item[n..=n]);
        self.value(&parts, &item[n + 1..]);
    }

    /// The value of the option `parts` names.
    fn value(&mut self, parts: &[String], value: &[usize]) {
        let mut path: Vec<&str> = parts.iter().flat_map(|part| part.split('.')).collect();
        // `dataset.` is an alias for the dataset's own fields.
        if path.len() > 1 && path[0] == "dataset" {
            path.remove(0);
        }
        // As the options are read: a key that does not start with a field is a
        // connector parameter named by the whole key, and a key into a
        // parameters map names the entry that follows the map.
        let first_is_field = path.first().is_some_and(|first| {
            ComponentKind::Dataset.is_field(first) || ComponentKind::Catalog.is_field(first)
        });
        let map = path.iter().position(|segment| is_params_map(segment));
        let parameter = match (first_is_field, map) {
            (false, _) => Some(path.join(".")),
            (true, Some(map)) if map + 1 < path.len() => Some(path[map + 1..].join(".")),
            _ => None,
        };
        let Some(shape) = self.shape(value) else {
            self.redact(value);
            return;
        };
        if let Some(name) = parameter {
            let reference =
                matches!(&shape, Shape::Scalar { string: Some(s), .. } if is_secret_reference(s));
            if reference || (self.shown)(&name) {
                self.keep(value);
            } else {
                self.redact(value);
            }
            return;
        }
        let whole_map = path.last().is_some_and(|key| is_params_map(key));
        self.field(&shape, whole_map);
    }

    /// `value` read as one of the shapes the option reader takes, or `None`.
    fn shape<'v>(&self, value: &'v [usize]) -> Option<Shape<'v>> {
        let token = |n: usize| self.token(value[n]);
        match value.len() {
            0 => None,
            1 => (matches!(token(0), Token::Word(_)) || carries_value(self.text(value[0]))).then(
                || Shape::Scalar {
                    tokens: value,
                    string: string_literal(token(0)).map(str::to_string),
                },
            ),
            2 if matches!(token(0), Token::Minus | Token::Plus)
                && matches!(token(1), Token::Number(..)) =>
            {
                Some(Shape::Scalar {
                    tokens: value,
                    string: None,
                })
            }
            2 if is_keyword(token(0), "json") => string_literal(token(1)).map(|text| Shape::Json {
                tokens: value,
                text: text.to_string(),
            }),
            len if len >= 3
                && is_keyword(token(0), "array")
                && matches!(token(1), Token::LBracket)
                && matches!(token(len - 1), Token::RBracket) =>
            {
                let inner = &value[2..len - 1];
                let mut elements = Vec::new();
                let mut depth = 0_usize;
                let mut start = 0;
                for (n, &i) in inner.iter().enumerate() {
                    match self.token(i) {
                        Token::LParen | Token::LBracket => depth += 1,
                        Token::RParen | Token::RBracket => {
                            // A `]` that closes the `ARRAY[` before its end
                            // leaves the value two arrays, not one.
                            depth = depth.checked_sub(1)?;
                        }
                        Token::Comma if depth == 0 => {
                            elements.push(self.shape(&inner[start..n])?);
                            start = n + 1;
                        }
                        _ => {}
                    }
                }
                if !inner.is_empty() {
                    elements.push(self.shape(&inner[start..])?);
                }
                Some(Shape::Array {
                    tokens: value,
                    elements,
                })
            }
            _ => None,
        }
    }

    /// A field's value: kept, but for the parameters maps in a structured one.
    /// `whole_map` when the field is itself a parameters map.
    fn field(&mut self, shape: &Shape<'_>, whole_map: bool) {
        match shape {
            Shape::Scalar { tokens, string } => match string.as_deref() {
                Some(text) if text.trim_start().starts_with(['{', '[']) => {
                    self.json(tokens, text, whole_map, false);
                }
                // A parameters map is written as JSON.
                _ if whole_map => self.redact(tokens),
                _ => self.keep(tokens),
            },
            Shape::Json { tokens, text } => self.json(tokens, text, whole_map, true),
            Shape::Array { tokens, elements } => {
                if whole_map {
                    self.redact(tokens);
                    return;
                }
                // `ARRAY`, its brackets, and its commas.
                self.keep(&tokens[..2]);
                self.keep(&tokens[tokens.len() - 1..]);
                for element in elements {
                    self.field(element, false);
                }
                for &i in *tokens {
                    if matches!(self.token(i), Token::Comma) {
                        self.handled[i] = true;
                    }
                }
            }
        }
    }

    /// A structured value, with each parameters map in it redacted; a value
    /// that is not JSON is taken out whole.
    fn json(&mut self, tokens: &[usize], text: &str, whole_map: bool, json_keyword: bool) {
        let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(text) else {
            self.redact(tokens);
            return;
        };
        let mut removed = Vec::new();
        if redact_json_parameters(&mut parsed, whole_map, self.shown, &mut removed) {
            let literal = quote_string(&parsed.to_string());
            let replacement = if json_keyword {
                format!("JSON {literal}")
            } else {
                literal
            };
            self.replace(tokens, replacement);
            self.removed.extend(removed);
        } else {
            self.keep(tokens);
        }
    }

    /// Keep the tokens of `span` as written. A comment among them is still
    /// taken out.
    fn keep(&mut self, span: &[usize]) {
        for &i in span {
            self.handled[i] = true;
        }
    }

    /// Replace the text from the first token of `span` to its last with the
    /// redaction placeholder, and remember each value taken out.
    fn redact(&mut self, span: &[usize]) {
        let (Some(&first), Some(&last)) = (span.first(), span.last()) else {
            return;
        };
        for i in first..=last {
            let token = self.token(i);
            if matches!(token, Token::Whitespace(_)) {
                continue;
            }
            let text = self.text(i);
            if matches!(token, Token::Word(_)) || carries_value(text) {
                remember(&mut self.removed, text, false);
                if let Some(value) = string_literal(token) {
                    remember(&mut self.removed, value, true);
                }
            }
        }
        self.replace(span, REDACTED_SQL.to_string());
    }

    fn replace(&mut self, span: &[usize], replacement: String) {
        let (Some(&first), Some(&last)) = (span.first(), span.last()) else {
            return;
        };
        for handled in &mut self.handled[first..=last] {
            *handled = true;
        }
        self.replacements.push((
            self.toks[first].range.start..self.toks[last].range.end,
            replacement,
        ));
    }

    fn finish(mut self) -> RecordedStatement<'s> {
        self.removed.sort();
        self.removed.dedup();
        if self.replacements.is_empty() {
            return RecordedStatement {
                text: Cow::Borrowed(self.sql),
                removed: self.removed,
            };
        }
        self.replacements
            .sort_by_key(|(range, _)| std::cmp::Reverse(range.start));
        let mut text = self.sql.to_string();
        for (range, replacement) in self.replacements {
            text.replace_range(range, &replacement);
        }
        RecordedStatement {
            text: Cow::Owned(text),
            removed: self.removed,
        }
    }
}

/// Remember `value`, taken out of a statement, to take it out of an error the
/// statement fails with too. A bare value too short to tell apart from other
/// text is not.
fn remember(removed: &mut Vec<String>, value: &str, bare: bool) {
    let quoted = value.starts_with(['\'', '"', '`', '$']) || value.contains('\'');
    if value.chars().count() >= SHORTEST_BARE_VALUE || (!bare && quoted) {
        removed.push(value.to_string());
    }
}

/// Redact the entries of each parameters map in `value` that `shown` does not
/// show, as `SHOW CREATE` does, and remember each value taken out. `shown` is
/// given only the entry's name; `in_parameters` when `value` is itself a
/// parameters map. Returns whether a value was replaced.
fn redact_json_parameters(
    value: &mut serde_json::Value,
    in_parameters: bool,
    shown: &dyn Fn(&str) -> bool,
    removed: &mut Vec<String>,
) -> bool {
    if in_parameters {
        // A parameters map that is not a map does not validate, and which of
        // its parts would be a parameter's value cannot be told: all of it is
        // taken out.
        let serde_json::Value::Object(map) = value else {
            take_out(value, removed);
            return true;
        };
        let mut redacted = false;
        for (key, entry) in map.iter_mut() {
            let keep = matches!(&*entry,
                serde_json::Value::String(s) if is_secret_reference(s) || s == REDACTED);
            if !keep && !shown(key) {
                take_out(entry, removed);
                redacted = true;
            }
        }
        return redacted;
    }
    match value {
        serde_json::Value::Object(map) => {
            let mut redacted = false;
            for (key, entry) in map.iter_mut() {
                redacted |= redact_json_parameters(entry, is_params_map(key), shown, removed);
            }
            redacted
        }
        serde_json::Value::Array(items) => {
            // Every item, not only up to the first with a redacted value.
            let mut redacted = false;
            for item in items {
                redacted |= redact_json_parameters(item, false, shown, removed);
            }
            redacted
        }
        _ => false,
    }
}

/// Take `value` out of a statement: replace it with the placeholder, and
/// remember it and each value in it.
fn take_out(value: &mut serde_json::Value, removed: &mut Vec<String>) {
    remember_json(removed, value);
    *value = serde_json::Value::String(REDACTED.to_string());
}

/// Remember `value`, a JSON value taken out of a statement, and each value in
/// it: an error can quote the whole, as its JSON text, or any part.
fn remember_json(removed: &mut Vec<String>, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(s) => remember(removed, s, true),
        serde_json::Value::Null => {}
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            remember(removed, &value.to_string(), true);
        }
        serde_json::Value::Array(items) => {
            remember(removed, &value.to_string(), true);
            for item in items {
                remember_json(removed, item);
            }
        }
        serde_json::Value::Object(map) => {
            remember(removed, &value.to_string(), true);
            for item in map.values() {
                remember_json(removed, item);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::{GenericDialect, MySqlDialect, PostgreSqlDialect};

    use super::*;

    /// The keyword check only spares the tokenizer: whatever is next to a
    /// keyword, and in every dialect a statement is recorded with, a statement
    /// is recorded as it is when every statement is read token by token, and a
    /// keyword the tokenizer reads is always found.
    #[test]
    fn the_keyword_check_never_changes_what_is_recorded() {
        let dialects: [&dyn Dialect; 3] =
            [&PostgreSqlDialect {}, &GenericDialect {}, &MySqlDialect {}];
        let neighbors = [
            "", " ", "\n", "\t", "a", "Z", "_", "0", "9", "$", "'", "\"", "`", "(", ")", ";", ",",
            ".", "-", "/", "*", "@", ":", "#", "=", "é", "\u{a0}",
        ];
        let shown = |name: &str| name == "pg_host";
        let mut found = 0;
        for dialect in dialects {
            for (_, spelling) in DEFINING_KEYWORDS {
                let lower = std::str::from_utf8(spelling).expect("ASCII");
                for keyword in [lower.to_string(), lower.to_ascii_uppercase()] {
                    for before in neighbors {
                        for after in neighbors {
                            let word = format!("{before}{keyword}{after}");
                            for sql in [
                                format!(
                                    "{word} TABLE t WITH (pg_host = 'db', pg_pass = 'hunter2')"
                                ),
                                format!("SELECT 1 AS {word}, 'hunter2' AS p"),
                                format!("SELECT 1; {word} TABLE t WITH (pg_pass = 'hunter2')"),
                            ] {
                                let Ok(tokens) = Tokenizer::new(dialect, &sql).tokenize() else {
                                    continue;
                                };
                                if tokens.iter().any(is_defining_keyword) {
                                    found += 1;
                                    assert!(
                                        DEFINING_KEYWORDS
                                            .iter()
                                            .any(|(_, spelling)| mentions(&sql, spelling)),
                                        "{dialect:?}: {sql}"
                                    );
                                }
                                assert_eq!(
                                    recorded_statement(&sql, dialect, &shown),
                                    record(&sql, dialect, &shown, false),
                                    "{dialect:?}: {sql}"
                                );
                            }
                        }
                    }
                }
            }
        }
        // The sweep reads keywords, not only text that hides them.
        assert!(found > 1_000, "{found}");
    }

    /// A value is taken out of a recorded result, which is JSON, too: there a
    /// quote or backslash in it is escaped.
    #[test]
    fn a_value_is_taken_out_of_json_that_escapes_it() {
        let removed = ["'pa\"ss\\word'".to_string(), "pa\"ss\\word".to_string()];
        let result =
            serde_json::json!([{"type": "text", "text": "refused: pg_pass = 'pa\"ss\\word'"}])
                .to_string();
        assert!(result.contains(r#"pa\"ss\\word"#), "{result}");
        assert_eq!(
            redact_values(&result, &removed),
            serde_json::json!([{"type": "text", "text": "refused: pg_pass = <redacted>"}])
                .to_string()
        );
        // As written, too.
        assert_eq!(
            redact_values("refused: pa\"ss\\word", &removed),
            "refused: <redacted>"
        );
    }

    /// A keyword inside a longer word is not read as one, and the text is
    /// kept without reading its tokens; next to a digit or a sign, it is read.
    #[test]
    fn a_keyword_inside_a_word_is_not_a_keyword() {
        for sql in [
            "SELECT created_at, altered_by, attachment FROM t",
            "SELECT createdAt FROM t",
            "SELECT recreate, _alter, alter1 FROM t",
        ] {
            assert!(
                !DEFINING_KEYWORDS
                    .iter()
                    .any(|(_, spelling)| mentions(sql, spelling)),
                "{sql}"
            );
        }
        for sql in [
            "CREATE TABLE t",
            "1CREATE TABLE t",
            "/*x*/create TABLE t",
            "SELECT 1;ALTER TABLE t",
            "$attach x",
            "\u{e9}create x",
        ] {
            assert!(
                DEFINING_KEYWORDS
                    .iter()
                    .any(|(_, spelling)| mentions(sql, spelling)),
                "{sql}"
            );
        }
    }
}
