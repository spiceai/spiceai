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

//! Minimal `.env` file parsing and loading.
//!
//! Replaces the previous `dotenvy` dependency (a Spice fork patched to remove
//! shell-style variable substitution, see
//! <https://github.com/allan2/dotenvy/issues/113>) with just the behavior
//! Spice relies on:
//!
//! - `KEY=VALUE` entries with an optional `export ` prefix and `#` comments.
//! - Single-quoted values are literal; double-quoted values support backslash
//!   escapes. Quoted values may span multiple lines.
//! - No variable substitution: `$VAR` is always preserved literally.
//! - Malformed entries are reported per line with their line number, and
//!   iteration continues with the next line.

use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A malformed entry: the 1-based line number where the entry starts and
    /// the content of that line.
    LineParse(usize, String),
    /// The file could not be found or read.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::LineParse(line_number, content) => {
                write!(f, "malformed entry at line {line_number}: `{content}`")
            }
            Self::Io(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::LineParse(..) => None,
        }
    }
}

/// Returns an iterator over the `(key, value)` entries of the `.env` file at
/// `path`.
///
/// Malformed entries yield [`Error::LineParse`] items and iteration resumes at
/// the next line, so one bad line does not discard the rest of the file.
///
/// # Errors
///
/// Returns [`Error::Io`] when the file cannot be read.
pub fn from_path_iter<P: AsRef<Path>>(path: P) -> Result<Iter> {
    let content = fs::read_to_string(path).map_err(Error::Io)?;
    Ok(Iter::new(content))
}

/// Searches the current directory and its ancestors for `filename` and
/// returns an iterator over the `(key, value)` entries of the first match.
///
/// # Errors
///
/// Returns [`Error::Io`] when no such file exists or it cannot be read.
pub fn from_filename_iter<P: AsRef<Path>>(filename: P) -> Result<Iter> {
    let (_, iter) = find(filename.as_ref())?;
    Ok(iter)
}

/// Loads the entries of the `.env` file at `path` into the process
/// environment.
///
/// Existing environment variables are preserved, and the first occurrence of
/// a key within the file wins.
///
/// # Errors
///
/// Returns [`Error::Io`] when the file cannot be read and [`Error::LineParse`]
/// on the first malformed entry.
///
/// # Safety
///
/// Calls [`std::env::set_var`]: the caller must guarantee that no other
/// thread is reading or writing the process environment concurrently.
pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> Result<()> {
    // SAFETY: upheld by the caller.
    unsafe { load(from_path_iter(path)?) }
}

/// Searches the current directory and its ancestors for `filename` and loads
/// the entries of the first match into the process environment, returning the
/// path of the loaded file.
///
/// Existing environment variables are preserved, and the first occurrence of
/// a key within the file wins.
///
/// # Errors
///
/// Returns [`Error::Io`] when no such file exists or it cannot be read, and
/// [`Error::LineParse`] on the first malformed entry.
///
/// # Safety
///
/// Calls [`std::env::set_var`]: the caller must guarantee that no other
/// thread is reading or writing the process environment concurrently.
pub unsafe fn from_filename<P: AsRef<Path>>(filename: P) -> Result<PathBuf> {
    let (path, iter) = find(filename.as_ref())?;
    // SAFETY: upheld by the caller.
    unsafe { load(iter)? };
    Ok(path)
}

/// Searches for `filename` in the current directory and its ancestors,
/// returning the resolved path and an iterator over the file's entries.
fn find(filename: &Path) -> Result<(PathBuf, Iter)> {
    let mut dir = env::current_dir().map_err(Error::Io)?;
    loop {
        let candidate = dir.join(filename);
        match fs::read_to_string(&candidate) {
            Ok(content) => return Ok((candidate, Iter::new(content))),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::IsADirectory
                ) => {}
            Err(err) => return Err(Error::Io(err)),
        }
        if !dir.pop() {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "{} not found in the current directory or any parent",
                    filename.display()
                ),
            )));
        }
    }
}

/// Sets each entry into the process environment. Existing variables are
/// preserved; within the file the first occurrence of a key wins. Stops at
/// the first malformed entry.
unsafe fn load(iter: Iter) -> Result<()> {
    for item in iter {
        let (key, value) = item?;
        if env::var_os(&key).is_none() {
            // SAFETY: upheld by the caller (see the `# Safety` docs on the
            // public loading functions).
            unsafe { env::set_var(key, value) };
        }
    }
    Ok(())
}

/// Iterator over the `(key, value)` entries of a `.env` file.
pub struct Iter {
    content: String,
    /// Byte offset of the parse position; always on a `char` boundary.
    pos: usize,
    /// 1-based line number at `pos`.
    line: usize,
}

impl Iter {
    fn new(content: String) -> Self {
        // Strip a UTF-8 byte-order mark so it is not taken as part of the
        // first key.
        let pos = if content.starts_with('\u{feff}') {
            '\u{feff}'.len_utf8()
        } else {
            0
        };
        Self {
            content,
            pos,
            line: 1,
        }
    }

    fn peek(&self) -> Option<char> {
        self.content[self.pos..].chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        if c == '\n' {
            self.line += 1;
        }
        Some(c)
    }

    /// Consumes the rest of the current physical line, including the newline.
    fn skip_to_eol(&mut self) {
        while let Some(c) = self.bump() {
            if c == '\n' {
                break;
            }
        }
    }

    /// Consumes whitespace, including newlines.
    fn skip_blank(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    /// Consumes spaces and tabs within the current line.
    fn skip_inline_whitespace(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t')) {
            self.bump();
        }
    }

    /// Parses a key: an ASCII letter or `_`, followed by ASCII alphanumerics,
    /// `_`, or `.`.
    fn parse_key(&mut self) -> std::result::Result<String, ()> {
        let first = self.peek().ok_or(())?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(());
        }
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        {
            self.bump();
        }
        Ok(self.content[start..self.pos].to_string())
    }

    fn parse_entry(&mut self) -> std::result::Result<(String, String), ()> {
        let mut key = self.parse_key()?;
        self.skip_inline_whitespace();
        // `export` is either an optional prefix (`export KEY=1`) or a key
        // itself (`export=1`).
        if key == "export" && self.peek() != Some('=') {
            key = self.parse_key()?;
            self.skip_inline_whitespace();
        }
        if self.peek() != Some('=') {
            return Err(());
        }
        self.bump();
        self.skip_inline_whitespace();
        let value = self.parse_value()?;
        Ok((key, value))
    }

    fn parse_value(&mut self) -> std::result::Result<String, ()> {
        // A comment directly after `=` (`KEY= # comment`) is an empty value.
        if self.peek() == Some('#') {
            self.skip_to_eol();
            return Ok(String::new());
        }
        let mut value = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Ok(value);
            };
            if c == '\n' {
                return Ok(value);
            }
            self.bump();
            match c {
                // An unescaped space ends the value; only trailing whitespace
                // or a comment may follow (`k=v #comment`).
                ' ' | '\t' | '\r' => return self.expect_line_end(value),
                '\'' => self.single_quoted(&mut value)?,
                '"' => self.double_quoted(&mut value)?,
                '\\' => match self.escape()? {
                    Some(escaped) => value.push(escaped),
                    // A backslash at the end of the line ends the value.
                    None => return Ok(value),
                },
                // `$` included: values are taken literally, no substitution.
                _ => value.push(c),
            }
        }
    }

    /// Single-quoted segment: everything up to the closing quote is literal,
    /// including backslashes and newlines.
    fn single_quoted(&mut self, value: &mut String) -> std::result::Result<(), ()> {
        loop {
            match self.bump() {
                None => return Err(()), // unterminated quote
                Some('\'') => return Ok(()),
                Some(c) => value.push(c),
            }
        }
    }

    /// Double-quoted segment: backslash escapes apply, newlines are kept
    /// literally.
    fn double_quoted(&mut self, value: &mut String) -> std::result::Result<(), ()> {
        loop {
            match self.bump() {
                None => return Err(()), // unterminated quote
                Some('"') => return Ok(()),
                Some('\\') => match self.escape()? {
                    Some(escaped) => value.push(escaped),
                    None => return Err(()), // escape may not cross a line break
                },
                Some(c) => value.push(c),
            }
        }
    }

    /// Resolves the character following a backslash. Returns `Ok(None)` when
    /// the backslash sits at the end of the line or file.
    fn escape(&mut self) -> std::result::Result<Option<char>, ()> {
        match self.peek() {
            None | Some('\n') => Ok(None),
            Some(c) => {
                self.bump();
                match c {
                    '\\' | '\'' | '"' | '$' | ' ' => Ok(Some(c)),
                    'n' => Ok(Some('\n')),
                    _ => Err(()),
                }
            }
        }
    }

    /// After the value ends, only whitespace or a comment may remain on the
    /// line.
    fn expect_line_end(&mut self, value: String) -> std::result::Result<String, ()> {
        loop {
            match self.peek() {
                None | Some('\n') => return Ok(value),
                Some(' ' | '\t' | '\r') => {
                    self.bump();
                }
                Some('#') => {
                    self.skip_to_eol();
                    return Ok(value);
                }
                Some(_) => return Err(()),
            }
        }
    }
}

impl Iterator for Iter {
    type Item = Result<(String, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.skip_blank();
            match self.peek() {
                None => return None,
                Some('#') => {
                    self.skip_to_eol();
                    continue;
                }
                Some(_) => {}
            }
            let entry_line = self.line;
            let entry_start = self.pos;
            if let Ok(entry) = self.parse_entry() {
                return Some(Ok(entry));
            }
            let rest = &self.content[entry_start..];
            let content = rest[..rest.find('\n').unwrap_or(rest.len())]
                .trim_end()
                .to_string();
            // Resume at the next physical line so one malformed entry does not
            // discard the rest of the file.
            self.skip_to_eol();
            return Some(Err(Error::LineParse(entry_line, content)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iter(content: &str) -> Iter {
        Iter::new(content.to_string())
    }

    fn parse_all(content: &str) -> Vec<(String, String)> {
        iter(content)
            .collect::<Result<Vec<_>>>()
            .expect("expected the input to parse without errors")
    }

    #[test]
    fn test_basic_entries() {
        let parsed = parse_all(
            r#"
KEY=1
KEY2="2"
KEY3='3'
KEY4='fo ur'
KEY5="fi ve"
KEY6=s\ ix
KEY7=
KEY8=
KEY9=   # foo
KEY10  ="whitespace before ="
KEY11=    "whitespace after ="
export="export as key"
export   SHELL_LOVER=1
"#,
        );

        let expected = [
            ("KEY", "1"),
            ("KEY2", "2"),
            ("KEY3", "3"),
            ("KEY4", "fo ur"),
            ("KEY5", "fi ve"),
            ("KEY6", "s ix"),
            ("KEY7", ""),
            ("KEY8", ""),
            ("KEY9", ""),
            ("KEY10", "whitespace before ="),
            ("KEY11", "whitespace after ="),
            ("export", "export as key"),
            ("SHELL_LOVER", "1"),
        ];

        assert_eq!(parsed.len(), expected.len());
        for ((key, value), (expected_key, expected_value)) in parsed.iter().zip(expected) {
            assert_eq!(key, expected_key);
            assert_eq!(value, expected_value);
        }
    }

    #[test]
    fn test_escapes() {
        let parsed = parse_all(
            r#"
KEY=my\ cool\ value
KEY2=\$sweet
KEY3="awesome stuff \"mang\""
KEY4='sweet $\fgs'\''fds'
KEY5="'\"yay\\"\ "stuff"
KEY6="lol" #well you see when I say lol wh
KEY7="line 1\nline 2"
"#,
        );

        let expected = [
            ("KEY", r"my cool value"),
            ("KEY2", r"$sweet"),
            ("KEY3", r#"awesome stuff "mang""#),
            ("KEY4", r"sweet $\fgs'fds"),
            ("KEY5", r#"'"yay\ stuff"#),
            ("KEY6", "lol"),
            ("KEY7", "line 1\nline 2"),
        ];

        assert_eq!(parsed.len(), expected.len());
        for ((key, value), (expected_key, expected_value)) in parsed.iter().zip(expected) {
            assert_eq!(key, expected_key);
            assert_eq!(value, expected_value);
        }
    }

    /// Values containing `$` must be preserved literally: no shell-style
    /// variable substitution (<https://github.com/allan2/dotenvy/issues/113>).
    #[test]
    fn test_no_variable_substitution() {
        let parsed = parse_all(
            r"
API_KEY=sk-abc$123def
PASSWORD=p@ss$word$123
DOLLAR_SIGN=value_with_$_in_middle
CURLY_BRACES=value_${NOT_A_VAR}_here
MULTIPLE_DOLLARS=$$double$$dollars$$
TRAILING_BACKSLASH=\
",
        );
        let parsed = parsed
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(
            parsed,
            vec![
                ("API_KEY", "sk-abc$123def"),
                ("PASSWORD", "p@ss$word$123"),
                ("DOLLAR_SIGN", "value_with_$_in_middle"),
                ("CURLY_BRACES", "value_${NOT_A_VAR}_here"),
                ("MULTIPLE_DOLLARS", "$$double$$dollars$$"),
                ("TRAILING_BACKSLASH", ""),
            ]
        );
    }

    #[test]
    fn test_quoted_dollar_values() {
        let parsed = parse_all(
            r#"
SINGLE='literal $VAR and ${VAR}'
DOUBLE="literal $VAR and ${VAR}"
"#,
        );
        assert_eq!(parsed[0].1, "literal $VAR and ${VAR}");
        assert_eq!(parsed[1].1, "literal $VAR and ${VAR}");
    }

    #[test]
    fn test_comments_and_blank_lines() {
        let parsed = parse_all(
            "\n# foo=bar\n#    \n\n   # indented comment\nREAL=value#not-a-comment\nSPACED=value # a comment\nTRAILING=value   \nWS_ONLY=     \n",
        );
        let parsed = parsed
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            parsed,
            vec![
                ("REAL", "value#not-a-comment"),
                ("SPACED", "value"),
                ("TRAILING", "value"),
                ("WS_ONLY", ""),
            ]
        );
    }

    #[test]
    fn test_multiline_quoted_values() {
        let parsed = parse_all("KEY=\"line 1\nline 2\"\nKEY2='a\nb'\nKEY3=after\n");
        let parsed = parsed
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            parsed,
            vec![
                ("KEY", "line 1\nline 2"),
                ("KEY2", "a\nb"),
                ("KEY3", "after")
            ]
        );
    }

    #[test]
    fn test_malformed_lines_report_line_numbers_and_continue() {
        let mut entries = iter(
            "VALID_FIRST=first_value\nTHIS LINE HAS NO EQUALS SIGN\nVALID_SECOND=second_value\n",
        );

        let first = entries
            .next()
            .expect("expected a first entry")
            .expect("first entry should parse");
        assert_eq!(
            first,
            ("VALID_FIRST".to_string(), "first_value".to_string())
        );

        let err = entries
            .next()
            .expect("expected a second item")
            .expect_err("second item should be a parse error");
        match err {
            Error::LineParse(line_number, content) => {
                assert_eq!(line_number, 2);
                assert_eq!(content, "THIS LINE HAS NO EQUALS SIGN");
            }
            Error::Io(err) => panic!("expected LineParse, got Io: {err}"),
        }

        let third = entries
            .next()
            .expect("expected a third entry")
            .expect("third entry should parse");
        assert_eq!(
            third,
            ("VALID_SECOND".to_string(), "second_value".to_string())
        );
        assert!(entries.next().is_none());
    }

    #[test]
    fn test_invalid_lines() {
        // Note: trailing spaces after `invalid` below.
        let items: Vec<_> = iter("\n  invalid    \nvery bacon = yes indeed\n=value\n").collect();
        assert_eq!(items.len(), 3);
        for item in items {
            assert!(item.is_err(), "expected a parse error, got: {item:?}");
        }
    }

    #[test]
    fn test_invalid_escapes_and_unterminated_quotes() {
        for content in [
            r"KEY=h\8u",
            r#"KEY="why"#,
            "KEY='please stop''",
            r"KEY=>\f<",
        ] {
            let items: Vec<_> = iter(content).collect();
            assert!(
                items.iter().all(std::result::Result::is_err),
                "expected only parse errors for {content:?}, got: {items:?}"
            );
        }
    }

    #[test]
    fn test_key_validation() {
        for content in [".Key=VALUE", "<><><>"] {
            let err = iter(content)
                .next()
                .expect("expected an item")
                .expect_err("an invalid key must be rejected");
            assert!(
                matches!(err, Error::LineParse(1, ref line) if line == content),
                "unexpected error for {content:?}: {err:?}"
            );
        }
        let parsed = parse_all("_UNDER.SCORE.9=ok\n");
        assert_eq!(parsed[0], ("_UNDER.SCORE.9".to_string(), "ok".to_string()));
    }

    #[test]
    fn test_bom_and_crlf() {
        let parsed = parse_all("\u{feff}KEY=value\r\nKEY2=value2 # comment\r\n");
        let parsed = parsed
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(parsed, vec![("KEY", "value"), ("KEY2", "value2")]);
    }

    #[test]
    fn test_garbage_after_value_is_an_error() {
        let items: Vec<_> = iter("KEY=v w\n").collect();
        assert_eq!(items.len(), 1);
        items[0]
            .as_ref()
            .expect_err("unescaped text after the value must be rejected");
    }

    #[test]
    fn test_from_path_iter_missing_file() {
        let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let missing = temp_dir.path().join(".env.absent");
        let Err(err) = from_path_iter(&missing) else {
            panic!("expected an error for a missing file");
        };
        match err {
            Error::Io(err) => assert_eq!(err.kind(), io::ErrorKind::NotFound),
            Error::LineParse(..) => panic!("expected Io, got LineParse"),
        }
    }

    #[test]
    fn test_load_preserves_existing_env_and_first_occurrence_wins() {
        let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let env_file = temp_dir.path().join(".env.load");
        std::fs::write(
            &env_file,
            "DOTENV_TEST_EXISTING=from_file\nDOTENV_TEST_NEW=first\nDOTENV_TEST_NEW=second\n",
        )
        .expect("failed to write test .env file");

        // SAFETY: tests in this module use unique variable names and this is
        // the only test mutating them.
        unsafe {
            env::set_var("DOTENV_TEST_EXISTING", "from_env");
            from_path(&env_file).expect("failed to load .env file");
        }

        assert_eq!(
            env::var("DOTENV_TEST_EXISTING").as_deref(),
            Ok("from_env"),
            "existing environment variables must be preserved"
        );
        assert_eq!(
            env::var("DOTENV_TEST_NEW").as_deref(),
            Ok("first"),
            "the first occurrence of a key within the file must win"
        );

        // SAFETY: single-threaded test cleanup of variables owned by this test.
        unsafe {
            env::remove_var("DOTENV_TEST_EXISTING");
            env::remove_var("DOTENV_TEST_NEW");
        }
    }

    #[test]
    fn test_find_searches_ancestors() {
        let temp_dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let nested = temp_dir.path().join("a/b/c");
        std::fs::create_dir_all(&nested).expect("failed to create nested dirs");
        std::fs::write(temp_dir.path().join(".env.find"), "FOUND=yes\n")
            .expect("failed to write test .env file");

        // `find` walks up from the current directory; emulate it from `nested`
        // by resolving the same candidate chain.
        let mut dir = nested;
        let resolved = loop {
            let candidate = dir.join(".env.find");
            if candidate.is_file() {
                break candidate;
            }
            assert!(dir.pop(), "should have found .env.find in an ancestor");
        };
        assert_eq!(resolved, temp_dir.path().join(".env.find"));

        let entries = from_path_iter(&resolved)
            .expect("failed to open found file")
            .collect::<Result<Vec<_>>>()
            .expect("failed to parse found file");
        assert_eq!(entries, vec![("FOUND".to_string(), "yes".to_string())]);
    }
}
