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

//! Credential redaction for URLs and for messages that embed them.
//!
//! A URL can carry a credential in two places: its userinfo
//! (`https://user:password@host/...`) and the values of secret-looking query keys
//! (`?token=...`, `?api_key=...`, `?sig=...`). Whatever reports such a URL to a reader wider
//! than whoever configured it — an API response, a plan, a log line, an error — reports it
//! through this crate, so there is one definition of what a credential in a URL is.
//! The redacted URL keeps everything else as written.

/// Schemes whose URLs WHATWG parsers accept with any number of `/` or `\`
/// (including none) before the authority, e.g. `https:user:pass@host`.
const SPECIAL_URL_SCHEMES: &[&str] = &["http", "https", "ws", "wss", "ftp"];

/// Schemes where the part before `@` is not a credential: a storage
/// container (`abfss://container@account.dfs.core.windows.net/path`), or a
/// file name (`file://alice@report.csv`).
const NON_CREDENTIAL_USERINFO_SCHEMES: &[&str] = &["abfs", "abfss", "wasb", "wasbs", "file"];

/// Query keys whose values are credentials, compared with case and
/// punctuation removed (`api_key` and `apiKey` are both `apikey`).
const SECRET_QUERY_KEYS: &[&str] = &["key", "sig", "auth", "pwd", "pass"];

/// Endings of query keys whose values are credentials (`password`,
/// `client_secret`, `access_token`, `X-Amz-Signature`, `x-goog-credential`),
/// compared the same way. `sort_key` and `monkey` do not match.
const SECRET_QUERY_KEY_ENDINGS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "apikey",
    "accesskey",
    "secretkey",
    "privatekey",
    "accountkey",
    "sharedkey",
    "signature",
    "credential",
    "credentials",
    "authorization",
];

const REDACTED: &str = "***";

fn is_scheme_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')
}

/// Characters that end a URL embedded in free text.
fn ends_embedded_url(c: char) -> bool {
    c.is_whitespace() || matches!(c, '"' | '\'' | '`' | '<' | '>')
}

fn is_secret_query_key(key: &str) -> bool {
    let key: String = percent_encoding::percent_decode_str(key)
        .decode_utf8_lossy()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect();
    SECRET_QUERY_KEYS.contains(&key.as_str())
        || SECRET_QUERY_KEY_ENDINGS.iter().any(|e| key.ends_with(e))
}

/// Whether `authority` (no userinfo) is a plausible `host[:port]`, i.e. its
/// port, if any, is numeric.
fn is_host_and_port(authority: &str) -> bool {
    let after_host = match authority.rfind(']') {
        Some(bracket) => &authority[bracket + 1..],
        None => authority,
    };
    match after_host.rfind(':') {
        Some(colon) => after_host[colon + 1..].chars().all(|c| c.is_ascii_digit()),
        None => true,
    }
}

/// If `s` starts with a URL that has an authority, `s`'s URL prefix with the
/// userinfo removed and secret query values replaced, plus how many bytes of
/// `s` that prefix covers. `None` when `s` does not start with such a URL.
///
/// `embedded` is set when `s` is free text (an error message), where
/// whitespace and quotes end the URL; otherwise `s` is a whole source.
///
/// This is a string scan, not a URL parse, so it also fails closed for URLs a
/// parser would reject (a bad port, an unknown scheme, a raw `?`, `#` or quote
/// inside the password).
fn redact_url_at(s: &str, embedded: bool) -> Option<(String, usize)> {
    let colon = s.find(':')?;
    let scheme = &s[..colon];
    if !scheme.starts_with(|c: char| c.is_ascii_alphabetic()) || !scheme.chars().all(is_scheme_char)
    {
        return None;
    }
    let scheme = scheme.to_ascii_lowercase();
    let after_colon = &s[colon + 1..];
    let authority_start = if SPECIAL_URL_SCHEMES.contains(&scheme.as_str()) {
        colon + 1 + (after_colon.len() - after_colon.trim_start_matches(['/', '\\']).len())
    } else if after_colon.starts_with("//") {
        colon + 1 + "//".len()
    } else {
        return None;
    };
    let ends_url = |c: char| embedded && ends_embedded_url(c);

    // A connector prefix written with `://` (`iceberg://https://u:p@h/...`):
    // redact the inner URL.
    if let Some((inner, len)) = redact_url_at(&s[authority_start..], embedded) {
        return Some((
            format!("{}{inner}", &s[..authority_start]),
            authority_start + len,
        ));
    }

    // The authority as a URL parser would read it ends at the path, query or
    // fragment. A password can hold a raw `?`, `#`, quote or tab, though
    // (`user:pa?ss@host`), and some drivers read it that way. So when the
    // region up to the path holds an `@` past that point, and the parsed
    // authority is userinfo-like, fail closed: remove everything up to the
    // last `@` before the path. The parsed authority is userinfo-like when it
    // has an `@`, is not a valid `host[:port]`, or has a `:` and the text up
    // to that `@` is not a `key=value` query (`u:123?SECRET@h` is, but
    // `h:8443?email=a@b.example` is a host, port and query). Only a space
    // ends that region in free text, since URL parsers strip tabs and
    // newlines.
    let after_start = &s[authority_start..];
    let strict_len = after_start
        .find(|c: char| matches!(c, '/' | '\\' | '?' | '#') || ends_url(c))
        .unwrap_or(after_start.len());
    let strict = &after_start[..strict_len];
    let extended_len = after_start
        .find(|c: char| matches!(c, '/' | '\\') || (embedded && c == ' '))
        .unwrap_or(after_start.len());
    let last_at = after_start[..extended_len].rfind('@');
    let authority_len = match last_at {
        Some(at)
            if at >= strict_len
                && (strict.contains('@')
                    || !is_host_and_port(strict)
                    || (strict.contains(':') && !after_start[strict_len..at].contains('='))) =>
        {
            let host_len = after_start[at + 1..extended_len]
                .find(|c: char| matches!(c, '?' | '#') || ends_url(c))
                .unwrap_or(extended_len - at - 1);
            at + 1 + host_len
        }
        _ => strict_len,
    };
    let authority_end = authority_start + authority_len;
    let authority = &s[authority_start..authority_end];

    let mut out = String::with_capacity(s.len());
    out.push_str(&s[..authority_start]);
    match authority.rfind('@') {
        Some(at) if !NON_CREDENTIAL_USERINFO_SCHEMES.contains(&scheme.as_str()) => {
            out.push_str(&authority[at + 1..]);
        }
        _ => out.push_str(authority),
    }

    let rest = &s[authority_end..];
    let mut url_len = rest.find(ends_url).unwrap_or(rest.len());
    if embedded {
        url_len = extend_over_quoted_secret(rest, url_len);
    }
    let rest = &rest[..url_len];
    let (before_fragment, fragment) = rest.split_at(rest.find('#').unwrap_or(rest.len()));
    match before_fragment.split_once('?') {
        Some((path, query)) => {
            out.push_str(path);
            out.push('?');
            for (i, pair) in query.split('&').enumerate() {
                if i > 0 {
                    out.push('&');
                }
                match pair.split_once('=') {
                    Some((key, _)) if is_secret_query_key(key) => {
                        out.push_str(key);
                        out.push('=');
                        out.push_str(REDACTED);
                    }
                    _ => out.push_str(pair),
                }
            }
        }
        None => out.push_str(before_fragment),
    }
    out.push_str(fragment);
    Some((out, authority_end + url_len))
}

/// In free text a quote ends a URL, but a secret query value can contain one
/// (`?token=abc'def`). If the query pair that the quote cuts off is a secret,
/// the URL is taken to run past that pair (to the next quote or whitespace),
/// and the scan goes on so later secret pairs are replaced too. A quote that
/// is the last character of a pair is taken as the closing quote.
fn extend_over_quoted_secret(rest: &str, mut url_len: usize) -> usize {
    let Some(query_start) = rest[..url_len].find('?') else {
        return url_len;
    };
    let region = &rest[..rest.find(char::is_whitespace).unwrap_or(rest.len())];
    let mut pos = query_start + 1;
    while pos < url_len {
        let pair_end = region[pos..]
            .find(['&', '#'])
            .map_or(region.len(), |i| pos + i);
        if pair_end > url_len {
            match region[pos..pair_end].split_once('=') {
                Some((key, _)) if is_secret_query_key(key) && url_len + 1 < pair_end => {
                    url_len = region[pair_end..]
                        .find(ends_embedded_url)
                        .map_or(region.len(), |i| pair_end + i);
                }
                _ => return url_len,
            }
        }
        if region[pair_end..].starts_with('#') {
            break;
        }
        pos = pair_end + 1;
    }
    url_len
}

/// `url` without credentials: its `user[:password]@` removed and the values of
/// secret-looking query keys (`token`, `api_key`, `sig`, `X-Amz-Signature`, ...)
/// replaced with `***`, everything else as written.
///
/// Only a URL at the start of `url` is redacted; text that does not start with a
/// URL that has an authority is returned unchanged.
#[must_use]
pub fn redact_url_credentials(url: &str) -> String {
    match redact_url_at(url, false) {
        Some((redacted, len)) => format!("{redacted}{}", &url[len..]),
        None => url.to_string(),
    }
}

/// `message` with every URL in it redacted as [`redact_url_credentials`]
/// would. For error messages, which connectors often build from a source URL.
#[must_use]
pub fn redact_message_credentials(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    let mut prev: Option<char> = None;
    while let Some(c) = rest.chars().next() {
        if prev.is_none_or(|p| !is_scheme_char(p))
            && let Some((redacted, len)) = redact_url_at(rest, true)
        {
            let (redacted, len) = without_closing_parentheses(rest, redacted, len);
            out.push_str(&redacted);
            prev = redacted.chars().last();
            rest = &rest[len..];
            continue;
        }
        out.push(c);
        prev = Some(c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

/// The URL [`redact_url_at`] found at the start of `rest`, `len` bytes long, without the
/// `)`s at its end that close a parenthesis opened before it: `for url (https://h/x)`, as
/// errors from HTTP clients put it. A `)` is the URL's own while the URL opens as many as
/// it closes (`https://h/wiki/Foo_(bar)`), the rule autolinkers use. Without this, a
/// secret query value at the end of the URL would take the `)` with it when it is replaced.
fn without_closing_parentheses(rest: &str, redacted: String, len: usize) -> (String, usize) {
    let mut end = len;
    while rest[..end].ends_with(')')
        && rest[..end].matches(')').count() > rest[..end].matches('(').count()
    {
        end -= 1;
    }
    if end == len {
        return (redacted, len);
    }
    redact_url_at(&rest[..end], true).unwrap_or((redacted, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_credentials_removes_userinfo_and_secret_query_values() {
        for (url, expected) in [
            (
                "https://user:secret@files.example.com/data.csv",
                "https://files.example.com/data.csv",
            ),
            (
                "http://token@files.example.com:8080/data.csv",
                "http://files.example.com:8080/data.csv",
            ),
            (
                "https://files.example.com/data.csv?api_key=secret&format=csv",
                "https://files.example.com/data.csv?api_key=***&format=csv",
            ),
            (
                "https://acct.blob.example.com/c/data.csv?sv=2024&sig=abc%2Fdef#frag",
                "https://acct.blob.example.com/c/data.csv?sv=2024&sig=***#frag",
            ),
        ] {
            assert_eq!(redact_url_credentials(url), expected, "url: {url}");
        }
        for url in [
            "https://httpbin.org/get?param1=value1&param2=value2",
            "http://127.0.0.1:8080/api",
            "postgres:public.orders",
            "",
        ] {
            assert_eq!(redact_url_credentials(url), url);
        }
    }

    #[test]
    fn redact_message_credentials_redacts_embedded_urls() {
        assert_eq!(
            redact_message_credentials(
                "Cannot setup the dataset d (https). https://alice:SECRET@example.com:bad/data.csv is not a valid URL."
            ),
            "Cannot setup the dataset d (https). https://example.com:bad/data.csv is not a valid URL."
        );
        assert_eq!(
            redact_message_credentials(
                "Error performing HEAD https://u:SECRET@h/x.csv?token=T0K in 4s; retry 'postgresql://a:B@db/app'"
            ),
            "Error performing HEAD https://h/x.csv?token=*** in 4s; retry 'postgresql://db/app'"
        );
        let plain = "Unable to authenticate with datasource credentials: user@example.com";
        assert_eq!(redact_message_credentials(plain), plain);
    }

    /// Passwords and query values holding characters free text would end a URL at.
    #[test]
    fn redact_message_credentials_handles_delimiters_in_secrets() {
        assert_eq!(
            redact_message_credentials("bad url 'https://u:pa?ss@h/f.csv': invalid port"),
            "bad url 'https://h/f.csv': invalid port"
        );
        assert_eq!(
            redact_message_credentials(
                "bad url https://u:pa\tss@h/x and iceberg://https://u:S@h/y"
            ),
            "bad url https://h/x and iceberg://https://h/y"
        );
        // Apostrophes inside a password or a secret query value.
        assert_eq!(
            redact_message_credentials("GET 'https://alice:p'ass@h/x?token=abc'def&a=1' failed"),
            "GET 'https://h/x?token=***&a=1' failed"
        );
        // Secret pairs after a quoted secret value.
        assert_eq!(
            redact_message_credentials("GET 'https://h/x?token=abc'def&api_key=SECOND' failed"),
            "GET 'https://h/x?token=***&api_key=***' failed"
        );
        assert_eq!(
            redact_message_credentials("GET https://h/x?token=abc'def&a=1&api_key=SECOND failed"),
            "GET https://h/x?token=***&a=1&api_key=*** failed"
        );
        // A host, port and query holding an `@` keeps its origin.
        assert_eq!(
            redact_message_credentials("GET https://h:8443?email=a@b.example failed"),
            "GET https://h:8443?email=a@b.example failed"
        );
    }

    /// The `)` that closes a parenthesis around a URL stays in the message, and one the URL
    /// opened itself stays in the URL.
    #[test]
    fn redact_message_credentials_keeps_closing_parentheses() {
        for (message, expected) in [
            (
                "error sending request for url (http://h:1/v1?api_key=K/chat/completions) Verify",
                "error sending request for url (http://h:1/v1?api_key=***) Verify",
            ),
            (
                "Error performing GET http://alice:pw@h:1/c?comp=list (after 3 retries)",
                "Error performing GET http://h:1/c?comp=list (after 3 retries)",
            ),
            (
                "(see https://u:p@h/x?token=abc)",
                "(see https://h/x?token=***)",
            ),
            (
                "docs at https://en.example.org/wiki/Foo_(bar) and (https://h/wiki/Foo_(bar))",
                "docs at https://en.example.org/wiki/Foo_(bar) and (https://h/wiki/Foo_(bar))",
            ),
        ] {
            assert_eq!(redact_message_credentials(message), expected, "{message}");
        }
    }
}
