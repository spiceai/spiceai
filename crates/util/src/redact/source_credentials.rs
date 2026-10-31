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

//! Taking credentials out of URLs: the user name and password a dataset or
//! catalog source URL can carry, and the values of secret-looking query keys.

use std::borrow::Cow;

use crate::source::find_first_delimiter;

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
/// `taken` gains what is taken out: the userinfo, its password alone, and each
/// secret query value.
///
/// `embedded` is set when `s` is free text (an error message), where
/// whitespace and quotes end the URL; otherwise `s` is a whole source.
///
/// This is a string scan, not a URL parse, so it also fails closed for URLs a
/// parser would reject (a bad port, an unknown scheme, a raw `?`, `#` or quote
/// inside the password).
fn redact_url_at(s: &str, embedded: bool, taken: &mut Vec<String>) -> Option<(String, usize)> {
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
    if let Some((inner, len)) = redact_url_at(&s[authority_start..], embedded, taken) {
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
            let userinfo = &authority[..at];
            if let Some((_, password)) = userinfo.split_once(':') {
                taken.push(password.to_string());
            }
            taken.push(userinfo.to_string());
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
                    Some((key, value)) if is_secret_query_key(key) => {
                        taken.push(value.to_string());
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

/// `from` as the HTTP API and `SHOW CREATE` report it, without credentials.
///
/// A component source can carry credentials in its URL, either directly
/// (`https://user:password@host/file.csv`) or after a connector prefix with
/// any delimiter the runtime accepts (`iceberg:https://...`,
/// `iceberg/https://...`, `iceberg://https://...`). Any API key that can list
/// components can read `from`, so the `user[:password]@` part and the values of
/// secret-looking query keys (`token`, `api_key`, `sig`, ...) are removed
/// before it is returned. Only the URL at the start of `from`, or right after
/// its connector prefix, is redacted; a `://` later in a path is left alone. A
/// source without credentials is returned unchanged.
#[must_use]
pub fn redact_source_credentials(from: &str) -> String {
    if let Some((redacted, len)) = redact_url_at(from, false, &mut Vec::new()) {
        let redacted = format!("{redacted}{}", &from[len..]);
        if redacted != from {
            return redacted;
        }
    }
    if let Some((pos, delimiter_len)) = find_first_delimiter(from) {
        let start = pos + delimiter_len;
        if let Some((redacted, len)) = redact_url_at(&from[start..], false, &mut Vec::new()) {
            return format!("{}{redacted}{}", &from[..start], &from[start + len..]);
        }
    }
    from.to_string()
}

/// `message` with every URL in it redacted as [`redact_source_credentials`]
/// would. Used for status error messages, which connectors often build from
/// the source URL.
#[must_use]
pub fn redact_message_credentials(message: &str) -> String {
    take_out_url_credentials(message).0.into_owned()
}

/// `text` with every URL in it redacted as [`redact_message_credentials`]
/// redacts it, and what that takes out: each URL's user name and password, the
/// password alone, and each secret query value, to take out of other text that
/// repeats them. Text without such a URL is returned as it is.
#[must_use]
pub fn take_out_url_credentials(text: &str) -> (Cow<'_, str>, Vec<String>) {
    let mut taken = Vec::new();
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let mut prev: Option<char> = None;
    while let Some(c) = rest.chars().next() {
        if prev.is_none_or(|p| !is_scheme_char(p))
            && let Some((redacted, len)) = redact_url_at(rest, true, &mut taken)
        {
            out.push_str(&redacted);
            prev = redacted.chars().last();
            rest = &rest[len..];
            continue;
        }
        out.push(c);
        prev = Some(c);
        rest = &rest[c.len_utf8()..];
    }
    if out == text {
        (Cow::Borrowed(text), Vec::new())
    } else {
        taken.retain(|part| !part.is_empty());
        (Cow::Owned(out), taken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_source_credentials_removes_userinfo() {
        for (from, expected) in [
            (
                "https://user:secret@files.example.com/data.csv",
                "https://files.example.com/data.csv",
            ),
            (
                "http://token@files.example.com:8080/data.csv",
                "http://files.example.com:8080/data.csv",
            ),
            (
                "https://:secret@files.example.com",
                "https://files.example.com",
            ),
            (
                "postgres://user:secret@db.example.com:5432/app?sslmode=require",
                "postgres://db.example.com:5432/app?sslmode=require",
            ),
            // `@` inside the password: everything up to the last `@` goes.
            (
                "https://user:p@ss@files.example.com/data.csv",
                "https://files.example.com/data.csv",
            ),
            // Forms WHATWG parsers accept for special schemes.
            (
                "https:///user:secret@files.example.com/data.csv",
                "https:///files.example.com/data.csv",
            ),
            (
                "https:user:secret@files.example.com/data.csv",
                "https:files.example.com/data.csv",
            ),
            (
                "HTTPS:\\\\user:secret@files.example.com/data.csv",
                "HTTPS:\\\\files.example.com/data.csv",
            ),
            // Every delimiter `DatasetSpec::path` / `CatalogSpec::catalog_id` accept.
            (
                "iceberg:https://user:secret@catalog.example.com/v1/ns",
                "iceberg:https://catalog.example.com/v1/ns",
            ),
            (
                "iceberg/https://user:secret@catalog.example.com/v1/ns",
                "iceberg/https://catalog.example.com/v1/ns",
            ),
            (
                "iceberg://https://user:secret@catalog.example.com/v1/ns",
                "iceberg://https://catalog.example.com/v1/ns",
            ),
            // Malformed authorities fail closed.
            (
                "https://user:secret@files.example.com:invalid/data.csv",
                "https://files.example.com:invalid/data.csv",
            ),
            (
                "iceberg:https://user:secret@catalog.example.com:bad/v1",
                "iceberg:https://catalog.example.com:bad/v1",
            ),
            ("https://user:secret@", "https://"),
            // Secret query values.
            (
                "https://files.example.com/data.csv?api_key=secret&format=csv",
                "https://files.example.com/data.csv?api_key=***&format=csv",
            ),
            (
                "https://acct.blob.example.com/c/data.csv?sv=2024&sig=abc%2Fdef#frag",
                "https://acct.blob.example.com/c/data.csv?sv=2024&sig=***#frag",
            ),
            (
                "iceberg:https://user:pw@catalog.example.com/v1?Token=abc",
                "iceberg:https://catalog.example.com/v1?Token=***",
            ),
        ] {
            assert_eq!(redact_source_credentials(from), expected, "from: {from}");
        }
    }

    #[test]
    fn redact_source_credentials_keeps_sources_without_credentials() {
        for from in [
            "postgres:public.orders",
            "s3://bucket/path/trips.parquet",
            "s3://bucket/reports/https://alice@example.csv",
            "abfs://container/path/",
            "abfss://container@account.dfs.core.windows.net/data.csv",
            "https://files.example.com/data.csv",
            "https://files.example.com/users/@me/data.csv",
            "https://files.example.com/data.csv?owner=a@example.com",
            "https://files.example.com/data.csv#a@b",
            "HTTPS://Files.Example.com:8443/Data.csv",
            "file:../data/orders.csv",
            "file:///home/me/a@b/orders.csv",
            "memory:store",
            "spice.ai:spiceai/quickstart/datasets/taxi_trips",
            "",
        ] {
            assert_eq!(redact_source_credentials(from), from);
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

    /// Passwords with characters a URL parser would treat as delimiters,
    /// percent-encoded secret keys, and keys that only look like secrets.
    #[test]
    fn redact_source_credentials_handles_delimiters_in_secrets() {
        for (from, want) in [
            ("https://u:pa?ss@h/f.csv", "https://h/f.csv"),
            ("https://u:pa#ss@h/f.csv", "https://h/f.csv"),
            ("https://u:pa\"ss@h/f.csv", "https://h/f.csv"),
            ("https://u:pa\tss@h/f.csv", "https://h/f.csv"),
            ("https://h/f.csv?%74oken=T0K", "https://h/f.csv?%74oken=***"),
            (
                "https://[::1]:8443/f.csv?a=1",
                "https://[::1]:8443/f.csv?a=1",
            ),
            (
                "https://h/f.csv?author=alice",
                "https://h/f.csv?author=alice",
            ),
            (
                "https://h/f.csv?monkey=banana&sort_key=date",
                "https://h/f.csv?monkey=banana&sort_key=date",
            ),
            (
                "https://h/f.csv?api_key=K&X-Amz-Signature=S",
                "https://h/f.csv?api_key=***&X-Amz-Signature=***",
            ),
            (
                "iceberg://https://u:S@h/x?token=T",
                "iceberg://https://h/x?token=***",
            ),
            ("https://u:123?SECRET@h/x", "https://h/x"),
            ("https://u:p@ss?SECRET@h/x", "https://h/x"),
            ("file://alice@report.csv", "file://alice@report.csv"),
            (
                "https://alice:p'ass@h/x?token=abc'def",
                "https://h/x?token=***",
            ),
            (
                "https://h/f.csv?email=a@b.example",
                "https://h/f.csv?email=a@b.example",
            ),
            (
                "https://h:8443?email=a@b.example",
                "https://h:8443?email=a@b.example",
            ),
            ("https://u:123#SECRET@h/x", "https://h/x"),
        ] {
            assert_eq!(redact_source_credentials(from), want, "{from}");
        }
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

    /// What [`take_out_url_credentials`] takes out is what another text that
    /// repeats the URL, such as an error, must lose too.
    #[test]
    fn take_out_url_credentials_reports_what_it_takes_out() {
        let (text, taken) = take_out_url_credentials(
            "CREATE TABLE t WITH (from = 'https://admin:pw-536271@files.example.com/a.csv?sig=s1g&v=1')",
        );
        assert_eq!(
            text,
            "CREATE TABLE t WITH (from = 'https://files.example.com/a.csv?sig=***&v=1')"
        );
        assert_eq!(taken, ["pw-536271", "admin:pw-536271", "s1g"]);

        let unchanged = "CREATE TABLE t WITH (from = 'postgres:public.t', pg_host = 'db@x')";
        let (text, taken) = take_out_url_credentials(unchanged);
        assert!(matches!(text, Cow::Borrowed(_)));
        assert!(taken.is_empty());
    }
}
