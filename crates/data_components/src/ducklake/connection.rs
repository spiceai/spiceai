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

//! The connection string of a `DuckLake` metadata database, with its password
//! held apart. `DuckDB`'s `postgres` and `mysql` extensions repeat the path
//! they attach in every error that connecting to the database raises, whether
//! a statement attaches it or a query reconnects later, so the password goes
//! to the database through a `DuckDB` secret, which they read but never print.

use std::ops::Range;

/// The database a metadata connection string reaches, whose extension reads a
/// password from a `DuckDB` secret of the same type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataDatabase {
    Postgres,
    Mysql,
}

impl MetadataDatabase {
    /// The `DuckDB` extension that attaches the database, which is also the
    /// type of the secret it reads.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }
}

/// A `DuckLake` metadata connection string, as `ducklake_connection_string`
/// sets it, with its password held apart.
#[derive(Debug, PartialEq, Eq)]
pub struct MetadataConnection {
    /// What `ATTACH 'ducklake:…'` names: the connection string without its
    /// password.
    pub path: String,
    /// The password, and the database whose extension reads it from a secret.
    pub password: Option<(MetadataDatabase, String)>,
}

impl MetadataConnection {
    /// Split `connection_string`. A `PostgreSQL` string may be `key = value`
    /// pairs or a URI, which is rewritten as pairs, since the extension reads
    /// a secret's fields as pairs put in front of the path; a `MySQL` string
    /// may be pairs. Any other string, one without a password, and one that
    /// cannot be read, is kept whole.
    #[must_use]
    pub fn parse(connection_string: &str) -> Self {
        let split = if let Some(rest) = connection_string.strip_prefix("postgres:") {
            if is_postgres_uri(rest) {
                postgres_uri(rest).and_then(|pairs| without_password(&pairs, &["password"]))
            } else {
                libpq_pairs(rest).and_then(|pairs| without_password_in(rest, &pairs, &["password"]))
            }
            .map(|(path, password)| {
                (
                    format!("postgres:{path}"),
                    MetadataDatabase::Postgres,
                    password,
                )
            })
        } else if let Some(rest) = connection_string.strip_prefix("mysql:") {
            mysql_pairs(rest)
                .and_then(|pairs| without_password_in(rest, &pairs, &["password", "passwd"]))
                .map(|(path, password)| {
                    (format!("mysql:{path}"), MetadataDatabase::Mysql, password)
                })
        } else {
            None
        };
        match split {
            Some((path, database, password)) => Self {
                path,
                password: Some((database, password)),
            },
            None => Self {
                path: connection_string.to_string(),
                password: None,
            },
        }
    }
}

/// One `key = value` pair of a connection string: its key, its value without
/// quotes or escapes, and the bytes it spans.
#[derive(Debug)]
struct Pair {
    key: String,
    value: String,
    span: Range<usize>,
}

/// The string without its password pairs, kept as written, and the password,
/// the last one as the database reads it. `None` when there is no password.
fn without_password_in(s: &str, pairs: &[Pair], keys: &[&str]) -> Option<(String, String)> {
    let is_password = |pair: &Pair| keys.iter().any(|key| pair.key.eq_ignore_ascii_case(key));
    let password = pairs
        .iter()
        .rev()
        .find(|pair| is_password(pair))?
        .value
        .clone();
    if password.is_empty() {
        return None;
    }
    let path = pairs
        .iter()
        .filter(|pair| !is_password(pair))
        .map(|pair| &s[pair.span.clone()])
        .collect::<Vec<_>>()
        .join(" ");
    Some((path, password))
}

/// `pairs`, written as libpq pairs, without the password, and the password.
fn without_password(pairs: &[(String, String)], keys: &[&str]) -> Option<(String, String)> {
    let is_password = |key: &str| keys.iter().any(|k| key.eq_ignore_ascii_case(k));
    let password = pairs
        .iter()
        .rev()
        .find(|(key, _)| is_password(key))?
        .1
        .clone();
    if password.is_empty() {
        return None;
    }
    let path = pairs
        .iter()
        .filter(|(key, _)| !is_password(key))
        .map(|(key, value)| {
            format!(
                "{key}='{}'",
                value.replace('\\', "\\\\").replace('\'', "\\'")
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    Some((path, password))
}

fn is_postgres_uri(s: &str) -> bool {
    let lower = s.get(..13).map(str::to_ascii_lowercase);
    lower.as_deref() == Some("postgresql://")
        || s.get(..11).map(str::to_ascii_lowercase).as_deref() == Some("postgres://")
}

/// The pairs of a libpq `key = value` string: a value may be quoted with `'`,
/// and a backslash escapes the next character, quoted or not. `None` when the
/// string is not one.
fn libpq_pairs(s: &str) -> Option<Vec<Pair>> {
    pairs(s, '\'', true)
}

/// The pairs of a `MySQL` extension's string: a value may be quoted with `"`,
/// inside which a backslash escapes `\` or `"`. `None` when the string is not
/// one.
fn mysql_pairs(s: &str) -> Option<Vec<Pair>> {
    pairs(s, '"', false)
}

fn pairs(s: &str, quote: char, escape_unquoted: bool) -> Option<Vec<Pair>> {
    let mut chars = s.char_indices().peekable();
    let mut result = Vec::new();
    loop {
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let Some(&(start, _)) = chars.peek() else {
            return Some(result);
        };
        let mut key = String::new();
        while let Some((_, c)) = chars.next_if(|(_, c)| *c != '=' && !c.is_whitespace()) {
            key.push(c);
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        if key.is_empty() || chars.next_if(|(_, c)| *c == '=').is_none() {
            return None;
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        let end = if chars.next_if(|(_, c)| *c == quote).is_some() {
            loop {
                match chars.next()? {
                    (_, '\\') => value.push(chars.next()?.1),
                    (i, c) if c == quote => break i + c.len_utf8(),
                    (_, c) => value.push(c),
                }
            }
        } else {
            let mut end = s.len();
            while let Some(&(i, c)) = chars.peek() {
                if c.is_whitespace() {
                    end = i;
                    break;
                }
                chars.next();
                if c == '\\' && escape_unquoted {
                    value.push(chars.next()?.1);
                } else {
                    value.push(c);
                }
            }
            end
        };
        result.push(Pair {
            key,
            value,
            span: start..end,
        });
    }
}

/// The pairs a libpq URI sets, in libpq's names, from
/// `postgresql://[user[:password]@][host][:port][,...][/dbname][?param=value&...]`,
/// with each part percent-decoded. `None` when it is not one.
fn postgres_uri(s: &str) -> Option<Vec<(String, String)>> {
    let rest = s.split_once("://")?.1;
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, dbname) = rest.split_once('/').unwrap_or((rest, ""));
    let (userinfo, hosts) = match authority.split_once('@') {
        Some((userinfo, hosts)) => (Some(userinfo), hosts),
        None => (None, authority),
    };
    let mut pairs = Vec::new();
    if let Some(userinfo) = userinfo {
        let (user, password) = match userinfo.split_once(':') {
            Some((user, password)) => (user, Some(password)),
            None => (userinfo, None),
        };
        if !user.is_empty() {
            pairs.push(("user".to_string(), percent_decoded(user)?));
        }
        if let Some(password) = password {
            pairs.push(("password".to_string(), percent_decoded(password)?));
        }
    }
    let mut host_list = Vec::new();
    let mut port_list = Vec::new();
    if !hosts.is_empty() {
        for spec in hosts.split(',') {
            let (host, port) = if let Some(bracketed) = spec.strip_prefix('[') {
                let (host, after) = bracketed.split_once(']')?;
                let port = match after {
                    "" => "",
                    port => port.strip_prefix(':')?,
                };
                (host, port)
            } else {
                spec.split_once(':').unwrap_or((spec, ""))
            };
            host_list.push(percent_decoded(host)?);
            port_list.push(percent_decoded(port)?);
        }
    }
    if host_list.iter().any(|host| !host.is_empty()) {
        pairs.push(("host".to_string(), host_list.join(",")));
    }
    if port_list.iter().any(|port| !port.is_empty()) {
        pairs.push(("port".to_string(), port_list.join(",")));
    }
    if !dbname.is_empty() {
        pairs.push(("dbname".to_string(), percent_decoded(dbname)?));
    }
    for param in query.split('&').filter(|param| !param.is_empty()) {
        let (key, value) = param.split_once('=')?;
        let (key, value) = (percent_decoded(key)?, percent_decoded(value)?);
        // As libpq reads it, for JDBC's sake.
        if key == "ssl" && value == "true" {
            pairs.push(("sslmode".to_string(), "require".to_string()));
        } else {
            pairs.push((key, value));
        }
    }
    Some(pairs)
}

fn percent_decoded(s: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(s.len());
    let mut iter = s.bytes();
    while let Some(byte) = iter.next() {
        if byte == b'%' {
            let hex = [iter.next()?, iter.next()?];
            bytes.push(u8::from_str_radix(std::str::from_utf8(&hex).ok()?, 16).ok()?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(s: &str) -> (String, Option<String>) {
        let connection = MetadataConnection::parse(s);
        (
            connection.path,
            connection.password.map(|(_, password)| password),
        )
    }

    /// A libpq `key = value` string loses its password wherever it is, quoted
    /// or not, and keeps the rest as written.
    #[test]
    fn a_libpq_string_loses_its_password() {
        assert_eq!(
            split("postgres:password=hunter2 dbname=x host=127.0.0.1 port=1 user=u"),
            (
                "postgres:dbname=x host=127.0.0.1 port=1 user=u".to_string(),
                Some("hunter2".to_string())
            )
        );
        assert_eq!(
            split(r"postgres:dbname=x host='db host' password = 'it\'s' user=u"),
            (
                "postgres:dbname=x host='db host' user=u".to_string(),
                Some("it's".to_string())
            )
        );
        assert_eq!(
            split(r"postgres:dbname=x password='a \'quoted\' \\ word' user=u"),
            (
                "postgres:dbname=x user=u".to_string(),
                Some(r"a 'quoted' \ word".to_string())
            )
        );
        assert_eq!(
            split("postgres:dbname=x user=u password=first password=second"),
            (
                "postgres:dbname=x user=u".to_string(),
                Some("second".to_string())
            )
        );
    }

    /// A URI is rewritten as libpq pairs without its password, from its user
    /// information or its query, percent-decoded.
    #[test]
    fn a_libpq_uri_becomes_pairs_without_its_password() {
        assert_eq!(
            split("postgres:postgresql://u:hunter%402@127.0.0.1:1/x"),
            (
                "postgres:user='u' host='127.0.0.1' port='1' dbname='x'".to_string(),
                Some("hunter@2".to_string())
            )
        );
        assert_eq!(
            split("postgres:postgres://u@[::1]:5432,db2/x?password=s%20t&sslmode=require&ssl=true"),
            (
                "postgres:user='u' host='::1,db2' port='5432,' dbname='x' sslmode='require' sslmode='require'"
                    .to_string(),
                Some("s t".to_string())
            )
        );
        assert_eq!(
            split("postgres:postgresql:///x?host=%2Fvar%2Frun%2Fpostgresql&password=it's"),
            (
                r"postgres:dbname='x' host='/var/run/postgresql'".to_string(),
                Some("it's".to_string())
            )
        );
    }

    /// A `MySQL` string loses its password under either name.
    #[test]
    fn a_mysql_string_loses_its_password() {
        assert_eq!(
            split(r#"mysql:host=localhost passwd="a \"b\" c" db=meta"#),
            (
                "mysql:host=localhost db=meta".to_string(),
                Some(r#"a "b" c"#.to_string())
            )
        );
        assert_eq!(
            split("mysql:password=hunter2 host=localhost"),
            (
                "mysql:host=localhost".to_string(),
                Some("hunter2".to_string())
            )
        );
    }

    /// A string without a password, one that cannot be read, and any other
    /// database's string are kept whole.
    #[test]
    fn other_strings_are_kept_whole() {
        for s in [
            "postgres:dbname=x host=127.0.0.1",
            "postgres:postgresql://u@127.0.0.1/x",
            "postgres:dbname=x password='unterminated",
            "postgres:dbname=x password",
            "postgres:dbname=x password='it''s'",
            "postgres:password=",
            "metadata.ducklake",
            "sqlite:metadata.sqlite",
            "md:my_db",
            "mysql:host=localhost",
        ] {
            assert_eq!(split(s), (s.to_string(), None), "{s}");
        }
    }
}
