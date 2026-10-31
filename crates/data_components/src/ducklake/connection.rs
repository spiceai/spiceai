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

/// Why a `PostgreSQL` or `MySQL` metadata connection string cannot be read:
/// its password could not be held apart, so it is not attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unreadable {
    pub database: MetadataDatabase,
    /// What is wrong with it, worded to follow "it has".
    pub reason: &'static str,
}

impl MetadataConnection {
    /// Split `connection_string`. The prefix that names the database is read
    /// as `DuckDB` reads it: in any case, by the extension's name or alias. A
    /// `PostgreSQL` string may be `key = value` pairs or a URI, which is
    /// rewritten as pairs, since the extension puts a secret's fields in front
    /// of the path as pairs; a `MySQL` string may be pairs. Any other string,
    /// and one without a password, is kept whole.
    ///
    /// # Errors
    ///
    /// Why a `PostgreSQL` or `MySQL` string cannot be read.
    pub fn parse(connection_string: &str) -> Result<Self, Unreadable> {
        let whole = || Self {
            path: connection_string.to_string(),
            password: None,
        };
        // A bare libpq URI names a PostgreSQL database; `DuckDB` attaches it
        // only behind the `postgres:` prefix, which it is given.
        if is_postgres_uri(connection_string) {
            let unreadable = |reason| Unreadable {
                database: MetadataDatabase::Postgres,
                reason,
            };
            let pairs = postgres_uri(connection_string).map_err(unreadable)?;
            return Ok(match without_password(&pairs, &["password"]) {
                Some((path, password)) => Self {
                    path: format!("postgres:{path}"),
                    password: password.map(|password| (MetadataDatabase::Postgres, password)),
                },
                None => Self {
                    path: format!("postgres:{connection_string}"),
                    password: None,
                },
            });
        }
        let Some((prefix, rest)) = connection_string.split_once(':') else {
            return Ok(whole());
        };
        let database = match prefix.to_ascii_lowercase().as_str() {
            "postgres" | "postgres_scanner" => MetadataDatabase::Postgres,
            "mysql" | "mysql_scanner" => MetadataDatabase::Mysql,
            _ => return Ok(whole()),
        };
        let unreadable = |reason| Unreadable { database, reason };
        let split = match database {
            MetadataDatabase::Postgres if is_postgres_uri(rest) => {
                without_password(&postgres_uri(rest).map_err(unreadable)?, &["password"])
            }
            MetadataDatabase::Postgres => {
                without_password_in(rest, &libpq_pairs(rest).map_err(unreadable)?, &["password"])
            }
            MetadataDatabase::Mysql => without_password_in(
                rest,
                &mysql_pairs(rest).map_err(unreadable)?,
                &["password", "passwd"],
            ),
        };
        Ok(match split {
            Some((path, password)) => Self {
                path: format!("{prefix}:{path}"),
                password: password.map(|password| (database, password)),
            },
            None => whole(),
        })
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
/// the last one as the database reads it, or `None` when that is empty: an
/// earlier password it overrides is no less a secret, and leaves the string
/// too. `None` when no password pair holds a value.
fn without_password_in(s: &str, pairs: &[Pair], keys: &[&str]) -> Option<(String, Option<String>)> {
    let is_password = |pair: &Pair| keys.iter().any(|key| pair.key.eq_ignore_ascii_case(key));
    if !pairs
        .iter()
        .any(|pair| is_password(pair) && !pair.value.is_empty())
    {
        return None;
    }
    let password = pairs
        .iter()
        .rev()
        .find(|pair| is_password(pair))?
        .value
        .clone();
    let path = pairs
        .iter()
        .filter(|pair| !is_password(pair))
        .map(|pair| &s[pair.span.clone()])
        .collect::<Vec<_>>()
        .join(" ");
    Some((path, (!password.is_empty()).then_some(password)))
}

/// `pairs`, written as libpq pairs, without the password pairs, and the
/// password, as [`without_password_in`] gives them.
fn without_password(pairs: &[(String, String)], keys: &[&str]) -> Option<(String, Option<String>)> {
    let is_password = |key: &str| keys.iter().any(|k| key.eq_ignore_ascii_case(k));
    if !pairs
        .iter()
        .any(|(key, value)| is_password(key) && !value.is_empty())
    {
        return None;
    }
    let password = pairs
        .iter()
        .rev()
        .find(|(key, _)| is_password(key))?
        .1
        .clone();
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
    Some((path, (!password.is_empty()).then_some(password)))
}

fn is_postgres_uri(s: &str) -> bool {
    let lower = s.get(..13).map(str::to_ascii_lowercase);
    lower.as_deref() == Some("postgresql://")
        || s.get(..11).map(str::to_ascii_lowercase).as_deref() == Some("postgres://")
}

const UNTERMINATED_QUOTE: &str = "an unterminated quote";
const MISSING_EQUALS: &str = "a parameter without `=`";
const TRAILING_BACKSLASH: &str = "a `\\` at its end";

/// The pairs of a libpq `key = value` string: a value may be quoted with `'`,
/// and a backslash escapes the next character, quoted or not.
fn libpq_pairs(s: &str) -> Result<Vec<Pair>, &'static str> {
    pairs(s, '\'', true)
}

/// The pairs of a `MySQL` extension's string: a value may be quoted with `"`,
/// inside which a backslash escapes `\` or `"`.
fn mysql_pairs(s: &str) -> Result<Vec<Pair>, &'static str> {
    pairs(s, '"', false)
}

fn pairs(s: &str, quote: char, escape_unquoted: bool) -> Result<Vec<Pair>, &'static str> {
    let mut chars = s.char_indices().peekable();
    let mut result = Vec::new();
    loop {
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let Some(&(start, _)) = chars.peek() else {
            return Ok(result);
        };
        let mut key = String::new();
        while let Some((_, c)) = chars.next_if(|(_, c)| *c != '=' && !c.is_whitespace()) {
            key.push(c);
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        if key.is_empty() || chars.next_if(|(_, c)| *c == '=').is_none() {
            return Err(MISSING_EQUALS);
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        let end = if chars.next_if(|(_, c)| *c == quote).is_some() {
            loop {
                match chars.next().ok_or(UNTERMINATED_QUOTE)? {
                    (_, '\\') => value.push(chars.next().ok_or(UNTERMINATED_QUOTE)?.1),
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
                    value.push(chars.next().ok_or(TRAILING_BACKSLASH)?.1);
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

const BAD_PERCENT: &str =
    "a `%` that is not followed by two hex digits, or that encodes bytes that are not UTF-8";
const NUL_BYTE: &str = "a `%00`, which encodes a byte no connection string can hold";
const BAD_HOST: &str = "a host in `[` without its `]`, or with text after `]` that is not a port";
const BAD_QUERY: &str = "a query parameter without `=`";

/// The pairs a libpq URI sets, in libpq's names, from
/// `postgresql://[user[:password]@][host][:port][,...][/dbname][?param=value&...]`,
/// with each part percent-decoded.
fn postgres_uri(s: &str) -> Result<Vec<(String, String)>, &'static str> {
    let rest = s.split_once("://").map_or(s, |(_, rest)| rest);
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
                let (host, after) = bracketed.split_once(']').ok_or(BAD_HOST)?;
                let port = match after {
                    "" => "",
                    port => port.strip_prefix(':').ok_or(BAD_HOST)?,
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
        let (key, value) = param.split_once('=').ok_or(BAD_QUERY)?;
        let (key, value) = (percent_decoded(key)?, percent_decoded(value)?);
        // As libpq reads it, for JDBC's sake.
        if key == "ssl" && value == "true" {
            pairs.push(("sslmode".to_string(), "require".to_string()));
        } else {
            pairs.push((key, value));
        }
    }
    Ok(pairs)
}

fn percent_decoded(s: &str) -> Result<String, &'static str> {
    let mut bytes = Vec::with_capacity(s.len());
    let mut iter = s.bytes();
    while let Some(byte) = iter.next() {
        if byte == b'%' {
            let hex = [
                iter.next().ok_or(BAD_PERCENT)?,
                iter.next().ok_or(BAD_PERCENT)?,
            ];
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return Err(BAD_PERCENT);
            }
            let hex = std::str::from_utf8(&hex).map_err(|_| BAD_PERCENT)?;
            match u8::from_str_radix(hex, 16).map_err(|_| BAD_PERCENT)? {
                0 => return Err(NUL_BYTE),
                byte => bytes.push(byte),
            }
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).map_err(|_| BAD_PERCENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(s: &str) -> (String, Option<String>) {
        let connection = MetadataConnection::parse(s).expect("readable");
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

    /// A password emptied by a later password pair leaves the string with
    /// neither: the database reads the empty one, and the earlier one is no
    /// less a secret. With no secret to give, none is given.
    #[test]
    fn a_password_emptied_later_leaves_the_string() {
        for (s, path) in [
            (
                "postgres:dbname=x user=u password=hunter2 password=",
                "postgres:dbname=x user=u",
            ),
            (
                "postgres:dbname=x password=hunter2 user=u password=''",
                "postgres:dbname=x user=u",
            ),
            (
                "postgres:postgresql://u:hunter2@127.0.0.1:1/x?password=",
                "postgres:user='u' host='127.0.0.1' port='1' dbname='x'",
            ),
            (
                "postgresql://u:hunter2@127.0.0.1:1/x?password=",
                "postgres:user='u' host='127.0.0.1' port='1' dbname='x'",
            ),
            (
                "mysql:host=localhost password=hunter2 passwd=",
                "mysql:host=localhost",
            ),
        ] {
            assert_eq!(split(s), (path.to_string(), None), "{s}");
        }
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

    /// The prefix is read as `DuckDB` reads it, in any case and by the
    /// extension's alias, and kept as written.
    #[test]
    fn a_prefix_in_any_case_or_by_alias_is_read() {
        for prefix in ["Postgres", "POSTGRES", "postgres_scanner"] {
            assert_eq!(
                split(&format!("{prefix}:dbname=x password=hunter2")),
                (format!("{prefix}:dbname=x"), Some("hunter2".to_string())),
                "{prefix}"
            );
        }
        assert_eq!(
            split("MYSQL_SCANNER:host=localhost password=hunter2"),
            (
                "MYSQL_SCANNER:host=localhost".to_string(),
                Some("hunter2".to_string())
            )
        );
    }

    /// A `PostgreSQL` or `MySQL` string that cannot be read is refused, saying
    /// why, rather than attached whole, password included.
    #[test]
    fn an_unreadable_string_is_refused() {
        for (s, database, reason) in [
            (
                "postgres:dbname=x password='unterminated",
                MetadataDatabase::Postgres,
                UNTERMINATED_QUOTE,
            ),
            (
                "postgres:dbname=x password",
                MetadataDatabase::Postgres,
                MISSING_EQUALS,
            ),
            (
                "postgres:dbname=x password='it''s'",
                MetadataDatabase::Postgres,
                MISSING_EQUALS,
            ),
            (
                "postgres:dbname=x password=a\\",
                MetadataDatabase::Postgres,
                TRAILING_BACKSLASH,
            ),
            (
                "postgres:postgresql://u:hunter%zz@127.0.0.1/x",
                MetadataDatabase::Postgres,
                BAD_PERCENT,
            ),
            (
                "postgres:postgresql://u:hunter%+5@127.0.0.1/x",
                MetadataDatabase::Postgres,
                BAD_PERCENT,
            ),
            (
                "postgres:postgresql://u:hunter2@[::1/x",
                MetadataDatabase::Postgres,
                BAD_HOST,
            ),
            (
                "postgresql://u:hunter%00@127.0.0.1/x",
                MetadataDatabase::Postgres,
                NUL_BYTE,
            ),
            (
                "postgres:postgresql://u@h/x?password",
                MetadataDatabase::Postgres,
                BAD_QUERY,
            ),
            (
                "mysql:mysql://root:hunter2@localhost/meta",
                MetadataDatabase::Mysql,
                MISSING_EQUALS,
            ),
        ] {
            assert_eq!(
                MetadataConnection::parse(s),
                Err(Unreadable { database, reason }),
                "{s}"
            );
        }
    }

    /// A bare libpq URI, which `DuckDB` attaches only behind `postgres:`, is
    /// given the prefix, and loses its password as a prefixed one does.
    #[test]
    fn a_bare_uri_is_read_as_postgres() {
        for scheme in ["postgres", "postgresql", "PostgreSQL"] {
            assert_eq!(
                split(&format!("{scheme}://u:hunter2@127.0.0.1:1/x")),
                (
                    "postgres:user='u' host='127.0.0.1' port='1' dbname='x'".to_string(),
                    Some("hunter2".to_string())
                ),
                "{scheme}"
            );
        }
        assert_eq!(
            split("postgres://u@127.0.0.1:1/x"),
            ("postgres:postgres://u@127.0.0.1:1/x".to_string(), None)
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

    /// A string without a password, and any other database's string, is
    /// kept whole.
    #[test]
    fn other_strings_are_kept_whole() {
        for s in [
            "postgres:dbname=x host=127.0.0.1",
            "postgres:postgresql://u@127.0.0.1/x",
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
