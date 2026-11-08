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

//! Connection pool settings for the `MySQL` data connector and catalog.
//!
//! Each setting comes from the Spicepod parameter when the user set one, else from the same
//! setting in the `connection_string` URL's query, else from its default. A setting given in
//! both places with different values is a configuration error, so that neither silently wins.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use datafusion_table_providers::sql::db_connection_pool::mysqlpool::{
    self, MySQLConnectionPool, MySQLServerIdentity,
};
use mysql_async::{PoolConstraints, PoolOpts};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;

const POOL_DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/mysql/deployment#connection-pool-sizing";

/// The `connection_max_lifetime` parameter's description, shared by the dataset connector and
/// the catalog.
pub const CONNECTION_MAX_LIFETIME_DESCRIPTION: &str = "How long a pooled connection is used before it is closed and replaced, such as '5m'. \
     Replacing connections makes the pool resolve the host again, so it spreads across \
     the servers behind a load-balanced hostname such as a reader endpoint. Each \
     connection's lifetime is extended by a random amount of up to a fifth of it, so \
     connections opened together are not replaced together. A connection is not closed \
     while a query is using it. '0s' keeps connections open indefinitely. Default: 5m.";

/// So that a pool re-resolves its host and spreads across the servers behind it.
const DEFAULT_CONNECTION_MAX_LIFETIME: Duration = Duration::from_mins(5);

/// The longest accepted lifetime or jitter. A connection's deadline is computed as an
/// `Instant`, which a value such as `inf` would overflow.
const MAX_CONNECTION_MAX_LIFETIME: Duration = Duration::from_hours(365 * 24);

/// Each connection's lifetime is extended by a random amount of up to this fraction of it, so
/// connections opened together are replaced at different times. Without it, a pool's connections
/// would all reconnect within one DNS cache window and reach the same server again.
const LIFETIME_JITTER_DIVISOR: u32 = 5;

/// After a pool's connection to a server is logged at INFO, its later connections to that
/// server are logged at DEBUG for this long.
const SERVER_LOG_INTERVAL: Duration = Duration::from_mins(10);

/// How long a pool keeps an idle connection above its minimum before closing it.
/// `mysql_async` resets a returned connection before it can be used again, and a query that
/// starts meanwhile opens another connection. Were that connection closed as soon as it was
/// returned, queries that follow each other closely would open a connection every other query.
pub const DEFAULT_IDLE_CONNECTION_TTL: Duration = Duration::from_secs(30);

/// How often a pool closes its idle connections that are past their idle time or lifetime.
const DEFAULT_IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// The `connection_string` query keys `mysql_async` reads for the settings resolved here.
const URL_POOL_MIN: &str = "pool_min";
const URL_POOL_MAX: &str = "pool_max";
const URL_MAX_LIFETIME: &str = "abs_conn_ttl";
const URL_MAX_LIFETIME_JITTER: &str = "abs_conn_ttl_jitter";
const URL_IDLE_TTL: &str = "inactive_connection_ttl";
const URL_IDLE_CHECK_INTERVAL: &str = "ttl_check_interval";
const URL_POOL_KEYS: [&str; 4] = [
    URL_POOL_MIN,
    URL_POOL_MAX,
    URL_MAX_LIFETIME,
    URL_MAX_LIFETIME_JITTER,
];

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display(
        "`mysql_{key}` must be a whole number of connections, such as `5`, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidPoolSize { key: &'static str, value: String },

    #[snafu(display(
        "`{url_key}` in `mysql_connection_string` must be a whole number{bound}, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidConnectionStringValue {
        url_key: &'static str,
        bound: &'static str,
        value: String,
    },

    #[snafu(display("The pool maximum ({max}) must be at least 1. See: {POOL_DOCS}"))]
    ZeroPoolMax { max: String },

    #[snafu(display(
        "The pool minimum ({min}) is greater than the pool maximum ({max}). Lower `mysql_pool_min` or raise `mysql_pool_max`. See: {POOL_DOCS}"
    ))]
    MinGreaterThanMax { min: String, max: String },

    #[snafu(display(
        "`mysql_connection_max_lifetime` must be a duration from 1s to 365d, such as `5m`, or `0s` to keep connections open indefinitely, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidMaxLifetime { value: String },

    #[snafu(display(
        "`mysql_{key}` is '{value}', but `mysql_connection_string` sets `{url_value}`. Set it in one place. See: {POOL_DOCS}"
    ))]
    Conflict {
        key: &'static str,
        value: String,
        url_value: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The pool settings as the user gave them: the Spicepod parameters the user set (`None` for
/// one left at its default) and the connection string, if any.
#[derive(Debug, Default, Clone, Copy)]
pub struct PoolParams<'a> {
    pub pool_min: Option<&'a str>,
    pub pool_max: Option<&'a str>,
    pub connection_max_lifetime: Option<&'a str>,
    pub connection_string: Option<&'a str>,
}

/// The pool settings used where neither the Spicepod nor the connection string sets them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolDefaults {
    pub min: usize,
    pub max: usize,
    /// How long an idle connection above `min` is kept before it is closed.
    pub idle_ttl: Duration,
}

/// The resolved pool settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSettings {
    /// The minimum and maximum pool size. `None` keeps the size the connection parameters
    /// give, for a component that has no pool size parameters.
    pub size: Option<PoolConstraints>,
    /// How long a connection is used before it is replaced. `None` keeps it indefinitely.
    pub max_lifetime: Option<Duration>,
    /// The upper bound of the random amount added to each connection's lifetime.
    pub max_lifetime_jitter: Option<Duration>,
    /// How long an idle connection above the pool minimum is kept before it is closed.
    pub idle_ttl: Duration,
    /// How often idle connections are checked against their idle time and lifetime.
    pub idle_check_interval: Duration,
}

/// A resolved pool bound, and where it came from, for an error to name.
struct Bound {
    value: usize,
    /// Set by the user, in the Spicepod or the connection string.
    explicit: bool,
    origin: String,
}

impl PoolSettings {
    /// Resolves the pool settings from the Spicepod parameters, the connection string, and
    /// the defaults. `defaults` is `None` for a component that has no pool size parameters,
    /// whose pool keeps the size the connection parameters give and the default idle time.
    ///
    /// # Errors
    ///
    /// Returns an error when a value is invalid, when the pool minimum exceeds a maximum the
    /// user set, or when a setting is given in both the Spicepod and the connection string
    /// with different values.
    pub fn resolve(params: PoolParams<'_>, defaults: Option<PoolDefaults>) -> Result<Self> {
        let url = UrlPoolSettings::parse(params.connection_string)?;

        let size = match defaults {
            Some(defaults) => {
                let min = resolve_bound(URL_POOL_MIN, params.pool_min, url.pool_min, defaults.min)?;
                let mut max =
                    resolve_bound(URL_POOL_MAX, params.pool_max, url.pool_max, defaults.max)?;
                // A maximum nobody set makes room for the minimum the user asked for.
                if !max.explicit && max.value < min.value {
                    max.value = min.value;
                    max.origin = format!("{}, the default raised to the minimum", max.value);
                }
                ensure!(max.value > 0, ZeroPoolMaxSnafu { max: max.origin });
                Some(PoolConstraints::new(min.value, max.value).context(
                    MinGreaterThanMaxSnafu {
                        min: min.origin,
                        max: max.origin,
                    },
                )?)
            }
            None => None,
        };

        let (max_lifetime, max_lifetime_jitter) = match params.connection_max_lifetime {
            Some(value) => {
                let lifetime = parse_max_lifetime(value)?;
                if let Some(url_ttl) = url.max_lifetime
                    && Some(Duration::from_secs(url_ttl)) != lifetime
                {
                    return ConflictSnafu {
                        key: "connection_max_lifetime",
                        value,
                        url_value: format!("{URL_MAX_LIFETIME}={url_ttl}"),
                    }
                    .fail();
                }
                (lifetime, jitter_for(lifetime, url.max_lifetime_jitter))
            }
            // A lifetime set in the URL is taken as `mysql_async` reads it, jitter included.
            None if url.max_lifetime.is_some() => (
                url.max_lifetime.map(Duration::from_secs),
                url.max_lifetime_jitter.map(Duration::from_secs),
            ),
            None => {
                let lifetime = Some(DEFAULT_CONNECTION_MAX_LIFETIME);
                (lifetime, jitter_for(lifetime, url.max_lifetime_jitter))
            }
        };

        let idle_ttl = match url.idle_ttl {
            Some(seconds) => Duration::from_secs(seconds),
            None => defaults.map_or(DEFAULT_IDLE_CONNECTION_TTL, |defaults| defaults.idle_ttl),
        };
        let idle_check_interval = url
            .idle_check_interval
            .map_or(DEFAULT_IDLE_CHECK_INTERVAL, Duration::from_secs);

        Ok(Self {
            size,
            max_lifetime,
            max_lifetime_jitter,
            idle_ttl,
            idle_check_interval,
        })
    }

    fn apply(self, pool_opts: PoolOpts) -> PoolOpts {
        let pool_opts = match self.size {
            Some(size) => pool_opts.with_constraints(size),
            None => pool_opts,
        };
        pool_opts
            .with_abs_conn_ttl(self.max_lifetime)
            .with_abs_conn_ttl_jitter(self.max_lifetime_jitter)
            .with_inactive_connection_ttl(self.idle_ttl)
            .with_ttl_check_interval(self.idle_check_interval)
    }
}

/// Opens a pool with these settings, logging the server each new connection reached for
/// `component` (such as `dataset 'orders'`). The pool settings are removed from a
/// `connection_string` first: they were resolved into `settings`, and `mysql_async` would
/// otherwise check the URL's `pool_min` and `pool_max` against its own defaults, rejecting a
/// `pool_max` below 10 given without a `pool_min`.
///
/// # Errors
///
/// Returns the pool's error when it cannot connect.
pub async fn connect(
    params: impl IntoIterator<Item = (String, SecretString)>,
    settings: PoolSettings,
    component: String,
) -> std::result::Result<MySQLConnectionPool, mysqlpool::Error> {
    let mut params: HashMap<String, SecretString> = params.into_iter().collect();
    if settings.size.is_some() {
        for key in ["connection_string", "mysql_connection_string"] {
            if let Some(connection_string) = params.get_mut(key) {
                let without = without_pool_settings(connection_string.expose_secret());
                *connection_string = SecretString::from(without);
            }
        }
    }
    let log = ConnectionLog {
        component,
        logged_at: parking_lot::Mutex::new(HashMap::new()),
    };
    MySQLConnectionPool::new_with_pool_opts_and_observer(
        params,
        move |pool_opts| settings.apply(pool_opts),
        move |identity| log.log(identity),
    )
    .await
}

/// Logs the server each new pooled connection reached: at INFO the first time in
/// [`SERVER_LOG_INTERVAL`] that the pool reaches that server, at DEBUG otherwise. A pool opens
/// a connection per query while concurrency is above its minimum, so a line per connection
/// would be a line per query; a line per server still names every server in use.
struct ConnectionLog {
    component: String,
    logged_at: parking_lot::Mutex<HashMap<String, Instant>>,
}

impl ConnectionLog {
    fn log(&self, identity: &MySQLServerIdentity) {
        let message = new_connection_message(&self.component, identity);
        let server = format!(
            "{}:{} {}",
            identity.hostname,
            identity.port,
            identity.aurora_server_id.as_deref().unwrap_or_default()
        );
        if self.first_in_interval(&server, Instant::now()) {
            tracing::info!("{message}");
        } else {
            tracing::debug!("{message}");
        }
    }

    /// Whether `server` has not been logged at INFO within [`SERVER_LOG_INTERVAL`] before
    /// `now`, recording it if so.
    fn first_in_interval(&self, server: &str, now: Instant) -> bool {
        let mut logged_at = self.logged_at.lock();
        logged_at.retain(|_, at| now.saturating_duration_since(*at) < SERVER_LOG_INTERVAL);
        if logged_at.contains_key(server) {
            return false;
        }
        logged_at.insert(server.to_string(), now);
        true
    }
}

/// The log line for a new pooled connection, naming the server it reached. The server's own
/// strings are escaped, so the line stays one line whatever the server reports.
fn new_connection_message(component: &str, identity: &MySQLServerIdentity) -> String {
    let aurora = identity
        .aurora_server_id
        .as_deref()
        .map(|instance| format!(" (Aurora instance '{}')", instance.escape_debug()))
        .unwrap_or_default();
    format!(
        "MySQL {component} opened connection {id} to '{host}:{port}', which reached server '{server}:{server_port}'{aurora}.",
        id = identity.connection_id,
        host = identity.configured_host.escape_debug(),
        port = identity.configured_port,
        server = identity.hostname.escape_debug(),
        server_port = identity.port,
    )
}

/// The connection string with the pool settings removed from its query. A string that does
/// not parse is returned as it is, for the pool to report.
fn without_pool_settings(connection_string: &str) -> String {
    let Ok(mut url) = url::Url::parse(connection_string) else {
        return connection_string.to_string();
    };
    let kept: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(key, _)| !URL_POOL_KEYS.contains(&key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    if kept.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(kept);
    }
    url.into()
}

/// The settings resolved here that the connection string's query sets, read the way
/// `mysql_async` reads them: the last occurrence of a key wins.
#[derive(Debug, Default)]
struct UrlPoolSettings {
    pool_min: Option<usize>,
    pool_max: Option<usize>,
    max_lifetime: Option<u64>,
    max_lifetime_jitter: Option<u64>,
    idle_ttl: Option<u64>,
    idle_check_interval: Option<u64>,
}

impl UrlPoolSettings {
    fn parse(connection_string: Option<&str>) -> Result<Self> {
        let mut settings = Self::default();
        // A connection string that does not parse is reported when the connection is opened,
        // with the error `mysql_async` gives for it.
        let Some(url) = connection_string.and_then(|s| url::Url::parse(s).ok()) else {
            return Ok(settings);
        };
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                URL_POOL_MIN => settings.pool_min = Some(parse_url_size(URL_POOL_MIN, &value)?),
                URL_POOL_MAX => settings.pool_max = Some(parse_url_size(URL_POOL_MAX, &value)?),
                URL_MAX_LIFETIME => {
                    settings.max_lifetime = Some(parse_url_seconds(URL_MAX_LIFETIME, &value)?);
                }
                URL_MAX_LIFETIME_JITTER => {
                    settings.max_lifetime_jitter =
                        Some(parse_url_seconds(URL_MAX_LIFETIME_JITTER, &value)?);
                }
                URL_IDLE_TTL => settings.idle_ttl = Some(parse_url_seconds(URL_IDLE_TTL, &value)?),
                URL_IDLE_CHECK_INTERVAL => {
                    settings.idle_check_interval =
                        Some(parse_url_seconds(URL_IDLE_CHECK_INTERVAL, &value)?);
                }
                _ => {}
            }
        }
        Ok(settings)
    }
}

fn parse_url_size(url_key: &'static str, value: &str) -> Result<usize> {
    value
        .parse()
        .ok()
        .context(InvalidConnectionStringValueSnafu {
            url_key,
            bound: "",
            value,
        })
}

/// Seconds, at most 365 days, which keeps a connection's deadline from overflowing.
fn parse_url_seconds(url_key: &'static str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds <= MAX_CONNECTION_MAX_LIFETIME.as_secs())
        .context(InvalidConnectionStringValueSnafu {
            url_key,
            bound: " of seconds up to 31536000 (365d)",
            value,
        })
}

fn resolve_bound(
    key: &'static str,
    spicepod: Option<&str>,
    url: Option<usize>,
    default: usize,
) -> Result<Bound> {
    match (spicepod, url) {
        (Some(value), url) => {
            let parsed = value
                .trim()
                .parse::<usize>()
                .ok()
                .context(InvalidPoolSizeSnafu { key, value })?;
            if let Some(url_value) = url
                && url_value != parsed
            {
                return ConflictSnafu {
                    key,
                    value,
                    url_value: format!("{key}={url_value}"),
                }
                .fail();
            }
            Ok(Bound {
                value: parsed,
                explicit: true,
                origin: format!("{parsed}, from `mysql_{key}`"),
            })
        }
        (None, Some(url_value)) => Ok(Bound {
            value: url_value,
            explicit: true,
            origin: format!("{url_value}, from `{key}` in `mysql_connection_string`"),
        }),
        (None, None) => Ok(Bound {
            value: default,
            explicit: false,
            origin: format!("{default}, the default"),
        }),
    }
}

/// Parses `connection_max_lifetime`; `0s` keeps connections open indefinitely.
fn parse_max_lifetime(value: &str) -> Result<Option<Duration>> {
    let lifetime = duration_parse::parse_duration(value)
        .ok()
        .context(InvalidMaxLifetimeSnafu { value })?;
    if lifetime.is_zero() {
        return Ok(None);
    }
    ensure!(
        (Duration::from_secs(1)..=MAX_CONNECTION_MAX_LIFETIME).contains(&lifetime),
        InvalidMaxLifetimeSnafu { value }
    );
    Ok(Some(lifetime))
}

/// The jitter for a lifetime Spice chose: the URL's `abs_conn_ttl_jitter` if it sets one,
/// otherwise a fifth of the lifetime, in whole seconds (`mysql_async` ignores fractions).
fn jitter_for(lifetime: Option<Duration>, url_jitter: Option<u64>) -> Option<Duration> {
    let lifetime = lifetime?;
    Some(match url_jitter {
        Some(seconds) => Duration::from_secs(seconds),
        None => Duration::from_secs((lifetime / LIFETIME_JITTER_DIVISOR).as_secs()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULTS: Option<PoolDefaults> = Some(PoolDefaults {
        min: 1,
        max: 5,
        idle_ttl: DEFAULT_IDLE_CONNECTION_TTL,
    });
    const CHANGES_MODE_DEFAULTS: Option<PoolDefaults> = Some(PoolDefaults {
        min: 0,
        max: 2,
        idle_ttl: Duration::ZERO,
    });

    fn sized(min: usize, max: usize) -> PoolConstraints {
        PoolConstraints::new(min, max).expect("a valid pool size")
    }

    fn resolve(params: PoolParams<'_>) -> Result<PoolSettings> {
        PoolSettings::resolve(params, DEFAULTS)
    }

    #[test]
    fn nothing_set_takes_the_defaults_for_both_connection_paths() {
        let expected = PoolSettings {
            size: Some(sized(1, 5)),
            max_lifetime: Some(Duration::from_mins(5)),
            max_lifetime_jitter: Some(Duration::from_mins(1)),
            idle_ttl: Duration::from_secs(30),
            idle_check_interval: Duration::from_secs(10),
        };
        assert_eq!(resolve(PoolParams::default()), Ok(expected));
        // A connection string without pool settings takes Spice's defaults, not the 10 and
        // 100 `mysql_async` would use.
        assert_eq!(
            resolve(PoolParams {
                connection_string: Some("mysql://user:pass@db.internal:3306/app"),
                ..PoolParams::default()
            }),
            Ok(expected)
        );
    }

    #[test]
    fn spicepod_parameters_apply_with_a_connection_string() {
        let settings = resolve(PoolParams {
            pool_min: Some("2"),
            pool_max: Some("20"),
            connection_max_lifetime: Some("10m"),
            connection_string: Some("mysql://user:pass@db.internal/app"),
        });
        assert_eq!(
            settings,
            Ok(PoolSettings {
                size: Some(sized(2, 20)),
                max_lifetime: Some(Duration::from_mins(10)),
                max_lifetime_jitter: Some(Duration::from_mins(2)),
                idle_ttl: Duration::from_secs(30),
                idle_check_interval: Duration::from_secs(10),
            })
        );
    }

    #[test]
    fn connection_string_settings_apply_when_the_spicepod_sets_none() {
        let settings = resolve(PoolParams {
            connection_string: Some(
                "mysql://user:pass@db.internal/app?pool_min=3&pool_max=30&abs_conn_ttl=120&inactive_connection_ttl=0&ttl_check_interval=3",
            ),
            ..PoolParams::default()
        });
        // A lifetime from the URL keeps the URL's jitter, none here, as `mysql_async` reads it.
        assert_eq!(
            settings,
            Ok(PoolSettings {
                size: Some(sized(3, 30)),
                max_lifetime: Some(Duration::from_mins(2)),
                max_lifetime_jitter: None,
                idle_ttl: Duration::ZERO,
                idle_check_interval: Duration::from_secs(3),
            })
        );

        let settings = resolve(PoolParams {
            connection_string: Some(
                "mysql://user:pass@db.internal/app?abs_conn_ttl=120&abs_conn_ttl_jitter=7",
            ),
            ..PoolParams::default()
        });
        assert_eq!(
            settings.map(|s| (s.max_lifetime, s.max_lifetime_jitter)),
            Ok((Some(Duration::from_mins(2)), Some(Duration::from_secs(7))))
        );
    }

    #[test]
    fn each_bound_resolves_on_its_own() {
        let settings = resolve(PoolParams {
            pool_max: Some("8"),
            connection_string: Some("mysql://user:pass@db.internal/app?pool_min=4"),
            ..PoolParams::default()
        });
        assert_eq!(settings.map(|s| s.size), Ok(Some(sized(4, 8))));
        // A maximum below `mysql_async`'s default minimum of 10, given alone.
        let settings = resolve(PoolParams {
            connection_string: Some("mysql://user:pass@db.internal/app?pool_max=3"),
            ..PoolParams::default()
        });
        assert_eq!(settings.map(|s| s.size), Ok(Some(sized(1, 3))));
    }

    #[test]
    fn an_unset_maximum_makes_room_for_the_minimum() {
        let settings = resolve(PoolParams {
            pool_min: Some("8"),
            ..PoolParams::default()
        });
        assert_eq!(settings.map(|s| s.size), Ok(Some(sized(8, 8))));
        let changes_mode = PoolSettings::resolve(
            PoolParams {
                connection_string: Some("mysql://user:pass@db.internal/app?pool_min=3"),
                ..PoolParams::default()
            },
            CHANGES_MODE_DEFAULTS,
        );
        assert_eq!(changes_mode.map(|s| s.size), Ok(Some(sized(3, 3))));
    }

    #[test]
    fn idle_connections_are_kept_unless_the_defaults_or_connection_string_say_otherwise() {
        let idle_ttl = |defaults, connection_string| {
            PoolSettings::resolve(
                PoolParams {
                    connection_string,
                    ..PoolParams::default()
                },
                defaults,
            )
            .map(|s| s.idle_ttl)
        };
        assert_eq!(idle_ttl(DEFAULTS, None), Ok(Duration::from_secs(30)));
        // A `refresh_mode: changes` dataset's pool closes each connection once it is returned.
        assert_eq!(idle_ttl(CHANGES_MODE_DEFAULTS, None), Ok(Duration::ZERO));
        assert_eq!(
            idle_ttl(
                CHANGES_MODE_DEFAULTS,
                Some("mysql://user:pass@db.internal/app?inactive_connection_ttl=45")
            ),
            Ok(Duration::from_secs(45))
        );
    }

    #[test]
    fn the_same_value_in_both_places_is_accepted() {
        let settings = resolve(PoolParams {
            pool_max: Some("8"),
            connection_max_lifetime: Some("2m"),
            connection_string: Some(
                "mysql://user:pass@db.internal/app?pool_max=8&abs_conn_ttl=120",
            ),
            ..PoolParams::default()
        });
        assert_eq!(
            settings.map(|s| (s.size, s.max_lifetime)),
            Ok((Some(sized(1, 8)), Some(Duration::from_mins(2))))
        );
    }

    #[test]
    fn different_values_in_both_places_are_an_error() {
        let error = resolve(PoolParams {
            pool_max: Some("5"),
            connection_string: Some("mysql://user:pass@db.internal/app?pool_max=20"),
            ..PoolParams::default()
        })
        .expect_err("conflicting pool_max");
        assert_eq!(
            error.to_string(),
            format!(
                "`mysql_pool_max` is '5', but `mysql_connection_string` sets `pool_max=20`. Set it in one place. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            connection_max_lifetime: Some("5m"),
            connection_string: Some("mysql://user:pass@db.internal/app?abs_conn_ttl=600"),
            ..PoolParams::default()
        })
        .expect_err("conflicting lifetime");
        assert_eq!(
            error.to_string(),
            format!(
                "`mysql_connection_max_lifetime` is '5m', but `mysql_connection_string` sets `abs_conn_ttl=600`. Set it in one place. See: {POOL_DOCS}"
            )
        );
    }

    #[test]
    fn invalid_pool_sizes_are_errors_not_fallbacks() {
        let error = resolve(PoolParams {
            pool_max: Some("five"),
            ..PoolParams::default()
        })
        .expect_err("non-integer pool_max");
        assert_eq!(
            error.to_string(),
            format!(
                "`mysql_pool_max` must be a whole number of connections, such as `5`, but is 'five'. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            pool_min: Some("0"),
            connection_string: Some("mysql://user:pass@db.internal/app?pool_max=0"),
            ..PoolParams::default()
        })
        .expect_err("zero pool_max");
        assert_eq!(
            error.to_string(),
            format!(
                "The pool maximum (0, from `pool_max` in `mysql_connection_string`) must be at least 1. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            pool_max: Some("4"),
            connection_string: Some("mysql://user:pass@db.internal/app?pool_min=10"),
            ..PoolParams::default()
        })
        .expect_err("pool_min above a pool_max the user set");
        assert_eq!(
            error.to_string(),
            format!(
                "The pool minimum (10, from `pool_min` in `mysql_connection_string`) is greater than the pool maximum (4, from `mysql_pool_max`). Lower `mysql_pool_min` or raise `mysql_pool_max`. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            connection_string: Some("mysql://user:pass@db.internal/app?pool_max=lots"),
            ..PoolParams::default()
        })
        .expect_err("non-integer pool_max in the URL");
        assert_eq!(
            error.to_string(),
            format!(
                "`pool_max` in `mysql_connection_string` must be a whole number, but is 'lots'. See: {POOL_DOCS}"
            )
        );
    }

    #[test]
    fn connection_string_durations_are_bounded() {
        for (url, key) in [
            ("abs_conn_ttl=31536001", "abs_conn_ttl"),
            (
                "abs_conn_ttl_jitter=18446744073709551615",
                "abs_conn_ttl_jitter",
            ),
            ("inactive_connection_ttl=soon", "inactive_connection_ttl"),
            ("ttl_check_interval=-1", "ttl_check_interval"),
        ] {
            let error = resolve(PoolParams {
                connection_string: Some(&format!("mysql://user:pass@db.internal/app?{url}")),
                ..PoolParams::default()
            })
            .expect_err(url);
            assert!(
                matches!(error, Error::InvalidConnectionStringValue { url_key, .. } if url_key == key),
                "{url}: {error}"
            );
        }
        let settings = resolve(PoolParams {
            connection_string: Some(
                "mysql://user:pass@db.internal/app?abs_conn_ttl=31536000&abs_conn_ttl_jitter=31536000",
            ),
            ..PoolParams::default()
        });
        assert_eq!(
            settings.map(|s| (s.max_lifetime, s.max_lifetime_jitter)),
            Ok((
                Some(MAX_CONNECTION_MAX_LIFETIME),
                Some(MAX_CONNECTION_MAX_LIFETIME)
            ))
        );
    }

    #[test]
    fn a_zero_lifetime_keeps_connections_open_indefinitely() {
        for value in ["0s", "0"] {
            let settings = resolve(PoolParams {
                connection_max_lifetime: Some(value),
                ..PoolParams::default()
            });
            assert_eq!(
                settings.map(|s| (s.max_lifetime, s.max_lifetime_jitter)),
                Ok((None, None)),
                "{value}"
            );
        }
    }

    #[test]
    fn lifetimes_outside_the_accepted_range_are_errors() {
        for value in ["soon", "500ms", "inf", "366d", "-5m"] {
            let error = resolve(PoolParams {
                connection_max_lifetime: Some(value),
                ..PoolParams::default()
            })
            .expect_err(value);
            assert_eq!(
                error,
                Error::InvalidMaxLifetime {
                    value: value.to_string()
                }
            );
        }
        for (value, expected) in [
            ("1s", Duration::from_secs(1)),
            ("365d", MAX_CONNECTION_MAX_LIFETIME),
        ] {
            let settings = resolve(PoolParams {
                connection_max_lifetime: Some(value),
                ..PoolParams::default()
            });
            assert_eq!(
                settings.map(|s| s.max_lifetime),
                Ok(Some(expected)),
                "{value}"
            );
        }
    }

    #[test]
    fn short_lifetimes_get_whole_second_jitter() {
        let jitter = |value| {
            resolve(PoolParams {
                connection_max_lifetime: Some(value),
                ..PoolParams::default()
            })
            .map(|s| s.max_lifetime_jitter)
        };
        assert_eq!(jitter("4s"), Ok(Some(Duration::ZERO)));
        assert_eq!(jitter("12s"), Ok(Some(Duration::from_secs(2))));
    }

    #[test]
    fn a_component_without_pool_size_parameters_keeps_its_size() {
        let settings = PoolSettings::resolve(
            PoolParams {
                connection_string: Some("mysql://user:pass@db.internal/app?pool_max=30"),
                ..PoolParams::default()
            },
            None,
        );
        assert_eq!(
            settings,
            Ok(PoolSettings {
                size: None,
                max_lifetime: Some(Duration::from_mins(5)),
                max_lifetime_jitter: Some(Duration::from_mins(1)),
                idle_ttl: Duration::from_secs(30),
                idle_check_interval: Duration::from_secs(10),
            })
        );
    }

    fn identity() -> MySQLServerIdentity {
        MySQLServerIdentity {
            configured_host: "reader.example.internal".to_string(),
            configured_port: 3306,
            connection_id: 12345,
            hostname: "ip-10-0-1-23".to_string(),
            port: 3306,
            aurora_server_id: Some("app-reader-2".to_string()),
        }
    }

    #[test]
    fn new_connection_message_names_the_server_reached() {
        let mut identity = identity();
        assert_eq!(
            new_connection_message("dataset 'orders'", &identity),
            "MySQL dataset 'orders' opened connection 12345 to 'reader.example.internal:3306', which reached server 'ip-10-0-1-23:3306' (Aurora instance 'app-reader-2')."
        );
        identity.aurora_server_id = None;
        identity.hostname = "evil\nWARN forged".to_string();
        assert_eq!(
            new_connection_message("catalog 'app'", &identity),
            "MySQL catalog 'app' opened connection 12345 to 'reader.example.internal:3306', which reached server 'evil\\nWARN forged:3306'."
        );
    }

    #[test]
    fn each_server_is_logged_at_info_once_per_interval() {
        let log = ConnectionLog {
            component: "dataset 'orders'".to_string(),
            logged_at: parking_lot::Mutex::new(HashMap::new()),
        };
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        assert!(log.first_in_interval("reader-1", at(0)));
        assert!(!log.first_in_interval("reader-1", at(1)));
        assert!(log.first_in_interval("reader-2", at(2)));
        assert!(!log.first_in_interval("reader-1", at(599)));
        assert!(log.first_in_interval("reader-1", at(600)));
        assert!(!log.first_in_interval("reader-2", at(601)));
        assert!(log.first_in_interval("reader-2", at(602)));
    }

    #[test]
    fn pool_settings_are_removed_from_the_connection_string_and_nothing_else() {
        assert_eq!(
            without_pool_settings(
                "mysql://user:p%40ss@db.internal:3306/app?pool_min=1&prefer_socket=false&pool_max=3&abs_conn_ttl=60&abs_conn_ttl_jitter=5"
            ),
            "mysql://user:p%40ss@db.internal:3306/app?prefer_socket=false"
        );
        assert_eq!(
            without_pool_settings("mysql://user:pass@db.internal/app?pool_max=3"),
            "mysql://user:pass@db.internal/app"
        );
        assert_eq!(
            without_pool_settings("mysql://user:pass@db.internal/app"),
            "mysql://user:pass@db.internal/app"
        );
        assert_eq!(without_pool_settings("not a url"), "not a url");
    }
}
