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

use std::fmt;
use std::time::Duration;

use mysql_async::{PoolConstraints, PoolOpts};
use snafu::prelude::*;

const POOL_DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/mysql/deployment#connection-pool-sizing";

/// How long a pooled connection is used before it is closed and replaced, unless configured
/// otherwise. Replacing connections makes the pool resolve the host again, so it spreads across
/// the servers behind a load-balanced hostname (such as the readers behind a cluster's reader
/// endpoint) instead of staying on the ones it reached first.
pub const DEFAULT_CONNECTION_MAX_LIFETIME: Duration = Duration::from_mins(5);

/// The longest accepted `connection_max_lifetime`. A connection's deadline is computed as an
/// `Instant`, which a value such as `inf` would overflow.
const MAX_CONNECTION_MAX_LIFETIME: Duration = Duration::from_hours(365 * 24);

/// Each connection's lifetime is extended by a random amount of up to this fraction of it, so
/// connections opened together are replaced at different times. Without it, a pool's connections
/// would all reconnect within one DNS cache window and reach the same server again.
const LIFETIME_JITTER_DIVISOR: u32 = 5;

/// The `connection_string` query keys `mysql_async` reads for the settings resolved here.
const URL_POOL_MIN: &str = "pool_min";
const URL_POOL_MAX: &str = "pool_max";
const URL_MAX_LIFETIME: &str = "abs_conn_ttl";
const URL_MAX_LIFETIME_JITTER: &str = "abs_conn_ttl_jitter";

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display(
        "`mysql_{key}` must be a whole number of connections, such as `5`, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidPoolSize { key: &'static str, value: String },

    #[snafu(display(
        "`{url_key}` in `mysql_connection_string` must be a whole number, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidConnectionStringValue {
        url_key: &'static str,
        value: String,
    },

    #[snafu(display(
        "The pool maximum ({max}) must be at least 1. Set `mysql_pool_max` to 1 or more. See: {POOL_DOCS}"
    ))]
    ZeroPoolMax { max: Setting<usize> },

    #[snafu(display(
        "The pool minimum ({min}) is greater than the pool maximum ({max}). Lower `mysql_pool_min` or raise `mysql_pool_max`. See: {POOL_DOCS}"
    ))]
    MinGreaterThanMax {
        min: Setting<usize>,
        max: Setting<usize>,
    },

    #[snafu(display(
        "`mysql_connection_max_lifetime` must be a duration from 1s to 365d, such as `5m`, or `0s` to keep connections open indefinitely, but is '{value}'. See: {POOL_DOCS}"
    ))]
    InvalidMaxLifetime { value: String },

    #[snafu(display(
        "`mysql_{key}` is '{value}', but `mysql_connection_string` sets `{url_key}={url_value}`. Set it in one place. See: {POOL_DOCS}"
    ))]
    Conflict {
        key: &'static str,
        value: String,
        url_key: &'static str,
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

/// The pool size used for a bound that neither the Spicepod nor the connection string sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSizeDefaults {
    pub min: usize,
    pub max: usize,
}

/// Where a resolved setting came from, so an error can say which value to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Spicepod(&'static str),
    ConnectionString(&'static str),
    Default,
}

/// A resolved setting and where it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting<T> {
    pub value: T,
    pub origin: Origin,
}

impl<T: fmt::Display> fmt::Display for Setting<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.origin {
            Origin::Spicepod(key) => write!(f, "{}, from `mysql_{key}`", self.value),
            Origin::ConnectionString(url_key) => write!(
                f,
                "{}, from `{url_key}` in `mysql_connection_string`",
                self.value
            ),
            Origin::Default => write!(f, "{}, the default", self.value),
        }
    }
}

/// The resolved pool settings, applied to the pool options read from the connection
/// parameters with [`PoolSettings::apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSettings {
    /// The minimum and maximum pool size. `None` keeps the size the connection parameters
    /// give, for a component that has no pool size parameters.
    pub size: Option<PoolConstraints>,
    /// How long a connection is used before it is replaced. `None` keeps it indefinitely.
    pub max_lifetime: Option<Duration>,
    /// The upper bound of the random amount added to each connection's lifetime.
    pub max_lifetime_jitter: Option<Duration>,
}

impl PoolSettings {
    /// Resolves the pool settings from the Spicepod parameters, the connection string, and
    /// the defaults. `size_defaults` is `None` for a component that has no pool size
    /// parameters, whose pool keeps the size the connection parameters give.
    ///
    /// # Errors
    ///
    /// Returns an error when a value is invalid, when the pool minimum exceeds the maximum,
    /// or when a setting is given in both the Spicepod and the connection string with
    /// different values.
    pub fn resolve(
        params: PoolParams<'_>,
        size_defaults: Option<PoolSizeDefaults>,
    ) -> Result<Self> {
        let url = UrlPoolSettings::parse(params.connection_string)?;

        let size = match size_defaults {
            Some(defaults) => {
                let min = resolve_pool_size(
                    "pool_min",
                    params.pool_min,
                    URL_POOL_MIN,
                    url.pool_min,
                    defaults.min,
                )?;
                let max = resolve_pool_size(
                    "pool_max",
                    params.pool_max,
                    URL_POOL_MAX,
                    url.pool_max,
                    defaults.max,
                )?;
                ensure!(max.value > 0, ZeroPoolMaxSnafu { max });
                ensure!(min.value <= max.value, MinGreaterThanMaxSnafu { min, max });
                Some(
                    PoolConstraints::new(min.value, max.value)
                        .context(MinGreaterThanMaxSnafu { min, max })?,
                )
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
                        url_key: URL_MAX_LIFETIME,
                        url_value: url_ttl.to_string(),
                    }
                    .fail();
                }
                (lifetime, jitter_for(lifetime, url.max_lifetime_jitter))
            }
            // A lifetime set in the URL is taken as `mysql_async` reads it, jitter included.
            None => match url.max_lifetime {
                Some(url_ttl) => (
                    Some(Duration::from_secs(url_ttl)),
                    url.max_lifetime_jitter.map(Duration::from_secs),
                ),
                None => {
                    let lifetime = Some(DEFAULT_CONNECTION_MAX_LIFETIME);
                    (lifetime, jitter_for(lifetime, url.max_lifetime_jitter))
                }
            },
        };

        Ok(Self {
            size,
            max_lifetime,
            max_lifetime_jitter,
        })
    }

    /// Applies these settings to the pool options read from the connection parameters.
    #[must_use]
    pub fn apply(self, pool_opts: PoolOpts) -> PoolOpts {
        let pool_opts = match self.size {
            Some(size) => pool_opts.with_constraints(size),
            None => pool_opts,
        };
        pool_opts
            .with_abs_conn_ttl(self.max_lifetime)
            .with_abs_conn_ttl_jitter(self.max_lifetime_jitter)
    }
}

/// The settings resolved here that the connection string's query sets, read the way
/// `mysql_async` reads them: the last occurrence of a key wins.
#[derive(Debug, Default)]
struct UrlPoolSettings {
    pool_min: Option<usize>,
    pool_max: Option<usize>,
    max_lifetime: Option<u64>,
    max_lifetime_jitter: Option<u64>,
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
                URL_POOL_MIN => settings.pool_min = Some(parse_url_value(URL_POOL_MIN, &value)?),
                URL_POOL_MAX => settings.pool_max = Some(parse_url_value(URL_POOL_MAX, &value)?),
                URL_MAX_LIFETIME => {
                    settings.max_lifetime = Some(parse_url_value(URL_MAX_LIFETIME, &value)?);
                }
                URL_MAX_LIFETIME_JITTER => {
                    settings.max_lifetime_jitter =
                        Some(parse_url_value(URL_MAX_LIFETIME_JITTER, &value)?);
                }
                _ => {}
            }
        }
        Ok(settings)
    }
}

fn parse_url_value<T: std::str::FromStr>(url_key: &'static str, value: &str) -> Result<T> {
    value
        .parse()
        .ok()
        .context(InvalidConnectionStringValueSnafu { url_key, value })
}

fn resolve_pool_size(
    key: &'static str,
    spicepod: Option<&str>,
    url_key: &'static str,
    url: Option<usize>,
    default: usize,
) -> Result<Setting<usize>> {
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
                    url_key,
                    url_value: url_value.to_string(),
                }
                .fail();
            }
            Ok(Setting {
                value: parsed,
                origin: Origin::Spicepod(key),
            })
        }
        (None, Some(url_value)) => Ok(Setting {
            value: url_value,
            origin: Origin::ConnectionString(url_key),
        }),
        (None, None) => Ok(Setting {
            value: default,
            origin: Origin::Default,
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

    const DEFAULTS: Option<PoolSizeDefaults> = Some(PoolSizeDefaults { min: 1, max: 5 });

    fn size(min: usize, max: usize) -> Option<PoolConstraints> {
        Some(PoolConstraints::new(min, max).expect("a valid pool size"))
    }

    fn resolve(params: PoolParams<'_>) -> Result<PoolSettings> {
        PoolSettings::resolve(params, DEFAULTS)
    }

    #[test]
    fn nothing_set_takes_the_defaults_for_both_connection_paths() {
        let expected = PoolSettings {
            size: size(1, 5),
            max_lifetime: Some(Duration::from_mins(5)),
            max_lifetime_jitter: Some(Duration::from_mins(1)),
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
                size: size(2, 20),
                max_lifetime: Some(Duration::from_mins(10)),
                max_lifetime_jitter: Some(Duration::from_mins(2)),
            })
        );
    }

    #[test]
    fn connection_string_settings_apply_when_the_spicepod_sets_none() {
        let settings = resolve(PoolParams {
            connection_string: Some(
                "mysql://user:pass@db.internal/app?pool_min=3&pool_max=30&abs_conn_ttl=120",
            ),
            ..PoolParams::default()
        });
        // A lifetime from the URL keeps the URL's jitter, none here, as `mysql_async` reads it.
        assert_eq!(
            settings,
            Ok(PoolSettings {
                size: size(3, 30),
                max_lifetime: Some(Duration::from_mins(2)),
                max_lifetime_jitter: None,
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
        assert_eq!(settings.map(|s| s.size), Ok(size(4, 8)));
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
            Ok((size(1, 8), Some(Duration::from_mins(2))))
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
            error,
            Error::Conflict {
                key: "connection_max_lifetime",
                value: "5m".to_string(),
                url_key: "abs_conn_ttl",
                url_value: "600".to_string(),
            }
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
            pool_max: Some("0"),
            ..PoolParams::default()
        })
        .expect_err("zero pool_max");
        assert_eq!(
            error.to_string(),
            format!(
                "The pool maximum (0, from `mysql_pool_max`) must be at least 1. Set `mysql_pool_max` to 1 or more. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            connection_string: Some("mysql://user:pass@db.internal/app?pool_min=10"),
            ..PoolParams::default()
        })
        .expect_err("pool_min from the URL above the default pool_max");
        assert_eq!(
            error.to_string(),
            format!(
                "The pool minimum (10, from `pool_min` in `mysql_connection_string`) is greater than the pool maximum (5, the default). Lower `mysql_pool_min` or raise `mysql_pool_max`. See: {POOL_DOCS}"
            )
        );

        let error = resolve(PoolParams {
            connection_string: Some("mysql://user:pass@db.internal/app?pool_max=lots"),
            ..PoolParams::default()
        })
        .expect_err("non-integer pool_max in the URL");
        assert_eq!(
            error,
            Error::InvalidConnectionStringValue {
                url_key: "pool_max",
                value: "lots".to_string(),
            }
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
            })
        );
    }

    #[test]
    fn apply_sets_the_pool_options() {
        let settings = resolve(PoolParams {
            pool_max: Some("9"),
            ..PoolParams::default()
        })
        .expect("valid settings");
        let pool_opts = settings.apply(PoolOpts::default());
        assert_eq!(
            pool_opts.constraints(),
            PoolConstraints::new(1, 9).expect("valid")
        );
        assert_eq!(pool_opts.abs_conn_ttl(), Some(Duration::from_mins(5)));
        assert_eq!(
            pool_opts.abs_conn_ttl_jitter(),
            Some(Duration::from_mins(1))
        );
    }
}
