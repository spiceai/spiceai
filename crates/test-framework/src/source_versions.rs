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

//! The release lines of each federated source database that Spice supports and
//! tests, read from `test/source_versions.json`. The integration suite starts its
//! source containers from this list, testoperator dispatches its benchmarks on
//! it, and CI builds its source-version matrix from the same file, so none of
//! them can drift from the others.
//!
//! A test starts its source container from [`source_image`]: the newest listed
//! line by default, or the line `SPICE_TEST_<SOURCE>_VERSION` names. A version
//! the file does not list is refused rather than pulled, so a test never
//! silently runs against an unsupported server.

use std::{collections::BTreeMap, env::VarError, sync::LazyLock};

use anyhow::Context;
use serde::Deserialize;

const SOURCE_VERSIONS_JSON: &str = include_str!("../../../test/source_versions.json");

/// A source database with container images in `test/source_versions.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Postgres,
    MySql,
    MongoDb,
}

impl Source {
    pub const ALL: [Self; 3] = [Self::Postgres, Self::MySql, Self::MongoDb];

    /// The source's key in `test/source_versions.json`.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::MongoDb => "mongodb",
        }
    }

    /// The environment variable that selects a listed version.
    #[must_use]
    pub fn version_env_var(self) -> &'static str {
        match self {
            Self::Postgres => "SPICE_TEST_POSTGRES_VERSION",
            Self::MySql => "SPICE_TEST_MYSQL_VERSION",
            Self::MongoDb => "SPICE_TEST_MONGODB_VERSION",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SourceVersions {
    pub image: String,
    pub versions: Vec<SourceVersion>,
}

#[derive(Debug, Deserialize)]
pub struct SourceVersion {
    pub version: String,
    pub end_of_life: String,
}

static SOURCE_VERSIONS: LazyLock<Result<BTreeMap<String, SourceVersions>, String>> =
    LazyLock::new(|| parse_source_versions(SOURCE_VERSIONS_JSON));

fn parse_source_versions(json: &str) -> Result<BTreeMap<String, SourceVersions>, String> {
    let file: BTreeMap<String, serde_json::Value> = serde_json::from_str(json)
        .map_err(|e| format!("test/source_versions.json is not valid JSON: {e}"))?;
    file.into_iter()
        .filter(|(key, _)| key != "description")
        .map(|(key, value)| {
            let versions = serde_json::from_value::<SourceVersions>(value).map_err(|e| {
                format!("test/source_versions.json entry '{key}' is malformed: {e}")
            })?;
            Ok((key, versions))
        })
        .collect()
}

/// The release lines listed for `source`, oldest first.
///
/// # Errors
///
/// When `test/source_versions.json` is malformed or does not list `source`.
pub fn source_versions(source: Source) -> anyhow::Result<&'static SourceVersions> {
    let all = SOURCE_VERSIONS
        .as_ref()
        .map_err(|message| anyhow::anyhow!("{message}"))?;
    all.get(source.key())
        .with_context(|| format!("test/source_versions.json has no '{}' entry", source.key()))
}

/// The container image a test starts for `source`: the newest listed line, or
/// the listed line that `SPICE_TEST_<SOURCE>_VERSION` names.
///
/// # Errors
///
/// When the environment variable names a version the file does not list, or
/// the file lists none for `source`.
pub fn source_image(source: Source) -> anyhow::Result<String> {
    let listed = source_versions(source)?;
    let requested = match std::env::var(source.version_env_var()) {
        Ok(version) if !version.trim().is_empty() => Some(version.trim().to_string()),
        Ok(_) | Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(_)) => anyhow::bail!(
            "{} is not valid UTF-8; set it to one of the versions listed for '{}' in test/source_versions.json",
            source.version_env_var(),
            source.key()
        ),
    };
    resolve_image(source, listed, requested.as_deref())
}

fn resolve_image(
    source: Source,
    listed: &SourceVersions,
    requested: Option<&str>,
) -> anyhow::Result<String> {
    let version = match requested {
        Some(version) => listed
            .versions
            .iter()
            .find(|listed| listed.version == version)
            .with_context(|| {
                format!(
                    "{}={version} is not a supported {} version. test/source_versions.json lists {}",
                    source.version_env_var(),
                    source.key(),
                    listed
                        .versions
                        .iter()
                        .map(|listed| listed.version.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?,
        None => listed.versions.last().with_context(|| {
            format!(
                "test/source_versions.json lists no '{}' versions",
                source.key()
            )
        })?,
    };
    Ok(format!("{}:{}", listed.image, version.version))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The checked-in file is what both the suite and CI's matrix read, so a
    /// malformed or out-of-order entry would silently change the default
    /// version, or drop a version from CI's matrix.
    #[test]
    fn checked_in_versions_are_ascending_dated_and_unique() {
        for source in Source::ALL {
            let listed = source_versions(source).expect("source is listed");
            assert!(
                !listed.versions.is_empty(),
                "{} lists no versions",
                source.key()
            );
            let numbers = listed
                .versions
                .iter()
                .map(|v| {
                    v.version
                        .split('.')
                        .map(|part| part.parse::<u32>().expect("numeric version part"))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert!(
                numbers.windows(2).all(|pair| pair[0] < pair[1]),
                "{} versions must be strictly ascending so the last is the newest: {:?}",
                source.key(),
                listed
                    .versions
                    .iter()
                    .map(|v| &v.version)
                    .collect::<Vec<_>>()
            );
            for version in &listed.versions {
                assert!(
                    chrono::NaiveDate::parse_from_str(&version.end_of_life, "%Y-%m-%d").is_ok(),
                    "{} {} has end_of_life '{}', expected YYYY-MM-DD",
                    source.key(),
                    version.version,
                    version.end_of_life
                );
            }
        }
    }

    #[test]
    fn resolves_the_newest_line_by_default_and_a_listed_line_on_request() {
        let listed = parse_source_versions(
            r#"{"description": "x", "mysql": {"image": "docker.io/library/mysql", "versions": [
                {"version": "8.4", "end_of_life": "2032-04-30"},
                {"version": "9.7", "end_of_life": "2034-04-30"}]}}"#,
        )
        .expect("valid fixture");
        let mysql = listed.get("mysql").expect("mysql entry");

        assert_eq!(
            resolve_image(Source::MySql, mysql, None).expect("default resolves"),
            "docker.io/library/mysql:9.7"
        );
        assert_eq!(
            resolve_image(Source::MySql, mysql, Some("8.4")).expect("listed version resolves"),
            "docker.io/library/mysql:8.4"
        );
        let refused = resolve_image(Source::MySql, mysql, Some("8.0"))
            .expect_err("an unlisted version is refused");
        assert_eq!(
            refused.to_string(),
            "SPICE_TEST_MYSQL_VERSION=8.0 is not a supported mysql version. test/source_versions.json lists 8.4, 9.7"
        );
    }
}
