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

use regex::Regex;
#[cfg(feature = "schemars")]
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::sync::LazyLock;
use std::{collections::HashMap, fmt::Debug};

use crate::component::catalog::Catalog;
use crate::component::embeddings::Embeddings;
use crate::component::function::Function;
use crate::component::is_default;
use crate::component::management::Management;
use crate::component::rerankers::Reranker;
use crate::component::runtime::Runtime;
use crate::component::secret::Secret;
use crate::component::snapshot::Snapshots;
use crate::component::tool::Tool;
use crate::component::{
    ComponentOrReference, dataset::Dataset, model::Model, view::View, worker::Worker,
};
use crate::extension::Extension;

/// The version of a Spicepod definition.
///
/// Accepts free-form version strings of the form:
/// - `v1`, `v2` — major only
/// - `v2.0` — major.minor
/// - `v2.0.0` — major.minor.patch
/// - `v2.0.0-rc.1` — major.minor.patch with a pre-release identifier
///
/// Currently only `v1` and `v2` majors are supported. Other shapes (`v3`, `v0`, etc.)
/// fail to parse with an "unsupported major" error.
///
/// Equality is structural: `v1` and `v1.0.0` are not equal.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct SpicepodVersion {
    major: u64,
    minor: Option<u64>,
    patch: Option<u64>,
    pre_release: Option<String>,
}

impl SpicepodVersion {
    /// `v1` — equivalent to parsing the string `"v1"`.
    pub const V1: Self = Self {
        major: 1,
        minor: None,
        patch: None,
        pre_release: None,
    };

    /// `v2` — equivalent to parsing the string `"v2"`.
    pub const V2: Self = Self {
        major: 2,
        minor: None,
        patch: None,
        pre_release: None,
    };

    #[must_use]
    pub fn major(&self) -> u64 {
        self.major
    }

    #[must_use]
    pub fn minor(&self) -> Option<u64> {
        self.minor
    }

    #[must_use]
    pub fn patch(&self) -> Option<u64> {
        self.patch
    }

    #[must_use]
    pub fn pre_release(&self) -> Option<&str> {
        self.pre_release.as_deref()
    }
}

impl Default for SpicepodVersion {
    fn default() -> Self {
        Self::V2
    }
}

impl Display for SpicepodVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "v{}", self.major)?;
        if let Some(minor) = self.minor {
            write!(f, ".{minor}")?;
        }
        if let Some(patch) = self.patch {
            write!(f, ".{patch}")?;
        }
        if let Some(pre) = &self.pre_release {
            write!(f, "-{pre}")?;
        }
        Ok(())
    }
}

/// Error returned when a spicepod version string cannot be accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpicepodVersionParseError {
    input: String,
    kind: SpicepodVersionParseErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SpicepodVersionParseErrorKind {
    /// The input does not match the expected `vMAJOR[.MINOR[.PATCH[-PRERELEASE]]]` format.
    InvalidFormat,
    /// The format is valid but the major version is not one of [`SUPPORTED_MAJORS`].
    UnsupportedMajor(u64),
}

impl Display for SpicepodVersionParseError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.kind {
            SpicepodVersionParseErrorKind::InvalidFormat => write!(
                f,
                "invalid spicepod version '{}': expected a version string like 'v1', 'v2', 'v2.0', 'v2.0.0', or 'v2.0.0-rc.1'",
                self.input
            ),
            SpicepodVersionParseErrorKind::UnsupportedMajor(major) => write!(
                f,
                "unsupported spicepod version '{}': major version v{major} is not supported (supported majors: v1, v2)",
                self.input
            ),
        }
    }
}

impl std::error::Error for SpicepodVersionParseError {}

/// Pattern accepted by [`SpicepodVersion::from_str`] as a well-formed version string.
///
/// Numeric components disallow leading zeros (`v01`, `v2.01.0` are invalid).
/// A pre-release identifier is only valid alongside a full `major.minor.patch`.
///
/// Note that this regex accepts any major version; [`FromStr::from_str`] additionally
/// validates that the parsed major is in [`SUPPORTED_MAJORS`].
const VERSION_PATTERN: &str = r"^v(0|[1-9]\d*)(?:\.(0|[1-9]\d*)(?:\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?)?)?$";

/// Currently supported major versions. Keep [`VERSION_SCHEMA_PATTERN`] in sync.
const SUPPORTED_MAJORS: &[u64] = &[1, 2];

/// JSON Schema pattern — restricted to currently supported major versions
/// so external schema validators reject unsupported majors up-front.
/// Keep in sync with [`SUPPORTED_MAJORS`].
#[cfg(feature = "schemars")]
const VERSION_SCHEMA_PATTERN: &str = r"^v(1|2)(?:\.(0|[1-9]\d*)(?:\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?)?)?$";

#[expect(clippy::expect_used)]
static VERSION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(VERSION_PATTERN).expect("spicepod version regex must compile"));

impl FromStr for SpicepodVersion {
    type Err = SpicepodVersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid_format = || SpicepodVersionParseError {
            input: s.to_string(),
            kind: SpicepodVersionParseErrorKind::InvalidFormat,
        };
        let captures = VERSION_REGEX.captures(s).ok_or_else(invalid_format)?;

        let parse_num = |idx: usize| -> Result<Option<u64>, SpicepodVersionParseError> {
            captures
                .get(idx)
                .map(|m| m.as_str().parse::<u64>().map_err(|_| invalid_format()))
                .transpose()
        };

        let major = parse_num(1)?.ok_or_else(invalid_format)?;
        let minor = parse_num(2)?;
        let patch = parse_num(3)?;
        let pre_release = captures.get(4).map(|m| m.as_str().to_string());

        if !SUPPORTED_MAJORS.contains(&major) {
            return Err(SpicepodVersionParseError {
                input: s.to_string(),
                kind: SpicepodVersionParseErrorKind::UnsupportedMajor(major),
            });
        }

        Ok(Self {
            major,
            minor,
            patch,
            pre_release,
        })
    }
}

impl Serialize for SpicepodVersion {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SpicepodVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::from_str(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schemars")]
impl JsonSchema for SpicepodVersion {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SpicepodVersion".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SpicepodVersion").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": VERSION_SCHEMA_PATTERN,
            "description": "Spicepod schema version. Supported majors: v1, v2. Examples: 'v1', 'v2', 'v2.0', 'v2.0.0', 'v2.0.0-rc.1'."
        })
    }
}

/// # Spicepod Definition
///
/// A Spicepod definition is a YAML file that describes a Spicepod.
#[derive(Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct SpicepodDefinition {
    /// The name of the Spicepod
    pub name: String,

    /// The version of the Spicepod
    pub version: SpicepodVersion,

    /// The kind of Spicepod
    pub kind: SpicepodKind,

    /// Optional runtime configuration
    #[serde(default, skip_serializing_if = "is_default")]
    pub runtime: Runtime,

    /// Optional management configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub management: Option<Management>,

    /// Optional acceleration snapshot configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Snapshots>,

    /// Optional extensions configuration
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub extensions: HashMap<String, Extension>,

    /// Optional spicepod secrets configuration
    /// Default value is:
    /// ```yaml
    /// secrets:
    ///   - from: env
    ///     name: env
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub catalogs: Vec<ComponentOrReference<Catalog>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub datasets: Vec<ComponentOrReference<Dataset>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub views: Vec<ComponentOrReference<View>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub models: Vec<ComponentOrReference<Model>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub embeddings: Vec<ComponentOrReference<Embeddings>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub rerankers: Vec<ComponentOrReference<Reranker>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub tools: Vec<ComponentOrReference<Tool>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub workers: Vec<ComponentOrReference<Worker>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub functions: Vec<ComponentOrReference<Function>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub enum SpicepodKind {
    #[default]
    Spicepod,
}

impl SpicepodDefinition {
    /// Create a new `SpicepodDefinition` with the given name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: SpicepodVersion::V2,
            kind: SpicepodKind::Spicepod,
            ..Default::default()
        }
    }
}
