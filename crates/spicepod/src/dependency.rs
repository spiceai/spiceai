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

//! Entries under a Spicepod's `dependencies:`, and how far each is trusted.

use std::fmt;

#[cfg(feature = "schemars")]
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// One entry under `dependencies:`: the short form `- acme/support`, or the
/// mapping form `- { from: acme/support, trust: full }`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Dependency {
    /// `- acme/support`: trusted as `restricted`.
    Short(String),
    /// `- from: acme/support` with an optional `trust`.
    Entry(DependencyEntry),
}

/// The mapping form of a `dependencies:` entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DependencyEntry {
    /// The same reference the short form holds: `org/pod`, or a path under
    /// `spicepods/`.
    pub from: String,

    /// What the dependency may declare. Defaults to `restricted`.
    #[serde(default)]
    pub trust: DependencyTrust,
}

/// How far the root `spicepod.yaml` trusts a dependency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DependencyTrust {
    /// May declare anything except a component that starts a program on the
    /// host.
    #[default]
    Restricted,
    /// May declare any component the root Spicepod may. Root-only sections
    /// (`runtime`, `secrets`, …) are still ignored.
    Full,
}

impl Dependency {
    /// The dependency's reference: `org/pod`, or a path under `spicepods/`.
    #[must_use]
    pub fn from(&self) -> &str {
        match self {
            Self::Short(from) | Self::Entry(DependencyEntry { from, .. }) => from,
        }
    }

    #[must_use]
    pub fn trust(&self) -> DependencyTrust {
        match self {
            Self::Short(_) => DependencyTrust::Restricted,
            Self::Entry(entry) => entry.trust,
        }
    }
}

impl fmt::Display for DependencyTrust {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Restricted => "restricted",
            Self::Full => "full",
        })
    }
}

/// Deserialized by hand rather than as an untagged enum, so a mistake in the
/// mapping form reports serde's own message (``unknown variant `yes`, expected
/// `restricted` or `full` ``, ``unknown field `trusted` ``) instead of "did not
/// match any variant".
impl<'de> Deserialize<'de> for Dependency {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DependencyVisitor;

        impl<'de> Visitor<'de> for DependencyVisitor {
            type Value = Dependency;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Spicepod reference such as `acme/support`, or a mapping with `from` and an optional `trust`")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Dependency, E> {
                Ok(Dependency::Short(value.to_string()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Dependency, E> {
                Ok(Dependency::Short(value))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Dependency, A::Error> {
                DependencyEntry::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(Dependency::Entry)
            }
        }

        deserializer.deserialize_any(DependencyVisitor)
    }
}

#[cfg(feature = "schemars")]
impl JsonSchema for Dependency {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Dependency".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Dependency").into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "oneOf": [
                { "type": "string" },
                {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["from"],
                    "properties": {
                        "from": { "type": "string" },
                        "trust": {
                            "type": "string",
                            "enum": ["restricted", "full"],
                            "default": "restricted"
                        }
                    }
                }
            ]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<Vec<Dependency>, String> {
        yaml::from_str(text).map_err(|error| error.to_string())
    }

    #[test]
    fn both_forms_parse_and_default_to_restricted() {
        let dependencies = parse(
            "- spiceai/quickstart\n- from: acme/support\n- from: acme/tools\n  trust: full\n- { from: acme/inline, trust: restricted }\n",
        )
        .expect("every form parses");

        let read: Vec<(&str, DependencyTrust)> = dependencies
            .iter()
            .map(|dependency| (dependency.from(), dependency.trust()))
            .collect();
        assert_eq!(
            read,
            [
                ("spiceai/quickstart", DependencyTrust::Restricted),
                ("acme/support", DependencyTrust::Restricted),
                ("acme/tools", DependencyTrust::Full),
                ("acme/inline", DependencyTrust::Restricted),
            ]
        );
    }

    #[test]
    fn an_invalid_trust_value_names_the_accepted_ones() {
        let error = parse("- from: acme/support\n  trust: yes\n")
            .expect_err("`yes` is not a trust level");
        assert!(
            error.contains("unknown variant `yes`, expected `restricted` or `full`"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_key_is_an_error() {
        let error = parse("- from: acme/support\n  trusted: full\n")
            .expect_err("`trusted` is not a key of a dependency entry");
        assert!(error.contains("unknown field `trusted`"), "{error}");
    }

    #[test]
    fn the_short_form_round_trips_unchanged() {
        let dependencies = parse("- spiceai/quickstart\n- from: acme/tools\n  trust: full\n")
            .expect("parses");
        let written = yaml::to_string(&dependencies).expect("serializes");
        assert_eq!(
            parse(&written).expect("re-parses"),
            dependencies,
            "{written}"
        );
        assert!(written.contains("- spiceai/quickstart"), "{written}");
    }
}
