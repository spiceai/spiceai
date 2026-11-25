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

//! A dataset's definition: the settings that define the rows its copy stores.
//!
//! A copy of a dataset in a shared state location records the definition it was
//! built from, so an instance whose Spicepod defines the dataset differently never
//! serves or writes that copy, even when the two definitions share a name and a
//! `from`. The record holds digests only, never a value, so it stores no secret.
//!
//! - Each dataset setting (`from`, `refresh_sql`, ...) has a digest of its own.
//! - The source's connection settings (`params`), as the source reads them with
//!   every `${ … }` reference resolved, have one digest together: two hosts behind
//!   one `from` differ, and so does a rotated secret.
//! - Each connection setting also has a digest of its value as the Spicepod writes
//!   it, which names the setting that changed. A value read from a secret store is
//!   written as its reference, so no secret's digest is kept on its own.
//! - The definition's id is a digest of the dataset settings and the connection
//!   digest. A copy per definition is keyed by it, in the registry entry beside the
//!   copy's folder.

use std::collections::BTreeMap;

use prost::Message;

/// Domain separation: a digest of one kind never equals a digest of another.
const SETTING_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition setting";
const CONNECTION_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition connection";
const CONNECTION_SETTING_CONTEXT: &str =
    "spice.ai cayenne 2026-11 dataset definition connection setting as written";
const ID_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition id";

/// The format of [`DefinitionRecord`] this build writes and reads.
const RECORD_FORMAT: u32 = 1;

/// The settings that define the rows a dataset's copy stores, and the app that
/// defines them. Built by the runtime from the Spicepod; only digests are kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetDefinition {
    /// Each dataset setting, by the name an operator configures it with.
    settings: BTreeMap<String, [u8; 32]>,
    /// Every connection setting of the source as it reads them, together.
    connection: [u8; 32],
    /// Each connection setting's name and a digest of its value as written.
    connection_settings: BTreeMap<String, [u8; 32]>,
    /// The Spicepod's name. Recorded and reported, never compared.
    app: String,
}

impl DatasetDefinition {
    /// An empty definition for app `app`, with no source connection settings.
    #[must_use]
    pub fn new(app: impl Into<String>) -> Self {
        Self {
            settings: BTreeMap::new(),
            connection: connection_digest(&BTreeMap::new()),
            connection_settings: BTreeMap::new(),
            app: app.into(),
        }
    }

    /// Add the dataset setting `name` with the value `value`.
    #[must_use]
    pub fn with_setting(mut self, name: &str, value: &str) -> Self {
        self.settings
            .insert(name.to_string(), setting_digest(name, &[value.as_bytes()]));
        self
    }

    /// Add the dataset setting `name`, which may be unset. Unset and empty differ.
    #[must_use]
    pub fn with_optional_setting(mut self, name: &str, value: Option<&str>) -> Self {
        let digest = match value {
            None => setting_digest(name, &[]),
            Some(value) => setting_digest(name, &[b"", value.as_bytes()]),
        };
        self.settings.insert(name.to_string(), digest);
        self
    }

    /// Add the dataset setting `name` whose value is an ordered list.
    #[must_use]
    pub fn with_list_setting<S: AsRef<str>>(mut self, name: &str, values: &[S]) -> Self {
        let parts: Vec<&[u8]> = values
            .iter()
            .map(|value| value.as_ref().as_bytes())
            .collect();
        self.settings
            .insert(name.to_string(), setting_digest(name, &parts));
        self
    }

    /// Set the source's connection settings: `written`, by name, as the Spicepod
    /// writes them (a secret as its `${ secrets:… }` reference), and `resolved`, as
    /// the source reads them. The resolved values decide the definition: two
    /// sources at different hosts differ, as do two values of one reference.
    #[must_use]
    pub fn with_connection<'a>(
        mut self,
        written: impl IntoIterator<Item = (&'a str, &'a str)>,
        resolved: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        self.connection = connection_digest(&resolved.into_iter().collect());
        self.connection_settings = written
            .into_iter()
            .map(|(name, value)| (name.to_string(), connection_setting_digest(name, value)))
            .collect();
        self
    }

    /// The app whose Spicepod defines the dataset.
    #[must_use]
    pub fn app(&self) -> &str {
        &self.app
    }

    /// The definition's id: equal for two definitions exactly when every setting
    /// and the source's connection settings are equal. The app is not part of it.
    #[must_use]
    pub fn id(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_derive_key(ID_CONTEXT);
        for (name, digest) in &self.settings {
            update_framed(&mut hasher, name.as_bytes());
            hasher.update(digest);
        }
        update_framed(&mut hasher, b"");
        hasher.update(&self.connection);
        *hasher.finalize().as_bytes()
    }

    /// The record a copy keeps of this definition.
    pub(crate) fn record(&self) -> DefinitionRecord {
        DefinitionRecord {
            format: RECORD_FORMAT,
            id: self.id().to_vec(),
            settings: self
                .settings
                .iter()
                .map(|(name, digest)| SettingDigest {
                    name: name.clone(),
                    digest: digest.to_vec(),
                })
                .collect(),
            connection: self.connection.to_vec(),
            connection_settings: self
                .connection_settings
                .iter()
                .map(|(name, digest)| SettingDigest {
                    name: name.clone(),
                    digest: digest.to_vec(),
                })
                .collect(),
            app: self.app.clone(),
        }
    }
}

/// A length-framed part, so two different sequences of parts never hash alike.
fn update_framed(hasher: &mut blake3::Hasher, part: &[u8]) {
    hasher.update(&(part.len() as u64).to_le_bytes());
    hasher.update(part);
}

fn setting_digest(name: &str, parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(SETTING_CONTEXT);
    update_framed(&mut hasher, name.as_bytes());
    hasher.update(&(parts.len() as u64).to_le_bytes());
    for part in parts {
        update_framed(&mut hasher, part);
    }
    *hasher.finalize().as_bytes()
}

fn connection_digest(settings: &BTreeMap<&str, &str>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(CONNECTION_CONTEXT);
    hasher.update(&(settings.len() as u64).to_le_bytes());
    for (name, value) in settings {
        update_framed(&mut hasher, name.as_bytes());
        update_framed(&mut hasher, value.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn connection_setting_digest(name: &str, value: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(CONNECTION_SETTING_CONTEXT);
    update_framed(&mut hasher, name.as_bytes());
    update_framed(&mut hasher, value.as_bytes());
    *hasher.finalize().as_bytes()
}

/// What a dataset's copy records of the definition it was built from. Numbered
/// fields are a persistent contract.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct DefinitionRecord {
    #[prost(uint32, tag = "1")]
    pub format: u32,
    /// [`DatasetDefinition::id`].
    #[prost(bytes = "vec", tag = "2")]
    pub id: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    pub settings: Vec<SettingDigest>,
    #[prost(bytes = "vec", tag = "4")]
    pub connection: Vec<u8>,
    /// Each connection setting's name, with a digest of its value as written.
    #[prost(message, repeated, tag = "5")]
    pub connection_settings: Vec<SettingDigest>,
    /// The app that last served the copy as its writer.
    #[prost(string, tag = "6")]
    pub app: String,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct SettingDigest {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(bytes = "vec", tag = "2")]
    pub digest: Vec<u8>,
}

impl DefinitionRecord {
    /// Decode a record a copy keeps.
    /// # Errors
    /// A record that does not decode, or is of a format this build cannot read.
    pub(crate) fn decode_stored(bytes: &[u8]) -> Result<Self, String> {
        let record = Self::decode(bytes)
            .map_err(|error| format!("its definition record is invalid: {error}"))?;
        if record.format != RECORD_FORMAT {
            return Err(format!(
                "its definition record has format {}, which this version of Spice cannot read",
                record.format
            ));
        }
        Ok(record)
    }

    /// What differs between this stored definition and `requested`, each named as
    /// an operator configures it and never with its value; empty when they match.
    pub(crate) fn changes(&self, requested: &DatasetDefinition) -> Vec<String> {
        if self.id == requested.id() {
            return Vec::new();
        }
        let stored: BTreeMap<&str, &[u8]> = self
            .settings
            .iter()
            .map(|setting| (setting.name.as_str(), setting.digest.as_slice()))
            .collect();
        let mut changes = Vec::new();
        let setting_names: std::collections::BTreeSet<&str> = stored
            .keys()
            .copied()
            .chain(requested.settings.keys().map(String::as_str))
            .collect();
        for name in setting_names {
            match (stored.get(name), requested.settings.get(name)) {
                (Some(stored), Some(requested)) if *stored == requested.as_slice() => {}
                _ => changes.push(format!("`{name}` changed")),
            }
        }
        if self.connection != requested.connection {
            let stored: BTreeMap<&str, &[u8]> = self
                .connection_settings
                .iter()
                .map(|setting| (setting.name.as_str(), setting.digest.as_slice()))
                .collect();
            let mut named = Vec::new();
            for (name, digest) in &requested.connection_settings {
                match stored.get(name.as_str()) {
                    None => named.push(format!("`{name}` added")),
                    Some(stored) if *stored != digest.as_slice() => {
                        named.push(format!("`{name}` changed"));
                    }
                    Some(_) => {}
                }
            }
            for name in stored.keys() {
                if !requested.connection_settings.contains_key(*name) {
                    named.push(format!("`{name}` removed"));
                }
            }
            // Every setting is written as before, so a value one of them reads
            // from a secret store or the environment changed.
            if named.is_empty() {
                named.push(
                    "a secret or environment value a setting in `params` reads changed".to_string(),
                );
            }
            changes.extend(named);
        }
        if changes.is_empty() {
            // The ids differ while every digest matches: a record of another
            // build's id scheme. Still a different definition.
            changes.push("the definition changed".to_string());
        }
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::{DatasetDefinition, DefinitionRecord};
    use prost::Message;

    fn orders(host: &str) -> DatasetDefinition {
        orders_with(host, "hunter2")
    }

    /// `pg_pass` is read from a secret store, as `${ secrets:pg_pass }`.
    fn orders_with(host: &str, pass: &str) -> DatasetDefinition {
        DatasetDefinition::new("shop")
            .with_setting("from", "postgres:public.orders")
            .with_optional_setting("refresh_sql", None)
            .with_list_setting::<&str>("primary_key", &["id"])
            .with_connection(
                [("pg_host", host), ("pg_pass", "${ secrets:pg_pass }")],
                [("pg_host", host), ("pg_pass", pass)],
            )
    }

    fn changes(stored: &DatasetDefinition, requested: &DatasetDefinition) -> Vec<String> {
        let bytes = stored.record().encode_to_vec();
        DefinitionRecord::decode_stored(&bytes)
            .expect("decodes")
            .changes(requested)
    }

    /// Two sources at different hosts behind one `from` are two definitions, and
    /// the change names the setting, never its value.
    #[test]
    fn a_different_host_is_a_different_definition_named_without_its_value() {
        let a = orders("db-a.internal");
        let b = orders("db-b.internal");
        assert_ne!(a.id(), b.id());
        assert_eq!(changes(&a, &b), vec!["`pg_host` changed".to_string()]);
        let record = a.record().encode_to_vec();
        for secret in [&b"db-a.internal"[..], b"hunter2"] {
            assert!(
                !record.windows(secret.len()).any(|window| window == secret),
                "the record holds a setting's value"
            );
        }
    }

    /// The app is recorded but is not part of the definition, so a renamed app
    /// keeps its copy.
    #[test]
    fn the_app_name_is_not_part_of_the_definition() {
        let a = orders("db");
        let renamed = DatasetDefinition {
            app: "renamed".to_string(),
            ..a.clone()
        };
        assert_eq!(a.id(), renamed.id());
        assert_eq!(changes(&a, &renamed), [] as [std::string::String; 0]);
    }

    #[test]
    fn each_dataset_setting_and_each_connection_setting_change_is_named() {
        let a = orders("db");
        let refresh = a
            .clone()
            .with_optional_setting("refresh_sql", Some("SELECT * FROM orders WHERE id < 10"));
        assert_eq!(
            changes(&a, &refresh),
            vec!["`refresh_sql` changed".to_string()]
        );
        // Unset and empty are different values.
        let empty = a.clone().with_optional_setting("refresh_sql", Some(""));
        assert_eq!(
            changes(&a, &empty),
            vec!["`refresh_sql` changed".to_string()]
        );
        let key = a
            .clone()
            .with_list_setting("primary_key", &["id", "region"]);
        assert_eq!(changes(&a, &key), vec!["`primary_key` changed".to_string()]);
        let added = a.clone().with_connection(
            [
                ("pg_host", "db"),
                ("pg_pass", "${ secrets:pg_pass }"),
                ("pg_sslmode", "require"),
            ],
            [
                ("pg_host", "db"),
                ("pg_pass", "hunter2"),
                ("pg_sslmode", "require"),
            ],
        );
        assert_eq!(changes(&a, &added), vec!["`pg_sslmode` added".to_string()]);
        let removed = a
            .clone()
            .with_connection([("pg_host", "db")], [("pg_host", "db")]);
        assert_eq!(changes(&a, &removed), vec!["`pg_pass` removed".to_string()]);
    }

    /// A rotated secret is a different definition, though the Spicepod reads the
    /// same: the change names what reads from a secret store, not the secret.
    #[test]
    fn a_rotated_secret_is_a_different_definition() {
        let a = orders_with("db", "hunter2");
        let rotated = orders_with("db", "correct horse");
        assert_ne!(a.id(), rotated.id());
        assert_eq!(
            changes(&a, &rotated),
            vec!["a secret or environment value a setting in `params` reads changed".to_string()]
        );
    }

    /// The same values written another way read the same source: one definition.
    #[test]
    fn the_resolved_values_decide_the_definition() {
        let inline = DatasetDefinition::new("shop")
            .with_connection([("pg_host", "db")], [("pg_host", "db")]);
        let from_env = DatasetDefinition::new("shop")
            .with_connection([("pg_host", "${ env:PG_HOST }")], [("pg_host", "db")]);
        assert_eq!(inline.id(), from_env.id());
        assert_eq!(changes(&inline, &from_env), [] as [std::string::String; 0]);
    }

    /// The digests are framed, so moving a boundary between two parts of a value
    /// changes the definition.
    #[test]
    fn parts_are_framed() {
        let ab = DatasetDefinition::new("app").with_list_setting("primary_key", &["a", "b"]);
        let a_b = DatasetDefinition::new("app").with_list_setting::<&str>("primary_key", &["ab"]);
        assert_ne!(ab.id(), a_b.id());
        let host = DatasetDefinition::new("app").with_connection([], [("pg_host", "ab")]);
        let renamed = DatasetDefinition::new("app").with_connection([], [("pg_hosta", "b")]);
        assert_ne!(host.id(), renamed.id());
    }

    #[test]
    fn a_record_of_another_format_is_refused() {
        let mut record = orders("db").record();
        record.format = 2;
        let error = DefinitionRecord::decode_stored(&record.encode_to_vec()).expect_err("format 2");
        assert!(error.contains("format 2"), "{error}");
    }
}
