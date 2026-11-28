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

//! A dataset's definition: which source its copy's rows come from, and the
//! settings that decide how they are stored and cannot change in place.
//!
//! A copy of a dataset in a shared state location records the definition it was
//! built from, so an instance whose Spicepod reads another source under the same
//! name never serves or writes that copy. The record holds digests only, never a
//! value, so it stores no secret.
//!
//! - The source: `from`, a digest of the connection settings the connector says
//!   identify it (server, database; never credentials), and for a view the source
//!   of each table it reads. Two hosts behind one `from` differ; a rotated
//!   password or a changed timeout does not.
//! - The settings a stored table cannot adopt in place, as on a single instance:
//!   `primary_key`, the `on_conflict` it acts on, and `partition_by`.
//! - Everything else (`refresh_sql`, refresh frequency, retention, layout,
//!   credentials, tuning) is not part of it, and applies to the copy in place.
//! - Each connection setting has a digest of its own, which names the setting
//!   that changed, except one the connector declares secret (only for a connector
//!   that does not say which settings identify its source): a digest of a secret
//!   alone could be tested against guesses.
//! - The definition's id is a digest of all of these. A copy per definition is
//!   keyed by it, in the registry entry beside the copy's folder.

use std::collections::BTreeMap;

use prost::Message;

/// Domain separation: a digest of one kind never equals a digest of another.
const SETTING_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition setting";
const CONNECTION_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition connection";
const CONNECTION_SETTING_CONTEXT: &str =
    "spice.ai cayenne 2026-11 dataset definition connection setting";
const READ_CONTEXT: &str = "spice.ai cayenne 2026-11 dataset definition read";

/// The prefix of a setting that holds the definition of a table a view reads.
const READ_PREFIX: &str = "reads:";
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
    /// Each connection setting's name, with a digest of its value unless the
    /// connector declares it secret.
    connection_settings: BTreeMap<String, Option<[u8; 32]>>,
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

    /// Set the source's connection settings as its connector reads them: each
    /// setting's name, value, and whether the connector declares it secret. Their
    /// values together decide the definition, so two sources at different hosts
    /// differ, as do two values of one secret.
    #[must_use]
    pub fn with_connection<'a>(
        mut self,
        settings: impl IntoIterator<Item = (&'a str, &'a str, bool)>,
    ) -> Self {
        let settings: Vec<(&str, &str, bool)> = settings.into_iter().collect();
        self.connection = connection_digest(
            &settings
                .iter()
                .map(|(name, value, _)| (*name, *value))
                .collect(),
        );
        self.connection_settings = settings
            .iter()
            .map(|(name, value, secret)| {
                (
                    (*name).to_string(),
                    (!secret).then(|| connection_setting_digest(name, value)),
                )
            })
            .collect();
        self
    }

    /// Add the settings that decide how stored rows are keyed: `primary_key`,
    /// and the `on_conflict` the table acts on with them, as the stored table
    /// records it. These cannot change in place (see
    /// `cayenne_catalog::identity_setting_changes`). Conflict detection is left to
    /// the check every reopen makes: turning it off applies in place, and turning
    /// it on is refused there.
    #[must_use]
    pub fn with_key(
        self,
        primary_key: &[String],
        on_conflict: Option<&datafusion_table_providers::util::on_conflict::OnConflict>,
    ) -> Self {
        // An upsert on the primary key is recorded as such, not by its columns, so a
        // new key is named once, as `primary_key`, as a single instance names it.
        let on_conflict =
            crate::provider::table::upsert_on_primary_key(primary_key, on_conflict.cloned()).map(
                |on_conflict| match &on_conflict {
                    datafusion_table_providers::util::on_conflict::OnConflict::Upsert(columns)
                        if !primary_key.is_empty()
                            && columns.iter().eq(primary_key.iter().map(String::as_str)) =>
                    {
                        "upsert on the primary key".to_string()
                    }
                    other => other.to_string(),
                },
            );
        self.with_list_setting("primary_key", primary_key)
            .with_optional_setting("on_conflict", on_conflict.as_deref())
    }

    /// Add the definition of `table`, which this one (a view's) reads.
    #[must_use]
    pub fn with_read(mut self, table: &str, definition: &DatasetDefinition) -> Self {
        let mut hasher = blake3::Hasher::new_derive_key(READ_CONTEXT);
        update_framed(&mut hasher, table.as_bytes());
        hasher.update(&definition.id());
        self.settings.insert(
            format!("{READ_PREFIX}{table}"),
            *hasher.finalize().as_bytes(),
        );
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
                    digest: digest.map(|digest| digest.to_vec()).unwrap_or_default(),
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
    /// Each connection setting's name, with a digest of its value, or none for a
    /// setting the connector declares secret.
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
                _ => changes.push(match name.strip_prefix(READ_PREFIX) {
                    Some(table) => format!("the definition of '{table}', which it reads, changed"),
                    None => format!("`{name}` changed"),
                }),
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
                match (stored.get(name.as_str()), digest) {
                    (None, _) => named.push(format!("`{name}` added")),
                    (Some(stored), Some(digest)) if *stored != digest.as_slice() => {
                        named.push(format!("`{name}` changed"));
                    }
                    _ => {}
                }
            }
            for name in stored.keys() {
                if !requested.connection_settings.contains_key(*name) {
                    named.push(format!("`{name}` removed"));
                }
            }
            // Every setting with a digest of its own matches, so a secret one
            // changed.
            if named.is_empty() {
                let secrets: Vec<String> = requested
                    .connection_settings
                    .iter()
                    .filter(|(_, digest)| digest.is_none())
                    .map(|(name, _)| format!("`{name}`"))
                    .collect();
                named.push(if secrets.is_empty() {
                    "a connection setting changed".to_string()
                } else {
                    format!("one of the secret settings {} changed", secrets.join(", "))
                });
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

    /// `pg_pass` is a setting the connector declares secret.
    fn orders_with(host: &str, pass: &str) -> DatasetDefinition {
        DatasetDefinition::new("shop")
            .with_setting("from", "postgres:public.orders")
            .with_list_setting::<&str>("primary_key", &["id"])
            .with_connection([("pg_host", host, false), ("pg_pass", pass, true)])
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
        for value in [&b"db-a.internal"[..], b"hunter2"] {
            assert!(
                !record.windows(value.len()).any(|window| window == value),
                "the record holds a setting's value"
            );
        }
    }

    /// A secret setting is recorded by name only: no digest of it alone, which
    /// could be tested against guesses, only the digest of every setting together.
    #[test]
    fn a_secret_setting_has_no_digest_of_its_own() {
        let record = orders("db").record();
        let pass = record
            .connection_settings
            .iter()
            .find(|setting| setting.name == "pg_pass")
            .expect("named");
        assert_eq!(pass.digest, [] as [u8; 0]);
        let host = record
            .connection_settings
            .iter()
            .find(|setting| setting.name == "pg_host")
            .expect("named");
        assert_eq!(host.digest.len(), 32);
    }

    /// A rotated secret is a different definition, though every other setting
    /// matches: the change names the secret settings, not their values.
    #[test]
    fn a_rotated_secret_is_a_different_definition() {
        let a = orders_with("db", "hunter2");
        let rotated = orders_with("db", "correct horse");
        assert_ne!(a.id(), rotated.id());
        assert_eq!(
            changes(&a, &rotated),
            vec!["one of the secret settings `pg_pass` changed".to_string()]
        );
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
        assert_eq!(changes(&a, &renamed), Vec::<String>::new());
    }

    #[test]
    fn each_dataset_setting_and_each_connection_setting_change_is_named() {
        let a = orders("db");
        let partitioned = a
            .clone()
            .with_list_setting("partition_by", &["bucket(4, id)"]);
        assert_eq!(
            changes(&a, &partitioned),
            vec!["`partition_by` changed".to_string()]
        );
        // Unset and empty are different values.
        let unset = DatasetDefinition::new("shop").with_optional_setting("on_conflict", None);
        let empty = DatasetDefinition::new("shop").with_optional_setting("on_conflict", Some(""));
        assert_eq!(
            changes(&unset, &empty),
            vec!["`on_conflict` changed".to_string()]
        );
        let key = a
            .clone()
            .with_list_setting("primary_key", &["id", "region"]);
        assert_eq!(changes(&a, &key), vec!["`primary_key` changed".to_string()]);
        let added = a.clone().with_connection([
            ("pg_host", "db", false),
            ("pg_pass", "hunter2", true),
            ("pg_sslmode", "require", false),
        ]);
        assert_eq!(changes(&a, &added), vec!["`pg_sslmode` added".to_string()]);
        let removed = a.clone().with_connection([("pg_host", "db", false)]);
        assert_eq!(changes(&a, &removed), vec!["`pg_pass` removed".to_string()]);
    }

    /// A new primary key is named once, as `primary_key`: an upsert on the key is
    /// recorded as such, not by the key's columns. Another conflict handling on
    /// the same key is named as `on_conflict`.
    #[test]
    fn a_new_primary_key_is_named_once() {
        use datafusion_table_providers::util::{
            column_reference::ColumnReference, on_conflict::OnConflict,
        };
        let upsert = |key: &[&str]| {
            OnConflict::Upsert(ColumnReference::new(
                key.iter().map(ToString::to_string).collect(),
            ))
        };
        let keyed = |key: &[&str], on_conflict: Option<&OnConflict>| {
            let key: Vec<String> = key.iter().map(ToString::to_string).collect();
            DatasetDefinition::new("shop").with_key(&key, on_conflict)
        };
        let by_id = keyed(&["id"], Some(&upsert(&["id"])));
        let by_id_region = keyed(&["id", "region"], Some(&upsert(&["id", "region"])));
        assert_eq!(
            changes(&by_id, &by_id_region),
            vec!["`primary_key` changed".to_string()]
        );
        let dropping = keyed(&["id"], Some(&OnConflict::DoNothingAll));
        assert_eq!(
            changes(&dropping, &keyed(&["id"], None)),
            vec!["`on_conflict` changed".to_string()]
        );
    }

    /// A view's definition holds those of the tables it reads: one SQL over two
    /// sources is two definitions, naming the table whose definition differs.
    #[test]
    fn a_view_over_a_different_source_is_a_different_definition() {
        let view = |source: &DatasetDefinition| {
            DatasetDefinition::new("shop")
                .with_setting("from", "SELECT * FROM orders")
                .with_read("orders", source)
        };
        let a = view(&orders("db-a"));
        let b = view(&orders("db-b"));
        assert_ne!(a.id(), b.id());
        assert_eq!(
            changes(&a, &b),
            vec!["the definition of 'orders', which it reads, changed".to_string()]
        );
        assert_eq!(view(&orders("db-a")).id(), a.id());
    }

    /// The digests are framed, so moving a boundary between two parts of a value
    /// changes the definition.
    #[test]
    fn parts_are_framed() {
        let ab = DatasetDefinition::new("app").with_list_setting("primary_key", &["a", "b"]);
        let a_b = DatasetDefinition::new("app").with_list_setting::<&str>("primary_key", &["ab"]);
        assert_ne!(ab.id(), a_b.id());
        let host = DatasetDefinition::new("app").with_connection([("pg_host", "ab", false)]);
        let renamed = DatasetDefinition::new("app").with_connection([("pg_hosta", "b", false)]);
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
