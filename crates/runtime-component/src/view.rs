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

use datafusion::common::TableReference;
use snafu::prelude::*;
use spicepod::{component::view as spicepod_view, vector::VectorStore};
use std::{collections::HashMap, fs, sync::Arc, time::Duration};

use crate::dataset::{ReadyState, acceleration};
use spicepod::semantic::Column;

/// Errors from parsing a Spicepod view into a [`ViewBuilder`]. The display text
/// matches the `runtime` error that each variant translates into.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    InvalidViewName { source: crate::Error },

    #[snafu(display(
        "Dataset names should not include a catalog. Unexpected '{}' in '{}'. Remove '{}' from the dataset name and try again.",
        catalog,
        name,
        catalog,
    ))]
    ViewNameIncludesCatalog { catalog: Arc<str>, name: Arc<str> },

    #[snafu(display("Unable to load SQL file {file}: {source}"))]
    UnableToLoadSqlFile {
        file: String,
        source: std::io::Error,
    },

    #[snafu(display(
        "Specify the SQL string for view {name} using either `sql: SELECT * FROM...` inline or as a file reference with `sql_ref: my_view.sql`"
    ))]
    NeedToSpecifySQLView { name: String },

    #[snafu(display(
        "An accelerated table has invalid configuration: {source}. Update the configuration and retry. For details, visit: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    ))]
    InvalidAccelerationConfiguration { source: acceleration::ParseError },

    #[snafu(display(
        "Configuration of '{view_name}' view is invalid: {reason}. Update the configuration and retry. For details, visit: https://spiceai.org/docs/components/views"
    ))]
    AcceleratedViewInvalidConfiguration { view_name: String, reason: String },
}

/// Config-only core of a view — every declared field of a
/// `runtime::component::view::View` except the runtime handles (`app`/`runtime`).
/// The runtime wrapper holds `Self` plus those handles and `Deref`s to it.
#[derive(Clone)]
pub struct ViewSpec {
    pub name: TableReference,
    pub sql: Arc<str>,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
    pub vectors: Option<VectorStore>,
    pub params: HashMap<String, String>,
}

impl PartialEq for ViewSpec {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.sql == other.sql
            && self.metadata == other.metadata
            && self.columns == other.columns
            && self.acceleration == other.acceleration
            && self.vectors == other.vectors
            && self.params == other.params
            && self.ready_state == other.ready_state
    }
}

impl std::fmt::Debug for ViewSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ViewSpec")
            .field("name", &self.name)
            .field("sql", &self.sql)
            .field("metadata", &self.metadata)
            .field("columns", &self.columns)
            .field("acceleration", &self.acceleration)
            .field("ready_state", &self.ready_state)
            .field("vectors", &self.vectors)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl ViewSpec {
    #[must_use]
    pub fn is_accelerated(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.enabled;
        }

        false
    }

    #[must_use]
    pub fn refresh_check_interval(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_check_interval;
        }
        None
    }

    #[must_use]
    pub fn refresh_max_jitter(&self) -> Option<Duration> {
        if let Some(acceleration) = &self.acceleration
            && acceleration.refresh_jitter_enabled
        {
            // If `refresh_jitter_max` is not set, use 10% of `refresh_check_interval`.
            return match acceleration.refresh_jitter_max {
                Some(jitter) => Some(jitter),
                None => self.refresh_check_interval().map(|i| i.mul_f64(0.1)),
            };
        }
        None
    }

    #[must_use]
    pub fn refresh_retry_enabled(&self) -> bool {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_enabled;
        }
        false
    }

    #[must_use]
    pub fn refresh_retry_max_attempts(&self) -> Option<usize> {
        if let Some(acceleration) = &self.acceleration {
            return acceleration.refresh_retry_max_attempts;
        }
        None
    }

    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        self.columns.iter().any(|c| !c.embeddings.is_empty())
    }

    #[must_use]
    pub fn has_full_text_column(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled))
    }
}

/// Parsed, validated view configuration. [`ViewBuilder::build`] produces the
/// [`ViewSpec`]; the runtime attaches its handles to that spec.
pub struct ViewBuilder {
    pub name: TableReference,
    pub sql: String,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub ready_state: ReadyState,
    pub vectors: Option<VectorStore>,
    pub params: HashMap<String, String>,
}

impl ViewBuilder {
    #[must_use]
    pub fn new(name: TableReference, sql: String) -> Self {
        Self {
            name,
            sql,
            metadata: HashMap::default(),
            columns: vec![],
            acceleration: None,
            ready_state: ReadyState::default(),
            vectors: None,
            params: HashMap::default(),
        }
    }

    #[must_use]
    pub fn build(self) -> ViewSpec {
        ViewSpec {
            name: self.name,
            sql: Arc::from(self.sql),
            metadata: self.metadata,
            columns: self.columns,
            acceleration: self.acceleration,
            ready_state: self.ready_state,
            vectors: self.vectors,
            params: self.params,
        }
    }
}

fn load_sql_ref(sql_ref: &str) -> Result<String, Error> {
    fs::read_to_string(sql_ref).context(UnableToLoadSqlFileSnafu { file: sql_ref })
}

/// A view name may name a schema but not a catalog.
fn parse_table_reference(name: &str) -> Result<TableReference, Error> {
    match TableReference::parse_str(name) {
        table_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => Ok(table_ref),
        TableReference::Full { catalog, .. } => ViewNameIncludesCatalogSnafu {
            catalog,
            name: name.to_string(),
        }
        .fail(),
    }
}

impl TryFrom<spicepod_view::View> for ViewBuilder {
    type Error = Error;

    fn try_from(view: spicepod_view::View) -> Result<Self, Self::Error> {
        crate::validate_identifier(&view.name).context(InvalidViewNameSnafu)?;

        let table_reference = parse_table_reference(&view.name)?;

        let sql = if let Some(view_sql) = &view.sql {
            view_sql.clone()
        } else if let Some(sql_ref) = &view.sql_ref {
            load_sql_ref(sql_ref)?
        } else {
            return NeedToSpecifySQLViewSnafu {
                name: table_reference.to_string(),
            }
            .fail();
        };

        let metadata = view.metadata();

        // `acceleration.ready_state` is a legitimate member of the acceleration block, so it
        // parses cleanly on a view as well as on a dataset. A dataset reads it out of the block
        // and applies it; resolve it the same way here so the key means one thing wherever it is
        // written, rather than being accepted and dropped on one of the two components. See
        // `DatasetBuilder::try_from` for the dataset side. The deprecation is reported by the
        // runtime load path, not from this conversion, which read-only callers run too.
        #[expect(deprecated)]
        let ready_state = match view.acceleration.as_ref().map(|a| a.ready_state) {
            Some(Some(ready_state)) => ReadyState::from(ready_state),
            _ => ReadyState::from(view.ready_state),
        };

        let acceleration = view
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()
            .context(InvalidAccelerationConfigurationSnafu)?;

        // verify that the acceleration configuration is fully supported
        if let Some(acc) = &acceleration {
            if acc.refresh_mode.is_some()
                && acc.refresh_mode != Some(acceleration::RefreshMode::Full)
            {
                return AcceleratedViewInvalidConfigurationSnafu {
                    view_name: view.name,
                    reason: "Only 'refresh_mode: full' is supported",
                }
                .fail();
            }

            if acc.refresh_sql.is_some() {
                return AcceleratedViewInvalidConfigurationSnafu {
                    view_name: view.name,
                    reason: "'refresh_sql' is not supported",
                }
                .fail();
            }
        }

        Ok(ViewBuilder {
            name: table_reference,
            sql,
            metadata,
            columns: view.columns,
            acceleration,
            ready_state,
            vectors: view.vectors,
            params: view
                .params
                .as_ref()
                .map(spicepod::param::Params::as_string_map)
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ReadyState, ViewBuilder};
    use datafusion::common::TableReference;
    use spicepod::component::view as spicepod_view;
    use std::io::Write;

    fn parse(view_yaml: &str) -> Result<ViewBuilder, Error> {
        let view: spicepod_view::View = yaml::from_str(view_yaml).expect("view yaml parses");
        ViewBuilder::try_from(view)
    }

    /// Resolves a view from its Spicepod YAML, so the test covers the same parse that a
    /// `spicepod.yaml` goes through rather than a hand-built struct that could disagree with it.
    fn ready_state_of(view_yaml: &str) -> ReadyState {
        parse(view_yaml).expect("view builds").ready_state
    }

    /// Regression test for #13615. The key parses on a view whether or not anything reads it, so
    /// assert the value the built view carries rather than that the Spicepod was accepted.
    #[test]
    fn acceleration_ready_state_is_applied_to_a_view() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
acceleration:
  enabled: true
  ready_state: on_registration
",
        );

        assert_eq!(
            ready_state,
            ReadyState::OnRegistration,
            "a view's `acceleration.ready_state` must reach the built view"
        );
    }

    /// The block being switched off does not discard the setting, matching the dataset. That is
    /// what `spicepod`'s `CONSUMED_WHEN_DISABLED` relies on when it leaves `ready_state` out of
    /// the "discarded because `enabled: false`" warning.
    #[test]
    fn acceleration_ready_state_is_applied_even_when_acceleration_is_disabled() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
acceleration:
  enabled: false
  ready_state: on_schema_resolved
",
        );

        assert_eq!(ready_state, ReadyState::OnSchemaResolved);
    }

    /// The deprecated key wins over the view's own field, the same precedence `DatasetBuilder`
    /// applies, so the two components cannot resolve the same pair of settings differently.
    ///
    /// Both values are non-default and differ from each other. `on_load` would be useless on
    /// either side: it is the `#[default]`, so a written-out `ready_state: on_load` is
    /// indistinguishable from an omitted one, and the assertion would hold for an implementation
    /// that ignored one of the two fields entirely.
    #[test]
    fn acceleration_ready_state_takes_precedence_over_the_views_own_field() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
ready_state: on_schema_resolved
acceleration:
  enabled: true
  ready_state: on_registration
",
        );

        assert_eq!(
            ready_state,
            ReadyState::OnRegistration,
            "the acceleration block's value must win over the view's own"
        );
    }

    #[test]
    fn the_views_own_ready_state_is_used_when_the_acceleration_block_omits_it() {
        let ready_state = ready_state_of(
            r"
name: daily_totals
sql: SELECT 1
ready_state: on_registration
acceleration:
  enabled: true
",
        );

        assert_eq!(ready_state, ReadyState::OnRegistration);
    }

    #[test]
    fn a_view_with_no_acceleration_block_uses_its_own_ready_state() {
        assert_eq!(
            ready_state_of(
                r"
name: daily_totals
sql: SELECT 1
ready_state: on_schema_resolved
"
            ),
            ReadyState::OnSchemaResolved
        );
        assert_eq!(
            ready_state_of(
                r"
name: daily_totals
sql: SELECT 1
"
            ),
            ReadyState::OnLoad,
            "an unset `ready_state` keeps the default"
        );
    }

    #[test]
    fn sql_ref_is_loaded_from_the_file() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        write!(file, "SELECT 2").expect("write sql");
        let path = file.path().to_str().expect("utf-8 path");

        let builder = parse(&format!("name: v\nsql_ref: {path:?}\n")).expect("view builds");
        assert_eq!(builder.sql, "SELECT 2");
    }

    #[test]
    fn inline_sql_takes_precedence_over_sql_ref() {
        let builder = parse("name: v\nsql: SELECT 1\nsql_ref: /does/not/exist.sql\n")
            .expect("inline sql must win without reading `sql_ref`");
        assert_eq!(builder.sql, "SELECT 1");
    }

    #[test]
    fn a_missing_sql_ref_file_is_reported() {
        let err = parse("name: v\nsql_ref: /does/not/exist.sql\n")
            .err()
            .expect("a missing file must fail");
        assert!(matches!(err, Error::UnableToLoadSqlFile { .. }), "{err}");
        assert!(
            err.to_string()
                .starts_with("Unable to load SQL file /does/not/exist.sql: "),
            "{err}"
        );
    }

    #[test]
    fn a_view_without_sql_is_rejected() {
        let err = parse("name: v\n").err().expect("a view needs SQL");
        assert!(matches!(err, Error::NeedToSpecifySQLView { ref name } if name == "v"));
    }

    #[test]
    fn view_names_are_validated_and_parsed() {
        let builder = parse("name: my_schema.v\nsql: SELECT 1\n").expect("view builds");
        assert_eq!(builder.name, TableReference::partial("my_schema", "v"));

        let err = parse("name: \"v; DROP TABLE t\"\nsql: SELECT 1\n")
            .err()
            .expect("an invalid identifier must fail");
        assert!(matches!(err, Error::InvalidViewName { .. }), "{err}");

        let err = parse("name: c.s.v\nsql: SELECT 1\n")
            .err()
            .expect("a catalog in the name must fail");
        assert!(
            matches!(err, Error::ViewNameIncludesCatalog { ref catalog, .. } if &**catalog == "c"),
            "{err}"
        );
    }

    #[test]
    fn only_full_refresh_is_supported_for_accelerated_views() {
        parse("name: v\nsql: SELECT 1\nacceleration:\n  enabled: true\n  refresh_mode: full\n")
            .expect("`refresh_mode: full` is supported");

        let err = parse(
            "name: v\nsql: SELECT 1\nacceleration:\n  enabled: true\n  refresh_mode: append\n",
        )
        .err()
        .expect("`refresh_mode: append` must fail");
        assert_eq!(
            err.to_string(),
            "Configuration of 'v' view is invalid: Only 'refresh_mode: full' is supported. Update the configuration and retry. For details, visit: https://spiceai.org/docs/components/views"
        );

        let err = parse(
            "name: v\nsql: SELECT 1\nacceleration:\n  enabled: true\n  refresh_sql: SELECT 1\n",
        )
        .err()
        .expect("`refresh_sql` must fail");
        assert!(
            matches!(err, Error::AcceleratedViewInvalidConfiguration { ref reason, .. } if reason == "'refresh_sql' is not supported"),
            "{err}"
        );
    }

    #[test]
    fn build_carries_every_field_into_the_spec() {
        let spec = parse(
            r"
name: s.v
sql: SELECT 1
description: a view
params:
  file_format: parquet
acceleration:
  enabled: true
",
        )
        .expect("view builds")
        .build();

        assert_eq!(spec.name, TableReference::partial("s", "v"));
        assert_eq!(&*spec.sql, "SELECT 1");
        assert_eq!(
            spec.metadata.get("description").map(String::as_str),
            Some("a view")
        );
        assert_eq!(
            spec.params.get("file_format").map(String::as_str),
            Some("parquet")
        );
        assert!(spec.is_accelerated());
    }
}
