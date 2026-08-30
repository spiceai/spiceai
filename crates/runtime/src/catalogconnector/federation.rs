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

//! Federation policy shared by the SQL catalog connectors.
//!
//! A catalog connector builds its own table factory, and that factory decides
//! two things a dataset connector's factory also decides: whether a plan may
//! federate at all, and which functions may be pushed into the SQL sent to the
//! remote database. Both defaults are permissive — federation on, no function
//! deny-list — so a factory built with a bare `::new(pool)` unparses every
//! Spice-only UDF (`json_get_str` and the rest of the JSON set, the
//! embedding/distance UDFs, every user-registered function) verbatim into the
//! remote statement, and the remote engine answers with an unknown-function
//! error. See issue #10703 for the original report and #13664 for the catalog
//! side of it.
//!
//! The deny-list itself is backend-specific — it carves out the functions each
//! backend's unparser dialect rewrites into real remote SQL — so each connector
//! names its own. What lives here is what they share: the `query_federation`
//! parameter's spelling, its default, and the one place the catalog connectors
//! turn its value into a `bool`, so no two catalogs accept different spellings.
//!
//! This is not yet the single source of truth across the whole runtime: the ADBC
//! *dataset* connector still declares and parses `query_federation` itself, so
//! the two agree only by matching. #13743 tracks folding both onto one
//! definition.

use runtime_parameters::Parameters;
use snafu::prelude::*;

/// The `query_federation` parameter, spelled and defaulted exactly as the ADBC
/// dataset connector spells it so one value means one thing across both.
pub(crate) const QUERY_FEDERATION_PARAMETER: super::ParameterSpec =
    super::ParameterSpec::runtime("query_federation")
        .description(
            "Enable or disable query federation for this catalog. Valid values: 'enabled' (default), 'disabled'.",
        )
        .default("enabled");

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display(
        "Invalid `query_federation` value '{value}'. Expected 'enabled' or 'disabled'."
    ))]
    InvalidQueryFederation { value: String },
}

/// Whether query federation is enabled for a catalog, from its
/// `query_federation` parameter. Absent means enabled, matching the dataset
/// connector's default.
pub(crate) fn is_query_federation_enabled(params: &Parameters) -> Result<bool, Error> {
    match params.get("query_federation").expose().ok() {
        None | Some("enabled") => Ok(true),
        Some("disabled") => Ok(false),
        Some(other) => InvalidQueryFederationSnafu {
            value: other.to_string(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters::ParameterType;
    use secrecy::SecretString;

    const TEST_PARAMETERS: &[super::super::ParameterSpec] = &[QUERY_FEDERATION_PARAMETER];

    fn params_with(entries: Vec<(&str, &str)>) -> Parameters {
        Parameters::new(
            entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
                .collect(),
            "test",
            TEST_PARAMETERS,
        )
    }

    #[test]
    fn absent_query_federation_is_enabled() {
        assert!(
            is_query_federation_enabled(&params_with(vec![]))
                .expect("absent 'query_federation' should parse")
        );
    }

    #[test]
    fn explicit_values_parse() {
        assert!(
            is_query_federation_enabled(&params_with(vec![("query_federation", "enabled")]))
                .expect("'enabled' should parse")
        );
        assert!(
            !is_query_federation_enabled(&params_with(vec![("query_federation", "disabled")]))
                .expect("'disabled' should parse")
        );
    }

    #[test]
    fn an_invalid_value_names_the_parameter_and_the_accepted_values() {
        let err = is_query_federation_enabled(&params_with(vec![("query_federation", "off")]))
            .expect_err("'off' is not an accepted value");
        let message = err.to_string();
        assert!(message.contains("query_federation"), "{message}");
        assert!(message.contains("'off'"), "{message}");
        assert!(message.contains("'enabled'"), "{message}");
        assert!(message.contains("'disabled'"), "{message}");
    }

    /// The parameter must stay a `runtime` parameter, and keep the `enabled`
    /// default. A `component` parameter is prefixed with the connector's prefix
    /// when it is surfaced to users, so the same setting would be spelled
    /// `adbc_query_federation` on a catalog and `query_federation` on a dataset
    /// — two names for one decision, which is the drift this shared constant
    /// exists to prevent. The default is asserted here because it is what makes
    /// installing the deny-list a non-breaking change: a catalog that says
    /// nothing keeps federating exactly as it did.
    #[test]
    fn the_parameter_is_spelled_the_way_the_dataset_connector_spells_it() {
        assert!(matches!(
            QUERY_FEDERATION_PARAMETER.r#type,
            ParameterType::Runtime
        ));
        assert_eq!(QUERY_FEDERATION_PARAMETER.name, "query_federation");
        assert_eq!(QUERY_FEDERATION_PARAMETER.default, Some("enabled"));
    }
}
