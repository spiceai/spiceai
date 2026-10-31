/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Runtime components (`dataset`/`catalog`/`view`).
//!
//! The pure-configuration cores of these components — and the component-level
//! helpers below — live in the [`runtime_component`] crate, which sits *below*
//! `runtime` so connectors can name a component's configuration without pulling
//! in the orchestrator. This module keeps the `Arc<Runtime>`-bound wrappers
//! (`dataset::Dataset`, `catalog::Catalog`, `view::View`) and re-exports the
//! moved items so existing `crate::component::…` paths keep resolving during the
//! migration.

// Component-level config helpers + config-only submodules moved down to
// `runtime-component`. Re-exported here for path compatibility.
pub use runtime_component::{
    ComponentInitialization, DatasetHealthMonitor, Error, StartupOptions, access, column,
    find_first_delimiter, validate_identifier,
};

use std::fmt::Write;

// The `Arc<Runtime>`-bound wrappers stay in `runtime`.
pub mod catalog;
pub mod dataset;
pub mod view;

/// Which component an acceleration block was written on. A closed set rather than
/// a `&str`, so a new component that grows an `acceleration:` block cannot reach
/// [`disabled_acceleration_warning`] without deciding on its noun and its
/// reference page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceleratedComponent {
    Dataset,
    View,
}

impl AcceleratedComponent {
    /// The component word as the operator's Spicepod spells it, lower case, for
    /// mid-sentence use.
    const fn noun(self) -> &'static str {
        match self {
            Self::Dataset => "dataset",
            Self::View => "view",
        }
    }

    /// The same word capitalised, to open a log line with.
    const fn titled_noun(self) -> &'static str {
        match self {
            Self::Dataset => "Dataset",
            Self::View => "View",
        }
    }

    /// The `acceleration` section of *this* component's Spicepod reference. A view
    /// pointed at the datasets page is pointed at a block it does not have.
    const fn acceleration_reference_url(self) -> &'static str {
        match self {
            Self::Dataset => "https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
            Self::View => "https://spiceai.org/docs/reference/spicepod/views#acceleration",
        }
    }

    /// This component's Spicepod reference, unanchored. The `#acceleration` anchor
    /// is wrong for a setting whose remedy is a field *outside* the acceleration
    /// block, which is what the `ready_state` deprecation asks for.
    const fn reference_url(self) -> &'static str {
        match self {
            Self::Dataset => "https://spiceai.org/docs/reference/spicepod/datasets",
            Self::View => "https://spiceai.org/docs/reference/spicepod/views",
        }
    }
}

/// The warning that a component set `acceleration.ready_state`, which is honoured
/// but deprecated in favour of the component's own top-level `ready_state`. Emitted
/// by the load path (`init::dataset::warn_about_acceleration_block`).
///
/// One function for both components because it is one deprecation of one key: a
/// dataset and a view that write the same thing should be told the same thing,
/// and a second copy is where the two drift apart.
///
/// Built as a pure function so the wording an operator acts on — which component,
/// the field to move the setting to, and the reference page — is asserted by a
/// test rather than through whatever a log capture happens to retain.
pub(crate) fn deprecated_ready_state_warning(
    component: AcceleratedComponent,
    name: &str,
) -> String {
    // `escape_debug` for the same reason as `disabled_acceleration_warning`:
    // `validate_identifier` accepts a *quoted* identifier, and a quoted one may
    // legally contain a newline, so a validated name can still break this line in
    // two and forge a second one.
    let name = name.escape_debug();
    let noun = component.noun();
    let reference = component.reference_url();
    format!(
        "{titled} '{name}' sets `acceleration.ready_state`, which is deprecated and will be removed. \
        Move the setting to the {noun}'s own `ready_state` to keep it working. \
        See: {reference}",
        titled = component.titled_noun()
    )
}

/// The guide every deprecated-engine warning points at. It carries what one line cannot:
/// which accelerations are safe to rebuild from their source, and, per engine, where its
/// acceleration files and snapshots are, so the user can delete them.
pub(crate) const CAYENNE_MIGRATION_DOCS: &str =
    "https://spiceai.org/docs/components/data-accelerators/cayenne/migration";

/// How many components the deprecated-engine warning names per engine before it
/// summarizes the rest as a count, so one line stays readable for a Spicepod with
/// hundreds of accelerated datasets.
const DEPRECATED_ENGINE_NAMED_COMPONENTS: usize = 5;

/// The one warning for the enabled accelerations among `accelerations` that use an
/// engine other than Cayenne, or `None` when none do. Logged once for the whole
/// Spicepod at startup, and once per hot reload for the components it added or changed.
///
/// One line for the whole Spicepod rather than one per component: the deprecation is a
/// single decision with a single remedy, and a Spicepod with hundreds of `duckdb`
/// datasets should not bury every other startup line under it. Components are grouped
/// by engine, in engine-name order, and named in the order the Spicepod declares them,
/// up to [`DEPRECATED_ENGINE_NAMED_COMPONENTS`] each.
///
/// An acceleration that leaves `engine` unset is judged by the engine it resolves to,
/// and one whose `engine` is not a known engine is left to the load path, which reports
/// it as an error. Disabled accelerations are skipped: they use no engine.
///
/// The remedy also says to delete the old engine's files and snapshots: Cayenne fills a
/// new acceleration from its source and neither reads nor removes them. Which files, and
/// which accelerations must not be rebuilt, is left to the linked guide.
pub(crate) fn deprecated_acceleration_engines_warning<'a>(
    accelerations: impl IntoIterator<
        Item = (
            AcceleratedComponent,
            &'a str,
            &'a spicepod::acceleration::Acceleration,
        ),
    >,
) -> Option<String> {
    let mut by_engine: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for (component, name, acceleration) in accelerations {
        if !acceleration.enabled {
            continue;
        }
        let Ok(engine) = runtime_acceleration::Engine::try_from(acceleration.engine_name()) else {
            continue;
        };
        if engine == runtime_acceleration::Engine::Cayenne {
            continue;
        }
        // Escaped for the same reason as `disabled_acceleration_warning`: a quoted
        // Spicepod identifier may carry a newline through validation.
        by_engine
            .entry(engine.to_string())
            .or_default()
            .push(format!("{} '{}'", component.noun(), name.escape_debug()));
    }
    if by_engine.is_empty() {
        return None;
    }

    let engines = by_engine
        .iter()
        .map(|(engine, components)| {
            let mut named = components
                .iter()
                .take(DEPRECATED_ENGINE_NAMED_COMPONENTS)
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            let unnamed = components
                .len()
                .saturating_sub(DEPRECATED_ENGINE_NAMED_COMPONENTS);
            if unnamed > 0 {
                let _ = write!(named, " and {unnamed} more");
            }
            format!("`{engine}` ({named})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "Acceleration engines other than `cayenne` are deprecated and will be removed in v2.5, so these accelerations must move to `cayenne` before upgrading: {engines}. Set `acceleration.engine: cayenne`, or remove `engine` to use the default, which is `cayenne`, then delete the old engine's acceleration files and snapshots. See: {CAYENNE_MIGRATION_DOCS}"
    ))
}

/// The acceleration blocks of `datasets` and `views`, in the shape
/// [`deprecated_acceleration_engines_warning`] reads: datasets first, each in declared
/// order.
pub(crate) fn component_accelerations<'a>(
    datasets: impl IntoIterator<Item = &'a spicepod::component::dataset::Dataset>,
    views: impl IntoIterator<Item = &'a spicepod::component::view::View>,
) -> impl Iterator<
    Item = (
        AcceleratedComponent,
        &'a str,
        &'a spicepod::acceleration::Acceleration,
    ),
> {
    let datasets = datasets.into_iter().filter_map(|ds| {
        ds.acceleration
            .as_ref()
            .map(|a| (AcceleratedComponent::Dataset, ds.name.as_str(), a))
    });
    let views = views.into_iter().filter_map(|view| {
        view.acceleration
            .as_ref()
            .map(|a| (AcceleratedComponent::View, view.name.as_str(), a))
    });
    datasets.chain(views)
}

/// What to tell an operator whose dataset or view sets `acceleration.enabled:
/// false` and leaves settings in the block that the runtime will not apply.
///
/// A function rather than an inline `tracing::warn!` so the wording — which is
/// the whole of this feature for the person reading the log — is assertable,
/// and shared between the two components rather than written twice: a dataset
/// and a view discard the same block for the same reason, and an operator who
/// learns to act on one message should not have to learn a second.
///
/// `component` is which component is being reported — it decides both the noun in
/// the message and which Spicepod reference page the reader is sent to.
///
/// Single quotes around the name the operator chose, backticks around the config
/// keys they are being told to act on, per the repo's message convention — and
/// the name escaped, since a quoted Spicepod identifier can carry a newline
/// through validation and would otherwise forge a second log line.
///
/// It names only the fields it was given, and does not say "the rest of the
/// block": `ready_state` is excluded by `CONSUMED_WHEN_DISABLED`, so a claim
/// about everything under `enabled` would be untrue.
///
/// The remedy sentence is load-bearing and constrains what may be passed in
/// `ignored`: it promises that removing `enabled: false` applies these
/// settings. Only pass fields for which that is true.
pub(crate) fn disabled_acceleration_warning(
    component: AcceleratedComponent,
    name: &str,
    ignored: &[String],
) -> String {
    let keys = ignored
        .iter()
        .map(|field| format!("`{field}`"))
        .collect::<Vec<_>>()
        .join(", ");
    // `escape_debug` rather than the raw name: `validate_identifier` accepts a
    // *quoted* identifier, and a quoted one may legally contain a newline, so a
    // validated name can still break this line in two and forge a second one.
    let name = name.escape_debug();
    let noun = component.noun();
    let reference = component.acceleration_reference_url();
    format!(
        "{titled} '{name}' sets `acceleration.enabled: false`, so these settings in its acceleration block are read and then ignored: {keys}. Remove `enabled: false` to apply them, or remove them to keep the {noun} unaccelerated. See: {reference}",
        titled = component.titled_noun()
    )
}

#[cfg(test)]
mod tests {
    use super::{
        AcceleratedComponent, deprecated_acceleration_engines_warning,
        deprecated_ready_state_warning, disabled_acceleration_warning,
    };

    #[test]
    fn the_warning_names_the_component_the_fields_and_the_remedy() {
        // Everything a reader needs to act, in one line: which dataset, what is
        // being dropped, and the two ways out. Asserted because the message is
        // the entire user-visible behaviour of this path (#13514).
        let warning = disabled_acceleration_warning(
            AcceleratedComponent::Dataset,
            "api_data",
            &["engine".to_string(), "refresh_mode".to_string()],
        );
        // Quoting and backticking are the repo's convention, not decoration: an
        // unquoted name vanishes when it is empty and reads as prose when it is
        // a word like `orders`.
        assert!(warning.starts_with("Dataset 'api_data'"), "{warning}");
        assert!(warning.contains("`engine`, `refresh_mode`"), "{warning}");
        assert!(warning.contains("acceleration.enabled: false"), "{warning}");
        assert!(warning.contains("Remove `enabled: false`"), "{warning}");
        assert!(
            warning.contains("keep the dataset unaccelerated"),
            "{warning}"
        );
        assert!(
            warning.contains("/reference/spicepod/datasets#acceleration"),
            "{warning}"
        );
    }

    #[test]
    fn the_warning_calls_a_view_a_view_and_links_to_the_views_reference() {
        // An operator greps the log for the component they were editing. A view
        // reported as a "dataset" sends them to the wrong block of the Spicepod,
        // the remedy sentence has to agree with the noun, and the link has to
        // land on a page that documents the block they actually wrote.
        //
        // Asserted on the specific phrases rather than `!contains("dataset")`:
        // the datasets *URL* legitimately contains that substring, so the blunt
        // form fails on a correct message.
        let warning = disabled_acceleration_warning(
            AcceleratedComponent::View,
            "daily_totals",
            &["engine".to_string()],
        );
        assert!(warning.starts_with("View 'daily_totals'"), "{warning}");
        assert!(warning.contains("keep the view unaccelerated"), "{warning}");
        assert!(
            warning.contains("/reference/spicepod/views#acceleration"),
            "{warning}"
        );
        assert!(!warning.contains("Dataset"), "{warning}");
        assert!(!warning.contains("the dataset"), "{warning}");
    }

    #[test]
    fn a_control_character_in_the_name_cannot_break_the_line_in_two() {
        // `validate_identifier` accepts a quoted identifier, and a quoted one
        // may contain a newline — so the name reaching this message is not
        // guaranteed to be one line, and an unescaped one would let a name
        // write a second log line of its own choosing.
        let warning = disabled_acceleration_warning(
            AcceleratedComponent::Dataset,
            "api\nWARN forged",
            &["engine".to_string()],
        );
        assert!(
            !warning.contains('\n'),
            "the message must stay one line: {warning}"
        );
        assert!(
            warning.contains("api\\nWARN forged"),
            "the name must still be readable, escaped: {warning}"
        );
    }

    #[test]
    fn the_warning_claims_only_the_fields_it_was_given() {
        // `ready_state` is never one of the reported fields, so the message must
        // not claim the whole block is ignored — a reader who saw that would go
        // looking for a `ready_state` that is not broken in the way implied.
        // This function must not widen the claim past the list it was handed.
        let warning = disabled_acceleration_warning(
            AcceleratedComponent::Dataset,
            "api_data",
            &["engine".to_string()],
        );
        assert!(
            !warning.contains("the rest of"),
            "the message must scope itself to the listed fields: {warning}"
        );
        assert!(!warning.contains("ready_state"), "{warning}");
    }

    #[test]
    fn the_ready_state_deprecation_names_the_replacement_and_the_components_reference() {
        // The operator has to learn three things from this line: that the key is
        // going away, which field to move it to, and where that field is written
        // up. The reference is the component's page *unanchored* — the remedy is
        // a top-level field, so `#acceleration` would land them back inside the
        // block they are being told to move the setting out of.
        let warning = deprecated_ready_state_warning(AcceleratedComponent::Dataset, "api_data");
        assert!(warning.starts_with("Dataset 'api_data'"), "{warning}");
        assert!(warning.contains("`acceleration.ready_state`"), "{warning}");
        assert!(warning.contains("deprecated"), "{warning}");
        assert!(
            warning.contains("the dataset's own `ready_state`"),
            "{warning}"
        );
        assert!(
            warning.contains("/reference/spicepod/datasets"),
            "{warning}"
        );
        assert!(!warning.contains("#acceleration"), "{warning}");
        assert!(!warning.contains('\n'), "{warning}");
    }

    #[test]
    fn the_ready_state_deprecation_calls_a_view_a_view() {
        // One function serves both components, so the noun and the link have to
        // follow the component rather than defaulting to the dataset — a view
        // told to edit "the dataset's" field is sent to a component it is not.
        //
        // Asserted on the specific phrases rather than `!contains("dataset")`:
        // the datasets URL legitimately contains that substring.
        let warning = deprecated_ready_state_warning(AcceleratedComponent::View, "daily_totals");
        assert!(warning.starts_with("View 'daily_totals'"), "{warning}");
        assert!(
            warning.contains("the view's own `ready_state`"),
            "{warning}"
        );
        assert!(warning.contains("/reference/spicepod/views"), "{warning}");
        assert!(!warning.contains("Dataset"), "{warning}");
        assert!(!warning.contains("the dataset"), "{warning}");
    }

    #[test]
    fn a_control_character_in_the_name_cannot_break_the_ready_state_line_in_two() {
        // Same exposure as `disabled_acceleration_warning`, and the same reason:
        // `validate_identifier` accepts a quoted identifier, which may legally
        // carry a newline, so an unescaped name could write a second log record.
        let warning =
            deprecated_ready_state_warning(AcceleratedComponent::View, "api\nWARN forged");
        assert!(
            !warning.contains('\n'),
            "the message must stay one line: {warning}"
        );
        assert!(
            warning.contains("WARN forged"),
            "the name is still reported, only escaped: {warning}"
        );
    }

    fn acceleration(engine: Option<&str>) -> spicepod::acceleration::Acceleration {
        spicepod::acceleration::Acceleration {
            engine: engine.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn the_deprecated_engine_warning_is_one_line_grouped_by_engine() {
        let duckdb = acceleration(Some("duckdb"));
        let sqlite = acceleration(Some("sqlite"));
        let postgres = acceleration(Some("postgresql"));
        let cayenne = acceleration(Some("cayenne"));
        let default = acceleration(None);
        let warning = deprecated_acceleration_engines_warning([
            (AcceleratedComponent::Dataset, "orders", &duckdb),
            (AcceleratedComponent::Dataset, "customers", &sqlite),
            (AcceleratedComponent::View, "daily_totals", &duckdb),
            (AcceleratedComponent::Dataset, "events", &cayenne),
            (AcceleratedComponent::Dataset, "lines", &default),
            (AcceleratedComponent::Dataset, "ledger", &postgres),
        ])
        .expect("deprecated engines are in use");
        insta::assert_snapshot!("deprecated_acceleration_engines_warning", warning);
    }

    #[test]
    fn the_deprecated_engine_warning_counts_components_past_the_named_ones() {
        let arrow = acceleration(Some("arrow"));
        let names: Vec<String> = (1..=8).map(|i| format!("ds{i}")).collect();
        let warning = deprecated_acceleration_engines_warning(
            names
                .iter()
                .map(|name| (AcceleratedComponent::Dataset, name.as_str(), &arrow)),
        )
        .expect("arrow is deprecated");
        assert!(
            warning.contains(
                "`arrow` (dataset 'ds1', dataset 'ds2', dataset 'ds3', dataset 'ds4', dataset 'ds5' and 3 more)"
            ),
            "{warning}"
        );
        assert!(!warning.contains("ds6"), "{warning}");
    }

    #[test]
    fn no_deprecated_engine_warning_without_a_deprecated_engine_in_use() {
        let cayenne = acceleration(Some("cayenne"));
        let vortex = acceleration(Some("vortex"));
        let default = acceleration(None);
        let unknown = acceleration(Some("not_an_engine"));
        let disabled_duckdb = spicepod::acceleration::Acceleration {
            enabled: false,
            ..acceleration(Some("duckdb"))
        };
        assert_eq!(
            deprecated_acceleration_engines_warning([
                (AcceleratedComponent::Dataset, "a", &cayenne),
                (AcceleratedComponent::Dataset, "b", &vortex),
                (AcceleratedComponent::View, "c", &default),
                (AcceleratedComponent::Dataset, "d", &unknown),
                (AcceleratedComponent::Dataset, "e", &disabled_duckdb),
            ]),
            None
        );
    }

    #[test]
    fn a_control_character_in_the_name_cannot_break_the_deprecated_engine_line_in_two() {
        let duckdb = acceleration(Some("duckdb"));
        let warning = deprecated_acceleration_engines_warning([(
            AcceleratedComponent::Dataset,
            "api\nWARN forged",
            &duckdb,
        )])
        .expect("duckdb is deprecated");
        assert!(
            !warning.contains('\n'),
            "the message must stay one line: {warning}"
        );
        assert!(
            warning.contains("WARN forged"),
            "the name is still reported, only escaped: {warning}"
        );
    }
}
