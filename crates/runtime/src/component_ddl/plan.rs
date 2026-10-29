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

//! What a component statement changes in the SQL-defined components.
//!
//! A component belongs to where it is defined, and the Spicepod's components
//! are the ones the app built from the Spicepods declares, before SQL's are
//! added. SQL creates a name no Spicepod component uses, and changes or drops
//! only what SQL defined. When a Spicepod later declares a name SQL defined,
//! the Spicepod's component is the one served, and SQL may still drop its own
//! definition of the name, which leaves the served one as it is.

use app::App;
use datafusion::common::{ResolvedTableReference, TableReference};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_ddl::component_ddl::{ComponentDdlOp, OptionsChange};
use datafusion_ddl::component_options::{
    ComponentKind, ConnectorParameters, component_with_options, component_without_options,
};
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;

use super::store::SqlDefinitions;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

/// What applying a statement changes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Planned {
    /// The SQL-defined components after the statement.
    pub definitions: SqlDefinitions,
    /// What the statement reports.
    pub message: String,
    /// The definition the statement creates or changes, to validate before it
    /// is applied.
    pub validate: Option<Component>,
}

/// A component definition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Component {
    Dataset(Box<Dataset>),
    Catalog(Box<Catalog>),
}

/// A dataset name as SQL resolves it, so `orders`, `public.orders`, and
/// `spice.public.orders` are one dataset.
pub(crate) fn resolved_dataset_name(name: &str) -> ResolvedTableReference {
    TableReference::parse_str(name).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
}

fn same_dataset(a: &str, b: &str) -> bool {
    resolved_dataset_name(a) == resolved_dataset_name(b)
}

/// Whether a Spicepod declares a dataset or view named `name`.
fn spicepod_declares_dataset(spicepod: &App, name: &str) -> bool {
    spicepod
        .datasets
        .iter()
        .any(|ds| same_dataset(&ds.name, name))
        || spicepod
            .views
            .iter()
            .any(|view| same_dataset(&view.name, name))
}

fn spicepod_declares_catalog(spicepod: &App, name: &str) -> bool {
    spicepod.catalogs.iter().any(|c| c.name == name)
}

/// Plan `op` against `spicepod`, the app the Spicepods declare, and the
/// components SQL defined. `parameters` are those of the connector an `ALTER`
/// sets options for, which tell its parameters from the component's fields.
pub(crate) fn plan(
    spicepod: &App,
    defined: &SqlDefinitions,
    op: &ComponentDdlOp,
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<Planned> {
    match op {
        ComponentDdlOp::CreateDataset {
            dataset,
            if_not_exists,
            or_replace,
        } => create_dataset(spicepod, defined, dataset, *if_not_exists, *or_replace),
        ComponentDdlOp::AlterDataset { name, change } => {
            alter_dataset(spicepod, defined, name, change, parameters)
        }
        ComponentDdlOp::DropDataset { name, if_exists } => {
            drop_dataset(spicepod, defined, name, *if_exists)
        }
        ComponentDdlOp::CreateCatalog {
            catalog,
            if_not_exists,
            or_replace,
        } => create_catalog(spicepod, defined, catalog, *if_not_exists, *or_replace),
        ComponentDdlOp::AlterCatalog { name, change } => {
            alter_catalog(spicepod, defined, name, change, parameters)
        }
        ComponentDdlOp::DropCatalog { name, if_exists } => {
            drop_catalog(spicepod, defined, name, *if_exists)
        }
    }
}

fn unchanged(defined: &SqlDefinitions, message: String) -> Planned {
    Planned {
        definitions: defined.clone(),
        message,
        validate: None,
    }
}

fn create_dataset(
    spicepod: &App,
    defined: &SqlDefinitions,
    dataset: &Dataset,
    if_not_exists: bool,
    or_replace: bool,
) -> DFResult<Planned> {
    let name = &dataset.name;
    let display = display_dataset_name(name);
    if spicepod_declares_dataset(spicepod, name) {
        if if_not_exists {
            return Ok(unchanged(
                defined,
                format!("Dataset {display} already exists."),
            ));
        }
        return Err(DataFusionError::Plan(format!(
            "Dataset {display} is defined in the Spicepod, so SQL cannot {action} it. Create the dataset under another name, or change it in the Spicepod.",
            action = if or_replace { "replace" } else { "create" },
        )));
    }

    let mut definitions = defined.clone();
    let existing = definitions
        .datasets
        .iter()
        .position(|d| same_dataset(&d.name, name));
    let message = match existing {
        Some(_) if if_not_exists => {
            return Ok(unchanged(
                defined,
                format!("Dataset {display} already exists."),
            ));
        }
        Some(index) if or_replace => {
            if definitions.datasets[index] == *dataset {
                return Ok(unchanged(
                    defined,
                    format!("Dataset {display} is unchanged."),
                ));
            }
            definitions.datasets[index] = dataset.clone();
            format!("Replaced dataset {display}.")
        }
        Some(_) => {
            return Err(DataFusionError::Plan(format!(
                "Dataset {display} already exists. Use CREATE OR REPLACE TABLE to replace it, CREATE TABLE IF NOT EXISTS to keep it, or ALTER TABLE to change it."
            )));
        }
        None => {
            definitions.datasets.push(dataset.clone());
            format!("Created dataset {display}.")
        }
    };
    Ok(Planned {
        definitions,
        message,
        validate: Some(Component::Dataset(Box::new(dataset.clone()))),
    })
}

fn alter_dataset(
    spicepod: &App,
    defined: &SqlDefinitions,
    name: &str,
    change: &OptionsChange,
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<Planned> {
    let display = display_dataset_name(name);
    if spicepod_declares_dataset(spicepod, name) {
        return Err(DataFusionError::Plan(format!(
            "Dataset {display} is defined in the Spicepod, so SQL cannot change it. Change it in the Spicepod."
        )));
    }
    let Some(index) = defined
        .datasets
        .iter()
        .position(|d| same_dataset(&d.name, name))
    else {
        return Err(DataFusionError::Plan(format!(
            "Dataset {display} does not exist."
        )));
    };
    let current = &defined.datasets[index];
    let changed = match change {
        OptionsChange::Set(options) => component_with_options(current, options, parameters)?,
        OptionsChange::Reset(keys) => component_without_options(current, keys, parameters)?,
    };
    if changed == *current {
        return Ok(unchanged(
            defined,
            format!("Dataset {display} is unchanged."),
        ));
    }
    let mut definitions = defined.clone();
    definitions.datasets[index] = changed.clone();
    Ok(Planned {
        definitions,
        message: format!("Changed dataset {display}."),
        validate: Some(Component::Dataset(Box::new(changed))),
    })
}

fn drop_dataset(
    spicepod: &App,
    defined: &SqlDefinitions,
    name: &str,
    if_exists: bool,
) -> DFResult<Planned> {
    let display = display_dataset_name(name);
    let mut definitions = defined.clone();
    let before = definitions.datasets.len();
    definitions
        .datasets
        .retain(|d| !same_dataset(&d.name, name));
    let dropped = definitions.datasets.len() < before;

    if spicepod_declares_dataset(spicepod, name) {
        if dropped {
            return Ok(Planned {
                definitions,
                message: format!(
                    "Dropped the SQL definition of dataset {display}. The Spicepod's definition of {display} is the one served, and stays."
                ),
                validate: None,
            });
        }
        return Err(DataFusionError::Plan(format!(
            "Dataset {display} is defined in the Spicepod, so SQL cannot drop it. Remove it from the Spicepod."
        )));
    }
    if !dropped {
        if if_exists {
            return Ok(unchanged(
                defined,
                format!("Dataset {display} does not exist."),
            ));
        }
        return Err(DataFusionError::Plan(format!(
            "Dataset {display} does not exist. Use DROP TABLE IF EXISTS to ignore a dataset that does not exist."
        )));
    }
    Ok(Planned {
        definitions,
        message: format!("Dropped dataset {display}."),
        validate: None,
    })
}

fn create_catalog(
    spicepod: &App,
    defined: &SqlDefinitions,
    catalog: &Catalog,
    if_not_exists: bool,
    or_replace: bool,
) -> DFResult<Planned> {
    let name = &catalog.name;
    if spicepod_declares_catalog(spicepod, name) {
        if if_not_exists {
            return Ok(unchanged(
                defined,
                format!("Catalog '{name}' already exists."),
            ));
        }
        return Err(DataFusionError::Plan(format!(
            "Catalog '{name}' is defined in the Spicepod, so SQL cannot {action} it. Create the catalog under another name, or change it in the Spicepod.",
            action = if or_replace { "replace" } else { "create" },
        )));
    }

    let mut definitions = defined.clone();
    let existing = definitions.catalogs.iter().position(|c| c.name == *name);
    let message = match existing {
        Some(_) if if_not_exists => {
            return Ok(unchanged(
                defined,
                format!("Catalog '{name}' already exists."),
            ));
        }
        Some(index) if or_replace => {
            if definitions.catalogs[index] == *catalog {
                return Ok(unchanged(
                    defined,
                    format!("Catalog '{name}' is unchanged."),
                ));
            }
            definitions.catalogs[index] = catalog.clone();
            format!("Replaced catalog '{name}'.")
        }
        Some(_) => {
            return Err(DataFusionError::Plan(format!(
                "Catalog '{name}' already exists. Use CREATE OR REPLACE CATALOG to replace it, CREATE CATALOG IF NOT EXISTS to keep it, or ALTER CATALOG to change it."
            )));
        }
        None => {
            definitions.catalogs.push(catalog.clone());
            format!("Created catalog '{name}'.")
        }
    };
    Ok(Planned {
        definitions,
        message,
        validate: Some(Component::Catalog(Box::new(catalog.clone()))),
    })
}

fn alter_catalog(
    spicepod: &App,
    defined: &SqlDefinitions,
    name: &str,
    change: &OptionsChange,
    parameters: Option<&dyn ConnectorParameters>,
) -> DFResult<Planned> {
    if spicepod_declares_catalog(spicepod, name) {
        return Err(DataFusionError::Plan(format!(
            "Catalog '{name}' is defined in the Spicepod, so SQL cannot change it. Change it in the Spicepod."
        )));
    }
    let Some(index) = defined.catalogs.iter().position(|c| c.name == name) else {
        return Err(DataFusionError::Plan(format!(
            "Catalog '{name}' does not exist."
        )));
    };
    let current = &defined.catalogs[index];
    let changed = match change {
        OptionsChange::Set(options) => component_with_options(current, options, parameters)?,
        OptionsChange::Reset(keys) => component_without_options(current, keys, parameters)?,
    };
    if changed == *current {
        return Ok(unchanged(
            defined,
            format!("Catalog '{name}' is unchanged."),
        ));
    }
    let mut definitions = defined.clone();
    definitions.catalogs[index] = changed.clone();
    Ok(Planned {
        definitions,
        message: format!("Changed catalog '{name}'."),
        validate: Some(Component::Catalog(Box::new(changed))),
    })
}

fn drop_catalog(
    spicepod: &App,
    defined: &SqlDefinitions,
    name: &str,
    if_exists: bool,
) -> DFResult<Planned> {
    let accelerated = defined
        .catalogs
        .iter()
        .any(|c| c.name == name && c.acceleration.is_some());
    let mut definitions = defined.clone();
    let before = definitions.catalogs.len();
    definitions.catalogs.retain(|c| c.name != name);
    let dropped = definitions.catalogs.len() < before;

    if spicepod_declares_catalog(spicepod, name) {
        if dropped {
            return Ok(Planned {
                definitions,
                message: format!(
                    "Dropped the SQL definition of catalog '{name}'. The Spicepod's definition of '{name}' is the one served, and stays."
                ),
                validate: None,
            });
        }
        return Err(DataFusionError::Plan(format!(
            "Catalog '{name}' is defined in the Spicepod, so SQL cannot drop it. Remove it from the Spicepod."
        )));
    }
    if !dropped {
        if if_exists {
            return Ok(unchanged(
                defined,
                format!("Catalog '{name}' does not exist."),
            ));
        }
        return Err(DataFusionError::Plan(format!(
            "Catalog '{name}' does not exist. Use DROP CATALOG IF EXISTS to ignore a catalog that does not exist."
        )));
    }
    let message = if accelerated {
        format!(
            "Dropped catalog '{name}'. It is accelerated, so its tables stay queryable until Spice restarts."
        )
    } else {
        format!("Dropped catalog '{name}'.")
    };
    Ok(Planned {
        definitions,
        message,
        validate: None,
    })
}

/// The served SQL-defined datasets whose acceleration data the change from
/// `defined` to `next` retires: the change drops them, points them at another
/// source (a new `from` or connector parameter, which can name another
/// database, file, or table), or, made by `op`, replaces them. A dataset
/// defined anew under a name starts from its source, never from data an earlier
/// definition loaded. A definition a Spicepod component shadows was never
/// served, so the data under its name is the Spicepod's.
///
/// `op` is `None` for a change another instance made, which is known only by
/// its result: a replacement that kept the source reads as a change to the
/// definition, which keeps the data, as a Spicepod edit does.
pub(crate) fn retired_datasets(
    spicepod: &App,
    defined: &SqlDefinitions,
    next: &SqlDefinitions,
    op: Option<&ComponentDdlOp>,
) -> Vec<Dataset> {
    let replaces = |name: &str| {
        matches!(op, Some(ComponentDdlOp::CreateDataset { dataset, or_replace: true, .. })
            if same_dataset(&dataset.name, name))
    };
    defined
        .datasets
        .iter()
        .filter(|old| !spicepod_declares_dataset(spicepod, &old.name))
        .filter(|old| {
            match next
                .datasets
                .iter()
                .find(|new| same_dataset(&new.name, &old.name))
            {
                None => true,
                Some(new) => {
                    new.from != old.from
                        || new.params != old.params
                        || (replaces(&old.name) && new != *old)
                }
            }
        })
        .cloned()
        .collect()
}

/// Whether `after` defines the component `op` names differently from
/// `before`: whether a change another instance made between the two is to
/// that component, and not only to others.
pub(crate) fn changes_component(
    op: &ComponentDdlOp,
    before: &SqlDefinitions,
    after: &SqlDefinitions,
) -> bool {
    let name = op.component_name();
    match op.kind() {
        ComponentKind::Dataset => {
            let find = |defined: &SqlDefinitions| {
                defined
                    .datasets
                    .iter()
                    .find(|dataset| same_dataset(&dataset.name, name))
                    .cloned()
            };
            find(before) != find(after)
        }
        ComponentKind::Catalog => {
            let find = |defined: &SqlDefinitions| {
                defined
                    .catalogs
                    .iter()
                    .find(|catalog| catalog.name == name)
                    .cloned()
            };
            find(before) != find(after)
        }
    }
}

/// How many components `defined` holds, for a message: `2 datasets and 1
/// catalog`, `1 catalog`. A kind it holds none of is left out.
pub(crate) fn component_counts(defined: &SqlDefinitions) -> String {
    let count =
        |n: usize, one: &str, many: &str| format!("{n} {}", if n == 1 { one } else { many });
    match (defined.datasets.len(), defined.catalogs.len()) {
        (datasets, 0) => count(datasets, "dataset", "datasets"),
        (0, catalogs) => count(catalogs, "catalog", "catalogs"),
        (datasets, catalogs) => format!(
            "{} and {}",
            count(datasets, "dataset", "datasets"),
            count(catalogs, "catalog", "catalogs")
        ),
    }
}

/// What changed from `known` to `stored`, for a message: each component that
/// differs, and how, as `dataset 'orders' (dropped)` or `catalog 'lake'
/// (created)`.
pub(crate) fn changed_components(known: &SqlDefinitions, stored: &SqlDefinitions) -> Vec<String> {
    let how = |before: bool, after: bool| match (before, after) {
        (false, _) => "created",
        (true, true) => "changed",
        (true, false) => "dropped",
    };
    let mut changed = Vec::new();
    let mut datasets: Vec<&str> = Vec::new();
    for ds in known.datasets.iter().chain(&stored.datasets) {
        if datasets.iter().any(|name| same_dataset(name, &ds.name)) {
            continue;
        }
        datasets.push(&ds.name);
        let before = known
            .datasets
            .iter()
            .find(|d| same_dataset(&d.name, &ds.name));
        let after = stored
            .datasets
            .iter()
            .find(|d| same_dataset(&d.name, &ds.name));
        if before != after {
            changed.push(format!(
                "dataset {} ({})",
                display_dataset_name(&ds.name),
                how(before.is_some(), after.is_some())
            ));
        }
    }
    let mut catalogs: Vec<&str> = Vec::new();
    for catalog in known.catalogs.iter().chain(&stored.catalogs) {
        if catalogs.contains(&catalog.name.as_str()) {
            continue;
        }
        catalogs.push(&catalog.name);
        let before = known.catalogs.iter().find(|c| c.name == catalog.name);
        let after = stored.catalogs.iter().find(|c| c.name == catalog.name);
        if before != after {
            changed.push(format!(
                "catalog '{}' ({})",
                catalog.name,
                how(before.is_some(), after.is_some())
            ));
        }
    }
    changed
}

/// A dataset name in a message: its parts quoted, as SQL resolves them.
pub(crate) fn display_dataset_name(name: &str) -> String {
    let reference = TableReference::parse_str(name);
    match reference.schema() {
        Some(schema) if schema != SPICE_DEFAULT_SCHEMA => {
            format!("'{schema}.{}'", reference.table())
        }
        _ => format!("'{}'", reference.table()),
    }
}

/// `spicepod`, the app the Spicepods declare, with the SQL-defined `defined`
/// added: the app the runtime serves. A SQL-defined component whose name the
/// Spicepod declares is left out: the Spicepod's is the one served (see
/// [`warn_shadowed`]).
pub(crate) fn serve(mut spicepod: App, defined: &SqlDefinitions) -> App {
    for dataset in &defined.datasets {
        if !spicepod_declares_dataset(&spicepod, &dataset.name) {
            spicepod.datasets.push(dataset.clone());
        }
    }
    for catalog in &defined.catalogs {
        if !spicepod_declares_catalog(&spicepod, &catalog.name) {
            spicepod.catalogs.push(catalog.clone());
        }
    }
    spicepod
}

/// The warnings, one per name, for the SQL-defined components `spicepod`
/// declares too, whose SQL definitions [`serve`] leaves out.
pub(crate) fn shadowed_warnings(spicepod: &App, defined: &SqlDefinitions) -> Vec<String> {
    let datasets = defined
        .datasets
        .iter()
        .filter(|dataset| spicepod_declares_dataset(spicepod, &dataset.name))
        .map(|dataset| {
            format!(
                "Dataset {} is defined both in the Spicepod and with SQL, so the Spicepod's definition is used and the SQL one is not loaded. Run `DROP TABLE {}` to remove the SQL definition, or rename one of them.",
                display_dataset_name(&dataset.name),
                datafusion_ddl::component_options::object_name_sql(&dataset.name),
            )
        });
    let catalogs = defined
        .catalogs
        .iter()
        .filter(|catalog| spicepod_declares_catalog(spicepod, &catalog.name))
        .map(|catalog| {
            format!(
                "Catalog '{}' is defined both in the Spicepod and with SQL, so the Spicepod's definition is used and the SQL one is not loaded. Run `DROP CATALOG {}` to remove the SQL definition, or rename one of them.",
                catalog.name,
                datafusion_ddl::component_options::quote_identifier(&catalog.name),
            )
        });
    datasets.chain(catalogs).collect()
}

/// Log [`shadowed_warnings`]: when the definitions load, and when the
/// Spicepod changes, the two moments a Spicepod name can come to shadow one
/// SQL defined. A statement cannot, so applying one logs nothing.
pub(crate) fn warn_shadowed(spicepod: &App, defined: &SqlDefinitions) {
    for warning in shadowed_warnings(spicepod, defined) {
        tracing::warn!("{warning}");
    }
}

#[cfg(test)]
mod tests {
    use app::AppBuilder;
    use datafusion_ddl::component_options::{ComponentOption, OptionValue};

    use super::*;

    fn dataset(name: &str) -> Dataset {
        Dataset::new("file:data.csv", name)
    }

    fn spicepod_with_dataset(name: &str) -> App {
        AppBuilder::new("test").with_dataset(dataset(name)).build()
    }

    fn create(dataset: Dataset, if_not_exists: bool, or_replace: bool) -> ComponentDdlOp {
        ComponentDdlOp::CreateDataset {
            dataset: Box::new(dataset),
            if_not_exists,
            or_replace,
        }
    }

    fn drop(name: &str, if_exists: bool) -> ComponentDdlOp {
        ComponentDdlOp::DropDataset {
            name: name.to_string(),
            if_exists,
        }
    }

    fn plan(spicepod: &App, defined: &SqlDefinitions, op: &ComponentDdlOp) -> DFResult<Planned> {
        super::plan(spicepod, defined, op, None)
    }

    fn message(result: DFResult<Planned>) -> String {
        match result {
            Ok(planned) => planned.message,
            Err(e) => e.strip_backtrace(),
        }
    }

    #[test]
    fn create_adds_a_dataset_and_refuses_a_name_in_use() {
        let spicepod = AppBuilder::new("test").build();
        let empty = SqlDefinitions::default();
        let created =
            plan(&spicepod, &empty, &create(dataset("orders"), false, false)).expect("create");
        assert_eq!(created.message, "Created dataset 'orders'.");
        assert_eq!(created.definitions.datasets, vec![dataset("orders")]);
        assert_eq!(
            created.validate,
            Some(Component::Dataset(Box::new(dataset("orders"))))
        );

        // The same dataset under another spelling of its name.
        let again = plan(
            &spicepod,
            &created.definitions,
            &create(dataset("spice.public.orders"), false, false),
        );
        assert_eq!(
            message(again),
            "Error during planning: Dataset 'orders' already exists. Use CREATE OR REPLACE TABLE to replace it, CREATE TABLE IF NOT EXISTS to keep it, or ALTER TABLE to change it."
        );

        let kept = plan(
            &spicepod,
            &created.definitions,
            &create(dataset("orders"), true, false),
        )
        .expect("if not exists");
        assert_eq!(kept.message, "Dataset 'orders' already exists.");
        assert_eq!(kept.definitions, created.definitions);
        assert_eq!(kept.validate, None);

        let mut replacement = dataset("orders");
        replacement.description = Some("Orders".to_string());
        let replaced = plan(
            &spicepod,
            &created.definitions,
            &create(replacement.clone(), false, true),
        )
        .expect("or replace");
        assert_eq!(replaced.message, "Replaced dataset 'orders'.");
        assert_eq!(replaced.definitions.datasets, vec![replacement]);
    }

    #[test]
    fn sql_does_not_create_change_or_drop_a_spicepod_dataset() {
        let spicepod = spicepod_with_dataset("orders");
        let empty = SqlDefinitions::default();
        assert_eq!(
            message(plan(
                &spicepod,
                &empty,
                &create(dataset("orders"), false, false)
            )),
            "Error during planning: Dataset 'orders' is defined in the Spicepod, so SQL cannot create it. Create the dataset under another name, or change it in the Spicepod."
        );
        assert_eq!(
            message(plan(
                &spicepod,
                &empty,
                &create(dataset("orders"), true, false)
            )),
            "Dataset 'orders' already exists."
        );
        assert_eq!(
            message(plan(
                &spicepod,
                &empty,
                &ComponentDdlOp::AlterDataset {
                    name: "orders".to_string(),
                    change: OptionsChange::Set(vec![ComponentOption::new(
                        "description",
                        OptionValue::String("x".to_string())
                    )]),
                }
            )),
            "Error during planning: Dataset 'orders' is defined in the Spicepod, so SQL cannot change it. Change it in the Spicepod."
        );
        assert_eq!(
            message(plan(&spicepod, &empty, &drop("orders", true))),
            "Error during planning: Dataset 'orders' is defined in the Spicepod, so SQL cannot drop it. Remove it from the Spicepod."
        );
    }

    /// A Spicepod that later declares a name SQL defined is served under it;
    /// dropping the SQL definition removes only that definition, and retires
    /// no data, since the data under the name is the Spicepod dataset's.
    #[test]
    fn dropping_a_shadowed_sql_definition_leaves_the_spicepod_dataset() {
        let spicepod = spicepod_with_dataset("orders");
        let mut sql_orders = dataset("orders");
        sql_orders.from = "file:other.csv".to_string();
        let defined = SqlDefinitions {
            datasets: vec![sql_orders],
            ..SqlDefinitions::default()
        };
        assert_eq!(
            serve(spicepod.clone(), &defined).datasets,
            vec![dataset("orders")],
            "the Spicepod's dataset is served"
        );
        assert_eq!(
            shadowed_warnings(&spicepod, &defined),
            vec![
                "Dataset 'orders' is defined both in the Spicepod and with SQL, so the Spicepod's definition is used and the SQL one is not loaded. Run `DROP TABLE orders` to remove the SQL definition, or rename one of them."
                    .to_string()
            ]
        );

        let op = drop("orders", false);
        let dropped = plan(&spicepod, &defined, &op).expect("drop the SQL definition");
        assert_eq!(
            dropped.message,
            "Dropped the SQL definition of dataset 'orders'. The Spicepod's definition of 'orders' is the one served, and stays."
        );
        assert!(dropped.definitions.datasets.is_empty());
        assert_eq!(
            retired_datasets(&spicepod, &defined, &dropped.definitions, Some(&op)),
            Vec::<Dataset>::new()
        );
        assert_eq!(
            shadowed_warnings(&spicepod, &dropped.definitions),
            Vec::<String>::new()
        );
        assert_eq!(
            serve(spicepod, &dropped.definitions).datasets,
            vec![dataset("orders")]
        );
    }

    #[test]
    fn drop_removes_only_what_sql_defined_and_retires_its_data() {
        let spicepod = AppBuilder::new("test").build();
        let defined = SqlDefinitions {
            datasets: vec![dataset("orders"), dataset("sales.customers")],
            ..SqlDefinitions::default()
        };
        let op = drop("sales.customers", false);
        let dropped = plan(&spicepod, &defined, &op).expect("drop");
        assert_eq!(dropped.message, "Dropped dataset 'sales.customers'.");
        assert_eq!(dropped.definitions.datasets, vec![dataset("orders")]);
        assert_eq!(
            retired_datasets(&spicepod, &defined, &dropped.definitions, Some(&op)),
            vec![dataset("sales.customers")]
        );

        assert_eq!(
            message(plan(
                &spicepod,
                &dropped.definitions,
                &drop("sales.customers", false)
            )),
            "Error during planning: Dataset 'sales.customers' does not exist. Use DROP TABLE IF EXISTS to ignore a dataset that does not exist."
        );
        assert_eq!(
            message(plan(
                &spicepod,
                &dropped.definitions,
                &drop("sales.customers", true)
            )),
            "Dataset 'sales.customers' does not exist."
        );
    }

    #[test]
    fn a_new_source_or_a_replacement_retires_the_data_and_other_changes_keep_it() {
        let spicepod = AppBuilder::new("test").build();
        let defined = SqlDefinitions {
            datasets: vec![dataset("orders")],
            ..SqlDefinitions::default()
        };
        let set = |key: &str, value: &str| ComponentDdlOp::AlterDataset {
            name: "orders".to_string(),
            change: OptionsChange::Set(vec![ComponentOption::new(
                key,
                OptionValue::String(value.to_string()),
            )]),
        };

        let described = set("description", "Orders");
        let changed = plan(&spicepod, &defined, &described).expect("describe");
        assert_eq!(changed.message, "Changed dataset 'orders'.");
        assert_eq!(
            retired_datasets(&spicepod, &defined, &changed.definitions, Some(&described)),
            Vec::<Dataset>::new()
        );

        let moved = set("from", "file:other.csv");
        let changed = plan(&spicepod, &defined, &moved).expect("new source");
        assert_eq!(
            retired_datasets(&spicepod, &defined, &changed.definitions, Some(&moved)),
            vec![dataset("orders")]
        );

        // A connector parameter can name another database, file, or table.
        let reconnected = set("file_format", "parquet");
        let changed_parameter = plan(&spicepod, &defined, &reconnected).expect("new parameter");
        assert_eq!(
            retired_datasets(
                &spicepod,
                &defined,
                &changed_parameter.definitions,
                Some(&reconnected)
            ),
            vec![dataset("orders")]
        );
        assert_eq!(
            retired_datasets(&spicepod, &defined, &changed_parameter.definitions, None),
            vec![dataset("orders")],
            "a new connector parameter, made by another instance"
        );

        let mut replacement = dataset("orders");
        replacement.description = Some("Orders".to_string());
        let replace = create(replacement, false, true);
        let replaced = plan(&spicepod, &defined, &replace).expect("replace");
        assert_eq!(
            retired_datasets(&spicepod, &defined, &replaced.definitions, Some(&replace)),
            vec![dataset("orders")]
        );

        let again = plan(&spicepod, &changed.definitions, &moved).expect("no-op");
        assert_eq!(again.message, "Dataset 'orders' is unchanged.");

        // The same changes, made by another instance and known by their result.
        assert_eq!(
            retired_datasets(&spicepod, &defined, &changed.definitions, None),
            vec![dataset("orders")],
            "a new source"
        );
        assert_eq!(
            retired_datasets(&spicepod, &defined, &replaced.definitions, None),
            Vec::<Dataset>::new(),
            "a replacement that kept the source"
        );
        assert_eq!(
            retired_datasets(&spicepod, &defined, &SqlDefinitions::default(), None),
            vec![dataset("orders")],
            "a drop"
        );
        assert_eq!(
            changed_components(&defined, &replaced.definitions),
            vec!["dataset 'orders' (changed)".to_string()]
        );
        let lake = Catalog::new("iceberg".to_string(), "lake".to_string());
        assert_eq!(
            changed_components(
                &defined,
                &SqlDefinitions {
                    datasets: vec![dataset("customers")],
                    catalogs: vec![lake],
                }
            ),
            vec![
                "dataset 'orders' (dropped)".to_string(),
                "dataset 'customers' (created)".to_string(),
                "catalog 'lake' (created)".to_string()
            ]
        );
    }

    #[test]
    fn component_counts_read_as_a_sentence() {
        let lake = Catalog::new("iceberg".to_string(), "lake".to_string());
        assert_eq!(
            component_counts(&SqlDefinitions {
                datasets: vec![dataset("orders"), dataset("customers")],
                catalogs: vec![lake],
            }),
            "2 datasets and 1 catalog"
        );
        assert_eq!(
            component_counts(&SqlDefinitions {
                datasets: vec![dataset("orders")],
                ..SqlDefinitions::default()
            }),
            "1 dataset"
        );
        assert_eq!(
            component_counts(&SqlDefinitions {
                catalogs: vec![Catalog::new("iceberg".to_string(), "lake".to_string())],
                ..SqlDefinitions::default()
            }),
            "1 catalog"
        );
    }

    /// A conflict is retried only when the other change left the statement's
    /// own component as it was planned against.
    #[test]
    fn only_a_change_to_the_statements_component_stops_it() {
        let op = ComponentDdlOp::DropDataset {
            name: "public.orders".to_string(),
            if_exists: false,
        };
        let before = SqlDefinitions {
            datasets: vec![dataset("orders"), dataset("customers")],
            ..SqlDefinitions::default()
        };
        let other_changed = SqlDefinitions {
            datasets: vec![dataset("orders")],
            catalogs: vec![Catalog::new("iceberg".to_string(), "lake".to_string())],
        };
        assert!(!changes_component(&op, &before, &other_changed));
        let mut changed = dataset("orders");
        changed.description = Some("changed".to_string());
        let own_changed = SqlDefinitions {
            datasets: vec![changed, dataset("customers")],
            ..SqlDefinitions::default()
        };
        assert!(changes_component(&op, &before, &own_changed));
        let own_dropped = SqlDefinitions {
            datasets: vec![dataset("customers")],
            ..SqlDefinitions::default()
        };
        assert!(changes_component(&op, &before, &own_dropped));
    }

    /// A Spicepod that declares a definition identical to one SQL made serves
    /// its own: the SQL one is shadowed, and dropping it removes only the SQL
    /// definition, retiring no data.
    #[test]
    fn an_identical_spicepod_definition_shadows_the_sql_one() {
        let spicepod = spicepod_with_dataset("orders");
        let defined = SqlDefinitions {
            datasets: vec![dataset("orders")],
            ..SqlDefinitions::default()
        };
        assert_eq!(
            serve(spicepod.clone(), &defined).datasets,
            vec![dataset("orders")]
        );
        assert_eq!(shadowed_warnings(&spicepod, &defined).len(), 1);
        let op = drop("orders", false);
        let dropped = plan(&spicepod, &defined, &op).expect("drop the SQL definition");
        assert!(dropped.definitions.datasets.is_empty());
        assert_eq!(
            retired_datasets(&spicepod, &defined, &dropped.definitions, Some(&op)),
            Vec::<Dataset>::new()
        );
        assert_eq!(
            serve(spicepod, &dropped.definitions).datasets,
            vec![dataset("orders")]
        );
    }
}
