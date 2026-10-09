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

//! Acceleration layouts for differential benchmark runs.
//!
//! A query's answer must not depend on how its accelerated tables are laid out:
//! the primary key, the secondary indexes, the sort or clustering order, the
//! time column, or the partitioning. [`TableKeys`] names, for every table of a
//! benchmark, the columns each [`LayoutFeature`] applies to, and a [`Layout`]
//! picks the features to configure. [`apply_layout`] writes them into the
//! accelerated datasets of a Spicepod, so one base Spicepod is validated under
//! every layout against the same oracle.

use std::{collections::BTreeSet, fmt, str::FromStr};

use anyhow::{Context, bail, ensure};
use spicepod::{
    acceleration::{Acceleration, IndexType, Mode},
    component::dataset::{Dataset, TimeFormat},
    param::{ParamValue, Params},
    partitioning::PartitionedBy,
};

use crate::queries::QuerySet;

mod chbench;
mod clickbench;
mod tpcds;
mod tpch;

/// One setting a [`Layout`] can configure on an accelerated table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LayoutFeature {
    /// `acceleration.primary_key`, on the table's unique key.
    PrimaryKey,
    /// `acceleration.indexes`, on the columns the benchmark joins and filters on.
    Indexes,
    /// The engine's sort parameter: `cayenne_sort_columns`,
    /// `on_refresh_sort_columns` (`DuckDB`) or `arrow_sort_columns`.
    Sort,
    /// `cayenne_cluster_by`.
    Cluster,
    /// The dataset's `time_column` and `time_format`.
    TimeColumn,
    /// `acceleration.partition_by`.
    Partition,
}

impl LayoutFeature {
    pub const ALL: [Self; 6] = [
        Self::PrimaryKey,
        Self::Indexes,
        Self::Sort,
        Self::Cluster,
        Self::TimeColumn,
        Self::Partition,
    ];

    /// The feature's name in a layout string.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::PrimaryKey => "primary_key",
            Self::Indexes => "indexes",
            Self::Sort => "sort",
            Self::Cluster => "cluster",
            Self::TimeColumn => "time_column",
            Self::Partition => "partition",
        }
    }
}

impl fmt::Display for LayoutFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for LayoutFeature {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|feature| feature.name() == s)
            .with_context(|| {
                format!(
                    "unknown layout feature '{s}'; expected one of: {}",
                    Self::ALL.map(Self::name).join(", ")
                )
            })
    }
}

/// A set of [`LayoutFeature`]s, written as their names joined by commas:
/// `primary_key,indexes,sort`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Layout(BTreeSet<LayoutFeature>);

impl Layout {
    #[must_use]
    pub fn contains(&self, feature: LayoutFeature) -> bool {
        self.0.contains(&feature)
    }

    pub fn features(&self) -> impl Iterator<Item = LayoutFeature> + '_ {
        self.0.iter().copied()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<LayoutFeature> for Layout {
    fn from_iter<T: IntoIterator<Item = LayoutFeature>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl fmt::Display for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.features().map(LayoutFeature::name).collect();
        f.write_str(&names.join(","))
    }
}

impl FromStr for Layout {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut features = BTreeSet::new();
        for name in s.split(',').map(str::trim) {
            let feature = name.parse::<LayoutFeature>()?;
            ensure!(
                features.insert(feature),
                "layout '{s}' names '{name}' more than once"
            );
        }
        Ok(Self(features))
    }
}

/// The columns each [`LayoutFeature`] applies to on one benchmark table.
///
/// Every column set here must hold for the benchmark's generated data:
/// `primary_key` must be unique and non-null, or a keyed engine would keep fewer
/// rows than the source and the run would report a difference that is the
/// layout's, not the engine's.
#[derive(Debug, Clone)]
pub struct TableKeys {
    pub table: &'static str,
    /// A key that is unique and non-null in the generated data; empty for a
    /// table that has none, which a `primary_key` layout then leaves unkeyed.
    pub primary_key: &'static [&'static str],
    /// Secondary index column sets: the columns the benchmark joins and filters on.
    pub indexes: &'static [&'static [&'static str]],
    /// A sort order other than the order the data is generated in.
    pub sort: &'static [&'static str],
    /// Clustering columns; empty where clustering a small table tests nothing.
    pub cluster: &'static [&'static str],
    /// A date or time column and its format, where the table has one.
    pub time_column: Option<(&'static str, TimeFormat)>,
    /// A partition expression, where partitioning the table is meaningful.
    pub partition_by: Option<&'static str>,
}

/// The layout keys of a query set's benchmark tables, or `None` for a query set
/// that has none.
#[must_use]
pub fn benchmark_tables(query_set: &QuerySet) -> Option<&'static [TableKeys]> {
    match query_set {
        QuerySet::Tpch | QuerySet::ParameterizedTpch => Some(tpch::TABLES),
        QuerySet::Tpcds => Some(tpcds::TABLES),
        QuerySet::Clickbench => Some(clickbench::TABLES),
        QuerySet::ChBench => Some(chbench::TABLES),
        QuerySet::Scenario { .. } => None,
    }
}

/// The acceleration engines a layout knows how to configure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Arrow,
    Cayenne,
    DuckDb,
    Postgres,
    Sqlite,
    Turso,
}

impl Engine {
    fn from_acceleration(acceleration: &Acceleration) -> anyhow::Result<Self> {
        Ok(match acceleration.engine_name() {
            "arrow" => Self::Arrow,
            "cayenne" => Self::Cayenne,
            "duckdb" => Self::DuckDb,
            "postgres" => Self::Postgres,
            "sqlite" => Self::Sqlite,
            "turso" => Self::Turso,
            other => bail!("layouts do not support the '{other}' acceleration engine"),
        })
    }

    /// The acceleration parameter that sets this engine's sort order, and how it
    /// spells a column: Cayenne takes bare names, the others take a direction.
    fn sort_param(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Cayenne => Some(("cayenne_sort_columns", "")),
            Self::DuckDb => Some(("on_refresh_sort_columns", " ASC")),
            Self::Arrow => Some(("arrow_sort_columns", " ASC")),
            Self::Postgres | Self::Sqlite | Self::Turso => None,
        }
    }

    /// Whether the engine accepts `feature` at all, in `mode`.
    fn supports(self, feature: LayoutFeature, mode: &Mode) -> bool {
        match feature {
            LayoutFeature::PrimaryKey | LayoutFeature::Indexes | LayoutFeature::TimeColumn => true,
            LayoutFeature::Sort => self.sort_param().is_some(),
            LayoutFeature::Cluster => self == Self::Cayenne,
            // Cayenne partitions only file-backed tables.
            LayoutFeature::Partition => {
                self == Self::Arrow || (self == Self::Cayenne && *mode != Mode::Memory)
            }
        }
    }
}

/// What [`apply_layout`] configured on one dataset, for the run log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedLayout {
    pub dataset: String,
    pub settings: Vec<String>,
}

impl fmt::Display for AppliedLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.dataset, self.settings.join("; "))
    }
}

/// Configure `layout` on every accelerated dataset in `datasets`.
///
/// Unaccelerated datasets are left alone, which is what keeps the
/// `__test_reference.*` oracle clones independent of the layout under test.
///
/// # Errors
///
/// Rather than run a weaker test than the one asked for, this fails when an
/// accelerated dataset has no entry in `tables`, when its engine (or, for
/// partitioning, its mode) cannot take a requested feature, when the base
/// Spicepod already sets a setting the layout would replace, when Cayenne is
/// asked to both sort and cluster, or when a requested feature ends up
/// configured on no dataset at all.
pub fn apply_layout(
    datasets: &mut [Dataset],
    tables: &[TableKeys],
    layout: &Layout,
) -> anyhow::Result<Vec<AppliedLayout>> {
    ensure!(!layout.is_empty(), "the layout names no features");
    let mut applied = Vec::new();
    let mut configured: BTreeSet<LayoutFeature> = BTreeSet::new();

    for dataset in datasets.iter_mut() {
        let Some(acceleration) = dataset.acceleration.as_ref() else {
            continue;
        };
        if !acceleration.enabled {
            continue;
        }
        let keys = tables
            .iter()
            .find(|keys| keys.table == dataset.name)
            .with_context(|| {
                format!(
                    "dataset '{}' is accelerated but has no layout keys; add its table to the benchmark's layout catalog",
                    dataset.name
                )
            })?;
        let settings = apply_to_dataset(dataset, keys, layout, &mut configured)?;
        if !settings.is_empty() {
            applied.push(AppliedLayout {
                dataset: dataset.name.clone(),
                settings,
            });
        }
    }

    let unconfigured: Vec<&str> = layout
        .features()
        .filter(|feature| !configured.contains(feature))
        .map(LayoutFeature::name)
        .collect();
    ensure!(
        unconfigured.is_empty(),
        "layout '{layout}' configured {} on no accelerated dataset, so the run would not test it",
        unconfigured.join(", ")
    );
    Ok(applied)
}

fn apply_to_dataset(
    dataset: &mut Dataset,
    keys: &TableKeys,
    layout: &Layout,
    configured: &mut BTreeSet<LayoutFeature>,
) -> anyhow::Result<Vec<String>> {
    let name = dataset.name.clone();
    let mut settings = Vec::new();

    if layout.contains(LayoutFeature::TimeColumn)
        && let Some((column, format)) = &keys.time_column
    {
        ensure!(
            dataset.time_column.is_none() && dataset.time_format.is_none(),
            "dataset '{name}' already sets `time_column`; give the layout a Spicepod that does not"
        );
        dataset.time_column = Some((*column).to_string());
        dataset.time_format = Some(format.clone());
        settings.push(format!("time_column={column} ({format:?})"));
        configured.insert(LayoutFeature::TimeColumn);
    }

    let Some(acceleration) = dataset.acceleration.as_mut() else {
        return Ok(settings);
    };
    let engine =
        Engine::from_acceleration(acceleration).with_context(|| format!("dataset '{name}'"))?;
    for feature in layout.features() {
        ensure!(
            engine.supports(feature, &acceleration.mode),
            "dataset '{name}' uses the {engine:?} engine in {:?} mode, which cannot take the layout feature '{feature}'",
            acceleration.mode
        );
    }
    ensure!(
        !(engine == Engine::Cayenne
            && layout.contains(LayoutFeature::Sort)
            && layout.contains(LayoutFeature::Cluster)),
        "Cayenne does not combine `cayenne_sort_columns` with `cayenne_cluster_by`; use one of `sort` and `cluster` per layout"
    );

    if layout.contains(LayoutFeature::PrimaryKey) && !keys.primary_key.is_empty() {
        ensure!(
            acceleration.primary_key.is_none(),
            "dataset '{name}' already sets `primary_key`; give the layout a Spicepod that does not"
        );
        let primary_key = column_set(keys.primary_key);
        settings.push(format!("primary_key={primary_key}"));
        acceleration.primary_key = Some(primary_key);
        configured.insert(LayoutFeature::PrimaryKey);
    }

    if layout.contains(LayoutFeature::Indexes) && !keys.indexes.is_empty() {
        ensure!(
            acceleration.indexes.is_empty(),
            "dataset '{name}' already sets `indexes`; give the layout a Spicepod that does not"
        );
        let indexes: Vec<String> = keys
            .indexes
            .iter()
            .map(|columns| column_set(columns))
            .collect();
        for index in &indexes {
            acceleration
                .indexes
                .insert(index.clone(), IndexType::Enabled);
        }
        settings.push(format!("indexes=[{}]", indexes.join(", ")));
        configured.insert(LayoutFeature::Indexes);
    }

    if layout.contains(LayoutFeature::Sort)
        && !keys.sort.is_empty()
        && let Some((param, direction)) = engine.sort_param()
    {
        let value = keys
            .sort
            .iter()
            .map(|column| format!("{column}{direction}"))
            .collect::<Vec<_>>()
            .join(", ");
        set_param(acceleration, &name, param, value.clone())?;
        settings.push(format!("{param}={value}"));
        configured.insert(LayoutFeature::Sort);
    }

    if layout.contains(LayoutFeature::Cluster) && !keys.cluster.is_empty() {
        let value = keys.cluster.join(",");
        set_param(acceleration, &name, "cayenne_cluster_by", value.clone())?;
        settings.push(format!("cayenne_cluster_by={value}"));
        configured.insert(LayoutFeature::Cluster);
    }

    if layout.contains(LayoutFeature::Partition)
        && let Some(expression) = keys.partition_by
    {
        ensure!(
            acceleration.partition_by.is_empty(),
            "dataset '{name}' already sets `partition_by`; give the layout a Spicepod that does not"
        );
        acceleration.partition_by = vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: expression.to_string(),
        }];
        settings.push(format!("partition_by={expression}"));
        configured.insert(LayoutFeature::Partition);
    }

    Ok(settings)
}

/// A column set as Spicepod writes it: `a` for one column, `(a, b)` for several.
fn column_set(columns: &[&str]) -> String {
    match columns {
        [column] => (*column).to_string(),
        columns => format!("({})", columns.join(", ")),
    }
}

fn set_param(
    acceleration: &mut Acceleration,
    dataset: &str,
    param: &str,
    value: String,
) -> anyhow::Result<()> {
    let params = acceleration.params.get_or_insert_with(Params::default);
    ensure!(
        !params.data.contains_key(param),
        "dataset '{dataset}' already sets `{param}`; give the layout a Spicepod that does not"
    );
    params
        .data
        .insert(param.to_string(), ParamValue::String(value));
    Ok(())
}

#[cfg(test)]
mod tests;
