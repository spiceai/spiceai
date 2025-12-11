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

use serde::{Deserialize, Serialize};
use spicepod::{
    acceleration::Acceleration,
    component::{Nameable, dataset::Dataset, view::View},
    semantic::Column,
    vector::VectorStore,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct SearchTable {
    // The name of the table (either dataset or view) to test search queries against.
    pub table_name: String,
    pub datasets: Vec<Dataset>,
    pub views: Vec<View>,
}

// Enrich the `table_name` [`View`]/[`Dataset`] in the `SearchTable` with the given columns and acceleration.
pub fn enrich_table(
    t: SearchTable,
    columns: Vec<Column>,
    vector: Option<VectorStore>,
    acceleration: &Acceleration,
) -> (Vec<View>, Vec<Dataset>) {
    let SearchTable {
        table_name,
        mut datasets,
        mut views,
    } = t;

    if let Some(ds) = datasets.iter_mut().find(|ds| ds.name() == table_name) {
        ds.acceleration = Some(acceleration.clone());
        ds.columns.extend(columns.clone());
        ds.vectors.clone_from(&vector);
    }

    if let Some(v) = views.iter_mut().find(|v| v.name() == table_name) {
        v.acceleration = Some(acceleration.clone());
        v.columns.extend(columns);
        v.vectors = vector;
    }

    (views, datasets)
}
