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
use async_trait::async_trait;
use datafusion::sql::TableReference;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{
    Runtime,
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    tools::SpiceModelTool,
};

pub struct ListDatasetsTool {
    name: String,
    description: String,
    table_allowlist: Option<Vec<String>>,
    rt: Arc<Runtime>,
}

impl ListDatasetsTool {
    #[must_use]
    pub fn new(
        name: Option<&str>,
        description: Option<&str>,
        table_allowlist: Option<Vec<&str>>,
        rt: Arc<Runtime>,
    ) -> Self {
        Self {
            rt,
            name: name.unwrap_or("list_datasets").to_string(),
            description: description
                .unwrap_or("List all SQL tables available.")
                .to_string(),
            table_allowlist: table_allowlist.map(|t| t.iter().map(ToString::to_string).collect()),
        }
    }
}

impl From<&Arc<Runtime>> for ListDatasetsTool {
    fn from(rt: &Arc<Runtime>) -> Self {
        Self::new(None, None, None, Arc::clone(rt))
    }
}

#[async_trait]
impl SpiceModelTool for ListDatasetsTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::list_datasets", tool = self.name().to_string(), input = arg);

        let elements = get_dataset_elements(Arc::clone(&self.rt), self.table_allowlist.as_deref())
            .await
            .iter()
            .map(serde_json::value::to_value)
            .collect::<Result<Vec<Value>, _>>()
            .boxed()?;

        let captured_output_json = serde_json::to_string(&elements).boxed()?;
        tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);

        Ok(Value::Array(elements))
    }
}

/// Return all datasets available in the runtime, with the properties visible to LLMs.
pub async fn get_dataset_elements(
    rt: Arc<Runtime>,
    opt_include: Option<&[String]>,
) -> Vec<ListDatasetElement> {
    let mut tables = get_table_elements(Arc::clone(&rt), opt_include).await;
    let views = get_view_elements(Arc::clone(&rt), opt_include).await;
    let catalogs = get_catalog_elements(Arc::clone(&rt), opt_include).await;
    tables.extend(views.into_iter());
    tables.extend(catalogs.into_iter());

    tables
}

pub async fn get_table_elements(
    rt: Arc<Runtime>,
    opt_include: Option<&[String]>,
) -> Vec<ListDatasetElement> {
    let Some(app) = &*rt.app.read().await else {
        return vec![];
    };

    app.datasets
        .iter()
        .filter(|d| opt_include.is_none_or(|ts| ts.contains(&d.name)))
        .map(|d| ListDatasetElement {
            table: TableReference::parse_str(&d.name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .to_string(),
            can_search_documents: d.has_embeddings(),
            description: d.description.clone(),
            metadata: d.metadata.clone(),
        })
        .collect_vec()
}

pub async fn get_catalog_elements(
    rt: Arc<Runtime>,
    _opt_include: Option<&[String]>,
) -> Vec<ListDatasetElement> {
    let Some(ref app) = *rt.app.read().await else {
        return vec![];
    };

    app.catalogs
        .iter()
        .flat_map(|c| {
            let Some(ctlg) = rt.datafusion().ctx.catalog(c.name.as_str()) else {
                return vec![];
            };
            ctlg.schema_names()
                .iter()
                .flat_map(|s| {
                    let Some(schm) = ctlg.schema(s.as_str()) else {
                        return vec![];
                    };
                    schm.table_names()
                        .iter()
                        .map(|t| ListDatasetElement {
                            table: TableReference::Full {
                                table: t.as_str().into(),
                                schema: s.as_str().into(),
                                catalog: c.name.as_str().into(),
                            }
                            .to_string(),
                            can_search_documents: false,
                            description: None,
                            metadata: HashMap::new(),
                        })
                        .collect()
                })
                .collect()
        })
        .collect()
}

pub async fn get_view_elements(
    rt: Arc<Runtime>,
    opt_include: Option<&[String]>,
) -> Vec<ListDatasetElement> {
    let Some(app) = &*rt.app.read().await else {
        return vec![];
    };

    app.views
        .iter()
        .filter(|v| opt_include.is_none_or(|ts| ts.contains(&v.name)))
        .map(|v| ListDatasetElement {
            table: TableReference::parse_str(&v.name)
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .to_string(),
            can_search_documents: false,
            description: v.description.clone(),
            metadata: v.metadata.clone(),
        })
        .collect_vec()
}

/// Details about each dataset outputted by the [`ListDatasetsTool`] tool.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListDatasetElement {
    pub table: String,
    pub can_search_documents: bool,
    pub description: Option<String>,
    pub metadata: HashMap<String, Value>,
}

impl ListDatasetElement {
    /// A pretty-printed version of the dataset element suitable LLM instructions.
    #[must_use]
    pub fn to_text_llms(&self) -> String {
        format!(
            "Dataset: {}\nDescription: {}\nMetadata: {}",
            self.table,
            self.description.as_deref().unwrap_or("None"),
            self.metadata
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .join(", ")
        )
    }
}
