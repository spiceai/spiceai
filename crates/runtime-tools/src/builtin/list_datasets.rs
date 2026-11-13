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
use crate::metadata::field_description;
use crate::utils::parameters;
use app::App;
use async_trait::async_trait;
use datafusion::common::TableReference;
use itertools::Itertools;
use tools::SpiceModelTool;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";
pub const SPICE_DEFAULT_SCHEMA: &str = "public";
use runtime_query_engine::allowlist::ResolvedTableAwareAllowlist;
use runtime_query_engine::query_engine::QueryEngine;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing_futures::Instrument;

pub struct ListDatasetsTool {
    name: String,
    description: String,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,
}

impl ListDatasetsTool {
    #[must_use]
    pub fn new(
        name: Option<&str>,
        description: Option<&str>,
        table_allowlist: Option<ResolvedTableAwareAllowlist>,
        df: Arc<dyn QueryEngine>,
        app: Arc<RwLock<Option<Arc<App>>>>,
    ) -> Self {
        Self {
            df,
            app,
            name: name.unwrap_or("list_datasets").to_string(),
            description: description
                .unwrap_or("List the datasets, views, and catalog tables this runtime can query: each one's fully qualified name (`table`), its description, and `can_search_documents: true` when `search` can query it. Pass `search` to find the datasets whose names, descriptions, columns, or column descriptions match, best match first; omit it to list every dataset by name. Returns `{\"datasets\": [...], \"total\": N, \"has_more\": bool}`, 50 datasets at a time (`limit`, up to 200); read the next page with `offset`. Use `table` as the identifier in `sql`, `search`, and `table_schema`.")
                .to_string(),
            table_allowlist,
        }
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
        parameters::<ListDatasetsParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::list_datasets", tool = self.name().to_string(), input = arg);

        let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let params = ListDatasetsParams::parse(arg).boxed()?;
            let elements = get_dataset_elements(
                Arc::clone(&self.df),
                Arc::clone(&self.app),
                self.table_allowlist.as_ref(),
            )
            .await;
            let df = Arc::clone(&self.df);
            let page = dataset_page(elements, &params, |table| columns_of(df.as_ref(), table));
            serde_json::to_value(page).boxed()
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

/// The names and descriptions of the columns of `table`, when its schema is at hand without a
/// lookup that could reach a remote catalog.
fn columns_of(df: &dyn QueryEngine, table: &str) -> Vec<(String, Option<String>)> {
    df.get_table_sync(&TableReference::parse_str(table))
        .map(|provider| {
            provider
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    (
                        field.name().clone(),
                        field_description(field).map(ToString::to_string),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Return all datasets available in the runtime, with the properties visible to LLMs.
pub async fn get_dataset_elements(
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,
    opt_include: Option<&ResolvedTableAwareAllowlist>,
) -> Vec<ListDatasetElement> {
    let mut tables = get_table_elements(Arc::clone(&app), opt_include).await;
    let views = get_view_elements(Arc::clone(&app), opt_include).await;
    let catalogs = get_catalog_elements(df, Arc::clone(&app), opt_include).await;
    tables.extend(views);
    tables.extend(catalogs);

    tables
}

pub async fn get_table_elements(
    app: Arc<RwLock<Option<Arc<App>>>>,
    opt_include: Option<&ResolvedTableAwareAllowlist>,
) -> Vec<ListDatasetElement> {
    let Some(app) = app.read().await.clone() else {
        return vec![];
    };

    app.datasets
        .iter()
        .filter(|&d| {
            opt_include.is_none_or(|ts| ts.table_is_allowed(&TableReference::parse_str(&d.name)))
        })
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
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,
    opt_include: Option<&ResolvedTableAwareAllowlist>,
) -> Vec<ListDatasetElement> {
    let Some(ref app) = *app.read().await else {
        return vec![];
    };

    app.catalogs
        .iter()
        .flat_map(|c| {
            let Some(ctlg) = df.session_context().catalog(c.name.as_str()) else {
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
                        .map(|t| TableReference::Full {
                            table: t.as_str().into(),
                            schema: s.as_str().into(),
                            catalog: c.name.as_str().into(),
                        })
                        .filter(|d| opt_include.is_none_or(|ts| ts.table_is_allowed(d)))
                        .map(|table| ListDatasetElement {
                            table: table.to_string(),
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
    app: Arc<RwLock<Option<Arc<App>>>>,
    opt_include: Option<&ResolvedTableAwareAllowlist>,
) -> Vec<ListDatasetElement> {
    let Some(app) = app.read().await.clone() else {
        return vec![];
    };

    app.views
        .iter()
        .filter(|&v| {
            opt_include.is_none_or(|ts| ts.table_is_allowed(&TableReference::parse_str(&v.name)))
        })
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatasetElement {
    pub table: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub can_search_documents: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde's skip_serializing_if passes a reference"
)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// The default number of datasets `list_datasets` returns.
pub const DEFAULT_LIST_LIMIT: usize = 50;
/// The most datasets one `list_datasets` call returns.
pub const MAX_LIST_LIMIT: usize = 200;

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Deserialize)]
pub struct ListDatasetsParams {
    /// Words to look for in dataset names, descriptions, column names, and column
    /// descriptions. Omit to list every dataset.
    #[serde(default)]
    pub search: Option<String>,
    /// The most datasets to return: 50 when omitted, at most 200.
    #[serde(default)]
    pub limit: Option<usize>,
    /// How many datasets to skip, to read the next page.
    #[serde(default)]
    pub offset: Option<usize>,
}

impl ListDatasetsParams {
    fn parse(arg: &str) -> Result<Self, serde_json::Error> {
        if arg.trim().is_empty() {
            Ok(Self::default())
        } else {
            serde_json::from_str(arg)
        }
    }
}

/// One page of datasets, as `list_datasets` returns it.
#[derive(Debug, Serialize, Deserialize)]
pub struct DatasetPage {
    pub datasets: Vec<ListDatasetElement>,
    /// The number of datasets that match, across every page.
    pub total: usize,
    pub has_more: bool,
}

/// The datasets matching `params`, ranked by how well they match its `search` (all of them, by
/// name, without one), one page of them.
pub fn dataset_page(
    mut elements: Vec<ListDatasetElement>,
    params: &ListDatasetsParams,
    columns_of: impl Fn(&str) -> Vec<(String, Option<String>)>,
) -> DatasetPage {
    let words = params
        .search
        .as_deref()
        .map(crate::metadata::search_words)
        .unwrap_or_default();
    if words.is_empty() {
        elements.sort_by(|a, b| a.table.cmp(&b.table));
    } else {
        let mut scored: Vec<(u32, ListDatasetElement)> = elements
            .into_iter()
            .filter_map(|element| {
                let columns = columns_of(&element.table);
                let score = crate::metadata::search_score(
                    &words,
                    &crate::metadata::Searchable {
                        name: &element.table,
                        description: element.description.as_deref(),
                        columns: columns
                            .iter()
                            .map(|(name, description)| (name.as_str(), description.as_deref()))
                            .collect(),
                    },
                );
                (score > 0).then_some((score, element))
            })
            .collect();
        scored.sort_by(|(a_score, a), (b_score, b)| {
            b_score.cmp(a_score).then_with(|| a.table.cmp(&b.table))
        });
        elements = scored.into_iter().map(|(_, element)| element).collect();
    }
    let total = elements.len();
    let offset = params.offset.unwrap_or(0).min(total);
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let datasets: Vec<ListDatasetElement> = elements.into_iter().skip(offset).take(limit).collect();
    let has_more = offset + datasets.len() < total;
    DatasetPage {
        datasets,
        total,
        has_more,
    }
}

/// The most bytes the dataset list in a model's system prompt may take (about 2,000 tokens).
pub const MAX_CATALOG_PROMPT_BYTES: usize = 8 * 1024;

/// The system-prompt text naming the datasets a model can query, from a `list_datasets` page of
/// every dataset: the datasets by name when the list fits in [`MAX_CATALOG_PROMPT_BYTES`],
/// otherwise their number and how to search them. The datasets come sorted by name, so the text,
/// and with it a provider's cached prompt prefix, changes only when the datasets do.
#[must_use]
pub fn catalog_prompt(page: &DatasetPage) -> String {
    if page.total == 0 {
        return "No datasets are available to query.".to_string();
    }
    if page.has_more {
        return too_many_datasets(page.total);
    }
    catalog_listing(&page.datasets).unwrap_or_else(|| too_many_datasets(page.total))
}

/// What the system prompt says when the `total` datasets do not fit in it.
fn too_many_datasets(total: usize) -> String {
    format!(
        "{total} datasets are available. Call list_datasets with search to find the ones relevant to the question."
    )
}

/// `datasets` by name, as the system prompt lists them; `None` when the list takes more than
/// [`MAX_CATALOG_PROMPT_BYTES`].
fn catalog_listing(datasets: &[ListDatasetElement]) -> Option<String> {
    let mut text = String::from("Datasets (use these names in sql, search, and table_schema):");
    for dataset in datasets {
        text.push_str("\n- ");
        text.push_str(&dataset.table);
        if let Some(description) = dataset
            .description
            .as_deref()
            .filter(|d| !d.trim().is_empty())
        {
            text.push_str(": ");
            text.push_str(&description.split_whitespace().collect::<Vec<_>>().join(" "));
        }
        if dataset.can_search_documents {
            text.push_str(" [searchable]");
        }
        if text.len() > MAX_CATALOG_PROMPT_BYTES {
            return None;
        }
    }
    Some(text)
}

/// The system-prompt text naming the datasets `tool`, a `list_datasets` tool, lists: see
/// [`catalog_prompt`]. It reads the tool's pages of [`MAX_LIST_LIMIT`] datasets until it has every
/// dataset or they no longer fit in [`MAX_CATALOG_PROMPT_BYTES`], so the page size is not a
/// second limit on the list. A catalog whose number of datasets changes between pages gets the
/// search hint instead of a list, because its pages could skip or repeat a dataset. `None` when
/// the tool fails.
pub async fn catalog_prompt_from_tool(tool: &dyn SpiceModelTool) -> Option<String> {
    let mut page = list_datasets_page(tool, 0).await?;
    while page.has_more && catalog_listing(&page.datasets).is_some() {
        let next = list_datasets_page(tool, page.datasets.len()).await?;
        if next.total != page.total || next.datasets.is_empty() {
            return Some(too_many_datasets(next.total));
        }
        page.datasets.extend(next.datasets);
        page.has_more = next.has_more;
    }
    Some(catalog_prompt(&page))
}

/// The page of [`MAX_LIST_LIMIT`] datasets `tool`, a `list_datasets` tool, lists from `offset`.
/// `None` when the tool fails.
async fn list_datasets_page(tool: &dyn SpiceModelTool, offset: usize) -> Option<DatasetPage> {
    let arguments = format!(r#"{{"limit":{MAX_LIST_LIMIT},"offset":{offset}}}"#);
    match tool.call(&arguments).await {
        Ok(value) => match serde_json::from_value::<DatasetPage>(value) {
            Ok(page) => Some(page),
            Err(error) => {
                tracing::debug!("Could not read the datasets list_datasets returned: {error}");
                None
            }
        },
        Err(error) => {
            tracing::debug!(
                "list_datasets failed while listing datasets for the system prompt: {error}"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element(table: &str, description: Option<&str>, searchable: bool) -> ListDatasetElement {
        ListDatasetElement {
            table: table.to_string(),
            can_search_documents: searchable,
            description: description.map(ToString::to_string),
            metadata: HashMap::new(),
        }
    }

    fn elements() -> Vec<ListDatasetElement> {
        vec![
            element("spice.public.orders", Some("Customer orders"), false),
            element("spice.docs.pages", Some("Product docs"), true),
            element("spice.public.store_sales", None, false),
        ]
    }

    fn tables(page: &DatasetPage) -> Vec<&str> {
        page.datasets.iter().map(|d| d.table.as_str()).collect()
    }

    fn no_columns(_: &str) -> Vec<(String, Option<String>)> {
        Vec::new()
    }

    #[test]
    fn without_search_every_dataset_is_listed_by_name() {
        let page = dataset_page(elements(), &ListDatasetsParams::default(), no_columns);
        assert_eq!(
            tables(&page),
            [
                "spice.docs.pages",
                "spice.public.orders",
                "spice.public.store_sales"
            ]
        );
        assert_eq!((page.total, page.has_more), (3, false));
    }

    #[test]
    fn search_returns_matches_best_first_and_counts_only_matches() {
        let params = ListDatasetsParams {
            search: Some("customer sales".to_string()),
            ..ListDatasetsParams::default()
        };
        let columns = |table: &str| {
            if table == "spice.docs.pages" {
                vec![("customer_id".to_string(), None)]
            } else {
                Vec::new()
            }
        };
        let page = dataset_page(elements(), &params, columns);
        // store_sales: `sales` in the name (4); pages: `customer` in a column (2);
        // orders: `customer` in the description (1).
        assert_eq!(
            tables(&page),
            [
                "spice.public.store_sales",
                "spice.docs.pages",
                "spice.public.orders"
            ]
        );
        let params = ListDatasetsParams {
            search: Some("pages".to_string()),
            ..ListDatasetsParams::default()
        };
        let page = dataset_page(elements(), &params, no_columns);
        assert_eq!(tables(&page), ["spice.docs.pages"]);
        assert_eq!((page.total, page.has_more), (1, false));
    }

    #[test]
    fn pages_follow_offset_and_limit() {
        let params = ListDatasetsParams {
            limit: Some(2),
            offset: Some(1),
            ..ListDatasetsParams::default()
        };
        let page = dataset_page(elements(), &params, no_columns);
        assert_eq!(
            tables(&page),
            ["spice.public.orders", "spice.public.store_sales"]
        );
        assert_eq!((page.total, page.has_more), (3, false));

        let params = ListDatasetsParams {
            limit: Some(1),
            ..ListDatasetsParams::default()
        };
        let page = dataset_page(elements(), &params, no_columns);
        assert_eq!((page.datasets.len(), page.has_more), (1, true));

        // A limit past the maximum is held to it; zero still returns one dataset.
        let params = ListDatasetsParams {
            limit: Some(0),
            ..ListDatasetsParams::default()
        };
        assert_eq!(
            dataset_page(elements(), &params, no_columns).datasets.len(),
            1
        );
        let many: Vec<ListDatasetElement> = (0..250)
            .map(|i| element(&format!("spice.public.t{i:03}"), None, false))
            .collect();
        let params = ListDatasetsParams {
            limit: Some(1_000),
            ..ListDatasetsParams::default()
        };
        let page = dataset_page(many, &params, no_columns);
        assert_eq!(
            (page.datasets.len(), page.total, page.has_more),
            (MAX_LIST_LIMIT, 250, true)
        );
    }

    #[test]
    fn a_dataset_shows_only_the_fields_it_has() {
        let page = dataset_page(elements(), &ListDatasetsParams::default(), no_columns);
        assert_eq!(
            serde_json::to_value(&page).expect("serializes"),
            serde_json::json!({
                "datasets": [
                    {"table": "spice.docs.pages", "can_search_documents": true, "description": "Product docs"},
                    {"table": "spice.public.orders", "description": "Customer orders"},
                    {"table": "spice.public.store_sales"},
                ],
                "total": 3,
                "has_more": false,
            })
        );
    }

    #[test]
    fn an_empty_argument_lists_everything() {
        assert!(
            ListDatasetsParams::parse("")
                .expect("parses")
                .search
                .is_none()
        );
        assert!(
            ListDatasetsParams::parse("  ")
                .expect("parses")
                .limit
                .is_none()
        );
        assert_eq!(
            ListDatasetsParams::parse(r#"{"search":"x","offset":5}"#)
                .expect("parses")
                .offset,
            Some(5)
        );
    }

    #[test]
    fn the_catalog_prompt_lists_datasets_or_says_how_to_search_them() {
        let page = dataset_page(elements(), &ListDatasetsParams::default(), no_columns);
        assert_eq!(
            catalog_prompt(&page),
            "Datasets (use these names in sql, search, and table_schema):\n- spice.docs.pages: Product docs [searchable]\n- spice.public.orders: Customer orders\n- spice.public.store_sales"
        );

        let none = DatasetPage {
            datasets: vec![],
            total: 0,
            has_more: false,
        };
        assert_eq!(catalog_prompt(&none), "No datasets are available to query.");

        let more = DatasetPage {
            datasets: vec![element("spice.public.a", None, false)],
            total: 992,
            has_more: true,
        };
        assert_eq!(
            catalog_prompt(&more),
            "992 datasets are available. Call list_datasets with search to find the ones relevant to the question."
        );

        let long: Vec<ListDatasetElement> = (0..100)
            .map(|i| element(&format!("spice.public.t{i}"), Some(&"d".repeat(100)), false))
            .collect();
        let page = DatasetPage {
            datasets: long,
            total: 100,
            has_more: false,
        };
        assert_eq!(
            catalog_prompt(&page),
            "100 datasets are available. Call list_datasets with search to find the ones relevant to the question."
        );
    }

    /// A `list_datasets` tool over `count` datasets named `spice.public.t0000`, … that counts its
    /// calls. From call number `grow_after` on, it lists one more dataset, as a catalog that gains
    /// a table while it is being listed would.
    struct PagedTool {
        count: usize,
        grow_after: Option<usize>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl PagedTool {
        fn new(count: usize) -> Self {
            Self {
                count,
                grow_after: None,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SpiceModelTool for PagedTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("list_datasets")
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let count = match self.grow_after {
                Some(after) if call >= after => self.count + 1,
                _ => self.count,
            };
            let elements = (0..count)
                .map(|i| element(&format!("spice.public.t{i:04}"), None, false))
                .collect();
            let params = ListDatasetsParams::parse(arg)?;
            Ok(serde_json::to_value(dataset_page(
                elements, &params, no_columns,
            ))?)
        }
    }

    #[tokio::test]
    async fn the_catalog_prompt_lists_every_dataset_past_one_page_when_they_fit() {
        let tool = PagedTool::new(MAX_LIST_LIMIT + 1);
        let prompt = catalog_prompt_from_tool(&tool).await.expect("a prompt");

        let expected = std::iter::once(
            "Datasets (use these names in sql, search, and table_schema):".to_string(),
        )
        .chain((0..=MAX_LIST_LIMIT).map(|i| format!("spice.public.t{i:04}")))
        .join("\n- ");
        assert!(
            expected.len() <= MAX_CATALOG_PROMPT_BYTES,
            "{}",
            expected.len()
        );
        assert_eq!(prompt, expected);
        assert_eq!(tool.calls(), 2, "one call per page of {MAX_LIST_LIMIT}");
    }

    #[tokio::test]
    async fn the_catalog_prompt_stops_paging_once_the_datasets_do_not_fit() {
        // 21 bytes a dataset, so 387 fit: the second page of 200 is the first past 8 KiB.
        let tool = PagedTool::new(5_000);
        assert_eq!(
            catalog_prompt_from_tool(&tool).await.expect("a prompt"),
            "5000 datasets are available. Call list_datasets with search to find the ones relevant to the question."
        );
        assert_eq!(tool.calls(), 2, "pages read before the budget ran out");
    }

    #[tokio::test]
    async fn a_catalog_that_changes_between_pages_gets_the_search_hint() {
        let tool = PagedTool {
            grow_after: Some(1),
            ..PagedTool::new(MAX_LIST_LIMIT + 1)
        };
        assert_eq!(
            catalog_prompt_from_tool(&tool).await.expect("a prompt"),
            "202 datasets are available. Call list_datasets with search to find the ones relevant to the question."
        );
    }
}
