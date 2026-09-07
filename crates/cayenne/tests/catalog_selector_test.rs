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

//! A Cayenne catalog registers the tables its `include`/`exclude` patterns
//! select, and only those.
//!
//! The patterns are matched against the namespace-qualified name
//! (`"{namespace}.{table}"`), which is the naming every SQL catalog connector
//! matches them against, and which is what a spicepod's `exclude:
//! [public.audit_log]` reads as.
//!
//! Both halves are covered here, and so is the lazy-load path: a table the
//! catalog withheld must stay withheld when a query names it directly, since
//! that path reaches the metastore without going through discovery.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalogProvider, CayenneCatalogProviderConfig};
use data_components::RefreshableCatalogProvider;
use data_components::catalog_filter::TableSelector;
use datafusion::catalog::CatalogProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use globset::{Glob, GlobSet, GlobSetBuilder};
use tempfile::TempDir;

/// `Send + Sync` so `RefreshableCatalogProvider::refresh`'s boxed error, which
/// carries both bounds, converts with `?`.
type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn globset(patterns: &[&str]) -> GlobSet {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).expect("valid glob"));
    }
    builder.build().expect("valid globset")
}

/// A provider config that pins nothing, so the test exercises the engine
/// defaults and only the selector varies between cases.
fn config_in(temp_dir: &TempDir) -> CayenneCatalogProviderConfig {
    let base = temp_dir.path().to_string_lossy().to_string();
    CayenneCatalogProviderConfig {
        data_dir: Some(format!("{base}/data")),
        metadata_dir: Some(format!("{base}/metadata")),
        spice_data_base_path: base,
        catalog_name: None,
        footer_cache_mb: None,
        segment_cache_mb: None,
        target_file_size_mb: None,
        compression_strategy: None,
        upload_concurrency: None,
        write_concurrency: None,
        inline_max_rows: None,
        inline_max_bytes: None,
        inline_max_buffer_bytes: None,
        inline_flush_max_rows: None,
        inline_flush_max_segments: None,
        inline_flush_max_bytes: None,
        pk_conflict_detection: None,
        dynamic_tuning: false,
        compaction_background_interval_ms: None,
        compaction_trigger_files: None,
        bake_deletion_index_trigger: None,
    }
}

/// Build a catalog holding `public/orders`, `public/audit_log` and
/// `reporting/summary`, then refresh it under `selector`.
async fn catalog_with(selector: TableSelector) -> TestResult<(CayenneCatalogProvider, TempDir)> {
    let temp_dir = TempDir::new()?;
    let provider = CayenneCatalogProvider::try_new(
        config_in(&temp_dir),
        Arc::new(RuntimeEnv::default()),
        selector,
    )
    .await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    for full_name in ["public/orders", "public/audit_log", "reporting/summary"] {
        provider
            .metadata_catalog()
            .create_table(CreateTableOptions {
                table_name: full_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None,
                base_path: format!(
                    "{}{}",
                    provider.data_base_path(),
                    full_name.replace('/', "_")
                ),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            })
            .await?;
    }

    provider.refresh().await?;

    Ok((provider, temp_dir))
}

/// Table names a refreshed catalog exposes in `namespace`, or `None` when it
/// registered no such namespace at all.
fn table_names_in(provider: &CayenneCatalogProvider, namespace: &str) -> Option<Vec<String>> {
    let schema = provider.schema(namespace)?;
    let mut names = schema.table_names();
    names.sort();
    Some(names)
}

/// The behavior every catalog that configured neither pattern must keep: it
/// registers everything it discovers.
#[tokio::test]
async fn unconfigured_catalog_registers_every_table() -> TestResult<()> {
    let (provider, _temp_dir) = catalog_with(TableSelector::select_all()).await?;

    let mut schemas = provider.schema_names();
    schemas.sort();
    assert_eq!(schemas, vec!["public".to_string(), "reporting".to_string()]);
    assert_eq!(
        table_names_in(&provider, "public"),
        Some(vec!["audit_log".to_string(), "orders".to_string()])
    );

    Ok(())
}

/// The reported bug: `exclude` had no effect, so the table a user asked to keep
/// out of the catalog was registered anyway.
#[tokio::test]
async fn exclude_withholds_the_table_it_names() -> TestResult<()> {
    let (provider, _temp_dir) = catalog_with(TableSelector::new(
        None,
        Some(globset(&["public.audit_log"])),
    ))
    .await?;

    assert_eq!(
        table_names_in(&provider, "public"),
        Some(vec!["orders".to_string()]),
        "public.audit_log is excluded and must not be registered"
    );
    // An `exclude` alone leaves every other table selected.
    assert_eq!(
        table_names_in(&provider, "reporting"),
        Some(vec!["summary".to_string()])
    );

    Ok(())
}

/// The other half a Cayenne catalog dropped: `include` narrowed nothing.
#[tokio::test]
async fn include_narrows_the_catalog_to_matching_tables() -> TestResult<()> {
    let (provider, _temp_dir) =
        catalog_with(TableSelector::new(Some(globset(&["public.*"])), None)).await?;

    assert_eq!(
        provider.schema_names(),
        vec!["public".to_string()],
        "no table outside `public` is included, so no other namespace is registered"
    );
    assert_eq!(
        table_names_in(&provider, "public"),
        Some(vec!["audit_log".to_string(), "orders".to_string()])
    );

    Ok(())
}

/// Both halves together, as the issue's spicepod configures them: `include`
/// selects the namespace and `exclude` vetoes one table inside it.
#[tokio::test]
async fn exclude_vetoes_a_table_include_selected() -> TestResult<()> {
    let (provider, _temp_dir) = catalog_with(TableSelector::new(
        Some(globset(&["public.*"])),
        Some(globset(&["public.audit_log"])),
    ))
    .await?;

    assert_eq!(provider.schema_names(), vec!["public".to_string()]);
    assert_eq!(
        table_names_in(&provider, "public"),
        Some(vec!["orders".to_string()]),
        "a name matched by both include and exclude is not selected"
    );

    Ok(())
}

/// Discovery is not the only way into the metastore: a schema provider loads a
/// table on demand when a query names one it has not cached. That path must
/// apply the same decision, or naming the table in a query re-admits it.
#[tokio::test]
async fn a_withheld_table_stays_withheld_when_named_directly() -> TestResult<()> {
    let (provider, _temp_dir) = catalog_with(TableSelector::new(
        None,
        Some(globset(&["public.audit_log"])),
    ))
    .await?;

    let public = provider.schema("public").expect("public is registered");

    assert!(
        public.table("audit_log").await?.is_none(),
        "an excluded table must not be loaded on demand either"
    );
    assert!(
        public.table("orders").await?.is_some(),
        "a selected table still loads"
    );

    Ok(())
}
