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

//! Iceberg datasets read from a pinned metadata file (`iceberg_metadata_path`), with
//! no catalog. The tables are written to the local filesystem by iceberg-rust, so
//! these tests need no network, container or credentials.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::AsArray as _;
use arrow::datatypes::Int64Type;
use aws_sdk_s3::config::{Credentials, Region};
use bollard::secret::HealthConfig;
use datafusion::prelude::SessionContext;
use iceberg::io::{LocalFsStorageFactory, StorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params as DatasetParams,
};

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{run_query, test_request_context},
};

const NAMESPACE: &str = "db";
const TABLE: &str = "events";

/// Writes `db.events` (one `id BIGINT` column) under `warehouse`, one commit per
/// entry of `appends`, and returns the table root. Table creation is commit 0, so
/// after `n` appends the metadata files are versions `0..=n`, named the way a
/// catalog-managed table names them (`<N>-<uuid>.metadata.json`), with no
/// `version-hint.text`.
pub(crate) async fn write_table(warehouse: &Path, appends: &[&[i64]]) -> anyhow::Result<PathBuf> {
    write_table_at(
        &format!("file://{}", warehouse.display()),
        Arc::new(LocalFsStorageFactory),
        HashMap::new(),
        appends,
    )
    .await?;
    Ok(warehouse.join(NAMESPACE).join(TABLE))
}

/// [`write_table`] against any warehouse URL the `factory` can write, with
/// `props` (endpoint, credentials) passed to its file IO.
async fn write_table_at(
    warehouse_url: &str,
    factory: Arc<dyn StorageFactory>,
    mut props: HashMap<String, String>,
    appends: &[&[i64]],
) -> anyhow::Result<()> {
    props.insert(
        MEMORY_CATALOG_WAREHOUSE.to_string(),
        warehouse_url.to_string(),
    );
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(factory)
        .load("memory", props)
        .await?;
    let namespace = NamespaceIdent::new(NAMESPACE.to_string());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(TABLE.to_string())
                .schema(schema)
                .build(),
        )
        .await?;

    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
    for rows in appends {
        let ctx = SessionContext::new();
        let provider =
            IcebergTableProvider::try_new(Arc::clone(&catalog), namespace.clone(), TABLE).await?;
        ctx.register_table("t", Arc::new(provider))?;
        let values = rows
            .iter()
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(", ");
        ctx.sql(&format!("INSERT INTO t VALUES {values}"))
            .await?
            .collect()
            .await?;
    }

    Ok(())
}

/// The metadata file name for each version of the table at `table_root`, indexed
/// by version.
pub(crate) fn metadata_file_names(table_root: &Path) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(table_root.join("metadata"))? {
        names.push(entry?.file_name().to_string_lossy().to_string());
    }
    ordered_metadata_names(names)
}

/// The `<N>-<uuid>.metadata.json` names among `names`, indexed by version `N`,
/// which must run contiguously from 0.
fn ordered_metadata_names(names: impl IntoIterator<Item = String>) -> anyhow::Result<Vec<String>> {
    let mut files: Vec<(u64, String)> = names
        .into_iter()
        .filter_map(|name| {
            let version = name
                .strip_suffix(".metadata.json")?
                .split('-')
                .next()?
                .parse()
                .ok()?;
            Some((version, name))
        })
        .collect();
    files.sort();
    anyhow::ensure!(
        files.iter().enumerate().all(|(i, (v, _))| *v == i as u64),
        "expected contiguous metadata versions from 0, found {files:?}"
    );
    Ok(files.into_iter().map(|(_, name)| name).collect())
}

/// A dataset reading the table at `from` with `params`.
fn dataset_with_params(from: String, name: &str, params: HashMap<String, String>) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(DatasetParams::from_string_map(params));
    dataset
}

pub(crate) fn pinned_dataset(table_root: &Path, name: &str, param: &str, value: &str) -> Dataset {
    dataset_with_params(
        format!("iceberg:file://{}", table_root.display()),
        name,
        HashMap::from([(param.to_string(), value.to_string())]),
    )
}

async fn start_runtime(app_name: &str, datasets: Vec<Dataset>) -> Arc<Runtime> {
    let mut builder = AppBuilder::new(app_name);
    for dataset in datasets {
        builder = builder.with_dataset(dataset);
    }
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(builder.build()).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            panic!("Timeout waiting for components to load");
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    rt
}

/// The first column of `sql`'s result, as `i64`s, in result order.
async fn int64_column(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<Vec<i64>> {
    Ok(run_query(rt, sql)
        .await?
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect())
}

async fn ids(rt: &Arc<Runtime>, table: &str) -> anyhow::Result<Vec<i64>> {
    int64_column(rt, &format!("SELECT id FROM {table} ORDER BY id")).await
}

/// Starts loading `datasets` and waits for every one of them to fail, returning
/// each dataset's error message by name.
async fn dataset_errors(
    app_name: &str,
    datasets: Vec<Dataset>,
) -> anyhow::Result<HashMap<String, Option<String>>> {
    let names: Vec<String> = datasets.iter().map(|d| d.name.clone()).collect();
    let mut builder = AppBuilder::new(app_name);
    for dataset in datasets {
        builder = builder.with_dataset(dataset);
    }
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(builder.build()).build().await);
    let load = tokio::spawn(Arc::clone(&rt).load_components());

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last_seen = HashMap::new();
    while std::time::Instant::now() < deadline {
        let statuses = rt.status().get_dataset_statuses();
        let errors: HashMap<String, Option<String>> = statuses
            .iter()
            .filter_map(|(name, status)| match status {
                runtime::status::ComponentStatus::Error(message) => {
                    Some((name.table().to_string(), message.clone()))
                }
                _ => None,
            })
            .collect();
        if names.iter().all(|name| errors.contains_key(name)) {
            load.abort();
            return Ok(errors);
        }
        last_seen = statuses;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    load.abort();
    anyhow::bail!("datasets {names:?} must all fail to load (last seen: {last_seen:?})")
}

/// Every spelling of the pinned metadata file reads exactly the snapshot that file
/// records. Pinning versions older than the latest makes the check non-vacuous: a
/// reader that ignored the pin and read the current snapshot would return
/// `[1, 2, 3]` for each of them.
#[tokio::test]
async fn iceberg_metadata_path_reads_the_pinned_snapshot_in_every_spelling() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[1], &[2], &[3]]).await?;
            let files = metadata_file_names(&table_root)?;
            anyhow::ensure!(files.len() == 4, "fixture has {files:?}");

            let absolute = format!("file://{}/metadata/{}", table_root.display(), files[1]);
            let cases: [(&str, &str, String, &[i64]); 4] = [
                (
                    "leading_slash",
                    "iceberg_metadata_path",
                    format!("/metadata/{}", files[1]),
                    &[1],
                ),
                (
                    "no_leading_slash",
                    "iceberg_metadata_path",
                    format!("metadata/{}", files[2]),
                    &[1, 2],
                ),
                ("full_url", "iceberg_metadata_path", absolute, &[1]),
                (
                    "legacy_name",
                    "metadata_path",
                    format!("file://{}/metadata/{}", table_root.display(), files[2]),
                    &[1, 2],
                ),
            ];

            let datasets = cases
                .iter()
                .map(|(name, param, value, _)| pinned_dataset(&table_root, name, param, value))
                .collect();
            let rt = start_runtime("iceberg_metadata_path_spellings", datasets).await;

            for (name, param, value, expected) in &cases {
                assert_eq!(
                    ids(&rt, name).await?,
                    expected.to_vec(),
                    "dataset {name} ({param}: {value}) must read the pinned snapshot"
                );
            }
            Ok(())
        })
        .await
}

/// A pinned table accelerates like any other. Once the acceleration has loaded, the
/// table's files are deleted: the rows can then only come from the acceleration,
/// which shows the query was served by it rather than by the source.
#[tokio::test]
async fn a_pinned_table_accelerates_into_cayenne() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[10, 20], &[30]]).await?;
            let files = metadata_file_names(&table_root)?;

            let mut dataset = pinned_dataset(
                &table_root,
                "accelerated",
                "iceberg_metadata_path",
                &format!("/metadata/{}", files[2]),
            );
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                ..Acceleration::default()
            });
            let rt = start_runtime("iceberg_metadata_path_cayenne", vec![dataset]).await;
            crate::utils::runtime_ready_check(&rt).await;

            assert_eq!(ids(&rt, "accelerated").await?, vec![10, 20, 30]);

            std::fs::remove_dir_all(&table_root)?;
            assert_eq!(
                ids(&rt, "accelerated").await?,
                vec![10, 20, 30],
                "with the source files gone, the rows must come from the acceleration"
            );
            Ok(())
        })
        .await
}

/// A table pinned to one metadata file has no catalog to commit to, so it cannot
/// take writes. Refusing `access: read_write` when the dataset loads stops an
/// insert from writing data files the rejected commit would orphan in the table's
/// location. Both spellings of the parameter are refused.
#[cfg(feature = "iceberg-write")]
#[tokio::test]
async fn a_pinned_table_refuses_read_write_access() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[1]]).await?;
            let files = metadata_file_names(&table_root)?;
            let data_files_before = std::fs::read_dir(table_root.join("data"))?.count();

            let mut pinned = pinned_dataset(
                &table_root,
                "pinned_writable",
                "iceberg_metadata_path",
                &format!("/metadata/{}", files[1]),
            );
            pinned.access = spicepod::component::access::AccessMode::ReadWrite;
            let mut legacy = pinned_dataset(
                &table_root,
                "legacy_writable",
                "metadata_path",
                &format!("file://{}/metadata/{}", table_root.display(), files[1]),
            );
            legacy.access = spicepod::component::access::AccessMode::ReadWrite;

            let errors =
                dataset_errors("iceberg_metadata_path_read_write", vec![pinned, legacy]).await?;
            for name in ["pinned_writable", "legacy_writable"] {
                assert_eq!(
                    errors.get(name),
                    Some(&Some(format!(
                        "Failed to setup the dataset {name} (iceberg). Unable to resolve table provider: Cannot setup the dataset {name} (iceberg) with an invalid configuration. An Iceberg table read from its metadata file (`iceberg_metadata_path`) has no catalog to commit to, so it is read-only. Remove `access: read_write` from the dataset, or read the table through an Iceberg catalog to write to it. See: https://spiceai.org/docs/components/data-connectors/iceberg"
                    ))),
                    "dataset {name}"
                );
            }
            assert_eq!(
                std::fs::read_dir(table_root.join("data"))?.count(),
                data_files_before,
                "refusing the datasets must not write into the table"
            );
            Ok(())
        })
        .await
}

/// A `from` that names the metadata file itself, rather than the table root, fails
/// with the exact `from` and `iceberg_metadata_path` to use instead.
#[tokio::test]
async fn a_from_naming_a_metadata_file_is_refused_with_the_corrected_configuration()
-> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[1]]).await?;
            let files = metadata_file_names(&table_root)?;

            let dataset = Dataset::new(
                format!("iceberg:file://{}/metadata/{}", table_root.display(), files[1]),
                "misplaced",
            );
            let errors = dataset_errors("iceberg_metadata_file_in_from", vec![dataset]).await?;
            assert_eq!(
                errors.get("misplaced"),
                Some(&Some(format!(
                    "Cannot setup the dataset misplaced (iceberg) with an invalid configuration. `from` names an Iceberg metadata file, but it must name the table root. Set `from` to 'iceberg:file://{}' and `iceberg_metadata_path` to '/metadata/{}'. See: https://spiceai.org/docs/components/data-connectors/iceberg",
                    table_root.display(),
                    files[1]
                )))
            );
            Ok(())
        })
        .await
}

const S3_ACCESS_KEY: &str = "spiceadmin";
const S3_SECRET_KEY: &str = "spiceintegrationsecret";
const S3_BUCKET: &str = "lake";

async fn start_object_store() -> anyhow::Result<RunningContainer> {
    let container = ContainerRunnerBuilder::new("spice_test_rustfs_iceberg_metadata_file")
        .image("rustfs/rustfs:latest".to_string())
        .publish_port(9000)
        .add_env_var("RUSTFS_ACCESS_KEY", S3_ACCESS_KEY)
        .add_env_var("RUSTFS_SECRET_KEY", S3_SECRET_KEY)
        .command(["/data"])
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD-SHELL".to_string(),
                "netstat -tulpn | grep 9000 || exit 1".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(1_000_000_000),
            retries: Some(20),
            start_period: Some(2_000_000_000),
            start_interval: None,
        })
        .build()?
        .run(Some(Duration::from_mins(2)))
        .await?;
    wait_for_tcp_port(
        "127.0.0.1",
        container.host_port(9000)?,
        Duration::from_mins(1),
    )
    .await?;
    Ok(container)
}

fn s3_client(endpoint: &str) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .credentials_provider(Credentials::new(
                S3_ACCESS_KEY,
                S3_SECRET_KEY,
                None,
                None,
                "test",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .behavior_version_latest()
            .build(),
    )
}

async fn create_bucket(endpoint: &str) -> anyhow::Result<()> {
    let s3 = s3_client(endpoint);
    // The store can accept connections a moment before it accepts requests.
    let mut last_error = None;
    for _ in 0..50 {
        match s3.create_bucket().bucket(S3_BUCKET).send().await {
            Ok(_) => return Ok(()),
            Err(e) => last_error = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("could not create bucket {S3_BUCKET}: {last_error:?}")
}

/// The metadata file name for each version under `prefix`, indexed by version.
async fn s3_metadata_file_names(endpoint: &str, prefix: &str) -> anyhow::Result<Vec<String>> {
    let listed = s3_client(endpoint)
        .list_objects_v2()
        .bucket(S3_BUCKET)
        .prefix(prefix)
        .send()
        .await?;
    ordered_metadata_names(
        listed
            .contents()
            .iter()
            .filter_map(|object| Some(object.key()?.rsplit('/').next()?.to_string())),
    )
}

/// A table written to S3 is read from its pinned metadata file through both S3
/// URL schemes. The writer records `s3://` locations, so the `s3a://` dataset also
/// shows that the scheme in `from` need not match the one inside the metadata.
/// Pinning a version older than the latest keeps the check non-vacuous.
#[tokio::test]
async fn iceberg_metadata_path_reads_a_pinned_table_from_s3() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let container = start_object_store().await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            create_bucket(&endpoint).await?;

            write_table_at(
                &format!("s3://{S3_BUCKET}/warehouse"),
                Arc::new(OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                }),
                HashMap::from([
                    ("s3.endpoint".to_string(), endpoint.clone()),
                    ("s3.path-style-access".to_string(), "true".to_string()),
                    ("s3.access-key-id".to_string(), S3_ACCESS_KEY.to_string()),
                    (
                        "s3.secret-access-key".to_string(),
                        S3_SECRET_KEY.to_string(),
                    ),
                    ("s3.region".to_string(), "us-east-1".to_string()),
                ]),
                &[&[1], &[2, 3], &[4]],
            )
            .await?;
            let files = s3_metadata_file_names(
                &endpoint,
                &format!("warehouse/{NAMESPACE}/{TABLE}/metadata/"),
            )
            .await?;
            anyhow::ensure!(files.len() == 4, "fixture has {files:?}");

            let s3_dataset = |scheme: &str, name: &str| {
                dataset_with_params(
                    format!("iceberg:{scheme}://{S3_BUCKET}/warehouse/{NAMESPACE}/{TABLE}"),
                    name,
                    HashMap::from([
                        (
                            "iceberg_metadata_path".to_string(),
                            format!("/metadata/{}", files[2]),
                        ),
                        ("iceberg_s3_endpoint".to_string(), endpoint.clone()),
                        (
                            "iceberg_s3_access_key_id".to_string(),
                            S3_ACCESS_KEY.to_string(),
                        ),
                        (
                            "iceberg_s3_secret_access_key".to_string(),
                            S3_SECRET_KEY.to_string(),
                        ),
                        ("iceberg_s3_region".to_string(), "us-east-1".to_string()),
                    ]),
                )
            };
            let rt = start_runtime(
                "iceberg_metadata_path_s3",
                vec![s3_dataset("s3", "via_s3"), s3_dataset("s3a", "via_s3a")],
            )
            .await;

            for name in ["via_s3", "via_s3a"] {
                assert_eq!(
                    ids(&rt, name).await?,
                    vec![1, 2, 3],
                    "dataset {name} must read the snapshot its pinned metadata file records"
                );
            }
            drop(container);
            Ok(())
        })
        .await
}

/// The current metadata file Glue records for `namespace.table` in the shared
/// Iceberg test fixture, read with the same `AWS_ICEBERG_*` credentials the Glue
/// REST tests use. Looked up rather than written down, so a rewritten fixture moves
/// the pin with it and both reads always see the same snapshot.
pub(crate) async fn glue_metadata_location(namespace: &str, table: &str) -> anyhow::Result<String> {
    use anyhow::Context as _;
    let env = |name: &str| std::env::var(name).with_context(|| format!("{name} is not set"));
    let config = aws_sdk_glue::Config::builder()
        .credentials_provider(aws_sdk_glue::config::Credentials::new(
            env("AWS_ICEBERG_ACCESS_KEY_ID")?,
            env("AWS_ICEBERG_SECRET_ACCESS_KEY")?,
            None,
            None,
            "iceberg-test",
        ))
        .region(aws_sdk_glue::config::Region::new(env(
            "AWS_ICEBERG_REGION",
        )?))
        .behavior_version_latest()
        .build();
    let response = aws_sdk_glue::Client::from_conf(config)
        .get_table()
        .catalog_id(env("AWS_ICEBERG_ACCOUNT_ID")?)
        .database_name(namespace)
        .name(table)
        .send()
        .await?;
    response
        .table()
        .and_then(|table| table.parameters())
        .and_then(|parameters| parameters.get("metadata_location"))
        .cloned()
        .with_context(|| format!("Glue records no metadata_location for {namespace}.{table}"))
}

/// A dataset pinned to the Glue-recorded `metadata_location`, written the
/// documented way: `from` is the table root and `iceberg_metadata_path` the file
/// relative to it.
pub(crate) fn pinned_glue_dataset(metadata_location: &str, name: &str) -> anyhow::Result<Dataset> {
    let (root, file) = metadata_location.rsplit_once("/metadata/").ok_or_else(|| {
        anyhow::anyhow!("'{metadata_location}' is not under a metadata/ directory")
    })?;
    let mut params = crate::iceberg::connector::glue_s3_params();
    params.insert(
        "iceberg_metadata_path".to_string(),
        format!("/metadata/{file}"),
    );
    Ok(dataset_with_params(format!("iceberg:{root}"), name, params))
}

/// The differential check against a real catalog: a table AWS Glue manages, on S3,
/// read once through the Glue Iceberg REST endpoint and once from the metadata file
/// Glue records for it, must return the same rows. The row multisets are compared
/// in both directions, and the count is pinned to TPC-H SF1's 150,000 customers so
/// two empty or truncated reads cannot agree.
#[tokio::test]
async fn a_glue_table_reads_the_same_rows_pinned_as_through_the_glue_catalog() -> anyhow::Result<()>
{
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let location = glue_metadata_location("tpch_sf1", "customer").await?;
            let through_glue = crate::iceberg::connector::make_iceberg_dataset(
                "tpch_sf1",
                "customer",
                "customer_glue",
            )?;
            let pinned = pinned_glue_dataset(&location, "customer_pinned")?;
            let rt = start_runtime("iceberg_metadata_path_glue", vec![through_glue, pinned]).await;

            for sql in [
                "SELECT count(*) FROM customer_glue",
                "SELECT count(*) FROM customer_pinned",
            ] {
                assert_eq!(count(&rt, sql).await?, 150_000, "{sql}");
            }
            for sql in [
                "SELECT count(*) FROM (SELECT * FROM customer_glue EXCEPT ALL SELECT * FROM customer_pinned)",
                "SELECT count(*) FROM (SELECT * FROM customer_pinned EXCEPT ALL SELECT * FROM customer_glue)",
            ] {
                assert_eq!(
                    count(&rt, sql).await?,
                    0,
                    "{sql}: the pinned read ({location}) and the Glue read differ"
                );
            }
            Ok(())
        })
        .await
}

async fn count(rt: &Arc<Runtime>, sql: &str) -> anyhow::Result<i64> {
    match int64_column(rt, sql).await?.as_slice() {
        [count] => Ok(*count),
        other => anyhow::bail!("{sql} returned {other:?}, not one count"),
    }
}
