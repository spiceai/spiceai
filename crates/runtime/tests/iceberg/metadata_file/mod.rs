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

//! Iceberg datasets read from their metadata files with no catalog: pinned with
//! `iceberg_metadata_path`, or found from the table root. The tables are written by
//! iceberg-rust to the local filesystem, or to S3 in a `RustFS` container.

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
    component::{caching::SQLResultsCacheConfig, dataset::Dataset},
    param::Params as DatasetParams,
};

use crate::{
    configure_test_datafusion,
    docker::{ContainerRunnerBuilder, RunningContainer, wait_for_tcp_port},
    init_tracing,
    utils::{run_query, test_request_context, wait_until_true},
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
/// `props` (endpoint, credentials) passed to its file IO. The returned writer
/// appends further commits.
async fn write_table_at(
    warehouse_url: &str,
    factory: Arc<dyn StorageFactory>,
    mut props: HashMap<String, String>,
    appends: &[&[i64]],
) -> anyhow::Result<TableWriter> {
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

    let writer = TableWriter {
        catalog: Arc::new(catalog),
        namespace,
    };
    for rows in appends {
        writer.append(rows).await?;
    }
    Ok(writer)
}

/// Commits appends to the table [`write_table_at`] created.
struct TableWriter {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
}

impl TableWriter {
    /// Appends `rows` as one commit.
    async fn append(&self, rows: &[i64]) -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        let provider =
            IcebergTableProvider::try_new(Arc::clone(&self.catalog), self.namespace.clone(), TABLE)
                .await?;
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
        Ok(())
    }
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

/// Starts a runtime serving `datasets`, without the results cache, so every query
/// reaches the table and sees its current commit.
async fn start_runtime(app_name: &str, datasets: Vec<Dataset>) -> Arc<Runtime> {
    let mut builder = AppBuilder::new(app_name).with_sql_cache(SQLResultsCacheConfig {
        enabled: false,
        ..SQLResultsCacheConfig::default()
    });
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
                        "Failed to setup the dataset {name} (iceberg). Unable to resolve table provider: Cannot setup the dataset {name} (iceberg) with an invalid configuration. An Iceberg table read from its metadata file (`iceberg_metadata_path`) has no catalog to commit to, so it is read-only. Remove `access: read_write`, keep writes in the acceleration with `acceleration.write_mode: acceleration`, or read the table through an Iceberg catalog to write to it. See: https://spiceai.org/docs/components/data-connectors/iceberg"
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

/// Writes kept in the acceleration (`write_mode: acceleration`) never reach the
/// source, so a pinned table with `access: read_write` loads and accepts them, and
/// the table's own files are left untouched.
#[tokio::test]
async fn a_pinned_table_takes_writes_kept_in_its_acceleration() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[1], &[2]]).await?;
            let files = metadata_file_names(&table_root)?;
            let data_files_before = std::fs::read_dir(table_root.join("data"))?.count();

            let mut dataset = pinned_dataset(
                &table_root,
                "accelerated_writes",
                "iceberg_metadata_path",
                &format!("/metadata/{}", files[2]),
            );
            dataset.access = spicepod::component::access::AccessMode::ReadWrite;
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                write_mode: spicepod::acceleration::WriteMode::Acceleration,
                ..Acceleration::default()
            });
            let rt = start_runtime("iceberg_metadata_path_accelerated_writes", vec![dataset]).await;
            crate::utils::runtime_ready_check(&rt).await;

            run_query(&rt, "INSERT INTO accelerated_writes VALUES (99)").await?;
            assert_eq!(ids(&rt, "accelerated_writes").await?, vec![1, 2, 99]);
            assert_eq!(
                std::fs::read_dir(table_root.join("data"))?.count(),
                data_files_before,
                "a write kept in the acceleration must not write into the table"
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

/// A `from` that is not an Iceberg table root, such as one level too deep, fails
/// to load with what the table root must contain.
#[tokio::test]
async fn a_from_with_no_metadata_files_is_refused_with_what_a_table_root_holds()
-> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let warehouse = tempfile::tempdir()?;
            let table_root = write_table(warehouse.path(), &[&[1]]).await?;
            let too_deep = table_root.join("metadata");
            let errors = dataset_errors(
                "iceberg_table_root_without_metadata",
                vec![dataset_with_params(
                    format!("iceberg:file://{}", too_deep.display()),
                    "too_deep",
                    HashMap::new(),
                )],
            )
            .await?;
            assert_eq!(
                errors.get("too_deep"),
                Some(&Some(format!(
                    "Failed to load the dataset too_deep (iceberg). Failed to find the current Iceberg metadata file for 'file://{root}': DataInvalid => No Iceberg metadata file was found in 'file://{root}/metadata/'. The table root must contain a 'metadata/' directory with files named 'v<N>.metadata.json' or '<N>-<uuid>.metadata.json'. Verify `from` is the table root, the directory that contains 'metadata/', or set `iceberg_metadata_path` to the metadata file to read. See: https://spiceai.org/docs/components/data-connectors/iceberg",
                    root = too_deep.display()
                )))
            );
            Ok(())
        })
        .await
}

pub(crate) const S3_ACCESS_KEY: &str = "spiceadmin";
pub(crate) const S3_SECRET_KEY: &str = "spiceintegrationsecret";
pub(crate) const S3_BUCKET: &str = "lake";

/// A RustFS object store in a container, healthy and reachable on its published port.
pub(crate) async fn start_object_store(name: &str) -> anyhow::Result<RunningContainer> {
    let container = ContainerRunnerBuilder::new(name)
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

pub(crate) async fn create_bucket(endpoint: &str) -> anyhow::Result<()> {
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

const S3_METADATA_PREFIX: &str = "warehouse/db/events/metadata/";

/// [`write_table`] into the bucket, at `s3://lake/warehouse/db/events`.
async fn write_s3_table(endpoint: &str, appends: &[&[i64]]) -> anyhow::Result<TableWriter> {
    write_table_at(
        &format!("s3://{S3_BUCKET}/warehouse"),
        Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }),
        HashMap::from([
            ("s3.endpoint".to_string(), endpoint.to_string()),
            ("s3.path-style-access".to_string(), "true".to_string()),
            ("s3.access-key-id".to_string(), S3_ACCESS_KEY.to_string()),
            (
                "s3.secret-access-key".to_string(),
                S3_SECRET_KEY.to_string(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
        ]),
        appends,
    )
    .await
}

/// A dataset reading the table [`write_s3_table`] writes, through the `scheme` URL
/// (`s3` or `s3a`), pinned to `metadata_path` when it is given.
fn s3_dataset(endpoint: &str, scheme: &str, name: &str, metadata_path: Option<String>) -> Dataset {
    let mut params = HashMap::from([
        ("iceberg_s3_endpoint".to_string(), endpoint.to_string()),
        (
            "iceberg_s3_access_key_id".to_string(),
            S3_ACCESS_KEY.to_string(),
        ),
        (
            "iceberg_s3_secret_access_key".to_string(),
            S3_SECRET_KEY.to_string(),
        ),
        ("iceberg_s3_region".to_string(), "us-east-1".to_string()),
    ]);
    if let Some(path) = metadata_path {
        params.insert("iceberg_metadata_path".to_string(), path);
    }
    dataset_with_params(
        format!("iceberg:{scheme}://{S3_BUCKET}/warehouse/{NAMESPACE}/{TABLE}"),
        name,
        params,
    )
}

/// The customer-shaped table: written to S3 the way a REST or Glue catalog writes
/// one (`<N>-<uuid>.metadata.json`, no `version-hint.text`), read with no pin from
/// the table root through both S3 schemes and through an acceleration. Commits
/// made after the datasets load, taking the table past version 9, must be read by
/// the next query and the next refresh, with no restart.
#[tokio::test]
async fn an_unpinned_table_root_on_s3_reads_each_new_commit() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let container = start_object_store("spice_test_rustfs_iceberg_metadata_file").await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            create_bucket(&endpoint).await?;
            let writer = write_s3_table(&endpoint, &[&[1], &[2]]).await?;

            let mut accelerated = s3_dataset(&endpoint, "s3", "accelerated", None);
            accelerated.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::Memory,
                refresh_mode: Some(RefreshMode::Full),
                refresh_check_interval: Some("1s".to_string()),
                ..Acceleration::default()
            });
            let rt = start_runtime(
                "iceberg_table_root_s3",
                vec![
                    s3_dataset(&endpoint, "s3", "via_s3", None),
                    s3_dataset(&endpoint, "s3a", "via_s3a", None),
                    accelerated,
                ],
            )
            .await;
            crate::utils::runtime_ready_check(&rt).await;
            for name in ["via_s3", "via_s3a", "accelerated"] {
                assert_eq!(ids(&rt, name).await?, vec![1, 2], "dataset {name} at load");
            }

            let expected: Vec<i64> = (1..=12).collect();
            for id in 3..=12 {
                writer.append(&[id]).await?;
            }
            let files = s3_metadata_file_names(&endpoint, S3_METADATA_PREFIX).await?;
            anyhow::ensure!(files.len() == 13, "fixture has {files:?}");
            for name in ["via_s3", "via_s3a"] {
                assert_eq!(
                    ids(&rt, name).await?,
                    expected,
                    "dataset {name} must read the latest commit on the next query"
                );
            }
            // The acceleration catches up on its next refresh.
            wait_until_true(Duration::from_mins(1), || async {
                ids(&rt, "accelerated")
                    .await
                    .is_ok_and(|ids| ids == expected)
            })
            .await;
            assert_eq!(ids(&rt, "accelerated").await?, expected);
            drop(container);
            Ok(())
        })
        .await
}

/// A `version-hint.text` names the metadata file to read, ahead of newer files in
/// `metadata/`, whether it is found from the table root or pinned with
/// `iceberg_metadata_path`. The hint is read on every query, so moving it moves
/// both datasets. The hint holds a file name stem, as `DuckDB` accepts.
#[tokio::test]
async fn a_version_hint_on_s3_is_read_on_every_query() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let container = start_object_store("spice_test_rustfs_iceberg_metadata_file").await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            create_bucket(&endpoint).await?;
            write_s3_table(&endpoint, &[&[1], &[2], &[3]]).await?;
            let stems: Vec<String> = s3_metadata_file_names(&endpoint, S3_METADATA_PREFIX)
                .await?
                .iter()
                .map(|name| name.trim_end_matches(".metadata.json").to_string())
                .collect();
            let hint_key = format!("{S3_METADATA_PREFIX}version-hint.text");
            let s3 = s3_client(&endpoint);
            let put_hint = |stem: &str| {
                s3.put_object()
                    .bucket(S3_BUCKET)
                    .key(&hint_key)
                    .body(stem.as_bytes().to_vec().into())
                    .send()
            };
            put_hint(&stems[1]).await?;

            let rt = start_runtime(
                "iceberg_version_hint_s3",
                vec![
                    s3_dataset(&endpoint, "s3", "from_root", None),
                    s3_dataset(
                        &endpoint,
                        "s3",
                        "pinned_hint",
                        Some("/metadata/version-hint.text".to_string()),
                    ),
                ],
            )
            .await;
            for name in ["from_root", "pinned_hint"] {
                assert_eq!(
                    ids(&rt, name).await?,
                    vec![1],
                    "dataset {name}, hint at version 1"
                );
            }

            put_hint(&stems[2]).await?;
            for name in ["from_root", "pinned_hint"] {
                assert_eq!(
                    ids(&rt, name).await?,
                    vec![1, 2],
                    "dataset {name}, hint at version 2"
                );
            }
            drop(container);
            Ok(())
        })
        .await
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
            let container = start_object_store("spice_test_rustfs_iceberg_metadata_file").await?;
            let endpoint = format!("http://127.0.0.1:{}", container.host_port(9000)?);
            create_bucket(&endpoint).await?;

            write_s3_table(&endpoint, &[&[1], &[2, 3], &[4]]).await?;
            let files = s3_metadata_file_names(&endpoint, S3_METADATA_PREFIX).await?;
            anyhow::ensure!(files.len() == 4, "fixture has {files:?}");

            let pin = Some(format!("/metadata/{}", files[2]));
            let rt = start_runtime(
                "iceberg_metadata_path_s3",
                vec![
                    s3_dataset(&endpoint, "s3", "via_s3", pin.clone()),
                    s3_dataset(&endpoint, "s3a", "via_s3a", pin),
                ],
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
    let (root, file) = split_metadata_location(metadata_location)?;
    let mut params = crate::iceberg::connector::glue_s3_params();
    params.insert(
        "iceberg_metadata_path".to_string(),
        format!("/metadata/{file}"),
    );
    Ok(dataset_with_params(format!("iceberg:{root}"), name, params))
}

/// A dataset reading the Glue table rooted where `metadata_location` lives, with no
/// pin, so each query finds the current metadata file by listing `metadata/`.
fn unpinned_glue_dataset(metadata_location: &str, name: &str) -> anyhow::Result<Dataset> {
    let (root, _) = split_metadata_location(metadata_location)?;
    Ok(dataset_with_params(
        format!("iceberg:{root}"),
        name,
        crate::iceberg::connector::glue_s3_params(),
    ))
}

/// Splits a metadata file URL into the table root and the file name.
fn split_metadata_location(metadata_location: &str) -> anyhow::Result<(&str, &str)> {
    metadata_location
        .rsplit_once("/metadata/")
        .ok_or_else(|| anyhow::anyhow!("'{metadata_location}' is not under a metadata/ directory"))
}

/// The metadata file `dataset` reads now: the location of the table its Iceberg
/// catalog loads, the same lookup Spice's Iceberg REST `loadTable` serves.
async fn current_metadata_location(rt: &Arc<Runtime>, dataset: &str) -> anyhow::Result<String> {
    use anyhow::Context as _;
    let table = rt
        .datafusion()
        .get_table(&datafusion::common::TableReference::bare(dataset))
        .await
        .with_context(|| format!("dataset {dataset} is not registered"))?;
    let provider = spice_table::peel_to(&table, spice_table::LayerWalk::Passthrough)
        .downcast_ref::<IcebergTableProvider>()
        .with_context(|| format!("dataset {dataset} is not read as an Iceberg table"))?;
    let loaded = provider
        .catalog()
        .load_table(provider.table_ident())
        .await?;
    loaded
        .metadata_location()
        .map(str::to_string)
        .with_context(|| format!("dataset {dataset} reads no metadata file"))
}

/// The differential check against a real catalog: a table AWS Glue manages, on S3,
/// read through the Glue Iceberg REST endpoint, from the metadata file Glue records
/// for it, and from its table root with no pin, must return the same rows. The
/// unpinned read must also pick the metadata file Glue records, so the check does
/// not depend on the newest commit changing rows. The row multisets are compared in
/// both directions, and the count is pinned to TPC-H SF1's 150,000 customers so two
/// empty or truncated reads cannot agree.
#[tokio::test]
async fn a_glue_table_reads_the_same_rows_pinned_and_unpinned_as_through_the_glue_catalog()
-> anyhow::Result<()> {
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
            let unpinned = unpinned_glue_dataset(&location, "customer_root")?;
            let rt = start_runtime(
                "iceberg_metadata_path_glue",
                vec![through_glue, pinned, unpinned],
            )
            .await;

            assert_eq!(
                current_metadata_location(&rt, "customer_root").await?,
                location,
                "the unpinned read must pick the metadata file Glue records"
            );
            for name in ["customer_glue", "customer_pinned", "customer_root"] {
                let sql = format!("SELECT count(*) FROM {name}");
                assert_eq!(count(&rt, &sql).await?, 150_000, "{sql}");
            }
            for read in ["customer_pinned", "customer_root"] {
                for (left, right) in [("customer_glue", read), (read, "customer_glue")] {
                    let sql = format!(
                        "SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})"
                    );
                    assert_eq!(
                        count(&rt, &sql).await?,
                        0,
                        "{sql}: the {read} read and the Glue read ({location}) differ"
                    );
                }
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
