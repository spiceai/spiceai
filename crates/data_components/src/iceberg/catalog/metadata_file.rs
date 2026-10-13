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

//! A read-only Iceberg catalog that holds exactly one table, read from its
//! `metadata.json` files rather than from a catalog service.
//!
//! The catalog's only role on the read path is to name a table's current metadata
//! file; given that file, the metadata → manifest list → manifest → data file chain
//! needs no catalog. Presenting the table through the [`Catalog`] trait keeps it an
//! ordinary `IcebergTableProvider`, which the distributed scan codec, the Iceberg
//! passthrough API and the availability monitor all recognise by that type.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::spec::TableMetadata;
use iceberg::table::{StaticTable, Table};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use opendal::Operator;

/// The file name of the Hadoop convention's pointer to the current metadata file.
const VERSION_HINT_FILE: &str = "version-hint.text";

/// A catalog serving one table read from its metadata files.
///
/// The table is read from a pinned metadata file, which is immutable once written
/// and so is loaded once, or from the metadata file a version hint names, which is
/// found again on every [`Catalog::load_table`] so each query reads the latest
/// commit. Every mutating operation fails with [`ErrorKind::FeatureUnsupported`].
#[derive(Debug, Clone)]
pub struct MetadataFileCatalog {
    ident: TableIdent,
    file_io: FileIO,
    source: Source,
}

#[derive(Debug, Clone)]
enum Source {
    File(Table),
    /// A version hint; with `listing`, a missing hint falls back to the highest
    /// version listed there.
    Hint {
        hint: String,
        listing: Option<MetadataListing>,
    },
}

/// Lists a table's `metadata/` directory: `path` under `operator`.
#[derive(Debug, Clone)]
pub struct MetadataListing {
    pub operator: Operator,
    pub path: String,
}

/// The metadata file a table is currently read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMetadata {
    pub location: String,
    /// Whether it was found by listing `metadata/`, which can pick up a commit that
    /// is in progress or failed, rather than named by a pin or a version hint.
    pub listed: bool,
}

impl MetadataFileCatalog {
    /// Serves the table pinned by `location`, a full URL the `file_io` can read.
    ///
    /// A metadata file is loaded once; gzip-compressed metadata is decompressed
    /// transparently. A version hint file (`*.text` or `*.txt`, such as
    /// `version-hint.text`) is read on every load instead.
    ///
    /// # Errors
    ///
    /// Returns an error if a metadata file cannot be read or is not valid Iceberg
    /// table metadata.
    pub async fn try_new(location: &str, ident: TableIdent, file_io: FileIO) -> Result<Self> {
        let source = if std::path::Path::new(location)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("text") || ext.eq_ignore_ascii_case("txt"))
        {
            Source::Hint {
                hint: location.to_string(),
                listing: None,
            }
        } else {
            Source::File(read_table(location, &ident, &file_io).await?)
        };
        Ok(Self {
            ident,
            file_io,
            source,
        })
    }

    /// Serves the table rooted at `root`, a full URL the `file_io` can read, whose
    /// `metadata/` directory `operator` lists (it is rooted at the table root).
    /// See [`find_current_metadata`] for how the current metadata file is found.
    #[must_use]
    pub fn for_table_root(
        root: &str,
        operator: Operator,
        ident: TableIdent,
        file_io: FileIO,
    ) -> Self {
        Self {
            ident,
            file_io,
            source: Source::Hint {
                hint: format!("{root}/metadata/{VERSION_HINT_FILE}"),
                listing: Some(MetadataListing {
                    operator,
                    path: "metadata/".to_string(),
                }),
            },
        }
    }

    /// Finds the metadata file the table is read from now.
    ///
    /// # Errors
    ///
    /// Returns an error if storage cannot be read, a version hint names no existing
    /// metadata file, or the table root has no metadata file.
    pub async fn current_metadata(&self) -> Result<CurrentMetadata> {
        match &self.source {
            Source::File(table) => Ok(CurrentMetadata {
                location: table.metadata_location().unwrap_or_default().to_string(),
                listed: false,
            }),
            Source::Hint { hint, listing } => {
                find_current_metadata(&self.file_io, hint, listing.as_ref()).await
            }
        }
    }

    fn is_table(&self, ident: &TableIdent) -> bool {
        ident == &self.ident
    }

    fn is_namespace(&self, namespace: &NamespaceIdent) -> bool {
        namespace == self.ident.namespace()
    }

    fn read_only(&self) -> Error {
        Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Iceberg table '{}' is read directly from its metadata file without a catalog, so it is read-only. Writes require a catalog.",
                self.ident
            ),
        )
    }

    fn table_not_found(table: &TableIdent) -> Error {
        Error::new(
            ErrorKind::TableNotFound,
            format!("Table '{table}' does not exist"),
        )
    }
}

/// Finds a table's current metadata file, in the directory of the version hint
/// `hint` (a full URL the `file_io` can read): the file the hint names, or, when
/// `listing` is given and the hint does not exist, the listed file with the highest
/// version number (`v<N>.metadata.json` or `<N>-<uuid>.metadata.json`). Files that
/// share the highest version, as a failed commit can leave behind, are told apart
/// by `last-updated-ms`, as the Iceberg spec directs, then by file name, so the
/// choice never depends on listing order.
///
/// # Errors
///
/// Returns an error if storage cannot be read, the hint names no existing metadata
/// file, or no metadata file is listed.
pub async fn find_current_metadata(
    file_io: &FileIO,
    hint: &str,
    listing: Option<&MetadataListing>,
) -> Result<CurrentMetadata> {
    let directory = hint.rsplit_once('/').map_or("", |(directory, _)| directory);
    match listing {
        Some(listing) if !file_io.exists(hint).await? => Ok(CurrentMetadata {
            location: newest_metadata_file(file_io, directory, listing).await?,
            listed: true,
        }),
        _ => Ok(CurrentMetadata {
            location: metadata_named_by(file_io, hint, directory).await?,
            listed: false,
        }),
    }
}

/// The metadata file the version hint names. The hint holds a version (`3`, for
/// `v3.metadata.json`) or a file name stem (`00003-8f2c`, for
/// `00003-8f2c.metadata.json`).
async fn metadata_named_by(file_io: &FileIO, hint: &str, directory: &str) -> Result<String> {
    let content = file_io.new_input(hint)?.read().await?;
    let version = std::str::from_utf8(&content)
        .map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("The version hint '{hint}' is not valid UTF-8: {e}"),
            )
        })?
        .trim();
    if version.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("The version hint '{hint}' is empty"),
        ));
    }

    let stems = if version.bytes().all(|b| b.is_ascii_digit()) {
        vec![format!("v{version}"), version.to_string()]
    } else {
        vec![version.to_string()]
    };
    let candidates: Vec<String> = stems
        .iter()
        .flat_map(|stem| {
            [
                format!("{stem}.metadata.json"),
                format!("{stem}.gz.metadata.json"),
            ]
        })
        .collect();
    for name in &candidates {
        let location = format!("{directory}/{name}");
        if file_io.exists(&location).await? {
            return Ok(location);
        }
    }
    Err(Error::new(
        ErrorKind::DataInvalid,
        format!(
            "The version hint '{hint}' names version '{version}', but none of {} exists in '{directory}/'",
            candidates.join(", ")
        ),
    ))
}

async fn newest_metadata_file(
    file_io: &FileIO,
    directory: &str,
    listing: &MetadataListing,
) -> Result<String> {
    let entries = match listing.operator.list(&listing.path).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == opendal::ErrorKind::NotFound => Vec::new(),
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to list '{directory}/': {e}"),
            ));
        }
    };
    let Some(names) = newest_metadata_names(entries.iter().map(opendal::Entry::name)) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "No Iceberg metadata file was found in '{directory}/'. The table root must contain a 'metadata/' directory with files named 'v<N>.metadata.json' or '<N>-<uuid>.metadata.json'"
            ),
        ));
    };

    let mut newest = None;
    for name in names {
        let location = format!("{directory}/{name}");
        let updated = TableMetadata::read_from(file_io, &location)
            .await?
            .last_updated_ms();
        if newest
            .as_ref()
            .is_none_or(|(newest_updated, newest_location)| {
                (updated, &location) > (*newest_updated, newest_location)
            })
        {
            newest = Some((updated, location));
        }
    }
    newest
        .map(|(_, location)| location)
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "No metadata file name"))
}

async fn read_table(location: &str, ident: &TableIdent, file_io: &FileIO) -> Result<Table> {
    Ok(
        StaticTable::from_metadata_file(location, ident.clone(), file_io.clone())
            .await?
            .into_table(),
    )
}

/// The version number of a metadata file named `v<N>.metadata.json` or
/// `<N>-<uuid>.metadata.json`, plain or gzip-compressed (`.gz.metadata.json`, or
/// the older `.metadata.json.gz`). Leading zeros are padding. Any other file name
/// has no version.
fn metadata_version(file_name: &str) -> Option<u64> {
    let stem = [".gz.metadata.json", ".metadata.json.gz", ".metadata.json"]
        .into_iter()
        .find_map(|suffix| file_name.strip_suffix(suffix))?;
    let stem = stem.strip_prefix('v').unwrap_or(stem);
    let digits_end = stem
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(stem.len());
    let (digits, rest) = stem.split_at(digits_end);
    if digits.is_empty() || !(rest.is_empty() || rest.starts_with('-')) {
        return None;
    }
    digits.parse().ok()
}

/// Every file name among `names` with the highest metadata version.
fn newest_metadata_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<Vec<&'a str>> {
    let mut newest: Option<(u64, Vec<&str>)> = None;
    for name in names {
        let Some(version) = metadata_version(name) else {
            continue;
        };
        match &mut newest {
            Some((highest, names)) if version == *highest => names.push(name),
            Some((highest, _)) if version < *highest => {}
            _ => newest = Some((version, vec![name])),
        }
    }
    newest.map(|(_, names)| names)
}

#[async_trait]
impl Catalog for MetadataFileCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Ok(match parent {
            None => vec![self.ident.namespace().clone()],
            Some(_) => Vec::new(),
        })
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(self.read_only())
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        if self.is_namespace(namespace) {
            Ok(Namespace::new(namespace.clone()))
        } else {
            Err(Error::new(
                ErrorKind::NamespaceNotFound,
                format!("Namespace '{}' does not exist", namespace.join(".")),
            ))
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        Ok(self.is_namespace(namespace))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(self.read_only())
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(self.read_only())
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        Ok(if self.is_namespace(namespace) {
            vec![self.ident.clone()]
        } else {
            Vec::new()
        })
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(self.read_only())
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        if !self.is_table(table) {
            return Err(Self::table_not_found(table));
        }
        match &self.source {
            Source::File(table) => Ok(table.clone()),
            Source::Hint { .. } => {
                let current = self.current_metadata().await?;
                read_table(&current.location, &self.ident, &self.file_io).await
            }
        }
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(self.read_only())
    }

    async fn purge_table(&self, _table: &TableIdent) -> Result<()> {
        Err(self.read_only())
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        Ok(self.is_table(table))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(self.read_only())
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(self.read_only())
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(self.read_only())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use datafusion::arrow::array::AsArray as _;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::catalog::TableProvider;
    use datafusion::logical_expr::{Expr, col, lit};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{CatalogBuilder, TableCreation};
    use iceberg_datafusion::IcebergTableProvider;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::iceberg::delete::IcebergDeletionProvider;

    const NAMESPACE: &str = "ns";
    const TABLE: &str = "events";

    /// A table iceberg-rust writes to the local filesystem through an in-memory
    /// catalog, so its metadata files are named the way a catalog-managed table
    /// names them (`<N>-<uuid>.metadata.json`) and there is no `version-hint.text`.
    struct Writer {
        catalog: Arc<dyn Catalog>,
        ident: TableIdent,
        root: PathBuf,
    }

    impl Writer {
        /// Creates `ns.events` (one `id BIGINT` column) under `warehouse`.
        async fn create(warehouse: &Path) -> Self {
            let catalog = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        format!("file://{}", warehouse.display()),
                    )]),
                )
                .await
                .expect("memory catalog loads");
            let namespace = NamespaceIdent::new(NAMESPACE.to_string());
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("namespace is created");
            let schema = iceberg::spec::Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema builds");
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name(TABLE.to_string())
                        .schema(schema)
                        .build(),
                )
                .await
                .expect("table is created");
            Self {
                catalog: Arc::new(catalog),
                ident: TableIdent::new(namespace, TABLE.to_string()),
                root: warehouse.join(NAMESPACE).join(TABLE),
            }
        }

        async fn provider(&self) -> IcebergTableProvider {
            IcebergTableProvider::try_new(
                Arc::clone(&self.catalog),
                self.ident.namespace().clone(),
                self.ident.name().to_string(),
            )
            .await
            .expect("writer provider is constructed")
        }

        /// Appends `rows` as one commit.
        async fn append(&self, rows: &[i64]) {
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(self.provider().await))
                .expect("writer provider registers");
            let values = rows
                .iter()
                .map(|id| format!("({id})"))
                .collect::<Vec<_>>()
                .join(", ");
            ctx.sql(&format!("INSERT INTO t VALUES {values}"))
                .await
                .expect("append plans")
                .collect()
                .await
                .expect("append commits");
        }

        /// Deletes the rows matching `filter` as one commit, through Spice's Iceberg
        /// delete path, which writes equality-delete files.
        async fn delete_where(&self, filter: Expr) {
            let inner: Arc<dyn TableProvider> = Arc::new(self.provider().await);
            let deletion = IcebergDeletionProvider::new(
                Arc::clone(&self.catalog),
                self.ident.namespace().clone(),
                self.ident.name().to_string(),
                Arc::clone(&inner),
            );
            let table = spice_table::SpiceTable::over(Arc::new(deletion), inner);
            let ctx = SessionContext::new();
            let plan = table
                .delete_from(&ctx.state(), vec![filter])
                .await
                .expect("delete plans");
            collect(plan, ctx.task_ctx()).await.expect("delete commits");
        }

        async fn metadata_location(&self) -> String {
            self.catalog
                .load_table(&self.ident)
                .await
                .expect("table loads")
                .metadata_location()
                .expect("a committed table has a metadata location")
                .to_string()
        }

        /// Every `id` the catalog's current snapshot holds, sorted.
        async fn ids_through_catalog(&self) -> Vec<i64> {
            ids_of(&Arc::new(self.provider().await)).await
        }
    }

    /// Creates the table and appends each of `appends` as its own commit. Table
    /// creation is commit 0, so after `n` appends the metadata versions are `0..=n`.
    async fn write_table(warehouse: &Path, appends: &[&[i64]]) -> PathBuf {
        let writer = Writer::create(warehouse).await;
        for rows in appends {
            writer.append(rows).await;
        }
        writer.root
    }

    /// The table's metadata files, ordered by the version number that prefixes
    /// each name.
    fn metadata_files(table_root: &Path) -> Vec<PathBuf> {
        let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(table_root.join("metadata"))
            .expect("metadata directory is listed")
            .map(|entry| entry.expect("metadata entry is read").path())
            .filter_map(|path| {
                let name = path.file_name()?.to_str()?.to_string();
                let version = name
                    .strip_suffix(".metadata.json")?
                    .split('-')
                    .next()?
                    .parse()
                    .ok()?;
                Some((version, path))
            })
            .collect();
        files.sort_by_key(|(version, _)| *version);
        files.into_iter().map(|(_, path)| path).collect()
    }

    fn served_ident() -> TableIdent {
        TableIdent::new(
            NamespaceIdent::new("spice".to_string()),
            "pinned".to_string(),
        )
    }

    /// Every `id` a scan of `provider` returns, sorted, read through SQL the way a
    /// query reads the dataset.
    async fn ids_of(provider: &Arc<IcebergTableProvider>) -> Vec<i64> {
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::clone(provider) as Arc<dyn TableProvider>)
            .expect("reader provider registers");
        let batches = ctx
            .sql("SELECT id FROM t ORDER BY id")
            .await
            .expect("scan plans")
            .collect()
            .await
            .expect("scan executes");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|value| value.expect("id is not null"))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Every `id` visible through `catalog`, read the way the data connector reads
    /// it: an `IcebergTableProvider` over the catalog.
    async fn ids_through(catalog: MetadataFileCatalog) -> Vec<i64> {
        ids_of(&Arc::new(provider_over(catalog).await)).await
    }

    async fn provider_over(catalog: MetadataFileCatalog) -> IcebergTableProvider {
        let ident = served_ident();
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        IcebergTableProvider::try_new(catalog, ident.namespace().clone(), ident.name().to_string())
            .await
            .expect("reader provider is constructed")
    }

    fn local_file_io() -> FileIO {
        FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build()
    }

    /// The catalog the data connector builds for a table root with no pin.
    fn table_root_catalog(table_root: &Path) -> MetadataFileCatalog {
        let mut config = opendal::services::FsConfig::default();
        config.root = Some(table_root.display().to_string());
        let operator =
            Operator::new(opendal::Configurator::into_builder(config)).expect("fs operator builds");
        MetadataFileCatalog::for_table_root(
            &format!("file://{}", table_root.display()),
            operator,
            served_ident(),
            local_file_io(),
        )
    }

    async fn catalog_at(location: &str) -> MetadataFileCatalog {
        MetadataFileCatalog::try_new(location, served_ident(), local_file_io())
            .await
            .expect("pinned metadata file loads")
    }

    async fn catalog_for(metadata_file: &Path) -> MetadataFileCatalog {
        catalog_at(&format!("file://{}", metadata_file.display())).await
    }

    #[tokio::test]
    async fn each_pinned_metadata_file_reads_exactly_its_own_snapshot() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &[&[1], &[2, 3], &[4]]).await;
        let files = metadata_files(&table_root);
        assert_eq!(
            files.len(),
            4,
            "table creation plus three appends write four metadata files: {files:?}"
        );

        let expected: [&[i64]; 4] = [&[], &[1], &[1, 2, 3], &[1, 2, 3, 4]];
        for (file, rows) in files.iter().zip(expected) {
            assert_eq!(
                ids_through(catalog_for(file).await).await,
                rows,
                "pinned to {}, a read must see that commit's rows and no later ones",
                file.display()
            );
        }
    }

    /// The differential check: a seeded history of appends and deletes, where after
    /// every commit the rows read through the pinned metadata file must equal both
    /// the rows the catalog serves for the same commit and an in-memory model of
    /// the history. Deletes go through Spice's Iceberg delete path, so pinned reads
    /// must apply equality-delete files written in later commits only to the rows
    /// they were committed against. Once the history is done, every earlier pin is
    /// read again and must still return its own commit's rows.
    #[tokio::test]
    async fn pinned_reads_match_the_catalog_and_a_model_across_seeded_appends_and_deletes() {
        for seed in [7_u64, 1_234, 98_765] {
            let mut rng = StdRng::seed_from_u64(seed);
            let warehouse = tempfile::tempdir().expect("temp warehouse is created");
            let writer = Writer::create(warehouse.path()).await;
            // Built once and scanned after every commit, as one dataset serves every
            // query: each scan must find the commit that just landed.
            let unpinned = Arc::new(provider_over(table_root_catalog(&writer.root)).await);
            let mut model: Vec<i64> = Vec::new();
            let mut history: Vec<(String, Vec<i64>)> = Vec::new();
            let mut deletes = 0;

            for step in 0..12 {
                let op = if model.is_empty() || rng.random_bool(0.6) {
                    let rows: Vec<i64> = (0..rng.random_range(1..=5))
                        .map(|_| rng.random_range(0..20))
                        .collect();
                    writer.append(&rows).await;
                    model.extend(&rows);
                    format!("append {rows:?}")
                } else if rng.random_bool(0.5) {
                    let target = model[rng.random_range(0..model.len())];
                    writer.delete_where(col("id").eq(lit(target))).await;
                    model.retain(|id| *id != target);
                    deletes += 1;
                    format!("delete id = {target}")
                } else {
                    let bound = rng.random_range(0..20);
                    writer.delete_where(col("id").lt(lit(bound))).await;
                    model.retain(|id| *id >= bound);
                    deletes += 1;
                    format!("delete id < {bound}")
                };
                model.sort_unstable();

                let location = writer.metadata_location().await;
                let through_catalog = writer.ids_through_catalog().await;
                let pinned = ids_through(catalog_at(&location).await).await;
                assert_eq!(
                    through_catalog, model,
                    "seed {seed} step {step} ({op}): the catalog read disagrees with the model"
                );
                assert_eq!(
                    pinned, model,
                    "seed {seed} step {step} ({op}): the read pinned to {location} disagrees with the model"
                );
                assert_eq!(
                    ids_of(&unpinned).await,
                    model,
                    "seed {seed} step {step} ({op}): the read of the table root disagrees with the model"
                );
                history.push((location, model.clone()));
            }

            assert!(
                deletes > 0,
                "seed {seed} produced no delete, so it checked no delete file"
            );
            for (step, (location, expected)) in history.iter().enumerate() {
                assert_eq!(
                    &ids_through(catalog_at(location).await).await,
                    expected,
                    "seed {seed}: the pin taken at step {step} ({location}) changed after later commits"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_table_with_no_data_files_reads_as_empty() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &[]).await;
        assert!(
            !table_root.join("data").exists(),
            "the fixture must have no data directory, or this checks nothing"
        );
        let files = metadata_files(&table_root);
        let file = files.first().expect("table creation wrote a metadata file");

        assert_eq!(
            ids_through(catalog_for(file).await).await,
            Vec::<i64>::new()
        );
    }

    #[tokio::test]
    async fn gzip_compressed_metadata_reads_the_same_rows() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &[&[7], &[8]]).await;
        let latest = metadata_files(&table_root)
            .pop()
            .expect("the table has metadata files");

        // Java Iceberg names compressed metadata `<N>-<uuid>.gz.metadata.json`.
        let plain = std::fs::read(&latest).expect("metadata file is read");
        let stem = latest
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".metadata.json"))
            .expect("metadata file name has the standard suffix");
        let compressed = latest.with_file_name(format!("{stem}.gz.metadata.json"));
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).expect("metadata is compressed");
        std::fs::write(&compressed, encoder.finish().expect("gzip stream finishes"))
            .expect("compressed metadata is written");
        std::fs::remove_file(&latest).expect("plain metadata is removed");

        assert_eq!(
            ids_through(catalog_for(&compressed).await).await,
            vec![7, 8]
        );
    }

    #[tokio::test]
    async fn the_catalog_serves_only_its_table_and_refuses_writes() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &[&[1]]).await;
        let file = metadata_files(&table_root)
            .pop()
            .expect("the table has metadata files");
        let catalog = catalog_for(&file).await;

        let other = TableIdent::new(
            NamespaceIdent::new("spice".to_string()),
            "other".to_string(),
        );
        let err = catalog
            .load_table(&other)
            .await
            .expect_err("only the pinned table is served");
        assert_eq!(err.kind(), ErrorKind::TableNotFound);

        let err = catalog
            .drop_table(&served_ident())
            .await
            .expect_err("a pinned table is read-only");
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
        assert_eq!(
            err.message(),
            "Iceberg table 'spice.pinned' is read directly from its metadata file without a catalog, so it is read-only. Writes require a catalog."
        );
    }

    /// Renames the table's metadata files to the Hadoop convention,
    /// `v<N>.metadata.json`, and returns the new paths in version order.
    fn rename_to_hadoop_names(table_root: &Path) -> Vec<PathBuf> {
        metadata_files(table_root)
            .into_iter()
            .enumerate()
            .map(|(version, file)| {
                let renamed = file.with_file_name(format!("v{version}.metadata.json"));
                std::fs::rename(&file, &renamed).expect("metadata file is renamed");
                renamed
            })
            .collect()
    }

    fn set_modified(file: &Path, time: std::time::SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(file)
            .expect("metadata file opens")
            .set_modified(time)
            .expect("modified time is set");
    }

    fn write_hint(table_root: &Path, content: &str) {
        std::fs::write(table_root.join("metadata").join(VERSION_HINT_FILE), content)
            .expect("version hint is written");
    }

    /// Eleven commits, so versions run past 9: `v10` and `v11` sort below `v9` as
    /// text, and `00010-…` sorts correctly only because of its padding.
    const APPENDS: [&[i64]; 11] = [
        &[0],
        &[1],
        &[2],
        &[3],
        &[4],
        &[5],
        &[6],
        &[7],
        &[8],
        &[9],
        &[10],
    ];
    const ALL_IDS: [i64; 11] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    #[test]
    fn metadata_versions_are_parsed_as_numbers_from_both_naming_conventions() {
        for (name, version) in [
            ("v1.metadata.json", Some(1)),
            ("v10.metadata.json", Some(10)),
            ("00010-8f2c-4d1e.metadata.json", Some(10)),
            ("00003-8f2c.gz.metadata.json", Some(3)),
            ("v3.metadata.json.gz", Some(3)),
            ("v3.gz.metadata.json", Some(3)),
            ("version-hint.text", None),
            ("snap-123.avro", None),
            ("v.metadata.json", None),
            ("3a.metadata.json", None),
            ("metadata.json", None),
        ] {
            assert_eq!(metadata_version(name), version, "{name}");
        }
        assert_eq!(
            newest_metadata_names(["v9.metadata.json", "v10.metadata.json", "v2.metadata.json"]),
            Some(vec!["v10.metadata.json"])
        );
        assert_eq!(newest_metadata_names(["version-hint.text"]), None);
    }

    /// Regression test for #15062: the current metadata file is the one with the
    /// highest version number, whatever the files' modified times say.
    #[tokio::test]
    async fn file_times_and_name_order_do_not_decide_the_current_metadata_file() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &APPENDS).await;
        let catalog = table_root_catalog(&table_root);

        // Newest commit oldest on disk, as after a copy that reverses the order.
        let start = std::time::SystemTime::UNIX_EPOCH;
        for (age, file) in metadata_files(&table_root).iter().rev().enumerate() {
            set_modified(file, start + std::time::Duration::from_secs(age as u64));
        }
        assert_eq!(
            ids_through(catalog.clone()).await,
            ALL_IDS,
            "padded <N>-<uuid> names"
        );

        let files = rename_to_hadoop_names(&table_root);
        for file in &files {
            set_modified(file, start);
        }
        assert_eq!(
            ids_through(catalog.clone()).await,
            ALL_IDS,
            "v<N> names, equal modified times"
        );
        assert_eq!(
            catalog
                .current_metadata()
                .await
                .expect("current metadata is found"),
            CurrentMetadata {
                location: format!("file://{}", files[11].display()),
                listed: true,
            }
        );
    }

    /// Regression test for #15062 through the Hadoop catalog the Iceberg catalog
    /// connector uses, which shares this resolution.
    #[tokio::test]
    async fn the_hadoop_catalog_reads_the_highest_version_whatever_the_file_times() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &APPENDS).await;
        let start = std::time::SystemTime::UNIX_EPOCH;
        for (age, file) in metadata_files(&table_root).iter().rev().enumerate() {
            set_modified(file, start + std::time::Duration::from_secs(age as u64));
        }

        let mut config = opendal::services::FsConfig::default();
        config.root = Some(warehouse.path().display().to_string());
        let catalog = crate::iceberg::catalog::hadoop::HadoopCatalogBuilder::default()
            .with_warehouse_root(format!("file://{}", warehouse.path().display()))
            .with_file_io(local_file_io())
            .with_operator(
                Operator::new(opendal::Configurator::into_builder(config))
                    .expect("fs operator builds"),
            )
            .build()
            .await
            .expect("hadoop catalog builds");
        let provider = IcebergTableProvider::try_new(
            Arc::new(catalog),
            NamespaceIdent::new(NAMESPACE.to_string()),
            TABLE.to_string(),
        )
        .await
        .expect("hadoop provider is constructed");
        assert_eq!(ids_of(&Arc::new(provider)).await, ALL_IDS);
    }

    /// A version hint decides the current metadata file even when newer files
    /// exist, and is read again on every load, through both the table root and a
    /// pin on the hint file itself. The hint may hold a version (Java's Hadoop
    /// tables) or a file name stem (`DuckDB`).
    #[tokio::test]
    async fn a_version_hint_names_the_current_metadata_file_and_is_reread_on_every_load() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &APPENDS).await;
        let stems: Vec<String> = metadata_files(&table_root)
            .iter()
            .map(|file| {
                file.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_suffix(".metadata.json"))
                    .expect("metadata file name has the standard suffix")
                    .to_string()
            })
            .collect();
        let hint = format!(
            "file://{}/metadata/{VERSION_HINT_FILE}",
            table_root.display()
        );
        let root = Arc::new(provider_over(table_root_catalog(&table_root)).await);
        write_hint(&table_root, &stems[2]);
        let pinned = Arc::new(provider_over(catalog_at(&hint).await).await);

        for provider in [&root, &pinned] {
            write_hint(&table_root, &stems[2]);
            assert_eq!(ids_of(provider).await, [0, 1], "hint names {}", stems[2]);
            write_hint(&table_root, &format!("{}\n", stems[11]));
            assert_eq!(ids_of(provider).await, ALL_IDS, "hint names {}", stems[11]);
        }

        rename_to_hadoop_names(&table_root);
        for provider in [&root, &pinned] {
            write_hint(&table_root, "3");
            assert_eq!(ids_of(provider).await, [0, 1, 2], "hint names v3");
            write_hint(&table_root, "10");
            assert_eq!(
                ids_of(provider).await,
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                "hint names v10"
            );
        }

        write_hint(&table_root, "12");
        let err = catalog_at(&hint)
            .await
            .current_metadata()
            .await
            .expect_err("a hint naming a missing version is an error");
        let directory = format!("file://{}/metadata", table_root.display());
        assert_eq!(
            err.message(),
            format!(
                "The version hint '{hint}' names version '12', but none of v12.metadata.json, v12.gz.metadata.json, 12.metadata.json, 12.gz.metadata.json exists in '{directory}/'"
            )
        );
    }

    /// Two files with the highest version, as a failed commit can leave behind, are
    /// told apart by `last-updated-ms` and then by name, never by which one is
    /// listed first.
    #[tokio::test]
    async fn a_duplicate_highest_version_reads_the_most_recently_updated_file() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let table_root = write_table(warehouse.path(), &[&[1], &[2]]).await;
        let files = metadata_files(&table_root);
        // Version 2 again, holding the older commit 1, named to list on either side.
        for name in ["00002-0000.metadata.json", "00002-ffff.metadata.json"] {
            std::fs::copy(&files[1], table_root.join("metadata").join(name))
                .expect("stale duplicate is written");
        }

        let catalog = table_root_catalog(&table_root);
        assert_eq!(ids_through(catalog.clone()).await, [1, 2]);
        assert_eq!(
            catalog
                .current_metadata()
                .await
                .expect("current metadata is found")
                .location,
            format!("file://{}", files[2].display())
        );

        // Equal `last-updated-ms` too: the greater file name is read, whatever the
        // listing order.
        for name in ["00003-ffff.metadata.json", "00003-0000.metadata.json"] {
            std::fs::copy(&files[2], table_root.join("metadata").join(name))
                .expect("identical duplicate is written");
        }
        assert_eq!(
            catalog
                .current_metadata()
                .await
                .expect("current metadata is found")
                .location,
            format!(
                "file://{}/metadata/00003-ffff.metadata.json",
                table_root.display()
            )
        );
    }

    #[tokio::test]
    async fn a_table_root_with_no_metadata_files_is_an_error() {
        let warehouse = tempfile::tempdir().expect("temp warehouse is created");
        let err = table_root_catalog(warehouse.path())
            .current_metadata()
            .await
            .expect_err("a directory with no metadata is not a table");
        assert_eq!(
            err.message(),
            format!(
                "No Iceberg metadata file was found in 'file://{}/metadata/'. The table root must contain a 'metadata/' directory with files named 'v<N>.metadata.json' or '<N>-<uuid>.metadata.json'",
                warehouse.path().display()
            )
        );
    }
}
