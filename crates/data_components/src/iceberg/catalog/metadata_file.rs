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

//! A read-only Iceberg catalog that holds exactly one table, loaded from a single
//! `metadata.json` file rather than from a catalog service.
//!
//! The catalog's only role on the read path is to name a table's current metadata
//! file; given that file, the metadata → manifest list → manifest → data file chain
//! needs no catalog. Presenting the table through the [`Catalog`] trait keeps it an
//! ordinary `IcebergTableProvider`, which the distributed scan codec, the Iceberg
//! passthrough API and the availability monitor all recognise by that type.

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::{StaticTable, Table};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

/// A catalog serving one table read from a pinned metadata file.
///
/// A metadata file is immutable once written, so the table is loaded once and every
/// [`Catalog::load_table`] returns it; scans therefore never re-read the metadata.
/// Every mutating operation fails with [`ErrorKind::FeatureUnsupported`].
#[derive(Debug, Clone)]
pub struct MetadataFileCatalog {
    table: Table,
}

impl MetadataFileCatalog {
    /// Loads the table described by the metadata file at `metadata_location` (a full
    /// URL the `file_io` can read), registering it under `ident`. Gzip-compressed
    /// metadata is decompressed transparently.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or is not valid Iceberg table
    /// metadata.
    pub async fn try_new(
        metadata_location: &str,
        ident: TableIdent,
        file_io: FileIO,
    ) -> Result<Self> {
        let table = StaticTable::from_metadata_file(metadata_location, ident, file_io)
            .await?
            .into_table();
        Ok(Self { table })
    }

    /// The identifier the table is served under.
    #[must_use]
    pub fn table_ident(&self) -> &TableIdent {
        self.table.identifier()
    }

    fn is_table(&self, ident: &TableIdent) -> bool {
        ident == self.table.identifier()
    }

    fn is_namespace(&self, namespace: &NamespaceIdent) -> bool {
        namespace == self.table.identifier().namespace()
    }

    fn read_only(&self) -> Error {
        Error::new(
            ErrorKind::FeatureUnsupported,
            format!(
                "Iceberg table '{}' is read directly from its metadata file without a catalog, so it is read-only. Writes require a catalog.",
                self.table.identifier()
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

#[async_trait]
impl Catalog for MetadataFileCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        Ok(match parent {
            None => vec![self.table.identifier().namespace().clone()],
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
            vec![self.table.identifier().clone()]
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
        if self.is_table(table) {
            Ok(self.table.clone())
        } else {
            Err(Self::table_not_found(table))
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
            ids_of(self.provider().await).await
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
    async fn ids_of(provider: IcebergTableProvider) -> Vec<i64> {
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider))
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
        let ident = catalog.table_ident().clone();
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        let provider = IcebergTableProvider::try_new(
            catalog,
            ident.namespace().clone(),
            ident.name().to_string(),
        )
        .await
        .expect("reader provider is constructed");
        ids_of(provider).await
    }

    async fn catalog_at(location: &str) -> MetadataFileCatalog {
        MetadataFileCatalog::try_new(
            location,
            served_ident(),
            FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build(),
        )
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
}
