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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, Runtime, TableCommit,
    TableCreation, TableIdent,
};
use opendal::{Entry, Operator};

use crate::iceberg::catalog::metadata_file::{MetadataListing, find_current_metadata};

/// A function that builds a `StorageFactory` for a given URL scheme (e.g. `s3`, `s3a`).
///
/// This is used by the `HadoopCatalogBuilder` to allow the storage factory to be
/// reconstructed when scheme inference detects that the warehouse root scheme does
/// not match the scheme used in table metadata locations.
pub type StorageFactoryBuilderFn = Arc<dyn Fn(&str) -> Arc<dyn StorageFactory> + Send + Sync>;

/// Builder for creating a new `HadoopCatalog`
#[derive(Default, Clone)]
pub struct HadoopCatalogBuilder {
    warehouse_root: Option<String>,
    file_io: Option<FileIO>,
    properties: HashMap<String, String>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    storage_factory_builder: Option<StorageFactoryBuilderFn>,
    operator: Option<Operator>,
}

impl std::fmt::Debug for HadoopCatalogBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HadoopCatalogBuilder")
            .field("warehouse_root", &self.warehouse_root)
            .field("file_io", &self.file_io)
            .field("properties", &self.properties)
            .field("storage_factory", &self.storage_factory)
            .field(
                "storage_factory_builder",
                &self.storage_factory_builder.as_ref().map(|_| "<fn>"),
            )
            .field("operator", &self.operator)
            .finish()
    }
}

impl HadoopCatalogBuilder {
    /// Sets the warehouse root for the Hadoop catalog.
    /// The warehouse root should be the absolute path to the warehouse directory, including the scheme prefix for the `FileIO`.
    #[must_use]
    pub fn with_warehouse_root(mut self, warehouse_root: impl Into<String>) -> Self {
        self.warehouse_root = Some(warehouse_root.into());
        self
    }

    /// Sets the `FileIO` instance for the Hadoop catalog.
    #[must_use]
    pub fn with_file_io(mut self, file_io: FileIO) -> Self {
        self.file_io = Some(file_io);
        self
    }

    /// Sets the `StorageFactory` for the Hadoop catalog.
    #[must_use]
    pub fn with_storage_factory(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
        self
    }

    /// Sets a builder function that produces a `StorageFactory` for a given URL scheme.
    ///
    /// When set, this function is invoked during scheme inference: if the inferred
    /// scheme differs from the warehouse root scheme, the storage factory is rebuilt
    /// using the new scheme so that the rebuilt `FileIO` accepts paths with that
    /// scheme.
    ///
    /// If both `with_storage_factory` and `with_storage_factory_builder` are set,
    /// the builder function takes precedence and is invoked with the warehouse root
    /// scheme during the initial build.
    #[must_use]
    pub fn with_storage_factory_builder<F>(mut self, builder: F) -> Self
    where
        F: Fn(&str) -> Arc<dyn StorageFactory> + Send + Sync + 'static,
    {
        self.storage_factory_builder = Some(Arc::new(builder));
        self
    }

    /// Sets the opendal `Operator` for directory listing operations.
    #[must_use]
    pub fn with_operator(mut self, operator: Operator) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Sets a property for the `FileIO` connection.
    #[must_use]
    pub fn set_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets properties for the `FileIO` connection.
    #[must_use]
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties.extend(properties);
        self
    }

    fn inner_build(mut self, infer_scheme: bool) -> BoxFuture<'static, Result<HadoopCatalog>> {
        async move {
            let mut cloned_self = self.clone();
            let mut warehouse_root = self.warehouse_root.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "Warehouse root must be specified")
            })?;

            if !warehouse_root.ends_with('/') {
                warehouse_root.push('/');
            }

            // If a storage factory builder was provided, materialize the factory
            // for the current warehouse root scheme. This lets scheme inference
            // rebuild the factory when re-entering inner_build with a new scheme.
            if let Some(factory_builder) = &self.storage_factory_builder {
                if let Some((scheme, _)) = warehouse_root.split_once("://") {
                    self.storage_factory = Some(factory_builder(scheme));
                } else if self.file_io.is_none() && self.storage_factory.is_none() {
                    // The builder needs a scheme to materialize a factory, and we
                    // have no other source for the FileIO.
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot materialize storage factory: warehouse root '{warehouse_root}' does not contain a URL scheme. Verify the warehouse root is in the format '<scheme>://<path>'.",
                        ),
                    ));
                }
            }

            let file_io = if let Some(file_io) = self.file_io {
                file_io
            } else if let Some(factory) = &self.storage_factory {
                FileIOBuilder::new(Arc::clone(factory))
                    .with_props(self.properties.iter().map(|(k, v)| (k.as_str(), v.as_str())))
                    .build()
            } else {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Either file_io, storage_factory, or storage_factory_builder must be provided",
                ));
            };

            let operator = self.operator.ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "An opendal Operator must be provided via with_operator()",
                )
            })?;

            // Verify the warehouse root exists using the file_io (which handles full paths)
            let root_input = file_io.new_input(&warehouse_root).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid warehouse root: {e}"),
                )
            })?;
            if !root_input.exists().await? {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Warehouse root '{warehouse_root}' does not exist"),
                ));
            }

            let cloned_warehouse_root = warehouse_root.clone();
            let catalog = HadoopCatalog {
                file_io,
                operator,
                warehouse_root,
            };

            if infer_scheme {
                // infer if the warehouse scheme matches the scheme specified from table metadata locations
                let cloned_catalog = catalog.clone();
                let namespaces = cloned_catalog.list_namespaces(None).await?;
                let tables = futures::stream::iter(namespaces)
                    .then(|namespace| {
                        let catalog = cloned_catalog.clone();
                        async move { catalog.list_tables(&namespace).await }
                    })
                    .try_collect::<Vec<Vec<_>>>()
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                let mut inferred_scheme = None;
                for table in tables {
                    let metadata = catalog.load_metadata(&table).await;
                    // lazy scheme inferring - only check until we get the first valid metadata
                    if let Ok(m) = metadata && let Some((scheme, _)) = m.location().split_once("://") && !cloned_warehouse_root.starts_with(scheme) {
                                inferred_scheme = Some(scheme.to_string());
                                break;
                            }
                }

                if let Some(scheme) = inferred_scheme {
                    tracing::debug!(
                        "Inferred scheme '{scheme}' for warehouse root '{cloned_warehouse_root}'",
                    );

                    if let Some((actual_scheme, _)) = cloned_warehouse_root.split_once("://") {
                        cloned_self.warehouse_root =
                            Some(cloned_warehouse_root.replace(actual_scheme, &scheme));
                        return cloned_self.inner_build(false).await;
                    }
                    // if the existing root doesn't contain a scheme, it's in an unknown format that we cannot fix
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Warehouse root '{cloned_warehouse_root}' does not start with the inferred scheme '{scheme}'. Verify the warehouse root is in the format of '<scheme>://<path>'.",
                        ),
                    ));
                }
            }

            Ok(catalog)
        }.boxed()
    }

    /// Builds the `HadoopCatalog` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the warehouse root is not specified, if the `FileIO` is not specified,
    /// if the warehouse root is not a directory, or if the warehouse root does not start with the `FileIO` scheme prefix.
    pub async fn build(self) -> Result<HadoopCatalog> {
        self.inner_build(true).await
    }
}

/// Represents a hadoop catalog backed by storage from a `FileIO`
#[derive(Debug, Clone)]
pub struct HadoopCatalog {
    file_io: FileIO,
    operator: Operator,
    warehouse_root: String,
}

#[async_trait]
impl Catalog for HadoopCatalog {
    /// Register an existing table to the catalog.
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Registering tables is not supported in hadoop catalog",
        ))
    }

    // Unsupported operations in Hadoop Catalog
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating namespaces is not supported in hadoop catalog",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating namespaces is not supported in hadoop catalog",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping namespaces is not supported in hadoop catalog",
        ))
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Creating tables is not supported in hadoop catalog",
        ))
    }

    async fn drop_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Dropping tables is not supported in hadoop catalog",
        ))
    }

    async fn purge_table(&self, _table: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Purging tables is not supported in hadoop catalog",
        ))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Renaming tables is not supported in hadoop catalog",
        ))
    }

    async fn update_table(&self, _commit: TableCommit) -> Result<Table> {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Updating tables is not supported in hadoop catalog",
        ))
    }

    // Supported operations in Hadoop Catalog
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let path = if let Some(namespace) = parent {
            format!(
                "{warehouse_root}{namespace}/",
                warehouse_root = self.warehouse_root,
                namespace = namespace.join("/")
            )
        } else {
            self.warehouse_root.clone()
        };

        let mut namespaces = Vec::new();
        let directories = self.get_directories(&path).await?;

        for entry in directories {
            let path = format!("{path}{entry}/", path = path, entry = entry.name());
            if self.directory_has_metadata_and_data(&path).await? {
                // This is a table, skip it
                continue;
            }

            let namespace_name = entry
                .name()
                .strip_suffix("/")
                .unwrap_or(entry.name())
                .to_string();

            let namespace = if let Some(parent) = parent.cloned() {
                let mut namespace = parent.inner();
                namespace.push(namespace_name);
                NamespaceIdent::from_vec(namespace)?
            } else {
                NamespaceIdent::from_vec(vec![namespace_name])?
            };

            namespaces.push(namespace);
        }

        Ok(namespaces)
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let path = format!(
            "{warehouse_root}{namespace}/",
            warehouse_root = self.warehouse_root,
            namespace = namespace.join("/")
        );

        self.directory_exists(&path).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        Ok(Namespace::new(namespace.clone()))
    }

    async fn load_table(&self, table_identifier: &TableIdent) -> Result<Table> {
        if !self.table_exists(table_identifier).await? {
            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("Table {table_identifier} does not exist"),
            ));
        }

        let location = self.current_metadata_location(table_identifier).await?;
        let table_metadata = TableMetadata::read_from(&self.file_io, &location).await?;

        // A table without its metadata location cannot be served to another
        // Iceberg client, which reads the table from that file.
        Table::builder()
            .metadata(table_metadata)
            .metadata_location(location)
            .identifier(table_identifier.clone())
            .file_io(self.file_io.clone())
            .runtime(Runtime::current())
            .readonly(true)
            .build()
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        let path = format!(
            "{warehouse_root}{namespace}/{table}/",
            warehouse_root = self.warehouse_root,
            namespace = table.namespace.join("/"),
            table = table.name
        );

        if !self.directory_exists(&path).await? {
            return Ok(false);
        }

        // Check if the table has metadata
        self.directory_has_metadata_and_data(&path).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        // List the tables in the specified namespace
        let path = format!(
            "{warehouse_root}{namespace}/",
            warehouse_root = self.warehouse_root,
            namespace = namespace.join("/")
        );
        let mut tables = Vec::new();

        let directories = self.get_directories(&path).await?;
        for entry in directories {
            let table_name = entry
                .name()
                .strip_suffix("/")
                .unwrap_or(entry.name())
                .to_string();

            let table_ident = TableIdent {
                namespace: namespace.clone(),
                name: table_name,
            };

            if self
                .directory_has_metadata_and_data(&format!(
                    "{path}/{table_name}",
                    table_name = table_ident.name
                ))
                .await?
            {
                tables.push(table_ident);
            }
        }

        Ok(tables)
    }
}

impl HadoopCatalog {
    /// Converts a full URL path (e.g., `s3://bucket/prefix/namespace/`) to a path
    /// relative to the opendal operator root. The operator root is configured to match
    /// the warehouse root path, so stripping the `warehouse_root` prefix yields the
    /// correct operator-relative path.
    fn to_operator_path(&self, full_path: &str) -> String {
        match full_path.strip_prefix(&self.warehouse_root) {
            Some("") => "/".to_string(),
            Some(relative) => relative.to_string(),
            None => full_path.to_string(),
        }
    }

    async fn get_directories(&self, root: &str) -> Result<Vec<Entry>> {
        let op_path = self.to_operator_path(root);
        let mut directories = Vec::new();
        let mut lister = self.operator.lister(&op_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to list directory: {e}"),
            )
        })?;

        while let Some(entry) = lister.try_next().await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to read directory entry: {e}"),
            )
        })? {
            if entry.metadata().is_dir() {
                directories.push(entry);
            }
        }

        Ok(directories)
    }

    async fn directory_has_metadata_and_data(&self, path: &str) -> Result<bool> {
        let data_dir = format!("{path}/data/");
        let op_path = self.to_operator_path(&data_dir);
        let is_data_dir = match self.operator.stat(&op_path).await {
            Ok(m) => m.is_dir(),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to stat: {e}"),
                ));
            }
        };
        if !is_data_dir {
            return Ok(false);
        }
        self.directory_has_metadata(path).await
    }

    async fn directory_exists(&self, path: &str) -> Result<bool> {
        let op_path = self.to_operator_path(path);
        match self.operator.stat(&op_path).await {
            Ok(m) => Ok(m.is_dir()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Failed to stat: {e}"),
            )),
        }
    }

    async fn directory_has_metadata(&self, path: &str) -> Result<bool> {
        let metadata_directory = format!("{path}/metadata/");
        let op_path = self.to_operator_path(&metadata_directory);

        // Check if the metadata directory exists
        match self.operator.stat(&op_path).await {
            Ok(m) if m.is_dir() => {}
            Ok(_) | Err(_) => return Ok(false),
        }

        let mut lister = self.operator.lister(&op_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to list directory: {e}"),
            )
        })?;

        while let Some(entry) = lister.try_next().await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to read metadata entry: {e}"),
            )
        })? {
            if entry.metadata().is_file() && entry.name().ends_with(".metadata.json") {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// The table's current metadata file: the one its `version-hint.text` names, or
    /// else the one with the highest version number in its `metadata/` directory.
    async fn current_metadata_location(&self, table: &TableIdent) -> Result<String> {
        let metadata_directory = format!(
            "{warehouse_root}{namespace}/{table}/metadata/",
            warehouse_root = self.warehouse_root,
            namespace = table.namespace.join("/"),
            table = table.name
        );
        let listing = MetadataListing {
            operator: self.operator.clone(),
            path: self.to_operator_path(&metadata_directory),
        };
        let hint = format!("{metadata_directory}version-hint.text");
        Ok(find_current_metadata(&self.file_io, &hint, Some(&listing))
            .await?
            .location)
    }

    async fn load_metadata(&self, table_identifier: &TableIdent) -> Result<TableMetadata> {
        let location = self.current_metadata_location(table_identifier).await?;
        TableMetadata::read_from(&self.file_io, &location).await
    }
}
