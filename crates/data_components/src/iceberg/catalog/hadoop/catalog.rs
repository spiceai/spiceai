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
use iceberg::io::{FileIO, FileIOBuilder, InputFile, StorageFactory};
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use opendal::{Entry, Operator};

/// Specifies the mode for identifying metadata files in a Hadoop catalog
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum MetadataMode {
    /// Infer the latest metadata file from the Hadoop structure
    #[default]
    Infer,
    /// Use the exact metadata file specified, or infer it if the file does not exist
    ExactOrInfer(String),
    /// Use the exact metadata file specified
    Exact(String),
}

/// Builder for creating a new `HadoopCatalog`
#[derive(Debug, Default, Clone)]
pub struct HadoopCatalogBuilder {
    warehouse_root: Option<String>,
    file_io: Option<FileIO>,
    metadata_mode: MetadataMode,
    properties: HashMap<String, String>,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    operator: Option<Operator>,
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

    /// Sets the opendal `Operator` for directory listing operations.
    #[must_use]
    pub fn with_operator(mut self, operator: Operator) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Sets the metadata mode for the Hadoop catalog.
    #[must_use]
    pub fn with_metadata_mode(mut self, metadata_mode: MetadataMode) -> Self {
        self.metadata_mode = metadata_mode;
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

    fn inner_build(self, infer_scheme: bool) -> BoxFuture<'static, Result<HadoopCatalog>> {
        async move {
            let mut cloned_self = self.clone();
            let mut warehouse_root = self.warehouse_root.ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "Warehouse root must be specified")
            })?;

            if !warehouse_root.ends_with('/') {
                warehouse_root.push('/');
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
                    "Either file_io or storage_factory must be provided",
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
                warehouse_root,
                file_io,
                operator,
                metadata_mode: self.metadata_mode,
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
    metadata_mode: MetadataMode,
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
            if self
                .directory_has_metadata_and_data(&path, self.metadata_mode.clone())
                .await?
            {
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
            if let MetadataMode::Exact(ref metadata_file) = self.metadata_mode {
                let input_file = self.file_io.new_input(metadata_file)?;
                if !input_file.exists().await? {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Expected metadata file '{metadata_file}' does not exist"),
                    ));
                }
            }

            return Err(Error::new(
                ErrorKind::TableNotFound,
                format!("Table {table_identifier} does not exist"),
            ));
        }

        let metadata_file_path = match self.metadata_mode {
            MetadataMode::Infer => None,
            MetadataMode::ExactOrInfer(ref metadata_file) => {
                let input_file = self.file_io.new_input(metadata_file)?;
                if input_file.exists().await? {
                    Some(metadata_file.clone())
                } else {
                    // If the exact metadata file does not exist, infer the latest metadata file
                    None
                }
            }
            MetadataMode::Exact(ref metadata_file) => Some(metadata_file.clone()),
        };

        let metadata_file = self
            .find_metadata_file(table_identifier, metadata_file_path)
            .await?;

        let metadata_file_content = metadata_file.read().await?;
        let table_metadata = serde_json::from_slice::<TableMetadata>(&metadata_file_content)?;

        Table::builder()
            .metadata(table_metadata)
            .identifier(table_identifier.clone())
            .file_io(self.file_io.clone())
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
        self.directory_has_metadata_and_data(&path, MetadataMode::Infer)
            .await
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
                .directory_has_metadata_and_data(
                    &format!("{path}/{table_name}", table_name = table_ident.name),
                    self.metadata_mode.clone(),
                )
                .await?
            {
                tables.push(table_ident);
            }
        }

        Ok(tables)
    }
}

impl HadoopCatalog {
    async fn get_directories(&self, root: &str) -> Result<Vec<Entry>> {
        let mut directories = Vec::new();
        let entries = self
            .operator
            .list(root)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to list directory: {e}")))?;

        for entry in entries {
            if entry.metadata().is_dir() && !root.ends_with(entry.path()) {
                directories.push(entry);
            }
        }

        Ok(directories)
    }

    async fn directory_has_metadata_and_data(
        &self,
        path: &str,
        metadata_mode: MetadataMode,
    ) -> Result<bool> {
        let data_dir = format!("{path}/data/");
        let exists = self
            .operator
            .exists(&data_dir)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to check existence: {e}")))?;
        if !exists {
            return Ok(false);
        }
        let stat = self
            .operator
            .stat(&data_dir)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to stat: {e}")))?;
        Ok(stat.is_dir() && self.directory_has_metadata(path, metadata_mode).await?)
    }

    async fn directory_exists(&self, path: &str) -> Result<bool> {
        let exists = self
            .operator
            .exists(path)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to check existence: {e}")))?;
        if !exists {
            return Ok(false);
        }

        let stat = self
            .operator
            .stat(path)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to stat: {e}")))?;
        Ok(stat.is_dir())
    }

    async fn directory_has_metadata(
        &self,
        path: &str,
        metadata_mode: MetadataMode,
    ) -> Result<bool> {
        let metadata_directory = format!("{path}/metadata/");
        let exists = self
            .operator
            .exists(&metadata_directory)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to check existence: {e}")))?;
        if !exists {
            return Ok(false);
        }

        let entries = self
            .operator
            .list(&metadata_directory)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to list directory: {e}")))?;
        for entry in entries {
            if entry.metadata().is_file() {
                let (metadata_file, fail_if_exact_missing) = match metadata_mode {
                    MetadataMode::Infer => (None, false),
                    MetadataMode::ExactOrInfer(ref metadata_file) => (Some(metadata_file), false),
                    MetadataMode::Exact(ref metadata_file) => (Some(metadata_file), true),
                };

                if let Some(metadata_file) = metadata_file {
                    if entry.name() == metadata_file {
                        return Ok(true);
                    } else if fail_if_exact_missing {
                        return Ok(false);
                    }
                }

                // Naive check if the file is a metadata file
                if entry.name().ends_with(".metadata.json") {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn version_hint_path(&self, table: &TableIdent, extension: &str) -> String {
        format!(
            "{warehouse_root}{namespace}/{table}/metadata/version-hint.{extension}",
            warehouse_root = self.warehouse_root,
            namespace = table.namespace.join("/"),
            table = table.name,
            extension = extension
        )
    }

    async fn find_metadata_file(
        &self,
        table_identifier: &TableIdent,
        metadata_file_path: Option<String>,
    ) -> Result<InputFile> {
        if let Some(metadata_file) = metadata_file_path {
            self.file_io.new_input(&metadata_file)
        } else {
            let hint_one = self
                .file_io
                .new_input(self.version_hint_path(table_identifier, "txt"))?;
            let hint_two = self
                .file_io
                .new_input(self.version_hint_path(table_identifier, "text"))?;
            let hint_input = if hint_one.exists().await? {
                Some(hint_one)
            } else if hint_two.exists().await? {
                Some(hint_two)
            } else {
                None
            };

            if let Some(input) = hint_input {
                // Load the version hint file to get the latest metadata file
                let metadata_version = input.read().await?;
                let metadata_version = std::str::from_utf8(&metadata_version).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid UTF-8 in version hint file: {e}"),
                    )
                })?;
                let metadata_file = format!(
                    "{warehouse_root}{namespace}/{table}/metadata/v{version}.metadata.json",
                    warehouse_root = self.warehouse_root,
                    namespace = table_identifier.namespace.join("/"),
                    table = table_identifier.name,
                    version = metadata_version.trim()
                );

                self.file_io.new_input(&metadata_file)
            } else {
                // If there is no version hint, list the metadata files and get the latest one
                let metadata_directory = format!(
                    "{warehouse_root}{namespace}/{table}/metadata/",
                    warehouse_root = self.warehouse_root,
                    namespace = table_identifier.namespace.join("/"),
                    table = table_identifier.name
                );

                let mut lister = self.operator
                    .lister(&metadata_directory)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to list metadata directory: {e}")))?;
                let mut latest_metadata_file: Option<Entry> = None;
                while let Some(entry) = lister.try_next().await.map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to read metadata entry: {e}")))? {
                    if entry.metadata().is_file()
                        && entry.name().ends_with(".metadata.json")
                    {
                        if let Some(latest_file) = &latest_metadata_file {
                            match (
                                latest_file.metadata().last_modified(),
                                entry.metadata().last_modified(),
                            ) {
                                (Some(latest_modified), Some(entry_modified)) => {
                                    // Compare last modified times
                                    if entry_modified > latest_modified {
                                        latest_metadata_file = Some(entry);
                                    }
                                }
                                _ => {
                                    // compare by name if last modified times are not available
                                    if entry.name() > latest_file.name() {
                                        latest_metadata_file = Some(entry);
                                    }
                                }
                            }
                        } else {
                            latest_metadata_file = Some(entry);
                        }
                    }
                }

                if let Some(latest_file) = latest_metadata_file {
                    let path = format!(
                        "{warehouse_root}{namespace}/{table}/metadata/{latest_file}",
                        warehouse_root = self.warehouse_root,
                        namespace = table_identifier.namespace.join("/"),
                        table = table_identifier.name,
                        latest_file = latest_file.name()
                    );

                    self.file_io.new_input(path)
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        "No metadata file found in the table directory",
                    ))
                }
            }
        }
    }

    async fn load_metadata(&self, table_identifier: &TableIdent) -> Result<TableMetadata> {
        let metadata_file_path = match self.metadata_mode {
            MetadataMode::Infer => None,
            MetadataMode::ExactOrInfer(ref metadata_file) => {
                let input_file = self.file_io.new_input(metadata_file)?;
                if input_file.exists().await? {
                    Some(metadata_file.clone())
                } else {
                    // If the exact metadata file does not exist, infer the latest metadata file
                    None
                }
            }
            MetadataMode::Exact(ref metadata_file) => Some(metadata_file.clone()),
        };

        let metadata_file = self
            .find_metadata_file(table_identifier, metadata_file_path)
            .await?;

        let metadata_file_content = metadata_file.read().await?;
        Ok(serde_json::from_slice::<TableMetadata>(
            &metadata_file_content,
        )?)
    }
}
