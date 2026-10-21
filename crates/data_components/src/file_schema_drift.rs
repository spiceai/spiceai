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

//! Detects file datasets whose source files no longer carry a registered column.
//!
//! A file dataset's schema is resolved once, at registration. Every later scan reads the
//! current files against that schema, and the scan fills a registered column that a file
//! lacks with `NULL`. That is correct for added-column evolution, where older files predate
//! a column that newer files carry. It is wrong when the column is gone from *every*
//! scanned file, which is what an upstream rename or drop looks like: queries return `NULL`
//! for the column and a full acceleration refresh overwrites the loaded data with `NULL`s,
//! with no error and the dataset still `Ready`.
//!
//! [`FileSchemaDriftGuard`] checks each scan's files for the projected registered columns.
//! Only formats that declare each file's full schema in metadata are checked (see
//! [`FileSchemaDriftGuard::supports_format`]).
//! A column that no scanned file carries is reported:
//!
//! - in a scan that feeds an acceleration refresh (the refresh session carries
//!   [`AccelerationRefreshScan`]), the scan fails, so the refresh fails and keeps the last
//!   loaded data;
//! - in any other scan, a `WARN` names the dataset and the columns once per change, and the
//!   query still returns `NULL` for them.

use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session},
    common::{
        Constraints, Statistics,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::{TableProvider, TableType, file_format::FileFormat},
    error::{DataFusionError, Result as DFResult},
    execution::{config::SessionConfig, object_store::ObjectStoreUrl},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_datasource::{file_scan_config::FileScanConfig, source::DataSourceExec};
use object_store::ObjectMeta;
use parking_lot::Mutex;

/// How many files' schemas are read per round while looking for a column. The newest file
/// is read alone first because it normally carries every registered column.
const SCHEMA_PROBE_CHUNK: usize = 64;

/// `SessionConfig` extension marking a session whose scans feed an acceleration refresh.
///
/// [`FileSchemaDriftGuard`] fails such a scan, rather than warning, when a registered column
/// is missing from every scanned file, so the refresh keeps the last loaded data instead of
/// replacing it with `NULL`s.
#[derive(Debug, Default, Clone, Copy)]
pub struct AccelerationRefreshScan;

/// Marks `config` as an acceleration refresh session. See [`AccelerationRefreshScan`].
pub fn mark_acceleration_refresh(config: &mut SessionConfig) {
    config.set_extension(Arc::new(AccelerationRefreshScan));
}

/// Whether `state` is an acceleration refresh session. See [`AccelerationRefreshScan`].
#[must_use]
pub fn is_acceleration_refresh(state: &dyn Session) -> bool {
    state
        .config()
        .get_extension::<AccelerationRefreshScan>()
        .is_some()
}

/// Registered columns missing from every file of one scan.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Drift {
    missing: Vec<String>,
    /// One scanned file, named in the message so the user can inspect it.
    example_file: String,
}

#[derive(Debug, Default)]
struct GuardState {
    /// Fingerprint of the last checked (columns, files) pair and its result, so repeated
    /// scans over unchanged files read no file footers.
    last_check: Option<(u64, Option<Drift>)>,
    /// The missing columns already reported by `WARN`. A column leaves the set only when a
    /// scan that reads it finds it again, so scans that do not read it neither repeat nor
    /// re-arm its warning, and a column that goes missing again is reported again.
    warned: BTreeSet<String>,
}

/// Checks a file dataset's scans for registered columns missing from every scanned file.
/// See the [module docs](self).
#[derive(Debug)]
pub struct FileSchemaDriftGuard {
    dataset_name: String,
    /// The registered columns stored in the data files, without partition or metadata
    /// columns.
    file_schema: SchemaRef,
    format: Arc<dyn FileFormat>,
    state: Mutex<GuardState>,
}

impl FileSchemaDriftGuard {
    /// Whether scans of `format` can be checked: its files declare their full schema in
    /// metadata (Parquet and ORC footers), so a column missing from a file's schema is absent
    /// from every record. Formats whose schema is inferred from a sample of records (CSV,
    /// JSON) cannot prove a column absent, so they are not checked.
    #[must_use]
    pub fn supports_format(format: &dyn FileFormat) -> bool {
        matches!(format.get_ext().as_str(), "parquet" | "orc")
    }

    #[must_use]
    pub fn new(
        dataset_name: impl Into<String>,
        file_schema: SchemaRef,
        format: Arc<dyn FileFormat>,
    ) -> Self {
        Self {
            dataset_name: dataset_name.into(),
            file_schema,
            format,
            state: Mutex::new(GuardState::default()),
        }
    }

    /// Checks the files `plan` scans for the registered file columns `projection` selects
    /// from `table_schema` (all of them when `None`).
    ///
    /// # Errors
    ///
    /// Returns a [`DataFusionError::Plan`] naming the missing columns when `state` is an
    /// acceleration refresh session ([`is_acceleration_refresh`]) and a projected registered
    /// column is missing from every scanned file. A file schema that cannot be read skips the
    /// check rather than failing the scan.
    pub async fn check_scan(
        &self,
        state: &dyn Session,
        table_schema: &SchemaRef,
        projection: Option<&[usize]>,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DFResult<()> {
        let columns = self.projected_file_columns(table_schema, projection);
        if columns.is_empty() {
            return Ok(());
        }
        let Some((store_url, mut files)) = scanned_files(plan) else {
            return Ok(());
        };
        if files.is_empty() {
            return Ok(());
        }

        let fingerprint = fingerprint(&columns, &files);
        let cached = self
            .state
            .lock()
            .last_check
            .as_ref()
            .filter(|(fp, _)| *fp == fingerprint)
            .map(|(_, drift)| drift.clone());
        let drift = if let Some(drift) = cached {
            drift
        } else {
            // Newest first: the registered schema was inferred from the newest file, so
            // it normally carries every column and ends the probe after one footer.
            files.sort_by_key(|f| std::cmp::Reverse(f.last_modified));
            let store = match state.runtime_env().object_store(&store_url) {
                Ok(store) => store,
                Err(e) => {
                    tracing::debug!(
                        "Dataset '{}': skipping the missing-column check, the object store is not available: {e}",
                        self.dataset_name
                    );
                    return Ok(());
                }
            };
            let drift = self
                .find_drift(state, &store, columns.clone(), &files)
                .await?;
            self.state.lock().last_check = Some((fingerprint, drift.clone()));
            drift
        };

        // Re-arm the warning for every column this scan read and found.
        {
            let missing = drift.as_ref().map_or(&[][..], |d| d.missing.as_slice());
            self.state
                .lock()
                .warned
                .retain(|c| !columns.contains(c) || missing.contains(c));
        }

        let Some(drift) = drift else {
            return Ok(());
        };

        if is_acceleration_refresh(state) {
            return Err(DataFusionError::Plan(refresh_error_message(
                &self.dataset_name,
                &drift,
            )));
        }

        let mut guard_state = self.state.lock();
        let unreported: Vec<String> = drift
            .missing
            .iter()
            .filter(|c| !guard_state.warned.contains(*c))
            .cloned()
            .collect();
        if !unreported.is_empty() {
            let report = Drift {
                missing: unreported,
                example_file: drift.example_file,
            };
            tracing::warn!("{}", read_warning_message(&self.dataset_name, &report));
            guard_state.warned.extend(report.missing);
        }
        Ok(())
    }

    /// The registered file columns `projection` selects from `table_schema`, in table order.
    fn projected_file_columns(
        &self,
        table_schema: &SchemaRef,
        projection: Option<&[usize]>,
    ) -> Vec<String> {
        let is_file_column = |name: &str| self.file_schema.field_with_name(name).is_ok();
        match projection {
            Some(indices) => indices
                .iter()
                .filter_map(|&i| table_schema.fields().get(i))
                .map(|f| f.name().clone())
                .filter(|name| is_file_column(name))
                .collect(),
            None => table_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|name| is_file_column(name))
                .collect(),
        }
    }

    /// Reads the files' schemas, newest first, until every column in `columns` is found.
    /// Returns the columns no file carries.
    async fn find_drift(
        &self,
        state: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        columns: Vec<String>,
        files: &[ObjectMeta],
    ) -> DFResult<Option<Drift>> {
        let mut remaining: BTreeSet<String> = columns.into_iter().collect();
        let (newest, rest) = files.split_at(files.len().min(1));
        for chunk in std::iter::once(newest).chain(rest.chunks(SCHEMA_PROBE_CHUNK)) {
            if chunk.is_empty() {
                continue;
            }
            // A file whose schema cannot be read here fails the scan itself if it is
            // unreadable; the check only reports drift it can prove, so it is skipped.
            let schema = match self.format.infer_schema(state, store, chunk).await {
                Ok(schema) => schema,
                Err(e) => {
                    tracing::debug!(
                        "Dataset '{}': skipping the missing-column check, a source file schema could not be read: {e}",
                        self.dataset_name
                    );
                    return Ok(None);
                }
            };
            remaining.retain(|name| schema.field_with_name(name).is_err());
            if remaining.is_empty() {
                return Ok(None);
            }
        }

        // Keep the registered order so messages are stable.
        let missing: Vec<String> = self
            .file_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|name| remaining.contains(name))
            .collect();
        Ok(Some(Drift {
            missing,
            example_file: files
                .first()
                .map(|f| f.location.to_string())
                .unwrap_or_default(),
        }))
    }
}

/// The object store and files of every file scan in `plan`, or `None` when `plan` scans no
/// files (or scans more than one object store, which a listing table never does).
fn scanned_files(plan: &Arc<dyn ExecutionPlan>) -> Option<(ObjectStoreUrl, Vec<ObjectMeta>)> {
    let mut store_url = None;
    let mut files: Vec<ObjectMeta> = Vec::new();
    let mut mixed_stores = false;
    let _ = plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
        {
            match &store_url {
                None => store_url = Some(config.object_store_url.clone()),
                Some(url) if *url != config.object_store_url => mixed_stores = true,
                Some(_) => {}
            }
            for group in &config.file_groups {
                files.extend(group.files().iter().map(|f| f.object_meta.clone()));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if mixed_stores {
        return None;
    }
    // A file split into byte ranges appears once per range.
    files.sort_by(|a, b| a.location.cmp(&b.location));
    files.dedup_by(|a, b| a.location == b.location);
    store_url.map(|url| (url, files))
}

fn fingerprint(columns: &[String], files: &[ObjectMeta]) -> u64 {
    let mut hasher = DefaultHasher::new();
    columns.hash(&mut hasher);
    for file in files {
        file.location.as_ref().hash(&mut hasher);
        file.size.hash(&mut hasher);
        file.last_modified.hash(&mut hasher);
        file.e_tag.hash(&mut hasher);
        file.version.hash(&mut hasher);
    }
    hasher.finish()
}

fn quoted(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn column_noun(columns: &[String]) -> &'static str {
    if columns.len() == 1 {
        "column"
    } else {
        "columns"
    }
}

fn refresh_error_message(dataset: &str, drift: &Drift) -> String {
    let noun = column_noun(&drift.missing);
    format!(
        "Dataset '{dataset}': registered {noun} {cols} missing from every source file scanned (for example '{file}'). The source schema changed after the dataset was registered, for example a column was renamed or dropped. The refresh was stopped so the accelerated data is not replaced with NULL values; the last loaded data is still served. Restore the {noun} in the source files, or restart the runtime to adopt the new schema.",
        cols = quoted(&drift.missing),
        file = drift.example_file,
    )
}

fn read_warning_message(dataset: &str, drift: &Drift) -> String {
    let noun = column_noun(&drift.missing);
    format!(
        "Dataset '{dataset}': registered {noun} {cols} missing from every source file scanned (for example '{file}'), so queries return NULL for {them}. The source schema changed after the dataset was registered, for example a column was renamed or dropped. Restore the {noun} in the source files, or restart the runtime to adopt the new schema.",
        cols = quoted(&drift.missing),
        file = drift.example_file,
        them = if drift.missing.len() == 1 {
            "it"
        } else {
            "them"
        },
    )
}

/// A file dataset's table with its scans checked by a [`FileSchemaDriftGuard`]. Every other
/// call passes through to `inner`.
#[derive(Debug)]
pub struct FileSchemaDriftCheckedTable {
    inner: Arc<dyn TableProvider>,
    guard: FileSchemaDriftGuard,
}

impl FileSchemaDriftCheckedTable {
    #[must_use]
    pub fn new(inner: Arc<dyn TableProvider>, guard: FileSchemaDriftGuard) -> Self {
        Self { inner, guard }
    }

    /// The table this one checks.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn TableProvider> {
        &self.inner
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for FileSchemaDriftCheckedTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        self.guard
            .check_scan(
                state,
                &self.inner.schema(),
                projection.map(Vec::as_slice),
                &plan,
            )
            .await?;
        Ok(plan)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        let projection = args.projection();
        let result = self.inner.scan_with_args(state, args).await?;
        self.guard
            .check_scan(state, &self.inner.schema(), projection, result.plan())
            .await?;
        Ok(result)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }

    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: datafusion::common::DFSchemaRef,
        on: Expr,
        clauses: Vec<datafusion::logical_expr::dml::MergeIntoClause>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner
            .merge_into(state, source, merge_schema, on, clauses)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::Path;

    fn write_parquet(path: &Path, columns: &[&str], values: &[i64]) {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns
            .iter()
            .map(|_| Arc::new(Int64Array::from(values.to_vec())) as arrow::array::ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("batch");
        let file = std::fs::File::create(path).expect("create parquet file");
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet writer");
    }

    fn registered_schema(columns: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int64, true))
                .collect::<Vec<_>>(),
        ))
    }

    /// A guarded listing table over `dir` with the registered `columns`, as a file dataset
    /// would have registered it before its files changed.
    fn guarded_table(dir: &Path, columns: &[&str]) -> Arc<FileSchemaDriftCheckedTable> {
        let schema = registered_schema(columns);
        let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let config = ListingTableConfig::new(url)
            .with_listing_options(
                ListingOptions::new(Arc::clone(&format)).with_file_extension(".parquet"),
            )
            .with_schema(Arc::clone(&schema));
        let table = Arc::new(ListingTable::try_new(config).expect("listing table"));
        Arc::new(FileSchemaDriftCheckedTable::new(
            table,
            FileSchemaDriftGuard::new("base", schema, format),
        ))
    }

    fn context(refresh: bool) -> SessionContext {
        let mut config = SessionConfig::new();
        if refresh {
            mark_acceleration_refresh(&mut config);
        }
        SessionContext::new_with_config(config)
    }

    async fn query(
        table: &Arc<FileSchemaDriftCheckedTable>,
        refresh: bool,
        sql: &str,
    ) -> DFResult<Vec<RecordBatch>> {
        let ctx = context(refresh);
        ctx.register_table("base", Arc::clone(table) as Arc<dyn TableProvider>)?;
        ctx.sql(sql).await?.collect().await
    }

    fn single_value(batches: &[RecordBatch]) -> Option<i64> {
        let column = batches[0].column(0);
        let column = column.as_any().downcast_ref::<Int64Array>().expect("int64");
        (!column.is_null(0)).then(|| column.value(0))
    }

    #[tokio::test]
    async fn refresh_scan_fails_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_parquet(
            &dir.path().join("base.parquet"),
            &["id", "amount_cents"],
            &[1, 2],
        );
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let before = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("refresh scan before the rename");
        assert_eq!(single_value(&before), Some(3));

        write_parquet(&dir.path().join("base.parquet"), &["id", "amt"], &[1, 2, 3]);

        let err = query(&table, true, "SELECT * FROM base")
            .await
            .expect_err("a refresh scan must not load NULLs for a renamed column");
        let message = err.to_string();
        assert!(message.contains("'amount_cents'"), "{message}");
        assert!(message.contains("Dataset 'base'"), "{message}");
        assert!(message.contains("base.parquet"), "{message}");
        assert!(!message.contains("'id'"), "{message}");
    }

    #[tokio::test]
    async fn federated_scan_warns_and_returns_null_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_parquet(&dir.path().join("base.parquet"), &["id", "amt"], &[1, 2]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, false, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a federated read still answers");
        assert_eq!(single_value(&batches), None);
        assert!(table.guard.state.lock().warned.contains("amount_cents"));

        // A scan that does not read the missing column neither repeats nor re-arms the
        // warning.
        query(&table, false, "SELECT sum(id) FROM base")
            .await
            .expect("read of a present column");
        assert!(table.guard.state.lock().warned.contains("amount_cents"));

        // Restoring the column clears the warning so a later drop is reported again.
        write_parquet(
            &dir.path().join("base.parquet"),
            &["id", "amount_cents"],
            &[1, 2],
        );
        let batches = query(&table, false, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("read after restore");
        assert_eq!(single_value(&batches), Some(3));
        assert!(table.guard.state.lock().warned.is_empty());
    }

    #[tokio::test]
    async fn refresh_scan_allows_an_added_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_parquet(
            &dir.path().join("base.parquet"),
            &["id", "amount_cents", "note"],
            &[1, 2],
        );
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("an added column is ordinary schema evolution");
        assert_eq!(single_value(&batches), Some(3));
    }

    #[tokio::test]
    async fn refresh_scan_allows_older_files_without_a_newer_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_parquet(&dir.path().join("a_old.parquet"), &["id"], &[1]);
        write_parquet(
            &dir.path().join("b_new.parquet"),
            &["id", "amount_cents"],
            &[2],
        );
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a column some scanned file carries is not drift");
        assert_eq!(single_value(&batches), Some(2));
    }

    #[tokio::test]
    async fn refresh_scan_ignores_a_missing_column_it_does_not_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_parquet(&dir.path().join("base.parquet"), &["id", "amt"], &[1, 2]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(id) FROM base")
            .await
            .expect("the renamed column is not read");
        assert_eq!(single_value(&batches), Some(3));
        query(&table, true, "SELECT count(*) FROM base")
            .await
            .expect("count(*) reads no column");
    }

    #[test]
    fn only_metadata_schema_formats_are_checked() {
        use datafusion::datasource::file_format::{csv::CsvFormat, json::JsonFormat};
        assert!(FileSchemaDriftGuard::supports_format(
            &ParquetFormat::default()
        ));
        assert!(!FileSchemaDriftGuard::supports_format(&CsvFormat::default()));
        assert!(!FileSchemaDriftGuard::supports_format(
            &JsonFormat::default()
        ));
    }

    #[test]
    fn messages_name_every_missing_column() {
        let drift = Drift {
            missing: vec!["a".to_string(), "b".to_string()],
            example_file: "dir/f.parquet".to_string(),
        };
        let refresh = refresh_error_message("ds", &drift);
        assert!(
            refresh.contains("registered columns 'a', 'b' missing"),
            "{refresh}"
        );
        assert!(
            refresh.contains("last loaded data is still served"),
            "{refresh}"
        );
        let read = read_warning_message("ds", &drift);
        assert!(read.contains("queries return NULL for them"), "{read}");
    }
}
