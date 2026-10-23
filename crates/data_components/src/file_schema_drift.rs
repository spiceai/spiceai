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
//! Only formats whose per-file column list can be proven complete are checked
//! (see [`FileSchemaDriftGuard::supports_format`]): a CSV/TSV header, or a
//! JSON/NDJSON object-key scan that reads every record. A column that no
//! scanned file carries is reported:
//!
//! - in a scan that feeds an acceleration refresh (the refresh session carries
//!   [`AccelerationRefreshScan`]) that reads every file, the scan fails, so the refresh
//!   fails and keeps the last loaded data. A refresh narrowed to some files (a partition
//!   filter in `refresh_sql`, say) only warns, because the files it selects may predate the
//!   column;
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
use dataformat_json::{NonRepartitionedFileScanConfig, SpiceJsonFormat};
use datafusion::{
    catalog::{ScanArgs, ScanResult, Session},
    common::tree_node::{TreeNode, TreeNodeRecursion},
    datasource::{
        TableProvider,
        file_format::{FileFormat, json::JsonFormat},
    },
    error::{DataFusionError, Result as DFResult},
    execution::{config::SessionConfig, object_store::ObjectStoreUrl},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_datasource::{file_scan_config::FileScanConfig, source::DataSourceExec};
use object_store::ObjectMeta;
use parking_lot::Mutex;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

use crate::csv_header_projection::HeaderProjectedCsvFormat;

/// How many files' schemas are read concurrently while looking for a column. Files are
/// read newest first and the probe stops as soon as every column is found, normally after
/// the newest file.
const SCHEMA_PROBE_CONCURRENCY: usize = 16;

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
    /// scans over unchanged files read no file schemas.
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
    /// Whether scans of `format` can be checked: the file's full column list can
    /// be proven (a CSV/TSV header via [`HeaderProjectedCsvFormat`], or every
    /// JSON/NDJSON record's object keys). A column missing from that list is
    /// absent from every record. Parquet and ORC footers are the same kind of
    /// declaration; they are left to the pending file-schema-drift change that
    /// lands that path.
    #[must_use]
    pub fn supports_format(format: &dyn FileFormat) -> bool {
        format.downcast_ref::<HeaderProjectedCsvFormat>().is_some()
            || format.downcast_ref::<JsonFormat>().is_some()
            || format.downcast_ref::<SpiceJsonFormat>().is_some()
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
    /// column is missing from every scanned file, or the error reading a file's schema in an
    /// acceleration refresh session. Any other scan skips the check when a file schema cannot
    /// be read.
    pub async fn check_scan(
        &self,
        state: &dyn Session,
        table_schema: &SchemaRef,
        projection: Option<&[usize]>,
        selects_all_files: bool,
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
            // it normally carries every column and ends the probe after one header.
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
            match self
                .find_drift(state, &store, &store_url, columns.clone(), &files)
                .await
            {
                Ok(drift) => {
                    self.state.lock().last_check = Some((fingerprint, drift.clone()));
                    drift
                }
                // Inconclusive, and not recorded, so the next scan probes again. A refresh
                // must not load data it could not check, so it fails with the read error
                // (the scan reads the same files); any other scan goes ahead.
                Err(e) if is_acceleration_refresh(state) => return Err(e),
                Err(e) => {
                    tracing::debug!(
                        "Dataset '{}': skipping the missing-column check, a source file schema could not be read: {e}",
                        self.dataset_name
                    );
                    return Ok(());
                }
            }
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

        // A scan narrowed to some files (a partition filter, a `_location` predicate, a
        // limit) can legitimately read only older files that predate a column, so only a scan
        // of every file proves the column gone.
        if selects_all_files && is_acceleration_refresh(state) {
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

    /// Whether a scan with `filters` and `limit` reads every file of the dataset. Only
    /// predicates on partition or metadata columns (columns not stored in the files) and a
    /// limit can drop files before the scan; predicates on file columns prune row groups
    /// inside each file, not files.
    #[must_use]
    pub fn scan_selects_all_files(&self, filters: &[Expr], limit: Option<usize>) -> bool {
        limit.is_none()
            && filters.iter().all(|filter| {
                filter
                    .column_refs()
                    .iter()
                    .all(|c| self.file_schema.field_with_name(&c.name).is_ok())
            })
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

    /// Reads the files' schemas, newest first and one file at a time (so unrelated columns
    /// whose types differ across files never have to merge), until every column in `columns`
    /// is found. Returns the columns no file carries.
    ///
    /// # Errors
    ///
    /// Returns the error reading a file's schema. The probe is then inconclusive: the caller
    /// must neither report drift nor record the files as checked.
    async fn find_drift(
        &self,
        state: &dyn Session,
        store: &Arc<dyn object_store::ObjectStore>,
        store_url: &ObjectStoreUrl,
        columns: Vec<String>,
        files: &[ObjectMeta],
    ) -> DFResult<Option<Drift>> {
        let mut remaining: BTreeSet<String> = columns.into_iter().collect();
        // The newest file alone first: it normally carries every column.
        let (newest, rest) = files.split_at(files.len().min(1));
        for chunk in std::iter::once(newest).chain(rest.chunks(SCHEMA_PROBE_CONCURRENCY)) {
            let probe_format = schema_probe_format(&self.format);
            let probes: Vec<_> = chunk
                .iter()
                .map(|file| probe_format.infer_schema(state, store, std::slice::from_ref(file)))
                .collect();
            for schema in futures::future::join_all(probes).await {
                let schema = schema?;
                remaining.retain(|name| schema.field_with_name(name).is_err());
            }
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
                .map(|f| display_source_file(store_url, &f.location))
                .unwrap_or_default(),
        }))
    }
}

/// The [`FileScanConfig`] a [`DataSourceExec`] scans, including JSON's
/// [`NonRepartitionedFileScanConfig`] wrapper. CSV/TSV header projection keeps a
/// plain [`FileScanConfig`]; no other file format in this path wraps the config.
fn file_scan_config(
    source: &dyn datafusion_datasource::source::DataSource,
) -> Option<&FileScanConfig> {
    if let Some(config) = source.downcast_ref::<FileScanConfig>() {
        return Some(config);
    }
    source
        .downcast_ref::<NonRepartitionedFileScanConfig>()
        .map(NonRepartitionedFileScanConfig::file_scan_config)
}

/// The object store and files of every file scan in `plan`, or `None` when `plan` scans no
/// files (or scans more than one object store, which a listing table never does).
fn scanned_files(plan: &Arc<dyn ExecutionPlan>) -> Option<(ObjectStoreUrl, Vec<ObjectMeta>)> {
    let mut store_url = None;
    let mut files: Vec<ObjectMeta> = Vec::new();
    let mut mixed_stores = false;
    let _ = plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = file_scan_config(exec.data_source().as_ref())
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

/// JSON schema inference defaults to a record sample, which cannot prove a
/// column absent (a sparse key can sit past the sample). The drift check reads
/// every record so a rename is detected and a rare key is not.
fn schema_probe_format(format: &Arc<dyn FileFormat>) -> Arc<dyn FileFormat> {
    if let Some(json) = format.as_ref().downcast_ref::<JsonFormat>() {
        let mut options = json.options().clone();
        options.schema_infer_max_rec = Some(usize::MAX);
        return Arc::new(JsonFormat::default().with_options(options));
    }
    if let Some(json) = format.as_ref().downcast_ref::<SpiceJsonFormat>() {
        return Arc::new(json.clone().with_schema_infer_max_rec(usize::MAX));
    }
    Arc::clone(format)
}

/// Local file object-store paths drop the leading `/`. Restore it so the
/// message names a path the user can open (`/tmp/...`, not `tmp/...`).
fn display_source_file(store_url: &ObjectStoreUrl, location: &object_store::path::Path) -> String {
    let path = location.to_string();
    let file_store = store_url.as_str().starts_with("file:");
    // A Windows drive path (`C:/...`) is already absolute.
    let windows_drive = path.chars().nth(1) == Some(':');
    if file_store && !path.starts_with('/') && !windows_drive {
        format!("/{path}")
    } else {
        path
    }
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

/// The [`TableLayer`] that checks a file dataset's scans with a [`FileSchemaDriftGuard`].
/// Every other call, and every layer walk, goes to the table beneath.
#[derive(Debug)]
pub struct FileSchemaDriftLayer {
    guard: FileSchemaDriftGuard,
}

impl FileSchemaDriftLayer {
    #[must_use]
    pub fn new(guard: FileSchemaDriftGuard) -> Self {
        Self { guard }
    }

    /// `table` with its scans checked by `guard`.
    #[must_use]
    pub fn over(
        table: Arc<dyn TableProvider>,
        guard: FileSchemaDriftGuard,
    ) -> Arc<dyn TableProvider> {
        SpiceTable::over(Arc::new(Self::new(guard)), table)
    }
}

#[async_trait]
impl TableLayer for FileSchemaDriftLayer {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind for this
        // layer without anyone deciding what it should say.
        match walk {
            // The check changes no row, column, or type a scan returns: a scan either
            // returns the plan of the table beneath or fails. So passthrough sees past it
            // too, and the Iceberg REST API refuses a file dataset because its files are not
            // an Iceberg table, rather than as a table whose scans Spice changes.
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::Write
            | LayerWalk::RetentionDelete
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
        }
    }

    async fn scan_with_args<'a>(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        let projection = args.projection();
        let selects_all_files = self
            .guard
            .scan_selects_all_files(args.filters().unwrap_or_default(), args.limit());
        let result = below.scan_with_args(state, args).await?;
        self.guard
            .check_scan(
                state,
                &below.schema(),
                projection,
                selects_all_files,
                result.plan(),
            )
            .await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_header_projection::HeaderProjectedCsvFormat;
    use arrow::array::{Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use dataformat_json::{Format, SpiceJsonFormat};
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::file_format::json::JsonFormat;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::Path;

    fn write_csv(path: &Path, header: &str, rows: &[&str]) {
        let mut text = String::from(header);
        text.push('\n');
        for row in rows {
            text.push_str(row);
            text.push('\n');
        }
        std::fs::write(path, text).expect("write csv");
    }

    fn write_ndjson(path: &Path, lines: &[&str]) {
        let mut text = String::new();
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        std::fs::write(path, text).expect("write ndjson");
    }

    fn write_json_array(path: &Path, objects: &[&str]) {
        std::fs::write(path, format!("[{}]", objects.join(","))).expect("write json array");
    }

    fn write_json_pointer_doc(path: &Path, objects: &[&str]) {
        std::fs::write(path, format!(r#"{{"data":[{}]}}"#, objects.join(",")))
            .expect("write json pointer document");
    }

    fn guarded_spice_json_table(dir: &Path, columns: &[&str], format: SpiceJsonFormat) -> Guarded {
        let schema = registered_schema(columns);
        let format: Arc<dyn FileFormat> = Arc::new(format);
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let config = ListingTableConfig::new(url)
            .with_listing_options(
                ListingOptions::new(Arc::clone(&format)).with_file_extension(".json"),
            )
            .with_schema(Arc::clone(&schema));
        let listing: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        let layer = Arc::new(FileSchemaDriftLayer::new(FileSchemaDriftGuard::new(
            "base", schema, format,
        )));
        let table: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::clone(&layer) as Arc<dyn TableLayer>, listing);
        Guarded { table, layer }
    }

    fn guarded_json_table(dir: &Path, columns: &[&str]) -> Guarded {
        let schema = registered_schema(columns);
        let format: Arc<dyn FileFormat> = Arc::new(JsonFormat::default());
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let config = ListingTableConfig::new(url)
            .with_listing_options(
                ListingOptions::new(Arc::clone(&format)).with_file_extension(".json"),
            )
            .with_schema(Arc::clone(&schema));
        let listing: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        let layer = Arc::new(FileSchemaDriftLayer::new(FileSchemaDriftGuard::new(
            "base", schema, format,
        )));
        let table: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::clone(&layer) as Arc<dyn TableLayer>, listing);
        Guarded { table, layer }
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
    struct Guarded {
        table: Arc<dyn TableProvider>,
        layer: Arc<FileSchemaDriftLayer>,
    }

    fn guarded_table(dir: &Path, columns: &[&str]) -> Guarded {
        let schema = registered_schema(columns);
        let format: Arc<dyn FileFormat> =
            HeaderProjectedCsvFormat::wrap(Arc::new(CsvFormat::default().with_has_header(true)));
        let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("url");
        let config = ListingTableConfig::new(url)
            .with_listing_options(
                ListingOptions::new(Arc::clone(&format)).with_file_extension(".csv"),
            )
            .with_schema(Arc::clone(&schema));
        let listing: Arc<dyn TableProvider> =
            Arc::new(ListingTable::try_new(config).expect("listing table"));
        let layer = Arc::new(FileSchemaDriftLayer::new(FileSchemaDriftGuard::new(
            "base", schema, format,
        )));
        let table: Arc<dyn TableProvider> =
            SpiceTable::over(Arc::clone(&layer) as Arc<dyn TableLayer>, listing);
        Guarded { table, layer }
    }

    fn context(refresh: bool) -> SessionContext {
        let mut config = SessionConfig::new();
        if refresh {
            mark_acceleration_refresh(&mut config);
        }
        SessionContext::new_with_config(config)
    }

    async fn query(table: &Guarded, refresh: bool, sql: &str) -> DFResult<Vec<RecordBatch>> {
        let ctx = context(refresh);
        ctx.register_table("base", Arc::clone(&table.table))?;
        ctx.sql(sql).await?.collect().await
    }

    fn single_value(batches: &[RecordBatch]) -> Option<i64> {
        let column = batches[0].column(0);
        let column = column.as_any().downcast_ref::<Int64Array>().expect("int64");
        (!column.is_null(0)).then(|| column.value(0))
    }

    fn two_values(batches: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
        let batch = &batches[0];
        let first = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0");
        let second = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 1");
        (
            (!first.is_null(0)).then(|| first.value(0)),
            (!second.is_null(0)).then(|| second.value(0)),
        )
    }

    #[tokio::test]
    async fn refresh_scan_fails_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(&dir.path().join("base.csv"), "id,amount_cents", &["1,2"]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let before = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("refresh scan before the rename");
        assert_eq!(single_value(&before), Some(2));

        write_csv(&dir.path().join("base.csv"), "id,amt", &["1,2", "3,4"]);

        let err = query(&table, true, "SELECT * FROM base")
            .await
            .expect_err("a refresh scan must not load NULLs for a renamed column");
        let message = err.to_string();
        assert!(message.contains("'amount_cents'"), "{message}");
        assert!(message.contains("Dataset 'base'"), "{message}");
        assert!(
            message.contains(&format!("'{}/base.csv'", dir.path().display())),
            "the example file must be an absolute path: {message}"
        );
        assert!(!message.contains("'id'"), "{message}");
    }

    #[tokio::test]
    async fn federated_scan_warns_and_returns_null_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(&dir.path().join("base.csv"), "id,amt", &["1,2"]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, false, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a federated read still answers");
        assert_eq!(single_value(&batches), None);
        assert!(
            table
                .layer
                .guard
                .state
                .lock()
                .warned
                .contains("amount_cents")
        );

        // A scan that does not read the missing column neither repeats nor re-arms the
        // warning.
        query(&table, false, "SELECT sum(id) FROM base")
            .await
            .expect("read of a present column");
        assert!(
            table
                .layer
                .guard
                .state
                .lock()
                .warned
                .contains("amount_cents")
        );

        // Restoring the column clears the warning so a later drop is reported again.
        write_csv(&dir.path().join("base.csv"), "id,amount_cents", &["1,2"]);
        let batches = query(&table, false, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("read after restore");
        assert_eq!(single_value(&batches), Some(2));
        assert!(table.layer.guard.state.lock().warned.is_empty());
    }

    #[tokio::test]
    async fn refresh_scan_allows_an_added_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(
            &dir.path().join("base.csv"),
            "id,amount_cents,note",
            &["1,2,x"],
        );
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("an added column is ordinary schema evolution");
        assert_eq!(single_value(&batches), Some(2));
    }

    #[tokio::test]
    async fn refresh_scan_allows_older_files_without_a_newer_column() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(&dir.path().join("a_old.csv"), "id", &["1"]);
        write_csv(&dir.path().join("b_new.csv"), "id,amount_cents", &["2,2"]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a column some scanned file carries is not drift");
        assert_eq!(single_value(&batches), Some(2));
    }

    #[tokio::test]
    async fn refresh_scan_ignores_a_missing_column_it_does_not_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_csv(&dir.path().join("base.csv"), "id,amt", &["1,2"]);
        let table = guarded_table(dir.path(), &["id", "amount_cents"]);

        let batches = query(&table, true, "SELECT sum(id) FROM base")
            .await
            .expect("the renamed column is not read");
        assert_eq!(single_value(&batches), Some(1));
        query(&table, true, "SELECT count(*) FROM base")
            .await
            .expect("count(*) reads no column");
    }

    #[tokio::test]
    async fn json_reordered_object_keys_keep_values_with_their_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ndjson(
            &dir.path().join("base.json"),
            &[
                r#"{"id":1,"amount_cents":100,"user_id":10}"#,
                r#"{"id":2,"amount_cents":200,"user_id":20}"#,
            ],
        );
        let table = guarded_json_table(dir.path(), &["id", "amount_cents", "user_id"]);
        let before = query(
            &table,
            false,
            "SELECT sum(amount_cents), sum(user_id) FROM base",
        )
        .await
        .expect("query before reorder");
        assert_eq!(two_values(&before), (Some(300), Some(30)));

        write_ndjson(
            &dir.path().join("base.json"),
            &[
                r#"{"id":1,"user_id":10,"amount_cents":100}"#,
                r#"{"id":2,"user_id":20,"amount_cents":200}"#,
            ],
        );
        let after = query(
            &table,
            false,
            "SELECT sum(amount_cents), sum(user_id) FROM base",
        )
        .await
        .expect("query after key reorder");
        assert_eq!(
            two_values(&after),
            (Some(300), Some(30)),
            "JSON already binds by name; a key reorder must not swap values"
        );
    }

    #[tokio::test]
    async fn json_refresh_scan_fails_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ndjson(
            &dir.path().join("base.json"),
            &[r#"{"id":1,"amount_cents":2}"#],
        );
        let table = guarded_json_table(dir.path(), &["id", "amount_cents"]);
        let before = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("refresh scan before the rename");
        assert_eq!(single_value(&before), Some(2));

        write_ndjson(
            &dir.path().join("base.json"),
            &[r#"{"id":1,"amt":2}"#, r#"{"id":3,"amt":4}"#],
        );
        let err = query(&table, true, "SELECT * FROM base")
            .await
            .expect_err("a refresh scan must not overwrite accelerated JSON with NULLs");
        let message = err.to_string();
        assert!(message.contains("'amount_cents'"), "{message}");
        assert!(message.contains("Dataset 'base'"), "{message}");
    }

    #[tokio::test]
    async fn json_federated_scan_warns_and_returns_null_when_a_registered_column_is_renamed() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ndjson(&dir.path().join("base.json"), &[r#"{"id":1,"amt":2}"#]);
        let table = guarded_json_table(dir.path(), &["id", "amount_cents"]);
        let batches = query(&table, false, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a federated read still answers");
        assert_eq!(single_value(&batches), None);
        assert!(
            table
                .layer
                .guard
                .state
                .lock()
                .warned
                .contains("amount_cents")
        );
    }

    #[tokio::test]
    async fn json_refresh_scan_finds_a_column_absent_from_the_first_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_ndjson(
            &dir.path().join("base.json"),
            &[r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":3,"amount_cents":2}"#],
        );
        let table = guarded_json_table(dir.path(), &["id", "amount_cents"]);
        let batches = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .expect("a sparse JSON key later in the file is not drift");
        assert_eq!(single_value(&batches), Some(2));
    }

    #[test]
    fn declared_column_formats_are_checked() {
        assert!(FileSchemaDriftGuard::supports_format(
            HeaderProjectedCsvFormat::wrap(Arc::new(CsvFormat::default().with_has_header(true)))
                .as_ref()
        ));
        assert!(FileSchemaDriftGuard::supports_format(&JsonFormat::default()));
        assert!(FileSchemaDriftGuard::supports_format(
            &dataformat_json::SpiceJsonFormat::default()
        ));
        assert!(!FileSchemaDriftGuard::supports_format(
            &CsvFormat::default().with_has_header(true)
        ));
        assert!(!FileSchemaDriftGuard::supports_format(
            &ParquetFormat::default()
        ));
    }

    #[test]
    fn only_unfiltered_unlimited_scans_select_all_files() {
        use datafusion::prelude::{col, lit};
        let guard = FileSchemaDriftGuard::new(
            "base",
            registered_schema(&["id", "amount_cents"]),
            HeaderProjectedCsvFormat::wrap(Arc::new(CsvFormat::default().with_has_header(true))),
        );
        assert!(guard.scan_selects_all_files(&[], None));
        assert!(guard.scan_selects_all_files(&[col("id").gt(lit(1))], None));
        assert!(!guard.scan_selects_all_files(&[col("p").eq(lit(1))], None));
        assert!(!guard.scan_selects_all_files(&[], Some(10)));
    }

    const JSON_BASELINE: &[&str] = &[r#"{"id":1,"amount_cents":2}"#];
    const JSON_RENAMED: &[&str] = &[r#"{"id":1,"amt":2}"#, r#"{"id":3,"amt":4}"#];

    async fn spice_json_rename_case(
        label: &str,
        format: SpiceJsonFormat,
        writer: fn(&Path, &[&str]),
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        writer(&dir.path().join("base.json"), JSON_BASELINE);
        let table = guarded_spice_json_table(dir.path(), &["id", "amount_cents"], format);

        let before = query(&table, true, "SELECT sum(amount_cents) FROM base")
            .await
            .unwrap_or_else(|e| panic!("{label}: refresh scan before the rename: {e}"));
        assert_eq!(single_value(&before), Some(2), "{label} last good sum");

        writer(&dir.path().join("base.json"), JSON_RENAMED);

        let federated = query(&table, false, "SELECT sum(amount_cents), sum(id) FROM base")
            .await
            .unwrap_or_else(|e| panic!("{label}: federated read after rename: {e}"));
        assert_eq!(
            two_values(&federated),
            (None, Some(4)),
            "{label}: federated rename returns NULL for the missing column"
        );
        assert!(
            table
                .layer
                .guard
                .state
                .lock()
                .warned
                .contains("amount_cents"),
            "{label}: federated rename must WARN"
        );

        let err = query(&table, true, "SELECT * FROM base")
            .await
            .expect_err("a refresh scan must not overwrite accelerated JSON with NULLs");
        let message = err.to_string();
        assert!(message.contains("'amount_cents'"), "{label}: {message}");
        assert!(message.contains("Dataset 'base'"), "{label}: {message}");
        assert!(
            message.contains(&format!("'{}/base.json'", dir.path().display())),
            "{label}: example file must be an absolute path: {message}"
        );
    }

    #[tokio::test]
    async fn json_array_refresh_scan_fails_when_a_registered_column_is_renamed() {
        spice_json_rename_case(
            "file_format json",
            SpiceJsonFormat::default().with_format(Format::Json),
            write_json_array,
        )
        .await;
    }

    #[tokio::test]
    async fn auto_json_refresh_scan_fails_when_a_registered_column_is_renamed() {
        spice_json_rename_case(
            "auto-detected json",
            SpiceJsonFormat::default().with_format(Format::Auto),
            write_json_array,
        )
        .await;
    }

    #[tokio::test]
    async fn json_pointer_refresh_scan_fails_when_a_registered_column_is_renamed() {
        spice_json_rename_case(
            "json_pointer",
            SpiceJsonFormat::default()
                .with_format(Format::Auto)
                .with_json_pointer("/data".to_string()),
            write_json_pointer_doc,
        )
        .await;
    }

    #[tokio::test]
    async fn scanned_files_sees_a_wrapped_json_array_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_json_array(&dir.path().join("base.json"), JSON_BASELINE);
        let table = guarded_spice_json_table(
            dir.path(),
            &["id", "amount_cents"],
            SpiceJsonFormat::default().with_format(Format::Json),
        );
        let ctx = context(false);
        ctx.register_table("base", Arc::clone(&table.table))
            .expect("register");
        let plan = ctx
            .sql("SELECT * FROM base")
            .await
            .expect("sql")
            .create_physical_plan()
            .await
            .expect("plan");
        let (store_url, files) =
            scanned_files(&plan).expect("a wrapped JSON scan must still list its files");
        assert!(
            store_url.as_str().starts_with("file:"),
            "{}",
            store_url.as_str()
        );
        assert!(
            files
                .iter()
                .any(|f| f.location.to_string().ends_with("base.json")),
            "{files:?}"
        );
    }

    #[test]
    fn file_store_example_path_keeps_the_leading_slash() {
        let url = ObjectStoreUrl::parse("file:///").expect("file store");
        let path = object_store::path::Path::from("tmp/spice-verify/data/events.csv");
        assert_eq!(
            display_source_file(&url, &path),
            "/tmp/spice-verify/data/events.csv"
        );
        let s3 = ObjectStoreUrl::parse("s3://bucket").expect("s3 store");
        assert_eq!(
            display_source_file(&s3, &path),
            "tmp/spice-verify/data/events.csv"
        );
    }

    #[test]
    fn messages_name_every_missing_column() {
        let drift = Drift {
            missing: vec!["a".to_string(), "b".to_string()],
            example_file: "dir/f.csv".to_string(),
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
