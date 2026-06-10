//! Snapshot directory/object-store IO: staging, moves, sync, and cleanup.
//!
//! Path/URL construction (`snapshot_dir_path`, `snapshot_dir_url`), listing
//! table construction, staging-directory clears, the staged-file moves
//! (`move_staging_files_local`/`_s3` — run inside the caller's held
//! `listing_fence.write()` and record moved files in
//! `last_moved_snapshot_files` for the list-files-cache delta-apply), the
//! ordering-tier directory fsync (`sync_snapshot_dir`, see
//! `provider/fsync_tier.rs`), and grace-period old-snapshot cleanup.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use datafusion::datasource::file_format::FileFormat;
use futures::TryStreamExt;

use super::{
    Arc, CatalogError, CatalogResult, CayenneTableProvider, Error, HashSet, ListingOptions,
    ListingTable, ListingTableConfig, ListingTableUrl, OBJECT_STORE_MOVE_CONCURRENCY, ObjectMeta,
    ObjectStore, ObjectStoreExt, ObjectStorePath, Ordering, PkDeletionStrategyWithCache,
    PositionDeletionAccessPlanProvider, Result, RuntimeEnv, STAGING_DIR_NAME, STAGING_WAL_FILENAME,
    STAGING_WAL_TMP_FILENAME, SchemaRef, SessionConfig, VortexFormat, stream,
};

impl CayenneTableProvider {
    /// Returns whether retention filters are configured for this table.
    #[must_use]
    pub(crate) fn has_retention_delete_filters(&self) -> bool {
        !self.retention_filters.is_empty()
    }

    /// Returns the path to a snapshot directory for this table.
    #[must_use]
    pub(crate) fn snapshot_dir_path_for(&self, snapshot_id: &str) -> std::path::PathBuf {
        Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        )
    }

    /// Atomically commit a snapshot rewrite to the catalog.
    ///
    /// Delegates to [`MetadataCatalog::commit_compaction`], which advances the
    /// snapshot pointer and clears file-level delete/insert tracking while
    /// preserving inlined rows. This is the correct commit primitive for sort
    /// rewrites and file compaction; true overwrite operations use the catalog's
    /// overwrite path directly.
    pub(crate) async fn commit_snapshot_rewrite(&self, new_snapshot_id: &str) -> CatalogResult<()> {
        self.catalog
            .commit_compaction(&self.table_metadata.table_id, new_snapshot_id)
            .await
    }

    /// Update the listing table to point to a new snapshot directory.
    ///
    /// This ensures subsequent queries in the same context will read from the new data.
    /// Holds [`Self::listing_fence`] for write across the Arc swap so any in-flight
    /// [`Self::scan`] using `listing_fence.read()` either resolves entirely
    /// before this swap or entirely after it.
    pub(crate) async fn update_listing_table_for_snapshot(
        &self,
        new_snapshot_id: &str,
    ) -> Result<()> {
        let snapshot_dir_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            new_snapshot_id,
        );

        let new_listing_table = Self::create_listing_table(
            &snapshot_dir_url,
            Arc::clone(&self.table_metadata.schema),
            self.context.file_format(),
            &self.pk_deletion_strategy,
        )?;

        let _fence = self.listing_fence.write().await;
        self.listing_table.store(new_listing_table);
        Ok(())
    }

    /// Trigger cleanup of old snapshot directories in the background.
    ///
    /// This is a non-blocking operation that logs warnings on failure but doesn't
    /// propagate errors, as cleanup failures shouldn't fail the write operation.
    ///
    /// Protected snapshots (those containing data written after deletions) are preserved
    /// alongside the current snapshot to prevent data loss for queries that reference them.
    pub(crate) async fn trigger_old_snapshot_cleanup(&self, current_snapshot: &str) {
        // Grace period before physically removing the old snapshot
        // directories. Scans hold `listing_fence.read()` during plan-build
        // (file paths are resolved against the old snapshot) but execute
        // the plan AFTER the fence is released. If cleanup races ahead of
        // plan execution the scan opens files that have been unlinked and
        // fails with NotFound. Sleeping `OLD_SNAPSHOT_CLEANUP_GRACE` before
        // deleting lets every plan that began under the old listing table
        // finish opening its files.
        const OLD_SNAPSHOT_CLEANUP_GRACE: std::time::Duration = std::time::Duration::from_mins(2);

        if self.table_metadata.path.starts_with("s3://") {
            // S3 cleanup uses `self.cleanup_old_snapshots_s3` which holds
            // `&self`; sleep + cleanup are awaited inline. The compaction
            // caller is itself a background task, so blocking it for
            // `OLD_SNAPSHOT_CLEANUP_GRACE` only delays the next compaction
            // cycle, not user writes or scans.
            tokio::time::sleep(OLD_SNAPSHOT_CLEANUP_GRACE).await;
            // Read the LIVE protected set after the grace period. During the
            // sleep, CDC writers may have created new protected snapshots that
            // must not be deleted.
            let protected_snapshot_ids: HashSet<String> =
                self.protected_snapshots.load().keys().cloned().collect();
            if let Err(err) = self
                .cleanup_old_snapshots_s3(current_snapshot, &protected_snapshot_ids)
                .await
            {
                tracing::warn!(
                    "Failed to cleanup old S3 snapshots for table {}: {err}",
                    &self.table_metadata.table_id
                );
            }
        } else {
            let table_path = self.table_metadata.path.clone();
            let table_id = self.table_metadata.table_id.clone();
            let current_snapshot = current_snapshot.to_string();
            let protected_snapshots = Arc::clone(&self.protected_snapshots);
            tokio::spawn(async move {
                tokio::time::sleep(OLD_SNAPSHOT_CLEANUP_GRACE).await;
                // Read the LIVE protected set after the grace period. During the
                // sleep, CDC writers may have created new protected snapshots
                // that must not be deleted. Capturing the set before the sleep
                // caused a race: compaction clears `protected_snapshots` at
                // commit time, new CDC writes re-populate it, then the stale
                // (empty) captured set causes cleanup to delete them.
                let protected_snapshot_ids: HashSet<String> =
                    protected_snapshots.load().keys().cloned().collect();
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(e) = Self::cleanup_old_snapshots_blocking(
                        &table_path,
                        &table_id,
                        &current_snapshot,
                        &protected_snapshot_ids,
                    ) {
                        tracing::warn!(
                            "Failed to cleanup old snapshots for table {}: {e}",
                            table_id
                        );
                    }
                })
                .await;
            });
        }
    }

    /// Construct the path to a snapshot directory.
    ///
    /// Directory structure: `[table_path]/[table_id]/[snapshot_id]/`
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    pub(in crate::provider) fn snapshot_dir_path(
        table_path: &str,
        table_id: &str,
        snapshot_id: &str,
    ) -> std::path::PathBuf {
        std::path::PathBuf::from(table_path)
            .join(table_id)
            .join(snapshot_id)
    }

    /// Convert a directory path to a `DataFusion`-compatible URL string with trailing slash.
    ///
    /// `DataFusion` requires directory URLs to end with a trailing slash.
    pub(super) fn dir_to_url_string(dir: &std::path::Path) -> String {
        let mut url_str = dir.to_string_lossy().to_string();
        if !url_str.ends_with('/') {
            url_str.push('/');
        }
        url_str
    }

    pub(super) fn register_object_store_if_needed(
        runtime_env: &Arc<RuntimeEnv>,
        config: &crate::metadata::ObjectStoreConfig,
    ) {
        // Use the object store registry to check if already registered
        let already_registered = runtime_env
            .object_store_registry
            .get_store(&config.url)
            .is_ok_and(|existing| Arc::ptr_eq(&existing, &config.store));

        if !already_registered {
            runtime_env.register_object_store(&config.url, Arc::clone(&config.store));
            tracing::debug!("Registered object store for {}", config.url.as_str());
        }
    }

    pub(super) fn runtime_env_cache_key(runtime_env: &Arc<RuntimeEnv>) -> usize {
        Arc::as_ptr(runtime_env) as usize
    }

    pub(super) fn register_object_store_for_runtime(
        &self,
        runtime_env: &Arc<RuntimeEnv>,
        config: &crate::metadata::ObjectStoreConfig,
    ) {
        let runtime_env_key = Self::runtime_env_cache_key(runtime_env);
        if self
            .object_store_registered_runtime_envs
            .lock()
            .contains(&runtime_env_key)
        {
            return;
        }

        Self::register_object_store_if_needed(runtime_env, config);
        self.object_store_registered_runtime_envs
            .lock()
            .insert(runtime_env_key);
    }

    pub(in crate::provider) fn require_object_store(
        &self,
    ) -> Result<&crate::metadata::ObjectStoreConfig> {
        self.object_store_config
            .as_ref()
            .ok_or_else(|| Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: "S3 storage requires an object_store_config".to_string(),
            })
    }

    pub(in crate::provider) fn snapshot_object_store_prefix(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<ObjectStorePath>> {
        if !self.table_metadata.path.starts_with("s3://") {
            return Ok(None);
        }

        let snapshot_url = Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            snapshot_id,
        );

        let url = url::Url::parse(&snapshot_url).map_err(|source| Error::UrlParse {
            url: snapshot_url.clone(),
            source,
        })?;

        let host = url.host_str().unwrap_or_default();
        let config = self.require_object_store()?;
        let config_host = config.url.host_str().unwrap_or_default();

        if !config_host.is_empty() && !host.is_empty() && config_host != host {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!(
                    "Snapshot host {host} does not match configured object store host {config_host}"
                ),
            });
        }

        let path = url.path().trim_start_matches('/');
        Ok(Some(ObjectStorePath::from(path)))
    }

    pub(super) async fn delete_prefix_with_object_store(
        &self,
        prefix: &ObjectStorePath,
    ) -> Result<()> {
        let config = self.require_object_store()?;
        let objects: Vec<_> = config
            .store
            .list(Some(prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list objects for snapshot cleanup",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        let store = Arc::clone(&config.store);
        let table_name = self.table_metadata.table_name.clone();
        stream::iter(objects.into_iter().map(Ok::<_, Error>))
            .try_for_each_concurrent(OBJECT_STORE_MOVE_CONCURRENCY, |meta| {
                let store = Arc::clone(&store);
                let table_name = table_name.clone();
                async move {
                    store
                        .delete(&meta.location)
                        .await
                        .map_err(|e| Error::ObjectStore {
                            operation: "delete object from snapshot cleanup",
                            table: table_name,
                            source: e,
                        })
                }
            })
            .await?;

        Ok(())
    }

    pub(super) async fn cleanup_old_snapshots_s3(
        &self,
        current_snapshot: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> Result<()> {
        let config = self.require_object_store()?;

        let base_url =
            url::Url::parse(&self.table_metadata.path).map_err(|source| Error::UrlParse {
                url: self.table_metadata.path.clone(),
                source,
            })?;

        let mut base_prefix = base_url.path().trim_start_matches('/').to_string();
        if !base_prefix.ends_with('/') {
            base_prefix.push('/');
        }

        let prefix =
            ObjectStorePath::from(format!("{base_prefix}{}/", self.table_metadata.table_id));

        let list_result = config
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list snapshots for cleanup",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        for common_prefix in list_result.common_prefixes {
            if let Some(snapshot_id) = common_prefix.parts().next_back() {
                let snapshot_id_str = snapshot_id.as_ref();
                // Skip current snapshot, protected snapshots, and the staging directory
                if snapshot_id_str == current_snapshot
                    || protected_snapshot_ids.contains(snapshot_id_str)
                    || snapshot_id_str == STAGING_DIR_NAME
                {
                    tracing::trace!(
                        "Keeping snapshot: {snapshot_id_str} (current, protected, or staging)"
                    );
                    continue;
                }
                self.delete_prefix_with_object_store(&common_prefix).await?;
            }
        }

        Ok(())
    }

    /// Create a new `ListingTable` for a snapshot directory.
    ///
    /// # Arguments
    ///
    /// * `snapshot_dir_url` - URL string for the snapshot directory (local path or S3 URL)
    /// * `schema` - Arrow schema for the table
    /// * `vortex_format` - Vortex format
    /// * `strategy` - The deletion strategy for this table (contains embedded caches)
    ///
    /// # Errors
    ///
    /// Returns an error if the listing table cannot be created.
    pub(super) fn create_listing_table(
        snapshot_dir_url: &str,
        schema: SchemaRef,
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
    ) -> Result<Arc<ListingTable>> {
        Self::create_listing_table_with_config(
            snapshot_dir_url,
            schema,
            vortex_format,
            strategy,
            &SessionConfig::default(),
        )
    }

    pub(super) fn create_listing_table_with_config(
        snapshot_dir_url: &str,
        schema: SchemaRef,
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
        session_config: &SessionConfig,
    ) -> Result<Arc<ListingTable>> {
        let table_url = ListingTableUrl::parse(snapshot_dir_url)?;

        let listing_options = Self::create_listing_options(vortex_format, strategy, session_config);

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let listing_table = ListingTable::try_new(config)?;

        Ok(Arc::new(listing_table))
    }

    /// Create listing options for Vortex format.
    ///
    /// Attaches a [`PositionDeletionAccessPlanProvider`] backed by the strategy's
    /// position cache for **every** strategy, so position-based deletes are
    /// pushed into the Vortex scan (`Selection::ExcludeRoaring`, page-skippable).
    /// For PK-less (`PositionBased`) tables this is the long-standing behavior.
    /// For PK tables (`Int64Pk`/`RowConverterBased`) the position cache is empty
    /// under `deletion_mode: key` (no position vectors are ever written or
    /// loaded), so the provider is a no-op there and behavior is byte-identical
    /// to not attaching it; under `deletion_mode: position` it carries the
    /// located-row deletes while the `{Int64Pk,KeyBased}DeletionFilterExec` above
    /// the scan still handles the unlocated/key-based rows (dual application).
    pub(super) fn create_listing_options(
        vortex_format: &Arc<VortexFormat>,
        strategy: &PkDeletionStrategyWithCache,
        session_config: &SessionConfig,
    ) -> ListingOptions {
        let provider = Arc::new(PositionDeletionAccessPlanProvider::new(Arc::clone(
            strategy.position_cache(),
        )));
        let file_format: Arc<dyn FileFormat> =
            Arc::new(vortex_format.with_access_plan_provider(provider));
        ListingOptions::new(file_format).with_session_config_options(session_config)
    }

    /// Construct the snapshot directory URL string.
    ///
    /// For local paths, returns a file:// URL or path string.
    /// For S3 paths, returns the S3 URL with proper path components.
    ///
    /// # Arguments
    ///
    /// * `table_path` - The base path for the table (local path or S3 URL)
    /// * `table_id` - The unique identifier for the table
    /// * `snapshot_id` - The snapshot identifier
    pub(super) fn snapshot_dir_url(table_path: &str, table_id: &str, snapshot_id: &str) -> String {
        if table_path.starts_with("s3://") {
            // S3 URL: join path components with /
            let base = table_path.trim_end_matches('/');
            format!("{base}/{table_id}/{snapshot_id}/")
        } else {
            // Local path: use PathBuf and convert to URL string
            let path = Self::snapshot_dir_path(table_path, table_id, snapshot_id);
            Self::dir_to_url_string(&path)
        }
    }

    /// Ensure a snapshot directory exists, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub(crate) async fn ensure_snapshot_dir_exists(
        snapshot_dir: &std::path::Path,
    ) -> std::io::Result<()> {
        if !snapshot_dir.exists() {
            // Capture the parent before creation so we can sync it afterwards.
            let parent = snapshot_dir.parent().map(std::path::Path::to_path_buf);
            tokio::fs::create_dir_all(snapshot_dir).await?;

            // Write the *creation of the new snapshot directory itself* through
            // to the device (ordering tier). On POSIX, creating a subdirectory
            // updates the parent's directory metadata. Without syncing the
            // parent, a crash can make the new snapshot directory "disappear"
            // from the filesystem even though we later write files into it and
            // commit the catalog to point at it. This is the same requirement
            // we enforce for file creation, renames, and WAL marker removal
            // elsewhere in the code. Directory ordering tier (plain fsync on
            // macOS, full fsync on other platforms — see
            // `fsync_tier::ordering_sync_dir_std`): this runs once per staged
            // batch, and the macOS full-drive-flush tier bought nothing — see
            // `sync_snapshot_dir`.
            if let Some(parent) = parent {
                tokio::task::spawn_blocking(move || {
                    let f = std::fs::File::open(&parent)?;
                    super::fsync_tier::ordering_sync_dir_std(&f)
                })
                .await
                .map_err(std::io::Error::other)??;
            }
        }
        Ok(())
    }

    /// Clear the whole staging directory, removing any leftover files.
    ///
    /// Legacy whole-`_staging/` cleanup, formerly run at the start of each
    /// staged append; the live CDC pipeline uses the per-snapshot
    /// [`Self::clear_orphan_staging_dirs`] / [`Self::clear_staging_snapshot_dir`]
    /// variants instead. If the directory does not exist it is treated as
    /// already clean; the next staged append recreates its isolated child
    /// directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be cleaned or created.
    // Retained as the legacy whole-`_staging/` cleanup path (referenced by the
    // doc links above); the live CDC pipeline now uses the per-snapshot
    // `clear_orphan_staging_dirs` / `clear_staging_snapshot_dir` variants.
    #[expect(dead_code)]
    pub(crate) async fn clear_staging_dir(&self) -> Result<()> {
        // Fast path: if a previous append completed cleanly (or this is the
        // first write after open and no orphan files were present), staging is
        // known empty. Skipping the recursive delete / S3 List+DeletePrefix
        // removes a significant per-write cost for the common small-append
        // (inline) ingestion path, especially on S3.
        if !self.staging_may_have_files().load(Ordering::Acquire) {
            if self.table_metadata.path.starts_with("s3://") {
                return Ok(());
            }

            let staging_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                STAGING_DIR_NAME,
            );
            let mut entries = match tokio::fs::read_dir(&staging_dir).await {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            if entries.next_entry().await?.is_none() {
                return Ok(());
            }
        }

        if self.table_metadata.path.starts_with("s3://") {
            // S3: delete all objects under the staging prefix
            if let Some(prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? {
                self.delete_prefix_with_object_store(&prefix).await?;
            }
        } else {
            // Local FS: removing the directory is enough; absence is the clean
            // state and avoids provider-open races between remove/create cycles.
            let staging_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                STAGING_DIR_NAME,
            );
            match tokio::fs::remove_dir_all(&staging_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        // Staging is now known to be empty.
        self.staging_may_have_files()
            .store(false, Ordering::Release);
        Ok(())
    }

    /// Clear only the *orphan* staging entries — children of `_staging/` whose
    /// id is not registered as an in-flight append, with the in-flight check
    /// re-evaluated per entry at removal time.
    ///
    /// Recovery's tail cleanup used to call [`Self::clear_staging_dir`], which
    /// removes the whole staging root. That is destructive under concurrency:
    /// an append that registers between recovery's "no in-flight appends"
    /// check and the recursive delete loses its staged files mid-move
    /// (observed live as an ENOENT inside the pipelined CDC finalize, which
    /// permanently wedged the table's changes stream). Per-entry removal with
    /// a per-entry re-check bounds the blast radius to genuinely orphaned
    /// entries regardless of interleaving. Loose non-directory files (pre-WAL
    /// leftovers) belong to no append and are always removed.
    ///
    /// On S3 the staging prefix is cleared as a whole (no per-entry listing) —
    /// the lock exclusion in `ensure_no_incomplete_write` is the guard there.
    pub(crate) async fn clear_orphan_staging_dirs(&self) -> Result<()> {
        if self.table_metadata.path.starts_with("s3://") {
            if let Some(prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? {
                self.delete_prefix_with_object_store(&prefix).await?;
            }
            self.staging_may_have_files()
                .store(false, Ordering::Release);
            return Ok(());
        }

        let staging_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            STAGING_DIR_NAME,
        );
        let mut entries = match tokio::fs::read_dir(&staging_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.staging_may_have_files()
                    .store(false, Ordering::Release);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let mut kept_any = false;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let id = name.to_string_lossy();
            let is_dir = entry.file_type().await.is_ok_and(|t| t.is_dir());
            if is_dir && self.staging_append_is_inflight(&id) {
                kept_any = true;
                continue;
            }
            let path = entry.path();
            let removal = if is_dir {
                tokio::fs::remove_dir_all(&path).await
            } else {
                tokio::fs::remove_file(&path).await
            };
            match removal {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        self.staging_may_have_files()
            .store(kept_any, Ordering::Release);
        Ok(())
    }

    /// Clear one isolated staging snapshot directory.
    ///
    /// CDC pipeline Stage A uses a unique child under `_staging/` so a later
    /// burst can write its staged files without deleting a prior burst that is
    /// still waiting for Stage B. The legacy `_staging/` path keeps the old
    /// whole-directory cleanup semantics through [`Self::clear_staging_dir`].
    pub(crate) async fn clear_staging_snapshot_dir(&self, staging_snapshot_id: &str) -> Result<()> {
        if self.table_metadata.path.starts_with("s3://") {
            if let Some(prefix) = self.snapshot_object_store_prefix(staging_snapshot_id)? {
                self.delete_prefix_with_object_store(&prefix).await?;
            }
        } else {
            let staging_dir = Self::snapshot_dir_path(
                &self.table_metadata.path,
                &self.table_metadata.table_id,
                staging_snapshot_id,
            );
            match tokio::fs::remove_dir_all(&staging_dir).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(())
    }

    /// Move all files from the staging directory into the current snapshot directory.
    ///
    /// On local filesystems `rename()` is used, which is atomic on the same filesystem
    /// (staging and snapshot dirs share `{table_path}/{table_id}/`).
    ///
    /// On S3, files are copied to the current snapshot prefix first, then the staging
    /// originals are deleted (copy-all-then-delete-all ordering to avoid data loss if
    /// the operation is interrupted).
    ///
    /// # Errors
    ///
    /// Returns an error if any file move/copy fails.
    pub(crate) async fn move_staged_files_to_current_snapshot(
        &self,
        staging_snapshot_id: &str,
    ) -> Result<()> {
        let target_snapshot = self.get_current_snapshot_id();

        self.move_staged_files_to_snapshot(staging_snapshot_id, &target_snapshot)
            .await
    }

    pub(crate) async fn move_staged_files_to_snapshot(
        &self,
        staging_snapshot_id: &str,
        target_snapshot_id: &str,
    ) -> Result<()> {
        if self.table_metadata.path.starts_with("s3://") {
            self.move_staging_files_s3(staging_snapshot_id, target_snapshot_id)
                .await
        } else {
            self.move_staging_files_local(staging_snapshot_id, target_snapshot_id)
                .await
        }
    }

    pub(super) fn record_current_snapshot_files_added(&self, file_count: usize) {
        if file_count == 0 {
            return;
        }

        self.new_files_since_last_compaction
            .fetch_add(file_count, Ordering::Relaxed);
    }

    /// Move staging files to the current snapshot on local filesystem.
    ///
    /// After all renames complete, the target snapshot directory is fsync'd so
    /// the rename operations are durable across a power-loss restart. Without
    /// this, the staging WAL could be removed (in the caller's next step)
    /// while individual renames are still only in the page cache — a crash
    /// would then leave the catalog blind to staged files that "should" be in
    /// the snapshot.
    pub(super) async fn move_staging_files_local(
        &self,
        staging_snapshot_id: &str,
        current_snapshot: &str,
    ) -> Result<()> {
        let staging_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            staging_snapshot_id,
        );
        let target_dir = Self::snapshot_dir_path(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            current_snapshot,
        );

        // Ensure target directory exists
        Self::ensure_snapshot_dir_exists(&target_dir).await?;

        let mut entries = tokio::fs::read_dir(&staging_dir).await?;
        // Names of the data files actually moved this call. Drives both the count
        // bookkeeping and the list-files-cache delta-apply (below).
        let mut moved_file_names: Vec<std::ffi::OsString> = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name();

            // Skip WAL bookkeeping files. The committed WAL (`_wal.json`) is
            // managed separately (removed after all data files have been
            // successfully moved). A leftover tmp (`_wal.json.tmp`) can be
            // present if a prior process crashed between writing the tmp and
            // renaming it into place — it never contained committed intent,
            // so just leave it for the next clear_staging_dir cycle rather
            // than promoting it into the snapshot.
            if file_name == STAGING_WAL_FILENAME || file_name == STAGING_WAL_TMP_FILENAME {
                continue;
            }

            let src = staging_dir.join(&file_name);
            let dst = target_dir.join(&file_name);

            tokio::fs::rename(&src, &dst).await?;
            moved_file_names.push(file_name);
        }

        let moved_count = moved_file_names.len();
        tracing::debug!(
            "Moved {moved_count} file(s) from staging to snapshot {current_snapshot} for table {table_name}",
            table_name = self.table_metadata.table_name,
        );

        // Durability: fsync the target snapshot directory so the rename
        // operations are persisted before the caller removes the staging WAL.
        // Without this, a power loss after WAL removal could leave the snapshot
        // directory missing files that were "moved" in the page cache but
        // never written through to disk. Skipped when `moved_count == 0` (no
        // renames happened, so no dir entry change to flush) — this is the
        // single source of truth for the post-move dir fsync; a previous
        // revision accidentally issued two back-to-back fsyncs of the same
        // directory, which doubled the per-commit fsync cost on local FS.
        if moved_count > 0 {
            Self::sync_snapshot_dir(&target_dir).await?;
            self.record_current_snapshot_files_added(moved_count);

            // Record the moved files' ObjectMeta in the side-channel so the
            // caller's `publish_current_snapshot_files_changed_under_held_fence`
            // can delta-apply them onto the list-files cache instead of evicting
            // the whole directory listing. Best-effort: if stat fails for any
            // file we skip the side-channel entirely (leaving `None`), so the
            // publish falls back to a full eviction + re-LIST — never wrong, just
            // not incremental.
            //
            // ONLY when the move target is the live current snapshot. A
            // compaction/overwrite move targets a not-yet-current snapshot and is
            // followed by `refresh_listing_table_under_held_fence` (which evicts),
            // NOT this delta path — recording its files would let a later
            // current-snapshot publish apply the WRONG snapshot's additions.
            if self.get_current_snapshot_id() == current_snapshot {
                let snapshot_dir_url = Self::snapshot_dir_url(
                    &self.table_metadata.path,
                    &self.table_metadata.table_id,
                    current_snapshot,
                );
                if let Some(metas) = self
                    .stat_moved_files_as_object_metas(
                        &snapshot_dir_url,
                        &target_dir,
                        &moved_file_names,
                    )
                    .await
                {
                    *self.last_moved_snapshot_files.lock() =
                        Some((current_snapshot.to_string(), metas));
                }
            }
        }

        Ok(())
    }

    /// Build the [`ObjectMeta`] entries for `moved_file_names` (just renamed into
    /// `target_dir`) for the list-files-cache delta-apply. Each location is
    /// derived the same way `DataFusion`'s listing does — the parsed
    /// `ListingTableUrl` prefix joined with the file name — so the entries are
    /// byte-identical to what a fresh LIST would produce. `size`/`last_modified`
    /// come from a `stat` of the moved file. Returns `None` (forcing the caller
    /// to fall back to eviction) if the URL cannot be parsed or any stat fails.
    pub(super) async fn stat_moved_files_as_object_metas(
        &self,
        snapshot_dir_url: &str,
        target_dir: &std::path::Path,
        moved_file_names: &[std::ffi::OsString],
    ) -> Option<Vec<ObjectMeta>> {
        let table_url = ListingTableUrl::parse(snapshot_dir_url).ok()?;
        let prefix = table_url.prefix();
        let mut metas = Vec::with_capacity(moved_file_names.len());
        for file_name in moved_file_names {
            let file_name_str = file_name.to_str()?;
            let metadata = tokio::fs::metadata(target_dir.join(file_name)).await.ok()?;
            metas.push(ObjectMeta {
                location: prefix.clone().join(file_name_str),
                last_modified: metadata.modified().map_or_else(
                    |_| chrono::Utc::now(),
                    chrono::DateTime::<chrono::Utc>::from,
                ),
                size: metadata.len(),
                e_tag: None,
                version: None,
            });
        }
        Some(metas)
    }

    /// Move staging files to the current snapshot on S3.
    ///
    /// Uses copy-all-then-delete-all ordering: all files are copied to the target
    /// prefix first, then staging originals are deleted. If interrupted after copies
    /// but before deletes, data exists in both locations (safe — deduplicated by PK
    /// or idempotent for append-only tables).
    pub(super) async fn move_staging_files_s3(
        &self,
        staging_snapshot_id: &str,
        current_snapshot: &str,
    ) -> Result<()> {
        let config = self.require_object_store()?;

        let Some(staging_prefix) = self.snapshot_object_store_prefix(staging_snapshot_id)? else {
            return Ok(());
        };
        let Some(target_prefix) = self.snapshot_object_store_prefix(current_snapshot)? else {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: format!("Cannot compute S3 prefix for snapshot '{current_snapshot}'"),
            });
        };

        // List all objects in staging
        let objects: Vec<_> = config
            .store
            .list(Some(&staging_prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list staging objects for move",
                table: self.table_metadata.table_name.clone(),
                source: e,
            })?;

        if objects.is_empty() {
            return Ok(());
        }

        // The list-files cache keys files by the parsed `ListingTableUrl` prefix
        // (bucket-stripped), so build the delta-apply locations the same way the
        // scan's LIST would — `prefix.clone().join(relative)` — rather than from the
        // object-store move prefix, which may not be byte-identical.
        let cache_prefix = ListingTableUrl::parse(Self::snapshot_dir_url(
            &self.table_metadata.path,
            &self.table_metadata.table_id,
            current_snapshot,
        ))
        .ok()
        .map(|url| url.prefix().clone());

        let mut file_moves = Vec::with_capacity(objects.len());
        // ObjectMeta of each moved file for the list-files-cache delta-apply. Size
        // is preserved by the copy; e_tag/version are dropped (a fresh path → the
        // footer cache re-reads once, never serves stale metadata).
        let mut moved_metas: Vec<ObjectMeta> = Vec::with_capacity(objects.len());
        for meta in &objects {
            let relative = meta
                .location
                .as_ref()
                .strip_prefix(staging_prefix.as_ref())
                .ok_or_else(|| Error::Internal {
                    table: self.table_metadata.table_name.clone(),
                    message: format!(
                        "Staging object '{}' does not have expected prefix '{}'",
                        meta.location,
                        staging_prefix.as_ref(),
                    ),
                })?;

            // Skip the WAL bookkeeping files — they are managed separately
            // (the committed WAL is removed after all data files have been
            // successfully copied/deleted; a leftover tmp from a prior
            // crashed write is ignored and overwritten on the next attempt).
            if relative == STAGING_WAL_FILENAME || relative == STAGING_WAL_TMP_FILENAME {
                continue;
            }
            let target_path =
                ObjectStorePath::from(format!("{}{relative}", target_prefix.as_ref()));
            if let Some(prefix) = &cache_prefix {
                moved_metas.push(ObjectMeta {
                    location: prefix.clone().join(relative),
                    last_modified: meta.last_modified,
                    size: meta.size,
                    e_tag: None,
                    version: None,
                });
            }
            file_moves.push((meta.location.clone(), target_path));
        }

        // Phase 1: copy data objects to target prefix. Keep Phase 2 separate so
        // an interrupted move never deletes a staging original before every
        // target copy has succeeded.
        let store = Arc::clone(&config.store);
        let table_name = self.table_metadata.table_name.clone();
        stream::iter(file_moves.iter().cloned().map(Ok::<_, Error>))
            .try_for_each_concurrent(OBJECT_STORE_MOVE_CONCURRENCY, |(source, target)| {
            let store = Arc::clone(&store);
            let table_name = table_name.clone();
            async move {
                store.copy(&source, &target).await.map_err(|e| {
                    // On S3, a copy failure for a file listed in a leftover staging WAL
                    // is often caused by a partial/incomplete multipart upload (crash
                    // during a large Vortex file upload). The recovery will fail for
                    // this WAL (safe), but we emit a clear error to aid diagnosis.
                    Error::ObjectStore {
                        operation: "copy staging file to snapshot (may be partial multipart upload from interrupted write)",
                        table: table_name,
                        source: e,
                    }
                })
            }
            })
            .await?;

        // Phase 2: delete staging originals.
        let store = Arc::clone(&config.store);
        let table_name = self.table_metadata.table_name.clone();
        stream::iter(
            file_moves
                .iter()
                .map(|(source, _)| source.clone())
                .map(Ok::<_, Error>),
        )
        .try_for_each_concurrent(OBJECT_STORE_MOVE_CONCURRENCY, |source| {
            let store = Arc::clone(&store);
            let table_name = table_name.clone();
            async move {
                store.delete(&source).await.map_err(|e| Error::ObjectStore {
                    operation: "delete staging file after copy",
                    table: table_name,
                    source: e,
                })
            }
        })
        .await?;

        tracing::debug!(
            "Moved {} file(s) from staging to snapshot {current_snapshot} (S3) for table {}",
            file_moves.len(),
            self.table_metadata.table_name,
        );

        self.record_current_snapshot_files_added(file_moves.len());

        // Record the moved files for the list-files-cache delta-apply (see
        // `last_moved_snapshot_files`). Only when (a) the move target is the live
        // current snapshot — a compaction/overwrite move to a not-yet-current
        // snapshot is published by an evicting refresh, not this delta path — and
        // (b) we built a cache-aligned location for every moved file; otherwise
        // leave `None` so the publish falls back to a full eviction + re-LIST.
        if self.get_current_snapshot_id() == current_snapshot
            && !moved_metas.is_empty()
            && moved_metas.len() == file_moves.len()
        {
            *self.last_moved_snapshot_files.lock() =
                Some((current_snapshot.to_string(), moved_metas));
        }

        Ok(())
    }

    /// Sync a directory to ensure all files are durably written to disk.
    ///
    /// This is critical for crash safety: we must ensure all data files are
    /// persisted before updating the catalog metadata. Otherwise, a crash
    /// after catalog update but before data flush could result in a catalog
    /// pointing to incomplete/missing data files.
    ///
    /// # ACID Durability
    ///
    /// This function is part of the durability guarantee:
    /// 1. Write data files to new snapshot directory
    /// 2. Sync directory (this function) - ensures data is on disk
    /// 3. Update catalog atomically - commits the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be synced.
    pub(crate) async fn sync_snapshot_dir(snapshot_dir: &std::path::Path) -> CatalogResult<()> {
        let snapshot_dir = snapshot_dir.to_path_buf();
        let dir_display = snapshot_dir.display().to_string();
        tokio::task::spawn_blocking(move || {
            // Open the directory and flush its entries with the directory
            // ORDERING tier (`fsync_tier::ordering_sync_dir_std`), not
            // full-tier `sync_all`.
            //
            // Durability-tier rationale (matters on macOS): Rust's `sync_all`
            // AND `sync_data` both map to `fcntl(F_FULLFSYNC)` on Apple
            // targets — a full drive-cache flush measured at ~4-5 ms per call
            // — while plain `fsync(2)` is ~66 µs. Plain fsync is the macOS
            // durability tier SQLite (synchronous=NORMAL), DuckDB, and
            // PostgreSQL all default to. On non-macOS platforms the directory
            // helper issues a full `fsync` (dirent flushing under plain
            // `fdatasync` is implementation-defined; on Linux the two are
            // equivalent for directories) — behavior is effectively unchanged
            // there.
            //
            // F_FULLFSYNC here would be strictly stronger than the visibility
            // commit it protects: the metastore runs SQLite with
            // `journal_mode=WAL, synchronous=NORMAL` and no `fullfsync`
            // pragma, so the catalog transaction that makes these files
            // visible is itself only plain-fsync durable on macOS (NORMAL
            // doesn't even fsync at every commit). A power-loss window that
            // loses ordering-tier data files necessarily also loses the
            // catalog rows referencing them — there is no inconsistent state
            // a full flush here would prevent. Paying 5-7 F_FULLFSYNCs per
            // staged CDC batch (~25 ms of pure barrier latency) bought no
            // additional end-to-end durability; it was the dominant fixed
            // cost of small staged upserts. Full details + measurements in
            // `provider/fsync_tier.rs`.
            let dir = std::fs::File::open(&snapshot_dir)
                .map_err(|source| CatalogError::IoError { source })?;
            super::fsync_tier::ordering_sync_dir_std(&dir)
                .map_err(|source| CatalogError::IoError { source })?;
            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|e| Error::TaskPanicked {
            table: dir_display,
            source: e,
        })?
    }

    /// Cleanup old snapshot directories after a full refresh.
    ///
    /// For full refresh mode, after the new snapshot is written and the catalog is updated,
    /// old snapshot directories are no longer needed and can be physically deleted.
    ///
    /// This function performs blocking filesystem I/O and should be called from within
    /// `tokio::task::spawn_blocking` to avoid blocking the async runtime thread pool.
    ///
    /// # Arguments
    ///
    /// * `table_path` - Base path for the table
    /// * `table_id` - Table identifier
    /// * `current_snapshot_id` - The current (active) snapshot ID that should be kept
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot directories cannot be listed or deleted.
    ///
    /// # Blocking I/O Warning
    ///
    /// This function uses `std::fs` for filesystem operations and will block the calling thread.
    /// It must be called from within `tokio::task::spawn_blocking`.
    pub(super) fn cleanup_old_snapshots_blocking(
        table_path: &str,
        table_id: &str,
        current_snapshot_id: &str,
        protected_snapshot_ids: &HashSet<String>,
    ) -> CatalogResult<()> {
        let table_dir = std::path::PathBuf::from(table_path).join(table_id);

        // Check if table directory exists
        if !table_dir.exists() {
            return Ok(());
        }

        tracing::debug!(
            "Cleaning up old snapshots for table {} (keeping current={}, protected={})",
            table_id,
            current_snapshot_id,
            protected_snapshot_ids.len()
        );

        // Parse the current snapshot UUID7 unix timestamp. Directories with a
        // UUID7 timestamp >= this might be in-flight writes that started after compaction committed
        // but haven't added themselves to `protected_snapshots` yet.
        let current_snapshot_unix = uuid::Uuid::parse_str(current_snapshot_id)
            .ok()
            .and_then(|u| u.get_timestamp())
            .map(|ts| ts.to_unix());

        if current_snapshot_unix.is_none() {
            tracing::warn!(
                "Unable to extract UUID7 timestamp from current snapshot '{}'; in-flight write protection disabled",
                current_snapshot_id
            );
        }

        // Read all entries in the table directory using blocking I/O
        let entries =
            std::fs::read_dir(&table_dir).map_err(|source| CatalogError::IoError { source })?;

        let mut deleted_count = 0;
        for entry_result in entries {
            let entry = entry_result.map_err(|source| CatalogError::IoError { source })?;
            let path = entry.path();

            // Only process directories (snapshots)
            if !path.is_dir() {
                continue;
            }

            // Get the snapshot ID (directory name)
            let Some(snapshot_id) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            // Skip the current snapshot, protected snapshots, and the staging directory
            if snapshot_id == current_snapshot_id
                || protected_snapshot_ids.contains(snapshot_id)
                || snapshot_id == STAGING_DIR_NAME
            {
                tracing::trace!(
                    "Keeping snapshot: {} (current, protected, or staging)",
                    snapshot_id
                );
                continue;
            }

            // Skip directories whose UUID7 timestamp is >= the current snapshot.
            // These might be in-flight writes. Deleting them would cause the writer's final rename to fail with ENOENT.
            if let Some(current_unix) = current_snapshot_unix {
                let dir_unix = uuid::Uuid::parse_str(snapshot_id)
                    .ok()
                    .and_then(|u| u.get_timestamp())
                    .map(|ts| ts.to_unix());

                match dir_unix {
                    Some(ts) if ts >= current_unix => {
                        tracing::trace!("Keeping snapshot: {snapshot_id} (newer than current)");
                        continue;
                    }
                    None => {
                        tracing::warn!(
                            "Unable to extract UUID7 timestamp from snapshot '{snapshot_id}'",
                        );
                    }
                    _ => {}
                }
            }

            // Delete the old snapshot directory using blocking I/O
            tracing::info!(
                "Deleting old snapshot directory for table {}: {}",
                table_id,
                snapshot_id
            );

            std::fs::remove_dir_all(&path).map_err(|source| CatalogError::IoError { source })?;

            deleted_count += 1;
        }

        if deleted_count > 0 {
            tracing::info!(
                "Cleaned up {} old snapshot(s) for table {}",
                deleted_count,
                table_id
            );
        } else {
            tracing::debug!("No old snapshots to cleanup for table {}", table_id);
        }

        Ok(())
    }
}
