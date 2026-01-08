/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use app::spicepod::component::runtime::Scheduler as SchedulerConfig;
use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, UpdateVersion};
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use runtime_secrets::{ExposeSecret, Secrets, get_params_with_secrets};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::Runtime;

const CLUSTER_SCHEMA_VERSION: u32 = 1;
const SCHEDULER_SCHEMA_VERSION: u32 = 1;
const DEFAULT_TTL_MS: u64 = 30_000;
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_DIVISOR: u64 = 3;
const CLOCK_SKEW_TOLERANCE_MS: u64 = 5_000;
const MAX_CONDITIONAL_ATTEMPTS: usize = 5;
const BACKOFF_BASE_MS: u64 = 200;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse scheduler state location {location}: {source}"))]
    InvalidStateLocation { location: String, source: url::ParseError },

    #[snafu(display(
        "Failed to initialize scheduler state object store for {location}: {source}"
    ))]
    ObjectStoreInit {
        location: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Scheduler registration record already exists for {scheduler_id} and is still active"
    ))]
    SchedulerIdConflict { scheduler_id: String },

    #[snafu(display("Missing scheduler advertise address for registration"))]
    MissingAdvertiseAddress,

    #[snafu(display("Failed to read scheduler state from object store: {source}"))]
    ObjectStoreRead { source: ObjectStoreError },

    #[snafu(display("Failed to write scheduler state to object store: {source}"))]
    ObjectStoreWrite { source: ObjectStoreError },

    #[snafu(display("Failed to serialize scheduler state: {source}"))]
    SerializeState { source: serde_json::Error },

    #[snafu(display("Failed to deserialize scheduler state: {source}"))]
    DeserializeState { source: serde_json::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerRecord {
    pub schema_version: u32,
    pub advertise_address: String,
    pub grpc_address: String,
    pub http_address: String,
    pub started_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub ttl_ms: u64,
    pub build_version: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterMetadata {
    schema_version: u32,
    created_at_ms: u64,
}

pub type SchedulerPeers = HashMap<String, SchedulerRecord>;

pub struct SchedulerRegistry {
    peers: Arc<RwLock<SchedulerPeers>>,
}

impl SchedulerRegistry {
    #[must_use]
    pub fn peers(&self) -> Arc<RwLock<SchedulerPeers>> {
        Arc::clone(&self.peers)
    }
}

struct SchedulerRegistryRunner {
    store: Arc<dyn ObjectStore>,
    scheduler_id: String,
    record_path: Path,
    metadata_path: Path,
    schedulers_prefix: Path,
    record: SchedulerRecord,
    update_version: Option<UpdateVersion>,
    peers: Arc<RwLock<SchedulerPeers>>,
}

pub async fn start_scheduler_registry(
    rt: Arc<Runtime>,
    config: &SchedulerConfig,
    cancel: CancellationToken,
    peers: Arc<RwLock<SchedulerPeers>>,
) -> Result<()> {
    let state_url = build_state_url(rt.secrets(), config).await?;
    let (store, base_prefix) = build_object_store(rt, &state_url)?;

    let advertise_host = rt
        .datafusion()
        .cluster_config
        .node_advertise_address()
        .ok_or(Error::MissingAdvertiseAddress)?;

    let scheduler_id = format!(
        "{advertise_host}:{}",
        rt.datafusion().cluster_config.node_bind_address().port()
    );

    let record = SchedulerRecord {
        schema_version: SCHEDULER_SCHEMA_VERSION,
        advertise_address: scheduler_id.clone(),
        grpc_address: format!(
            "{advertise_host}:{}",
            rt.config().flight_bind_address.port()
        ),
        http_address: format!(
            "{advertise_host}:{}",
            rt.config().http_bind_address.port()
        ),
        started_at_ms: now_ms()?,
        last_heartbeat_ms: now_ms()?,
        ttl_ms: DEFAULT_TTL_MS,
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        labels: HashMap::new(),
    };

    let runner = SchedulerRegistryRunner::new(
        store,
        base_prefix,
        scheduler_id,
        record,
        Arc::clone(&peers),
    )?;

    runner.run(cancel).await
}

impl SchedulerRegistryRunner {
    fn new(
        store: Arc<dyn ObjectStore>,
        base_prefix: String,
        scheduler_id: String,
        record: SchedulerRecord,
        peers: Arc<RwLock<SchedulerPeers>>,
    ) -> Result<Self> {
        let metadata_path = join_path(&base_prefix, "metadata/cluster.json");
        let record_path = join_path(&base_prefix, &format!("schedulers/{scheduler_id}.json"));
        let schedulers_prefix = join_path(&base_prefix, "schedulers");

        Ok(Self {
            store,
            scheduler_id,
            record_path,
            metadata_path,
            schedulers_prefix,
            record,
            update_version: None,
            peers,
        })
    }

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        self.ensure_cluster_metadata().await?;
        self.bootstrap_record().await?;

        let heartbeat_interval = Duration::from_millis(
            self.record
                .ttl_ms
                .saturating_div(HEARTBEAT_DIVISOR)
                .max(1),
        );
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        let mut discovery = tokio::time::interval(DISCOVERY_INTERVAL);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    self.delete_record().await;
                    break;
                }
                _ = heartbeat.tick() => {
                    if let Err(err) = self.heartbeat().await {
                        tracing::warn!("Scheduler heartbeat failed: {err}");
                    }
                }
                _ = discovery.tick() => {
                    if let Err(err) = self.refresh_peers().await {
                        tracing::warn!("Scheduler discovery failed: {err}");
                    }
                }
            }
        }

        Ok(())
    }

    async fn ensure_cluster_metadata(&self) -> Result<()> {
        let metadata = ClusterMetadata {
            schema_version: CLUSTER_SCHEMA_VERSION,
            created_at_ms: now_ms()?,
        };
        let payload = serde_json::to_vec(&metadata).context(SerializeStateSnafu)?;

        let put_result = self
            .store
            .put_opts(
                &self.metadata_path,
                payload.into(),
                PutOptions::from(PutMode::Create),
            )
            .await;

        match put_result {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::AlreadyExists { .. }) => Ok(()),
            Err(err) => Err(Error::ObjectStoreWrite { source: err }),
        }
    }

    async fn bootstrap_record(&mut self) -> Result<()> {
        let payload = serde_json::to_vec(&self.record).context(SerializeStateSnafu)?;

        match self
            .store
            .put_opts(
                &self.record_path,
                payload.clone().into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(result) => {
                self.update_version = Some(UpdateVersion::from(result));
                return Ok(());
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {}
            Err(err) => return Err(Error::ObjectStoreWrite { source: err }),
        }

        let existing = self.read_record_with_meta().await?;
        let is_stale = record_is_stale(&existing.record, now_ms()?);

        if !is_stale {
            return Err(Error::SchedulerIdConflict {
                scheduler_id: self.scheduler_id.clone(),
            });
        }

        self.update_version = Some(existing.version);
        self.conditional_update(payload).await
    }

    async fn heartbeat(&mut self) -> Result<()> {
        self.record.last_heartbeat_ms = now_ms()?;
        let payload = serde_json::to_vec(&self.record).context(SerializeStateSnafu)?;
        self.conditional_update(payload).await
    }

    async fn conditional_update(&mut self, payload: Vec<u8>) -> Result<()> {
        let mut attempt = 0;
        loop {
            if attempt >= MAX_CONDITIONAL_ATTEMPTS {
                let source = Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Conditional update failed after retries",
                ));
                return Err(Error::ObjectStoreWrite {
                    source: ObjectStoreError::Precondition {
                        path: self.record_path.to_string(),
                        source,
                    },
                });
            }
            attempt += 1;

            if self.update_version.is_none() {
                self.update_version = Some(self.read_record_with_meta().await?.version);
            }

            let update_version = self
                .update_version
                .clone()
                .unwrap_or(UpdateVersion { e_tag: None, version: None });

            let put_result = self
                .store
                .put_opts(
                    &self.record_path,
                    payload.clone().into(),
                    PutOptions::from(PutMode::Update(update_version)),
                )
                .await;

            match put_result {
                Ok(result) => {
                    self.update_version = Some(UpdateVersion::from(result));
                    return Ok(());
                }
                Err(ObjectStoreError::Precondition { .. }) => {
                    self.update_version = None;
                    backoff_sleep(attempt).await;
                }
                Err(err) => return Err(Error::ObjectStoreWrite { source: err }),
            }
        }
    }

    async fn refresh_peers(&self) -> Result<()> {
        let mut records = HashMap::new();
        let mut stream = self.store.list(Some(&self.schedulers_prefix));
        let now = now_ms()?;
        while let Some(entry) = stream.next().await {
            let meta = entry.map_err(|source| Error::ObjectStoreRead { source })?;
            let bytes = self
                .store
                .get(&meta.location)
                .await
                .map_err(|source| Error::ObjectStoreRead { source })?
                .bytes()
                .await
                .map_err(|source| Error::ObjectStoreRead { source })?;
            let record: SchedulerRecord =
                serde_json::from_slice(&bytes).context(DeserializeStateSnafu)?;

            if !record_is_stale(&record, now) {
                records.insert(record.advertise_address.clone(), record);
            }
        }

        let mut peers = self.peers.write().await;
        let previous: HashSet<String> = peers.keys().cloned().collect();
        let next: HashSet<String> = records.keys().cloned().collect();

        let added: Vec<_> = next.difference(&previous).cloned().collect();
        let removed: Vec<_> = previous.difference(&next).cloned().collect();

        if !added.is_empty() || !removed.is_empty() {
            tracing::info!(
                "Scheduler membership updated; added={}, removed={}",
                added.len(),
                removed.len()
            );
        }

        *peers = records;
        Ok(())
    }

    async fn delete_record(&self) {
        if let Err(err) = self.store.delete(&self.record_path).await {
            tracing::warn!("Failed to delete scheduler record: {err}");
        }
    }

    async fn read_record_with_meta(&self) -> Result<RecordWithVersion> {
        let result = self
            .store
            .get(&self.record_path)
            .await
            .map_err(|source| Error::ObjectStoreRead { source })?;
        let meta = result.meta.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|source| Error::ObjectStoreRead { source })?;
        let record: SchedulerRecord =
            serde_json::from_slice(&bytes).context(DeserializeStateSnafu)?;
        let version = UpdateVersion {
            e_tag: meta.e_tag,
            version: meta.version,
        };

        Ok(RecordWithVersion { record, version })
    }
}

struct RecordWithVersion {
    record: SchedulerRecord,
    version: UpdateVersion,
}

fn build_object_store(
    rt: Arc<Runtime>,
    url: &Url,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let registry = SpiceObjectStoreRegistry::new(rt.tokio_io_runtime());
    let store = registry
        .get_store(url)
        .map_err(|source| Error::ObjectStoreInit {
            location: url.to_string(),
            source,
        })?;
    let base_prefix = url.path().trim_matches('/').to_string();
    Ok((store, base_prefix))
}

async fn build_state_url(
    rt_secrets: Arc<RwLock<Secrets>>,
    config: &SchedulerConfig,
) -> Result<Url> {
    let mut url = Url::parse(&config.state_location).context(InvalidStateLocationSnafu {
        location: config.state_location.clone(),
    })?;

    let params = config
        .params
        .as_ref()
        .map(|params| params.as_string_map())
        .unwrap_or_default();
    let params = get_params_with_secrets(rt_secrets, &params).await;

    let fragment = build_params_fragment(url.scheme(), &params);
    if !fragment.is_empty() {
        url.set_fragment(Some(&fragment));
    }

    Ok(url)
}

fn build_params_fragment(scheme: &str, params: &HashMap<String, SecretString>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());

    for (key, value) in params {
        let normalized_key = if scheme == "s3" {
            normalize_s3_param(key)
        } else {
            key.as_str()
        };
        serializer.append_pair(normalized_key, value.expose_secret());
    }

    serializer.finish()
}

fn normalize_s3_param(key: &str) -> &str {
    match key {
        "s3_access_key_id" | "aws_access_key_id" => "key",
        "s3_secret_access_key" | "aws_secret_access_key" => "secret",
        "s3_region" | "aws_region" => "region",
        "s3_session_token" | "aws_session_token" => "session_token",
        "s3_endpoint" | "aws_endpoint" => "endpoint",
        "s3_allow_http" => "allow_http",
        "s3_client_timeout" => "client_timeout",
        "s3_auth" => "auth",
        _ => key,
    }
}

fn join_path(prefix: &str, suffix: &str) -> Path {
    if prefix.is_empty() {
        Path::from(suffix)
    } else {
        Path::from(format!("{prefix}/{suffix}"))
    }
}

fn record_is_stale(record: &SchedulerRecord, now_ms: u64) -> bool {
    now_ms.saturating_sub(record.last_heartbeat_ms)
        > record
            .ttl_ms
            .saturating_add(CLOCK_SKEW_TOLERANCE_MS)
}

fn now_ms() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| Error::ObjectStoreRead {
            source: ObjectStoreError::Generic {
                store: "scheduler_registry",
                source: Box::new(source),
            },
        })?;
    u64::try_from(now.as_millis()).map_err(|source| Error::ObjectStoreRead {
        source: ObjectStoreError::Generic {
            store: "scheduler_registry",
            source: Box::new(source),
        },
    })
}

async fn backoff_sleep(attempt: usize) {
    let backoff = BACKOFF_BASE_MS.saturating_mul(2_u64.saturating_pow(attempt as u32));
    tokio::time::sleep(Duration::from_millis(backoff)).await;
}
