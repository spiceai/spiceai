/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use app::spicepod::component::runtime::Scheduler as SchedulerConfig;
use aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilder;
use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, UpdateVersion};
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use runtime_parameters::{ParameterSpec, Parameters};
use runtime_secrets::{Secrets, get_params_with_secrets};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use url::Url;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::Runtime;

const CLUSTER_SCHEMA_VERSION: u32 = 1;
const SCHEDULER_SCHEMA_VERSION: u32 = 1;
const DEFAULT_TTL_MS: u64 = 30_000;
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_DIVISOR: u64 = 3;
const CLOCK_SKEW_TOLERANCE_MS: u64 = 5_000;
const MAX_CONDITIONAL_ATTEMPTS: usize = 5;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to parse scheduler state location {location}: {source}"))]
    InvalidStateLocation {
        location: String,
        source: url::ParseError,
    },

    #[snafu(display("Failed to initialize scheduler state object store for {location}: {source}"))]
    ObjectStoreInit {
        location: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Failed to build S3 object store for scheduler state at {location}: {source}"
    ))]
    S3ObjectStoreInit {
        location: String,
        source: aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilderError,
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
    let state_url = Url::parse(&config.state_location).context(InvalidStateLocationSnafu {
        location: config.state_location.clone(),
    })?;
    let (store, base_prefix) = build_object_store(rt.as_ref(), &state_url, config).await?;

    let datafusion = rt.datafusion();
    let advertise_host = datafusion
        .cluster_config
        .node_advertise_address()
        .ok_or(Error::MissingAdvertiseAddress)?
        .to_string();

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
        http_address: format!("{advertise_host}:{}", rt.config().http_bind_address.port()),
        started_at_ms: now_ms()?,
        last_heartbeat_ms: now_ms()?,
        ttl_ms: DEFAULT_TTL_MS,
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        labels: HashMap::new(),
    };

    let runner = SchedulerRegistryRunner::new(
        store,
        &base_prefix,
        scheduler_id,
        record,
        Arc::clone(&peers),
    );

    runner.run(cancel).await
}

impl SchedulerRegistryRunner {
    fn new(
        store: Arc<dyn ObjectStore>,
        base_prefix: &str,
        scheduler_id: String,
        record: SchedulerRecord,
        peers: Arc<RwLock<SchedulerPeers>>,
    ) -> Self {
        let metadata_path = join_path(base_prefix, "metadata/cluster.json");
        let record_path = join_path(base_prefix, &format!("schedulers/{scheduler_id}.json"));
        let schedulers_prefix = join_path(base_prefix, "schedulers");

        Self {
            store,
            scheduler_id,
            record_path,
            metadata_path,
            schedulers_prefix,
            record,
            update_version: None,
            peers,
        }
    }

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        self.ensure_cluster_metadata().await?;
        self.bootstrap_record().await?;

        let heartbeat_interval =
            Duration::from_millis(self.record.ttl_ms.saturating_div(HEARTBEAT_DIVISOR).max(1));
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        let mut discovery = tokio::time::interval(DISCOVERY_INTERVAL);

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
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
            Ok(_) | Err(ObjectStoreError::AlreadyExists { .. }) => Ok(()),
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
        let mut backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_CONDITIONAL_ATTEMPTS))
            .build();

        loop {
            if self.update_version.is_none() {
                self.update_version = Some(self.read_record_with_meta().await?.version);
            }

            let update_version = self.update_version.clone().unwrap_or(UpdateVersion {
                e_tag: None,
                version: None,
            });

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
                    let Some(delay) = backoff.next_duration() else {
                        let source = Box::new(std::io::Error::other(
                            "Conditional update failed after retries",
                        ));
                        return Err(Error::ObjectStoreWrite {
                            source: ObjectStoreError::Precondition {
                                path: self.record_path.to_string(),
                                source,
                            },
                        });
                    };
                    tokio::time::sleep(delay).await;
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

static S3_PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    vec![
        ParameterSpec::component("region").secret(),
        ParameterSpec::component("endpoint").secret(),
        ParameterSpec::component("key").secret(),
        ParameterSpec::component("secret").secret(),
        ParameterSpec::component("session_token").secret(),
        ParameterSpec::component("auth")
            .description("Configures the authentication method for S3. Supported methods are: iam_role, key.")
            .default("iam_role")
            .one_of(&["iam_role", "key"])
            .secret(),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for S3 client."),
        ParameterSpec::runtime("allow_http")
            .description("Allow HTTP protocol for S3 endpoint."),
    ]
});

async fn build_object_store(
    rt: &Runtime,
    url: &Url,
    config: &SchedulerConfig,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    let base_prefix = url.path().trim_matches('/').to_string();
    let io_runtime = rt.tokio_io_runtime();

    let store: Arc<dyn ObjectStore> = if url.scheme() == "s3" {
        let params = config
            .params
            .as_ref()
            .map(spicepod::param::Params::as_string_map);
        let s3_params = build_s3_parameters(rt.secrets(), params.as_ref()).await;

        S3ObjectStoreBuilder::from_url(url, io_runtime)
            .map_err(|source| Error::S3ObjectStoreInit {
                location: url.to_string(),
                source,
            })?
            .with_secret_params(&s3_params.to_secret_map())
            .map_err(|source| Error::S3ObjectStoreInit {
                location: url.to_string(),
                source,
            })?
            .build()
            .await
            .map_err(|source| Error::S3ObjectStoreInit {
                location: url.to_string(),
                source,
            })?
    } else {
        let registry = SpiceObjectStoreRegistry::new(io_runtime);
        registry
            .get_store(url)
            .map_err(|source| Error::ObjectStoreInit {
                location: url.to_string(),
                source,
            })?
    };

    Ok((store, base_prefix))
}

async fn build_s3_parameters(
    secrets: Arc<RwLock<Secrets>>,
    params: Option<&HashMap<String, String>>,
) -> Parameters {
    let default_params = || Parameters::new(vec![], "s3", &S3_PARAMETERS);
    match params {
        Some(p) => {
            let secret_params = get_params_with_secrets(Arc::clone(&secrets), p).await;
            Parameters::try_new(
                "scheduler",
                secret_params.into_iter().collect(),
                "s3",
                secrets,
                &S3_PARAMETERS,
            )
            .await
            .unwrap_or_else(|_| default_params())
        }
        None => default_params(),
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
        > record.ttl_ms.saturating_add(CLOCK_SKEW_TOLERANCE_MS)
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
