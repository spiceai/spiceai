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

//! A cluster node acts only as the executor its own certificate registered:
//! the scheduler refuses a control stream, a partition allocation, or a Ballista
//! executor call that names another node's executor id, and the named executor
//! stays registered and serving.
//!
//! Reproduces the cross-node impersonation of the `cluster-trust` security
//! findings: before the identity binding, a node holding any certificate the
//! cluster CA issued could claim another executor's id, evict it, and redirect
//! its traffic.
//!
//! A node's certificate must also name the address it advertises
//! (spiceai/spiceai#16481): the scheduler refuses a registration at any other
//! address, an executor whose certificate does not name its address exits
//! before registering, and a reloaded certificate that drops the address is not
//! loaded.

use std::time::Duration;

use app::AppBuilder;
use arrow::array::RecordBatch;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, GetJobStatusParams, HeartBeatParams, RegisterExecutorParams,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, Issuer, KeyPair, KeyUsagePurpose};
use runtime_cluster::peer::{
    MismatchSite, PeerIdentity, advertise_address_mismatch_message,
    registration_address_mismatch_message,
};
use runtime_proto::{
    AllocateInitialPartitionsRequest, ExecutorControlMessage, ExecutorHeartbeat,
    cluster_service_client::ClusterServiceClient, executor_control_message::Message,
};
use runtime_tls::{ReloadScope, reload::reload_count_for_tests};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use spicepod::component::dataset::Dataset;
use test_framework::pki::PkiConfig;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

const NAMES_CSV: &str = include_str!("../acceleration/data/names.csv");

/// An address no harness certificate names: the harness issues every node a
/// certificate naming `localhost` and `127.0.0.1` only.
const UNNAMED_ADDRESS: &str = "10.0.0.7";

/// A TLS channel to the scheduler's cluster port, presenting the certificate the
/// harness PKI issues for `node_name` — a node identity of the test's choosing.
async fn channel_as(harness: &ClusterHarness, node_name: &str) -> Channel {
    let pki = harness
        .cluster_pki()
        .expect("harness runs with cluster mTLS");
    let cert = pki
        .create_client_cert(node_name)
        .expect("issue a node certificate");
    let ca = std::fs::read(&pki.ca_cert_path).expect("read CA");
    let node_cert = std::fs::read(&cert.cert_path).expect("read node cert");
    let node_key = std::fs::read(&cert.key_path).expect("read node key");
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(node_cert, node_key))
        // The scheduler's certificate names `localhost`.
        .domain_name("localhost");
    let host_port = harness.scheduler_cluster_address();
    let port = host_port.rsplit_once(':').expect("host:port").1;
    Endpoint::from_shared(format!("https://localhost:{port}"))
        .expect("endpoint")
        .tls_config(tls)
        .expect("tls config")
        .connect()
        .await
        .expect("connect to scheduler cluster port")
}

/// A TLS channel to `address` that verifies the server's certificate against
/// `server_name`, presenting `node_name`'s certificate.
async fn connect_verifying(
    pki: &PkiConfig,
    node_name: &str,
    address: std::net::SocketAddr,
    server_name: &str,
) -> Result<Channel, tonic::transport::Error> {
    let cert = pki
        .create_client_cert(node_name)
        .expect("issue a node certificate");
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(
            std::fs::read(&pki.ca_cert_path).expect("read CA"),
        ))
        .identity(Identity::from_pem(
            std::fs::read(&cert.cert_path).expect("read node cert"),
            std::fs::read(&cert.key_path).expect("read node key"),
        ))
        .domain_name(server_name);
    Endpoint::from_shared(format!("https://{address}"))
        .expect("endpoint")
        .tls_config(tls)
        .expect("tls config")
        .connect()
        .await
}

/// Issues `node_name` a certificate from the harness CA that names only
/// `hosts`, unlike the harness's own certificates. Returns the certificate and
/// key as PEM.
fn certificate_naming_only(pki: &PkiConfig, node_name: &str, hosts: &[&str]) -> (String, String) {
    let ca_key =
        KeyPair::from_pem(&std::fs::read_to_string(&pki.ca_key_path).expect("read CA key"))
            .expect("parse CA key");
    let issuer = Issuer::from_ca_cert_pem(
        &std::fs::read_to_string(&pki.ca_cert_path).expect("read CA"),
        ca_key,
    )
    .expect("CA issuer");
    let mut params = CertificateParams::new(
        hosts
            .iter()
            .map(|host| (*host).to_string())
            .collect::<Vec<_>>(),
    )
    .expect("certificate params");
    params
        .distinguished_name
        .push(DnType::CommonName, node_name);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    let key = KeyPair::generate().expect("generate key");
    let cert = params.signed_by(&key, &issuer).expect("sign certificate");
    (cert.pem(), key.serialize_pem())
}

/// The subject a node's certificate file gives it, as refusals print it.
fn subject_of(cert_path: &std::path::Path) -> String {
    let der = CertificateDer::from_pem_file(cert_path).expect("read certificate");
    PeerIdentity::from_certificate(&der)
        .expect("parse certificate")
        .subject()
        .to_string()
}

/// Writes `contents` to `path` by renaming a sibling file over it, the way a
/// certificate manager rotates material.
fn replace_file(path: &std::path::Path, contents: &str) {
    let staged = path.with_extension("staged");
    std::fs::write(&staged, contents).expect("stage file");
    std::fs::rename(&staged, path).expect("replace file");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_cannot_act_as_an_executor_another_node_registered() -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let harness = ClusterHarness::builder().executors(1).start().await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                // The one executor the harness started registers as `executor0`.
                let victim = "executor0";
                assert_eq!(
                    harness.registered_executors().await?,
                    vec![(
                        victim.to_string(),
                        format!(
                            "127.0.0.1:{}",
                            harness.executors[0]
                                .datafusion()
                                .cluster_config
                                .node_bind_address()
                                .port()
                        ),
                    )],
                );

                // An intruder node holds a CA-issued certificate of its own.
                let intruder_channel = channel_as(&harness, "intruder").await;

                // (1) A control stream that claims the victim's id is refused
                //     before anything it carries is acted on.
                let mut cluster = ClusterServiceClient::new(intruder_channel.clone());
                let claim = victim.to_string();
                let outbound = tokio_stream::iter(vec![ExecutorControlMessage {
                    executor_id: claim.clone(),
                    process_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                    message: Some(Message::Heartbeat(ExecutorHeartbeat { timestamp_ms: 1 })),
                }]);
                let mut inbound = cluster
                    .control_stream(outbound)
                    .await
                    .expect("the stream opens; the refusal arrives on it")
                    .into_inner();
                match inbound.next().await {
                    Some(Err(status)) => {
                        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
                        assert!(
                            status.message().contains(victim)
                                && status.message().contains("registered"),
                            "refusal names the executor and the cause: {}",
                            status.message()
                        );
                    }
                    other => panic!("expected a permission-denied refusal, got {other:?}"),
                }

                // (2) A partition allocation that claims the victim's id, with an
                //     attacker endpoint, is refused.
                let allocate = cluster
                    .allocate_initial_partitions(AllocateInitialPartitionsRequest {
                        executor_url: "https://127.0.0.1:1".to_string(),
                        executor_id: claim.clone(),
                        process_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                    })
                    .await;
                let status = allocate.expect_err("partition allocation is refused");
                assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

                // (3) A Ballista executor-half call that names the victim — the
                //     executor's own poll/heartbeat path — is refused. This is
                //     the identity check on the Ballista wrapper, distinct from
                //     the client-half refusal in (4).
                let mut scheduler = SchedulerGrpcClient::new(intruder_channel);
                let heartbeat = scheduler
                    .heart_beat_from_executor(HeartBeatParams {
                        executor_id: claim.clone(),
                        metrics: Vec::new(),
                        status: None,
                        metadata: None,
                    })
                    .await;
                let status =
                    heartbeat.expect_err("a heartbeat naming another node's executor is refused");
                assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

                // (4) The Ballista client half is not served on this port at all.
                let job = scheduler
                    .get_job_status(GetJobStatusParams {
                        job_id: "no-such-job".to_string(),
                    })
                    .await;
                let status = job.expect_err("the job methods are not served on the cluster port");
                assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

                // The victim stayed registered throughout.
                let registered = harness
                    .scheduler_executor_registry()
                    .expect("scheduler has an executor registry")
                    .connected_executors()
                    .await;
                assert_eq!(registered, vec![victim.to_string()]);
                assert_eq!(harness.registered_executors().await?.len(), 1);
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn an_executor_registers_only_at_an_address_its_certificate_names()
-> Result<(), anyhow::Error> {
    crate::utils::require_process_per_test(
        "this test installs the process-global meter provider to observe shuffle transfers",
    );
    let registry = &*crate::utils::PROMETHEUS;
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let tempdir = tempfile::tempdir()?;
            let csv = tempdir.path().join("names.csv");
            std::fs::write(&csv, NAMES_CSV)?;
            let mut app = AppBuilder::new("advertise_address")
                .with_dataset(Dataset::new(format!("file:{}", csv.display()), "names"))
                .build();
            app.runtime
                .caching
                .sql_results
                .get_or_insert_default()
                .enabled = false;
            let mut harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;

                // (1) Executors whose certificates name their address register
                //     and serve: a distributed query, its shuffle read through
                //     the executors' Flight servers, returns the rows the
                //     scheduler computes alone.
                let registered: Vec<String> = harness
                    .registered_executors()
                    .await?
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();
                assert_eq!(registered, vec!["executor0", "executor1"]);
                {
                    let state = harness.scheduler.datafusion().ctx.state_ref();
                    let mut state = state.write();
                    let config = state.config_mut();
                    *config = config
                        .clone()
                        .with_ballista_shuffle_reader_force_remote_read(true);
                }
                let sql = "SELECT city, count(*) AS n, sum(score) AS total FROM names GROUP BY city ORDER BY city";
                let expected = harness.query(sql).await?;
                let actual = harness.query_distributed(sql, "advertise_address_query").await?;
                assert!(expected.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
                assert_eq!(
                    arrow::util::pretty::pretty_format_batches(&actual)?.to_string(),
                    arrow::util::pretty::pretty_format_batches(&expected)?.to_string(),
                );
                assert!(
                    crate::utils::metric_value(registry, "executor_shuffle_read_remote_rows", &[])
                        > 0.0,
                    "executors must fetch nonempty shuffle partitions through Flight"
                );

                // (2) A node that does not check its own certificate (a release
                //     before this rule) registers at an address its certificate
                //     does not name: the scheduler refuses it, on the Spice
                //     control path and on the Ballista registration.
                let pki = harness
                    .cluster_pki()
                    .expect("harness runs with cluster mTLS")
                    .clone();
                let node = "executor7";
                let channel = channel_as(&harness, node).await;
                let subject = subject_of(&pki.client_cert_path(node));
                let advertised = format!("{UNNAMED_ADDRESS}:50052");
                let expected_refusal =
                    registration_address_mismatch_message(node, &subject, &advertised);

                let allocate = ClusterServiceClient::new(channel.clone())
                    .allocate_initial_partitions(AllocateInitialPartitionsRequest {
                        executor_url: format!("https://{advertised}"),
                        executor_id: node.to_string(),
                        process_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                    })
                    .await
                    .expect_err("a registration at an unnamed address is refused");
                assert_eq!(allocate.code(), Code::PermissionDenied, "{allocate:?}");
                assert_eq!(allocate.message(), expected_refusal);

                let register = SchedulerGrpcClient::new(channel)
                    .register_executor(RegisterExecutorParams {
                        metadata: Some(ExecutorRegistration {
                            id: node.to_string(),
                            host: Some(UNNAMED_ADDRESS.to_string()),
                            port: 50052,
                            ..Default::default()
                        }),
                    })
                    .await
                    .expect_err("a Ballista registration at an unnamed address is refused");
                assert_eq!(register.code(), Code::PermissionDenied, "{register:?}");
                assert_eq!(register.message(), expected_refusal);

                // (3) An executor started with a certificate that does not name
                //     its advertise address exits before it registers, with the
                //     startup message.
                let started = harness
                    .add_executor_advertising(UNNAMED_ADDRESS)
                    .await
                    .expect_err("the executor does not start");
                let startup_message = advertise_address_mismatch_message(
                    MismatchSite::Startup,
                    "executor",
                    "executor2",
                    &subject_of(&pki.client_cert_path("executor2")),
                    UNNAMED_ADDRESS,
                );
                assert!(
                    started.to_string().contains(&startup_message),
                    "startup error carries the message: {started}"
                );

                // Neither node registered.
                let registered: Vec<String> = harness
                    .registered_executors()
                    .await?
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();
                assert_eq!(registered, vec!["executor0", "executor1"]);
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reloaded_certificate_that_drops_the_advertise_address_is_not_loaded()
-> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let harness = ClusterHarness::builder().executors(1).start().await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                let pki = harness
                    .cluster_pki()
                    .expect("harness runs with cluster mTLS");
                let executor = &harness.executors[0];
                let cluster_address = executor.datafusion().cluster_config.node_bind_address();
                let tls = executor
                    .datafusion()
                    .cluster_config
                    .tls_config()
                    .expect("executor runs with cluster mTLS")
                    .clone();
                let bundle = tls.bundle_for_tests();
                let loaded = bundle.fingerprint();

                // The executor's certificate names 127.0.0.1, where it advertises.
                connect_verifying(pki, "probe", cluster_address, "127.0.0.1")
                    .await
                    .expect("the executor's certificate names its advertise address");

                // Rotate executor0's files to a CA-issued certificate that names
                // another host only, then reload.
                let rejected_before = reload_count_for_tests(ReloadScope::Cluster, "parse_error");
                let (cert, key) =
                    certificate_naming_only(pki, "executor0", &["executor0.elsewhere.invalid"]);
                replace_file(&pki.client_key_path("executor0"), &key);
                replace_file(&pki.client_cert_path("executor0"), &cert);
                bundle.force_reload_for_tests();

                // The executor still serves the certificate that names its
                // address, because the reload read the new material and refused it.
                assert_eq!(
                    bundle.fingerprint(),
                    loaded,
                    "the previous certificate stays loaded"
                );
                assert!(tls.self_certificate_names_host("127.0.0.1"));
                connect_verifying(pki, "probe", cluster_address, "127.0.0.1")
                    .await
                    .expect("the executor still presents a certificate naming its address");
                assert!(
                    reload_count_for_tests(ReloadScope::Cluster, "parse_error") > rejected_before,
                    "the reload read the rotated certificate and refused it"
                );
                assert_eq!(harness.registered_executors().await?.len(), 1);

                // A renewal that names the address is loaded.
                pki.create_client_cert("executor0")
                    .expect("renew executor0's certificate");
                bundle.force_reload_for_tests();
                assert_ne!(
                    bundle.fingerprint(),
                    loaded,
                    "the renewed certificate is loaded"
                );
                connect_verifying(pki, "probe", cluster_address, "127.0.0.1")
                    .await
                    .expect("the renewed certificate names the advertise address");
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}
