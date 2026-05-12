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

//! End-to-end test for cluster-mTLS certificate hot-reload.
//!
//! Spawns an `Arc<rustls::ServerConfig>` from a real `ClusterTlsConfig`
//! built against on-disk cert/key/CA files, runs a tonic server with two
//! cluster-mode services over `tls_incoming`, executes a real mTLS RPC
//! using the matching client identity, atomically rotates *all three*
//! files (CA + cert + key) to a new self-consistent PKI, then verifies a
//! second mTLS RPC works against the new identity.
//!
//! This exercises the same plumbing the production cluster servers use
//! (`crate::cluster::servers::start_internal_cluster_server` /
//! `start_executor_flight_server`).

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use crate::{init_tracing, utils::test_request_context};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_client::FlightServiceClient,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{CommandStatementQuery, ProstMessageExt},
};
use futures::stream::BoxStream;
use prost::Message;
use rand::RngExt;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use runtime::{
    cluster::ClusterTlsConfig,
    tls::{ReloadScope, flight_incoming::tls_incoming, reload::reload_count_for_tests},
};
use tempfile::TempDir;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Certificate, Channel, ClientTlsConfig, Identity, Server},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn install_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

/// PKI generated for a single mTLS identity round.
struct GeneratedClusterPki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    /// Client cert + key signed by the same CA. Cluster mTLS uses one CA
    /// for both sides.
    client_cert_pem: String,
    client_key_pem: String,
    /// Tag included in `CommonName` so we can assert the rotation actually
    /// happened (tagged via a test-only header).
    tag: String,
}

fn generate_cluster_pki(tag: &str) -> GeneratedClusterPki {
    // CA
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, format!("Spice cluster reload CA {tag}"));
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = ca_dn;
    ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("ca keypair");
    let ca = ca_params.self_signed(&ca_key).expect("self-signed CA");
    let ca_issuer = Issuer::new(ca_params, ca_key);

    // Server leaf
    let mut srv_dn = DistinguishedName::new();
    srv_dn.push(DnType::CommonName, format!("cluster-server-{tag}"));
    let mut srv_params = CertificateParams::default();
    srv_params.distinguished_name = srv_dn;
    srv_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().expect("dns")));
    srv_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let srv_key = KeyPair::generate().expect("srv keypair");
    let srv = srv_params
        .signed_by(&srv_key, &ca_issuer)
        .expect("server signed by CA");

    // Client leaf
    let mut cli_dn = DistinguishedName::new();
    cli_dn.push(DnType::CommonName, format!("cluster-client-{tag}"));
    let mut cli_params = CertificateParams::default();
    cli_params.distinguished_name = cli_dn;
    let cli_key = KeyPair::generate().expect("cli keypair");
    let cli = cli_params
        .signed_by(&cli_key, &ca_issuer)
        .expect("client signed by CA");

    GeneratedClusterPki {
        ca_pem: ca.pem(),
        server_cert_pem: srv.pem(),
        server_key_pem: srv_key.serialize_pem(),
        client_cert_pem: cli.pem(),
        client_key_pem: cli_key.serialize_pem(),
        tag: tag.to_string(),
    }
}

fn atomic_write(dst: &std::path::Path, contents: &str) {
    let tmp = dst.with_extension("tmp.write");
    std::fs::write(&tmp, contents).expect("write tmp");
    std::fs::rename(&tmp, dst).expect("atomic rename");
}

/// Minimal Flight service used purely as a tonic-compatible RPC target.
/// `get_flight_info` is the only method exercised; everything else returns
/// `Unimplemented`.
struct PingFlight;

#[tonic::async_trait]
impl FlightService for PingFlight {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Ok(Response::new(FlightInfo::default()))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

async fn build_mtls_client(
    pki: &GeneratedClusterPki,
    port: u16,
) -> anyhow::Result<FlightServiceClient<Channel>> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(pki.ca_pem.as_bytes()))
        .identity(Identity::from_pem(
            pki.client_cert_pem.as_bytes(),
            pki.client_key_pem.as_bytes(),
        ))
        .domain_name("localhost");
    let channel = Channel::from_shared(format!("https://localhost:{port}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    Ok(FlightServiceClient::new(channel))
}

async fn ping(client: &mut FlightServiceClient<Channel>) -> anyhow::Result<()> {
    let sql = CommandStatementQuery {
        query: "ping".to_string(),
        transaction_id: None,
    };
    let req = FlightDescriptor::new_cmd(sql.as_any().encode_to_vec());
    let _ = client.get_flight_info(req).await?;
    Ok(())
}

#[tokio::test]
async fn test_cluster_mtls_hot_reload() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_cluster_mtls_hot_reload");
            let _span_guard = span.enter();

            // 1. Generate v1 PKI on disk.
            let pki_v1 = generate_cluster_pki("v1");
            let dir = TempDir::new()?;
            let ca_path = dir.path().join("ca.pem");
            let cert_path = dir.path().join("server.pem");
            let key_path = dir.path().join("server.key");
            std::fs::write(&ca_path, &pki_v1.ca_pem)?;
            std::fs::write(&cert_path, &pki_v1.server_cert_pem)?;
            std::fs::write(&key_path, &pki_v1.server_key_pem)?;

            // 2. Build a real ClusterTlsConfig (registers a single
            //    `ClusterPkiBundle` reload callback on the supplied
            //    `TlsControl`; bundle implements both ResolvesServerCert
            //    and ClientCertVerifier).
            let control = runtime::tls::TlsControl::new()?;
            let cluster_tls = ClusterTlsConfig::try_new(
                ca_path.to_str().expect("utf8"),
                cert_path.to_str().expect("utf8"),
                key_path.to_str().expect("utf8"),
                &control,
            )?;

            // 3. Launch a tonic server with `tls_incoming` over the
            //    reloadable rustls config — same plumbing as
            //    `start_internal_cluster_server` / `start_executor_flight_server`.
            let mut rng = rand::rng();
            let port: u16 = rng.random_range(50000..60000);
            let bind = SocketAddr::new(LOCALHOST, port);
            let listener = tokio::net::TcpListener::bind(bind).await?;
            let server_cfg = cluster_tls.server_config();
            let incoming = tls_incoming(listener, server_cfg);
            tokio::spawn(async move {
                let _ = Server::builder()
                    .add_service(FlightServiceServer::new(PingFlight))
                    .serve_with_incoming(incoming)
                    .await;
            });

            // 4. mTLS RPC succeeds with the v1 client identity.
            //    Retry briefly for the server to accept.
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut client = loop {
                match build_mtls_client(&pki_v1, port).await {
                    Ok(c) => break c,
                    Err(e) if Instant::now() < deadline => {
                        tracing::debug!("waiting for cluster server: {e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => return Err(e),
                }
            };
            ping(&mut client).await?;
            tracing::info!("cluster mTLS RPC succeeded under {}", pki_v1.tag);

            // 5. Rotate all three files atomically to a v2 PKI signed by a
            //    completely different CA.
            let pki_v2 = generate_cluster_pki("v2");
            // Use the per-(scope, result) bucket so this test does not race
            // with other reload tests in the same process.
            let reload_before = reload_count_for_tests(ReloadScope::Cluster, "ok");
            atomic_write(&ca_path, &pki_v2.ca_pem);
            atomic_write(&cert_path, &pki_v2.server_cert_pem);
            atomic_write(&key_path, &pki_v2.server_key_pem);
            tracing::info!("rotated cluster CA + cert + key on disk to v2");

            // 6. Wait for the cluster-scope `ok` reload bucket to tick (covers
            //    cert + verifier + outbound ClientTlsConfig swap). PollWatcher
            //    uses ~2s interval; 15s is plenty.
            let saw_reload = wait_for(Duration::from_secs(15), || async {
                reload_count_for_tests(ReloadScope::Cluster, "ok") > reload_before
            })
            .await;
            assert!(
                saw_reload,
                "cluster TLS reload metric never incremented after rotation"
            );

            // 7. Old client (v1 CA + v1 client cert) must now FAIL — its
            //    client cert is signed by the v1 CA, which the server no
            //    longer trusts. We drop the existing v1 client because its
            //    Channel was created against the v1 CA; new connections
            //    against the v2 CA prove the swap.
            drop(client);

            // 8. New mTLS RPC with v2 identity succeeds. Retry to absorb
            //    the swap-vs-handshake race.
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut client_v2 = loop {
                match build_mtls_client(&pki_v2, port).await {
                    Ok(c) => break c,
                    Err(e) => {
                        if Instant::now() >= deadline {
                            return Err(e);
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            };
            ping(&mut client_v2).await?;
            tracing::info!("cluster mTLS RPC succeeded under {}", pki_v2.tag);

            // 9. Build a fresh v1 client and verify it is REJECTED — server
            //    no longer trusts the v1 CA. We expect a TLS handshake
            //    error or an immediate Status::unauthenticated-style failure.
            //    `connect()` itself triggers the handshake when using
            //    `connect()` (vs `connect_lazy`), so this should fail at
            //    connect time.
            let v1_after = build_mtls_client(&pki_v1, port).await;
            assert!(
                v1_after.is_err(),
                "v1 client should be rejected after rotation; got Ok"
            );
            tracing::info!(
                "v1 client correctly rejected after rotation: {}",
                v1_after.expect_err("checked above")
            );

            // 10. ClusterTlsConfig::client_tls_config() now reflects the v2
            //     material — check that the snapshot it returns connects
            //     successfully against the v2 server.
            let snapshot = cluster_tls.client_tls_config();
            let snapshot_channel = Channel::from_shared(format!("https://localhost:{port}"))?
                .tls_config(snapshot.domain_name("localhost"))?
                .connect()
                .await?;
            let mut snapshot_client = FlightServiceClient::new(snapshot_channel);
            ping(&mut snapshot_client).await?;
            tracing::info!(
                "ClusterTlsConfig::client_tls_config() produced a v2 snapshot that connects"
            );

            Ok(())
        })
        .await
}

async fn wait_for<F, Fut>(timeout: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
