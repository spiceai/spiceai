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

//! End-to-end tests for outbound mTLS on the `flightsql` data connector.
//!
//! Spins up an "upstream" `Runtime` with `client_auth_mode: required` and a
//! test table, then loads a "downstream" `Runtime` whose dataset
//! federates into the upstream over mTLS via the `flightsql` connector
//! and the `tls_client_certificate_file` / `tls_client_key_file` params
//! (or the inline `tls_client_certificate` / `tls_client_key` variants).
//!
//! Also covers the connector-level validation that rejects a
//! half-configured client identity at dataset-load time with a clear
//! error rather than letting it surface as an opaque transport error
//! on first query.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use arrow::array::RecordBatch;
use futures::TryStreamExt as _;
use rand::RngExt as _;
use rcgen::{
    CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use runtime::{Runtime, auth::EndpointAuth, config::Config, tls::TlsConfig};
use runtime_auth::IdentitySource;
use spicepod::{component::dataset::Dataset, param::Params};
use tempfile::TempDir;

use crate::{
    init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

fn install_crypto_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
}

#[expect(
    clippy::struct_field_names,
    reason = "`_pem` postfix matches the surrounding test harness's PEM-shaped vars"
)]
struct ConnectorTestPki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn generate_pki(tag: &str) -> ConnectorTestPki {
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, format!("Spice connector mTLS CA {tag}"));
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

    let mut srv_dn = DistinguishedName::new();
    srv_dn.push(DnType::CommonName, format!("upstream-server-{tag}"));
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

    let mut cli_dn = DistinguishedName::new();
    cli_dn.push(DnType::CommonName, format!("downstream-client-{tag}"));
    let mut cli_params = CertificateParams::default();
    cli_params.distinguished_name = cli_dn;
    let cli_key = KeyPair::generate().expect("cli keypair");
    let cli = cli_params
        .signed_by(&cli_key, &ca_issuer)
        .expect("client signed by CA");

    ConnectorTestPki {
        ca_pem: ca.pem(),
        server_cert_pem: srv.pem(),
        server_key_pem: srv_key.serialize_pem(),
        client_cert_pem: cli.pem(),
        client_key_pem: cli_key.serialize_pem(),
    }
}

fn random_ports() -> (u16, u16, u16) {
    let mut rng = rand::rng();
    let http: u16 = rng.random_range(50000..60000);
    (http, http + 1, http + 2)
}

async fn write_csv_dataset(dir: &std::path::Path) -> std::path::PathBuf {
    let csv_path = dir.join("hello.csv");
    tokio::fs::write(&csv_path, "a,b\n1,alice\n2,bob\n3,carol\n")
        .await
        .expect("write csv");
    csv_path
}

/// Spawn an upstream `Runtime` with mTLS + a CSV-backed `hello`
/// dataset and return the public Flight port. The upstream loads the
/// CSV via the `file:` connector so its Flight server can serve
/// federated queries through `FlightSQL` with no extra writer plumbing.
async fn start_upstream(pki: &ConnectorTestPki, csv_path: &std::path::Path) -> u16 {
    register_test_connectors().await;

    let (http_port, flight_port, metrics_port) = random_ports();
    tracing::debug!(
        "upstream ports: http={http_port}, flight={flight_port}, metrics={metrics_port}"
    );

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    let tls_config = TlsConfig::try_new_with_client_auth(
        pki.server_cert_pem.as_bytes(),
        pki.server_key_pem.as_bytes(),
        Some(pki.ca_pem.as_bytes()),
    )
    .expect("upstream TlsConfig with client auth");

    let registry = prometheus::Registry::new();

    // CSV-backed dataset registered into the upstream's app definition.
    let mut hello = Dataset::new(format!("file:{}", csv_path.display()), "hello".to_string());
    hello.params = Some(Params::from_string_map(HashMap::from([(
        "file_format".to_string(),
        "csv".to_string(),
    )])));
    let app = app::AppBuilder::new("upstream").with_dataset(hello).build();

    let rt = Arc::new(
        Runtime::builder()
            .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
            .with_app(app)
            .build()
            .await,
    );

    let load_handle = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(60)) => {
            panic!("upstream timed out loading components");
        }
        () = load_handle.load_components() => {}
    }

    let endpoint_auth = EndpointAuth::no_auth().with_identity_source(IdentitySource::Channel);
    let rt_for_servers = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_for_servers.start_servers(
            api_config,
            Some(Arc::new(tls_config)),
            endpoint_auth,
        ))
        .await
    });

    // Probe HTTPS health with our own client cert; abort early if the
    // listener does not come up.
    let probe_ca = reqwest::tls::Certificate::from_pem(pki.ca_pem.as_bytes()).expect("probe CA");
    let mut probe_id_buf =
        Vec::with_capacity(pki.client_cert_pem.len() + pki.client_key_pem.len() + 1);
    probe_id_buf.extend_from_slice(pki.client_cert_pem.as_bytes());
    probe_id_buf.push(b'\n');
    probe_id_buf.extend_from_slice(pki.client_key_pem.as_bytes());
    let probe_identity = reqwest::Identity::from_pem(&probe_id_buf).expect("probe identity");
    let probe = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(probe_ca)
        .identity(probe_identity)
        .build()
        .expect("probe client");
    let url = format!("https://localhost:{http_port}/health");
    wait_until_true(Duration::from_secs(15), || {
        let probe = probe.clone();
        let url = url.clone();
        async move { probe.get(&url).send().await.is_ok() }
    })
    .await;

    runtime_ready_check(&rt).await;
    flight_port
}

/// Build a downstream `Runtime` that federates into the upstream Flight
/// port over mTLS using the supplied PKI material on disk, then return
/// the runtime so the caller can drive queries through it.
async fn start_downstream(
    flight_port: u16,
    cert_dir: &TempDir,
    pki: &ConnectorTestPki,
    include_client_cert: bool,
) -> Result<Runtime, Box<dyn std::error::Error + Send + Sync>> {
    let ca_path = cert_dir.path().join("ca.pem");
    let client_cert_path = cert_dir.path().join("client.pem");
    let client_key_path = cert_dir.path().join("client.key");
    tokio::fs::write(&ca_path, &pki.ca_pem).await?;
    tokio::fs::write(&client_cert_path, &pki.client_cert_pem).await?;
    tokio::fs::write(&client_key_path, &pki.client_key_pem).await?;

    let mut params: HashMap<String, String> = HashMap::from([
        (
            "flightsql_endpoint".to_string(),
            format!("grpc+tls://localhost:{flight_port}"),
        ),
        (
            "flightsql_tls_ca_certificate_file".to_string(),
            ca_path.display().to_string(),
        ),
    ]);
    if include_client_cert {
        params.insert(
            "flightsql_tls_client_certificate_file".to_string(),
            client_cert_path.display().to_string(),
        );
        params.insert(
            "flightsql_tls_client_key_file".to_string(),
            client_key_path.display().to_string(),
        );
    }

    let mut dataset = Dataset::new("flightsql:hello".to_string(), "hello".to_string());
    dataset.params = Some(Params::from_string_map(params));

    register_test_connectors().await;
    let app = app::AppBuilder::new("downstream")
        .with_dataset(dataset)
        .build();
    let rt = Runtime::builder().with_app(app).build().await;
    Ok(rt)
}

/// Happy path: upstream `client_auth: required`, downstream presents a
/// matching client cert via `tls_client_certificate_file` + `tls_client_key_file`,
/// federated query runs end-to-end.
#[tokio::test]
async fn test_flightsql_connector_mtls_end_to_end() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_flightsql_connector_mtls_end_to_end");
            let _span_guard = span.enter();

            let pki = generate_pki("happy");
            let csv_dir = TempDir::new().expect("csv dir");
            let csv_path = write_csv_dataset(csv_dir.path()).await;
            let flight_port = start_upstream(&pki, &csv_path).await;
            tracing::info!("upstream Flight ready on port {flight_port}");

            let cert_dir = TempDir::new().expect("temp dir");
            let downstream = start_downstream(flight_port, &cert_dir, &pki, true)
                .await
                .map_err(|e| anyhow::anyhow!("downstream build: {e}"))?;
            let downstream_handle = Arc::new(downstream.clone());

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for downstream to load"));
                }
                () = downstream_handle.load_components() => {}
            }

            let result_batches = downstream
                .datafusion()
                .query_builder("SELECT a, b FROM hello ORDER BY a")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("query failed: {e}"))?
                .data
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| anyhow::anyhow!("collect: {e}"))?;

            let total_rows: usize = result_batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(
                total_rows, 3,
                "expected 3 rows from upstream over mTLS, got {total_rows}"
            );

            let pretty =
                arrow::util::pretty::pretty_format_batches(&result_batches).expect("pretty");
            let pretty = pretty.to_string();
            for needle in ["alice", "bob", "carol"] {
                assert!(
                    pretty.contains(needle),
                    "expected '{needle}' in mTLS-federated query results:\n{pretty}"
                );
            }

            Ok::<_, anyhow::Error>(())
        })
        .await
}

/// Setting `tls_client_certificate_file` without `tls_client_key_file`
/// (or vice versa) is rejected at dataset-load time, with an error
/// that names both fields.
#[tokio::test]
async fn test_flightsql_connector_mtls_half_configured_rejected() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    install_crypto_provider();

    test_request_context()
        .scope(async {
            let pki = generate_pki("halfcfg");
            let cert_dir = TempDir::new().expect("temp dir");
            let cert_path = cert_dir.path().join("client.pem");
            let ca_path = cert_dir.path().join("ca.pem");
            tokio::fs::write(&cert_path, &pki.client_cert_pem)
                .await
                .expect("write cert");
            tokio::fs::write(&ca_path, &pki.ca_pem)
                .await
                .expect("write ca");

            let params: HashMap<String, String> = HashMap::from([
                (
                    "flightsql_endpoint".to_string(),
                    "grpc+tls://localhost:1".to_string(),
                ),
                (
                    "flightsql_tls_ca_certificate_file".to_string(),
                    ca_path.display().to_string(),
                ),
                // cert without key => half-configured
                (
                    "flightsql_tls_client_certificate_file".to_string(),
                    cert_path.display().to_string(),
                ),
            ]);
            let mut dataset =
                Dataset::new("flightsql:hello".to_string(), "hello".to_string());
            dataset.params = Some(Params::from_string_map(params));

            register_test_connectors().await;
            let app = app::AppBuilder::new("downstream-halfcfg")
                .with_dataset(dataset)
                .build();
            let rt = Runtime::builder().with_app(app).build().await;

            // load_components surfaces a status::Error per dataset
            // rather than panicking; check the runtime status reflects
            // a failed load.
            // load_components retries forever on transient errors, so
            // run it in the background and just wait until the per-dataset
            // status flips to Error after the first connector failure.
            let load_handle = tokio::spawn(Arc::new(rt.clone()).load_components());

            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            let mut last_seen: Option<runtime::status::ComponentStatus> = None;
            while std::time::Instant::now() < deadline {
                let states = rt.status().get_dataset_statuses();
                if let Some((_, status)) = states
                    .iter()
                    .find(|(name, _)| name.table() == "hello")
                {
                    last_seen = Some(status.clone());
                    if matches!(status, runtime::status::ComponentStatus::Error(_)) {
                        load_handle.abort();
                        return Ok::<_, anyhow::Error>(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            load_handle.abort();
            Err(anyhow::anyhow!(
                "hello dataset never reached Error state on half-configured load (last seen: {last_seen:?})"
            ))
        })
        .await
}
