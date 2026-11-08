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

//! Latency of a full TLS 1.3 handshake with a client certificate against
//! the Flight listener's `ServerConfig`, with and without client CRLs.
//!
//! Both peers run in this thread over in-memory buffers, so a sample is the
//! CPU cost of one handshake (both sides, including the server verifying
//! the client certificate and its revocation status) with no network noise.
//! Session resumption is off, so every sample is a full handshake.
//!
//! ```text
//! cargo bench -p runtime-tls --bench client_auth_handshake -- \
//!     [--crl-entries N] [--samples N] [--warmup N]
//! ```
//!
//! Without `--crl-entries` no CRL is configured. With it, the CA's CRL
//! revokes `N` serial numbers, none of them the client's, so every
//! handshake succeeds after a full revocation lookup.

#![expect(clippy::expect_used)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DistinguishedName,
    DnType, IsCa, Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose, RevocationReason,
    RevokedCertParams, SanType, SerialNumber,
};
use runtime_tls::{ClientAuthEnforcement, ClientTrustPem, TlsConfig};
use rustls::{
    ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig, ServerConnection,
    client::Resumption,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};

const CLIENT_SERIAL: u64 = 1_000_000_000;

struct Args {
    crl_entries: Option<u64>,
    samples: usize,
    warmup: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        crl_entries: None,
        samples: 20_000,
        warmup: 1_000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = |name: &str| -> u64 {
            it.next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{name} takes a number"))
        };
        match arg.as_str() {
            "--crl-entries" => args.crl_entries = Some(value("--crl-entries")),
            "--samples" => args.samples = usize::try_from(value("--samples")).expect("samples"),
            "--warmup" => args.warmup = usize::try_from(value("--warmup")).expect("warmup"),
            // `cargo bench` passes `--bench`; ignore it and anything else.
            _ => {}
        }
    }
    args
}

struct Pki {
    ca_pem: String,
    ca_der: CertificateDer<'static>,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_der: CertificateDer<'static>,
    client_key_der: Vec<u8>,
    crl_pem: Option<String>,
}

fn pki(crl_entries: Option<u64>) -> Pki {
    let mut ca_params = CertificateParams::default();
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "handshake bench CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("CA key");
    let ca = ca_params.self_signed(&ca_key).expect("CA cert");
    let issuer = Issuer::new(ca_params, ca_key);

    let mut server_params = CertificateParams::default();
    server_params
        .subject_alt_names
        .push(SanType::DnsName("localhost".try_into().expect("DNS name")));
    let server_key = KeyPair::generate().expect("server key");
    let server = server_params
        .signed_by(&server_key, &issuer)
        .expect("server cert");

    let mut client_params = CertificateParams::default();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "bench client");
    client_params.serial_number = Some(SerialNumber::from(CLIENT_SERIAL));
    let client_key = KeyPair::generate().expect("client key");
    let client = client_params
        .signed_by(&client_key, &issuer)
        .expect("client cert");

    let crl_pem = crl_entries.map(|entries| {
        let now = time::OffsetDateTime::now_utc();
        CertificateRevocationListParams {
            this_update: now - time::Duration::minutes(5),
            next_update: now + time::Duration::days(7),
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: (1..=entries)
                .map(|serial| RevokedCertParams {
                    serial_number: SerialNumber::from(serial),
                    revocation_time: now - time::Duration::minutes(5),
                    reason_code: Some(RevocationReason::KeyCompromise),
                    invalidity_date: None,
                })
                .collect(),
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&issuer)
        .expect("CRL")
        .pem()
        .expect("CRL PEM")
    });

    Pki {
        ca_pem: ca.pem(),
        ca_der: ca.der().clone(),
        server_cert_pem: server.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_der: client.der().clone(),
        client_key_der: client_key.serialize_der(),
        crl_pem,
    }
}

fn server_config(pki: &Pki) -> Arc<ServerConfig> {
    let tls = TlsConfig::try_new_with_client_auth_mode(
        pki.server_cert_pem.as_bytes(),
        pki.server_key_pem.as_bytes(),
        Some(ClientTrustPem {
            ca: pki.ca_pem.as_bytes(),
            crl: pki.crl_pem.as_deref().map(str::as_bytes),
        }),
        ClientAuthEnforcement::Required,
    )
    .expect("TLS config");
    Arc::clone(&tls.flight_server_config)
}

fn client_config(pki: &Pki) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(pki.ca_der.clone()).expect("trust the CA");
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![pki.client_cert_der.clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pki.client_key_der.clone())),
        )
        .expect("client identity");
    config.resumption = Resumption::disabled();
    Arc::new(config)
}

/// Move every pending TLS record from `from` to `to`.
fn transfer(from: &mut Connection, to: &mut Connection) {
    let mut buf = Vec::new();
    while from.wants_write() {
        from.write_tls(&mut buf).expect("write TLS");
    }
    let mut rd = buf.as_slice();
    while !rd.is_empty() {
        to.read_tls(&mut rd).expect("read TLS");
        to.process_new_packets().expect("handshake succeeds");
    }
}

fn handshake(server: &Arc<ServerConfig>, client: &Arc<ClientConfig>) -> Duration {
    let start = Instant::now();
    let mut c = Connection::from(
        ClientConnection::new(
            Arc::clone(client),
            ServerName::try_from("localhost").expect("server name"),
        )
        .expect("client connection"),
    );
    let mut s =
        Connection::from(ServerConnection::new(Arc::clone(server)).expect("server connection"));
    while c.is_handshaking() || s.is_handshaking() {
        transfer(&mut c, &mut s);
        transfer(&mut s, &mut c);
    }
    let elapsed = start.elapsed();
    assert!(
        s.peer_certificates().is_some(),
        "the server must have verified a client certificate"
    );
    elapsed
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "index into a sample vector far below 2^52"
    )]
    let idx = ((sorted.len() as f64) * p).ceil() as usize;
    sorted[idx.saturating_sub(1).min(sorted.len() - 1)]
}

fn main() {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let args = parse_args();
    let pki = pki(args.crl_entries);
    let server = server_config(&pki);
    let client = client_config(&pki);

    for _ in 0..args.warmup {
        handshake(&server, &client);
    }
    let mut samples: Vec<Duration> = (0..args.samples)
        .map(|_| handshake(&server, &client))
        .collect();
    samples.sort_unstable();

    println!(
        "client_auth_handshake crl_entries={} samples={} p50_us={:.1} p99_us={:.1} p99.9_us={:.1} max_us={:.1}",
        args.crl_entries
            .map_or_else(|| "none".to_string(), |n| n.to_string()),
        samples.len(),
        percentile(&samples, 0.50).as_secs_f64() * 1e6,
        percentile(&samples, 0.99).as_secs_f64() * 1e6,
        percentile(&samples, 0.999).as_secs_f64() * 1e6,
        samples.last().map_or(0.0, |d| d.as_secs_f64() * 1e6),
    );
}
