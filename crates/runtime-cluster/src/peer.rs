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

//! Who a cluster peer is, from the certificate it presented on the cluster's
//! mutual TLS connection.
//!
//! The cluster CA admits a node to the cluster; it does not say which node.
//! A node's certificate does: its issuer, subject and subject alternative names
//! name one node (or, when the operator issues one certificate to several nodes,
//! the nodes that share it). [`PeerIdentity`] is that name. It is what binds an
//! executor id to the node that registered it, and what tells an executor's
//! Flight server the scheduler from the other executors: the scheduler is the
//! node that presents the identity the scheduler's address presented when this
//! executor connected to it ([`SchedulerIdentities`]).
//!
//! A node that shares its certificate with another node cannot be told apart
//! from it, here or anywhere else in TLS: the identity is as fine as the
//! certificates the operator issues.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use parking_lot::RwLock;
use rustls::{
    pki_types::{CertificateDer, ServerName},
    server::ParsedCertificate,
};
use sha2::{Digest, Sha256};
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use x509_certificate::X509Certificate;

/// DER content octets of the subject alternative name extension's OID,
/// 2.5.29.17 (RFC 5280, section 4.2.1.6).
const SUBJECT_ALT_NAME_OID: [u8; 3] = [85, 29, 17];

/// The identity of a cluster node, from the parts of its certificate that name
/// it and that a renewal keeps: the issuer, the subject and the subject
/// alternative names. A renewed certificate, with a new key and validity
/// period, keeps the identity; a certificate issued for another node does not.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PeerIdentity {
    fingerprint: [u8; 32],
    subject: Arc<str>,
}

impl PeerIdentity {
    /// The identity of the node that presented the leaf certificate `leaf`, or
    /// `None` when it does not parse as an X.509 certificate.
    #[must_use]
    pub fn from_certificate(leaf: &CertificateDer<'_>) -> Option<Self> {
        let certificate = X509Certificate::from_der(leaf.as_ref()).ok()?;
        let subject = certificate
            .subject_name()
            .user_friendly_str()
            .unwrap_or_default();
        let issuer = certificate
            .issuer_name()
            .user_friendly_str()
            .unwrap_or_default();

        let mut hasher = Sha256::new();
        hash_field(&mut hasher, b"issuer", issuer.as_bytes());
        hash_field(&mut hasher, b"subject", subject.as_bytes());
        for extension in certificate.iter_extensions() {
            if extension.id.as_ref() == SUBJECT_ALT_NAME_OID.as_slice() {
                hash_field(&mut hasher, b"san", &extension.value.to_bytes());
            }
        }

        Some(Self {
            fingerprint: hasher.finalize().into(),
            subject: Arc::from(subject),
        })
    }

    /// The certificate's subject, such as `CN=executor-1`, for messages.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.subject.is_empty() {
            f.write_str("a certificate with an empty subject")
        } else {
            write!(f, "'{}'", self.subject)
        }
    }
}

impl fmt::Debug for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerIdentity")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

fn hash_field(hasher: &mut Sha256, label: &[u8], value: &[u8]) {
    hasher.update(label);
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// The certificate a cluster peer presented on its connection, already
/// verified against the cluster CA by the TLS handshake.
#[derive(Clone)]
pub struct PeerCertificate {
    leaf: CertificateDer<'static>,
    identity: PeerIdentity,
}

impl PeerCertificate {
    /// The certificate of the peer that sent `request`, or `None` when the
    /// connection carries no client certificate (cluster mTLS is off).
    #[must_use]
    pub fn from_request<T>(request: &tonic::Request<T>) -> Option<Self> {
        Self::from_chain(request.peer_certs()?.as_slice())
    }

    /// The certificate in the connection information `tonic` attaches to every
    /// request served over the cluster's TLS listener.
    #[must_use]
    pub fn from_connect_info(info: &TlsConnectInfo<TcpConnectInfo>) -> Option<Self> {
        Self::from_chain(info.peer_certs()?.as_slice())
    }

    /// The certificate whose leaf heads `chain`.
    #[must_use]
    pub fn from_chain(chain: &[CertificateDer<'static>]) -> Option<Self> {
        let leaf = chain.first()?.clone();
        let identity = PeerIdentity::from_certificate(&leaf)?;
        Some(Self { leaf, identity })
    }

    #[must_use]
    pub fn identity(&self) -> &PeerIdentity {
        &self.identity
    }

    /// Whether the certificate is valid for `host`, as a TLS client checks the
    /// certificate of a server it reaches at `host`: the address a node may
    /// advertise is one its certificate names.
    #[must_use]
    pub fn names_host(&self, host: &str) -> bool {
        certificate_names_host(&self.leaf, host)
    }
}

impl fmt::Debug for PeerCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerCertificate")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

/// Whether `leaf` is valid for `host` (a DNS name or an IP address, without a
/// port), by the rules a TLS client applies to a server's certificate.
#[must_use]
pub fn certificate_names_host(leaf: &CertificateDer<'_>, host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let Ok(name) = ServerName::try_from(host) else {
        return false;
    };
    let Ok(parsed) = ParsedCertificate::try_from(leaf) else {
        return false;
    };
    rustls::client::verify_server_name(&parsed, &name).is_ok()
}

/// Where a cluster node is told its certificate does not name its advertise
/// address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MismatchSite {
    /// The node checks its own certificate at startup, before it registers.
    Startup,
    /// The node rechecks a certificate reloaded from disk.
    Reload,
}

const DISTRIBUTED_QUERY_DOCS: &str = "https://spiceai.org/docs/features/distributed-query";

/// The message a node logs when its own certificate does not name the address it
/// advertises, at startup or on reload. `node_kind` is `executor` or `scheduler`;
/// `node_id` is its instance identity; `subject` is the certificate's subject
/// (for example `CN=executor-1`); `advertise_host` is the host it advertises.
#[must_use]
pub fn advertise_address_mismatch_message(
    site: MismatchSite,
    node_kind: &str,
    node_id: &str,
    subject: &str,
    advertise_host: &str,
) -> String {
    let lead = match site {
        MismatchSite::Startup => format!("Failed to start {node_kind} '{node_id}'"),
        MismatchSite::Reload => {
            format!("Failed to reload the cluster certificate of {node_kind} '{node_id}'")
        }
    };
    format!(
        "{lead}: its cluster certificate ({subject}) does not name its advertise address '{advertise_host}', so other cluster nodes cannot connect to it. Issue a certificate that names it, for example `spice cluster tls add {node_id} --host {advertise_host}`, or set `--node-advertise-address` to a name the certificate covers. See: {DISTRIBUTED_QUERY_DOCS}"
    )
}

/// The message the scheduler logs, and returns as `PermissionDenied`, when it
/// refuses `executor_id` because the sending node's certificate (`subject`) does
/// not name the address `advertised` it reported.
#[must_use]
pub fn registration_address_mismatch_message(
    executor_id: &str,
    subject: &str,
    advertised: &str,
) -> String {
    format!(
        "Refused to register executor '{executor_id}': it advertises '{advertised}', which its cluster certificate ({subject}) does not name, so other cluster nodes could not connect to it. Issue each node a certificate that names its `--node-advertise-address`. See: {DISTRIBUTED_QUERY_DOCS}"
    )
}

/// The host of a cluster address given as a URL (`https://host:port`) or as
/// `host:port`: what a TLS client verifies the server's certificate against.
#[must_use]
pub fn address_host(address: &str) -> Option<&str> {
    let authority = address.split_once("://").map_or(address, |(_, rest)| rest);
    let authority = authority.split(['/', '?', '#']).next().unwrap_or(authority);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = if let Some(bracketed) = authority.strip_prefix('[') {
        // `[v6]` or `[v6]:port`.
        &authority[..bracketed.find(']')? + 2]
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };
    (!host.is_empty()).then_some(host)
}

/// The identities the scheduler presents, as an executor observed them by
/// connecting to each address it knows the scheduler by.
///
/// An executor trusts whoever answers at a scheduler address as the scheduler:
/// it fetches its app definition and secrets there and runs the tasks it is
/// given. A request that arrives at the executor from a node presenting one of
/// those identities is from the scheduler; a request from any other node of the
/// cluster is from a peer executor.
#[derive(Debug, Default)]
pub struct SchedulerIdentities {
    /// Per scheduler address, the identity last observed there.
    by_address: RwLock<HashMap<String, PeerIdentity>>,
}

impl SchedulerIdentities {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `identity` is one a scheduler presented.
    #[must_use]
    pub fn contains(&self, identity: &PeerIdentity) -> bool {
        self.by_address
            .read()
            .values()
            .any(|scheduler| scheduler == identity)
    }

    /// Whether no scheduler identity has been observed yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_address.read().is_empty()
    }

    /// Records what was observed at the scheduler addresses `addresses`: the
    /// identity found at each address that answered. An address that did not
    /// answer keeps the identity last observed there, so a scheduler that is
    /// briefly unreachable is still recognized; an address no longer among
    /// `addresses` is forgotten.
    pub fn record(&self, addresses: &[String], observed: Vec<(String, PeerIdentity)>) {
        let current: HashSet<&str> = addresses.iter().map(String::as_str).collect();
        let mut by_address = self.by_address.write();
        by_address.retain(|address, _| current.contains(address.as_str()));
        for (address, identity) in observed {
            by_address.insert(address, identity);
        }
    }

    /// The subjects of the scheduler identities, for messages.
    #[must_use]
    pub fn subjects(&self) -> Vec<String> {
        let mut subjects: Vec<String> = self
            .by_address
            .read()
            .values()
            .map(ToString::to_string)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        subjects.sort();
        subjects
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::PeerIdentity;

    /// A node identity for tests: that of a self-signed certificate for
    /// `common_name`.
    pub(crate) fn identity(common_name: &str) -> PeerIdentity {
        let mut params = rcgen::CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        let key = rcgen::KeyPair::generate().expect("key");
        let certificate = params.self_signed(&key).expect("certificate");
        PeerIdentity::from_certificate(certificate.der()).expect("identity")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose,
        SanType,
    };

    #[test]
    fn advertise_mismatch_messages_name_the_node_address_and_fix() {
        assert_eq!(
            advertise_address_mismatch_message(
                MismatchSite::Startup,
                "executor",
                "executor-1",
                "CN=executor-1",
                "10.0.0.7",
            ),
            "Failed to start executor 'executor-1': its cluster certificate (CN=executor-1) does not name its advertise address '10.0.0.7', so other cluster nodes cannot connect to it. Issue a certificate that names it, for example `spice cluster tls add executor-1 --host 10.0.0.7`, or set `--node-advertise-address` to a name the certificate covers. See: https://spiceai.org/docs/features/distributed-query"
        );
        assert_eq!(
            advertise_address_mismatch_message(
                MismatchSite::Reload,
                "executor",
                "executor-1",
                "CN=executor-1",
                "10.0.0.7",
            ),
            "Failed to reload the cluster certificate of executor 'executor-1': its cluster certificate (CN=executor-1) does not name its advertise address '10.0.0.7', so other cluster nodes cannot connect to it. Issue a certificate that names it, for example `spice cluster tls add executor-1 --host 10.0.0.7`, or set `--node-advertise-address` to a name the certificate covers. See: https://spiceai.org/docs/features/distributed-query"
        );
        assert_eq!(
            registration_address_mismatch_message("executor-1", "CN=executor-1", "10.0.0.7:50052",),
            "Refused to register executor 'executor-1': it advertises '10.0.0.7:50052', which its cluster certificate (CN=executor-1) does not name, so other cluster nodes could not connect to it. Issue each node a certificate that names its `--node-advertise-address`. See: https://spiceai.org/docs/features/distributed-query"
        );
    }

    struct TestCa {
        issuer: Issuer<'static, KeyPair>,
    }

    impl TestCa {
        fn new(name: &str) -> Self {
            let mut params = CertificateParams::default();
            params.distinguished_name.push(DnType::CommonName, name);
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
            let key = KeyPair::generate().expect("CA key");
            Self {
                issuer: Issuer::new(params, key),
            }
        }

        fn issue(&self, common_name: &str, names: &[&str]) -> CertificateDer<'static> {
            let mut params = CertificateParams::default();
            params
                .distinguished_name
                .push(DnType::CommonName, common_name);
            for name in names {
                params.subject_alt_names.push(match name.parse() {
                    Ok(ip) => SanType::IpAddress(ip),
                    Err(_) => SanType::DnsName((*name).try_into().expect("DNS name")),
                });
            }
            let key = KeyPair::generate().expect("node key");
            params
                .signed_by(&key, &self.issuer)
                .expect("node certificate")
                .der()
                .clone()
        }
    }

    #[test]
    fn a_renewed_certificate_keeps_the_identity_and_another_node_has_its_own() {
        let ca = TestCa::new("cluster CA");
        let executor = ca.issue("executor-1", &["executor-1.cluster.local", "10.0.0.5"]);
        let renewed = ca.issue("executor-1", &["executor-1.cluster.local", "10.0.0.5"]);
        let other = ca.issue("executor-2", &["executor-2.cluster.local", "10.0.0.6"]);
        let same_names_other_subject =
            ca.issue("scheduler", &["executor-1.cluster.local", "10.0.0.5"]);
        let same_subject_other_names = ca.issue("executor-1", &["executor-9.cluster.local"]);

        let identity = PeerIdentity::from_certificate(&executor).expect("identity");
        assert_ne!(executor, renewed, "renewal issues a new certificate");
        assert_eq!(
            PeerIdentity::from_certificate(&renewed).expect("identity"),
            identity
        );
        for different in [other, same_names_other_subject, same_subject_other_names] {
            assert_ne!(
                PeerIdentity::from_certificate(&different).expect("identity"),
                identity
            );
        }
        assert_eq!(identity.subject(), "CN=executor-1");
        assert_eq!(identity.to_string(), "'CN=executor-1'");
    }

    #[test]
    fn the_same_names_from_another_issuer_are_another_identity() {
        let executor = TestCa::new("CA one").issue("executor-1", &["executor-1"]);
        let impostor = TestCa::new("CA two").issue("executor-1", &["executor-1"]);
        assert_ne!(
            PeerIdentity::from_certificate(&executor),
            PeerIdentity::from_certificate(&impostor)
        );
    }

    #[test]
    fn a_certificate_names_the_hosts_a_tls_client_would_accept_it_for() {
        let ca = TestCa::new("cluster CA");
        let certificate = ca.issue(
            "executor-1",
            &[
                "executor-1.cluster.local",
                "*.pods.cluster.local",
                "10.0.0.5",
                "::1",
            ],
        );
        for host in [
            "executor-1.cluster.local",
            "EXECUTOR-1.cluster.local",
            "a.pods.cluster.local",
            "10.0.0.5",
            "::1",
            "[::1]",
        ] {
            assert!(certificate_names_host(&certificate, host), "{host}");
        }
        for host in [
            "executor-2.cluster.local",
            "a.b.pods.cluster.local",
            "10.0.0.6",
            "scheduler",
            "",
            "not a host",
        ] {
            assert!(!certificate_names_host(&certificate, host), "{host}");
        }
    }

    #[test]
    fn address_host_is_what_tls_verifies() {
        for (address, host) in [
            (
                "https://scheduler.cluster.local:50052",
                Some("scheduler.cluster.local"),
            ),
            ("http://10.0.0.5:50052", Some("10.0.0.5")),
            ("10.0.0.5:50052", Some("10.0.0.5")),
            ("scheduler", Some("scheduler")),
            ("https://[::1]:50052", Some("[::1]")),
            ("[::1]", Some("[::1]")),
            ("https://scheduler:50052/path", Some("scheduler")),
            ("https://", None),
            ("", None),
        ] {
            assert_eq!(address_host(address), host, "{address}");
        }
    }

    #[test]
    fn scheduler_identities_follow_the_scheduler_addresses() {
        let ca = TestCa::new("cluster CA");
        let one = PeerIdentity::from_certificate(&ca.issue("scheduler-1", &["s1"])).expect("id");
        let two = PeerIdentity::from_certificate(&ca.issue("scheduler-2", &["s2"])).expect("id");
        let executor =
            PeerIdentity::from_certificate(&ca.issue("executor-1", &["e1"])).expect("id");

        let schedulers = SchedulerIdentities::new();
        assert!(schedulers.is_empty());
        let both = vec![
            "https://s1:50052".to_string(),
            "https://s2:50052".to_string(),
        ];
        schedulers.record(
            &both,
            vec![
                ("https://s1:50052".to_string(), one.clone()),
                ("https://s2:50052".to_string(), two.clone()),
            ],
        );
        assert!(schedulers.contains(&one) && schedulers.contains(&two));
        assert!(!schedulers.contains(&executor));

        // An address that did not answer keeps what was seen there.
        schedulers.record(&both, vec![("https://s1:50052".to_string(), one.clone())]);
        assert!(schedulers.contains(&two));

        // An address that left the membership is forgotten.
        schedulers.record(&both[..1], vec![]);
        assert!(schedulers.contains(&one));
        assert!(!schedulers.contains(&two));
        assert_eq!(schedulers.subjects(), vec!["'CN=scheduler-1'".to_string()]);
    }
}
