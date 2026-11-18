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

//! Server-side request forgery (SSRF) protection for HTTP requests whose destination a query
//! chooses.
//!
//! One classifier decides which addresses and host names are internal. URL tables apply it to
//! the URL a query names, to every address that URL's host resolves to, and to every redirect;
//! the HTTPS data connector applies it to every redirect that changes the origin and to every
//! address a redirect's new host resolves to, and once the dataset's own host first resolves to
//! a public address, to every later answer for that host. The policy itself is the
//! `http-egress` crate's, shared by every client that applies these guards.

pub use http_egress::{is_blocked_internal_hostname, is_internal_ip, redirect_target_is_internal};
use object_store_spawn::DestinationGuard;
use url::Url;

/// Environment variable that, when set to a truthy value (`1`/`true`/`yes`/`on`),
/// disables the SSRF guard on `http`/`https` URL tables, on redirects to another
/// host from `http`/`https` datasets in file formats such as Parquet or CSV, and
/// on storage endpoints and `file://` locations supplied by an Iceberg REST
/// catalog, allowing them to target private, loopback, and link-local addresses
/// or the local filesystem.
/// Off by default — intended only for operators who knowingly query an internal
/// HTTP object store or a local Iceberg warehouse.
pub const ALLOW_PRIVATE_HOSTS_ENV: &str = "SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS";

/// Whether the SSRF guard is disabled via [`ALLOW_PRIVATE_HOSTS_ENV`].
#[must_use]
pub fn allow_private_hosts() -> bool {
    std::env::var(ALLOW_PRIVATE_HOSTS_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Scheme prefix under which `http`/`https` URL tables register their object stores.
///
/// A URL table's store refuses internal destinations; a dataset's store for the same host does
/// not. Registering the two under different schemes keeps them in separate registry entries, so
/// neither is ever served in place of the other.
pub const URL_TABLE_SCHEME_PREFIX: &str = "spice-url-table+";

/// The address a URL table whose store is registered under `url` actually reads, or `None`
/// when `url` is not a URL table's store URL.
#[must_use]
pub fn url_table_target(url: &Url) -> Option<Url> {
    let target = url.as_str().strip_prefix(URL_TABLE_SCHEME_PREFIX)?;
    let target = Url::parse(target).ok()?;
    matches!(target.scheme(), "http" | "https").then_some(target)
}

/// What a URL table's HTTP store refuses: any internal resolved address, and any redirect to
/// an internal IP literal or a well-known internal name.
///
/// A proxy from `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` is resolved by the same guard, so a proxy
/// on a private address is refused unless `SPICE_URL_TABLE_ALLOW_PRIVATE_HOSTS=true` is set. The
/// guard does not see the address a proxy resolves the URL to; the URL's own pre-flight check
/// and the redirect check still apply.
#[must_use]
pub fn url_table_destination_guard() -> DestinationGuard {
    DestinationGuard::new(
        is_internal_ip,
        redirect_target_is_internal,
        format!(
            "URL tables cannot read from private, loopback, or link-local addresses, including a proxy host that resolves to one. If this runtime intentionally queries an internal HTTP endpoint or uses a private proxy, set {ALLOW_PRIVATE_HOSTS_ENV}=true"
        ),
    )
}

/// What the HTTPS data connector refuses for a destination a redirect introduced: an internal
/// address, including another port or scheme on a dataset host that resolves to one.
#[must_use]
pub fn redirect_destination_guard() -> DestinationGuard {
    connector_redirect_guard("HTTPS")
}

/// What the data connector named `connector` refuses for a destination a redirect introduced,
/// as [`redirect_destination_guard`] does for the HTTPS data connector.
#[must_use]
pub fn connector_redirect_guard(connector: &str) -> DestinationGuard {
    DestinationGuard::new(
        is_internal_ip,
        redirect_target_is_internal,
        format!(
            "the {connector} data connector does not follow a redirect to a private, loopback, or link-local address"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::IpAddr, str::FromStr};

    #[test]
    fn test_is_internal_ip_blocks_dangerous_ranges() {
        // Must be blocked (SSRF targets).
        for ip in [
            "127.0.0.1",
            "127.1.2.3",
            "169.254.169.254", // AWS/GCP/Azure IMDS
            "10.0.0.5",
            "192.168.1.1",
            "172.16.0.1",
            "172.31.255.255",
            "100.64.0.1", // CGNAT
            "0.0.0.0",
            "198.18.0.1", // benchmarking
            "240.0.0.1",  // reserved
            "255.255.255.255",
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // IPv6 internal ranges.
        for ip in [
            "::1",                    // loopback
            "::",                     // unspecified
            "fe80::1",                // link local
            "fc00::1",                // unique local
            "fd12:3456::1",           // unique local
            "::ffff:169.254.169.254", // v4-mapped metadata
            "::ffff:127.0.0.1",       // v4-mapped loopback
            "ff02::1",                // multicast
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(is_internal_ip(addr), "{ip} should be treated as internal");
        }

        // Must be allowed (legitimate public addresses).
        for ip in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",        // example.com
            "2606:4700:4700::1111", // Cloudflare DNS
        ] {
            let addr = IpAddr::from_str(ip).expect("valid ip");
            assert!(!is_internal_ip(addr), "{ip} should be treated as public");
        }
    }

    #[test]
    fn test_is_blocked_internal_hostname() {
        // Well-known internal names blocked without DNS (case-insensitive).
        for host in [
            "localhost",
            "LOCALHOST",
            "service.localhost",
            "metadata.google.internal",
            "metadata.goog",
        ] {
            assert!(
                is_blocked_internal_hostname(host),
                "{host} should be blocked"
            );
        }

        // Legitimate public hostnames must pass (DNS resolution handles these).
        for host in [
            "example.com",
            "localhost.example.com", // not the `.localhost` TLD
            "metadata.google.com",   // not the metadata sentinel
        ] {
            assert!(
                !is_blocked_internal_hostname(host),
                "{host} should not be blocked by literal match"
            );
        }
    }

    #[test]
    fn redirect_target_is_internal_classifies_each_host_form() {
        for url in [
            "http://169.254.169.254/latest/meta-data/",
            "http://172.30.0.2:18077/secret.parquet",
            "https://[::1]/data",
            "http://[::ffff:10.0.0.1]/data",
            "http://localhost:8080/data",
            "http://metadata.google.internal/computeMetadata/v1/",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert!(redirect_target_is_internal(&url), "{url} must be refused");
        }
        for url in [
            "https://8.8.8.8/data.parquet",
            "https://example.com/data.parquet",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert!(!redirect_target_is_internal(&url), "{url} must be allowed");
        }
    }

    #[test]
    fn url_table_target_round_trips_only_http_urls() {
        let store_url =
            Url::parse("spice-url-table+https://example.com:8443/a/b.csv#client_timeout=5s")
                .expect("valid URL");
        assert_eq!(
            url_table_target(&store_url).map(String::from),
            Some("https://example.com:8443/a/b.csv#client_timeout=5s".to_string())
        );

        for url in [
            "https://example.com/a.csv",
            "spice-url-table+s3://bucket/a.csv",
            "spice-url-table+file:///etc/passwd",
        ] {
            let url = Url::parse(url).expect("valid URL");
            assert_eq!(
                url_table_target(&url),
                None,
                "{url} is not a URL table store"
            );
        }
    }
}
