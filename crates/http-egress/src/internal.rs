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

//! Which addresses and host names are internal: the one classifier every guard in Spice applies.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use url::Url;

/// Returns `true` if `ip` is in a range that must not be reachable through a
/// user-supplied URL: loopback, private/RFC1918, link-local (incl. the
/// `169.254.169.254` cloud-metadata endpoint), unique-local, CGNAT,
/// unspecified, multicast, and other reserved ranges.
#[must_use]
pub fn is_internal_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_internal_ipv4(v4),
        // Unwrap IPv4-mapped/-compatible addresses (e.g. `::ffff:169.254.169.254`)
        // and re-check them as IPv4 so they can't bypass the v4 rules.
        IpAddr::V6(v6) => match v6.to_ipv4() {
            Some(v4) => is_internal_ipv4(v4),
            None => is_internal_ipv6(v6),
        },
    }
}

fn is_internal_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    // Only the four oldest, universally-stable classifiers are used as methods;
    // the rest are explicit prefix checks so behavior is obvious and portable.
    ip.is_loopback()            // 127.0.0.0/8
        || ip.is_private()      // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()   // 169.254.0.0/16 (incl. 169.254.169.254 metadata)
        || ip.is_unspecified()  // 0.0.0.0
        || o[0] == 0                                 // 0.0.0.0/8 "this network"
        || (o[0] == 100 && (o[1] & 0xc0) == 64)      // 100.64.0.0/10 CGNAT
        || (o[0] == 198 && (o[1] & 0xfe) == 18)      // 198.18.0.0/15 benchmarking
        || (o[0] == 192 && o[1] == 0 && o[2] == 2)   // 192.0.2.0/24 TEST-NET-1
        || (o[0] == 198 && o[1] == 51 && o[2] == 100) // 198.51.100.0/24 TEST-NET-2
        || (o[0] == 203 && o[1] == 0 && o[2] == 113) // 203.0.113.0/24 TEST-NET-3
        || o[0] >= 224 // 224.0.0.0/4 multicast, 240.0.0.0/4 reserved, 255.255.255.255 broadcast
}

fn is_internal_ipv6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()                  // ::1
        || ip.is_unspecified()        // ::
        || (first & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (first & 0xffc0) == 0xfe80 // fe80::/10 link local
        || (first & 0xff00) == 0xff00 // ff00::/8 multicast
}

/// Returns `true` for hostnames that never legitimately point at a remote
/// endpoint and must be blocked without needing DNS resolution: `localhost`
/// (and `*.localhost`, RFC 6761) and the well-known cloud-metadata names.
#[must_use]
pub fn is_blocked_internal_hostname(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "localhost"
        || lower.ends_with(".localhost")
        || lower == "metadata.google.internal"
        || lower == "metadata.goog"
}

/// Whether a redirect to `url` must be refused because its host is an internal IP literal or
/// a well-known internal name.
///
/// This check needs no DNS, so it can run inside a synchronous redirect policy. A host name it
/// lets through can still resolve to an internal address; a guard also checks every resolved
/// address.
#[must_use]
pub fn redirect_target_is_internal(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => is_internal_ip(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => is_internal_ip(IpAddr::V6(ip)),
        Some(url::Host::Domain(domain)) => is_blocked_internal_hostname(domain),
        None => false,
    }
}
