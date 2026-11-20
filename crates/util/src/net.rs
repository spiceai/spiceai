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

//! Loopback: addresses and host names that only processes on this machine can reach.
//!
//! One definition, shared by every check that treats loopback differently from the
//! network: `127.0.0.0/8`, `::1` (and `::ffff:127.0.0.0/104`, the same IPv4 addresses
//! mapped into IPv6), `localhost` and any name under `.localhost`. The unspecified addresses
//! `0.0.0.0` and `::` are not loopback: a listener bound to one accepts connections on every
//! interface, and a client sending to one reaches whatever answers there.

use std::net::{IpAddr, SocketAddr};

/// The suffix of a name under the special-use `localhost` domain (RFC 6761).
const LOCALHOST_SUFFIX: &str = ".localhost";

/// Whether `ip` is a loopback address: in `127.0.0.0/8`, or `::1`. An IPv4 address mapped
/// into IPv6 (`::ffff:127.0.0.1`) is the IPv4 address it maps.
#[must_use]
pub fn is_loopback_ip(ip: IpAddr) -> bool {
    ip.to_canonical().is_loopback()
}

/// Whether `addr`, a listener's bind address or a peer's address, is loopback (see
/// [`is_loopback_ip`]).
#[must_use]
pub fn is_loopback_addr(addr: &SocketAddr) -> bool {
    is_loopback_ip(addr.ip())
}

/// Whether `host`, a host name or IP literal without a port (as a URL's host or a `Host`
/// header's name), names this machine: `localhost`, a name under `.localhost`, or a
/// loopback address (see [`is_loopback_ip`]), with or without the brackets an IPv6 literal
/// takes in a URL. Names compare case-insensitively and may end in the root `.`.
///
/// A host that still carries a port (`localhost:8090`) is not recognized, so a caller that
/// forgets to strip it gets the answer for a remote host.
#[must_use]
pub fn is_loopback_host(host: &str) -> bool {
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    if let Ok(ip) = unbracketed.parse::<IpAddr>() {
        return is_loopback_ip(ip);
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // A name under `.localhost` has at least one non-empty label before the suffix.
    let Some(start) = name.len().checked_sub(LOCALHOST_SUFFIX.len()) else {
        return false;
    };
    let (Some(label), Some(suffix)) = (name.get(..start), name.get(start..)) else {
        return false;
    };
    suffix.eq_ignore_ascii_case(LOCALHOST_SUFFIX) && !label.is_empty() && !label.ends_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The edges of `127.0.0.0/8` and `::1`, their IPv4-mapped forms, and the unspecified
    /// addresses, which are not loopback.
    #[test]
    fn loopback_ip_boundaries() {
        for ip in [
            "127.0.0.0",
            "127.0.0.1",
            "127.0.0.2",
            "127.255.255.255",
            "::1",
            "0:0:0:0:0:0:0:1",
            "::ffff:127.0.0.1",
            "::ffff:127.255.255.255",
        ] {
            assert!(
                is_loopback_ip(ip.parse().expect("an IP literal")),
                "{ip} is loopback"
            );
        }
        for ip in [
            "0.0.0.0",
            "::",
            "::ffff:0.0.0.0",
            "126.255.255.255",
            "128.0.0.0",
            "10.0.0.1",
            "169.254.169.254",
            "192.168.1.1",
            "255.255.255.255",
            "::2",
            "::127.0.0.1",
            "fe80::1",
            "fc00::1",
            "2001:db8::1",
            "::ffff:126.255.255.255",
            "::ffff:128.0.0.0",
            "::ffff:10.0.0.1",
        ] {
            assert!(
                !is_loopback_ip(ip.parse().expect("an IP literal")),
                "{ip} is not loopback"
            );
        }
    }

    #[test]
    fn loopback_bind_addresses() {
        for addr in ["127.0.0.1:8090", "127.0.0.42:0", "[::1]:50051"] {
            assert!(
                is_loopback_addr(&addr.parse().expect("a socket address")),
                "{addr} is loopback"
            );
        }
        for addr in [
            "0.0.0.0:8090",
            "[::]:8090",
            "10.1.2.3:8090",
            "[::ffff:0.0.0.0]:1",
        ] {
            assert!(
                !is_loopback_addr(&addr.parse().expect("a socket address")),
                "{addr} is not loopback"
            );
        }
    }

    #[test]
    fn loopback_host_boundaries() {
        for host in [
            "localhost",
            "LocalHost",
            "LOCALHOST",
            "localhost.",
            "app.localhost",
            "app.localhost.",
            "a.b.LOCALHOST",
            "127.0.0.1",
            "127.1.2.3",
            "127.255.255.255",
            "::1",
            "[::1]",
            "[0:0:0:0:0:0:0:1]",
            "[::ffff:127.0.0.1]",
        ] {
            assert!(is_loopback_host(host), "{host} is loopback");
        }
        for host in [
            "",
            ".",
            "0.0.0.0",
            "::",
            "[::]",
            "128.0.0.1",
            "126.255.255.255",
            "10.0.0.1",
            "[10.0.0.1]",
            "example.com",
            "localhost.example.com",
            "localhost.localdomain",
            "notlocalhost",
            "localhostx",
            ".localhost",
            "..localhost",
            "localhost..",
            "app..localhost",
            "[::1",
            "::1]",
            "[localhost]",
            "localhost:8090",
            "127.0.0.1:8090",
            "[::1]:8090",
        ] {
            assert!(!is_loopback_host(host), "{host} is not loopback");
        }
    }
}
