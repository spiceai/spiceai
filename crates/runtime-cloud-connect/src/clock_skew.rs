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

//! Host clock-skew measurement and diagnosis for the enroll/renew/connect
//! paths.
//!
//! A wrong host clock is the most common self-hosted setup failure, and every
//! layer of Cloud Connect reports it the same unhelpful way: X.509 validity is
//! evaluated against the local clock, so a host hours behind rejects the
//! cloud's perfectly valid server certificate, refuses its own freshly-issued
//! leaf, and fails the gateway handshake — each with a bare "certificate has
//! expired". This module turns that into the measured offset and the fix.
//!
//! Skew is measured from the `Date` header of an HTTPS response, which every
//! Spice Cloud response carries. When the TLS handshake itself fails there is
//! no response to read, so [`diagnose`] makes one deliberately
//! verification-free request whose *only* use is that header — see its docs
//! for why that is safe.

use std::time::Duration;

/// Offset past which a skew is worth reporting. NTP-synchronised hosts sit
/// within milliseconds of true time, and certificate validity is never decided
/// by a second either way, so anything past a minute is a misconfigured clock
/// rather than normal drift.
pub const SIGNIFICANT_SKEW: Duration = Duration::from_mins(1);

/// A measured difference between this host's clock and Spice Cloud's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSkew {
    /// Host clock minus cloud clock, in seconds. Positive means this host is
    /// *ahead* of the cloud, negative means *behind*.
    offset_secs: i64,
}

impl ClockSkew {
    /// Measure the skew between `local_unix` (this host) and `server_unix`
    /// (the cloud), both Unix seconds.
    #[must_use]
    pub fn measure(local_unix: i64, server_unix: i64) -> Self {
        Self {
            offset_secs: local_unix.saturating_sub(server_unix),
        }
    }

    /// Host clock minus cloud clock, in seconds (positive: host is ahead).
    #[must_use]
    pub fn offset_secs(self) -> i64 {
        self.offset_secs
    }

    /// `true` when the offset is large enough to be a misconfigured clock
    /// rather than ordinary drift (see [`SIGNIFICANT_SKEW`]).
    #[must_use]
    pub fn is_significant(self) -> bool {
        let significant = i64::try_from(SIGNIFICANT_SKEW.as_secs()).unwrap_or(i64::MAX);
        self.offset_secs.saturating_abs() >= significant
    }

    /// Human-readable direction and magnitude, e.g.
    /// `host clock is 42 minutes behind Spice Cloud`.
    #[must_use]
    pub fn describe(self) -> String {
        let direction = if self.offset_secs >= 0 {
            "ahead of"
        } else {
            "behind"
        };
        format!(
            "host clock is {} {direction} Spice Cloud",
            humanize_secs(self.offset_secs.unsigned_abs())
        )
    }

    /// [`Self::describe`] plus the action that fixes it. This is the string
    /// that goes in front of the customer, so it names the offset and one
    /// concrete command.
    #[must_use]
    pub fn advice(self) -> String {
        format!(
            "{} — enable NTP time synchronization on this host (for example \
             `sudo timedatectl set-ntp true`) and retry",
            self.describe()
        )
    }
}

/// Render a whole number of seconds as the coarsest unit that still reads
/// precisely (`45 seconds`, `42 minutes`, `3 hours 5 minutes`, `2 days`).
fn humanize_secs(secs: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    let plural = |n: u64, unit: &str| {
        if n == 1 {
            format!("1 {unit}")
        } else {
            format!("{n} {unit}s")
        }
    };

    if secs < MINUTE {
        return plural(secs, "second");
    }
    if secs < HOUR {
        return plural(secs / MINUTE, "minute");
    }
    if secs < DAY {
        let hours = secs / HOUR;
        let minutes = (secs % HOUR) / MINUTE;
        if minutes == 0 {
            return plural(hours, "hour");
        }
        return format!("{} {}", plural(hours, "hour"), plural(minutes, "minute"));
    }
    let days = secs / DAY;
    let hours = (secs % DAY) / HOUR;
    if hours == 0 {
        return plural(days, "day");
    }
    format!("{} {}", plural(days, "day"), plural(hours, "hour"))
}

/// Current Unix time from the host clock, in seconds. A clock set before the
/// epoch reads as `0` — which any real cloud `Date` will dwarf, so the skew
/// still comes out large and in the right direction.
#[must_use]
pub fn local_unix_now() -> i64 {
    let now = std::time::SystemTime::now();
    match now.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
        Err(err) => -i64::try_from(err.duration().as_secs()).unwrap_or(i64::MAX),
    }
}

/// Parse an HTTP `Date` header (RFC 7231 §7.1.1.1 — the RFC 2822 form) and
/// measure the skew against this host's clock. Returns `None` when the header
/// is absent or unparseable, so a proxy that rewrites it can never turn a real
/// failure into a bogus clock diagnosis.
#[must_use]
pub fn from_date_header(date_header: &str) -> Option<ClockSkew> {
    let server = chrono::DateTime::parse_from_rfc2822(date_header.trim()).ok()?;
    Some(ClockSkew::measure(local_unix_now(), server.timestamp()))
}

/// `true` when this transport failure is a TLS certificate-*validity* rejection
/// — the shape a skewed clock produces — rather than an untrusted issuer, a
/// hostname mismatch, or a connection failure.
///
/// Matched on the error chain's rendered text because neither `reqwest` nor
/// `rustls` exposes the alert as a typed variant through `reqwest::Error`. The
/// match is deliberately narrow: a false positive would blame the clock for an
/// unrelated TLS problem.
#[must_use]
pub fn looks_like_certificate_validity_failure(err: &(dyn std::error::Error + 'static)) -> bool {
    /// Substrings `rustls`/`webpki` use for the two validity alerts, plus the
    /// `native-tls` phrasings — all time-flavoured, all lowercased. A
    /// hostname mismatch ("certificate is not valid for <host>") matches none
    /// of them, which is the point.
    const VALIDITY_MARKERS: &[&str] = &[
        "certificate has expired",
        "certificateexpired",
        "cert_expired",
        "expiredcertificate",
        "certificate is not valid yet",
        "certificatenotvalidyet",
        "not yet valid",
    ];

    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = source {
        let text = err.to_string().to_ascii_lowercase();
        if VALIDITY_MARKERS.iter().any(|marker| text.contains(marker)) {
            return true;
        }
        source = err.source();
    }
    false
}

/// Measure this host's clock skew against `base_url`, for the path where the
/// TLS handshake itself failed and no verified response is available.
///
/// # Why this ignores certificate validation
///
/// The caller reaches here precisely because certificate verification failed,
/// so a verifying client would fail identically and diagnose nothing. This
/// request therefore disables verification — and is safe to do so because it
/// is *only* a clock probe:
///
/// - it is a bare `GET` on the base URL that sends no enrollment authority, no
///   identity, no bearer token, and no body;
/// - the response body is discarded unread — only the `Date` header is used;
/// - the result feeds a diagnostic message and nothing else. A hostile `Date`
///   can make the message wrong, never the enrollment.
///
/// Returns `None` when the probe cannot be made or the header is missing, in
/// which case the caller reports the certificate failure without a measurement.
pub async fn diagnose(base_url: &str, ca_cert_pem: Option<&str>) -> Option<ClockSkew> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(5))
        // See the safety argument above: the probe carries no credential and
        // its only output is the `Date` header.
        .danger_accept_invalid_certs(true);
    if let Some(ca_pem) = ca_cert_pem
        && let Ok(certs) = reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes())
    {
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    let client = builder.build().ok()?;
    let response = client.get(base_url).send().await.ok()?;
    let date = response
        .headers()
        .get(reqwest::header::DATE)?
        .to_str()
        .ok()?;
    from_date_header(date)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measure_reports_direction() {
        // Host ahead of the cloud.
        let ahead = ClockSkew::measure(1_000_600, 1_000_000);
        assert_eq!(ahead.offset_secs(), 600);
        assert!(ahead.describe().contains("10 minutes ahead of"));

        // Host behind the cloud — the case the spec's example message names.
        let behind = ClockSkew::measure(1_000_000, 1_000_000 + 42 * 60);
        assert_eq!(behind.offset_secs(), -42 * 60);
        assert_eq!(
            behind.describe(),
            "host clock is 42 minutes behind Spice Cloud"
        );
        assert!(behind.advice().contains("NTP"));
    }

    #[test]
    fn significance_threshold_is_symmetric() {
        assert!(!ClockSkew::measure(0, 0).is_significant());
        assert!(!ClockSkew::measure(59, 0).is_significant());
        assert!(!ClockSkew::measure(0, 59).is_significant());
        assert!(ClockSkew::measure(60, 0).is_significant());
        assert!(ClockSkew::measure(0, 60).is_significant());
    }

    #[test]
    fn humanize_picks_the_coarsest_precise_unit() {
        assert_eq!(humanize_secs(1), "1 second");
        assert_eq!(humanize_secs(45), "45 seconds");
        assert_eq!(humanize_secs(60), "1 minute");
        assert_eq!(humanize_secs(42 * 60), "42 minutes");
        assert_eq!(humanize_secs(3 * 3600), "3 hours");
        assert_eq!(humanize_secs(3 * 3600 + 5 * 60), "3 hours 5 minutes");
        assert_eq!(humanize_secs(2 * 86400), "2 days");
        assert_eq!(humanize_secs(86400 + 3600), "1 day 1 hour");
    }

    #[test]
    fn date_header_parses_rfc2822() {
        // A fixed cloud date with the host's own clock: the sign must follow
        // whichever side is later, so assert against a computed expectation
        // rather than a hardcoded offset.
        let skew = from_date_header("Wed, 29 Jul 2026 12:00:00 GMT").expect("parses");
        let expected = ClockSkew::measure(local_unix_now(), 1_785_326_400);
        // Allow a second of wall-clock movement between the two measurements.
        assert!(
            (skew.offset_secs() - expected.offset_secs()).abs() <= 1,
            "{} vs {}",
            skew.offset_secs(),
            expected.offset_secs()
        );
    }

    #[test]
    fn date_header_rejects_garbage() {
        assert!(from_date_header("").is_none());
        assert!(from_date_header("tomorrow-ish").is_none());
        // An RFC 3339 timestamp is NOT the HTTP `Date` form; refusing it keeps
        // a rewriting proxy from producing a bogus measurement.
        assert!(from_date_header("2026-07-29T12:00:00Z").is_none());
    }

    /// Minimal error type for exercising the source-chain walk.
    #[derive(Debug)]
    struct Chained {
        message: String,
        source: Option<Box<Chained>>,
    }

    impl std::fmt::Display for Chained {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.message)
        }
    }

    impl std::error::Error for Chained {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_deref()
                .map(|s| s as &(dyn std::error::Error + 'static))
        }
    }

    fn chain(messages: &[&str]) -> Chained {
        let mut source = None;
        for message in messages.iter().rev() {
            source = Some(Box::new(Chained {
                message: (*message).to_string(),
                source: source.take(),
            }));
        }
        *source.expect("at least one message")
    }

    #[test]
    fn detects_validity_failure_anywhere_in_the_chain() {
        let err = chain(&[
            "error sending request",
            "invalid peer certificate: Expired",
            "certificate has expired",
        ]);
        assert!(looks_like_certificate_validity_failure(&err));

        let not_yet = chain(&["invalid peer certificate: CertificateNotValidYet"]);
        assert!(looks_like_certificate_validity_failure(&not_yet));
    }

    #[test]
    fn does_not_blame_the_clock_for_other_tls_failures() {
        // An untrusted issuer, a hostname mismatch, and a plain connection
        // failure must not be reported as clock skew.
        for messages in [
            &["invalid peer certificate: UnknownIssuer"][..],
            &["certificate is not valid for name cloud.example"][..],
            &["error sending request", "connection refused"][..],
            &["invalid peer certificate: BadSignature"][..],
        ] {
            let err = chain(messages);
            assert!(
                !looks_like_certificate_validity_failure(&err),
                "{messages:?} must not be diagnosed as clock skew"
            );
        }
    }
}
