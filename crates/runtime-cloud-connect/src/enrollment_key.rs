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

//! The canonical Spice Cloud enrollment key: a single-use, short-lived,
//! organization-scoped bearer secret with the shape
//! `spice-enroll-` followed by exactly 32 base64url characters
//! (`[A-Za-z0-9_-]`, 192 random bits).
//!
//! [`EnrollmentKey`] is the secret-wrapping parser every enrollment-key
//! input goes through, including `spiced --token`. Wrapping the secret in a
//! type buys three guarantees at once:
//!
//! - **Redaction**: `Debug` never prints the secret, so a key cannot leak
//!   through argument structs, error chains, traces, or panic reports.
//!   There is deliberately no `Display`, `Serialize`, or way to format one.
//! - **Validation**: only canonically-shaped values construct; a malformed
//!   value is rejected client-side (the same class the cloud answers with
//!   `400 invalid_token`) without ever being echoed back.
//! - **Zeroization**: the backing buffer is wiped on drop, so a consumed
//!   one-time key does not linger in freed heap memory.
//!
//! Prefix matching is validation and redaction only — it never confers
//! authorization; the cloud validates the secret authoritatively at enroll.

use snafu::Snafu;
use zeroize::Zeroizing;

/// Prefix every canonical enrollment key carries. Treated as a
/// secret-scanner signature; never authorization.
pub const ENROLLMENT_KEY_PREFIX: &str = "spice-enroll-";

/// Exact length of the secret part after [`ENROLLMENT_KEY_PREFIX`]:
/// 32 base64url characters carrying 192 random bits.
pub const ENROLLMENT_KEY_SECRET_LEN: usize = 32;

/// Why a value failed to parse as an enrollment key.
///
/// Deliberately coarse, and the messages never echo the rejected value —
/// a near-miss may be a real key with one character mangled.
#[derive(Debug, Snafu, Clone, Copy, PartialEq, Eq)]
pub enum InvalidEnrollmentKey {
    #[snafu(display(
        "The value is not an enrollment key: it does not start with '{ENROLLMENT_KEY_PREFIX}'. \
         Mint an enrollment key in the Spice Cloud portal and pass it verbatim. \
         See: https://spiceai.org/docs"
    ))]
    WrongPrefix,

    #[snafu(display(
        "The value is not a valid enrollment key: expected exactly \
         {ENROLLMENT_KEY_SECRET_LEN} characters after '{ENROLLMENT_KEY_PREFIX}'. \
         Copy the key exactly as the Spice Cloud portal shows it. \
         See: https://spiceai.org/docs"
    ))]
    WrongLength,

    #[snafu(display(
        "The value is not a valid enrollment key: it contains characters outside \
         letters, digits, '-', and '_' after '{ENROLLMENT_KEY_PREFIX}'. \
         Copy the key exactly as the Spice Cloud portal shows it. \
         See: https://spiceai.org/docs"
    ))]
    InvalidCharacter,
}

/// A parsed, canonically-shaped enrollment key.
///
/// Construct with [`EnrollmentKey::parse`]; read with
/// [`EnrollmentKey::expose_secret`] — named so every place the plaintext
/// leaves the wrapper is greppable.
#[derive(Clone)]
pub struct EnrollmentKey(Zeroizing<String>);

impl EnrollmentKey {
    /// Parse a raw value as a canonical enrollment key.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidEnrollmentKey`] when the value does not have the
    /// canonical `spice-enroll-` + 32 base64url-character shape. The error
    /// never contains the rejected value.
    pub fn parse(raw: &str) -> Result<Self, InvalidEnrollmentKey> {
        let Some(secret) = raw.strip_prefix(ENROLLMENT_KEY_PREFIX) else {
            return Err(InvalidEnrollmentKey::WrongPrefix);
        };
        if secret.len() != ENROLLMENT_KEY_SECRET_LEN {
            return Err(InvalidEnrollmentKey::WrongLength);
        }
        if !secret
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(InvalidEnrollmentKey::InvalidCharacter);
        }
        Ok(Self(Zeroizing::new(raw.to_string())))
    }

    /// The full plaintext key (`spice-enroll-…`), for placing in the one
    /// enrollment request that consumes it.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for EnrollmentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EnrollmentKey({ENROLLMENT_KEY_PREFIX}[REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_key() -> String {
        format!("{ENROLLMENT_KEY_PREFIX}{}", "A".repeat(32))
    }

    #[test]
    fn parses_a_canonical_key() {
        let key = EnrollmentKey::parse(&valid_key()).expect("canonical key parses");
        assert_eq!(key.expose_secret(), valid_key());
    }

    #[test]
    fn accepts_the_full_base64url_charset() {
        let raw = format!("{ENROLLMENT_KEY_PREFIX}Az09-_Az09-_Az09-_Az09-_Az09-_Az");
        let key = EnrollmentKey::parse(&raw).expect("base64url charset parses");
        assert_eq!(key.expose_secret(), raw);
    }

    #[test]
    fn rejects_surrounding_whitespace() {
        for raw in [format!(" {}", valid_key()), format!("{}\n", valid_key())] {
            assert!(
                EnrollmentKey::parse(&raw).is_err(),
                "the canonical key must be passed verbatim"
            );
        }
    }

    #[test]
    fn rejects_a_wrong_prefix() {
        for raw in [
            "",
            "spice-enroll",                                  // no trailing dash
            "SPICE-ENROLL-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", // case-sensitive
            "not-an-enrollment-key",
        ] {
            assert_eq!(
                EnrollmentKey::parse(raw)
                    .map(|_| ())
                    .expect_err("must fail"),
                InvalidEnrollmentKey::WrongPrefix,
                "{raw:?} must be rejected for its prefix"
            );
        }
    }

    #[test]
    fn rejects_wrong_secret_lengths() {
        for len in [0, 31, 33, 64] {
            let raw = format!("{ENROLLMENT_KEY_PREFIX}{}", "a".repeat(len));
            assert_eq!(
                EnrollmentKey::parse(&raw)
                    .map(|_| ())
                    .expect_err("must fail"),
                InvalidEnrollmentKey::WrongLength,
                "{len}-char secret must be rejected"
            );
        }
    }

    #[test]
    fn rejects_characters_outside_base64url() {
        for bad in ["+", "/", "=", " ", ".", "£"] {
            let raw = format!("{ENROLLMENT_KEY_PREFIX}{}{bad}", "a".repeat(31));
            assert!(
                EnrollmentKey::parse(&raw).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn interior_whitespace_is_not_trimmed() {
        let raw = format!("{ENROLLMENT_KEY_PREFIX}aaaaaaaaaaaaaaaa aaaaaaaaaaaaaaa");
        assert_eq!(
            EnrollmentKey::parse(&raw)
                .map(|_| ())
                .expect_err("must fail"),
            InvalidEnrollmentKey::InvalidCharacter
        );
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let key = EnrollmentKey::parse(&valid_key()).expect("parse");
        let debug = format!("{key:?}");
        assert!(
            !debug.contains(&"A".repeat(32)),
            "Debug leaked the secret: {debug}"
        );
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn parse_errors_never_echo_the_value() {
        // A near-miss may be a real key with one mangled character, so the
        // error must not reproduce it.
        let near_miss = format!("{ENROLLMENT_KEY_PREFIX}{}+", "B".repeat(31));
        let err = EnrollmentKey::parse(&near_miss).expect_err("must fail");
        assert!(
            !err.to_string().contains(&"B".repeat(16)),
            "the parse error must not echo the rejected value: {err}"
        );
    }
}
