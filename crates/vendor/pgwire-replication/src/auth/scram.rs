//! SCRAM-SHA-256 authentication implementation.
//!
//! This module implements the SCRAM-SHA-256 authentication mechanism as specified
//! in RFC 5802 and RFC 7677, used by PostgreSQL for secure password authentication.
//!
//! # Protocol Overview
//!
//! SCRAM (Salted Challenge Response Authentication Mechanism) provides:
//! - Password never sent in plaintext
//! - Mutual authentication (client verifies server)
//! - Protection against replay attacks via nonces
//!
//! # Example Flow
//!
//! ```no_run
//! use pgwire_replication::auth::scram::ScramClient;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = ScramClient::new("postgres");
//!
//!     // Send to server: client.client_first.as_bytes()
//!     let server_first = String::new(); // received from server
//!
//!     let (client_final, auth_msg, salted_pw) =
//!         client.client_final("password", &server_first)?;
//!
//!     // Send to server: client_final.as_bytes()
//!     let server_final = String::new(); // received from server
//!
//!     ScramClient::verify_server_final(&server_final, &salted_pw, &auth_msg)?;
//!     Ok(())
//! }
//! ```

#[cfg(feature = "scram")]
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
#[cfg(feature = "scram")]
use hmac::{Hmac, Mac};
#[cfg(feature = "scram")]
use rand::RngCore;
#[cfg(feature = "scram")]
use sha2::{Digest, Sha256};

use crate::error::{PgWireError, Result};

#[cfg(feature = "scram")]
type HmacSha256 = Hmac<Sha256>;

/// SCRAM-SHA-256 client state.
///
/// Holds the client nonce and first message needed for the authentication exchange.
#[cfg(feature = "scram")]
#[derive(Debug, Clone)]
pub struct ScramClient {
    /// Base64-encoded client nonce (18 random bytes)
    pub client_nonce_b64: String,
    /// Client-first-message-bare (without channel binding prefix)
    pub client_first_bare: String,
    /// Complete client-first-message to send to server
    pub client_first: String,
}

#[cfg(feature = "scram")]
impl ScramClient {
    /// Create a new SCRAM client with a random nonce.
    ///
    /// # Arguments
    /// * `username` - PostgreSQL username (will be SASL-escaped)
    pub fn new(username: &str) -> ScramClient {
        let mut nonce = [0u8; 18];
        rand::rng().fill_bytes(&mut nonce);
        let nonce_b64 = B64.encode(nonce);

        let user = sasl_escape_username(username);
        let client_first_bare = format!("n={user},r={nonce_b64}");
        let client_first = format!("n,,{client_first_bare}");

        ScramClient {
            client_nonce_b64: nonce_b64,
            client_first_bare,
            client_first,
        }
    }

    /// Create a SCRAM client with a specific nonce (for testing).
    #[cfg(test)]
    pub(crate) fn with_nonce(username: &str, nonce_b64: &str) -> ScramClient {
        let user = sasl_escape_username(username);
        let client_first_bare = format!("n={user},r={nonce_b64}");
        let client_first = format!("n,,{client_first_bare}");

        ScramClient {
            client_nonce_b64: nonce_b64.to_string(),
            client_first_bare,
            client_first,
        }
    }

    /// Parse server-first-message.
    ///
    /// Extracts:
    /// - `r`: Combined nonce (client nonce + server nonce)
    /// - `s`: Base64-encoded salt
    /// - `i`: Iteration count
    ///
    /// # Errors
    /// Returns error if any required field is missing or malformed.
    pub fn parse_server_first(server_first: &str) -> Result<(String, String, u32)> {
        let mut r = None;
        let mut s = None;
        let mut i = None;

        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                r = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("s=") {
                s = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("i=") {
                i = v.parse::<u32>().ok();
            }
        }

        Ok((
            r.ok_or_else(|| PgWireError::Auth("SCRAM server-first missing nonce (r=)".into()))?,
            s.ok_or_else(|| PgWireError::Auth("SCRAM server-first missing salt (s=)".into()))?,
            i.ok_or_else(|| {
                PgWireError::Auth(
                    "SCRAM server-first missing or invalid iteration count (i=)".into(),
                )
            })?,
        ))
    }

    /// Compute client-final-message.
    ///
    /// # Arguments
    /// * `password` - User's password
    /// * `server_first` - Server-first-message received from server
    ///
    /// # Returns
    /// Tuple of:
    /// - `client_final`: Message to send to server
    /// - `auth_message`: Full auth message (needed for server verification)
    /// - `salted_password`: Derived key (needed for server verification)
    ///
    /// # Errors
    /// - Nonce doesn't start with client nonce (possible MITM)
    /// - Invalid base64 in salt
    pub fn client_final(
        &self,
        password: &str,
        server_first: &str,
    ) -> Result<(String, String, Vec<u8>)> {
        let (rnonce, salt_b64, iters) = Self::parse_server_first(server_first)?;

        // Security check: server nonce must start with our nonce
        if !rnonce.starts_with(&self.client_nonce_b64) {
            return Err(PgWireError::Auth(
                "SCRAM nonce mismatch: server nonce doesn't include client nonce".into(),
            ));
        }

        let salt = B64
            .decode(salt_b64.as_bytes())
            .map_err(|e| PgWireError::Auth(format!("SCRAM invalid salt base64: {e}")))?;

        // Channel binding for non-TLS or tls-unique not supported: "biws" = base64("n,,")
        let channel_binding = "biws";
        let client_final_wo_proof = format!("c={channel_binding},r={rnonce}");

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_wo_proof
        );

        // SCRAM key derivation
        let salted_password = hi_sha256(password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(&client_key);

        // Compute proof
        let client_sig = hmac_sha256(stored_key.as_slice(), auth_message.as_bytes());
        let proof = xor_bytes(&client_key, &client_sig);
        let proof_b64 = B64.encode(proof);

        let client_final = format!("{client_final_wo_proof},p={proof_b64}");
        Ok((client_final, auth_message, salted_password))
    }

    /// Verify server-final-message.
    ///
    /// This provides mutual authentication - ensures we're talking to a server
    /// that knows the password, not an impostor.
    ///
    /// # Arguments
    /// * `server_final` - Server-final-message received
    /// * `salted_password` - From `client_final()` return value
    /// * `auth_message` - From `client_final()` return value
    ///
    /// # Errors
    /// - Missing server signature
    /// - Invalid base64
    /// - Signature mismatch (server doesn't know password)
    pub fn verify_server_final(
        server_final: &str,
        salted_password: &[u8],
        auth_message: &str,
    ) -> Result<()> {
        // Check for error from server
        if let Some(err) = server_final.split(',').find_map(|p| p.strip_prefix("e=")) {
            return Err(PgWireError::Auth(format!("SCRAM server error: {err}")));
        }

        let v = server_final
            .split(',')
            .find_map(|p| p.strip_prefix("v="))
            .ok_or_else(|| PgWireError::Auth("SCRAM server-final missing signature (v=)".into()))?;

        let server_sig = B64.decode(v.trim().as_bytes()).map_err(|e| {
            PgWireError::Auth(format!("SCRAM invalid server signature base64: {e}"))
        })?;

        // Compute expected server signature
        let server_key = hmac_sha256(salted_password, b"Server Key");
        let expected = hmac_sha256(&server_key, auth_message.as_bytes());

        // Constant-time comparison to prevent timing attacks
        if !constant_time_eq(&server_sig, &expected) {
            return Err(PgWireError::Auth(
                "SCRAM server signature mismatch: server may not know the password".into(),
            ));
        }

        Ok(())
    }
}

/// SASL-escape a username per RFC 5802.
///
/// Escapes `=` as `=3D` and `,` as `=2C`.
#[cfg(feature = "scram")]
fn sasl_escape_username(u: &str) -> String {
    u.replace('=', "=3D").replace(',', "=2C")
}

/// Hi() function from RFC 5802 - essentially PBKDF2-HMAC-SHA256.
///
/// Derives a key from password and salt using the specified iteration count.
#[cfg(feature = "scram")]
fn hi_sha256(password: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
    // U1 = HMAC(password, salt || INT(1))
    let mut s1 = Vec::with_capacity(salt.len() + 4);
    s1.extend_from_slice(salt);
    s1.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &s1);
    let mut out = u.clone();

    // Ui = HMAC(password, U(i-1)), result = U1 XOR U2 XOR ... XOR Ui
    for _ in 1..iters {
        u = hmac_sha256(password, &u);
        for (o, ui) in out.iter_mut().zip(u.iter()) {
            *o ^= *ui;
        }
    }

    out
}

/// Compute HMAC-SHA-256.
#[cfg(feature = "scram")]
fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length is always valid");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

/// XOR two byte slices of equal length.
#[cfg(feature = "scram")]
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert_eq!(a.len(), b.len(), "XOR operands must have equal length");
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

/// Constant-time byte slice comparison.
///
/// Returns true if slices are equal, using constant-time comparison
/// to prevent timing side-channel attacks.
#[cfg(feature = "scram")]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // XOR all bytes, OR results together - any difference results in non-zero
    let result = a
        .iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y));

    result == 0
}

#[cfg(test)]
#[cfg(feature = "scram")]
mod tests {
    use super::*;

    // ==================== ScramClient::new tests ====================

    #[test]
    fn scram_builds_first_message() {
        let c = ScramClient::new("user");
        assert!(c.client_first.starts_with("n,,n=user,r="));
        assert!(c.client_first_bare.starts_with("n=user,r="));
        assert!(!c.client_nonce_b64.is_empty());
    }

    #[test]
    fn scram_escapes_special_chars_in_username() {
        let c = ScramClient::new("user=name,test");
        // = becomes =3D, , becomes =2C
        assert!(c.client_first.contains("n=user=3Dname=2Ctest,r="));
    }

    #[test]
    fn scram_unique_nonces() {
        let c1 = ScramClient::new("user");
        let c2 = ScramClient::new("user");
        assert_ne!(c1.client_nonce_b64, c2.client_nonce_b64);
    }

    // ==================== parse_server_first tests ====================

    #[test]
    fn parse_server_first_valid() {
        let (r, s, i) = ScramClient::parse_server_first("r=abc123,s=c2FsdA==,i=4096").unwrap();
        assert_eq!(r, "abc123");
        assert_eq!(s, "c2FsdA==");
        assert_eq!(i, 4096);
    }

    #[test]
    fn parse_server_first_different_order() {
        // Fields can appear in any order
        let (r, s, i) = ScramClient::parse_server_first("i=1000,s=Zm9v,r=xyz").unwrap();
        assert_eq!(r, "xyz");
        assert_eq!(s, "Zm9v");
        assert_eq!(i, 1000);
    }

    #[test]
    fn parse_server_first_with_extensions() {
        // Should ignore unknown extensions
        let (r, s, i) =
            ScramClient::parse_server_first("r=nonce,s=c2FsdA==,i=4096,x=unknown").unwrap();
        assert_eq!(r, "nonce");
        assert_eq!(i, 4096);
        let _ = s; // unused but parsed
    }

    #[test]
    fn parse_server_first_missing_nonce() {
        let err = ScramClient::parse_server_first("s=c2FsdA==,i=4096").unwrap_err();
        assert!(err.to_string().contains("nonce"));
    }

    #[test]
    fn parse_server_first_missing_salt() {
        let err = ScramClient::parse_server_first("r=abc,i=4096").unwrap_err();
        assert!(err.to_string().contains("salt"));
    }

    #[test]
    fn parse_server_first_missing_iterations() {
        let err = ScramClient::parse_server_first("r=abc,s=c2FsdA==").unwrap_err();
        assert!(err.to_string().contains("iteration"));
    }

    #[test]
    fn parse_server_first_invalid_iterations() {
        let err = ScramClient::parse_server_first("r=abc,s=c2FsdA==,i=notanumber").unwrap_err();
        assert!(err.to_string().contains("iteration"));
    }

    // ==================== client_final tests ====================

    #[test]
    fn client_final_computes_proof() {
        // Use deterministic nonce for reproducible test
        let client = ScramClient::with_nonce("user", "rOprNGfwEbeRWgbNEkqO");

        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

        let (client_final, auth_message, salted_password) =
            client.client_final("pencil", server_first).unwrap();

        // Verify structure
        assert!(client_final.starts_with("c=biws,r="));
        assert!(client_final.contains(",p="));

        // Auth message should contain all three messages
        assert!(auth_message.contains(&client.client_first_bare));
        assert!(auth_message.contains(server_first));

        // Salted password should be 32 bytes (SHA-256 output)
        assert_eq!(salted_password.len(), 32);
    }

    #[test]
    fn client_final_rejects_nonce_mismatch() {
        let client = ScramClient::with_nonce("user", "clientnonce");

        // Server returns nonce that doesn't start with client nonce
        let server_first = "r=differentnonce,s=c2FsdA==,i=4096";

        let err = client.client_final("password", server_first).unwrap_err();
        assert!(err.to_string().contains("nonce mismatch"));
    }

    #[test]
    fn client_final_rejects_invalid_salt_base64() {
        let client = ScramClient::with_nonce("user", "abc");

        let server_first = "r=abcdef,s=!!!invalid!!!,i=4096";

        let err = client.client_final("password", server_first).unwrap_err();
        assert!(err.to_string().contains("base64"));
    }

    // ==================== verify_server_final tests ====================

    #[test]
    fn verify_server_final_accepts_valid_signature() {
        // This is a complete SCRAM exchange with known values
        let client = ScramClient::with_nonce("user", "fyko+d2lbbFgONRv9qkxdawL");

        let server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";

        let (_, auth_message, salted_password) =
            client.client_final("pencil", server_first).unwrap();

        // Compute expected server signature manually
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        let server_final = format!("v={}", B64.encode(&server_sig));

        // Should succeed
        ScramClient::verify_server_final(&server_final, &salted_password, &auth_message).unwrap();
    }

    #[test]
    fn verify_server_final_rejects_wrong_signature() {
        let salted_password = vec![0u8; 32];
        let auth_message = "test";
        let server_final = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="; // wrong

        let err = ScramClient::verify_server_final(server_final, &salted_password, auth_message)
            .unwrap_err();
        assert!(err.to_string().contains("signature mismatch"));
    }

    #[test]
    fn verify_server_final_rejects_missing_signature() {
        let err = ScramClient::verify_server_final("", &[], "").unwrap_err();
        assert!(err.to_string().contains("missing signature"));
    }

    #[test]
    fn verify_server_final_handles_server_error() {
        let err = ScramClient::verify_server_final("e=invalid-proof", &[], "").unwrap_err();
        assert!(err.to_string().contains("server error"));
        assert!(err.to_string().contains("invalid-proof"));
    }

    #[test]
    fn verify_server_final_rejects_invalid_base64() {
        let err = ScramClient::verify_server_final("v=!!!invalid!!!", &[], "").unwrap_err();
        assert!(err.to_string().contains("base64"));
    }

    // ==================== Helper function tests ====================

    #[test]
    fn sasl_escape_username_escapes_equals() {
        assert_eq!(sasl_escape_username("a=b"), "a=3Db");
    }

    #[test]
    fn sasl_escape_username_escapes_comma() {
        assert_eq!(sasl_escape_username("a,b"), "a=2Cb");
    }

    #[test]
    fn sasl_escape_username_escapes_both() {
        assert_eq!(sasl_escape_username("a=b,c"), "a=3Db=2Cc");
    }

    #[test]
    fn sasl_escape_username_preserves_normal() {
        assert_eq!(sasl_escape_username("normal_user123"), "normal_user123");
    }

    #[test]
    fn hi_sha256_single_iteration() {
        // With 1 iteration, result is just HMAC(password, salt || 0x00000001)
        let result = hi_sha256(b"password", b"salt", 1);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn hi_sha256_multiple_iterations() {
        let result = hi_sha256(b"password", b"salt", 4096);
        assert_eq!(result.len(), 32);

        // More iterations should produce different result
        let result2 = hi_sha256(b"password", b"salt", 1000);
        assert_ne!(result, result2);
    }

    #[test]
    fn hmac_sha256_produces_correct_length() {
        let result = hmac_sha256(b"key", b"message");
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn xor_bytes_works() {
        assert_eq!(xor_bytes(&[0xFF, 0x00], &[0x0F, 0xF0]), vec![0xF0, 0xF0]);
        assert_eq!(xor_bytes(&[0x00], &[0x00]), vec![0x00]);
    }

    #[test]
    fn constant_time_eq_equal() {
        assert!(constant_time_eq(&[1, 2, 3], &[1, 2, 3]));
        assert!(constant_time_eq(&[], &[]));
    }

    #[test]
    fn constant_time_eq_not_equal() {
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2, 4]));
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2]));
    }

    #[test]
    fn constant_time_eq_different_lengths() {
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2, 3, 4]));
    }
}
