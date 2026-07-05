//! Authentication mechanisms for PostgreSQL connections.
//!
//! This module provides implementations for PostgreSQL authentication methods:
//!
//! - **SCRAM-SHA-256** (feature: `scram`): Modern, secure password authentication
//!   recommended for PostgreSQL 10+. Provides mutual authentication and doesn't
//!   transmit the password.
//!
//! # Feature Flags
//!
//! - `scram`: Enables SCRAM-SHA-256 authentication support. Adds dependencies on
//!   `sha2`, `hmac`, `rand`, and `base64`.
//!
//! # Example
//!
//! ```no_run
//! #[cfg(feature = "scram")]
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     use pgwire_replication::auth::scram::ScramClient;
//!     // Create SCRAM client with random nonce
//!     let client = ScramClient::new("postgres");
//!
//!     // Send client-first-message to server
//!     let _client_first = &client.client_first;
//!
//!     // In a real exchange, these come from the server.
//!     // Placeholders here so the example compiles.
//!     let server_first = String::new();
//!     let server_final = String::new();
//!
//!     // After receiving server-first-message, compute response
//!     let (client_final, auth_msg, salted_pw) =
//!         client.client_final("mypassword", &server_first)?;
//!     let _ = client_final;
//!
//!     // After receiving server-final-message, verify server
//!     ScramClient::verify_server_final(&server_final, &salted_pw, &auth_msg)?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Unsupported Methods
//!
//! The following authentication methods are not currently supported:
//! - MD5 (deprecated, insecure)
//! - GSSAPI / Kerberos
//! - SSPI (Windows)
//! - Certificate authentication (handled at TLS layer)

pub mod scram;

#[cfg(feature = "scram")]
pub use scram::ScramClient;
