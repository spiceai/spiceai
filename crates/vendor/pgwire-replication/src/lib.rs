//! # pgwire-replication
//!
//! A Tokio-based PostgreSQL logical replication client implementing the pgoutput protocol.
//!
//! ## Features
//!
//! - **Async/await** - Built on Tokio for high-performance async I/O
//! - **TLS support** - Optional rustls-based encryption with verify modes
//! - **SCRAM-SHA-256** - Secure password authentication  
//! - **pgoutput protocol** - Native logical replication decoding
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, Lsn};
//! use std::time::Duration;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = ReplicationConfig {
//!     host: "localhost".into(),
//!     port: 5432,
//!     user: "postgres".into(),
//!     password: "secret".into(),
//!     database: "mydb".into(),
//!     slot: "my_slot".into(),
//!     publication: "my_publication".into(),
//!     start_lsn: Lsn::ZERO,
//!     ..Default::default()
//! };
//!
//! let mut client = ReplicationClient::connect(config).await?;
//!
//! while let Some(ev) = client.recv().await? {
//!     match ev {
//!         ReplicationEvent::XLogData { wal_end, data, .. } => {
//!             println!("Got data at {}: {} bytes", wal_end, data.len());
//!         }
//!         ReplicationEvent::KeepAlive { wal_end, .. } => {
//!             println!("Keepalive at {}", wal_end);
//!         }
//!         ReplicationEvent::Message { prefix, content, .. } => println!(
//!             "Logical message: prefix={}, {} bytes", prefix, content.len()
//!         ),
//!         ReplicationEvent::StoppedAt {reached: _} => break,
//!         ReplicationEvent::Begin { .. } => println!(
//!             "Transaction start, probably want to flush in-flight events to the sinks."
//!         ),
//!         ReplicationEvent::Commit { .. } => println!(
//!             "Transanction finished, good time to store a checkpoint at the higher level."
//!         ),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! - `tls-rustls` (default) - TLS support via rustls
//! - `scram` (default) - SCRAM-SHA-256 authentication
//! - `md5` - MD5 authentication (legacy)

// spiceai vendoring: this is third-party code excluded from the workspace lint
// policy (see `--exclude pgwire-replication` in the Makefile). It is still
// compiled as a path dependency of `data_components`, and path deps are linted
// (not `--cap-lints`-suppressed), so the workspace's forced `-D` clippy flags
// would otherwise fire on unmodified upstream code. We suppress them here
// rather than editing upstream sources — mirroring the other vendored crates
// (lopdf/ttf-parser/pdf-extract). Upstream's own `#![warn(clippy::cargo, ...)]`
// is intentionally dropped: under the workspace `-Dwarnings` it escalated
// `clippy::cargo` to deny and reported `cargo_common_metadata` for every other
// workspace crate.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::assertions_on_result_states,
    clippy::equatable_if_let,
    clippy::needless_collect,
    clippy::redundant_clone,
    clippy::allow_attributes
)]

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod lsn;
pub mod protocol;
pub mod tls;

pub use client::{ReplicationClient, ReplicationEvent, ReplicationEventReceiver};
pub use config::{ReplicationConfig, SslMode, TlsConfig};
pub use error::{PgWireError, Result};
pub use lsn::Lsn;
