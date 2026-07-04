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

#![warn(
    clippy::all,
    clippy::cargo,
    clippy::perf,
    clippy::style,
    clippy::correctness,
    clippy::suspicious
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::multiple_crate_versions
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
