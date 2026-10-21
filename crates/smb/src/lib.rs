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

//! Internal SMB 3.1.1 client — speaks the wire protocol directly over TCP.
//!
//! No local mount, no libsmbclient, no FUSE. Pure Rust with cross-platform
//! crypto via `md-4`, `hmac`, `sha2`, `aes`, and `cmac` crates.
//!
//! Ported from <https://github.com/spiceai/spiceio>.

// Wire-protocol code does a lot of safe fixed-size casts between
// usize/u8/u16/u32 that pedantic clippy flags. These four are audited and
// safe by construction: SMB2 header offsets fit in u16, message IDs in u64,
// etc. — adding `try_from` plumbing at every site would just move the panic
// from compile time to a `.unwrap()`.
//
// TODO: tighten doc-quality lints (`missing_errors_doc`,
// `missing_panics_doc`, `doc_markdown`) on a per-public-function basis as
// part of a follow-up docs sweep — they are kept here as a crate-wide
// allow only to keep the SMB-3.1.1 port reviewable in this PR.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown
)]

pub mod auth;
pub mod client;
pub mod crypto;
pub mod ops;
pub mod pool;
pub mod protocol;

pub use client::{SmbClient, SmbConfig};
pub use ops::{FileHandle, ObjectInfo, ObjectMeta, ShareSession, WalWriter};
pub use pool::SmbPool;
