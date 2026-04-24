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

pub mod auth;
pub mod client;
pub mod crypto;
pub mod ops;
pub mod pool;
pub mod protocol;

pub use client::{SmbClient, SmbConfig};
pub use ops::{FileHandle, ObjectInfo, ObjectMeta, ShareSession, WalWriter};
pub use pool::SmbPool;
