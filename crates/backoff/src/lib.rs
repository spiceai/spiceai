/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016 Tibor Benke

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

//! Retrying fallible async operations with a backoff policy.
//!
//! An operation returns [`Error::Permanent`] to stop immediately or
//! [`Error::Transient`] to be retried after the next [`backoff::Backoff`]
//! interval; [`future::retry`] drives it on the Tokio timer.
//!
//! The public paths follow the crates.io `backoff` 0.4 API: the root
//! `Cargo.toml` patches that crate to this one, so dependents outside the
//! workspace compile against these paths unchanged.

pub mod backoff;
mod error;
mod exponential;
pub mod future;

pub use crate::error::Error;
pub use crate::exponential::ExponentialBackoff;
pub use crate::future::{NoopNotify, Notify};
