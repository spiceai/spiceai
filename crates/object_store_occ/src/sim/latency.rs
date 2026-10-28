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

//! An [`ObjectStore`] wrapper that delays each request by a configurable,
//! per-operation random latency, so an experiment can sweep how sensitive a
//! protocol is to the store's round trip. Every number measured through it is
//! simulated latency.

use std::fmt::{self, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// The kinds of request a [`LatencyProfile`] can delay differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Op {
    /// `PUT` with `If-None-Match: *`.
    Create,
    /// `PUT` with `If-Match`.
    Cas,
    /// Unconditional `PUT`, and multipart uploads.
    Put,
    /// `GET` with `If-None-Match`.
    GetConditional,
    /// Plain `GET`, ranged or not.
    Get,
    /// `HEAD`.
    Head,
    /// `LIST`, with or without a delimiter.
    List,
    /// `DELETE`.
    Delete,
    /// `COPY` and `RENAME`.
    Copy,
}

impl Op {
    pub const ALL: [Op; 9] = [
        Op::Create,
        Op::Cas,
        Op::Put,
        Op::GetConditional,
        Op::Get,
        Op::Head,
        Op::List,
        Op::Delete,
        Op::Copy,
    ];

    fn index(self) -> usize {
        self as usize
    }
}

/// A log-normal delay given by its median and its 99th percentile. A zero
/// median means no delay.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Delay {
    pub median: Duration,
    pub p99: Duration,
}

impl Delay {
    #[must_use]
    pub fn new(median: Duration, p99: Duration) -> Self {
        Self {
            median,
            p99: p99.max(median),
        }
    }

    /// The `z` of the 99th percentile of the standard normal distribution.
    const Z99: f64 = 2.326_347_874;

    fn sample(&self, rng: &mut StdRng) -> Duration {
        if self.median.is_zero() {
            return Duration::ZERO;
        }
        let median = self.median.as_secs_f64();
        let sigma = (self.p99.as_secs_f64() / median).ln() / Self::Z99;
        // Box-Muller: one standard normal from two uniforms in (0, 1].
        let u1: f64 = 1.0 - rng.random::<f64>();
        let u2: f64 = rng.random::<f64>();
        let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
        Duration::from_secs_f64(median * (sigma * z).exp())
    }
}

/// The delay for each kind of request.
#[derive(Debug, Clone, Default)]
pub struct LatencyProfile {
    delays: [Delay; 9],
}

impl LatencyProfile {
    /// The same delay for every request.
    #[must_use]
    pub fn uniform(delay: Delay) -> Self {
        Self { delays: [delay; 9] }
    }

    /// Sets the delay for one kind of request.
    #[must_use]
    pub fn with(mut self, op: Op, delay: Delay) -> Self {
        self.delays[op.index()] = delay;
        self
    }

    #[must_use]
    pub fn delay(&self, op: Op) -> Delay {
        self.delays[op.index()]
    }
}

/// Delays every request to `inner` by a delay drawn from `profile` for its
/// kind, before the request is sent. Seeded, so a sweep is repeatable.
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    profile: LatencyProfile,
    rng: Mutex<StdRng>,
}

impl LatencyStore {
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, profile: LatencyProfile, seed: u64) -> Self {
        Self {
            inner,
            profile,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    fn draw(&self, op: Op) -> Duration {
        self.profile.delay(op).sample(&mut self.rng.lock())
    }

    async fn wait(&self, op: Op) {
        let delay = self.draw(op);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

impl fmt::Debug for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyStore")
            .field("inner", &self.inner)
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

impl Display for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

fn put_op(mode: &PutMode) -> Op {
    match mode {
        PutMode::Create => Op::Create,
        PutMode::Update(_) => Op::Cas,
        PutMode::Overwrite => Op::Put,
    }
}

fn get_op(options: &GetOptions) -> Op {
    if options.head {
        Op::Head
    } else if options.if_none_match.is_some() {
        Op::GetConditional
    } else {
        Op::Get
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.wait(put_op(&opts.mode)).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.wait(Op::Put).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.wait(get_op(&options)).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.wait(Op::Get).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        // Each delete is its own request, so each waits its own delay.
        let delays: Vec<Duration> = (0..1024).map(|_| self.draw(Op::Delete)).collect();
        let mut index = 0_usize;
        let delayed = locations
            .then(move |location| {
                let delay = delays[index % delays.len()];
                index += 1;
                async move {
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    location
                }
            })
            .boxed();
        self.inner.delete_stream(delayed)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let delay = self.draw(Op::List);
        let inner = self.inner.list(prefix);
        futures::stream::once(async move {
            tokio::time::sleep(delay).await;
            inner
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let delay = self.draw(Op::List);
        let inner = self.inner.list_with_offset(prefix, offset);
        futures::stream::once(async move {
            tokio::time::sleep(delay).await;
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.wait(Op::List).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.wait(Op::Copy).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.wait(Op::Copy).await;
        self.inner.rename_opts(from, to, options).await
    }
}
