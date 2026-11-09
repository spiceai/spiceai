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

use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::task::{Context, Poll};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    error::Result,
    physical_plan::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::Stream;

#[cfg(test)]
mod tests;

const NOT_STARTED: u8 = 0;
const RUNNING: u8 = 1;
const COMPLETE: u8 = 2;
const LIMITED: u8 = 3;
const TRUNCATED: u8 = 4;
const FAILED: u8 = 5;
const CANCELLED: u8 = 6;
const REUSED: u8 = 7;

/// Completion of one dedicated HTTP source execution, across all its partitions.
/// Reusing a tracked plan invalidates the token rather than sharing a prior result.
#[derive(Clone, Debug)]
pub struct HttpFetchCompletion {
    partitions: Arc<[AtomicU8]>,
    followed_page: Arc<AtomicBool>,
}

impl HttpFetchCompletion {
    pub(super) fn new(partitions: usize) -> Self {
        Self {
            partitions: (0..partitions)
                .map(|_| AtomicU8::new(NOT_STARTED))
                .collect(),
            followed_page: Arc::new(AtomicBool::new(false)),
        }
    }

    /// True only after every stream naturally reaches EOF without any source cap.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.partitions.is_empty()
            && self
                .partitions
                .iter()
                .all(|state| state.load(Ordering::Acquire) == COMPLETE)
    }

    /// True after one request partition reaches EOF without following another page.
    /// Pagination configuration alone does not imply that multiple pages were fetched.
    #[must_use]
    pub fn is_complete_single_request(&self) -> bool {
        self.partitions.len() == 1
            && self.is_complete()
            && !self.followed_page.load(Ordering::Acquire)
    }

    pub(super) fn start(&self, partition: usize, limited: bool) -> CompletionGuard {
        let state = &self.partitions[partition];
        if state
            .compare_exchange(
                NOT_STARTED,
                if limited { LIMITED } else { RUNNING },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            state.store(REUSED, Ordering::Release);
        }
        CompletionGuard {
            progress: Progress {
                completion: self.clone(),
                partition,
            },
            finished: false,
        }
    }
}

#[derive(Clone)]
pub(super) struct Progress {
    completion: HttpFetchCompletion,
    partition: usize,
}

impl Progress {
    fn mark(&self, outcome: u8) {
        let state = &self.completion.partitions[self.partition];
        let _ = state.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            if current == REUSED || (outcome == COMPLETE && current != RUNNING) {
                None
            } else {
                Some(outcome)
            }
        });
    }

    pub(super) fn followed_page(&self) {
        self.completion.followed_page.store(true, Ordering::Release);
    }

    pub(super) fn truncated(&self) {
        self.mark(TRUNCATED);
    }

    pub(super) fn failed(&self) {
        self.mark(FAILED);
    }
}

pub(super) struct CompletionGuard {
    progress: Progress,
    finished: bool,
}

impl CompletionGuard {
    pub(super) fn progress(&self) -> Progress {
        self.progress.clone()
    }

    fn finish(&mut self, outcome: u8) {
        if !self.finished {
            self.progress.mark(outcome);
            self.finished = true;
        }
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.progress.mark(CANCELLED);
        }
    }
}

pub(super) fn track(
    stream: SendableRecordBatchStream,
    completion: Option<CompletionGuard>,
) -> SendableRecordBatchStream {
    match completion {
        Some(completion) => Box::pin(TrackedStream { stream, completion }),
        None => stream,
    }
}

struct TrackedStream {
    stream: SendableRecordBatchStream,
    completion: CompletionGuard,
}

impl Stream for TrackedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.stream.as_mut().poll_next(context);
        match &result {
            Poll::Ready(None) => self.completion.finish(COMPLETE),
            Poll::Ready(Some(Err(_))) => self.completion.finish(FAILED),
            _ => {}
        }
        result
    }
}

impl RecordBatchStream for TrackedStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}
