/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Instant,
};

use futures::Stream;
use opentelemetry::metrics::Histogram;
use pin_project::pin_project;

use crate::KeyValue;

/// The value, in milliseconds, a [`TimeMeasurement`] recorded, readable by
/// whoever holds a clone.
///
/// Lets a caller report a duration it already records — in a response header,
/// say — without starting a second clock that would disagree with the metric.
/// Set once, when the measurement records; a later write is ignored.
#[derive(Clone, Debug, Default)]
pub struct RecordedDuration(Arc<OnceLock<f64>>);

impl RecordedDuration {
    /// The recorded milliseconds, or `None` before the measurement records.
    #[must_use]
    pub fn get(&self) -> Option<f64> {
        self.0.get().copied()
    }

    /// Stores `milliseconds` unless a value is already stored.
    pub fn set(&self, milliseconds: f64) {
        let _ = self.0.set(milliseconds);
    }
}

/// Measures the time in milliseconds it takes to execute a block of code and records it in a histogram metric.
pub struct TimeMeasurement {
    start: Instant,
    metric: &'static Histogram<f64>,
    labels: Vec<KeyValue>,
    reported_to: Option<RecordedDuration>,
}

impl TimeMeasurement {
    #[must_use]
    pub fn new(metric: &'static Histogram<f64>, labels: impl Into<Vec<KeyValue>>) -> Self {
        Self {
            start: Instant::now(),
            metric,
            labels: labels.into(),
            reported_to: None,
        }
    }

    pub fn with_labels(&mut self, labels: impl Into<Vec<KeyValue>>) {
        self.labels.extend(labels.into());
    }

    /// Also hands the value this measurement records to `recorded`, so the
    /// sample can be read back exactly as the histogram received it.
    pub fn report_to(&mut self, recorded: RecordedDuration) {
        self.reported_to = Some(recorded);
    }
}

impl Drop for TimeMeasurement {
    fn drop(&mut self) {
        let milliseconds = 1000_f64 * self.start.elapsed().as_secs_f64();
        self.metric.record(milliseconds, &self.labels);
        if let Some(recorded) = &self.reported_to {
            recorded.set(milliseconds);
        }
    }
}

/// Measures the time in milliseconds it takes to execute a block of code and records it in a histogram metric
/// with multiple independent sets of labels.
pub struct MultiTimeMeasurement<'a> {
    start: Instant,
    metric: &'static Histogram<f64>,
    label_sets: &'a Vec<Vec<KeyValue>>,
}

impl<'a> MultiTimeMeasurement<'a> {
    #[must_use]
    pub fn new(metric: &'static Histogram<f64>, label_sets: &'a Vec<Vec<KeyValue>>) -> Self {
        Self {
            start: Instant::now(),
            metric,
            label_sets,
        }
    }
}

impl Drop for MultiTimeMeasurement<'_> {
    fn drop(&mut self) {
        let elapsed = 1000_f64 * self.start.elapsed().as_secs_f64();
        for label_set in self.label_sets {
            self.metric.record(elapsed, label_set);
        }
    }
}

#[pin_project]
pub struct TimedStream<S, F>
where
    F: FnOnce() -> TimeMeasurement,
{
    #[pin]
    stream: S,
    start: Option<TimeMeasurement>,
    start_timer: Option<F>,
}

impl<S: Stream, F> TimedStream<S, F>
where
    F: FnOnce() -> TimeMeasurement,
{
    pub fn new(stream: S, start_timer: F) -> Self {
        TimedStream {
            stream,
            start: None,
            start_timer: Some(start_timer),
        }
    }

    fn emit_metric(mut metric: Option<TimeMeasurement>) {
        if metric.is_some() {
            let Some(start) = metric.take() else {
                return;
            };
            drop(start);
        }
    }
}

impl<S: Stream, F> Stream for TimedStream<S, F>
where
    F: FnOnce() -> TimeMeasurement,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if this.start.is_none()
            && let Some(start_timer) = this.start_timer.take()
        {
            this.start.replace(start_timer());
        }

        let stream = &mut this.stream;

        match stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                if this.start.is_some() {
                    Self::emit_metric(this.start.take());
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Pending => Poll::Pending,
        }
    }
}
