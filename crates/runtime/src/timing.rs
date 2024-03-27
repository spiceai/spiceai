/*
Copyright 2024 Spice AI, Inc.

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
    task::{Context, Poll},
    time::Instant,
};

use futures::Stream;
use pin_project::pin_project;

/// `measure_scope_ms!` measures the time for which the designated scope lives.
///
/// ## Usage
///   - Example, measure a function's whole duration:
///   ```
///   fn my_function() {
///     measure_scope_ms!("process_data");
///     sleep(Duration::from_secs(1))
///   
///   } // 'process_data' measures until the end of the function scope (via implementing `Drop`).
///   ```
///
///   - Example, measure a specific scope
///   ```
///   fn my_function() {
///     // Some work
///     sleep(Duration::from_secs(1))
///     {
///         // Some work we don't want to measure
///         let x = 1+2;
///
///         // Some work we want to measure
///         measure_scope_ms!("process_data");
///         let y = 2*3;
///         sleep(Duration::from_secs(1))
///     } // 'process_data' duration ends here.
///   }
///   ```
///   - **Example**: Add properties to the measurement (key `&str`, value `ToString`)
///   ```
///   fn my_function(x: int, y: String) {
///     measure_scope_ms!("process_data", "x" => x, "y" => y);
///   }
///   ```
/// ## Parameters
///
/// - `$name:expr` — A string literal representing the name of the scope being measured.
/// - `$key:expr => $value:expr` — Optional key-value pairs provided as additional metadata
///   for the timing measurement.
///
/// ```
#[macro_export]
macro_rules! measure_scope_ms {
    ($name:expr, $($key:expr => $value:expr),+ $(,)?) => {
        let args = vec![$(($key, $value.to_string())),+];
        let _ = $crate::timing::TimeMeasurement::new($name, args);
    };
    ($name:expr) => {
        let _ = $crate::timing::TimeMeasurement::new($name, vec![]);
    };
}

pub struct TimeMeasurement {
    start: Instant,
    metric_name: &'static str,
    labels: Vec<(&'static str, String)>,
}

impl TimeMeasurement {
    #[must_use]
    pub fn new(metric_name: &'static str, labels: Vec<(&'static str, String)>) -> Self {
        Self {
            start: Instant::now(),
            metric_name,
            labels,
        }
    }

    pub fn with_labels(&mut self, labels: Vec<(&'static str, String)>) {
        self.labels.extend(labels);
    }
}

impl Drop for TimeMeasurement {
    fn drop(&mut self) {
        metrics::histogram!(self.metric_name, &self.labels)
            .record(1000_f64 * self.start.elapsed().as_secs_f64());
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

        if this.start.is_none() {
            if let Some(start_timer) = this.start_timer.take() {
                this.start.replace(start_timer());
            }
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
