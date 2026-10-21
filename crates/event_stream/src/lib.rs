/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Allows code to be notified when a new tracing event (i.e. `tracing::info!`, `tracing::error!`, etc.) is emitted for a specific span.
//!
//! This is useful for capturing intermediate output from tasks and other components.
//!
//! # Usage
//!
//! ```rust
//! use event_stream::get_event_stream;
//!
//! let span = tracing::span!(tracing::Level::INFO, "my_span");
//! let event_stream = get_event_stream(&span);
//!
//! tokio::spawn(async move {
//!     while let Some(event) = event_stream.next().await {
//!         println!("Event emitted in this span: {:?}", event);
//!     }
//! });
//!
//! // ... do other work that emits tracing events ...
//!
//! drop(span);
//! ```

use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;
use tracing::span::Attributes;
use tracing::{Event, Id, Subscriber, field::Visit};
use tracing_subscriber::layer::{Context, Layer};

#[derive(Debug, Snafu)]
pub enum TracingError {
    #[snafu(display("Failed to set global subscriber: {}", source))]
    SetSubscriber {
        source: tracing::dispatcher::SetGlobalDefaultError,
    },
    #[snafu(display("Span has no ID"))]
    NoSpanId,
    #[snafu(display("Span not found"))]
    SpanNotFound,
}

// Global EventStreamStore
static EVENT_STREAM_STORE: LazyLock<EventStreamStore> = LazyLock::new(EventStreamStore::new);

/// Get a stream of events emitted from the current span and all its (future) descendants.
///
/// # Errors
///
/// - `NoSpanId` if the span has no ID.
/// - `SpanNotFound` if the span is not found.
pub fn get_event_stream() -> Result<Pin<Box<dyn Stream<Item = String> + Send>>, TracingError> {
    let current_span = tracing::Span::current();
    let span_id = current_span.id().ok_or(TracingError::NoSpanId)?;
    let rx = EVENT_STREAM_STORE.get_receiver(span_id);
    let stream = BroadcastStream::new(rx).filter_map(|res| async move { res.ok() });
    Ok(Box::pin(stream))
}

struct EventStreamStore {
    senders: Arc<RwLock<HashMap<Id, Sender<String>>>>,
    parents: Arc<RwLock<HashMap<Id, Id>>>,
}

impl EventStreamStore {
    #[must_use]
    fn new() -> Self {
        EventStreamStore {
            senders: Arc::new(RwLock::new(HashMap::new())),
            parents: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_receiver(&'static self, span_id: Id) -> Receiver<String> {
        {
            let senders = self.senders.read().unwrap_or_else(|e| {
                tracing::error!("EventStreamStore lock poisoned: {}", e);
                e.into_inner()
            });
            if let Some(sender) = senders.get(&span_id) {
                return sender.subscribe();
            }
        }

        let (tx, rx) = broadcast::channel(16);
        let mut senders = self.senders.write().unwrap_or_else(|e| {
            tracing::error!("EventStreamStore lock poisoned: {}", e);
            e.into_inner()
        });
        senders.entry(span_id).or_insert(tx);
        rx
    }

    fn register_span(&'static self, span_id: &Id, parent_id: Option<Id>) {
        if let Some(parent) = parent_id {
            let mut parents = self.parents.write().unwrap_or_else(|e| {
                tracing::error!("EventStreamStore lock poisoned: {}", e);
                e.into_inner()
            });
            parents.insert(span_id.clone(), parent);
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    fn send_event(&'static self, span_id: &Id, event: String) {
        let senders = self.senders.read().unwrap_or_else(|e| {
            tracing::error!("EventStreamStore lock poisoned: {}", e);
            e.into_inner()
        });
        let parents = self.parents.read().unwrap_or_else(|e| {
            tracing::error!("EventStreamStore lock poisoned: {}", e);
            e.into_inner()
        });

        // Send to the span itself and all ancestors
        let mut current_id = Some(span_id.clone());
        while let Some(id) = current_id {
            if let Some(sender) = senders.get(&id) {
                let _ = sender.send(event.clone());
            }
            current_id = parents.get(&id).cloned();
        }
    }

    fn unregister_span(&'static self, span_id: &Id) {
        let mut senders = self.senders.write().unwrap_or_else(|e| {
            tracing::error!("EventStreamStore lock poisoned: {}", e);
            e.into_inner()
        });
        senders.remove(span_id);

        let mut parents = self.parents.write().unwrap_or_else(|e| {
            tracing::error!("EventStreamStore lock poisoned: {}", e);
            e.into_inner()
        });
        parents.remove(span_id);
    }
}

#[derive(Debug)]
pub struct EventStreamLayer {
    // The field to include in the event message.
    field: &'static str,
}

impl EventStreamLayer {
    #[must_use]
    pub fn new(field: &'static str) -> Self {
        Self { field }
    }
}

impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> Layer<S>
    for EventStreamLayer
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let parent_id = ctx.span(id).and_then(|s| s.parent().map(|p| p.id()));
        EVENT_STREAM_STORE.register_span(id, parent_id);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let current_span = ctx.current_span();
        let parent_id = match event.parent() {
            Some(parent) => Some(parent),
            None => current_span.id(),
        };

        if let Some(span_id) = parent_id {
            let mut message = String::new();
            let mut visitor = EventMessageVisitor {
                message: &mut message,
                field_name: self.field,
            };
            event.record(&mut visitor);
            if !message.is_empty() {
                EVENT_STREAM_STORE.send_event(span_id, message);
            }
        }
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        EVENT_STREAM_STORE.unregister_span(&id);
    }
}

struct EventMessageVisitor<'a> {
    message: &'a mut String,
    field_name: &'static str,
}

impl Visit for EventMessageVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == self.field_name {
            let _ = write!(self.message, "{value:?}");
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write;
        if field.name() == self.field_name {
            let _ = write!(self.message, "{value}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;
    use tracing::{Level, info, span, subscriber::with_default};
    use tracing_subscriber::{Registry, prelude::*};

    #[tokio::test]
    async fn test_get_event_stream_no_span() {
        with_default(
            Registry::default().with(EventStreamLayer::new("message")),
            || {
                // The current span is the "no span" no-op that has no ID.
                let res = get_event_stream();
                assert!(matches!(res, Err(TracingError::NoSpanId)));
            },
        );
    }

    #[tokio::test]
    async fn test_get_event_stream_receives_event() {
        // The span is dropped with the closure, which closes its channel, so
        // the stream below ends on the events already buffered.
        let events = with_default(
            Registry::default().with(EventStreamLayer::new("message")),
            || {
                let span = span!(Level::INFO, "test_span");
                span.in_scope(|| {
                    let events = get_event_stream().expect("Failed to obtain event stream");
                    // Only the configured `message` field is published.
                    info!(message = "hello world", unrelated = "not published");
                    events
                })
            },
        );

        // A deadlock guard, not a wait: the channel is already closed. Keep it
        // short so a regression that leaves the stream open fails fast.
        let received: Vec<String> = timeout(Duration::from_secs(1), events.collect())
            .await
            .expect("the event stream must end when its span closes");
        assert_eq!(received, vec!["hello world".to_string()]);
    }

    #[tokio::test]
    async fn test_get_event_stream_descendant_event() {
        let subscriber = Registry::default().with(EventStreamLayer::new("message"));

        // Both spans are dropped with the closure, which closes the parent's
        // channel, so the stream below ends on the events already buffered.
        let events = with_default(subscriber, || {
            let parent_span = span!(Level::INFO, "parent_span");
            parent_span.in_scope(|| {
                // Emitted before anyone subscribed: the stream does not replay it.
                info!(message = "before subscribing");

                // Get the event stream from the parent span.
                let events = get_event_stream().expect("Failed to obtain event stream");
                info!(message = "parent event");

                // An event emitted in a child span reaches the parent's stream.
                {
                    let child_span = span!(Level::INFO, "child_span");
                    let _enter_child = child_span.enter();
                    info!(message = "child event");
                }
                events
            })
        });

        // A deadlock guard, not a wait: the channel is already closed.
        let received: Vec<String> = timeout(Duration::from_secs(1), events.collect())
            .await
            .expect("the event stream must end when its span closes");
        assert_eq!(
            received,
            vec!["parent event".to_string(), "child event".to_string()]
        );
    }
}
