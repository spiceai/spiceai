//! # No-op OpenTelemetry Metrics Implementation
//!
//! This implementation is returned as the global Meter if no `MeterProvider`
//! has been set. It is expected to have minimal resource utilization and
//! runtime impact.
//!
//! Copying this implementation since opentelemetry decided to make it private:
//! <https://github.com/open-telemetry/opentelemetry-rust/pull/2191>
use opentelemetry::metrics::{InstrumentProvider, Meter, MeterProvider};
use std::sync::Arc;

/// A no-op instance of a `MetricProvider`
#[derive(Debug, Default)]
pub(crate) struct NoopMeterProvider {
    _private: (),
}

impl NoopMeterProvider {
    /// Create a new no-op meter provider.
    pub(crate) fn new() -> Self {
        NoopMeterProvider { _private: () }
    }
}

impl MeterProvider for NoopMeterProvider {
    fn meter_with_scope(&self, _scope: opentelemetry::InstrumentationScope) -> Meter {
        Meter::new(Arc::new(NoopMeter::new()))
    }
}

/// A no-op instance of a `Meter`
#[derive(Debug, Default)]
pub(crate) struct NoopMeter {
    _private: (),
}

impl NoopMeter {
    /// Create a new no-op meter core.
    pub(crate) fn new() -> Self {
        NoopMeter { _private: () }
    }
}

impl InstrumentProvider for NoopMeter {}
