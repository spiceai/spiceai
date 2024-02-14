use std::{collections::HashSet, sync::Mutex};

/// Traces a log with a given parameter once, to prevent log spam.
///
/// Not suitable for high-frequency logs.
pub struct OnceTracer {
    pub logged_values: Mutex<HashSet<String>>,
}

impl OnceTracer {
    pub fn new() -> Self {
        OnceTracer {
            logged_values: Mutex::new(HashSet::new()),
        }
    }
}

#[macro_export]
macro_rules! warn_once {
    ($tracer:expr, $msg:expr, $value:expr) => {{
        let mut logged_values = $tracer.logged_values.lock().unwrap_or_else(|poisoned| {
            tracing::error!("Lock poisoned while logging: {poisoned}");
            tracing::warn!($msg, $value);
            return poisoned.into_inner();
        });
        if !logged_values.contains(&$value) {
            logged_values.insert($value.clone());
            tracing::warn!($msg, $value);
        }
    }};
}
