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

//! Forwards llama.cpp's log to `tracing`, and keeps the errors it reported on each thread
//! so a failed call can say why.

use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_void};

use crate::sys;

thread_local! {
    static ERRORS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn forward_to_tracing() {
    // SAFETY: `callback` is a `'static` function that only reads its arguments.
    unsafe { sys::llama_log_set(Some(callback), std::ptr::null_mut()) };
}

/// Forgets the errors earlier calls on this thread reported.
pub(crate) fn clear_last_error() {
    ERRORS.with(|errors| errors.borrow_mut().clear());
}

/// The errors llama.cpp reported on this thread since [`clear_last_error`], in order and
/// joined, or a placeholder when it reported none. Its specific reason comes first and its
/// summary ("failed to load model") last, so both are kept.
pub(crate) fn last_error() -> String {
    let errors = ERRORS.with(|errors| std::mem::take(&mut *errors.borrow_mut()));
    if errors.is_empty() {
        "llama.cpp reported no reason".to_string()
    } else {
        errors.join("; ")
    }
}

/// llama.cpp's own lines are debug output: what a user needs from an error reaches them in
/// the error Spice returns, which carries the lines llama.cpp logged at error level.
unsafe extern "C" fn callback(level: sys::ggml_log_level, text: *const c_char, _: *mut c_void) {
    if text.is_null() {
        return;
    }
    // SAFETY: llama.cpp passes a NUL-terminated string that lives for the call.
    let text = unsafe { CStr::from_ptr(text) }.to_string_lossy();
    let text = text.trim_end();
    if text.is_empty() {
        return;
    }
    match level {
        sys::ggml_log_level_GGML_LOG_LEVEL_ERROR => {
            tracing::debug!(target: "llama_cpp", "{text}");
            let message = text.to_string();
            ERRORS.with(|errors| {
                let mut errors = errors.borrow_mut();
                // A run-away error loop must not grow without bound.
                if errors.len() < 16 {
                    errors.push(message);
                }
            });
        }
        sys::ggml_log_level_GGML_LOG_LEVEL_WARN | sys::ggml_log_level_GGML_LOG_LEVEL_INFO => {
            tracing::debug!(target: "llama_cpp", "{text}");
        }
        _ => tracing::trace!(target: "llama_cpp", "{text}"),
    }
}
