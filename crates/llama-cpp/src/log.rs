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

//! Forwards llama.cpp's log to `tracing`.

use std::ffi::{CStr, c_char, c_void};

use crate::sys;

pub(crate) fn forward_to_tracing() {
    // SAFETY: `callback` is a `'static` function that only reads its arguments.
    unsafe { sys::llama_log_set(Some(callback), std::ptr::null_mut()) };
}

/// llama.cpp's informational lines describe its own loading and scheduling, so they are
/// debug output here; its warnings and errors keep their level.
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
        sys::ggml_log_level_GGML_LOG_LEVEL_ERROR => tracing::error!(target: "llama_cpp", "{text}"),
        sys::ggml_log_level_GGML_LOG_LEVEL_WARN => tracing::warn!(target: "llama_cpp", "{text}"),
        sys::ggml_log_level_GGML_LOG_LEVEL_INFO => tracing::debug!(target: "llama_cpp", "{text}"),
        _ => tracing::trace!(target: "llama_cpp", "{text}"),
    }
}
