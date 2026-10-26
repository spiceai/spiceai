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

//! The devices llama.cpp places a model on, as its backend registry reports them.

use crate::{Backend, sys};

/// The memory, in bytes, free for a model on llama.cpp's GPUs (a unified-memory GPU reports the
/// memory it may use of the system's). `None` without a GPU: the CPU device reports the
/// machine's whole memory as free, whatever is in use.
#[must_use]
pub fn gpu_free_memory(_backend: &Backend) -> Option<u64> {
    // SAFETY: the registry holds `count` devices, which it never frees.
    let count = unsafe { sys::ggml_backend_dev_count() };
    let devices: Vec<(sys::ggml_backend_dev_type, u64)> = (0..count)
        .map(|i| {
            // SAFETY: `i` is below the registry's device count.
            let device = unsafe { sys::ggml_backend_dev_get(i) };
            let (mut free, mut total) = (0, 0);
            // SAFETY: a registry device, and both pointers are to locals that outlive the call.
            unsafe { sys::ggml_backend_dev_memory(device, &raw mut free, &raw mut total) };
            // SAFETY: a registry device.
            let kind = unsafe { sys::ggml_backend_dev_type(device) };
            (kind, u64::try_from(free).unwrap_or(u64::MAX))
        })
        .collect();
    let gpu = |kind: &sys::ggml_backend_dev_type| {
        *kind == sys::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_GPU
            || *kind == sys::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_IGPU
    };
    let on_gpus: u64 = devices
        .iter()
        .filter(|(kind, _)| gpu(kind))
        .map(|(_, free)| free)
        .sum();
    (on_gpus > 0).then_some(on_gpus)
}
