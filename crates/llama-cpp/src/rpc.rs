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

//! One model split over the GPUs of several nodes, through llama.cpp's RPC backend: each
//! other node serves its GPUs ([`serve`]), the node that serves the model reaches them as
//! [`Device`]s ([`connect`]), and the model splits its tensors across them
//! ([`crate::ModelParams::with_devices`]). When there are more than two, the nodes reduce
//! their partial results over direct links between every pair of them.

use std::ffi::{CStr, CString};
use std::path::Path;

use snafu::{ResultExt, ensure};

use crate::{Backend, Result, RpcConnectSnafu, RpcEndpointSnafu, RpcServeSnafu, sys};

/// A device that a model can place tensors on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Device(pub(crate) sys::ggml_backend_dev_t);

// SAFETY: a device is an entry of ggml's process-wide registry, which is never freed, and
// ggml's device calls take no lock that ties them to a thread.
unsafe impl Send for Device {}
unsafe impl Sync for Device {}

impl Device {
    /// The registry's name for the device, such as `CUDA0` or `RPC1`.
    #[must_use]
    pub fn name(&self) -> String {
        // SAFETY: a registry device stays valid for the process, and so does its name.
        c_string(unsafe { sys::ggml_backend_dev_name(self.0) })
    }

    /// What the device is: the GPU's name, or the endpoint of a remote one.
    #[must_use]
    pub fn description(&self) -> String {
        // SAFETY: as for the name.
        c_string(unsafe { sys::ggml_backend_dev_description(self.0) })
    }

    /// Free and total memory, in bytes.
    #[must_use]
    pub fn memory(&self) -> (usize, usize) {
        let (mut free, mut total) = (0, 0);
        // SAFETY: the device is valid, and both pointers are to locals that outlive the call.
        unsafe { sys::ggml_backend_dev_memory(self.0, &raw mut free, &raw mut total) };
        (free, total)
    }

    /// Whether the device is another node's, reached over RPC.
    #[must_use]
    pub fn is_remote(&self) -> bool {
        // SAFETY: a registry device belongs to a reg that lives for the process, and so does
        // the reg's name.
        c_string(unsafe { sys::ggml_backend_reg_name(sys::ggml_backend_dev_backend_reg(self.0)) })
            == "RPC"
    }

    /// This node's GPUs, which [`serve`] offers to the node that serves the model; another
    /// node's GPU this node has reached is not one of them.
    #[must_use]
    pub fn local_gpus(_backend: &Backend) -> Vec<Device> {
        // SAFETY: the registry holds `count` devices, which it never frees.
        let count = unsafe { sys::ggml_backend_dev_count() };
        (0..count)
            // SAFETY: `i` is below the registry's device count.
            .map(|i| Device(unsafe { sys::ggml_backend_dev_get(i) }))
            .filter(|device| {
                // SAFETY: a registry device.
                let kind = unsafe { sys::ggml_backend_dev_type(device.0) };
                (kind == sys::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_GPU
                    || kind == sys::ggml_backend_dev_type_GGML_BACKEND_DEVICE_TYPE_IGPU)
                    && !device.is_remote()
            })
            .collect()
    }
}

fn c_string(ptr: *const std::ffi::c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    // SAFETY: a non-null name from ggml is a NUL-terminated string that outlives this read.
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

/// Reaches the RPC server at `endpoint` (`host:port`) and returns its devices. Fails while
/// nothing serves there; the caller decides how long to keep trying.
pub fn connect(_backend: &Backend, endpoint: &str) -> Result<Vec<Device>> {
    let c_endpoint = CString::new(endpoint).context(RpcEndpointSnafu { endpoint })?;
    // SAFETY: `c_endpoint` is NUL-terminated and outlives the call; ggml keeps its own copy.
    let reg = unsafe { sys::ggml_backend_rpc_add_server(c_endpoint.as_ptr()) };
    ensure!(!reg.is_null(), RpcConnectSnafu { endpoint });
    // SAFETY: a non-null reg from `ggml_backend_rpc_add_server` lives for the process.
    let count = unsafe { sys::ggml_backend_reg_dev_count(reg) };
    ensure!(count > 0, RpcConnectSnafu { endpoint });
    Ok((0..count)
        // SAFETY: `i` is below the reg's device count.
        .map(|i| Device(unsafe { sys::ggml_backend_reg_dev_get(reg, i) }))
        .collect())
}

/// Serves `devices` to the node that serves the model, on `endpoint` (`host:port`), one
/// connection at a time. Returns only when the server cannot start: on a dedicated thread,
/// it runs for the life of the process.
pub fn serve(
    _backend: &Backend,
    endpoint: &str,
    devices: &[Device],
    cache_dir: Option<&Path>,
    threads: usize,
) -> Result<()> {
    ensure!(
        !devices.is_empty(),
        RpcServeSnafu {
            endpoint,
            reason: "this node has no GPU to serve",
        }
    );
    let c_endpoint = CString::new(endpoint).context(RpcEndpointSnafu { endpoint })?;
    let c_cache = cache_dir
        .map(|dir| CString::new(dir.as_os_str().as_encoded_bytes()))
        .transpose()
        .context(RpcEndpointSnafu { endpoint })?;
    let mut raw: Vec<sys::ggml_backend_dev_t> = devices.iter().map(|d| d.0).collect();
    // SAFETY: the strings are NUL-terminated, and they and `raw` outlive the call, which
    // returns only when the server cannot run.
    unsafe {
        sys::ggml_backend_rpc_start_server(
            c_endpoint.as_ptr(),
            c_cache.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            threads,
            raw.len(),
            raw.as_mut_ptr(),
        );
    }
    RpcServeSnafu {
        endpoint,
        reason: "llama.cpp's RPC server stopped; see the lines it logged before this",
    }
    .fail()
}
