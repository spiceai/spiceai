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

//! Flight/DoPut tunables shared between the cluster write-through logic and
//! the runtime flight service.

/// `app_metadata` sentinel that marks a `FlightData` message as a keepalive,
/// used to prevent the executor's `DoPut` idle timeout from firing on streams
/// that receive data in bursts with long idle gaps between them.
pub const KEEPALIVE_APP_METADATA: &[u8] = b"spice-keepalive";

/// Returns the `DoPut` idle timeout. Override with the
/// `SPICE_DO_PUT_IDLE_TIMEOUT_SECS` env-var (useful for tests).
#[must_use]
pub fn do_put_idle_timeout() -> std::time::Duration {
    std::env::var("SPICE_DO_PUT_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(
            std::time::Duration::from_mins(2),
            std::time::Duration::from_secs,
        )
}
