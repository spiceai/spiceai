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

/// Whether a `FlightData` message is a keepalive: the sentinel over an empty envelope.
///
/// The sentinel alone cannot decide it. `app_metadata` is the writer's to set, so any message
/// can wear it, and a receiver that skips on the sentinel alone discards whatever it was
/// attached to while the write still reports success. Requiring the envelope to be empty is what
/// makes the skip lossless: a message with no header declares nothing and a message with no body
/// carries no bytes, so anything either half is populated is client data and has to reach the
/// receiver's own decode or count path. A heartbeat has neither --
/// `forward_batches_to_executor` builds one from `FlightData::default()` and sets only the
/// sentinel.
///
/// Asking instead what the header *declares* cannot give that guarantee, because such a
/// predicate is open over the kinds it does not know: a schema message, a trailer, a `Tensor`
/// and any IPC header a later Arrow adds all answer "not data", so the sentinel over any one of
/// them would be skipped. A schema re-declared partway through a write is the case that costs
/// the most, since the batches after it would still be decoded under the schema the stream
/// opened with.
#[must_use]
pub fn is_keepalive(message: &arrow_flight::FlightData) -> bool {
    message.app_metadata.as_ref() == KEEPALIVE_APP_METADATA
        && message.data_header.is_empty()
        && message.data_body.is_empty()
}

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
