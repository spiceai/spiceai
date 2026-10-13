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

//! The Flight/DoPut keepalive and tunables shared between the cluster
//! write-through logic and the runtime flight service.

/// `app_metadata` sentinel that marks a `FlightData` message as a keepalive,
/// used to prevent the executor's `DoPut` idle timeout from firing on streams
/// that receive data in bursts with long idle gaps between them.
pub const KEEPALIVE_APP_METADATA: &[u8] = b"spice-keepalive";

/// Whether a `FlightData` message is a keepalive: the sentinel over an empty envelope.
///
/// The sentinel alone cannot decide it. `app_metadata` is the writer's to set, so any message
/// can wear it, and a receiver that skips on the sentinel alone discards whatever it was
/// attached to while the write still reports success. Requiring the envelope to be empty is what
/// makes the skip lossless: a message with no header declares nothing, a message with no body
/// carries no bytes, and a message with no descriptor names no stream, so a message populating
/// any of the three is client data and has to reach the receiver's own decode or count path. A
/// heartbeat has none of them -- see [`keepalive`], which is what every sender builds one with.
///
/// The envelope is *every* field `FlightData` carries besides the sentinel itself, checked one
/// by one rather than by a shape that looks empty. That is the rule for the next field as much
/// as for these ones, which is why the fields are destructured below.
///
/// Asking instead what the header *declares* cannot give that guarantee, because such a
/// predicate is open over the kinds it does not know: a schema message, a trailer, a `Tensor`
/// and any IPC header a later Arrow adds all answer "not data", so the sentinel over any one of
/// them would be skipped. A schema re-declared partway through a write is the case that costs
/// the most, since it leaves the stream out of step with what it declared.
///
/// None of which makes the envelope the *definition* of a heartbeat. It is a mitigation: the
/// heartbeat is signalled in-band, per message, on a field the writer owns, and the guarantee
/// is only ever as good as the narrowest shape a real sender emits. Moving the signal out of
/// the message envelope is what would remove the question; tightening this predicate further
/// is not.
///
/// `runtime`'s `do_put.rs` reads the sentinel its own way and does not use this yet; #14221 is
/// where the two receivers are reconciled.
#[must_use]
pub fn is_keepalive(message: &arrow_flight::FlightData) -> bool {
    // Destructured rather than read through `message.`: the doc above claims the envelope is
    // every field, and this is what makes that claim fail to compile when an `arrow-flight`
    // release adds one, instead of leaving the predicate quietly answering `true` for a message
    // carrying it.
    let arrow_flight::FlightData {
        app_metadata,
        data_header,
        data_body,
        flight_descriptor,
    } = message;

    app_metadata.as_ref() == KEEPALIVE_APP_METADATA
        && data_header.is_empty()
        && data_body.is_empty()
        && flight_descriptor.is_none()
}

/// The keepalive a sender emits -- the one message [`is_keepalive`] recognises.
///
/// Built here rather than at the send site so that the shape a sender emits and the shape a
/// receiver skips are one definition. They are coupled either way, but written out separately
/// the coupling is invisible: a sender that grows a sequence number in its body, or a progress
/// header, stops satisfying [`is_keepalive`] and its heartbeats start failing live writes as
/// `NonBatchMessage`, with nothing in the tree to catch it.
#[must_use]
pub fn keepalive() -> arrow_flight::FlightData {
    arrow_flight::FlightData {
        app_metadata: bytes::Bytes::from_static(KEEPALIVE_APP_METADATA),
        ..Default::default()
    }
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

#[cfg(test)]
mod tests {
    use super::{is_keepalive, keepalive};

    /// The message a sender emits is the message a receiver skips.
    #[test]
    fn the_emitted_keepalive_is_recognised_as_one() {
        assert!(is_keepalive(&keepalive()));
    }
}
