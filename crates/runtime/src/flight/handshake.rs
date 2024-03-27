/*
 Copyright 2024 Spice AI, Inc.

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

use std::pin::Pin;

use arrow_flight::HandshakeResponse;
use futures::Stream;
use tonic::{metadata::MetadataValue, Response, Status};
use uuid::Uuid;

use crate::timing::{TimeMeasurement, TimedStream};

type HandshakeResponseStream =
    Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

pub(crate) fn handle() -> Result<Response<HandshakeResponseStream>, Status> {
    // THIS IS PLACEHOLDER NO-OP AUTH THAT DOES NOT CHECK THE PROVIDED TOKEN AND SIMPLY RETURNS A UUID.
    // TODO: Implement proper auth.
    let token = Uuid::new_v4().to_string();
    let result = HandshakeResponse {
        protocol_version: 0,
        payload: token.as_bytes().to_vec().into(),
    };
    let result = Ok(result);
    let output = TimedStream::new(futures::stream::iter(vec![result]), || {
        TimeMeasurement::new("flight_handshake_request_duration_ms", vec![])
    });
    let str = format!("Bearer {token}");
    let mut resp: Response<HandshakeResponseStream> = Response::new(Box::pin(output));
    let md = MetadataValue::try_from(str)
        .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
    resp.metadata_mut().insert("authorization", md);
    Ok(resp)
}
