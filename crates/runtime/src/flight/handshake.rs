/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow_flight::HandshakeResponse;
use futures::Stream;
use runtime_auth::FlightBasicAuth;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{
    metadata::{MetadataMap, MetadataValue},
    Response, Status,
};
use uuid::Uuid;

use crate::timing::TimedStream;
use runtime_auth::layer::flight as flight_auth;

use super::metrics::track_flight_request;

type HandshakeResponseStream =
    Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

pub(crate) async fn handle(
    metadata: &MetadataMap,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
) -> Result<Response<HandshakeResponseStream>, Status> {
    let start = track_flight_request("handshake", None).await;

    let token = match flight_auth::validate_basic_auth_handshake(metadata, basic_auth)? {
        Some(token) => token,
        None => Uuid::new_v4().to_string(),
    };
    let result = HandshakeResponse {
        protocol_version: 0,
        payload: token.as_bytes().to_vec().into(),
    };
    let result = Ok(result);
    let output = TimedStream::new(futures::stream::iter(vec![result]), || start);
    let str = format!("Bearer {token}");
    let mut resp: Response<HandshakeResponseStream> = Response::new(Box::pin(output));
    let md = MetadataValue::try_from(str)
        .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
    resp.metadata_mut().insert("authorization", md);
    Ok(resp)
}
