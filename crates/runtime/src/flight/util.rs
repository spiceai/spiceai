/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow_flight::flight_service_server::FlightService;
use tonic::{
    metadata::{Ascii, MetadataValue},
    Response,
};

use crate::{
    flight::Service,
    request::{AsyncMarker, Protocol, RequestContext},
};

pub fn attach_cache_metadata(
    response: &mut Response<<Service as FlightService>::DoGetStream>,
    from_cache: Option<bool>,
) {
    if let Some(from_cache) = from_cache {
        let val: Result<MetadataValue<Ascii>, _> = if from_cache {
            "Hit from spiceai".parse()
        } else {
            "Miss from spiceai".parse()
        };
        match val {
            Ok(val) => {
                response.metadata_mut().insert("x-cache", val);
            }
            Err(e) => {
                tracing::error!("Failed to parse metadata value: {}", e);
            }
        }
    }
}

pub(crate) async fn set_flightsql_protocol() {
    let request_context = RequestContext::current(AsyncMarker::new().await);
    request_context.update_protocol(Protocol::FlightSQL);
}
