use arrow::error::ArrowError;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use arrow_flight::HandshakeResponse;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::stream;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use tonic::metadata::AsciiMetadataKey;
use tonic::transport::Channel;
use tonic::IntoRequest;

pub struct SqlFlightClient {
    token: Option<String>,
    headers: HashMap<String, String>,
    client: FlightServiceClient<Channel>,
    api_key: String,
}

fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::IpcError(format!("{status:?}"))
}

impl SqlFlightClient {
    pub fn new(chan: Channel, api_key: String) -> Self {
        SqlFlightClient {
            api_key,
            client: FlightServiceClient::new(chan),
            headers: HashMap::default(),
            token: None,
        }
    }

    pub async fn handshake(&mut self, username: &str, password: &str) -> Result<Bytes, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Default::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let req = self.set_request_headers(req)?;
        let resp = self
            .client
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IpcError(format!("Can't handshake {e}")))?;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .map_err(|_| ArrowError::ParseError("Can't read auth header".to_string()))?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            self.token = Some(auth);
        }
        let responses: Vec<HandshakeResponse> = resp
            .into_inner()
            .try_collect()
            .await
            .map_err(|_| ArrowError::ParseError("Can't collect responses".to_string()))?;
        let resp = match responses.as_slice() {
            [resp] => resp.payload.clone(),
            [] => Bytes::new(),
            _ => Err(ArrowError::ParseError(
                "Multiple handshake responses".to_string(),
            ))?,
        };
        Ok(resp)
    }

    pub async fn authenticate(&mut self) -> std::result::Result<(), Box<dyn Error>> {
        if self.api_key.split('|').collect::<String>().len() < 2 {
            return Err("Invalid API key format".into());
        }
        self.handshake("", &self.api_key.to_string()).await?;
        Ok(())
    }

    fn set_request_headers<T>(
        &self,
        mut req: tonic::Request<T>,
    ) -> Result<tonic::Request<T>, ArrowError> {
        for (k, v) in &self.headers {
            let k = AsciiMetadataKey::from_str(k.as_str()).map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header key \"{k}\": {e}"))
            })?;
            let v = v.parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header value \"{v}\": {e}"))
            })?;
            req.metadata_mut().insert(k, v);
        }
        if let Some(token) = &self.token {
            let val = format!("Bearer {token}").parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert token to header value: {e}"))
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }

    pub async fn query(
        &mut self,
        query: &str,
    ) -> std::result::Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.authenticate().await?;

        let descriptor = FlightDescriptor::new_cmd(query.to_string());
        let req = self.set_request_headers(descriptor.into_request())?;

        let info = self
            .client
            .get_flight_info(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();

        for ep in info.endpoint {
            if let Some(tkt) = ep.ticket {
                let req = tkt.into_request();
                let req = self.set_request_headers(req)?;
                let (md, response_stream, _ext) = self
                    .client
                    .do_get(req)
                    .await
                    .map_err(status_to_arrow_error)?
                    .into_parts();

                return Ok(FlightRecordBatchStream::new_from_flight_data(
                    response_stream.map_err(FlightError::Tonic),
                )
                .with_headers(md));
            }
        }
        Err("No endpoints found".into())
    }
}
