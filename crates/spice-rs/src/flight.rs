use crate::config::get_user_agent;
use crate::config::GenericError;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::FlightDescriptor;
use arrow_flight::HandshakeRequest;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::stream;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::AsciiMetadataKey;
use tonic::transport::Channel;
use tonic::IntoRequest;

#[derive(Clone)]
pub struct SqlFlightClient {
    headers: Arc<HashMap<String, String>>,
    client: FlightServiceClient<Channel>,
    api_key: Option<Arc<str>>,
}

impl SqlFlightClient {
    pub fn new(
        chan: Channel,
        api_key: Option<String>,
        user_agent: Option<String>,
        cache_control: Option<String>,
    ) -> Self {
        // Prepend the user agent with the provided user agent if it exists
        let user_agent = match user_agent {
            Some(ua) => format!("{ua} {}", get_user_agent()),
            None => get_user_agent(),
        };

        let mut headers = HashMap::new();
        headers.insert("User-Agent".to_string(), user_agent);

        if let Some(cache_control) = cache_control {
            headers.insert("Cache-Control".to_string(), cache_control);
        }

        SqlFlightClient {
            api_key: api_key.map(|s| Arc::from(s.into_boxed_str())),
            headers: Arc::new(headers),
            client: FlightServiceClient::new(chan),
        }
    }

    async fn handshake(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<String>, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Bytes::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let req = self.set_request_headers(req, None)?;
        let resp = self
            .client
            .clone()
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IpcError(format!("Can't handshake {e}")))?;

        let mut token: Option<String> = None;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .map_err(|_| ArrowError::ParseError("Can't read auth header".to_string()))?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            token = Some(auth);
        }
        Ok(token)
    }

    async fn authenticate(&self) -> std::result::Result<Option<String>, GenericError> {
        let (username, password) = match &self.api_key {
            Some(api_key) => ("", api_key.as_ref()),
            None => return Ok(None),
        };

        let token = self.handshake(username, password).await?;

        Ok(token)
    }

    fn set_request_headers<T>(
        &self,
        mut req: tonic::Request<T>,
        token: Option<String>,
    ) -> Result<tonic::Request<T>, ArrowError> {
        for (k, v) in self.headers.iter() {
            let k = AsciiMetadataKey::from_str(k.as_str()).map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header key \"{k}\": {e}"))
            })?;
            let v = v.parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header value \"{v}\": {e}"))
            })?;
            req.metadata_mut().insert(k, v);
        }
        if let Some(token) = token {
            let val = format!("Bearer {token}").parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert token to header value: {e}"))
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }

    pub async fn query(
        &self,
        query: &str,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        let token = self.authenticate().await?;

        let descriptor = FlightDescriptor::new_cmd(query.to_string());
        let req = self.set_request_headers(descriptor.into_request(), token.clone())?;

        let info = self.client.clone().get_flight_info(req).await?.into_inner();

        for ep in info.endpoint {
            if let Some(tkt) = ep.ticket {
                let req = tkt.into_request();
                let req = self.set_request_headers(req, token.clone())?;
                let (md, response_stream, _ext) =
                    self.client.clone().do_get(req).await?.into_parts();

                return Ok(FlightRecordBatchStream::new_from_flight_data(
                    response_stream.map_err(|e| FlightError::Tonic(Box::new(e))),
                )
                .with_headers(md));
            }
        }
        Err("No endpoints found".into())
    }

    pub async fn query_with_params(
        &self,
        query: &str,
        params: Option<RecordBatch>,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        if let Some(params) = params {
            Ok(self.execute_prepared_statement(query, params).await?)
        } else {
            Ok(self.query(query).await?)
        }
    }

    async fn execute_prepared_statement(
        &self,
        query: &str,
        parameters: RecordBatch,
    ) -> std::result::Result<FlightRecordBatchStream, GenericError> {
        let mut client = FlightSqlServiceClient::new_from_inner(self.client.clone());
        let mut prepared_stmt = client.prepare(query.to_string(), None).await?;

        prepared_stmt.set_parameters(parameters)?;

        let flight_info = prepared_stmt.execute().await?;

        let endpoint = flight_info
            .endpoint
            .first()
            .ok_or("No endpoint in flight info")?;

        let stream = client
            .do_get(
                endpoint
                    .ticket
                    .clone()
                    .ok_or("No flight ticket in response")?,
            )
            .await?;
        Ok(stream)
    }
}
