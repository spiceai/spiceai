use arrow_flight::sql::{client::FlightSqlServiceClient, CommandStatementQuery, ProstMessageExt};
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use prost::Message;
use std::error::Error;
use tonic::transport::channel::{ClientTlsConfig, Endpoint};
use tonic::{transport::Channel, Request, Streaming};

pub struct SqlFlightClient {
    client: FlightSqlServiceClient<Channel>,
    api_key: String,
}

impl SqlFlightClient {
    pub fn new(chan: Channel, api_key: String) -> Self {
        SqlFlightClient {
            api_key: api_key,
            client: FlightSqlServiceClient::new(chan),
        }
    }

    pub async fn authenticate(&mut self) -> Result<(), Box<dyn Error>> {
        let api_key_ = format!("Bearer {}", self.api_key);
        let parts: Vec<&str> = api_key_.split("|").collect();
        if parts.len() < 2 {
            return Err("Invalid API key format".into());
        }
        match self.client.handshake(parts[0], parts[1]).await {
            Err(e) => Err(e.into()),
            Ok(v) => {
                self.client
                    .set_token(String::from_utf8(v.to_vec()).expect("something"));
                Ok(())
            }
        }
    }

    pub async fn query(
        &mut self,
        query: String,
        _timeout: Option<u32>,
    ) -> Result<Streaming<FlightData>, Box<dyn Error>> {
        match self.authenticate().await {
            Err(e) => return Err(e.into()),
            Ok(()) => {}
        };

        let cmd = CommandStatementQuery {
            query: query.clone(),
            ..Default::default()
        };
        let fd = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let req = Request::new(fd);

        match self.client.inner_mut().get_flight_info(req).await {
            Ok(resp) => {
                let flight_info = resp.into_inner();
                for ep in flight_info.endpoint {
                    if let Some(tkt) = ep.ticket {
                        return self.client.do_get(tkt).await.map_err(|e| e.into());
                    }
                }
                Err("no tickets for flight endpoint".into())
            }
            // Err(e) => {
            //     // Handle re-authentication similar to the Python client and then retry the request.
            //     self.authenticate().await?;
            //     self.query(query, _timeout)
            // },
            Err(e) => Err(e.into()),
        }
    }
}
