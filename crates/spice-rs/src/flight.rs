use arrow_flight::sql::{client::FlightSqlServiceClient, CommandStatementQuery, ProstMessageExt};
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use prost::Message;
use core::panic;
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
        if self.api_key.split("|").collect::<String>().len() < 2 {
            return Err("Invalid API key format".into());
        }
        match self.client.handshake("", &self.api_key.clone()).await {
            Err(e) => Err(e.into()),
            Ok(_) => Ok(()),
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
        match self.client.execute(query, Option::None).await {
            Ok(resp) => {
                for ep in resp.endpoint {
                    if let Some(tkt) = ep.ticket {
                        return self.client.do_get(tkt).await.map_err(|e| e.into());
                    }
                }
                Err("no tickets for flight endpoint".into())
            }
            Err(e) => Err(e.into()),
        }
    }
}
