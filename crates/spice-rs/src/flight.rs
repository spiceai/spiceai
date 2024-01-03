use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::Ticket;

use std::error::Error;
use tonic::transport::Channel;

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

    pub async fn authenticate(&mut self) -> std::result::Result<(), Box<dyn Error>> {
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
        query: &str,
        firecache: bool,
    ) -> std::result::Result<FlightRecordBatchStream, Box<dyn Error>> {
        match self.authenticate().await {
            Err(e) => return Err(e.into()),
            Ok(()) => {}
        };

        match self.client.execute(query.to_string(), Option::None).await {
            Ok(resp) => {
                for ep in resp.endpoint {
                    if let Some(tkt) = ep.ticket {
                        // There seems to be an issue with ticket parsing in arrow-flight crate
                        // This is a workaround to fix the issue
                        let fixed_ticket = if firecache {
                            Ticket::new(
                                tkt.ticket
                                    .into_iter()
                                    .skip_while(|&x| x != b'}')
                                    .skip(1)
                                    .collect::<Vec<u8>>(),
                            )
                        } else {
                            tkt
                        };
                        return self.client.do_get(fixed_ticket).await.map_err(|e| e.into());
                    }
                }
                Err("no tickets for flight endpoint".into())
            }
            Err(e) => Err(e.into()),
        }
    }
}
