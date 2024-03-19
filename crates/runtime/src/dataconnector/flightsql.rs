use std::collections::HashMap;
use std::pin::Pin;
use std::{future::Future, sync::Arc};

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::Ticket;
use async_trait::async_trait;
use spicepod::component::dataset::Dataset;
use tonic::transport::Channel;

use flight_client::tls::new_tls_flight_channel;
use flightsql_datafusion::FlightSQLTable;
use secrets::Secret;

use super::DataConnector;
use arrow::error::ArrowError;
use futures::stream::TryStreamExt;

#[derive(Debug, Clone)]
pub struct FlightSQL {
    pub client: FlightSqlServiceClient<Channel>,
}

impl FlightSQL {
    async fn query(
        client: FlightSqlServiceClient<Channel>,
        query: String,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, Box<dyn std::error::Error>> {
        let flight_info = client.clone().execute(query, None).await?;

        let mut batches = vec![];
        for ep in &flight_info.endpoint {
            if let Some(tkt) = &ep.ticket {
                let mut do_get_client = if ep.location.is_empty() {
                    client.clone() // expectation is that the ticket can only be redeemed on the current service
                } else {
                    let channel = new_tls_flight_channel(&ep.location[0].uri).await?;
                    FlightSqlServiceClient::new(channel)
                };
                match batch_from_ticket(&mut do_get_client, tkt.to_owned()).await {
                    Ok(flight_data) => batches.extend(flight_data),
                    Err(err) => {
                        tracing::error!("Failed to read batch from flight client: {:?}", err);
                        break;
                    }
                }
            };
        }
        Ok(batches)
    }
}

#[async_trait]
impl DataConnector for FlightSQL {
    fn new(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let endpoint: String = params
                .as_ref() // &Option<HashMap<String, String>>
                .as_ref() // Option<&HashMap<String, String>>
                .and_then(|params| params.get("endpoint").cloned())
                .ok_or(super::Error::UnableToCreateDataConnector {
                    source: "Missing required parameter: endpoint".into(),
                })?;
            let flight_channel = new_tls_flight_channel(&endpoint)
                .await
                .map_err(|e| super::Error::UnableToCreateDataConnector { source: e.into() })?;

            let mut client = FlightSqlServiceClient::new(flight_channel);
            if let Some(s) = secret {
                let _ = client
                    .handshake(
                        s.get("username").unwrap_or_default(),
                        s.get("password").unwrap_or_default(),
                    )
                    .await;
            };
            Ok(Self { client })
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let dataset_path = dataset.path().clone();
        let client = self.client.clone();

        Box::pin(async move {
            match Self::query(client.clone(), format!("SELECT * FROM {dataset_path}")).await {
                Ok(batches) => batches,
                Err(e) => {
                    tracing::error!("Failed to get data from flight client: {:?}", e);
                    Vec::new()
                }
            }
        })
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        match FlightSQLTable::new(self.client.clone(), dataset.path()).await {
            Ok(provider) => Ok(Arc::new(provider)),
            Err(error) => Err(super::Error::UnableToGetTableProvider {
                source: error.into(),
            }),
        }
    }
}

async fn batch_from_ticket(
    client: &mut FlightSqlServiceClient<Channel>,
    ticket: Ticket,
) -> Result<Vec<arrow::record_batch::RecordBatch>, ArrowError> {
    let flight_data = client
        .do_get(ticket)
        .await?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| ArrowError::CastError(format!("Cannot collect flight data: {err:#?}")))?;
    Ok(flight_data)
}
