use crate::{AnyErrorResult, SecretStore};
use async_trait::async_trait;
use prost::{Message, bytes};
use runtime_proto::{ExecutorExpandSecretRequest, ExecutorExpandSecretResponse};
use secrecy::SecretString;

/// Used by cluster mode to resolve secrets declared in the scheduler
/// via flight RPC
pub struct SchedulerRPCSecretStore {
    scheduler_url: String,
    executor_id: String,
}

impl SchedulerRPCSecretStore {
    #[must_use]
    pub fn new(scheduler_url: String, executor_id: String) -> Self {
        Self {
            scheduler_url,
            executor_id,
        }
    }
}

#[async_trait]
impl SecretStore for SchedulerRPCSecretStore {
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        tracing::trace!("SchedulerRPCSecretStore: Requesting secret {}", key);

        let flight_client = flight_client::FlightClient::try_new(
            self.scheduler_url.clone().into(),
            flight_client::Credentials::anonymous(),
            None,
        )
        .await?;

        let request = ExecutorExpandSecretRequest {
            executor_id: self.executor_id.clone(),
            key: key.to_string(),
        };

        let action = arrow_flight::Action {
            r#type: "ExpandSecret".to_string(),
            body: bytes::Bytes::from(request.encode_to_vec()),
        };

        let response = flight_client.client().clone().do_action(action).await?;

        let mut stream = response.into_inner();

        let Some(result) = stream.message().await? else {
            return Ok(None);
        };

        let response = ExecutorExpandSecretResponse::decode(&*result.body)?;
        Ok(Some(SecretString::from(response.value)))
    }
}
