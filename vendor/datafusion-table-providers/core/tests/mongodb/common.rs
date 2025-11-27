use bollard::secret::HealthConfig;
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use mongodb::{options::ClientOptions, Client};
use secrecy::SecretString;
use std::collections::HashMap;
use tracing::instrument;

use crate::{
    container_registry,
    docker::{ContainerRunnerBuilder, RunningContainer},
};

const MONGODB_DOCKER_CONTAINER: &str = "runtime-integration-test-mongodb";

pub(super) fn get_mongodb_params(
    port: usize,
    unnest_depth: Option<usize>,
) -> HashMap<String, SecretString> {
    let mut params = HashMap::new();
    params.insert(
        "mongodb_host".to_string(),
        SecretString::from("localhost".to_string()),
    );
    params.insert(
        "mongodb_port".to_string(),
        SecretString::from(port.to_string()),
    );
    params.insert(
        "mongodb_database".to_string(),
        SecretString::from("testdb".to_string()),
    );
    params.insert(
        "mongodb_username".to_string(),
        SecretString::from("root".to_string()),
    );
    params.insert(
        "mongodb_password".to_string(),
        SecretString::from("integration-test-pw".to_string()),
    );
    params.insert(
        "mongodb_auth_source".to_string(),
        SecretString::from("admin".to_string()),
    );
    params.insert(
        "mongodb_connection_string".to_string(),
        SecretString::from(format!(
            "mongodb://root:integration-test-pw@localhost:{port}/testdb?authSource=admin"
        )),
    );
    params.insert(
        "mongodb_pool_min".to_string(),
        SecretString::from("1".to_string()),
    );
    params.insert(
        "mongodb_pool_max".to_string(),
        SecretString::from("10".to_string()),
    );
    params.insert(
        "mongodb_sslmode".to_string(),
        SecretString::from("disabled".to_string()),
    );

    if let Some(unnest_depth) = unnest_depth {
        params.insert(
            "unnest_depth".to_string(),
            SecretString::from(unnest_depth.to_string()),
        );
    }

    params
}

#[instrument]
pub async fn start_mongodb_docker_container(
    port: usize,
) -> Result<RunningContainer, anyhow::Error> {
    let container_name = format!("{MONGODB_DOCKER_CONTAINER}-{port}");

    let port = port.try_into().unwrap_or(27017);

    let mongodb_docker_image = std::env::var("MONGODB_DOCKER_IMAGE")
        .unwrap_or_else(|_| format!("{}/mongo:7", container_registry()));

    let running_container = ContainerRunnerBuilder::new(container_name)
        .image(mongodb_docker_image)
        .add_port_binding(27017, port)
        .add_env_var("MONGO_INITDB_ROOT_USERNAME", "root")
        .add_env_var("MONGO_INITDB_ROOT_PASSWORD", "integration-test-pw")
        .add_env_var("MONGO_INITDB_DATABASE", "testdb")
        .healthcheck(HealthConfig {
            test: Some(vec![
                "CMD".to_string(),
                "mongosh".to_string(),
                "mongodb://root:integration-test-pw@localhost:27017/testdb?authSource=admin"
                    .to_string(),
                "--quiet".to_string(),
                "--eval".to_string(),
                "db.runCommand({ ping: 1 }).ok".to_string(),
            ]),
            interval: Some(500_000_000),
            timeout: Some(500_000_000),
            retries: Some(10),
            start_period: Some(3_000_000_000),
            start_interval: None,
        })
        .build()?
        .run()
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    Ok(running_container)
}

#[instrument]
pub(super) async fn get_mongodb_connection_pool(
    port: usize,
    unnest_depth: Option<usize>,
) -> Result<MongoDBConnectionPool, anyhow::Error> {
    let mongodb_pool = MongoDBConnectionPool::new(get_mongodb_params(port, unnest_depth))
        .await
        .expect("Failed to create MongoDB Connection Pool");

    Ok(mongodb_pool)
}

#[instrument]
pub(super) async fn get_mongodb_client(port: usize) -> Result<Client, anyhow::Error> {
    let connection_string =
        format!("mongodb://root:integration-test-pw@localhost:{port}/testdb?authSource=admin");

    let client_options = ClientOptions::parse(connection_string)
        .await
        .expect("Failed to parse MongoDB connection string");

    let client = Client::with_options(client_options).expect("Failed to create MongoDB client");

    let mut retries = 10;
    let mut _last_err = None;
    while retries > 0 {
        match client
            .database("testdb")
            .run_command(mongodb::bson::doc! { "ping": 1 })
            .await
        {
            Ok(_) => {
                println!("Client created");
                return Ok(client);
            }
            Err(e) => {
                _last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                retries -= 1;
                println!("Ping failed");
            }
        }
    }

    Ok(client)
}
