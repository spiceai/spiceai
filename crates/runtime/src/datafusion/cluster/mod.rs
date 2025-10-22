use crate::Error::FailedToStartClusterScheduler;
use crate::datafusion::cluster;
use crate::datafusion::cluster::codec::spice_logical_codec::SpiceLogicalCodec;
use crate::datafusion::cluster::codec::spice_physical_codec::SpicePhysicalCodec;
use crate::datafusion::cluster::config::SpiceClusterConfig;
use crate::datafusion::cluster::physical_plan::optimizer::distribute_file_scan::DistributeFileScanOptimizer;
use crate::datafusion::cluster::physical_plan::optimizer::union_projection_pushdown::UnionProjectionPushdownOptimizer;
use crate::status::ComponentStatus;
use crate::{Error, Runtime};
use app::App;
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigExt;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::serde::BallistaCodec;
use ballista_core::serde::protobuf::executor_resource::Resource;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{
    ExecutorRegistration, ExecutorResource, ExecutorSpecification,
};
use ballista_core::utils::create_grpc_client_connection;
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_executor::execution_loop;
use ballista_executor::executor::Executor;
use ballista_executor::executor_process::ExecutorProcessConfig;
use ballista_executor::metrics::LoggingMetricsCollector;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::common::{Result, config_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use runtime_object_store::registry::default_runtime_env;
use serde_json::error::Category::Data;
use std::env;
use std::num::NonZero;
use std::sync::Arc;
use futures::TryFutureExt;
use tokio::net::TcpListener;
use uuid::Uuid;

pub mod codec;
pub mod common;
pub mod config;
pub mod physical_plan;

pub async fn create_scheduler_server(
    rt: &Arc<Runtime>,
) -> Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
    let bind_addr = rt.runtime_config.cluster.scheduler_url.clone();

    let mut scheduler_config = SchedulerConfig::default();

    bind_addr.host_str().iter().for_each(|h| {
        scheduler_config.bind_host = (*h).to_string();
    });

    bind_addr
        .port()
        .iter()
        .for_each(|p| scheduler_config.bind_port = *p);

    scheduler_config.override_logical_codec =
        Some(SpiceLogicalCodec::new_with_runtime(Arc::clone(rt)));
    scheduler_config.override_physical_codec = Some(SpicePhysicalCodec::new_codec(Arc::clone(rt))?);

    scheduler_config.grpc_server_max_decoding_message_size = u32::MAX;
    scheduler_config.grpc_server_max_encoding_message_size = u32::MAX;

    // Bind Spice Datafusion configuration incl SpiceQueryPlanner as bound in `DataFusionBuilder`
    let current_context = Arc::clone(&rt.df.ctx);

    scheduler_config.override_session_builder = Some(Arc::new(move |_cfg| {
        let cfg = current_context
            .copied_config()
            .with_option_extension(SpiceClusterConfig::default());

        Ok(
            SessionStateBuilder::new_from_existing(current_context.as_ref().state().clone())
                .with_config(cfg)
                .with_runtime_env(default_runtime_env())
                .with_physical_optimizer_rule(DistributeFileScanOptimizer::new())
                .with_physical_optimizer_rule(UnionProjectionPushdownOptimizer::new())
                .build(),
        )
    }));

    let cluster = BallistaCluster::new_from_config(&scheduler_config)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    tracing::info!("Starting Ballista scheduler on {}", bind_addr);

    scheduler_process::create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(
        cluster,
        scheduler_config.into(),
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub async fn create_executor_loop(rt: &Arc<Runtime>) -> Result<impl Future<Output = Result<()>>> {
    executor_bind_app(rt, rt.runtime_config.cluster.scheduler_url.to_string()).await?;

    let runtime_handle = Arc::clone(rt);

    let runtime_producer: RuntimeProducer =
        Arc::new(move |_cfg| Ok(Arc::clone(&runtime_handle.df.ctx.runtime_env())));

    let config_producer: ConfigProducer = Arc::new(move || {
        SessionConfig::new_with_ballista().with_option_extension(SpiceClusterConfig::default())
    });

    let Some(work_dir) = rt
        .df
        .temp_directory
        .clone()
        .or(env::temp_dir().to_str().map(|s| s.to_string()))
    else {
        return config_err!("Unable to bind executor temp dir");
    };

    let scheduler_connection =
        create_grpc_client_connection(rt.runtime_config.cluster.scheduler_url.clone().to_string())
            .await
            .map_err(|e| {
                DataFusionError::Configuration(format!(
                    "Unable to connect to scheduler at {}",
                    rt.runtime_config.cluster.scheduler_url
                ))
            })?;

    let scheduler = SchedulerGrpcClient::new(scheduler_connection)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);

    // Try to bind the same flight port Spice usually does, but if we cannot, bind a different
    // port to allow for easy local deployments
    let default_grpc_binding = TcpListener::bind(rt.runtime_config.flight_bind_address)
        .await
        .and_then(|l| l.local_addr());

    let dynamic_grpc_binding = TcpListener::bind("0.0.0.0:0")
        .await
        .and_then(|l| l.local_addr());

    let bindable_addr = default_grpc_binding
        .or(dynamic_grpc_binding)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let Some(concurrent_tasks) = std::thread::available_parallelism()
        .ok()
        .and_then(|nz| u32::try_from(nz.get()).ok())
    else {
        return config_err!("Unable to determine executor task parallelism.");
    };

    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        host: None,
        port: bindable_addr.port() as u32,
        grpc_port: 50052,
        specification: Some(ExecutorSpecification {
            resources: vec![ExecutorResource {
                resource: Some(Resource::TaskSlots(concurrent_tasks)),
            }],
        }),
    };

    let executor = Arc::new(Executor::new(
        executor_meta,
        &work_dir,
        runtime_producer,
        config_producer,
        Arc::new(BallistaFunctionRegistry::default()),
        Arc::new(LoggingMetricsCollector::default()),
        concurrent_tasks as usize,
        None,
    ));

    let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> = BallistaCodec::new(
        SpiceLogicalCodec::new_codec(),
        SpicePhysicalCodec::new_codec(Arc::clone(rt))?,
    );

    rt.df
        .bind_executor(Arc::clone(&executor))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(execution_loop::poll_loop(scheduler.clone(), executor.clone(), codec)
        .map_err(|e| DataFusionError::External(Box::new(e))))
}

pub async fn executor_bind_app(
    rt: &Arc<Runtime>,
    scheduler_flight_url: impl Into<Arc<str>>,
) -> Result<()> {
    let flight_client = flight_client::FlightClient::try_new(
        scheduler_flight_url.into(),
        flight_client::Credentials::anonymous(),
        None,
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let action = arrow_flight::Action {
        r#type: "GetAppDefinition".to_string(),
        body: bytes::Bytes::new(),
    };

    let response = flight_client
        .client()
        .clone()
        .do_action(action)
        .await
        .map_err(|e| {
            DataFusionError::Configuration(format!("Failed to call GetAppDefinition: {e}"))
        })?;

    let mut stream = response.into_inner();
    if let Some(result) = stream
        .message()
        .await
        .map_err(|e| DataFusionError::Configuration(format!("Failed to read response: {e}")))?
    {
        let app_def: App = serde_json::from_slice(&result.body).map_err(|e| {
            DataFusionError::Configuration(format!("Failed to deserialize app definition: {e}"))
        })?;

        *rt.app.write().await = Some(Arc::new(app_def));
    }

    Arc::clone(rt).load_catalogs().await;
    rt.load_embeddings().await;
    Arc::clone(rt).load_models().await;
    Arc::clone(rt).load_tools().await;

    Ok(())
}
