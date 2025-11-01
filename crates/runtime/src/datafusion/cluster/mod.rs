use crate::Error::{FailedToStartClusterExecutor, FailedToStartClusterScheduler};
use crate::datafusion::cluster::codec::spice_logical_codec::SpiceLogicalCodec;
use crate::datafusion::cluster::codec::spice_physical_codec::SpicePhysicalCodec;
use crate::datafusion::cluster::config::SpiceClusterConfig;
use crate::datafusion::cluster::physical_plan::optimizer::distribute_file_scan::DistributeFileScanOptimizer;
use crate::datafusion::cluster::physical_plan::optimizer::union_projection_pushdown::UnionProjectionPushdownOptimizer;
use crate::status::ComponentStatus;
use crate::{FailedToStartClusterExecutorSnafu, FailedToStartClusterSchedulerSnafu, Runtime};
use app::App;
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
use ballista_executor::metrics::LoggingMetricsCollector;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process;
use ballista_scheduler::scheduler_server::SchedulerServer;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use futures::TryFutureExt;
use runtime_object_store::registry::default_runtime_env;
use snafu::ResultExt;
use std::env;
use std::sync::Arc;
use tokio::net::TcpListener;
use uuid::Uuid;

pub mod codec;
pub mod common;
pub mod config;
pub mod physical_plan;

/// Creates & binds a Ballista scheduler to the Runtime handle, then updates status
pub async fn initialize_cluster_scheduler(rt: &Arc<Runtime>) -> crate::Result<()> {
    let scheduler = create_scheduler_server(rt).await?;

    rt.df
        .bind_scheduler_server(Arc::new(scheduler))
        .map_err(|e| FailedToStartClusterScheduler {
            source: Box::new(e),
        })?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    Ok(())
}

/// Creates a Ballista executor, binds it to the `Runtime` handle, and returns its configured
/// work loop as a future
pub async fn initialize_cluster_executor(
    rt: Arc<Runtime>,
) -> crate::Result<impl Future<Output = crate::Result<()>>> {
    executor_bind_app(&rt, rt.config.cluster.scheduler_url.to_string()).await?;

    let runtime_handle = Arc::clone(&rt);

    let runtime_producer: RuntimeProducer =
        Arc::new(move |_cfg| Ok(Arc::clone(&runtime_handle.df.ctx.runtime_env())));

    let config_producer: ConfigProducer = Arc::new(move || {
        SessionConfig::new_with_ballista().with_option_extension(SpiceClusterConfig::default())
    });

    let work_dir = rt
        .df
        .temp_directory
        .clone()
        .unwrap_or(env::temp_dir().to_string_lossy().to_string());

    let scheduler_connection =
        create_grpc_client_connection(rt.config.cluster.scheduler_url.clone().to_string())
            .await
            .map_err(|_| FailedToStartClusterExecutor {
                source: format!(
                    "Unable to connect to scheduler at {}",
                    rt.config.cluster.scheduler_url
                )
                .into(),
            })?;

    let scheduler = SchedulerGrpcClient::new(scheduler_connection)
        .max_encoding_message_size(usize::MAX)
        .max_decoding_message_size(usize::MAX);

    // Try to bind the same flight port Spice usually does, but if we cannot, bind a different
    // port to allow for easy local deployments
    let bind_addr = if let Ok(flight_bind_addr) = TcpListener::bind(rt.config.flight_bind_address)
        .await
        .and_then(|l| l.local_addr())
    {
        flight_bind_addr
    } else if let Ok(dynamic_addr) = TcpListener::bind("127.0.0.1:0")
        .await
        .and_then(|l| l.local_addr())
    {
        dynamic_addr
    } else {
        return Err(FailedToStartClusterExecutor {
            source: format!(
                "Unable to bind Flight service to configured address ({}) or fallback",
                rt.config.flight_bind_address
            )
            .into(),
        });
    };

    let Some(concurrent_tasks) = std::thread::available_parallelism()
        .ok()
        .and_then(|nz| u32::try_from(nz.get()).ok())
    else {
        return Err(FailedToStartClusterExecutor {
            source: "Unable to determine executor task parallelism."
                .to_string()
                .into(),
        });
    };

    let executor_id = Uuid::new_v4().to_string();
    let executor_meta = ExecutorRegistration {
        id: executor_id.clone(),
        // flight service
        host: Some(bind_addr.ip().to_string()),
        port: u32::from(bind_addr.port()),
        // grpc_port is used only for push mode, and not initialized for pull mode (default)
        grpc_port: 0,
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
        SpicePhysicalCodec::new(Arc::clone(&rt))
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?,
    );

    rt.df
        .bind_executor(Arc::clone(&executor))
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;

    rt.status.update_cluster("executor", ComponentStatus::Ready);

    Ok(
        execution_loop::poll_loop(scheduler, Arc::clone(&executor), codec).map_err(|e| {
            FailedToStartClusterExecutor {
                source: Box::new(e),
            }
        }),
    )
}

async fn create_scheduler_server(
    rt: &Arc<Runtime>,
) -> crate::Result<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
    let bind_addr = rt.config.flight_bind_address;

    // Bind Spice Datafusion configuration incl SpiceQueryPlanner as bound in `DataFusionBuilder`
    let current_context = Arc::clone(&rt.df.ctx);
    let io_runtime = rt.tokio_io_runtime();

    let scheduler_config = SchedulerConfig {
        bind_host: bind_addr.ip().to_string(),
        bind_port: bind_addr.port(),

        override_logical_codec: Some(SpiceLogicalCodec::new_with_runtime(Arc::clone(rt))),
        override_physical_codec: Some(
            SpicePhysicalCodec::new(Arc::clone(rt))
                .boxed()
                .context(FailedToStartClusterSchedulerSnafu)?,
        ),

        grpc_server_max_decoding_message_size: u32::MAX,
        grpc_server_max_encoding_message_size: u32::MAX,

        override_session_builder: Some(Arc::new(move |_cfg| {
            let cfg = current_context
                .copied_config()
                .with_option_extension(SpiceClusterConfig::default());

            Ok(
                SessionStateBuilder::new_from_existing(current_context.as_ref().state().clone())
                    .with_config(cfg)
                    .with_runtime_env(default_runtime_env(io_runtime.clone()))
                    .with_physical_optimizer_rule(DistributeFileScanOptimizer::new())
                    .with_physical_optimizer_rule(UnionProjectionPushdownOptimizer::new())
                    .build(),
            )
        })),
        ..Default::default()
    };

    let cluster = BallistaCluster::new_from_config(&scheduler_config)
        .await
        .boxed()
        .context(FailedToStartClusterSchedulerSnafu)?;

    rt.status
        .update_cluster("scheduler", ComponentStatus::Ready);

    tracing::info!("Starting Ballista scheduler on {}", bind_addr);

    scheduler_process::create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(
        cluster,
        scheduler_config.into(),
    )
    .await
    .boxed()
    .context(FailedToStartClusterSchedulerSnafu)
}

/// Initializes relevant `App` runtime components retrieved from the scheduler node
async fn executor_bind_app(
    rt: &Arc<Runtime>,
    scheduler_flight_url: impl Into<Arc<str>>,
) -> crate::Result<()> {
    let flight_client = flight_client::FlightClient::try_new(
        scheduler_flight_url.into(),
        flight_client::Credentials::anonymous(),
        None,
    )
    .await
    .boxed()
    .context(FailedToStartClusterExecutorSnafu)?;

    let action = arrow_flight::Action {
        r#type: "GetAppDefinition".to_string(),
        body: bytes::Bytes::new(),
    };

    let response = flight_client
        .client()
        .clone()
        .do_action(action)
        .await
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?;

    let mut stream = response.into_inner();
    if let Some(result) = stream
        .message()
        .await
        .boxed()
        .context(FailedToStartClusterExecutorSnafu)?
    {
        let app_def: App = serde_json::from_slice(&result.body)
            .boxed()
            .context(FailedToStartClusterExecutorSnafu)?;

        *rt.app.write().await = Some(Arc::new(app_def));
    }

    Arc::clone(rt).load_catalogs().await;
    rt.load_embeddings().await;
    Arc::clone(rt).load_models().await;
    Arc::clone(rt).load_tools().await;

    Ok(())
}
