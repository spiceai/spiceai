/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! A cluster node acts only as the executor its own certificate registered:
//! the scheduler refuses a control stream, a partition allocation, or a Ballista
//! executor call that names another node's executor id, and the named executor
//! stays registered and serving.
//!
//! Reproduces the cross-node impersonation of the `cluster-trust` security
//! findings: before the identity binding, a node holding any certificate the
//! cluster CA issued could claim another executor's id, evict it, and redirect
//! its traffic.

use std::time::Duration;

use ballista_core::serde::protobuf::{
    GetJobStatusParams, scheduler_grpc_client::SchedulerGrpcClient,
};
use runtime_proto::{
    AllocateInitialPartitionsRequest, ExecutorControlMessage, ExecutorHeartbeat,
    cluster_service_client::ClusterServiceClient, executor_control_message::Message,
};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

/// A TLS channel to the scheduler's cluster port, presenting the certificate the
/// harness PKI issues for `node_name` — a node identity of the test's choosing.
async fn channel_as(harness: &ClusterHarness, node_name: &str) -> Channel {
    let pki = harness
        .cluster_pki()
        .expect("harness runs with cluster mTLS");
    let cert = pki
        .create_client_cert(node_name)
        .expect("issue a node certificate");
    let ca = std::fs::read(&pki.ca_cert_path).expect("read CA");
    let node_cert = std::fs::read(&cert.cert_path).expect("read node cert");
    let node_key = std::fs::read(&cert.key_path).expect("read node key");
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(node_cert, node_key))
        // The scheduler's certificate names `localhost`.
        .domain_name("localhost");
    let host_port = harness.scheduler_cluster_address();
    let port = host_port.rsplit_once(':').expect("host:port").1;
    Endpoint::from_shared(format!("https://localhost:{port}"))
        .expect("endpoint")
        .tls_config(tls)
        .expect("tls config")
        .connect()
        .await
        .expect("connect to scheduler cluster port")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_cannot_act_as_an_executor_another_node_registered() -> Result<(), anyhow::Error> {
    let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let harness = ClusterHarness::builder().executors(1).start().await?;
            let result = async {
                harness.wait_for_executors(Duration::from_secs(30)).await?;
                // The one executor the harness started registers as `executor0`.
                let victim = "executor0";
                assert_eq!(
                    harness.registered_executors().await?,
                    vec![(
                        victim.to_string(),
                        format!(
                            "127.0.0.1:{}",
                            harness.executors[0]
                                .datafusion()
                                .cluster_config
                                .node_bind_address()
                                .port()
                        ),
                    )],
                );

                // An intruder node holds a CA-issued certificate of its own.
                let intruder_channel = channel_as(&harness, "intruder").await;

                // (1) A control stream that claims the victim's id is refused
                //     before anything it carries is acted on.
                let mut cluster = ClusterServiceClient::new(intruder_channel.clone());
                let claim = victim.to_string();
                let outbound = tokio_stream::iter(vec![ExecutorControlMessage {
                    executor_id: claim.clone(),
                    process_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                    message: Some(Message::Heartbeat(ExecutorHeartbeat { timestamp_ms: 1 })),
                }]);
                let mut inbound = cluster
                    .control_stream(outbound)
                    .await
                    .expect("the stream opens; the refusal arrives on it")
                    .into_inner();
                match inbound.next().await {
                    Some(Err(status)) => {
                        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
                        assert!(
                            status.message().contains(victim)
                                && status.message().contains("registered"),
                            "refusal names the executor and the cause: {}",
                            status.message()
                        );
                    }
                    other => panic!("expected a permission-denied refusal, got {other:?}"),
                }

                // (2) A partition allocation that claims the victim's id, with an
                //     attacker endpoint, is refused.
                let allocate = cluster
                    .allocate_initial_partitions(AllocateInitialPartitionsRequest {
                        executor_url: "https://127.0.0.1:1".to_string(),
                        executor_id: claim.clone(),
                        process_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                    })
                    .await;
                let status = allocate.expect_err("partition allocation is refused");
                assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

                // (3) A Ballista executor-half call that names the victim is
                //     refused, and the client half is not served here at all.
                let mut scheduler = SchedulerGrpcClient::new(intruder_channel);
                let job = scheduler
                    .get_job_status(GetJobStatusParams {
                        job_id: "no-such-job".to_string(),
                    })
                    .await;
                let status = job.expect_err("the job methods are not served on the cluster port");
                assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");

                // The victim stayed registered throughout.
                let registered = harness
                    .scheduler_executor_registry()
                    .expect("scheduler has an executor registry")
                    .connected_executors()
                    .await;
                assert_eq!(registered, vec![victim.to_string()]);
                assert_eq!(harness.registered_executors().await?.len(), 1);
                Ok::<(), anyhow::Error>(())
            }
            .await;
            harness.shutdown().await;
            result
        })
        .await
}
