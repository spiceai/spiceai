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

//! A Cayenne table on an S3-compatible store keeps answering while the session
//! credentials its store signs with expire and are replaced.
//!
//! The store signs with `aws_sdk_credential_bridge::S3CredentialProvider`, the
//! provider an S3 data path gets when it signs in through the AWS SDK chain (IAM
//! roles, web identity, profiles), over a provider that takes real STS session
//! credentials from `RustFS` (`AssumeRole`). `RustFS` issues them for at least 15
//! minutes, so the provider reports each one as expiring after
//! [`REPORTED_LIFETIME`]: the bridge then fetches a new set every few seconds,
//! and every request after that is signed with a key `RustFS` issued moments
//! earlier. Writes and scans run across several of those replacements and must
//! all succeed with the right rows.
//!
//! Needs `RustFS`: `CAYENNE_TEST_RUSTFS_ENDPOINT`, `CAYENNE_TEST_RUSTFS_BUCKET`,
//! `CAYENNE_TEST_RUSTFS_ACCESS_KEY`, `CAYENNE_TEST_RUSTFS_SECRET_KEY` (the
//! nightly `RustFS` lane in `.github/workflows/cayenne_data_store.yml` sets them).

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::common::{self, TestFixture};
use arrow::array::Int64Array;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, future};
use cayenne::MetadataCatalog;
use cayenne::metadata::{CreateTableOptions, ObjectStoreConfig, VortexConfig};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// How long each set of session credentials is reported to live. The SDK's
/// identity cache refreshes 10 s before the reported expiry.
const REPORTED_LIFETIME: Duration = Duration::from_secs(15);

/// How many credential replacements the run must span.
const MIN_REPLACEMENTS: usize = 3;

/// The longest the run may take.
const DEADLINE: Duration = Duration::from_secs(120);

fn env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is not set; this test needs RustFS").into())
}

/// Session credentials from `RustFS`'s STS `AssumeRole`, one new set per call.
#[derive(Debug)]
struct RustfsSessionCredentials {
    sts: aws_sdk_sts::Client,
    issued: Arc<parking_lot::Mutex<Vec<String>>>,
}

impl RustfsSessionCredentials {
    async fn assume_role(&self) -> aws_credential_types::provider::Result {
        let output = self
            .sts
            .assume_role()
            .role_arn("arn:aws:iam::000000000000:role/cayenne-test")
            .role_session_name("cayenne-session-credentials-test")
            .duration_seconds(900)
            .send()
            .await
            .map_err(aws_credential_types::provider::error::CredentialsError::provider_error)?;
        let issued = output.credentials().ok_or_else(|| {
            aws_credential_types::provider::error::CredentialsError::provider_error(
                "AssumeRole returned no credentials",
            )
        })?;
        self.issued.lock().push(issued.access_key_id().to_string());
        Ok(Credentials::new(
            issued.access_key_id(),
            issued.secret_access_key(),
            Some(issued.session_token().to_string()),
            Some(SystemTime::now() + REPORTED_LIFETIME),
            "rustfs-sts",
        ))
    }
}

impl ProvideCredentials for RustfsSessionCredentials {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.assume_role())
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs RustFS (CAYENNE_TEST_RUSTFS_*); the nightly RustFS lane runs it"]
async fn a_table_keeps_answering_across_session_credential_replacements() -> TestResult {
    let endpoint = env("CAYENNE_TEST_RUSTFS_ENDPOINT")?;
    let bucket = env("CAYENNE_TEST_RUSTFS_BUCKET")?;
    let admin = Credentials::new(
        env("CAYENNE_TEST_RUSTFS_ACCESS_KEY")?,
        env("CAYENNE_TEST_RUSTFS_SECRET_KEY")?,
        None,
        None,
        "rustfs-admin",
    );
    let region = aws_config::Region::new("us-east-1");
    let sts = aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::Config::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .endpoint_url(&endpoint)
            .region(region.clone())
            .credentials_provider(admin)
            .build(),
    );
    let issued = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let sessions = RustfsSessionCredentials {
        sts,
        issued: Arc::clone(&issued),
    };
    // Loaded as Spice loads it, so the identity cache gets its clock and sleep.
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .credentials_provider(SharedCredentialsProvider::new(sessions))
        .load()
        .await;
    let signer = aws_sdk_credential_bridge::S3CredentialProvider::from_config(&sdk_config)?;
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket)
        .with_region("us-east-1")
        .with_allow_http(endpoint.starts_with("http://"))
        .with_virtual_hosted_style_request(false)
        .with_credentials(Arc::new(signer))
        .build()?;
    let data_url = format!(
        "s3://{bucket}/cayenne-test/{}/data",
        uuid::Uuid::now_v7().simple()
    );

    let fixture = TestFixture::new(common::BackendType::ObjectNative).await?;
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog);
    let ctx = SessionContext::new();
    let table = crate::common::table_builder_on_store(
        catalog,
        ctx.runtime_env(),
        ObjectStoreConfig {
            url: url::Url::parse(&data_url)?,
            store: Arc::new(store),
        },
    )
    .create(CreateTableOptions {
        table_name: "sessions".to_string(),
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_url,
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            inline_max_bytes: 0,
            ..VortexConfig::default()
        },
    })
    .await?;
    ctx.register_table("sessions", Arc::new(table) as Arc<dyn TableProvider>)?;

    let started = Instant::now();
    let mut written = 0_i64;
    loop {
        let first = written + 1;
        written += 500;
        ctx.sql(&format!(
            "INSERT INTO sessions SELECT value AS id, value * 10 AS v FROM generate_series({first}, {written})"
        ))
        .await?
        .collect()
        .await?;
        let batches = ctx
            .sql("SELECT count(*), sum(v) FROM sessions WHERE v % 3 = 0")
            .await?
            .collect()
            .await?;
        let batch = batches.first().ok_or("no result batch")?;
        let column = |i: usize| -> TestResult<i64> {
            Ok(batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("not an Int64 column")?
                .value(0))
        };
        let expected: Vec<i64> = (1..=written)
            .map(|id| id * 10)
            .filter(|v| v % 3 == 0)
            .collect();
        assert_eq!(
            (column(0)?, column(1)?),
            (i64::try_from(expected.len())?, expected.iter().sum::<i64>()),
            "after {written} rows"
        );
        let distinct: BTreeSet<String> = issued.lock().iter().cloned().collect();
        if distinct.len() > MIN_REPLACEMENTS {
            eprintln!(
                "{written} rows written and scanned over {} sets of session credentials in {:?}",
                distinct.len(),
                started.elapsed()
            );
            return Ok(());
        }
        assert!(
            started.elapsed() < DEADLINE,
            "only {} set(s) of session credentials were issued in {DEADLINE:?}: the store did not replace expiring credentials",
            distinct.len()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
