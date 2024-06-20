/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::sync::Arc;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::accelerated_table::refresh_task::RefreshTask;
use crate::accelerated_table::refresh_task_runner::RefreshTaskResult;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::TimeFormat;
use cache::QueryResultsCacheProvider;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

use super::refresh_task_runner::RefreshTaskRunner;

#[derive(Clone, Debug)]
pub struct Refresh {
    pub(crate) time_column: Option<String>,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) check_interval: Option<Duration>,
    pub(crate) sql: Option<String>,
    pub(crate) mode: RefreshMode,
    pub(crate) period: Option<Duration>,
    pub(crate) append_overlap: Option<Duration>,
}

impl Refresh {
    #[allow(clippy::needless_pass_by_value)]
    #[must_use]
    pub fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        check_interval: Option<Duration>,
        sql: Option<String>,
        mode: RefreshMode,
        period: Option<Duration>,
        append_overlap: Option<Duration>,
    ) -> Self {
        Self {
            time_column,
            time_format,
            check_interval,
            sql,
            mode,
            period,
            append_overlap,
        }
    }
}

impl Default for Refresh {
    fn default() -> Self {
        Self {
            time_column: None,
            time_format: None,
            check_interval: None,
            sql: None,
            mode: RefreshMode::Full,
            period: None,
            append_overlap: None,
        }
    }
}

pub(crate) enum AccelerationRefreshMode {
    Full(Receiver<()>),
    Append(Option<Receiver<()>>),
}

pub struct Refresher {
    dataset_name: TableReference,
    federated: Arc<dyn TableProvider>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    refresh_task_runner: RefreshTaskRunner,
}

impl Refresher {
    pub(crate) fn new(
        dataset_name: TableReference,
        federated: Arc<dyn TableProvider>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
    ) -> Self {
        let refresh_task_runner = RefreshTaskRunner::new(
            dataset_name.clone(),
            Arc::clone(&federated),
            Arc::clone(&refresh),
            Arc::clone(&accelerator),
        );

        Self {
            dataset_name,
            federated,
            refresh,
            accelerator,
            cache_provider: None,
            refresh_task_runner,
        }
    }

    pub fn cache_provider(
        &mut self,
        cache_provider: Option<Arc<QueryResultsCacheProvider>>,
    ) -> &mut Self {
        self.cache_provider = cache_provider;
        self
    }

    fn start_streamed_append(
        &mut self,
        ready_sender: oneshot::Sender<()>,
    ) -> tokio::task::JoinHandle<()> {
        let refresh_task = Arc::new(RefreshTask::new(
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            Arc::clone(&self.refresh),
            Arc::clone(&self.accelerator),
        ));

        tokio::spawn(async move {
            if let Err(err) = refresh_task.start_streamed_append(Some(ready_sender)).await {
                tracing::error!("Failed to start append refresh: {err}");
            }
        })
    }

    pub(crate) async fn start(
        &mut self,
        acceleration_refresh_mode: AccelerationRefreshMode,
        ready_sender: oneshot::Sender<()>,
    ) -> tokio::task::JoinHandle<()> {
        let time_column = self.refresh.read().await.time_column.clone();

        let mut refresh_receiver = match acceleration_refresh_mode {
            AccelerationRefreshMode::Append(receiver) => {
                if let (Some(receiver), Some(_)) = (receiver, time_column) {
                    receiver
                } else {
                    return self.start_streamed_append(ready_sender);
                }
            }
            AccelerationRefreshMode::Full(receiver) => receiver,
        };

        let (task_sender, mut task_callback) = self.refresh_task_runner.start();

        let mut ready_sender = Some(ready_sender);
        let dataset_name = self.dataset_name.clone();
        let refresh = Arc::clone(&self.refresh);

        let cache_provider = self.cache_provider.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = refresh_receiver.recv() => {
                        tracing::debug!("Received outside triggered for refresh");

                        if let Err(err) = task_sender.send(()).await {
                            tracing::error!("Failed to execute refresh: {err}");
                        }
                    },
                    Some(res) = task_callback.recv() => {
                        tracing::debug!("Received refresh task completion callback: {res}");

                        match res {
                            RefreshTaskResult::Success => {
                                notify_refresh_done(&dataset_name, &refresh, &mut ready_sender).await;

                                if let Some(cache_provider) = &cache_provider {
                                    if let Err(e) = cache_provider
                                        .invalidate_for_table(&dataset_name.to_string())
                                        .await
                                    {
                                        tracing::error!("Failed to invalidate cached results for dataset {}: {e}", &dataset_name.to_string());
                                    }
                                }
                            },
                            RefreshTaskResult::Error => {
                            }
                        }
                    }
                }
            }
        })
    }
}

async fn notify_refresh_done(
    dataset_name: &TableReference,
    refresh: &Arc<RwLock<Refresh>>,
    ready_sender: &mut Option<oneshot::Sender<()>>,
) {
    if let Some(sender) = ready_sender.take() {
        sender.send(()).ok();
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut labels = vec![("dataset", dataset_name.to_string())];
    if let Some(sql) = &refresh.read().await.sql {
        labels.push(("sql", sql.to_string()));
    };

    metrics::gauge!("datasets_acceleration_last_refresh_time", &labels).set(now.as_secs_f64());
}

impl Drop for Refresher {
    fn drop(&mut self) {
        self.refresh_task_runner.stop();
    }
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use arrow::{
        array::{ArrowNativeTypeOp, RecordBatch, StringArray, StructArray, UInt64Array},
        datatypes::{DataType, Fields, Schema},
    };
    use data_components::arrow::write::MemTable;
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use tokio::{sync::mpsc, time::timeout};

    use crate::status;

    use super::*;

    async fn setup_and_test(
        source_data: Vec<&str>,
        existing_data: Vec<&str>,
        expected_size: usize,
    ) {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "time_in_string",
            DataType::Utf8,
            false,
        )]));
        let arr = StringArray::from(source_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let federated = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                .expect("mem table should be created"),
        );

        let arr = StringArray::from(existing_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let accelerator = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        let refresh = Refresh::new(None, None, None, None, RefreshMode::Full, None, None);

        let mut refresher = Refresher::new(
            TableReference::bare("test"),
            federated,
            Arc::new(RwLock::new(refresh)),
            Arc::clone(&accelerator),
        );

        let (trigger, receiver) = mpsc::channel::<()>(1);
        let (ready_sender, is_ready) = oneshot::channel::<()>();
        let acceleration_refresh_mode = AccelerationRefreshMode::Full(receiver);
        let refresh_handle = refresher
            .start(acceleration_refresh_mode, ready_sender)
            .await;

        trigger
            .send(())
            .await
            .expect("trigger sent correctly to refresh");

        timeout(Duration::from_secs(2), async move {
            is_ready.await.expect("data is received");
        })
        .await
        .expect("finish before the timeout");

        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = accelerator
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        assert_eq!(expected_size, result.first().expect("result").num_rows());

        drop(refresh_handle);
    }

    #[tokio::test]
    async fn test_refresh_full() {
        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;
        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            3,
        )
        .await;
        setup_and_test(
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn test_refresh_status_change_to_ready() {
        fn wait_until_ready_status(
            snapshotter: &Snapshotter,
            desired: status::ComponentStatus,
        ) -> bool {
            for _i in 1..20 {
                let hashmap = snapshotter.snapshot().into_vec();
                let (_, _, _, value) = hashmap.first().expect("at least one metric exists");
                match value {
                    DebugValue::Gauge(i) => {
                        let value = i.into_inner();

                        if value.is_eq(f64::from(desired as i32)) {
                            return true;
                        }
                    }
                    _ => panic!("not testing this"),
                }

                sleep(Duration::from_micros(100));
            }

            false
        }

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::set_global_recorder(recorder).expect("recorder is set globally");

        status::update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;

        assert!(wait_until_ready_status(
            &snapshotter,
            status::ComponentStatus::Ready
        ));

        status::update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(vec![], vec![], 0).await;

        assert!(wait_until_ready_status(
            &snapshotter,
            status::ComponentStatus::Ready
        ));
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_iso8601() {
        async fn test(
            source_data: Vec<&str>,
            existing_data: Vec<&str>,
            expected_size: usize,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time_in_string",
                DataType::Utf8,
                false,
            )]));
            let arr = StringArray::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = StringArray::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time_in_string".to_string()),
                None,
                None,
                None,
                RefreshMode::Append,
                None,
                None,
            );

            let mut refresher = Refresher::new(
                TableReference::bare("test"),
                federated,
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
            );

            let (trigger, receiver) = mpsc::channel::<()>(1);
            let (ready_sender, is_ready) = oneshot::channel::<()>();
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(Some(receiver));
            let refresh_handle = refresher
                .start(acceleration_refresh_mode, ready_sender)
                .await;

            trigger
                .send(())
                .await
                .expect("trigger sent correctly to refresh");

            timeout(Duration::from_secs(2), async move {
                is_ready.await.expect("data is received");
            })
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec!["2012-12-01T11:11:16Z", "2012-12-01T11:11:17Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            6,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec!["2012-12-01T11:11:15Z", "2012-12-01T11:11:15Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should not apply same timestamp data",
        )
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = UInt64Array::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time".to_string()),
                time_format,
                None,
                None,
                RefreshMode::Append,
                None,
                append_overlap,
            );

            let mut refresher = Refresher::new(
                TableReference::bare("test"),
                federated,
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
            );

            let (trigger, receiver) = mpsc::channel::<()>(1);
            let (ready_sender, is_ready) = oneshot::channel::<()>();
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(Some(receiver));
            let refresh_handle = refresher
                .start(acceleration_refresh_mode, ready_sender)
                .await;

            trigger
                .send(())
                .await
                .expect("trigger sent correctly to refresh");

            timeout(Duration::from_secs(2), async move {
                is_ready.await.expect("data is received");
            })
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            None,
            Some(Duration::from_secs(3)),
            "should default to time unix seconds",
        )
        .await;
        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp_with_more_complicated_structs() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            message: &str,
        ) {
            let original_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");

            let struct_array = StructArray::from(batch);
            let schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("time", DataType::UInt64, false),
                arrow::datatypes::Field::new(
                    "struct",
                    DataType::Struct(Fields::from(vec![arrow::datatypes::Field::new(
                        "time",
                        DataType::UInt64,
                        false,
                    )])),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let federated = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );

            let arr = UInt64Array::from(existing_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");
            let struct_array = StructArray::from(batch);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(
                Some("time".to_string()),
                time_format,
                None,
                None,
                RefreshMode::Append,
                None,
                append_overlap,
            );

            let mut refresher = Refresher::new(
                TableReference::bare("test"),
                federated,
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
            );

            let (trigger, receiver) = mpsc::channel::<()>(1);
            let (ready_sender, is_ready) = oneshot::channel::<()>();
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(Some(receiver));
            let refresh_handle = refresher
                .start(acceleration_refresh_mode, ready_sender)
                .await;
            trigger
                .send(())
                .await
                .expect("trigger sent correctly to refresh");

            timeout(Duration::from_secs(2), async move {
                is_ready.await.expect("data is received");
            })
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            None,
            Some(Duration::from_secs(3)),
            "should default to time unix seconds",
        )
        .await;
        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }
}
