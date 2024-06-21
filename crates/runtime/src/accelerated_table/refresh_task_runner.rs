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

use super::refresh_task::RefreshTask;
use futures::future::BoxFuture;
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};

use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};
use tokio::sync::RwLock;

use datafusion::{datasource::TableProvider, sql::TableReference};

use super::refresh::Refresh;

#[derive(Debug)]
pub enum RefreshTaskResult {
    Success,
    Error,
}

impl Display for RefreshTaskResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshTaskResult::Success => write!(f, "Success"),
            RefreshTaskResult::Error => write!(f, "Error"),
        }
    }
}

pub struct RefreshTaskRunner {
    dataset_name: TableReference,
    federated: Arc<dyn TableProvider>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    task_thread: Option<JoinHandle<()>>,
}

impl RefreshTaskRunner {
    #[must_use]
    pub fn new(
        dataset_name: TableReference,
        federated: Arc<dyn TableProvider>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            dataset_name,
            federated,
            refresh,
            accelerator,
            task_thread: None,
        }
    }

    pub fn start(&mut self) -> (Sender<()>, Receiver<RefreshTaskResult>) {
        let (task_sender, mut task_receiver) = mpsc::channel::<()>(1);

        let (callback_sender, callback_receiver) = mpsc::channel::<RefreshTaskResult>(1);

        let dataset_name = self.dataset_name.clone();
        let callback_sender = Arc::new(callback_sender);

        let refresh_task = Arc::new(RefreshTask::new(
            dataset_name.clone(),
            Arc::clone(&self.federated),
            Arc::clone(&self.refresh),
            Arc::clone(&self.accelerator),
        ));

        self.task_thread = Some(tokio::spawn(async move {
            let mut task_completion: Option<BoxFuture<super::Result<()>>> = None;

            loop {
                if let Some(task) = task_completion.take() {
                    select! {
                        res = task => {
                            match res {
                                Ok(()) => {
                                    tracing::debug!("Refresh task successfully completed for dataset {dataset_name}");
                                    _ = callback_sender.send(RefreshTaskResult::Success).await;
                                },
                                Err(err) => {
                                    tracing::debug!("Refresh task for dataset {dataset_name} failed with error: {err}");
                                    _ = callback_sender.send(RefreshTaskResult::Error).await;
                                }
                            }
                        },
                        _ = task_receiver.recv() => {
                            task_completion = Some(Box::pin(refresh_task.run()));
                        }
                    }
                } else {
                    select! {
                        _ = task_receiver.recv() => {
                            task_completion = Some(Box::pin(refresh_task.run()));
                        }
                    }
                }
            }
        }));

        (task_sender, callback_receiver)
    }

    pub fn stop(&mut self) {
        if let Some(task_thread) = &self.task_thread {
            task_thread.abort();
            self.task_thread = None;
        }
    }
}
