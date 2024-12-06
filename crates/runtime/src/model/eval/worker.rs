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

use std::{collections::HashMap, sync::Arc};

use crate::model::{
    eval::{dataset::get_eval_data, run_model, scorer::score_results, Result},
    DatasetInput, DatasetOutput,
};
use crate::{
    datafusion::DataFusion,
    model::{LLMModelStore, Scorer},
};
use snafu::ResultExt;
use spicepod::component::eval::Eval;
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};

use super::{
    result::{write_result_to_table, ResultBuilder},
    runs::{add_metrics_to_eval_run, update_eval_run_status, EvalRunId, EvalRunStatus},
    scorer::result_metrics,
    Error, FailedToOffloadEvalRunSnafu,
};

/// Handles processing eval runs in a standalone thread.
pub struct EvalWorker {
    /// Run a [`EvalWorkerCommand`] in the background thread.
    command_sender: mpsc::Sender<EvalWorkerCommand>,

    /// Not used, but dropping [`EvalWorker`] will stop background worker [`EvalThread`].
    _backend_thread: Arc<EvalThread>,

    df: Arc<DataFusion>,
    /// Scorer registry, shared with [`EvalThread`], but allows [`EvalWorker`] to add scorers.
    scorers: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
}

impl EvalWorker {
    pub fn new(
        llms: Arc<RwLock<LLMModelStore>>,
        df: Arc<DataFusion>,
        scorers: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(8);

        Self {
            command_sender: tx,
            _backend_thread: Arc::new(EvalThread::new(
                rx,
                llms,
                Arc::clone(&df),
                Arc::clone(&scorers),
            )),
            scorers,
            df,
        }
    }

    pub async fn add_scorer(&self, name: &str, scorer: Arc<dyn Scorer>) {
        let mut scorers = self.scorers.write().await;
        scorers.insert(name.to_string(), scorer);
    }

    /// Start the eval run, but does not wait for completion.
    ///
    /// The `id` should already exist in the [`EVAL_RUNS_TABLE_REFERENCE`] table.
    ///
    /// To check for the status of the run, query the status of [`EVAL_RUNS_TABLE_REFERENCE`] table.
    pub async fn queue_eval_job(
        &self,
        id: &EvalRunId,
        eval: &Eval,
        model_name: &str,
    ) -> Result<()> {
        self.command_sender
            .send(EvalWorkerCommand::RunEval((
                id.clone(),
                eval.clone(),
                model_name.to_string(),
            )))
            .await
            .boxed()
            .context(FailedToOffloadEvalRunSnafu {
                eval_run_id: id.to_string(),
            })?;

        update_eval_run_status(Arc::clone(&self.df), id, &EvalRunStatus::Queued, None).await?;

        Ok(())
    }
}

pub enum EvalWorkerCommand {
    RunEval((EvalRunId, Eval, String)),
}

#[derive(Debug)]
struct EvalThread(Option<JoinHandle<()>>);

impl EvalThread {
    fn new(
        mut receiver: mpsc::Receiver<EvalWorkerCommand>,
        llms: Arc<RwLock<LLMModelStore>>,
        df: Arc<DataFusion>,
        scorers: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            while let Some(cmd) = receiver.recv().await {
                match cmd {
                    EvalWorkerCommand::RunEval((id, eval, model_name)) => {
                        // Set [`EvalRunStatus::Running`]
                        if let Err(e) = update_eval_run_status(
                            Arc::clone(&df),
                            &id,
                            &EvalRunStatus::Running,
                            None,
                        )
                        .await
                        {
                            tracing::error!("{e}");
                        }

                        tracing::trace!("Running eval job='{id}'.");
                        let (status, err_opt) = match run_eval(
                            &id,
                            Arc::clone(&llms),
                            model_name,
                            &eval,
                            Arc::clone(&df),
                            Arc::clone(&scorers),
                        )
                        .await
                        {
                            Err(e) => (EvalRunStatus::Failed, Some(e.to_string())),
                            Ok(()) => (EvalRunStatus::Completed, None),
                        };

                        if let Err(e) =
                            update_eval_run_status(Arc::clone(&df), &id, &status, err_opt).await
                        {
                            tracing::error!("{e}");
                        }
                    }
                }
            }
        });
        Self(Some(handle))
    }
}

/// The core logic for [`EvalWorkerCommand::RunEval`].
///
/// Does not handle updating the status of the eval run.
#[allow(clippy::implicit_hasher)]
pub async fn run_eval(
    id: &EvalRunId,
    llm_store: Arc<RwLock<LLMModelStore>>,
    model_name: String,
    eval: &Eval,
    df: Arc<DataFusion>,
    scorer_registry: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
) -> Result<()> {
    let (input, ideal) = get_eval_data(Arc::clone(&df), eval).await?;

    let llms = llm_store.read().await;
    let model = llms
        .get(&model_name)
        .ok_or_else(|| Error::FailedToGetModel {
            model_name: model_name.clone(),
            eval_name: eval.name.clone(),
        })?;

    let actual: Vec<DatasetOutput> = if let Some(first_ideal) = ideal.first() {
        run_model(eval.name.clone(), Arc::clone(model), &input, first_ideal).await?
    } else {
        // Not error, no data in dataset
        vec![]
    };

    let scorers_to_use = get_scorers_for_eval(eval, Arc::clone(&scorer_registry)).await?;

    let scores = score_results(&input, &actual, &ideal, &scorers_to_use).await;
    write_results(id, Arc::clone(&df), &input, &actual, &ideal, &scores).await?;

    let metrics = result_metrics(scores, &scorers_to_use).await;
    add_metrics_to_eval_run(Arc::clone(&df), id, &metrics).await?;
    Ok(())
}

async fn get_scorers_for_eval(
    eval: &Eval,
    scorers: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
) -> Result<HashMap<String, Arc<dyn Scorer>>> {
    let mut scorer_subset = HashMap::with_capacity(eval.scorers.len());
    for name in &eval.scorers {
        let scorers_unlock = scorers.read().await;
        let scorer = scorers_unlock
            .get(name)
            .ok_or_else(|| Error::EvalScorerUnavailable {
                scorer_name: name.clone(),
                eval_name: eval.name.clone(),
            })?;
        scorer_subset.insert(name.clone(), Arc::clone(scorer));
    }
    Ok(scorer_subset)
}

async fn write_results(
    run_id: &EvalRunId,
    df: Arc<DataFusion>,
    input: &[DatasetInput],
    output: &[DatasetOutput],
    expected: &[DatasetOutput],
    scores: &HashMap<String, Vec<f32>>,
) -> Result<()> {
    let mut bldr = ResultBuilder::new();
    for i in 0..input.len() {
        let input = &input[i];
        let output = &output[i];
        let expected = &expected[i];
        for (name, score) in scores {
            bldr.append(
                run_id,
                chrono::Utc::now(),
                input,
                output,
                expected,
                name,
                score[i],
            )?;
        }
    }

    write_result_to_table(Arc::clone(&df), run_id, &mut bldr).await
}

impl Drop for EvalThread {
    fn drop(&mut self) {
        if let EvalThread(Some(v)) = self {
            v.abort();
        } else {
            tracing::warn!("Eval runner background process already stopped");
        };
    }
}
