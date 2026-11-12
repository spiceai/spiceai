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

//! Serves a Strands decider (`/v1/decisions`) on llama.cpp: the base model GGUF with the
//! `LoRA` adapter GGUF applied at decode time, and the checkpoint's pointer head
//! ([`strands_decider`]).
//!
//! ```text
//!  evaluate() ──permit──► job channel ──► worker 1: context 1 ─┐
//!     ▲                                └► worker N: context N ─┤ one Model + LoraAdapter,
//!     └──────────── answers (oneshot) ◄────────────────────────┘ owned by one thread
//! ```
//!
//! One thread loads the model and the adapter and owns them; [`DECIDER_CONTEXTS`] worker
//! threads borrow them, each with its own context, so that many requests decode at once and
//! a decode never blocks an async worker. Dropping the decider stops the workers and waits for
//! the model to be freed.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;

use async_trait::async_trait;
use llama_cpp::{Backend, LoraAdapter, Model, ModelParams};
use llms::decider::{BoxError, DeciderFiles, DeciderLoad, DeciderRequest, LocalDeciderEngine};
use llms::evaluate::{
    Error as EvaluateError, Evaluate, EvaluateRequest, EvaluateResponse, Result as EvaluateResult,
};
use runtime_rate_control::RateController;
use strands_decider::llama::LlamaHiddenStates;
use strands_decider::{Content, Decider, Evaluation, Question};
use tokio::sync::oneshot;

llms::register_local_decider_engine!(
    LLAMA_CPP_DECIDER,
    LocalDeciderEngine {
        name: "llama.cpp",
        load: load_decider,
    }
);

/// Requests decoded at once, one per context. Each context holds a cache for
/// [`QUESTIONS_PER_DECODE`] prompts of the checkpoint's window.
const DECIDER_CONTEXTS: usize = 2;

/// The most questions of one request decoded together after its state; more are decoded in
/// groups.
const QUESTIONS_PER_DECODE: NonZeroU32 = NonZeroU32::MIN.saturating_add(7);

/// The most tokens one decode takes.
const DECODE_BATCH: u32 = 512;

fn load_decider(
    load: DeciderLoad<'_>,
) -> futures::future::BoxFuture<'_, Result<Arc<dyn Evaluate>, BoxError>> {
    Box::pin(async move {
        LlamaDecider::start(load)
            .await
            .map(|decider| Arc::new(decider) as Arc<dyn Evaluate>)
    })
}

/// One request for a worker.
struct Job {
    state: Content,
    questions: Vec<(String, Question)>,
    answers: oneshot::Sender<strands_decider::Result<Evaluation>>,
}

/// A Strands decider served on llama.cpp.
#[derive(Debug)]
pub struct LlamaDecider {
    name: String,
    /// `None` once dropping, which stops the workers.
    jobs: Option<std_mpsc::Sender<Job>>,
    /// The thread that owns the model; it ends once every worker has.
    owner: Option<thread::JoinHandle<()>>,
    rate_controller: Arc<RateController>,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("questions", &self.questions.len())
            .finish_non_exhaustive()
    }
}

impl LlamaDecider {
    /// Loads the decider's readout, model and adapter on a thread of its own, and returns once
    /// its workers are ready.
    async fn start(load: DeciderLoad<'_>) -> Result<Self, BoxError> {
        let files = load.files.clone();
        let (jobs, receiver) = std_mpsc::channel();
        let (ready, loaded) = oneshot::channel();
        let owner = thread::Builder::new()
            .name(format!("decider-{}", load.name))
            .spawn(move || serve(&files, receiver, ready))?;
        match loaded.await {
            Ok(Ok(())) => Ok(Self {
                name: load.name.to_string(),
                jobs: Some(jobs),
                owner: Some(owner),
                rate_controller: load.rate_controller,
            }),
            Ok(Err(e)) => {
                let _ = owner.join();
                Err(e)
            }
            Err(_) => {
                let _ = owner.join();
                Err("the decider's thread stopped while it loaded".into())
            }
        }
    }

    fn failed(&self, source: impl Into<BoxError>) -> EvaluateError {
        EvaluateError::ModelCallFailed {
            model: self.name.clone(),
            source: source.into(),
        }
    }
}

impl Drop for LlamaDecider {
    fn drop(&mut self) {
        // Closing the channel ends each worker after its current request, then the owner frees
        // the model; waiting for it means a reload never holds two copies.
        self.jobs.take();
        if let Some(owner) = self.owner.take() {
            let _ = owner.join();
        }
    }
}

/// The owner thread: loads everything, reports through `ready`, then lends the model to the
/// workers until the channel closes.
fn serve(
    files: &DeciderFiles,
    jobs: std_mpsc::Receiver<Job>,
    ready: oneshot::Sender<Result<(), BoxError>>,
) {
    let backend = Backend::get();
    let decider = match Decider::from_config_file(&files.config) {
        Ok(decider) => decider,
        Err(e) => {
            let _ = ready.send(Err(e.into()));
            return;
        }
    };
    let model = match load_model(backend, &files.base) {
        Ok(model) => model,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let contexts = match contexts(backend, &model, &decider, &files.adapter) {
        Ok(contexts) => contexts,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    if ready.send(Ok(())).is_err() {
        return;
    }
    let jobs = Mutex::new(jobs);
    thread::scope(|scope| {
        for (i, hidden) in contexts.into_iter().enumerate() {
            let (decider, jobs) = (&decider, &jobs);
            let spawned = thread::Builder::new()
                .name(format!("decider-worker-{i}"))
                .spawn_scoped(scope, move || work(decider, hidden, jobs));
            if let Err(e) = spawned {
                tracing::warn!("A decider worker thread could not start: {e}");
            }
        }
    });
}

fn load_model(backend: &Backend, base: &Path) -> Result<Model, BoxError> {
    Ok(Model::load(backend, base, &ModelParams::default())?)
}

/// One context per worker, each with the adapter applied.
fn contexts<'m>(
    backend: &Backend,
    model: &'m Model,
    decider: &Decider,
    adapter: &Path,
) -> Result<Vec<LlamaHiddenStates<'m>>, BoxError> {
    let adapter = Arc::new(LoraAdapter::load(model, adapter)?);
    let max_length = u32::try_from(decider.config().max_length)?;
    (0..DECIDER_CONTEXTS)
        .map(|_| {
            LlamaHiddenStates::new(
                backend,
                model,
                Some(Arc::clone(&adapter)),
                max_length,
                QUESTIONS_PER_DECODE,
                DECODE_BATCH,
                None,
            )
            .map_err(Into::into)
        })
        .collect()
}

/// A worker: answers jobs on its context until the channel closes.
fn work(
    decider: &Decider,
    mut hidden: LlamaHiddenStates<'_>,
    jobs: &Mutex<std_mpsc::Receiver<Job>>,
) {
    loop {
        let job = jobs.lock().unwrap_or_else(PoisonError::into_inner).recv();
        let Ok(job) = job else {
            return;
        };
        // The caller went away while it waited, so no one reads the answers.
        if job.answers.is_closed() {
            continue;
        }
        let answers = decider.evaluate(&mut hidden, &job.state, &job.questions);
        let _ = job.answers.send(answers);
    }
}

#[async_trait]
impl Evaluate for LlamaDecider {
    async fn evaluate(&self, request: EvaluateRequest) -> EvaluateResult<EvaluateResponse> {
        let asked = DeciderRequest::new(&request).map_err(|e| EvaluateError::InvalidRequest {
            model: self.name.clone(),
            message: e.0,
        })?;
        if let Some(answers) = asked.foregone_only() {
            return Ok(EvaluateResponse {
                model: self.name.clone(),
                answers,
                usage: Some(llms::evaluate::Usage::default()),
            });
        }

        let _permit =
            self.rate_controller
                .acquire()
                .await
                .map_err(|e| EvaluateError::RatePermitFailed {
                    model: self.name.clone(),
                    source: Box::new(e),
                })?;
        let (answers, answered) = oneshot::channel();
        let job = Job {
            state: asked.state.clone(),
            questions: asked.questions.clone(),
            answers,
        };
        self.jobs
            .as_ref()
            .ok_or("the decider is stopping")
            .and_then(|jobs| jobs.send(job).map_err(|_| "the decider's workers stopped"))
            .map_err(|e| EvaluateError::ServiceUnavailable {
                model: self.name.clone(),
                message: e.to_string(),
            })?;
        let evaluation = answered
            .await
            .map_err(|_| self.failed("the decider's worker stopped before it answered"))?
            .map_err(|e| match e {
                strands_decider::Error::InvalidQuestion { .. }
                | strands_decider::Error::NoQuestions
                | strands_decider::Error::PromptTooLong { .. }
                | strands_decider::Error::OptionTruncated { .. } => EvaluateError::InvalidRequest {
                    model: self.name.clone(),
                    message: e.to_string(),
                },
                e => self.failed(e),
            })?;
        Ok(asked.response(&self.name, evaluation))
    }

    /// Healthy while the thread that owns the model runs: the model is local, so there is no
    /// provider to ask.
    async fn health(&self) -> EvaluateResult<()> {
        if self
            .owner
            .as_ref()
            .is_some_and(|owner| !owner.is_finished())
        {
            Ok(())
        } else {
            Err(EvaluateError::HealthCheckFailed {
                source: format!("the workers of decider '{}' stopped", self.name).into(),
            })
        }
    }
}
