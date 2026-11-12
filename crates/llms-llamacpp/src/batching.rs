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

//! Continuous batching: several requests decoded together, each in its own sequence of one
//! shared cache. Every step decodes one batch that holds each running request's next token, and
//! with a draft head its draft, so the weights are read once for all of them. A request that
//! arrives while others run reads its prompt into a free sequence between steps, and decodes in
//! the next step.
//!
//! A request is admitted with room for its whole possible reply, its prompt plus `max_tokens`
//! or the rest of the context, so a running request never runs out of cache. A request that does
//! not fit beside the running ones waits, and the requests behind it wait in order. A model that
//! is otherwise idle admits any request whose prompt fits the context, as one request at a time
//! does.
//!
//! An idle prompt leaves the shared cache for host memory when a request is admitted, unless
//! that request continues it, and when a step needs its room; a request that continues a saved
//! prompt restores it, as llama.cpp's server keeps prompts in RAM. Each cell an idle prompt kept
//! in the shared cache would widen every step's attention for the running requests, and change
//! the kernels' sums, and so their replies. A request that finishes while others run keeps its
//! prompt in its sequence until the next admission or until a step needs the room, so the
//! running ones are not held up while it saves.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use async_openai::types::chat::FinishReason;
use llama_cpp::{Batch, Context, DraftKind, DraftRequest, Model, Mtp, Token};
use tokio::sync::mpsc;

use super::{
    CachedPrompt, DECODE_STOPPED, Decoder, Event, HOST_CACHE_BYTES, Job, Output, PROMPT_BATCH,
    Picker, PromptCache, Target, read_prompt, send_unless_stopping, shared_prefix,
};

/// How long the decode thread waits when every running request waits on its client.
const PAUSED_POLL: Duration = Duration::from_millis(1);

/// Serves `jobs` up to `running` at a time, each request in its own sequence of the shared
/// cache, one per entry of `caches`, until the queue closes and every request has finished.
/// Idle prompts move to host memory and back as requests need them.
pub(super) fn serve<'m>(
    model: &'m Model,
    decoder: &mut Decoder<'m>,
    split: bool,
    (caches, running): (Vec<PromptCache>, usize),
    jobs: &std_mpsc::Receiver<Job>,
    eog: &[Token],
) {
    match decoder {
        Decoder::Plain(context) => run(model, context, split, (caches, running), jobs, eog),
        Decoder::Mtp(spec) => run(model, spec.as_mut(), split, (caches, running), jobs, eog),
    }
}

/// What a step needs from either engine, across every sequence of the shared cache.
trait Shared<'m> {
    fn context(&self) -> &Context<'m>;
    fn context_mut(&mut self) -> &mut Context<'m>;
    /// The most tokens one draft proposes; zero without a draft head.
    fn n_draft(&self) -> usize;
    /// Drafts for every request's sequence in one pass of the head; empty drafts without one.
    fn draft(
        &mut self,
        requests: &[DraftRequest<'_>],
    ) -> std::result::Result<Vec<Vec<Token>>, String>;
    /// Cuts the draft head's cache of `seq` back to `from`: drafting wrote positions there that
    /// the verify batch writes again.
    fn truncate_draft(&mut self, seq: i32, from: i32) -> bool;
    /// Called after every target decode; the MTP head reads the batch's hidden states.
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String>;
    /// Starts drafting in `seq`, whose prompt is decoded.
    fn begin(&mut self, seq: i32, prompt: &[Token]) -> std::result::Result<(), String>;
    /// Reports how many of `seq`'s drafted tokens the target accepted.
    fn accept(&mut self, seq: i32, accepted: u16) -> std::result::Result<(), String>;
    /// Removes `seq`'s entries from position `from` on, in every cache.
    fn truncate(&mut self, seq: i32, from: i32) -> bool;
    fn has_head(&self) -> bool;
    fn head_state(&mut self, seq: i32) -> Option<Vec<u8>>;
    fn restore_head_state(&mut self, seq: i32, state: &[u8]) -> bool;
    /// `seq`'s whole state in every cache, and the draft head's: everything a request that
    /// continues its prompt needs, to restore into an empty sequence. `None` when a part does
    /// not save.
    fn save_seq(&mut self, seq: i32) -> Option<SeqState>;
    /// Restores what `save_seq` returned into `seq`, which holds nothing; false when a part does
    /// not restore, which leaves `seq` to be emptied.
    fn restore_seq(&mut self, seq: i32, state: &SeqState) -> bool;
    /// About how many bytes `save_seq` returns for `seq`, counted without reading the cells.
    fn seq_state_size(&mut self, seq: i32) -> usize;
}

/// A sequence's state saved to host memory ([`Shared::save_seq`]).
struct SeqState {
    target: Vec<u8>,
    /// A draft model's cache for the sequence.
    draft: Option<Vec<u8>>,
    /// The draft head's own state for the sequence.
    head: Option<Vec<u8>>,
}

impl SeqState {
    fn bytes(&self) -> usize {
        self.target.len()
            + self.draft.as_ref().map_or(0, Vec::len)
            + self.head.as_ref().map_or(0, Vec::len)
    }
}

impl<'m> Shared<'m> for Context<'m> {
    fn context(&self) -> &Context<'m> {
        self
    }
    fn context_mut(&mut self) -> &mut Context<'m> {
        self
    }
    fn n_draft(&self) -> usize {
        0
    }
    fn draft(
        &mut self,
        requests: &[DraftRequest<'_>],
    ) -> std::result::Result<Vec<Vec<Token>>, String> {
        Ok(vec![Vec::new(); requests.len()])
    }
    fn truncate_draft(&mut self, _: i32, _: i32) -> bool {
        true
    }
    fn decoded(&mut self, _: &Batch) -> std::result::Result<(), String> {
        Ok(())
    }
    fn begin(&mut self, _: i32, _: &[Token]) -> std::result::Result<(), String> {
        Ok(())
    }
    fn accept(&mut self, _: i32, _: u16) -> std::result::Result<(), String> {
        Ok(())
    }
    fn truncate(&mut self, seq: i32, from: i32) -> bool {
        self.truncate_seq(seq, from)
    }
    fn has_head(&self) -> bool {
        false
    }
    fn head_state(&mut self, _: i32) -> Option<Vec<u8>> {
        None
    }
    fn restore_head_state(&mut self, _: i32, _: &[u8]) -> bool {
        false
    }
    fn save_seq(&mut self, seq: i32) -> Option<SeqState> {
        Some(SeqState {
            target: self.state_seq(seq)?,
            draft: None,
            head: None,
        })
    }
    fn restore_seq(&mut self, seq: i32, state: &SeqState) -> bool {
        self.restore_state_seq(seq, &state.target)
    }
    fn seq_state_size(&mut self, seq: i32) -> usize {
        self.state_seq_size(seq)
    }
}

impl<'m> Shared<'m> for Mtp<'m> {
    fn context(&self) -> &Context<'m> {
        self.target()
    }
    fn context_mut(&mut self) -> &mut Context<'m> {
        self.target_mut()
    }
    fn n_draft(&self) -> usize {
        self.n_max()
    }
    fn draft(
        &mut self,
        requests: &[DraftRequest<'_>],
    ) -> std::result::Result<Vec<Vec<Token>>, String> {
        self.draft_many(requests).map_err(|e| e.to_string())
    }
    fn truncate_draft(&mut self, seq: i32, from: i32) -> bool {
        self.draft_mut().truncate_seq(seq, from)
    }
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String> {
        self.process(batch).map_err(|e| e.to_string())
    }
    fn begin(&mut self, seq: i32, prompt: &[Token]) -> std::result::Result<(), String> {
        Mtp::begin(self, seq, prompt).map_err(|e| e.to_string())
    }
    fn accept(&mut self, seq: i32, accepted: u16) -> std::result::Result<(), String> {
        Mtp::accept(self, seq, accepted).map_err(|e| e.to_string())
    }
    fn truncate(&mut self, seq: i32, from: i32) -> bool {
        self.target_mut().truncate_seq(seq, from) && self.draft_mut().truncate_seq(seq, from)
    }
    fn has_head(&self) -> bool {
        // as for one sequence: only an MTP head carries a hidden state from one read to the next
        self.kind() == DraftKind::Mtp
    }
    fn head_state(&mut self, seq: i32) -> Option<Vec<u8>> {
        self.state(seq).ok().flatten()
    }
    fn restore_head_state(&mut self, seq: i32, state: &[u8]) -> bool {
        self.restore_state(seq, state).is_ok()
    }
    fn save_seq(&mut self, seq: i32) -> Option<SeqState> {
        Some(SeqState {
            target: self.target().state_seq(seq)?,
            draft: Some(self.draft_mut().state_seq(seq)?),
            head: self.state(seq).ok()?,
        })
    }
    fn restore_seq(&mut self, seq: i32, state: &SeqState) -> bool {
        self.target_mut().restore_state_seq(seq, &state.target)
            && state
                .draft
                .as_ref()
                .is_none_or(|draft| self.draft_mut().restore_state_seq(seq, draft))
            && state
                .head
                .as_ref()
                .is_none_or(|head| self.restore_state(seq, head).is_ok())
    }
    fn seq_state_size(&mut self, seq: i32) -> usize {
        // the head's own state is a few hidden rows, small beside the caches
        self.target().state_seq_size(seq) + self.draft_mut().state_seq_size(seq)
    }
}

/// One sequence of the shared cache, as prompt reading and the prompt cache see a target.
struct InSeq<'a, S> {
    shared: &'a mut S,
    seq: i32,
}

impl<'m, S: Shared<'m>> Target<'m> for InSeq<'_, S> {
    fn context(&self) -> &Context<'m> {
        self.shared.context()
    }
    fn context_mut(&mut self) -> &mut Context<'m> {
        self.shared.context_mut()
    }
    fn truncate(&mut self, from: i32) -> bool {
        self.shared.truncate(self.seq, from)
    }
    fn clear(&mut self) {
        // only this sequence: the others hold requests still decoding
        if !self.shared.truncate(self.seq, 0) {
            tracing::debug!("Could not empty sequence {} of the cache.", self.seq);
        }
    }
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String> {
        self.shared.decoded(batch)
    }
    fn has_head(&self) -> bool {
        self.shared.has_head()
    }
    fn head_state(&mut self) -> Option<Vec<u8>> {
        self.shared.head_state(self.seq)
    }
    fn restore_head_state(&mut self, state: &[u8]) -> bool {
        self.shared.restore_head_state(self.seq, state)
    }
    fn seq(&self) -> i32 {
        self.seq
    }
    fn partial_state(&self) -> Option<Vec<u8>> {
        self.shared.context().partial_state_seq(self.seq)
    }
    fn restore_partial_state(&mut self, state: &[u8]) -> bool {
        self.shared
            .context_mut()
            .restore_partial_state_seq(self.seq, state)
    }
}

/// A request decoding in one sequence.
struct Running<'v> {
    out: Output,
    picker: Picker<'v>,
    /// Every token the sequence holds: the prompt, then the reply's.
    history: Vec<Token>,
    prompt_tokens: usize,
    /// The token sampled last and already sent, which the next step decodes at `history.len()`.
    id: Token,
    /// The cells it was admitted with: its prompt and its whole possible reply.
    reserved: usize,
    events: mpsc::Sender<Event>,
    stopping: Arc<AtomicBool>,
}

/// One sequence of the shared cache, its prompt cache, and the request decoding in it.
struct Slot<'v> {
    seq: i32,
    cache: PromptCache,
    running: Option<Running<'v>>,
    /// When its last request finished, so the cache idle longest is the first emptied for room.
    last_used: u64,
}

/// One running request's part of a step's batch.
struct Row {
    slot: usize,
    /// Where its tokens start in the batch.
    offset: i32,
    draft: Vec<Token>,
    /// How many of `draft`'s tokens the head drafted, which alone can be accepted.
    real: usize,
    /// Whether the head drafted anything, which owes an accept.
    drafted: bool,
}

/// The room a request's client buffer needs before a step decodes for it: each token a step can
/// accept may send two events (reasoning and content), and a reply's end a few more. A request
/// whose client reads slower waits, rather than holding the decode thread every request shares.
fn event_headroom(n_draft: usize) -> usize {
    2 * (n_draft + 1) + 4
}

fn run<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    (caches, max_running): (Vec<PromptCache>, usize),
    jobs: &std_mpsc::Receiver<Job>,
    eog: &[Token],
) {
    let mut slots: Vec<Slot<'m>> = caches
        .into_iter()
        .enumerate()
        .map(|(seq, cache)| Slot {
            seq: i32::try_from(seq).unwrap_or(i32::MAX),
            cache,
            running: None,
            last_used: 0,
        })
        .collect();
    let mut host = HostCache::new(slots.first().map_or("", |slot| slot.cache.model.as_str()));
    let mut waiting: VecDeque<Job> = VecDeque::new();
    let mut open = true;
    let mut tick: u64 = 0;
    loop {
        let idle = slots.iter().all(|slot| slot.running.is_none());
        if idle && waiting.is_empty() {
            if !open {
                return;
            }
            match jobs.recv() {
                Ok(job) => waiting.push_back(job),
                Err(_) => return,
            }
        }
        // a few at a time, so requests arriving faster than they are taken cannot hold off the
        // steps of the running ones
        for _ in 0..slots.len() {
            match jobs.try_recv() {
                Ok(job) => waiting.push_back(job),
                Err(std_mpsc::TryRecvError::Empty) => break,
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    open = false;
                    break;
                }
            }
        }
        admit(
            model,
            shared,
            split,
            (&mut slots, &mut host, max_running),
            &mut waiting,
            eog,
            &mut tick,
        );
        if !step(model, shared, split, (&mut slots, &mut host), &mut tick) {
            std::thread::sleep(PAUSED_POLL);
        }
    }
}

/// Starts as many waiting requests, in order, as there are free sequences and room for, while
/// fewer than `max_running` run.
fn admit<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    (slots, host, max_running): (&mut [Slot<'m>], &mut HostCache, usize),
    waiting: &mut VecDeque<Job>,
    eog: &[Token],
    tick: &mut u64,
) {
    let n_ctx = usize::try_from(shared.context().n_ctx()).unwrap_or(usize::MAX);
    while let Some(job) = waiting.front() {
        // A client that has gone, as one that stopped reading before a later choice's turn,
        // is not decoded for.
        if job.events.is_closed() {
            waiting.pop_front();
            continue;
        }
        if slots.iter().filter(|slot| slot.running.is_some()).count() >= max_running {
            return;
        }
        let tokens = model
            .vocab()
            .tokenize(&job.prompt, false, true)
            .unwrap_or_default();
        let Some(free) = choose_slot(slots, &tokens, job) else {
            return;
        };
        let prompt_len = tokens.len();
        let reply = job
            .limit
            .unwrap_or(n_ctx)
            .min(n_ctx.saturating_sub(prompt_len));
        let reserve = prompt_len + reply + shared.n_draft() + 1;
        // The running requests' reservations and this one's must fit together, so none runs out
        // of room mid-reply. With nothing running, the request is served as it would be alone.
        let running = slots.iter().any(|slot| slot.running.is_some());
        let reserved: usize = slots
            .iter()
            .filter_map(|slot| slot.running.as_ref())
            .map(|running| running.reserved)
            .sum();
        if running && reserved + reserve > n_ctx {
            return;
        }
        if running_cells(slots) + prompt_len + shared.n_draft() + 1 > n_ctx && running {
            return;
        }
        let Some(job) = waiting.pop_front() else {
            return;
        };
        // Only the running requests' prompts and the one this request continues stay in the
        // shared cache.
        for (index, slot) in slots.iter_mut().enumerate() {
            if index != free && slot.running.is_none() {
                spill(shared, slot, host, *tick);
            }
        }
        prepare(
            shared,
            &mut slots[free],
            host,
            (&tokens, &job.namespace),
            *tick,
        );
        let argmax = split && greedy_alone(slots);
        start(
            model,
            shared,
            argmax,
            &mut slots[free],
            job,
            eog,
            reserve,
            tick,
        );
    }
}

/// Readies `slot`'s sequence for a request reading `tokens` from the caller in `namespace`. A
/// prompt saved to host memory that the request continues further than the sequence's own
/// replaces it, which moves to host memory; the saved prompt stays saved unless the request
/// continues all of it, so the conversation that left it can still continue its end. Otherwise
/// the sequence's prompt stays for the request to continue, saved to host memory first when the
/// request keeps only part of it, for the same reason; one the request shares nothing with
/// moves out whole.
fn prepare<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    host: &mut HostCache,
    (tokens, namespace): (&[Token], &runtime_request_context::CacheNamespace),
    tick: u64,
) {
    let held = slot.cache.tokens.len();
    let kept = slot.cache.shares(tokens, namespace);
    if let Some(saved) = host.take_best(tokens, namespace, kept) {
        spill(shared, slot, host, tick);
        let continued = shared_prefix(&saved.prompt.tokens, tokens);
        if continued >= saved.prompt.len() {
            restore(shared, slot, &saved.state, saved.prompt);
        } else {
            restore(shared, slot, &saved.state, saved.prompt.up_to(continued));
            host.put_back(saved);
        }
    } else if kept == 0 {
        spill(shared, slot, host, tick);
    } else if kept < held {
        save_copy(shared, slot, host, tick);
    }
}

/// The free sequence for a request reading `tokens`: the one whose prompt it continues
/// furthest, then an empty one, then the one idle longest, then the lowest. `None` when every
/// sequence runs a request.
fn choose_slot(slots: &[Slot<'_>], tokens: &[Token], job: &Job) -> Option<usize> {
    slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.running.is_none())
        .max_by_key(|&(index, slot)| {
            // An empty sequence comes before the one idle longest, however recently it was
            // used: one whose request failed, or whose client left while its reply waited for
            // room, is empty with a recent `last_used`, and taking it saves nothing.
            (
                slot.cache.shares(tokens, &job.namespace),
                slot.cache.tokens.is_empty(),
                std::cmp::Reverse(slot.last_used),
                std::cmp::Reverse(index),
            )
        })
        .map(|(index, _)| index)
}

/// The cells the running requests hold.
fn running_cells(slots: &[Slot<'_>]) -> usize {
    slots
        .iter()
        .filter_map(|slot| slot.running.as_ref())
        .map(|running| running.history.len())
        .sum()
}

/// Whether `need` more cells fit beside what the sequences hold now, saving idle prompts to
/// host memory, the one unused longest first, until they do. When `need` would not fit with
/// every idle prompt saved, none is moved.
fn make_room<'m, S: Shared<'m>>(
    shared: &mut S,
    (slots, host): (&mut [Slot<'m>], &mut HostCache),
    need: usize,
    n_ctx: usize,
    tick: u64,
) -> bool {
    let running = running_cells(slots);
    if running + need > n_ctx {
        return false;
    }
    let mut idle: usize = slots
        .iter()
        .filter(|slot| slot.running.is_none())
        .map(|slot| slot.cache.tokens.len())
        .sum();
    while running + idle + need > n_ctx {
        let Some(victim) = slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.running.is_none() && !slot.cache.tokens.is_empty())
            .min_by_key(|(_, slot)| slot.last_used)
            .map(|(index, _)| index)
        else {
            return false;
        };
        idle -= slots[victim].cache.tokens.len();
        spill(shared, &mut slots[victim], host, tick);
    }
    true
}

/// Moves an idle sequence's prompt out of the shared cache into host memory, leaving the
/// sequence empty. A prompt that would cost its next request more to restore than to read again,
/// that does not fit the host memory the cache may use, or whose state does not save, is
/// forgotten.
fn spill<'m, S: Shared<'m>>(shared: &mut S, slot: &mut Slot<'m>, host: &mut HostCache, tick: u64) {
    let token_s = slot.cache.cost.token_s;
    let Some(prompt) = slot.cache.take() else {
        return;
    };
    let bytes = shared.seq_state_size(slot.seq) + prompt.bytes();
    let state = host
        .admits(bytes, prompt.len(), token_s)
        .then(|| save(shared, slot.seq))
        .flatten();
    InSeq {
        shared: &mut *shared,
        seq: slot.seq,
    }
    .clear();
    if let Some((state, took)) = state {
        host.keep(prompt, state, tick, took);
    } else {
        tracing::debug!(
            "Model '{}' forgot an idle prompt of {} tokens rather than save its {bytes} bytes to host memory.",
            host.model,
            prompt.len()
        );
    }
}

/// Saves an idle sequence's prompt to host memory, leaving it in the shared cache too, for a
/// request that continues only part of it; as [`spill`] would, or not at all.
fn save_copy<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    host: &mut HostCache,
    tick: u64,
) {
    let Some(prompt) = slot.cache.copy() else {
        return;
    };
    let bytes = shared.seq_state_size(slot.seq) + prompt.bytes();
    if host.admits(bytes, prompt.len(), slot.cache.cost.token_s)
        && let Some((state, took)) = save(shared, slot.seq)
    {
        host.keep(prompt, state, tick, took);
    }
}

/// `seq`'s state, and how long saving it took.
fn save<'m, S: Shared<'m>>(shared: &mut S, seq: i32) -> Option<(SeqState, Duration)> {
    let started = Instant::now();
    shared.save_seq(seq).map(|state| (state, started.elapsed()))
}

/// Restores `state` into `slot`'s empty sequence, with what `prompt` says about its cells. One
/// that does not restore is forgotten, and the sequence stays empty.
fn restore<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    state: &SeqState,
    prompt: CachedPrompt,
) {
    let started = Instant::now();
    if shared.restore_seq(slot.seq, state) {
        tracing::debug!(
            "Model '{}' restored a prompt of {} tokens ({} bytes) from host memory in {:.1} ms.",
            slot.cache.model,
            prompt.len(),
            state.bytes(),
            started.elapsed().as_secs_f64() * 1000.0
        );
        slot.cache.put(prompt);
    } else {
        InSeq {
            shared: &mut *shared,
            seq: slot.seq,
        }
        .clear();
        tracing::debug!(
            "Model '{}' could not restore a prompt of {} tokens from host memory, and reads it again.",
            slot.cache.model,
            prompt.len()
        );
    }
}

/// A prompt saved out of the shared cache.
struct Saved {
    prompt: CachedPrompt,
    state: SeqState,
    /// When it was saved, so the one unused longest leaves first.
    last_used: u64,
}

impl Saved {
    fn bytes(&self) -> usize {
        self.state.bytes() + self.prompt.bytes()
    }
}

/// Idle prompts saved to host memory, within [`HOST_CACHE_BYTES`] and half the memory the
/// process can still use, the one saved longest ago leaving first.
struct HostCache {
    /// The model's name, for the log lines.
    model: String,
    saved: Vec<Saved>,
    bytes: usize,
    /// Seconds a saved byte takes to save, smoothed over saves; restoring it takes about as
    /// long. On one machine a save copies memory; split across machines it reads every cell
    /// back from each of them.
    byte_s: Option<f64>,
}

impl HostCache {
    fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            saved: Vec::new(),
            bytes: 0,
            byte_s: None,
        }
    }

    /// Whether a prompt of `tokens` tokens whose state takes `bytes` is worth saving, and room
    /// is made for it: restoring it must cost its next request less than reading its tokens
    /// again at `per_token` seconds each (until both are measured, it is assumed to), and it must
    /// fit the host memory the cache may use, beside what is held after the prompts saved
    /// longest ago leave. One that could not fit alone moves nothing out, and nothing is
    /// allocated for either before this says so.
    fn admits(&mut self, bytes: usize, tokens: usize, per_token: Option<f64>) -> bool {
        let budget = host_budget(self.bytes);
        self.admits_within(bytes, tokens, per_token, budget)
    }

    /// [`HostCache::admits`] within `budget` bytes.
    fn admits_within(
        &mut self,
        bytes: usize,
        tokens: usize,
        per_token: Option<f64>,
        budget: usize,
    ) -> bool {
        if let (Some(byte_s), Some(per_token)) = (self.byte_s, per_token)
            && 2.0 * super::count(bytes) * byte_s > super::count(tokens) * per_token
        {
            return false;
        }
        if bytes > budget {
            return false;
        }
        while self.bytes + bytes > budget {
            let Some(oldest) = self
                .saved
                .iter()
                .enumerate()
                .min_by_key(|(_, saved)| saved.last_used)
                .map(|(index, _)| index)
            else {
                return false;
            };
            let dropped = self.saved.swap_remove(oldest);
            self.bytes -= dropped.bytes();
        }
        true
    }

    /// Keeps a prompt [`HostCache::admits`] made room for.
    fn keep(&mut self, prompt: CachedPrompt, state: SeqState, tick: u64, took: Duration) {
        let saved = Saved {
            prompt,
            state,
            last_used: tick,
        };
        let bytes = saved.bytes();
        if bytes > 0 {
            self.byte_s = Some(super::smoothed(
                self.byte_s,
                took.as_secs_f64() / super::count(bytes),
            ));
        }
        tracing::debug!(
            "Model '{}' saved an idle prompt of {} tokens ({bytes} bytes) to host memory in {:.1} ms; it holds {} prompts in {} bytes.",
            self.model,
            saved.prompt.len(),
            took.as_secs_f64() * 1000.0,
            self.saved.len() + 1,
            self.bytes + bytes
        );
        self.bytes += bytes;
        self.saved.push(saved);
    }

    /// Takes the saved prompt a request reading `tokens` from the caller in `namespace`
    /// continues furthest, if that is further than `than` tokens.
    fn take_best(
        &mut self,
        tokens: &[Token],
        namespace: &runtime_request_context::CacheNamespace,
        than: usize,
    ) -> Option<Saved> {
        let (index, _) = self
            .saved
            .iter()
            .enumerate()
            .filter(|(_, saved)| saved.prompt.namespace.as_ref() == Some(namespace))
            .map(|(index, saved)| (index, shared_prefix(&saved.prompt.tokens, tokens)))
            .filter(|&(_, shared)| shared > than)
            .max_by_key(|&(index, shared)| (shared, self.saved[index].last_used))?;
        let saved = self.saved.swap_remove(index);
        self.bytes -= saved.bytes();
        Some(saved)
    }

    /// Returns a prompt [`HostCache::take_best`] took and a request continued only part of, so
    /// its conversation can still continue all of it, within the memory it held before.
    fn put_back(&mut self, saved: Saved) {
        let bytes = saved.bytes();
        if bytes <= host_budget(self.bytes) {
            self.bytes += bytes;
            self.saved.push(saved);
        }
    }
}

/// The host memory the prompt cache may use with `held` bytes already in it: within
/// [`HOST_CACHE_BYTES`] and half of what the process can still use besides.
fn host_budget(held: usize) -> usize {
    let cap = usize::try_from(HOST_CACHE_BYTES).unwrap_or(usize::MAX);
    match runtime_resources::get_available_memory() {
        Some(available) => {
            let available = usize::try_from(available).unwrap_or(usize::MAX);
            cap.min(usize::midpoint(held, available))
        }
        None => cap,
    }
}

/// Whether every running request picks greedily alone, so that a split model may take its
/// picks from the argmax output ([`Context::set_argmax_output`]).
fn greedy_alone(slots: &[Slot<'_>]) -> bool {
    slots
        .iter()
        .filter_map(|slot| slot.running.as_ref())
        .all(|running| running.picker.picks_greedily())
}

/// Reads `job`'s prompt into `slot`'s sequence and samples its first token, which it sends;
/// the request then decodes with the others from the next step, unless that token ended it.
/// With `argmax`, every running request picks greedily alone on a split model, and the prompt
/// is read with the argmax output on when this request does too.
#[expect(
    clippy::too_many_arguments,
    reason = "the request's slot, its job and the scheduler's state it reads and updates"
)]
fn start<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    argmax: bool,
    slot: &mut Slot<'m>,
    job: Job,
    eog: &[Token],
    reserved: usize,
    tick: &mut u64,
) {
    let namespace = job.namespace.clone();
    let events = job.events.clone();
    let stopping = Arc::clone(&job.stopping);
    let (prompt, sampling, mut out) = Output::new(job);
    let n_ctx = shared.context().n_ctx();
    let picker = Picker::new(
        model.vocab(),
        &sampling,
        &out.ruled_out(eog),
        n_ctx,
        out.pending(),
    );
    let mut picker = match picker {
        Ok(picker) => picker,
        Err(message) => {
            send_unless_stopping(&events, &stopping, Event::Failed(message));
            return;
        }
    };
    out.begin(picker.goes_on());
    shared
        .context_mut()
        .set_argmax_output(argmax && picker.picks_greedily());
    let mut target = InSeq {
        shared: &mut *shared,
        seq: slot.seq,
    };
    let read = read_prompt(
        model,
        &mut target,
        &mut slot.cache,
        (&prompt, &namespace),
        &out.stopping,
    );
    let (history, batch) = match read {
        Ok((history, batch, cached)) => {
            out.cached = cached;
            (history, batch)
        }
        Err(message) => {
            target.clear();
            slot.cache.forget();
            send_unless_stopping(&events, &stopping, Event::Failed(message));
            return;
        }
    };
    if let Err(message) = shared.begin(slot.seq, &history) {
        fail(shared, slot, &events, &stopping, message);
        return;
    }
    let id = picker.sample(shared.context(), batch.n_tokens() - 1, out.in_content());
    let running = Running {
        out,
        picker,
        prompt_tokens: history.len(),
        history,
        id,
        reserved,
        events,
        stopping,
    };
    sent(model, shared, slot, running, tick);
}

/// Sends a running request's sampled token, then keeps it running, or finishes it when the
/// token ended its reply or the context has no room for the next.
fn sent<'m, S: Shared<'m>>(
    model: &Model,
    shared: &mut S,
    slot: &mut Slot<'m>,
    mut running: Running<'m>,
    tick: &mut u64,
) {
    let n_ctx = usize::try_from(shared.context().n_ctx()).unwrap_or(usize::MAX);
    if let Some(finish) = running.out.token(model, running.id) {
        end(shared, slot, running, finish, tick);
    } else if running.history.len() + 1 >= n_ctx {
        end(shared, slot, running, FinishReason::Length, tick);
    } else {
        slot.running = Some(running);
    }
}

/// Finishes a request: its sequence keeps its tokens for the next request to reuse, and its
/// client gets the outcome.
fn end<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    running: Running<'m>,
    finish: FinishReason,
    tick: &mut u64,
) {
    let Running {
        out,
        history,
        prompt_tokens,
        events,
        stopping,
        ..
    } = running;
    slot.cache.keep(
        &mut InSeq {
            shared,
            seq: slot.seq,
        },
        history,
    );
    *tick += 1;
    slot.last_used = *tick;
    let event = match out.finish(finish, prompt_tokens) {
        Ok(outcome) => Event::Done(outcome),
        Err(message) => Event::Failed(message),
    };
    send_unless_stopping(&events, &stopping, event);
}

/// Fails a request, emptying its sequence, so nothing it left is reused.
fn fail<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    events: &mpsc::Sender<Event>,
    stopping: &AtomicBool,
    message: String,
) {
    InSeq {
        shared,
        seq: slot.seq,
    }
    .clear();
    slot.cache.forget();
    send_unless_stopping(events, stopping, Event::Failed(message));
}

/// One step: drafts for every running request whose client has room, verifies all their
/// drafts in one batch, and sends what each accepted. Returns false when no request could step.
fn step<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    (slots, host): (&mut [Slot<'m>], &mut HostCache),
    tick: &mut u64,
) -> bool {
    let n_ctx = usize::try_from(shared.context().n_ctx()).unwrap_or(usize::MAX);
    let headroom = event_headroom(shared.n_draft());
    // Requests whose clients left, or whose decode thread is stopping, end before the step.
    for slot in slots.iter_mut() {
        let Some(running) = &slot.running else {
            continue;
        };
        if running.stopping.load(Ordering::Relaxed) {
            if let Some(running) = slot.running.take() {
                fail(
                    shared,
                    slot,
                    &running.events,
                    &running.stopping,
                    DECODE_STOPPED.to_string(),
                );
            }
        } else if running.events.is_closed() {
            slot.running = None;
            InSeq {
                shared: &mut *shared,
                seq: slot.seq,
            }
            .clear();
            slot.cache.forget();
        }
    }
    let stepping: Vec<usize> = slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| {
            slot.running
                .as_ref()
                .is_some_and(|running| running.events.capacity() >= headroom)
        })
        .map(|(index, _)| index)
        .collect();
    if stepping.is_empty() {
        return slots.iter().all(|slot| slot.running.is_none());
    }
    // A request's step writes its last token and a draft cut to the context, as below, which
    // idle prompts make room for by moving to host memory. The running requests' reservations
    // fit together, so it fits.
    let need: usize = stepping
        .iter()
        .filter_map(|&index| slots[index].running.as_ref())
        .map(|running| {
            let past = running.history.len();
            1 + shared.n_draft().min(n_ctx.saturating_sub(past + 1))
        })
        .sum();
    make_room(shared, (slots, host), need, n_ctx, *tick);
    let drafts = {
        let requests: Vec<DraftRequest<'_>> = stepping
            .iter()
            .filter_map(|&index| {
                let slot = &slots[index];
                let running = slot.running.as_ref()?;
                Some(DraftRequest {
                    seq: slot.seq,
                    n_past: i32::try_from(running.history.len()).unwrap_or(i32::MAX),
                    last: running.id,
                    history: &running.history,
                })
            })
            .collect();
        shared.draft(&requests)
    };
    let drafts = match drafts {
        Ok(drafts) => drafts,
        Err(message) => {
            fail_all(shared, slots, &stepping, &message);
            return true;
        }
    };
    let mut batch = Batch::new(PROMPT_BATCH.max(stepping.len() * (shared.n_draft() + 1)));
    let mut rows = Vec::with_capacity(stepping.len());
    for (&index, mut draft) in stepping.iter().zip(drafts) {
        let slot = &slots[index];
        let Some(running) = &slot.running else {
            continue;
        };
        let past = running.history.len();
        let n_past = i32::try_from(past).unwrap_or(i32::MAX);
        let drafted = !draft.is_empty();
        // A model split across nodes verifies every drafted token even when the request has
        // fewer left, so each request keeps one shape in the batch: a shorter batch at a
        // request's end rebuilds the verify graph twice (to the short shape and back), which
        // over RPC re-serializes every rank's graph. Tokens past the limit are verified but
        // never emitted.
        draft.truncate(if split {
            n_ctx - past - 1
        } else {
            running.out.remaining().min(n_ctx - past - 1)
        });
        let real = draft.len();
        if split && real > 0 {
            let want = shared.n_draft().min(n_ctx - past - 1);
            if real < want {
                let last = draft[real - 1];
                draft.resize(want, last);
            }
        }
        // drafting wrote its positions into the head's cache, and the verify batch writes
        // them again
        if !shared.truncate_draft(slot.seq, n_past) {
            fail_all(
                shared,
                slots,
                &stepping,
                &format!("llama.cpp could not roll its cache back to position {n_past}"),
            );
            return true;
        }
        let offset = batch.n_tokens();
        let added = batch
            .add_seq(running.id, n_past, slot.seq, true)
            .and_then(|()| {
                draft
                    .iter()
                    .zip(1..)
                    .try_for_each(|(&token, i)| batch.add_seq(token, n_past + i, slot.seq, true))
            });
        if let Err(e) = added {
            fail_all(shared, slots, &stepping, &e.to_string());
            return true;
        }
        rows.push(Row {
            slot: index,
            offset,
            draft,
            real,
            drafted,
        });
    }
    // The decode outputs a row for every request in it, and the argmax output (which has no
    // logits) serves only requests that pick greedily alone, so it is on when every running
    // request does. It changes, and rebuilds the graph, only when that set changes.
    let argmax = split && greedy_alone(slots);
    let context = shared.context_mut();
    context.set_argmax_output(argmax);
    let decoded = context
        .decode(&batch)
        .map_err(|e| e.to_string())
        .and_then(|()| shared.decoded(&batch));
    if let Err(message) = decoded {
        fail_all(shared, slots, &stepping, &message);
        return true;
    }
    for row in rows {
        let slot = &mut slots[row.slot];
        let Some(mut running) = slot.running.take() else {
            continue;
        };
        running.history.push(running.id);
        let mut accepted: usize = 0;
        let next = loop {
            let index = row.offset + i32::try_from(accepted).unwrap_or(i32::MAX);
            let in_content = running.out.in_content();
            let token = running.picker.sample(shared.context(), index, in_content);
            if accepted < row.real && token == row.draft[accepted] {
                accepted += 1;
                // an accepted draft was decoded in the verify batch, so the cache holds it
                // whether or not it ends the reply, and the next request may reuse it
                running.history.push(token);
                if let Some(finish) = running.out.token(model, token) {
                    break Err(finish);
                }
            } else {
                break Ok(token);
            }
        };
        let next = match next {
            Ok(next) => next,
            Err(finish) => {
                end(shared, slot, running, finish, tick);
                continue;
            }
        };
        if row.drafted
            && let Err(message) =
                shared.accept(slot.seq, u16::try_from(accepted).unwrap_or(u16::MAX))
        {
            fail(shared, slot, &running.events, &running.stopping, message);
            continue;
        }
        // the rejected drafts leave both caches
        let kept = i32::try_from(running.history.len()).unwrap_or(i32::MAX);
        if !shared.truncate(slot.seq, kept) {
            let message = format!("llama.cpp could not roll its cache back to position {kept}");
            fail(shared, slot, &running.events, &running.stopping, message);
            continue;
        }
        running.id = next;
        sent(model, shared, slot, running, tick);
    }
    true
}

/// Fails every request in `stepping`, after a step they shared could not decode.
fn fail_all<'m, S: Shared<'m>>(
    shared: &mut S,
    slots: &mut [Slot<'m>],
    stepping: &[usize],
    message: &str,
) {
    for &index in stepping {
        let slot = &mut slots[index];
        if let Some(running) = slot.running.take() {
            fail(
                shared,
                slot,
                &running.events,
                &running.stopping,
                message.to_string(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedPrompt, HostCache, Saved, SeqState};

    /// A saved prompt of `bytes` bytes, saved at `tick`.
    fn saved(bytes: usize, tick: u64) -> Saved {
        Saved {
            prompt: CachedPrompt {
                tokens: vec![llama_cpp::Token(1)],
                namespace: None,
                checkpoints: Vec::new(),
                head_stash: None,
            },
            state: SeqState {
                target: vec![0; bytes],
                draft: None,
                head: None,
            },
            last_used: tick,
        }
    }

    fn holding(entries: &[(usize, u64)]) -> HostCache {
        let mut host = HostCache::new("m");
        for &(bytes, tick) in entries {
            host.bytes += bytes;
            host.saved.push(saved(bytes, tick));
        }
        host
    }

    fn ticks(host: &HostCache) -> Vec<u64> {
        let mut ticks: Vec<u64> = host.saved.iter().map(|saved| saved.last_used).collect();
        ticks.sort_unstable();
        ticks
    }

    /// A prompt larger than the whole budget is refused before anything is saved, and the
    /// prompts already held stay.
    #[test]
    fn a_prompt_over_the_budget_moves_nothing_out() {
        let mut host = holding(&[(300, 1), (300, 2)]);
        assert!(!host.admits_within(1_100, 10, None, 1_000));
        assert_eq!(ticks(&host), [1, 2]);
        assert_eq!(host.bytes, 600);
    }

    /// A prompt that fits the budget moves out the prompts saved longest ago until it fits
    /// beside the rest, and no more.
    #[test]
    fn a_prompt_that_fits_moves_out_only_the_oldest_it_needs() {
        let mut host = holding(&[(300, 3), (300, 1), (300, 2)]);
        assert!(host.admits_within(500, 10, None, 1_000));
        assert_eq!(ticks(&host), [3]);
        assert_eq!(host.bytes, 300);
    }

    /// A prompt that would take longer to save and restore than its tokens take to read again
    /// is not saved, and moves nothing out.
    #[test]
    fn a_prompt_cheaper_to_read_again_is_not_saved() {
        let mut host = holding(&[(300, 1)]);
        host.byte_s = Some(1e-6);
        // 2 x 1000 bytes x 1 us = 2 ms to save and restore, against 10 tokens x 0.1 ms = 1 ms
        assert!(!host.admits_within(1_000, 10, Some(1e-4), 10_000));
        // against 100 tokens x 0.1 ms = 10 ms, it pays
        assert!(host.admits_within(1_000, 100, Some(1e-4), 10_000));
        assert_eq!(ticks(&host), [1]);
    }
}
