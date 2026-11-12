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
//! A request is admitted when its prompt fits beside the running requests' cells with room left
//! for [`ADMISSION_HEADROOM`] more tokens, or for as many as it may still write when that is
//! fewer, whatever it may go on to write; one that does not fit waits, and the requests behind
//! it wait in order. A model that is otherwise idle admits any request whose prompt fits the
//! context, as one request at a time does. When the running requests' next step no longer fits
//! together, even with every idle prompt in host memory, a request is preempted: its tokens
//! leave the shared cache for host memory as an idle prompt's do, and it resumes, before any
//! request that waits to start, once its tokens fit again with the same room, as vLLM preempts a
//! request. The one preempted is the request admitted last, unless a request whose client is not
//! reading holds cells it cannot use. Such a request also leaves for a preempted request whose
//! client reads again, which would otherwise wait on a client that is not reading. One request
//! alone always fits, as its draft is cut to the context.
//! A model whose cache keeps a stream of the whole context per sequence, as DeepSeek-V4's does,
//! gives each request that room in its own sequence, so its requests neither wait for nor preempt
//! each other.
//!
//! llama.cpp decodes such sequences in one pass of the model only when their ids are
//! consecutive, so a request takes the free sequence that keeps the running ones in the fewest
//! runs of consecutive ids; a request placed apart from them would cost every step it shares
//! with them a pass of its own. On a cache that every sequence shares, ids cost nothing, and a
//! request takes the sequence whose cache it continues.
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
    Picker, PromptCache, Target, read_prompt, read_tokens, send_unless_stopping, shared_prefix,
};

/// How long the decode thread waits when every running request waits on its client.
const PAUSED_POLL: Duration = Duration::from_millis(1);

/// The room, in tokens beyond its own, that a request starts or resumes with beside the running
/// requests on a cache every sequence shares, so it does not start only to be preempted a few
/// steps later, as vLLM keeps a watermark of the cache free when it admits a request.
const ADMISSION_HEADROOM: usize = 256;

/// The room a request that may still write `remaining` tokens starts or resumes with: the
/// headroom, or `remaining` when that is less, and never less than one step of `n_draft` drafts.
fn admission_room(remaining: usize, n_draft: usize) -> usize {
    ADMISSION_HEADROOM.min(remaining).max(n_draft + 1)
}

/// How a model's cache holds the sequences of the requests it serves at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CacheLayout {
    /// One cache of `n_ctx` cells that every sequence shares (llama.cpp's unified KV cache):
    /// the running requests' prompts and replies must fit it together.
    Unified,
    /// A stream of `n_ctx` cells per sequence, as DeepSeek-V4's cache keeps even when asked
    /// for a unified one: each request has the whole context, and llama.cpp decodes sequences
    /// together only when their ids are consecutive.
    PerSequence,
}

/// Serves `jobs` up to `running` at a time, each request in its own sequence of a cache laid out
/// as `layout`, one per entry of `caches`, until the queue closes and every request has
/// finished. Idle prompts move to host memory and back as requests need them.
pub(super) fn serve<'m>(
    model: &'m Model,
    decoder: &mut Decoder<'m>,
    split: bool,
    (caches, running, layout): (Vec<PromptCache>, usize, CacheLayout),
    jobs: &std_mpsc::Receiver<Job>,
    eog: &[Token],
) {
    let serving = (caches, running, layout);
    match decoder {
        Decoder::Plain(context) => run(model, context, split, serving, jobs, eog),
        Decoder::Mtp(spec) => run(model, spec.as_mut(), split, serving, jobs, eog),
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
    /// The caller's cache namespace, which the tokens of a preempted request are saved under.
    namespace: runtime_request_context::CacheNamespace,
    /// When it was admitted, so the request admitted last is the one preempted for room.
    admitted: u64,
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
    (caches, max_running, layout): (Vec<PromptCache>, usize, CacheLayout),
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
    // requests preempted for room, in the order they were admitted
    let mut preempted: VecDeque<Running<'m>> = VecDeque::new();
    let mut open = true;
    let mut tick: u64 = 0;
    loop {
        let idle = slots.iter().all(|slot| slot.running.is_none());
        if idle && waiting.is_empty() && preempted.is_empty() {
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
            (&mut slots, &mut host, max_running, layout),
            (&mut preempted, &mut waiting),
            eog,
            &mut tick,
        );
        if !step(
            model,
            shared,
            split,
            (&mut slots, &mut host, layout),
            &mut preempted,
            &mut tick,
        ) {
            std::thread::sleep(PAUSED_POLL);
        }
    }
}

/// Resumes the preempted requests, then starts the waiting ones, each in order, as there are
/// free sequences and room for, while fewer than `max_running` run. A waiting request does not
/// start while a preempted one whose client reads waits for room.
fn admit<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    (slots, host, max_running, layout): (&mut [Slot<'m>], &mut HostCache, usize, CacheLayout),
    (preempted, waiting): (&mut VecDeque<Running<'m>>, &mut VecDeque<Job>),
    eog: &[Token],
    tick: &mut u64,
) {
    let n_ctx = usize::try_from(shared.context().n_ctx()).unwrap_or(usize::MAX);
    let headroom = event_headroom(shared.n_draft());
    let mut next = 0;
    while let Some(request) = preempted.get(next) {
        if request.stopping.load(Ordering::Relaxed) || request.events.is_closed() {
            if let Some(request) = preempted.remove(next) {
                send_unless_stopping(
                    &request.events,
                    &request.stopping,
                    Event::Failed(DECODE_STOPPED.to_string()),
                );
            }
            continue;
        }
        // One whose client is not reading would hold the room it took without stepping; it
        // waits, without holding up the requests behind it, until its client reads.
        if request.events.capacity() < headroom {
            next += 1;
            continue;
        }
        let need = (
            request.history.len(),
            admission_room(request.out.remaining(), shared.n_draft()),
        );
        let mut displaced = Vec::new();
        let ready = displace_stalled(
            shared,
            (slots, host, max_running, layout),
            (need, n_ctx, headroom),
            &mut displaced,
            *tick,
        );
        if !ready {
            requeue(preempted, displaced, &mut next);
            return;
        }
        let Some(request) = preempted.remove(next) else {
            requeue(preempted, displaced, &mut next);
            return;
        };
        requeue(preempted, displaced, &mut next);
        let Some(free) = choose_slot(slots, &request.history, &request.namespace, layout) else {
            requeue(preempted, vec![request], &mut next);
            return;
        };
        make_way(shared, (slots, host), free, *tick);
        prepare(
            shared,
            &mut slots[free],
            host,
            (&request.history, &request.namespace),
            *tick,
        );
        let argmax = split && greedy_alone(slots);
        resume(shared, argmax, &mut slots[free], request);
    }
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
        let room = admission_room(job.limit.unwrap_or(usize::MAX), shared.n_draft());
        if !fits(slots, tokens.len(), room, n_ctx, layout) {
            return;
        }
        let Some(free) = choose_slot(slots, &tokens, &job.namespace, layout) else {
            return;
        };
        let Some(job) = waiting.pop_front() else {
            return;
        };
        make_way(shared, (slots, host), free, *tick);
        prepare(
            shared,
            &mut slots[free],
            host,
            (&tokens, &job.namespace),
            *tick,
        );
        let argmax = split && greedy_alone(slots);
        start(model, shared, argmax, &mut slots[free], job, eog, tick);
    }
}

/// Whether a request holding `tokens` cells fits beside the running ones with `room` cells more
/// (see [`admission_room`]). On a cache every sequence shares, the running requests' cells and
/// its own must fit the context together; a stream per sequence holds each request's alone. With
/// nothing running, a request is served as it would be alone.
fn fits(slots: &[Slot<'_>], tokens: usize, room: usize, n_ctx: usize, layout: CacheLayout) -> bool {
    let running = slots.iter().any(|slot| slot.running.is_some());
    !running || layout == CacheLayout::PerSequence || running_cells(slots) + tokens + room <= n_ctx
}

/// Whether a preempted request holding `tokens` cells can resume with `room` more, preempting
/// for it, into `displaced`, the running requests whose clients are not reading, the one admitted
/// last first: such a request holds cells it does not step with, and would otherwise hold off a
/// request whose client reads, even as the only one running. None is preempted unless all of
/// them leaving would let the request resume: under `max_running`, in a free sequence, with room.
fn displace_stalled<'m, S: Shared<'m>>(
    shared: &mut S,
    (slots, host, max_running, layout): (&mut [Slot<'m>], &mut HostCache, usize, CacheLayout),
    ((tokens, room), n_ctx, headroom): ((usize, usize), usize, usize),
    displaced: &mut Vec<Running<'m>>,
    tick: u64,
) -> bool {
    let can_resume = |slots: &[Slot<'m>]| {
        let running = slots.iter().filter(|slot| slot.running.is_some()).count();
        running < max_running.min(slots.len()) && fits(slots, tokens, room, n_ctx, layout)
    };
    if can_resume(slots) {
        return true;
    }
    // (index, admitted, cells) of each running request whose client is not reading
    let mut stalled: Vec<(usize, u64, usize)> = slots
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| {
            let running = slot.running.as_ref()?;
            (running.events.capacity() < headroom).then_some((
                index,
                running.admitted,
                running.history.len(),
            ))
        })
        .collect();
    let running = slots.iter().filter(|slot| slot.running.is_some()).count();
    let left = running - stalled.len();
    let cells_left =
        running_cells(slots) - stalled.iter().map(|&(_, _, cells)| cells).sum::<usize>();
    let room_left =
        left == 0 || layout == CacheLayout::PerSequence || cells_left + tokens + room <= n_ctx;
    if stalled.is_empty() || left >= max_running.min(slots.len()) || !room_left {
        return false;
    }
    stalled.sort_by_key(|&(_, admitted, _)| std::cmp::Reverse(admitted));
    for (index, _, _) in stalled {
        displaced.extend(preempt(shared, &mut slots[index], host, tick));
        if can_resume(slots) {
            return true;
        }
    }
    can_resume(slots)
}

/// Puts preempted requests back among `preempted`, in the order they were admitted, and moves
/// `next` past any that land before it, so it still names the request it named.
fn requeue<'m>(
    preempted: &mut VecDeque<Running<'m>>,
    requests: Vec<Running<'m>>,
    next: &mut usize,
) {
    for request in requests {
        let at = preempted.partition_point(|other| other.admitted < request.admitted);
        if at < *next {
            *next += 1;
        }
        preempted.insert(at, request);
    }
}

/// Moves every idle prompt but `free`'s to host memory, so only the running requests' tokens
/// and those the next request continues stay in the shared cache.
fn make_way<'m, S: Shared<'m>>(
    shared: &mut S,
    (slots, host): (&mut [Slot<'m>], &mut HostCache),
    free: usize,
    tick: u64,
) {
    for (index, slot) in slots.iter_mut().enumerate() {
        if index != free && slot.running.is_none() {
            spill(shared, slot, host, tick);
        }
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

/// The free sequence for a request reading `tokens`, from a cache laid out as `layout` (see
/// [`pick_slot`]). `None` when every sequence runs a request.
fn choose_slot(
    slots: &[Slot<'_>],
    tokens: &[Token],
    namespace: &runtime_request_context::CacheNamespace,
    layout: CacheLayout,
) -> Option<usize> {
    let running: Vec<bool> = slots.iter().map(|slot| slot.running.is_some()).collect();
    pick_slot(&running, layout, |index| {
        let slot = &slots[index];
        (
            slot.cache.shares(tokens, namespace),
            slot.cache.tokens.is_empty(),
            slot.last_used,
        )
    })
}

/// The free sequence a request takes, given which sequences run requests and, for each free one,
/// `idle(index)`: how many of the request's tokens its cache gives, whether it is empty, and
/// when its last request finished. With a stream per sequence, first the one that leaves the
/// running sequences in the fewest passes of the model; then the one whose prompt the request
/// continues furthest, then an empty one, then the one idle longest, then the lowest.
fn pick_slot(
    running: &[bool],
    layout: CacheLayout,
    idle: impl Fn(usize) -> (usize, bool, u64),
) -> Option<usize> {
    (0..running.len())
        .filter(|&index| !running[index])
        .max_by_key(|&index| {
            let (shares, empty, last_used) = idle(index);
            // A pass more costs every step the request shares with the running ones, more than
            // restoring its prompt from host memory costs once.
            let passes = match layout {
                CacheLayout::PerSequence => passes_with(running, index),
                CacheLayout::Unified => 0,
            };
            // An empty sequence comes before the one idle longest, however recently it was
            // used: one whose request failed, or whose client left while its reply waited for
            // room, is empty with a recent `last_used`, and taking it saves nothing.
            (
                std::cmp::Reverse(passes),
                shares,
                empty,
                std::cmp::Reverse(last_used),
                std::cmp::Reverse(index),
            )
        })
}

/// How many passes of the model a step takes over the running sequences and `index`, each with a
/// cache stream of its own: llama.cpp decodes such sequences together only when their ids are
/// consecutive, so each run of consecutive ids is a pass.
fn passes_with(running: &[bool], index: usize) -> usize {
    let busy = |i: usize| i == index || running[i];
    (0..running.len())
        .filter(|&i| busy(i) && (i == 0 || !busy(i - 1)))
        .count()
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
        self.shrink_to(budget - bytes)
    }

    /// Moves out the prompts saved longest ago until what the cache holds fits in `budget`
    /// bytes, and says whether it does.
    fn shrink_to(&mut self, budget: usize) -> bool {
        while self.bytes > budget {
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
    /// its conversation can still continue all of it. What the cache holds stays within the
    /// memory it may use: the sequence's own prompt may have taken the room this one left, so
    /// the prompts saved longest ago leave, this one among them, until everything fits. One that
    /// no longer fits alone is forgotten, and moves nothing out.
    fn put_back(&mut self, saved: Saved) {
        // Its state is in memory already: it counts as held, not as memory still available.
        let budget = host_budget(self.bytes + saved.bytes());
        self.put_back_within(saved, budget);
    }

    /// [`HostCache::put_back`] within `budget` bytes.
    fn put_back_within(&mut self, saved: Saved, budget: usize) {
        let bytes = saved.bytes();
        if bytes > budget {
            return;
        }
        self.bytes += bytes;
        self.saved.push(saved);
        self.shrink_to(budget);
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
fn start<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    argmax: bool,
    slot: &mut Slot<'m>,
    job: Job,
    eog: &[Token],
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
    *tick += 1;
    let running = Running {
        out,
        picker,
        prompt_tokens: history.len(),
        history,
        id,
        namespace,
        admitted: *tick,
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
/// drafts in one batch, and sends what each accepted. A request preempted for the step's room
/// joins `preempted`. Returns false when no request could step.
fn step<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    (slots, host, layout): (&mut [Slot<'m>], &mut HostCache, CacheLayout),
    preempted: &mut VecDeque<Running<'m>>,
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
    let mut stepping: Vec<usize> = slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| {
            slot.running
                .as_ref()
                .is_some_and(|running| running.events.capacity() >= headroom)
        })
        .map(|(index, _)| index)
        .collect();
    // A request's step writes its last token and a draft cut to the context, as below, which
    // idle prompts make room for by moving to host memory. When the running requests' steps do
    // not fit together even so, requests are preempted (see [`victim`]) until they do; one
    // request alone always fits. A stream per sequence holds each request's step in its own.
    if layout == CacheLayout::Unified && !stepping.is_empty() {
        loop {
            let need: usize = stepping
                .iter()
                .filter_map(|&index| slots[index].running.as_ref())
                .map(|running| {
                    let past = running.history.len();
                    1 + shared.n_draft().min(n_ctx.saturating_sub(past + 1))
                })
                .sum();
            if make_room(shared, (slots, host), need, n_ctx, *tick) {
                break;
            }
            let Some(victim) = victim(slots, headroom) else {
                break;
            };
            if let Some(running) = preempt(shared, &mut slots[victim], host, *tick) {
                let at = preempted.partition_point(|other| other.admitted < running.admitted);
                preempted.insert(at, running);
            }
            stepping.retain(|&index| index != victim);
        }
    }
    // With no request stepping, the caller pauses: a request preempted for room may wait on a
    // client that is not reading even when no request runs, and with no request at all the
    // caller blocks for the next one after the pause.
    if stepping.is_empty() {
        return false;
    }
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

/// The running request to preempt for room, when another runs beside it: of the requests whose
/// client is not reading, which hold their cells without stepping, the one admitted last; else
/// the one admitted last of all.
fn victim(slots: &[Slot<'_>], headroom: usize) -> Option<usize> {
    let running: Vec<(usize, bool, u64)> = slots
        .iter()
        .enumerate()
        .filter_map(|(index, slot)| {
            let running = slot.running.as_ref()?;
            Some((
                index,
                running.events.capacity() < headroom,
                running.admitted,
            ))
        })
        .collect();
    if running.len() < 2 {
        return None;
    }
    running
        .into_iter()
        .max_by_key(|&(_, stalled, admitted)| (stalled, admitted))
        .map(|(index, _, _)| index)
}

/// Preempts `slot`'s request for room: its tokens leave the shared cache as an idle prompt's
/// do, saved to host memory when restoring them would cost less than reading them again, and the
/// request waits to resume.
fn preempt<'m, S: Shared<'m>>(
    shared: &mut S,
    slot: &mut Slot<'m>,
    host: &mut HostCache,
    tick: u64,
) -> Option<Running<'m>> {
    let running = slot.running.take()?;
    slot.cache.keep(
        &mut InSeq {
            shared: &mut *shared,
            seq: slot.seq,
        },
        running.history.clone(),
    );
    spill(shared, slot, host, tick);
    // a cache that could not keep the tokens has nothing to save, and the sequence still holds
    // them
    InSeq {
        shared: &mut *shared,
        seq: slot.seq,
    }
    .clear();
    tracing::debug!(
        "Model '{}' preempted a request of {} tokens for room; it resumes when they fit again.",
        slot.cache.model,
        running.history.len()
    );
    Some(running)
}

/// Resumes a preempted request in `slot`'s sequence, which [`prepare`] readied, to decode with
/// the others from the next step. Its next token was sampled and sent before it was preempted,
/// so when its tokens came back from host memory whole, it continues from them as they were;
/// otherwise it reads again what the cache lacks, and the read's logits go unused. With
/// `argmax`, as in [`start`].
fn resume<'m, S: Shared<'m>>(
    shared: &mut S,
    argmax: bool,
    slot: &mut Slot<'m>,
    running: Running<'m>,
) {
    if !slot.cache.resume(&running.history, &running.namespace) {
        shared
            .context_mut()
            .set_argmax_output(argmax && running.picker.picks_greedily());
        let mut target = InSeq {
            shared: &mut *shared,
            seq: slot.seq,
        };
        let read = read_tokens(
            &mut target,
            &mut slot.cache,
            (&running.history, &running.namespace),
            &running.out.stopping,
        );
        if let Err(message) = read {
            target.clear();
            slot.cache.forget();
            send_unless_stopping(&running.events, &running.stopping, Event::Failed(message));
            return;
        }
    }
    if let Err(message) = shared.begin(slot.seq, &running.history) {
        fail(shared, slot, &running.events, &running.stopping, message);
        return;
    }
    tracing::debug!(
        "Model '{}' resumed a preempted request of {} tokens.",
        slot.cache.model,
        running.history.len()
    );
    slot.running = Some(running);
}

#[cfg(test)]
mod tests {
    use super::{CacheLayout, CachedPrompt, HostCache, Saved, SeqState, pick_slot};

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

    /// A prompt a request continued only part of goes back within the budget, though the
    /// sequence's own prompt took the room its taking left: the prompt saved longest ago leaves.
    #[test]
    fn a_partly_continued_prompt_goes_back_within_the_budget() {
        let mut host = holding(&[(400, 1), (400, 3)]);
        // the request takes the prompt saved at 3 ...
        let index = host
            .saved
            .iter()
            .position(|saved| saved.last_used == 3)
            .expect("the prompt saved at 3");
        let taken = host.saved.swap_remove(index);
        host.bytes -= taken.bytes();
        // ... its sequence's own prompt moves out into the room that left ...
        assert!(host.admits_within(500, 10, None, 1_000));
        let own = saved(500, 4);
        host.keep(own.prompt, own.state, 4, std::time::Duration::ZERO);
        assert_eq!(host.bytes, 900);
        // ... and the taken prompt goes back
        host.put_back_within(taken, 1_000);
        assert_eq!(ticks(&host), [3, 4]);
        assert_eq!(host.bytes, 900);
    }

    /// A prompt a request continued only part of that no longer fits the budget alone, which
    /// shrinks as the memory the process can still use does, is forgotten and moves nothing
    /// out, as [`HostCache::admits`] refuses such a prompt.
    #[test]
    fn a_partly_continued_prompt_over_the_budget_moves_nothing_out() {
        let mut host = holding(&[(300, 1), (300, 2)]);
        host.put_back_within(saved(300, 3), 250);
        assert_eq!(ticks(&host), [1, 2]);
        assert_eq!(host.bytes, 600);
    }

    /// What `pick_slot` reads of each free sequence: an empty cache, idle since `last_used`.
    fn empty_since(last_used: &[u64]) -> impl Fn(usize) -> (usize, bool, u64) + '_ {
        move |index| (0, true, last_used[index])
    }

    #[test]
    fn a_request_on_a_cache_per_sequence_joins_the_running_ones_in_one_pass() {
        // 0 runs, and 5 is the sequence idle longest, which a unified cache takes
        let running = [true, false, false, false, false, false, false, false];
        let last_used = [0, 9, 8, 7, 6, 1, 5, 4];
        let per_sequence = pick_slot(&running, CacheLayout::PerSequence, empty_since(&last_used));
        assert_eq!(per_sequence, Some(1));
        let unified = pick_slot(&running, CacheLayout::Unified, empty_since(&last_used));
        assert_eq!(unified, Some(5));
        // 0 and 2 run in two passes: 1 makes them one
        let gap = [true, false, true, false];
        let per_sequence = pick_slot(&gap, CacheLayout::PerSequence, empty_since(&[0, 9, 0, 1]));
        assert_eq!(per_sequence, Some(1));
        // 2 and 3 run: 1 and 4 each keep them in one pass, and 1 is the lower
        let high = [false, false, true, true, false];
        let per_sequence = pick_slot(
            &high,
            CacheLayout::PerSequence,
            empty_since(&[1, 9, 0, 0, 9]),
        );
        assert_eq!(per_sequence, Some(1));
    }

    #[test]
    fn a_request_whose_prompt_sits_apart_from_the_running_ones_moves_only_on_a_cache_per_sequence()
    {
        // 0 runs, and the request continues the prompt sequence 5 holds
        let running = [true, false, false, false, false, false, false, false];
        let idle = |index| {
            if index == 5 {
                (700, false, 3)
            } else {
                (0, true, 0)
            }
        };
        assert_eq!(pick_slot(&running, CacheLayout::Unified, idle), Some(5));
        assert_eq!(pick_slot(&running, CacheLayout::PerSequence, idle), Some(1));
    }
}
