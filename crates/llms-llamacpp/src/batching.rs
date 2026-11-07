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
//! arrives while others run reads its prompt in the same steps, a chunk at a time beside their
//! tokens, so they keep decoding while it reads, and it decodes with them once its prompt is
//! read.
//!
//! A request is admitted with room for its whole possible reply, its prompt plus `max_tokens`
//! or the rest of the context, so a running request never runs out of cache. The caches of idle
//! sequences give up their room first, longest unused first; a request that still does not fit
//! beside the running ones waits, and the requests behind it wait in order. A model that is
//! otherwise idle admits any request whose prompt fits the context, as one request at a time
//! does, after emptying every cache but the one it reuses.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use async_openai::types::chat::FinishReason;
use llama_cpp::{Batch, Context, DraftKind, DraftRequest, Model, Mtp, Token};
use tokio::sync::mpsc;

use super::{
    DECODE_STOPPED, Decoder, Event, Job, Output, PROMPT_BATCH, Picker, PromptCache, PromptPlan,
    Target, plan_prompt, send_unless_stopping,
};

/// How long the decode thread waits when every running request waits on its client.
const PAUSED_POLL: Duration = Duration::from_millis(1);

/// Serves `jobs` up to `caches.len()` at a time, each request in the sequence of the cache it
/// reuses, until the queue closes and every request has finished.
pub(super) fn serve<'m>(
    model: &'m Model,
    decoder: &mut Decoder<'m>,
    split: bool,
    caches: Vec<PromptCache>,
    jobs: &std_mpsc::Receiver<Job>,
    eog: &[Token],
) {
    match decoder {
        Decoder::Plain(context) => run(model, context, split, caches, jobs, eog),
        Decoder::Mtp(spec) => run(model, spec.as_mut(), split, caches, jobs, eog),
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

/// A request in one sequence: reading its prompt, then decoding its reply.
struct Running<'v> {
    out: Output,
    picker: Picker<'v>,
    /// Every token the sequence holds: the prompt, then the reply's. Empty while it reads.
    history: Vec<Token>,
    prompt_tokens: usize,
    /// The token sampled last and already sent, which the next step decodes at `history.len()`;
    /// none is sampled while the prompt is read.
    id: Token,
    /// The prompt still being read, if it is.
    reading: Option<Reading>,
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

/// A prompt being read in steps: the plan, and the next position to decode.
struct Reading {
    plan: PromptPlan,
    next: usize,
}

impl Reading {
    /// Where this step's chunk of the prompt ends: at the split, where a checkpoint or the draft
    /// head's state is saved after exactly the tokens before it, then at the prompt's end.
    fn stop(&self) -> usize {
        let split = self.plan.split();
        if self.next < split {
            split
        } else {
            self.plan.tokens.len()
        }
    }
}

/// A reading request's chunk of a step's batch: prompt positions `from..to`.
struct ReadRow {
    slot: usize,
    offset: i32,
    from: usize,
    to: usize,
}

/// One decoding request's part of a step's batch.
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
    caches: Vec<PromptCache>,
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
        admit(model, shared, &mut slots, &mut waiting, eog);
        if !step(model, shared, split, &mut slots, &mut tick) {
            std::thread::sleep(PAUSED_POLL);
        }
    }
}

/// Starts as many waiting requests, in order, as there are free sequences and room for.
fn admit<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    slots: &mut [Slot<'m>],
    waiting: &mut VecDeque<Job>,
    eog: &[Token],
) {
    let n_ctx = usize::try_from(shared.context().n_ctx()).unwrap_or(usize::MAX);
    while let Some(job) = waiting.front() {
        // A client that has gone, as one that stopped reading before a later choice's turn,
        // is not decoded for.
        if job.events.is_closed() {
            waiting.pop_front();
            continue;
        }
        let Some(free) = choose_slot(model, slots, job) else {
            return;
        };
        let prompt_len = model
            .vocab()
            .tokenize(&job.prompt, false, true)
            .map_or(0, |tokens| tokens.len());
        let reply = job
            .limit
            .unwrap_or(n_ctx)
            .min(n_ctx.saturating_sub(prompt_len));
        let reserve = prompt_len + reply + shared.n_draft() + 1;
        // With nothing running, the request is served as it would be alone, whatever room the
        // idle caches gave up.
        let running = slots.iter().any(|slot| slot.running.is_some());
        if !make_room(shared, slots, free, reserve, n_ctx) && running {
            return;
        }
        let Some(job) = waiting.pop_front() else {
            return;
        };
        start(model, shared, &mut slots[free], job, eog, reserve);
    }
}

/// The free sequence for `job`: the one whose cache shares the longest prefix with its prompt,
/// from the same caller, then the one idle longest, then the lowest. `None` when every sequence
/// runs a request.
fn choose_slot(model: &Model, slots: &[Slot<'_>], job: &Job) -> Option<usize> {
    let tokens = model
        .vocab()
        .tokenize(&job.prompt, false, true)
        .unwrap_or_default();
    slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.running.is_none())
        .max_by_key(|&(index, slot)| {
            let shared = if slot.cache.namespace.as_ref() == Some(&job.namespace) {
                slot.cache
                    .tokens
                    .iter()
                    .zip(&tokens)
                    .take_while(|(a, b)| a == b)
                    .count()
            } else {
                0
            };
            (
                shared,
                std::cmp::Reverse(slot.last_used),
                std::cmp::Reverse(index),
            )
        })
        .map(|(index, _)| index)
}

/// Whether `reserve` more cells fit beside the running requests and the idle caches, emptying
/// the idle caches longest unused first until they do; `free`'s own cache is the new request's
/// to reuse, so it is not counted.
fn make_room<'m, S: Shared<'m>>(
    shared: &mut S,
    slots: &mut [Slot<'m>],
    free: usize,
    reserve: usize,
    n_ctx: usize,
) -> bool {
    let used = |slots: &[Slot<'m>]| -> usize {
        slots
            .iter()
            .enumerate()
            .map(|(index, slot)| match &slot.running {
                Some(running) => running.reserved,
                None if index == free => 0,
                None => slot.cache.tokens.len(),
            })
            .sum()
    };
    while used(slots) + reserve > n_ctx {
        let Some(victim) = slots
            .iter()
            .enumerate()
            .filter(|(index, slot)| {
                *index != free && slot.running.is_none() && !slot.cache.tokens.is_empty()
            })
            .min_by_key(|(_, slot)| slot.last_used)
            .map(|(index, _)| index)
        else {
            return false;
        };
        let slot = &mut slots[victim];
        let mut target = InSeq {
            shared: &mut *shared,
            seq: slot.seq,
        };
        target.clear();
        slot.cache.forget();
    }
    true
}

/// Readies `job`'s prompt to be read into `slot`'s sequence, from the longest prefix its cache
/// holds; the steps that follow read it, beside the other requests' tokens.
fn start<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    slot: &mut Slot<'m>,
    job: Job,
    eog: &[Token],
    reserved: usize,
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
    let picker = match picker {
        Ok(picker) => picker,
        Err(message) => {
            send_unless_stopping(&events, &stopping, Event::Failed(message));
            return;
        }
    };
    out.begin(picker.goes_on());
    let mut target = InSeq {
        shared: &mut *shared,
        seq: slot.seq,
    };
    let plan = match plan_prompt(model, &mut target, &mut slot.cache, (&prompt, &namespace)) {
        Ok(plan) => plan,
        Err(message) => {
            target.clear();
            slot.cache.forget();
            send_unless_stopping(&events, &stopping, Event::Failed(message));
            return;
        }
    };
    slot.running = Some(Running {
        out,
        picker,
        history: Vec::new(),
        prompt_tokens: plan.tokens.len(),
        id: Token(0),
        reading: Some(Reading {
            next: plan.start,
            plan,
        }),
        reserved,
        events,
        stopping,
    });
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

/// One step: drafts for every decoding request whose client has room, verifies all their drafts
/// in one batch beside the next chunk of each reading request's prompt, and sends what each
/// accepted, and each prompt read to its end its first token. Returns false when no request
/// could step.
fn step<'m, S: Shared<'m>>(
    model: &'m Model,
    shared: &mut S,
    split: bool,
    slots: &mut [Slot<'m>],
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
    let (decoding, reading): (Vec<usize>, Vec<usize>) = stepping.iter().partition(|&&index| {
        slots[index]
            .running
            .as_ref()
            .is_some_and(|running| running.reading.is_none())
    });
    let drafts = {
        let requests: Vec<DraftRequest<'_>> = decoding
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
    let mut batch = Batch::new(PROMPT_BATCH.max(decoding.len() * (shared.n_draft() + 1)));
    let mut rows = Vec::with_capacity(decoding.len());
    for (&index, mut draft) in decoding.iter().zip(drafts) {
        let slot = &slots[index];
        let Some(running) = &slot.running else {
            continue;
        };
        let past = running.history.len();
        let n_past = i32::try_from(past).unwrap_or(i32::MAX);
        let drafted = !draft.is_empty();
        // A model split across nodes verifies every drafted token even when the request has
        // fewer left, so each request keeps one shape in the batch; see `generate_mtp`.
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
    // The reading requests' prompts fill the rest of the batch, oldest request first, each chunk
    // ending at its split or its prompt's end; only a prompt's last token outputs logits.
    let mut read_rows = Vec::with_capacity(reading.len());
    for &index in &reading {
        let budget = PROMPT_BATCH.saturating_sub(usize::try_from(batch.n_tokens()).unwrap_or(0));
        if budget == 0 {
            break;
        }
        let slot = &slots[index];
        let Some(read) = slot.running.as_ref().and_then(|r| r.reading.as_ref()) else {
            continue;
        };
        let from = read.next;
        let to = read.stop().min(from + budget);
        let offset = batch.n_tokens();
        let len = read.plan.tokens.len();
        let added = (from..to).try_for_each(|pos| {
            let at = i32::try_from(pos).unwrap_or(i32::MAX);
            batch.add_seq(read.plan.tokens[pos], at, slot.seq, pos + 1 == len)
        });
        if let Err(e) = added {
            fail_all(shared, slots, &stepping, &e.to_string());
            return true;
        }
        read_rows.push(ReadRow {
            slot: index,
            offset,
            from,
            to,
        });
    }
    let decoded = shared
        .context_mut()
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
    for row in read_rows {
        let slot = &mut slots[row.slot];
        let Some(mut running) = slot.running.take() else {
            continue;
        };
        let Some(mut read) = running.reading.take() else {
            slot.running = Some(running);
            continue;
        };
        read.next = row.to;
        let mut target = InSeq {
            shared: &mut *shared,
            seq: slot.seq,
        };
        if read.plan.checkpoint == Some(read.next) {
            slot.cache.save_checkpoint(&mut target, read.next);
        } else if read.plan.stash_at == Some(read.next) {
            slot.cache.head_stash = target.head_state().map(|state| (read.next, state));
        }
        if read.next < read.plan.tokens.len() {
            running.reading = Some(read);
            slot.running = Some(running);
            continue;
        }
        // The whole prompt is read: drafting begins, and its last row predicts the first token.
        running.history = read.plan.tokens;
        if let Err(message) = shared.begin(slot.seq, &running.history) {
            fail(shared, slot, &running.events, &running.stopping, message);
            continue;
        }
        let last = row.offset + i32::try_from(row.to - row.from).unwrap_or(i32::MAX) - 1;
        let in_content = running.out.in_content();
        running.id = running.picker.sample(shared.context(), last, in_content);
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
