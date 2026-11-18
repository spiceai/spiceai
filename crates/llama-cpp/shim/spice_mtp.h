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

// A C interface to llama.cpp's MTP speculative decoding (`common/speculative.h`), which is
// C++ only. It drafts for sequences 0..n_seq-1 of the target, the way llama.cpp's server drives
// its slots: one draft call drafts for every sequence it names in one pass of the head.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "llama.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct spice_mtp spice_mtp;

// How a draft model drafts: an MTP head reads the target's hidden states; a DFlash drafter
// reads the target's layer inputs; a DSpark drafter is a DFlash drafter with a Markov head.
typedef enum spice_draft_kind {
    SPICE_DRAFT_NONE = 0,
    SPICE_DRAFT_MTP = 1,
    SPICE_DRAFT_DFLASH = 2,
    SPICE_DRAFT_DSPARK = 3,
} spice_draft_kind;

// SPICE_DRAFT_DFLASH or SPICE_DRAFT_DSPARK for a GGUF whose architecture is `dflash`, read from
// its header; SPICE_DRAFT_NONE for any other file, or one that cannot be read.
spice_draft_kind spice_dflash_kind(const char * path);

typedef enum spice_mtp_status {
    SPICE_MTP_OK = 0,
    // The call was made out of order or with bad arguments; see spice_mtp_error.
    SPICE_MTP_INVALID = -1,
    // llama.cpp threw or reported a failure; see spice_mtp_error.
    SPICE_MTP_FAILED = -2,
} spice_mtp_status;

// Returns NULL when llama.cpp cannot draft with these contexts. `draft` must be a context of
// the draft model created with `target` as its `ctx_other` (an MTP context for an MTP head),
// and both must hold exactly `n_seq` sequences. Neither context is owned.
spice_mtp * spice_mtp_init(struct llama_context * target, struct llama_context * draft,
                           spice_draft_kind kind, uint32_t n_seq, int32_t n_max, int32_t n_min, float p_min);
void spice_mtp_free(spice_mtp * mtp);

// The message of the last call that did not return SPICE_MTP_OK; valid until the next call.
const char * spice_mtp_error(const spice_mtp * mtp);

// Starts a generation in sequence `seq`, after its prompt has been decoded and passed to
// spice_mtp_process.
spice_mtp_status spice_mtp_begin(spice_mtp * mtp, llama_seq_id seq, const llama_token * prompt, size_t n_prompt);

// Hands the head a batch the target just decoded, so it reads the target's hidden states. Each
// token belongs to one sequence, and each sequence's tokens are contiguous in the batch.
spice_mtp_status spice_mtp_process(spice_mtp * mtp, const struct llama_batch * batch);

// One sequence's draft: up to n_max tokens to follow `id_last` at position `n_past`, where
// `history` is every token before `id_last`. The count goes to `n_out`; more than `capacity`
// fails the call.
typedef struct spice_mtp_draft_request {
    llama_seq_id        seq;
    llama_pos           n_past;
    llama_token         id_last;
    const llama_token * history;
    size_t              n_history;
    llama_token *       out;
    size_t              capacity;
    size_t              n_out;
} spice_mtp_draft_request;

// Drafts for every request's sequence in one pass of the head. No two requests may name the
// same sequence.
spice_mtp_status spice_mtp_draft(spice_mtp * mtp, spice_mtp_draft_request * requests, size_t n_requests);

// Reports how many of sequence `seq`'s drafted tokens the target accepted; owed after every
// non-empty draft of that sequence.
spice_mtp_status spice_mtp_accept(spice_mtp * mtp, llama_seq_id seq, uint16_t n_accepted);

// Sequence `seq`'s draft-head state that a cache truncation does not restore: the target's
// hidden state at the last position the head read for it, which it pairs with the token after
// it. `*data` and `*n` describe it until the next call; `*n` is 0 when the head holds none.
spice_mtp_status spice_mtp_get_state(spice_mtp * mtp, llama_seq_id seq, const uint8_t ** data, size_t * n);

// Restores a state spice_mtp_get_state returned for sequence `seq`, for its cache cut back to the
// position after it.
spice_mtp_status spice_mtp_set_state(spice_mtp * mtp, llama_seq_id seq, const uint8_t * data, size_t n);

#ifdef __cplusplus
}
#endif
