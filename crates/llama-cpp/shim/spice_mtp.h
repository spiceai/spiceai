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
// C++ only. It drafts for the contexts' sequences the way llama.cpp's server drafts for its
// slots: one pass of the head drafts for every sequence that asks.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include "llama.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct spice_mtp spice_mtp;

typedef enum spice_mtp_status {
    SPICE_MTP_OK = 0,
    // The call was made out of order or with bad arguments; see spice_mtp_error.
    SPICE_MTP_INVALID = -1,
    // llama.cpp threw or reported a failure; see spice_mtp_error.
    SPICE_MTP_FAILED = -2,
} spice_mtp_status;

// One sequence's draft: the tokens it follows, and room for it.
typedef struct spice_mtp_draft_request {
    // The sequence, below the `n_seq` spice_mtp_init was given.
    llama_seq_id seq;
    // The position of `id_last`.
    llama_pos n_past;
    // The token the draft follows.
    llama_token id_last;
    // Every token of the sequence before `id_last`.
    const llama_token * history;
    size_t n_history;
    // Room for `capacity` tokens: spice_mtp_draft writes the draft there and its length to `n_out`.
    llama_token * out;
    size_t capacity;
    size_t n_out;
} spice_mtp_draft_request;

// Returns NULL when llama.cpp cannot draft with these contexts. `draft` must be an MTP
// context created with `target` as its `ctx_other`, and both must hold at least `n_seq`
// sequences. Neither context is owned.
spice_mtp * spice_mtp_init(struct llama_context * target, struct llama_context * draft, uint32_t n_seq,
                           int32_t n_max, int32_t n_min, float p_min);
void spice_mtp_free(spice_mtp * mtp);

// The message of the last call that did not return SPICE_MTP_OK; valid until the next call.
const char * spice_mtp_error(const spice_mtp * mtp);

// Starts a generation in sequence `seq`, after its prompt has been decoded and passed to
// spice_mtp_process.
spice_mtp_status spice_mtp_begin(spice_mtp * mtp, llama_seq_id seq, const llama_token * prompt, size_t n_prompt);

// Hands the head a batch the target just decoded, so it reads the target's hidden states.
// Every token belongs to one sequence below `n_seq`, and each sequence's tokens are contiguous.
spice_mtp_status spice_mtp_process(spice_mtp * mtp, const struct llama_batch * batch);

// Drafts up to `n_max` tokens for each request's sequence, in one pass of the head over all of
// them. No two requests name the same sequence. A call that fails leaves no accept owed.
spice_mtp_status spice_mtp_draft(spice_mtp * mtp, spice_mtp_draft_request * requests, size_t n_requests);

// Reports how many of sequence `seq`'s drafted tokens the target accepted; owed after each
// non-empty draft of that sequence, before its next draft.
spice_mtp_status spice_mtp_accept(spice_mtp * mtp, llama_seq_id seq, uint16_t n_accepted);

#ifdef __cplusplus
}
#endif
