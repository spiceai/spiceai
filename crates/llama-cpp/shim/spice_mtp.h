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
// C++ only. It drives one sequence, id 0, the way llama.cpp's server drives a slot.

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

// Returns NULL when llama.cpp cannot draft with these contexts. `draft` must be an MTP
// context created with `target` as its `ctx_other`. Neither context is owned.
spice_mtp * spice_mtp_init(struct llama_context * target, struct llama_context * draft,
                           int32_t n_max, int32_t n_min, float p_min);
void spice_mtp_free(spice_mtp * mtp);

// The message of the last call that did not return SPICE_MTP_OK; valid until the next call.
const char * spice_mtp_error(const spice_mtp * mtp);

// Starts a generation, after the prompt has been decoded and passed to spice_mtp_process.
spice_mtp_status spice_mtp_begin(spice_mtp * mtp, const llama_token * prompt, size_t n_prompt);

// Hands the head a batch the target just decoded, so it reads the target's hidden states.
spice_mtp_status spice_mtp_process(spice_mtp * mtp, const struct llama_batch * batch);

// Drafts up to n_max tokens to follow `id_last` at position `n_past`. `history` is every
// token before `id_last`. Writes the count to `n_out`, failing if it exceeds `capacity`.
spice_mtp_status spice_mtp_draft(spice_mtp * mtp, llama_pos n_past, llama_token id_last,
                                 const llama_token * history, size_t n_history,
                                 llama_token * out, size_t capacity, size_t * n_out);

// Reports how many drafted tokens the target accepted; owed after every non-empty draft.
spice_mtp_status spice_mtp_accept(spice_mtp * mtp, uint16_t n_accepted);

#ifdef __cplusplus
}
#endif
