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

#include "spice_mtp.h"

#include "common.h"
#include "speculative.h"

#include <cstring>
#include <exception>
#include <string>
#include <vector>

namespace {

constexpr llama_seq_id SEQ = 0;

}  // namespace

struct spice_mtp {
    common_params_speculative params;
    common_speculative * spec = nullptr;
    common_batch batch;
    std::vector<llama_token> history;
    std::vector<llama_token> draft;
    // A non-empty draft owes an accept before the next draft.
    bool accept_owed = false;
    std::string error;
};

namespace {

spice_mtp_status invalid(spice_mtp * mtp, const char * message) {
    mtp->error = message;
    return SPICE_MTP_INVALID;
}

// Runs `f`, turning a C++ exception into SPICE_MTP_FAILED with its message.
template <typename F> spice_mtp_status guarded(spice_mtp * mtp, F && f) {
    try {
        return f();
    } catch (const std::exception & e) {
        mtp->error = e.what();
    } catch (...) {
        mtp->error = "llama.cpp threw a non-standard exception";
    }
    return SPICE_MTP_FAILED;
}

}  // namespace

extern "C" spice_mtp * spice_mtp_init(struct llama_context * target, struct llama_context * draft,
                                      int32_t n_max, int32_t n_min, float p_min) {
    if (target == nullptr || draft == nullptr || n_max <= 0 || n_min < 0 || n_min > n_max) {
        return nullptr;
    }
    try {
        auto * mtp = new spice_mtp();
        mtp->params.types = { COMMON_SPECULATIVE_TYPE_DRAFT_MTP };
        mtp->params.draft.ctx_tgt = target;
        mtp->params.draft.ctx_dft = draft;
        mtp->params.draft.n_max = n_max;
        mtp->params.draft.n_min = n_min;
        mtp->params.draft.p_min = p_min;
        mtp->batch = common_batch(target);
        mtp->spec = common_speculative_init(mtp->params, 1);
        if (mtp->spec == nullptr) {
            delete mtp;
            return nullptr;
        }
        return mtp;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void spice_mtp_free(spice_mtp * mtp) {
    if (mtp == nullptr) {
        return;
    }
    if (mtp->spec != nullptr) {
        common_speculative_free(mtp->spec);
    }
    delete mtp;
}

extern "C" const char * spice_mtp_error(const spice_mtp * mtp) {
    return mtp == nullptr ? "no MTP state" : mtp->error.c_str();
}

extern "C" spice_mtp_status spice_mtp_begin(spice_mtp * mtp, const llama_token * prompt, size_t n_prompt) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (prompt == nullptr && n_prompt > 0) {
        return invalid(mtp, "the prompt is null");
    }
    return guarded(mtp, [&] {
        mtp->history.assign(prompt, prompt + n_prompt);
        mtp->accept_owed = false;
        common_speculative_begin(mtp->spec, SEQ, mtp->history);
        return SPICE_MTP_OK;
    });
}

extern "C" spice_mtp_status spice_mtp_process(spice_mtp * mtp, const struct llama_batch * batch) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (batch == nullptr || batch->n_tokens <= 0 || batch->token == nullptr || batch->pos == nullptr ||
        batch->n_seq_id == nullptr || batch->seq_id == nullptr || batch->logits == nullptr) {
        return invalid(mtp, "the batch has no tokens, positions, sequences or output flags");
    }
    for (int32_t k = 0; k < batch->n_tokens; ++k) {
        if (batch->n_seq_id[k] != 1 || batch->seq_id[k][0] != SEQ) {
            return invalid(mtp, "every token of the batch must belong to sequence 0 alone");
        }
    }
    return guarded(mtp, [&] {
        mtp->batch.clear();
        for (int32_t k = 0; k < batch->n_tokens; ++k) {
            mtp->batch.add(batch->token[k], batch->pos[k], SEQ, batch->logits[k] != 0);
        }
        if (!common_speculative_process(mtp->spec, mtp->batch)) {
            mtp->error = "llama.cpp's MTP head could not process the batch";
            return SPICE_MTP_FAILED;
        }
        return SPICE_MTP_OK;
    });
}

extern "C" spice_mtp_status spice_mtp_draft(spice_mtp * mtp, llama_pos n_past, llama_token id_last,
                                            const llama_token * history, size_t n_history,
                                            llama_token * out, size_t capacity, size_t * n_out) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (n_out == nullptr || n_past < 0 || (history == nullptr && n_history > 0)) {
        return invalid(mtp, "bad draft arguments");
    }
    if (mtp->accept_owed) {
        return invalid(mtp, "the previous draft has not been accepted");
    }
    return guarded(mtp, [&] {
        mtp->history.assign(history, history + n_history);
        mtp->draft.clear();
        auto & params = common_speculative_get_draft_params(mtp->spec, SEQ);
        params.drafting = true;
        params.n_max = mtp->params.draft.n_max;
        params.pos0 = n_past;
        params.id_last = id_last;
        params.prompt = &mtp->history;
        params.result = &mtp->draft;
        common_speculative_draft(mtp->spec);
        *n_out = mtp->draft.size();
        if (mtp->draft.size() > capacity) {
            mtp->error = "the draft is longer than the buffer";
            return SPICE_MTP_INVALID;
        }
        if (!mtp->draft.empty()) {
            if (out == nullptr) {
                mtp->error = "the draft buffer is null";
                return SPICE_MTP_INVALID;
            }
            std::memcpy(out, mtp->draft.data(), mtp->draft.size() * sizeof(llama_token));
        }
        mtp->accept_owed = !mtp->draft.empty();
        return SPICE_MTP_OK;
    });
}

extern "C" spice_mtp_status spice_mtp_accept(spice_mtp * mtp, uint16_t n_accepted) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (!mtp->accept_owed) {
        return invalid(mtp, "there is no draft to accept");
    }
    return guarded(mtp, [&] {
        common_speculative_accept(mtp->spec, SEQ, n_accepted);
        mtp->accept_owed = false;
        return SPICE_MTP_OK;
    });
}
