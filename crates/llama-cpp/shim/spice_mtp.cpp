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
#include <memory>
#include <string>
#include <vector>

struct spice_mtp {
    common_params_speculative params;
    common_speculative * spec = nullptr;
    common_batch batch;
    // Per sequence: the tokens its draft follows, its last draft, and whether that draft still
    // owes an accept. llama.cpp reads the first two through pointers while it drafts.
    std::vector<std::vector<llama_token>> history;
    std::vector<std::vector<llama_token>> draft;
    std::vector<char> accept_owed;
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

// Whether `seq` is one of the sequences `mtp` drafts for. llama.cpp asserts, ending the
// process, on a sequence it does not hold, so every sequence is checked here first.
bool known(const spice_mtp * mtp, llama_seq_id seq) {
    return seq >= 0 && static_cast<size_t>(seq) < mtp->draft.size();
}

}  // namespace

extern "C" spice_mtp * spice_mtp_init(struct llama_context * target, struct llama_context * draft, uint32_t n_seq,
                                      int32_t n_max, int32_t n_min, float p_min) {
    if (target == nullptr || draft == nullptr || n_seq == 0 || n_seq > llama_n_seq_max(target) ||
        n_seq > llama_n_seq_max(draft) || n_max <= 0 || n_min < 0 || n_min > n_max) {
        return nullptr;
    }
    try {
        auto mtp = std::make_unique<spice_mtp>();
        mtp->params.types = { COMMON_SPECULATIVE_TYPE_DRAFT_MTP };
        mtp->params.draft.ctx_tgt = target;
        mtp->params.draft.ctx_dft = draft;
        mtp->params.draft.n_max = n_max;
        mtp->params.draft.n_min = n_min;
        mtp->params.draft.p_min = p_min;
        mtp->batch = common_batch(target);
        mtp->history.resize(n_seq);
        mtp->draft.resize(n_seq);
        mtp->accept_owed.assign(n_seq, 0);
        mtp->spec = common_speculative_init(mtp->params, n_seq);
        if (mtp->spec == nullptr) {
            return nullptr;
        }
        return mtp.release();
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

extern "C" spice_mtp_status spice_mtp_begin(spice_mtp * mtp, llama_seq_id seq, const llama_token * prompt,
                                            size_t n_prompt) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (!known(mtp, seq)) {
        return invalid(mtp, "the sequence is not one the MTP state drafts for");
    }
    if (prompt == nullptr && n_prompt > 0) {
        return invalid(mtp, "the prompt is null");
    }
    return guarded(mtp, [&] {
        auto & history = mtp->history[seq];
        history.assign(prompt, prompt + n_prompt);
        mtp->accept_owed[seq] = 0;
        common_speculative_begin(mtp->spec, seq, history);
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
    // The head pairs each token with the hidden state of the token before it in the batch, so
    // a sequence whose tokens are split across the batch would read another sequence's state.
    std::vector<char> seen(mtp->draft.size(), 0);
    for (int32_t k = 0; k < batch->n_tokens; ++k) {
        if (batch->n_seq_id[k] != 1 || !known(mtp, batch->seq_id[k][0])) {
            return invalid(mtp, "every token of the batch must belong to one sequence the MTP state drafts for");
        }
        const llama_seq_id seq = batch->seq_id[k][0];
        if (k == 0 || seq != batch->seq_id[k - 1][0]) {
            if (seen[seq]) {
                return invalid(mtp, "each sequence's tokens must be contiguous in the batch");
            }
            seen[seq] = 1;
        }
    }
    return guarded(mtp, [&] {
        mtp->batch.clear();
        for (int32_t k = 0; k < batch->n_tokens; ++k) {
            mtp->batch.add(batch->token[k], batch->pos[k], batch->seq_id[k][0], batch->logits[k] != 0);
        }
        if (!common_speculative_process(mtp->spec, mtp->batch)) {
            mtp->error = "llama.cpp's MTP head could not process the batch";
            return SPICE_MTP_FAILED;
        }
        return SPICE_MTP_OK;
    });
}

extern "C" spice_mtp_status spice_mtp_draft(spice_mtp * mtp, spice_mtp_draft_request * requests, size_t n_requests) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (requests == nullptr && n_requests > 0) {
        return invalid(mtp, "the draft requests are null");
    }
    std::vector<char> asked(mtp->draft.size(), 0);
    for (size_t i = 0; i < n_requests; ++i) {
        const auto & request = requests[i];
        if (!known(mtp, request.seq)) {
            return invalid(mtp, "a draft request names a sequence the MTP state does not draft for");
        }
        if (asked[request.seq]) {
            return invalid(mtp, "two draft requests name the same sequence");
        }
        asked[request.seq] = 1;
        if (request.n_past < 0 || (request.history == nullptr && request.n_history > 0) ||
            (request.out == nullptr && request.capacity > 0)) {
            return invalid(mtp, "bad draft arguments");
        }
        if (mtp->accept_owed[request.seq]) {
            return invalid(mtp, "the sequence's previous draft has not been accepted");
        }
    }
    return guarded(mtp, [&] {
        // Every call names its own sequences: a flag left set by a call that threw would
        // otherwise draft a sequence nobody asked for.
        for (llama_seq_id seq = 0; known(mtp, seq); ++seq) {
            common_speculative_get_draft_params(mtp->spec, seq).drafting = false;
        }
        for (size_t i = 0; i < n_requests; ++i) {
            auto & request = requests[i];
            request.n_out = 0;
            auto & history = mtp->history[request.seq];
            auto & draft = mtp->draft[request.seq];
            history.assign(request.history, request.history + request.n_history);
            draft.clear();
            auto & params = common_speculative_get_draft_params(mtp->spec, request.seq);
            params.drafting = true;
            params.n_max = mtp->params.draft.n_max;
            params.pos0 = request.n_past;
            params.id_last = request.id_last;
            params.prompt = &history;
            params.result = &draft;
        }
        common_speculative_draft(mtp->spec);
        for (size_t i = 0; i < n_requests; ++i) {
            if (mtp->draft[requests[i].seq].size() > requests[i].capacity) {
                mtp->error = "a draft is longer than its buffer";
                return SPICE_MTP_INVALID;
            }
        }
        for (size_t i = 0; i < n_requests; ++i) {
            auto & request = requests[i];
            const auto & draft = mtp->draft[request.seq];
            if (!draft.empty()) {
                std::memcpy(request.out, draft.data(), draft.size() * sizeof(llama_token));
            }
            request.n_out = draft.size();
            mtp->accept_owed[request.seq] = !draft.empty();
        }
        return SPICE_MTP_OK;
    });
}

extern "C" spice_mtp_status spice_mtp_accept(spice_mtp * mtp, llama_seq_id seq, uint16_t n_accepted) {
    if (mtp == nullptr) {
        return SPICE_MTP_INVALID;
    }
    if (!known(mtp, seq)) {
        return invalid(mtp, "the sequence is not one the MTP state drafts for");
    }
    if (!mtp->accept_owed[seq]) {
        return invalid(mtp, "the sequence has no draft to accept");
    }
    if (n_accepted > mtp->draft[seq].size()) {
        return invalid(mtp, "more tokens were accepted than were drafted");
    }
    return guarded(mtp, [&] {
        common_speculative_accept(mtp->spec, seq, n_accepted);
        mtp->accept_owed[seq] = 0;
        return SPICE_MTP_OK;
    });
}
