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

#include "spice_split.h"

#include "llama-arch.h"

#include <string>

extern "C" bool spice_arch_splits_by_tensor(const char * arch) {
    if (arch == nullptr) {
        return false;
    }
    try {
        const llm_arch parsed = llm_arch_from_string(arch);
        return parsed != LLM_ARCH_UNKNOWN && llm_arch_supports_sm_tensor(parsed);
    } catch (...) {
        return false;
    }
}
