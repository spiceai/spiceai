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

// Whether llama.cpp can split a model of an architecture across devices tensor by tensor
// (`LLAMA_SPLIT_MODE_TENSOR`), which it decides from llama.cpp's internal architecture table.

#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Whether a model whose GGUF header names `arch` (`general.architecture`) loads split by
// tensor. False for an architecture llama.cpp does not know.
bool spice_arch_splits_by_tensor(const char * arch);

#ifdef __cplusplus
}
#endif
