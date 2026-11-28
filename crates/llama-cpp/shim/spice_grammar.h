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

// A C interface to llama.cpp's JSON-schema-to-grammar conversion
// (`common/json-schema-to-grammar.h`), which is C++ only.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Converts a JSON schema to a GBNF grammar whose start rule is `root`. Returns the grammar,
// or NULL with the reason in `*error`; free either string with spice_string_free. A schema
// whose lower bound on a count or a value exceeds its upper bound (`minItems` above
// `maxItems`, and the like) is refused before conversion, and the grammar must parse.
char * spice_json_schema_to_grammar(const char * schema, char ** error);

// Whether llama.cpp parses the GBNF `grammar` without a vocabulary, so one that names a
// token by its text (`<think>`) fails; one by its id (`<[42]>`) parses. Returns 1, or 0 with the reason in `*error`; free
// it with spice_string_free.
int spice_grammar_check(const char * grammar, char ** error);

void spice_string_free(char * text);

#ifdef __cplusplus
}
#endif
