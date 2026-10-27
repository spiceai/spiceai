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

#include "spice_grammar.h"

#include "json-schema-to-grammar.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

namespace {

char * copy(const std::string & text) {
    char * out = static_cast<char *>(std::malloc(text.size() + 1));
    if (out != nullptr) {
        std::memcpy(out, text.c_str(), text.size() + 1);
    }
    return out;
}

}  // namespace

extern "C" char * spice_json_schema_to_grammar(const char * schema, char ** error) {
    if (error != nullptr) {
        *error = nullptr;
    }
    if (schema == nullptr) {
        if (error != nullptr) {
            *error = copy("the schema is null");
        }
        return nullptr;
    }
    try {
        return copy(json_schema_to_grammar(common_json::parse(schema)));
    } catch (const std::exception & e) {
        if (error != nullptr) {
            *error = copy(e.what());
        }
    } catch (...) {
        if (error != nullptr) {
            *error = copy("llama.cpp threw a non-standard exception");
        }
    }
    return nullptr;
}

extern "C" void spice_string_free(char * text) {
    std::free(text);
}
