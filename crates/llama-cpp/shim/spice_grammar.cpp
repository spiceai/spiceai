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

#include "common.h"
#include "json-schema-to-grammar.h"
#include "llama-grammar.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>

namespace {

char * copy(const std::string & text) {
    char * out = static_cast<char *>(std::malloc(text.size() + 1));
    if (out != nullptr) {
        std::memcpy(out, text.c_str(), text.size() + 1);
    }
    return out;
}

// The JSON Schema keywords that bound something from below and from above. A schema with
// the lower bound above the upper admits no value, and llama.cpp's conversion turns the
// count pairs into a GBNF repetition `{min,max}` with max below min, which llama.cpp before
// b12604 parses without end (CVE-2026-137057).
constexpr const char * BOUNDS[][2] = {
    {"minItems", "maxItems"},
    {"minLength", "maxLength"},
    {"minProperties", "maxProperties"},
    {"minContains", "maxContains"},
    {"minimum", "maximum"},
};

// Keywords whose values are data rather than schemas, so their objects are not checked.
bool is_data_keyword(const std::string & key) {
    return key == "const" || key == "enum" || key == "default" || key == "examples";
}

// Throws when `node`, or any schema inside it, bounds something from below more tightly
// than from above. `path` is the node's JSON pointer, for the message.
void check_bounds(const common_json & node, const std::string & path, int depth) {
    if (depth > COMMON_RECURSIVE_LIMIT) {
        throw std::runtime_error("the schema nests deeper than " + std::to_string(COMMON_RECURSIVE_LIMIT) + " levels");
    }
    if (node.is_object()) {
        for (const auto & bound : BOUNDS) {
            if (!node.contains(bound[0]) || !node.contains(bound[1])) {
                continue;
            }
            const common_json & lo = node.at(bound[0]);
            const common_json & hi = node.at(bound[1]);
            if (lo.is_number() && hi.is_number() && lo.get<double>() > hi.get<double>()) {
                throw std::runtime_error(
                    "the schema at '" + (path.empty() ? std::string("/") : path) + "' has " + bound[0] + " " +
                    lo.dump() + " above " + bound[1] + " " + hi.dump() + ", which no value satisfies");
            }
        }
        for (const auto & [key, value] : node.items()) {
            if (!is_data_keyword(key)) {
                check_bounds(value, path + "/" + key, depth + 1);
            }
        }
    } else if (node.is_array()) {
        for (size_t i = 0; i < node.size(); i++) {
            check_bounds(node.at(i), path + "/" + std::to_string(i), depth + 1);
        }
    }
}

// The blanks and comments before a grammar's first rule, as llama.cpp's own `parse_space`
// (file-local in `src/llama-grammar.cpp`) skips them; each rule skips those after it.
const char * skip_space(const char * pos) {
    while (*pos == ' ' || *pos == '\t' || *pos == '#' || *pos == '\r' || *pos == '\n') {
        if (*pos == '#') {
            while (*pos && *pos != '\r' && *pos != '\n') {
                pos++;
            }
        } else {
            pos++;
        }
    }
    return pos;
}

// Throws llama.cpp's reason when it cannot parse the GBNF `grammar` without a vocabulary.
// `llama_grammar_parser::parse` only prints the reason, so its steps are taken here.
void parse_grammar(const char * grammar) {
    llama_grammar_parser parser;
    const char * pos = skip_space(grammar);
    while (*pos) {
        pos = parser.parse_rule(pos);
    }
    for (const auto & rule : parser.rules) {
        if (rule.empty()) {
            throw std::runtime_error("the grammar refers to a rule it does not define");
        }
        for (const auto & elem : rule) {
            if (elem.type == LLAMA_GRETYPE_RULE_REF && (elem.value >= parser.rules.size() || parser.rules[elem.value].empty())) {
                throw std::runtime_error("the grammar refers to a rule it does not define");
            }
        }
    }
    if (parser.rules.empty()) {
        throw std::runtime_error("the grammar has no rules");
    }
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
        const common_json parsed = common_json::parse(schema);
        check_bounds(parsed, "", 0);
        std::string grammar = json_schema_to_grammar(parsed);
        parse_grammar(grammar.c_str());
        return copy(grammar);
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

extern "C" int spice_grammar_check(const char * grammar, char ** error) {
    if (error != nullptr) {
        *error = nullptr;
    }
    if (grammar == nullptr) {
        if (error != nullptr) {
            *error = copy("the grammar is null");
        }
        return 0;
    }
    try {
        parse_grammar(grammar);
        return 1;
    } catch (const std::exception & e) {
        if (error != nullptr) {
            *error = copy(e.what());
        }
    } catch (...) {
        if (error != nullptr) {
            *error = copy("llama.cpp threw a non-standard exception");
        }
    }
    return 0;
}

extern "C" void spice_string_free(char * text) {
    std::free(text);
}
