# Structured Output from LLMs

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**Structured Output: The Missing Link Between LLMs and Production Systems**

LLMs generate text. Production systems need data structures. This fundamental mismatch is where most AI integrations break.

The problem isn't that LLMs can't produce structured data—it's that they produce it inconsistently. Ask for a score from 1-10, and you might get:

```
"The sentiment is quite positive, I'd give it roughly 8.2"
"Score: 8.2/10 (positive)"
"8.2"
"Positive sentiment. Score = 8.2."
```

Same information. Four formats. Most parsers handle one or two.

The traditional approach—generate text, then extract structure—is fundamentally flawed. You're fighting the model's natural variability instead of constraining it.

```
┌─────────────────────────────────────────────────────────────────┐
│              STRUCTURED OUTPUT: HOW IT WORKS                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   APPROACH 1: Post-hoc parsing (fragile)                        │
│                                                                  │
│   Prompt ──→ LLM ──→ Free text ──→ Regex/Parser ──→ Maybe data  │
│                           │                              │       │
│                           ▼                              ▼       │
│                    Unpredictable              Fails on edge cases│
│                                                                  │
│   APPROACH 2: Constrained decoding (robust)                     │
│                                                                  │
│   Prompt + Schema ──→ LLM ──→ Valid JSON ──→ Validated data     │
│                  │            │                    │             │
│                  ▼            ▼                    ▼             │
│           Token sampling   Structurally      Type-safe,         │
│           restricted to    guaranteed        guaranteed          │
│           valid outputs    correct                               │
│                                                                  │
│   FOUR TECHNIQUES:                                               │
│                                                                  │
│   1. JSON Mode                                                   │
│      └─ Guarantees valid JSON syntax                            │
│      └─ Does NOT enforce schema (fields may be wrong/missing)   │
│                                                                  │
│   2. Schema Enforcement (Structured Outputs)                    │
│      └─ You provide JSON schema or Pydantic model               │
│      └─ Model's token probabilities constrained to valid output │
│      └─ Type errors mathematically impossible                   │
│                                                                  │
│   3. Function/Tool Calling                                       │
│      └─ Define function signatures with typed parameters        │
│      └─ Model returns arguments matching the signature          │
│      └─ Supports enums, nested objects, arrays                  │
│                                                                  │
│   4. SQL-Based Extraction                                        │
│      └─ Output schema defined as table columns                  │
│      └─ SELECT ai_extract(...).* FROM documents                 │
│      └─ Results are rows with typed columns                     │
└─────────────────────────────────────────────────────────────────┘
```

**How constrained decoding works under the hood:**

Modern LLMs use token-by-token generation. At each step, the model produces probability scores for all possible next tokens. Constrained decoding modifies this process: tokens that would produce invalid output get their probabilities zeroed out.

If your schema says `{"score": number}`, the model literally cannot generate `{"score": "eight"}`. The token "eight" has zero probability in that context.

**When to use each technique:**

- **JSON Mode**: Quick prototyping, when schema flexibility is okay
- **Schema Enforcement**: Production systems requiring type safety
- **Function Calling**: When output triggers downstream actions
- **SQL Extraction**: Batch processing, analytics pipelines

From experience: switching from regex parsing to schema enforcement took our failure rate from 5% to zero. Not reduced—eliminated. The constraint is structural, not probabilistic.

The principle: treat the LLM boundary like any other system boundary. Define the contract explicitly. Enforce it structurally.

---

## X

LLMs generate text. Applications need data structures.

The parsing nightmare:
- "7/10" vs "7 out of 10" vs "score: 7"
- "Sure! Here's the JSON: {...}"
- Missing quotes, trailing commas, wrong types

Structured output techniques that actually work:

1. JSON mode → guaranteed valid JSON (but schema not enforced)
2. Schema enforcement → Pydantic models, type-safe
3. Function calling → rich constraints, enums
4. SQL extraction → columns as schema

The insight: constrain generation, don't parse afterward.

```python
response_format=SentimentAnalysis  # Schema enforced at generation
```

5% parse failures in production = nightmare. Fix it at the source.
