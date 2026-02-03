# Structured Output from LLMs

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

Structured Output: The Missing Link Between LLMs and Production Systems

LLMs generate text. Production systems need data structures. This fundamental mismatch is where most AI integrations break.

The problem isn't that LLMs can't produce structured data—it's that they produce it inconsistently. Ask for a score from 1-10, and you might get "The sentiment is quite positive, I'd give it roughly 8.2" or "Score: 8.2/10 (positive)" or "8.2" or "Positive sentiment. Score = 8.2."

Same information. Four formats. Most parsers handle one or two.

The traditional approach—generate text, then extract structure—is fundamentally flawed. You're fighting the model's natural variability instead of constraining it.

Approach 1: Post-hoc parsing. Prompt goes to LLM, LLM generates free text, regex or parser tries to extract data. This is fragile. Unpredictable output leads to failures on edge cases.

Approach 2: Constrained decoding. Prompt plus schema goes to LLM, LLM generates valid JSON that matches the schema, output is validated and type-safe. This is robust. Token sampling is restricted to valid outputs, so structure is guaranteed.

Four techniques that work:

→ JSON Mode: Guarantees valid JSON syntax. Does NOT enforce schema—fields may be wrong or missing. Good for quick prototyping.

→ Schema Enforcement (Structured Outputs): You provide a JSON schema or Pydantic model. Model's token probabilities are constrained to valid output. Type errors are mathematically impossible. Use for production systems.

→ Function/Tool Calling: Define function signatures with typed parameters. Model returns arguments matching the signature. Supports enums, nested objects, arrays. Use when output triggers downstream actions.

→ SQL-Based Extraction: Output schema defined as table columns. SELECT ai_extract(...) FROM documents. Results are rows with typed columns. Use for batch processing and analytics pipelines.

How constrained decoding works under the hood: Modern LLMs use token-by-token generation. At each step, the model produces probability scores for all possible next tokens. Constrained decoding modifies this process: tokens that would produce invalid output get their probabilities zeroed out.

If your schema says the score field is a number, the model literally cannot generate "score": "eight". The token "eight" has zero probability in that context.

From experience: switching from regex parsing to schema enforcement took our failure rate from 5% to zero. Not reduced—eliminated. The constraint is structural, not probabilistic.

The principle: treat the LLM boundary like any other system boundary. Define the contract explicitly. Enforce it structurally.

---

## X (5 posts, 280 characters each)

Post 1:
LLMs generate text. Applications need data structures. Ask for a score, you get "7/10" or "7 out of 10" or "score: 7" or "Sure! Here's the score...". Same info, different formats. Parsers fail.

Post 2:
The parsing nightmare: missing quotes, trailing commas, wrong types, "Sure! Here's the JSON:" prefixes. Post-hoc parsing is fragile. 5% failure rate in production is a nightmare.

Post 3:
Structured output techniques: JSON mode (valid syntax, no schema). Schema enforcement (Pydantic models, type-safe). Function calling (rich constraints, enums). SQL extraction (columns as schema).

Post 4:
How it works: constrained decoding. At each token, probabilities for invalid tokens are zeroed. If schema says number, the model cannot output "eight". The constraint is structural, not probabilistic.

Post 5:
The insight: constrain generation, don't parse afterward. We went from 5% parse failures to zero. Treat the LLM boundary like any system boundary. Define the contract. Enforce it structurally.
