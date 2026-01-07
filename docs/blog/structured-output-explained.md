# Structured Output from LLMs: Reliable AI for Production Applications

*How to get consistent, parseable responses from language models—from JSON schemas to function calling.*

> **Part of the Spice Use Case Series**: This article is one of our deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Unstructured Output Problem](#the-unstructured-output-problem)
3. [Structured Output Techniques](#structured-output-techniques)
4. [Implementation Patterns](#implementation-patterns)
5. [Validation and Error Handling](#validation-and-error-handling)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Getting Started](#getting-started)
8. [Conclusion](#conclusion)

---

## Introduction

LLMs generate text. Applications need data structures.

```text
What we get:                           What we need:
────────────────────────────           ────────────────────────────
"The customer sentiment is             {
positive with a score of                 "sentiment": "positive",
around 8 out of 10, mainly               "score": 0.8,
due to the fast shipping                 "drivers": [
and product quality."                      "fast_shipping",
                                           "product_quality"
                                         ]
                                       }
```

**Structured Output** bridges this gap—ensuring LLMs return data in formats your application can reliably parse and use.

---

## The Unstructured Output Problem

### The Parsing Nightmare

```python
# Fragile parsing of LLM output
def parse_sentiment(llm_response: str):
    # Hope it contains "positive" or "negative"
    if "positive" in llm_response.lower():
        sentiment = "positive"
    elif "negative" in llm_response.lower():
        sentiment = "negative"
    else:
        sentiment = "neutral"  # Best guess
    
    # Try to find a number
    import re
    numbers = re.findall(r'\d+\.?\d*', llm_response)
    score = float(numbers[0]) / 10 if numbers else 0.5
    
    return {"sentiment": sentiment, "score": score}

# This breaks on:
# "Not positive, not negative, somewhere in between"
# "I'd rate this 7/10 but the sentiment is mixed"
# "The score would be eight out of ten"
```

### Common Failure Modes

| Failure             | Example                               |
| ------------------- | ------------------------------------- |
| Format variation    | "7/10" vs "7 out of 10" vs "score: 7" |
| Extra text          | "Sure! Here's the JSON: {...}"        |
| Invalid JSON        | Missing quotes, trailing commas       |
| Wrong types         | "7" instead of 7                      |
| Missing fields      | Omits optional fields entirely        |
| Hallucinated fields | Adds unexpected fields                |

### The Cost of Unreliability

```text
Production pipeline:
User Request → LLM → Parse Response → Business Logic → Database

If parsing fails 5% of the time:
- 5% of requests need retry or manual handling
- Error handling complexity increases
- Monitoring/alerting overhead
- User experience degradation
```

---

## Structured Output Techniques

### Technique 1: JSON Mode

Modern LLMs have native JSON mode:

```python
import openai

response = openai.chat.completions.create(
    model="gpt-4",
    response_format={"type": "json_object"},
    messages=[
        {
            "role": "system",
            "content": "Return JSON with keys: sentiment (string), score (number 0-1), drivers (array)"
        },
        {
            "role": "user", 
            "content": "Analyze: Great product, fast shipping!"
        }
    ]
)

# Guaranteed valid JSON (but schema not enforced)
result = json.loads(response.choices[0].message.content)
```

**Pros**: Always valid JSON
**Cons**: Schema not enforced, fields may be missing/wrong

### Technique 2: Structured Outputs (Schema Enforcement)

OpenAI and Anthropic support schema-constrained generation:

```python
from pydantic import BaseModel
from openai import OpenAI

class SentimentAnalysis(BaseModel):
    sentiment: str  # "positive", "negative", "neutral"
    score: float    # 0.0 to 1.0
    drivers: list[str]

client = OpenAI()

response = client.beta.chat.completions.parse(
    model="gpt-4o",
    messages=[
        {"role": "user", "content": "Analyze: Great product!"}
    ],
    response_format=SentimentAnalysis
)

result = response.choices[0].message.parsed
# result is a SentimentAnalysis instance with guaranteed fields
```

**Pros**: Schema enforced, type-safe
**Cons**: Limited to supported schemas, may increase latency

### Technique 3: Function Calling

Use function/tool calling for structured extraction:

```python
tools = [
    {
        "type": "function",
        "function": {
            "name": "record_sentiment",
            "description": "Record the sentiment analysis result",
            "parameters": {
                "type": "object",
                "properties": {
                    "sentiment": {
                        "type": "string",
                        "enum": ["positive", "negative", "neutral"]
                    },
                    "score": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1
                    },
                    "drivers": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["sentiment", "score", "drivers"]
            }
        }
    }
]

response = openai.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": "Analyze: Great product!"}],
    tools=tools,
    tool_choice={"type": "function", "function": {"name": "record_sentiment"}}
)

# Extract from tool call
args = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
```

**Pros**: Rich schema support, enum constraints
**Cons**: Slightly more complex API

### Technique 4: SQL-Based Extraction

Use SQL to enforce structure directly:

```sql
-- Define expected output as table columns
SELECT 
    ai_extract(
        content,
        'sentiment VARCHAR, score DECIMAL, drivers TEXT[]',
        'gpt-4'
    ).*
FROM reviews;

-- Result is a structured table:
-- | sentiment | score | drivers                    |
-- | positive  | 0.85  | {fast_shipping, quality}   |
```

---

## Implementation Patterns

### Pattern 1: Entity Extraction

Extract structured entities from text:

```yaml
# Spice configuration
models:
  - name: extractor
    from: openai
    params:
      model: gpt-4
```

```sql
-- Extract entities from support tickets
SELECT 
    ticket_id,
    ai(
        'Extract as JSON {customer_name, product, issue_type, urgency}:' 
        || description,
        'extractor'
    ) as entities
FROM support_tickets
WHERE created_at > NOW() - INTERVAL '1 day';
```

### Pattern 2: Classification Pipeline

Classify data into predefined categories:

```python
from pydantic import BaseModel
from typing import Literal

class TicketClassification(BaseModel):
    category: Literal["billing", "technical", "sales", "general"]
    subcategory: str
    priority: Literal["low", "medium", "high", "urgent"]
    requires_human: bool

async def classify_ticket(ticket_text: str) -> TicketClassification:
    response = await client.beta.chat.completions.parse(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "Classify support tickets accurately."
            },
            {"role": "user", "content": ticket_text}
        ],
        response_format=TicketClassification
    )
    return response.choices[0].message.parsed
```

### Pattern 3: Data Transformation

Transform unstructured data to structured:

```sql
-- Transform free-text product descriptions to structured catalog
INSERT INTO product_catalog (name, category, features, price_range)
SELECT 
    ai_extract(
        raw_description,
        'name VARCHAR, category VARCHAR, features TEXT[], price_range VARCHAR',
        'gpt-4'
    ).*
FROM raw_product_feeds;
```

### Pattern 4: Multi-Turn Extraction

Complex extraction with validation:

```python
async def extract_with_validation(text: str, schema: type[BaseModel]):
    # First pass: extract
    result = await extract(text, schema)
    
    # Validate
    issues = validate(result, schema)
    
    if issues:
        # Second pass: fix issues
        result = await extract(
            text,
            schema,
            additional_context=f"Previous extraction had issues: {issues}"
        )
    
    return result
```

---

## Validation and Error Handling

### Schema Validation

Always validate LLM output:

```python
from pydantic import BaseModel, validator, ValidationError
from typing import Literal

class OrderExtraction(BaseModel):
    product_id: str
    quantity: int
    shipping_method: Literal["standard", "express", "overnight"]
    
    @validator('quantity')
    def quantity_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError('quantity must be positive')
        return v
    
    @validator('product_id')
    def product_id_must_be_valid(cls, v):
        if not v.startswith('PROD-'):
            raise ValueError('invalid product_id format')
        return v

def safe_extract(llm_response: str) -> OrderExtraction | None:
    try:
        data = json.loads(llm_response)
        return OrderExtraction(**data)
    except (json.JSONDecodeError, ValidationError) as e:
        log.warning(f"Extraction failed: {e}")
        return None
```

### Retry Strategies

Handle extraction failures gracefully:

```python
from tenacity import retry, stop_after_attempt, retry_if_exception_type

@retry(
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(ValidationError)
)
async def extract_with_retry(text: str, schema: type[BaseModel]):
    response = await client.beta.chat.completions.parse(
        model="gpt-4o",
        messages=[{"role": "user", "content": text}],
        response_format=schema
    )
    return response.choices[0].message.parsed
```

### Fallback Chains

Multiple extraction strategies:

```python
async def robust_extract(text: str) -> dict:
    # Try structured output first
    try:
        return await structured_extract(text)
    except StructuredOutputError:
        pass
    
    # Fall back to JSON mode
    try:
        return await json_mode_extract(text)
    except JSONDecodeError:
        pass
    
    # Last resort: regex parsing
    return regex_extract(text)
```

---

## Real-World Use Cases

### Use Case 1: Invoice Processing

```python
class InvoiceData(BaseModel):
    vendor_name: str
    invoice_number: str
    date: str  # ISO format
    line_items: list[LineItem]
    subtotal: float
    tax: float
    total: float
    
class LineItem(BaseModel):
    description: str
    quantity: int
    unit_price: float
    amount: float

async def process_invoice(invoice_text: str) -> InvoiceData:
    return await client.beta.chat.completions.parse(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": "Extract invoice data accurately. Use ISO date format."
            },
            {"role": "user", "content": invoice_text}
        ],
        response_format=InvoiceData
    ).choices[0].message.parsed
```

### Use Case 2: Lead Qualification

```sql
-- Qualify leads from form submissions
SELECT 
    lead_id,
    email,
    ai(
        'Qualify this lead. Return JSON with: ' ||
        'company_size (small/medium/enterprise), ' ||
        'budget_range (string), ' ||
        'intent_score (1-10), ' ||
        'recommended_action (contact_now/nurture/disqualify). ' ||
        'Form data: ' || form_response,
        'gpt-4'
    ) as qualification
FROM raw_leads
WHERE processed = false;
```

### Use Case 3: Content Moderation

```python
class ModerationResult(BaseModel):
    is_safe: bool
    categories: list[Literal[
        "spam", "harassment", "hate_speech", 
        "violence", "adult", "none"
    ]]
    confidence: float
    action: Literal["approve", "review", "reject"]
    reason: str | None

async def moderate_content(content: str) -> ModerationResult:
    return await extract(content, ModerationResult)
```

### Use Case 4: Resume Parsing

```python
class ResumeData(BaseModel):
    name: str
    email: str
    phone: str | None
    summary: str
    experience: list[Experience]
    education: list[Education]
    skills: list[str]
    
class Experience(BaseModel):
    company: str
    title: str
    start_date: str
    end_date: str | None
    description: str
    
class Education(BaseModel):
    institution: str
    degree: str
    field: str
    graduation_year: int

# Parse resumes with guaranteed structure
parsed_resume = await extract(resume_text, ResumeData)
```

---

## Getting Started

### Step 1: Define Your Schema

```python
from pydantic import BaseModel

class YourOutputSchema(BaseModel):
    field1: str
    field2: int
    field3: list[str]
```

### Step 2: Configure Spice

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: structured-output

models:
  - name: extractor
    from: openai
    params:
      model: gpt-4o
```

### Step 3: Use in Queries

```sql
-- SQL approach
SELECT 
    ai('Extract {name, age, interests}: ' || bio, 'extractor')
FROM users;
```

```python
# Python approach
from spicepy import Client

client = Client()

result = await client.query("""
    SELECT ai(
        'Extract as JSON {category, sentiment, score}: ' || review,
        'extractor'
    ) as analysis
    FROM reviews
    LIMIT 10
""")
```

### Step 4: Add Validation

```python
from pydantic import BaseModel, validator

class Analysis(BaseModel):
    category: str
    sentiment: str
    score: float
    
    @validator('score')
    def score_in_range(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('score must be 0-1')
        return v

# Validate each result
for row in result:
    try:
        analysis = Analysis.parse_raw(row['analysis'])
        # Use validated data
    except ValidationError as e:
        # Handle extraction failure
        log.error(f"Validation failed: {e}")
```

---

## Conclusion

Structured output transforms LLMs from text generators to data extractors:

| Aspect         | Unstructured | Structured       |
| -------------- | ------------ | ---------------- |
| Parsing        | Regex, hope  | Schema-validated |
| Reliability    | ~90%         | ~99.9%           |
| Error handling | Complex      | Simple           |
| Integration    | Manual       | Automatic        |
| Maintenance    | High         | Low              |

Key techniques:

1. **JSON Mode**: Guaranteed valid JSON
2. **Structured Outputs**: Schema enforcement
3. **Function Calling**: Rich type constraints
4. **SQL Integration**: Direct database population

Build for structure from the start. Your future self will thank you.

---

## Related Articles in This Series

- **[LLM Inference](llm-inference-explained.md)**: AI-native query capabilities
- **[Agentic Data Access](agentic-data-access-explained.md)**: AI agents with tools
- **[Cost-Optimized AI](cost-optimized-ai-explained.md)**: Efficiency strategies
- **[RAG Explained](rag-explained.md)**: Retrieval-augmented generation

---

## Further Reading

- [Spice Documentation](https://spiceai.org/docs)
- [OpenAI Structured Outputs](https://platform.openai.com/docs/guides/structured-outputs)
- [Pydantic Documentation](https://docs.pydantic.dev/)

