# LLM Inference: Bringing AI Capabilities Directly into SQL

*How to call language models from SQL queries for generation, summarization, enrichment, and text-to-SQL conversion.*

> **Part of the Spice Use Case Series**: This article is one of nine deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why SQL-Based LLM Inference?](#why-sql-based-llm-inference)
3. [The AI() SQL Function](#the-ai-sql-function)
4. [Supported Model Providers](#supported-model-providers)
5. [Text-to-SQL: Natural Language Queries](#text-to-sql-natural-language-queries)
6. [Use Case Patterns](#use-case-patterns)
7. [OpenAI-Compatible API Gateway](#openai-compatible-api-gateway)
8. [Getting Started](#getting-started)
9. [Conclusion](#conclusion)

---

## Introduction

Large Language Models (LLMs) have transformed how we process, generate, and understand text. But integrating LLMs into data workflows typically requires:

1. Extracting data from databases
2. Processing through Python/Node.js code
3. Calling LLM APIs
4. Loading results back

What if you could call an LLM directly from SQL?

```sql
SELECT 
    customer_name,
    feedback_text,
    ai('Classify this feedback as positive, negative, or neutral: ' || feedback_text, 'gpt-4') as sentiment
FROM customer_feedback;
```

**LLM Inference in SQL** brings AI capabilities directly into your data layer—no ETL, no application code, just SQL.

---

## Understanding LLM Inference

Before exploring SQL integration, let's understand what LLM inference involves:

### What is LLM Inference?

**Inference** is the process of using a trained model to generate outputs from inputs:

```text
Input (Prompt)                      Output (Completion)
      │                                    │
      ▼                                    ▼
"Summarize this article..."  ─→  LLM  ─→  "The article discusses..."
```

Unlike training (which adjusts model weights), inference uses fixed weights to transform inputs to outputs. This is the computational work that happens every time you use ChatGPT, Claude, or any LLM API.

### Inference Cost Factors

| Factor            | Impact                                | Optimization                   |
| ----------------- | ------------------------------------- | ------------------------------ |
| **Input tokens**  | Linear cost                           | Shorter prompts                |
| **Output tokens** | Higher cost (generation is expensive) | Constrain response length      |
| **Model size**    | Larger = more expensive               | Use appropriate model for task |
| **Batch size**    | Amortizes overhead                    | Batch similar requests         |

### Common Inference Tasks

LLMs excel at various text processing tasks:

**Classification**: Categorizing text into predefined labels

```text
"This product is terrible" → Negative
"Fast shipping, great quality" → Positive
```

**Extraction**: Pulling structured data from unstructured text

```text
"Contact John at john@example.com" → {"name": "John", "email": "john@example.com"}
```

**Generation**: Creating new text based on input

```text
"Write a product description for..." → "Introducing the revolutionary..."
```

**Summarization**: Condensing longer text

```text
[1000-word article] → "This article covers three main points..."
```

**Translation/Transformation**: Converting between formats

```text
"SELECT * FROM users WHERE age > 21" → "Find all users older than 21"
```

### The Integration Challenge

Traditionally, using LLMs with database data requires a multi-step pipeline:

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Traditional Pipeline                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Step 1: Extract data from database                             │
│          SELECT feedback_text FROM customer_feedback             │
│                                                                  │
│  Step 2: Load into Python/Node.js                               │
│          results = db.execute(query).fetchall()                  │
│                                                                  │
│  Step 3: Format prompts                                          │
│          prompts = [template.format(row) for row in results]     │
│                                                                  │
│  Step 4: Batch and call LLM API                                  │
│          responses = asyncio.gather(*[llm(p) for p in prompts])  │
│                                                                  │
│  Step 5: Parse responses                                         │
│          parsed = [parse_json(r) for r in responses]             │
│                                                                  │
│  Step 6: Write back to database                                  │
│          db.executemany("UPDATE ... SET sentiment = ?", parsed) │
│                                                                  │
│  Typical: 50-200 lines of code per use case                     │
└─────────────────────────────────────────────────────────────────┘
```

This creates friction: every new LLM use case requires custom integration code, error handling, rate limiting, and monitoring.

---

## Why SQL-Based LLM Inference?

### Traditional LLM Integration

```text
┌─────────────────────────────────────────────────────────────────┐
│                Traditional LLM Workflow                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Database → Extract → Transform → LLM API → Transform → Load    │
│                                                                  │
│  Steps:                                                          │
│  1. Query database                                               │
│  2. Export to Python/Node                                        │
│  3. Batch data into LLM requests                                 │
│  4. Call OpenAI/Anthropic API                                    │
│  5. Parse responses                                              │
│  6. Write results back to database                               │
│                                                                  │
│  Time: Hours to implement, minutes to run                        │
│  Maintenance: Custom code, API changes, error handling           │
└─────────────────────────────────────────────────────────────────┘
```

### SQL-Native LLM Integration

```text
┌─────────────────────────────────────────────────────────────────┐
│                SQL-Native LLM Workflow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  SELECT ai('prompt', 'model') FROM table;                        │
│                                                                  │
│  Steps:                                                          │
│  1. Write SQL query with ai() function                           │
│  2. Execute                                                      │
│                                                                  │
│  Time: Minutes to implement, same time to run                    │
│  Maintenance: None—just SQL                                      │
└─────────────────────────────────────────────────────────────────┘
```

### Benefits

| Aspect               | Traditional           | SQL-Native         |
| -------------------- | --------------------- | ------------------ |
| **Development time** | Hours                 | Minutes            |
| **Infrastructure**   | Python/Node service   | Just Spice         |
| **Batching**         | Manual implementation | Automatic          |
| **Error handling**   | Custom code           | Built-in retries   |
| **Data movement**    | Export/Import         | In-place           |
| **Observability**    | Custom logging        | Integrated tracing |

---

## The AI() SQL Function

Spice provides the `ai()` user-defined function for inline LLM calls:

### Basic Syntax

```sql
ai(message, model_name)
```

- `message`: The prompt to send to the LLM
- `model_name`: Name of a configured model (optional if only one model is configured)

### How Spice Implements ai()

Under the hood, the `ai()` function is an **AsyncScalarUDF** implemented in Rust. When you execute a query with `ai()`:

1. **Argument Validation**: The UDF validates message size (max 1MB) and batch size (max 100 rows per batch)
2. **Model Resolution**: Looks up the configured model in the `ChatModelStore`
3. **Parallel Execution**: Processes multiple rows concurrently using tokio
4. **Streaming Response**: Each model call uses `chat_stream()` for efficient token-by-token processing
5. **Tracing Integration**: All calls are instrumented with OpenTelemetry spans for observability

```text
SELECT ai('Summarize: ' || content, 'gpt-4') FROM documents;
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Ai::invoke_async_with_args()                                    │
│                                                                  │
│  1. Validate args.args.len() ∈ {1, 2}                           │
│  2. Check args.number_rows <= MAX_BATCH_SIZE (100)              │
│  3. Resolve model from ChatModelStore                           │
│  4. For each row, call model.chat_stream() in parallel          │
│  5. Accumulate streaming responses                              │
│  6. Return StringArray with all completions                     │
└─────────────────────────────────────────────────────────────────┘
```

The `ChatModelStore` is a `HashMap<String, Arc<dyn Chat>>` where `Chat` is a trait implemented by all supported providers (OpenAI, Anthropic, Bedrock, xAI, local models).

### Simple Examples

```sql
-- Single generation
SELECT ai('What is the capital of France?', 'gpt-4');

-- Row-by-row processing
SELECT 
    product_name,
    ai('Write a 50-word product description for: ' || product_name, 'gpt-4') as description
FROM products
LIMIT 10;

-- Classification
SELECT 
    review_text,
    ai('Classify sentiment (positive/negative/neutral): ' || review_text, 'claude') as sentiment
FROM reviews;
```

### Advanced Patterns

#### Structured Output

```sql
SELECT 
    ai(
        'Extract the following from this text as JSON: 
         - company_name
         - contact_email
         - phone_number
         
         Text: ' || raw_text,
        'gpt-4'
    )::json as extracted_data
FROM documents;
```

#### Multi-Step Reasoning

```sql
WITH analyzed AS (
    SELECT 
        ticket_id,
        description,
        ai('What is the main issue in this support ticket? ' || description, 'gpt-4') as issue_summary
    FROM support_tickets
    WHERE status = 'new'
)
SELECT 
    ticket_id,
    issue_summary,
    ai('Suggest a resolution for this issue: ' || issue_summary, 'gpt-4') as suggested_resolution
FROM analyzed;
```

#### Combining with Data Context

```sql
SELECT 
    o.order_id,
    c.name as customer_name,
    o.status,
    ai(
        'Write a personalized order status update email for ' || c.name || 
        ' whose order #' || o.order_id || ' is currently ' || o.status || 
        '. Be friendly and concise.',
        'gpt-4'
    ) as email_content
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'delayed';
```

---

## Supported Model Providers

Spice supports multiple LLM providers:

### OpenAI

```yaml
models:
  - name: gpt-4
    from: openai
    params:
      model: gpt-4-turbo
      openai_api_key: ${secrets:OPENAI_API_KEY}
      
  - name: gpt-4o-mini
    from: openai
    params:
      model: gpt-4o-mini
      openai_api_key: ${secrets:OPENAI_API_KEY}
```

### Anthropic

```yaml
models:
  - name: claude
    from: anthropic
    params:
      model: claude-3-5-sonnet-latest
      anthropic_api_key: ${secrets:ANTHROPIC_API_KEY}
      
  - name: claude-haiku
    from: anthropic
    params:
      model: claude-3-5-haiku-latest
      anthropic_api_key: ${secrets:ANTHROPIC_API_KEY}
```

### Amazon Bedrock

```yaml
models:
  - name: bedrock-claude
    from: bedrock:anthropic.claude-3-sonnet
    params:
      aws_region: us-east-1
      aws_access_key_id: ${secrets:AWS_ACCESS_KEY_ID}
      aws_secret_access_key: ${secrets:AWS_SECRET_ACCESS_KEY}
```

### xAI (Grok)

```yaml
models:
  - name: grok
    from: xai
    params:
      model: grok-4-fast-non-reasoning
      xai_api_key: ${secrets:XAI_API_KEY}
```

### Local Models (CUDA/Metal Accelerated)

```yaml
models:
  - name: local-llama
    from: huggingface:meta-llama/Llama-3.2-3B-Instruct
    params:
      device: cuda  # or 'metal' for Apple Silicon
```

### NVIDIA NIM

```yaml
models:
  - name: nim-llama
    from: nvidia:meta/llama-3.1-8b-instruct
    params:
      nvidia_api_key: ${secrets:NVIDIA_API_KEY}
```

---

## Text-to-SQL: Natural Language Queries

One of the most powerful applications of LLM inference is **Text-to-SQL**—converting natural language questions into SQL queries.

### How It Works

```text
User: "Show me the top 10 customers by revenue last month"
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Text-to-SQL Pipeline                          │
├─────────────────────────────────────────────────────────────────┤
│  1. Analyze schema (tables, columns, types)                      │
│  2. Generate SQL using LLM                                       │
│  3. Validate query syntax                                        │
│  4. Execute query                                                │
│  5. Return results                                               │
└─────────────────────────────────────────────────────────────────┘
                    │
                    ▼
SELECT customer_name, SUM(amount) as revenue
FROM orders
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND order_date < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY customer_name
ORDER BY revenue DESC
LIMIT 10;
```

### Enabling Text-to-SQL

```yaml
models:
  - name: sql-generator
    from: openai
    params:
      model: gpt-4-turbo
      openai_api_key: ${secrets:OPENAI_API_KEY}
      tools: auto  # Enable tool use for SQL generation
```

### Using the API

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1")

response = client.chat.completions.create(
    model="sql-generator",
    messages=[
        {"role": "user", "content": "Show me total revenue by product category for Q1 2024"}
    ]
)

print(response.choices[0].message.content)
```

### Semantic Model for Better Accuracy

Define a semantic layer to improve SQL generation:

```yaml
semantic:
  - name: orders
    description: "Customer orders including purchase details"
    columns:
      - name: order_id
        description: "Unique order identifier"
      - name: customer_id
        description: "Reference to the customer who placed the order"
      - name: total
        description: "Order total in USD"
      - name: order_date
        description: "When the order was placed"
    relationships:
      - from: customer_id
        to: customers.id
        type: many-to-one
```

---

## Use Case Patterns

### Pattern 1: Data Enrichment

Add AI-generated columns to existing data:

```sql
-- Enrich product catalog with AI descriptions
INSERT INTO product_descriptions
SELECT 
    p.id,
    p.name,
    ai(
        'Write an SEO-optimized product description for: ' || p.name || 
        '. Category: ' || p.category || 
        '. Features: ' || p.features,
        'gpt-4'
    ) as description,
    CURRENT_TIMESTAMP as generated_at
FROM products p
WHERE p.id NOT IN (SELECT id FROM product_descriptions);
```

### Pattern 2: Sentiment Analysis

Classify text at scale:

```sql
SELECT 
    review_id,
    review_text,
    ai(
        'Rate the sentiment of this review on a scale of 1-5 where 1 is very negative and 5 is very positive. Return only the number: ' || review_text,
        'gpt-4o-mini'
    )::int as sentiment_score
FROM product_reviews
WHERE sentiment_score IS NULL
LIMIT 1000;
```

### Pattern 3: Summarization

Condense long text:

```sql
SELECT 
    document_id,
    title,
    ai(
        'Summarize this document in 3 bullet points: ' || LEFT(content, 8000),
        'gpt-4'
    ) as summary
FROM documents
WHERE word_count > 1000;
```

### Pattern 4: Translation

Multi-language support:

```sql
SELECT 
    product_id,
    description_en,
    ai('Translate to Spanish: ' || description_en, 'gpt-4') as description_es,
    ai('Translate to French: ' || description_en, 'gpt-4') as description_fr,
    ai('Translate to German: ' || description_en, 'gpt-4') as description_de
FROM products
WHERE international_listing = true;
```

### Pattern 5: Entity Extraction

Structure unstructured data:

```sql
SELECT 
    email_id,
    body,
    ai(
        'Extract as JSON: {sender_name, sender_company, intent, urgency_level, action_items}
         
         Email: ' || body,
        'gpt-4'
    )::json as extracted
FROM emails
WHERE processed = false;
```

### Pattern 6: Content Generation

Generate dynamic content:

```sql
-- Generate personalized marketing emails
SELECT 
    c.id,
    c.name,
    c.last_purchase_category,
    ai(
        'Write a short, personalized marketing email for ' || c.name || 
        ' who last purchased from category: ' || c.last_purchase_category ||
        '. Mention a new arrival in that category. Keep under 100 words.',
        'gpt-4'
    ) as email_content
FROM customers c
WHERE c.email_opt_in = true
  AND c.last_purchase_date > CURRENT_DATE - INTERVAL '90 days';
```

---

## OpenAI-Compatible API Gateway

Beyond the `ai()` function, Spice provides a full OpenAI-compatible HTTP API:

### Chat Completions

```bash
curl -X POST http://localhost:8090/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4",
    "messages": [
      {"role": "system", "content": "You are a helpful assistant."},
      {"role": "user", "content": "What is quantum computing?"}
    ]
  }'
```

### Streaming Responses

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1")

stream = client.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": "Write a poem about databases"}],
    stream=True
)

for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="")
```

### Embeddings

```bash
curl -X POST http://localhost:8090/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{
    "model": "openai_embeddings",
    "input": ["Hello world", "How are you?"]
  }'
```

### Model Routing

Configure multiple models and route by name:

```yaml
models:
  # Fast and cheap for simple tasks
  - name: fast
    from: openai
    params:
      model: gpt-4o-mini
      openai_api_key: ${secrets:OPENAI_API_KEY}
      
  # Powerful for complex tasks
  - name: powerful
    from: anthropic
    params:
      model: claude-3-5-sonnet-latest
      anthropic_api_key: ${secrets:ANTHROPIC_API_KEY}
```

```python
# Route to appropriate model
client.chat.completions.create(model="fast", ...)  # Uses GPT-4o-mini
client.chat.completions.create(model="powerful", ...)  # Uses Claude
```

---

## Getting Started

### 1. Install Spice

```bash
curl https://install.spiceai.org | /bin/bash
```

### 2. Configure Models

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: llm-demo

models:
  - name: gpt-4
    from: openai
    params:
      model: gpt-4-turbo
      openai_api_key: ${secrets:OPENAI_API_KEY}
```

### 3. Add Datasets

```yaml
datasets:
  - name: reviews
    from: postgres:ecommerce.reviews
    acceleration:
      enabled: true
```

### 4. Start Spice

```bash
spiced
```

### 5. Use the AI() Function

```sql
-- Interactive SQL
spice sql

-- Run AI query
SELECT 
    product_name,
    review_text,
    ai('Is this review positive or negative? ' || review_text, 'gpt-4') as sentiment
FROM reviews
LIMIT 5;
```

### 6. Use the HTTP API

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1")

response = client.chat.completions.create(
    model="gpt-4",
    messages=[
        {"role": "user", "content": "Hello, how are you?"}
    ]
)

print(response.choices[0].message.content)
```

---

## Conclusion

LLM Inference in SQL transforms how organizations integrate AI into data workflows:

| Capability           | How It Works                             |
| -------------------- | ---------------------------------------- |
| **AI() Function**    | Call LLMs inline in SQL queries          |
| **Text-to-SQL**      | Natural language to SQL conversion       |
| **Model Gateway**    | OpenAI-compatible API for any model      |
| **Multi-Provider**   | OpenAI, Anthropic, Bedrock, local models |
| **Batch Processing** | Automatic batching for efficiency        |

By bringing LLMs to your data layer, you eliminate ETL complexity, reduce development time, and enable new use cases—all with familiar SQL.

---

## Related Articles in This Series

- **[RAG (Retrieval-Augmented Generation)](rag-explained.md)**: Grounding LLM responses in retrieved context
- **[Secure AI Agents](secure-ai-agents-explained.md)**: Building governed agents that use LLM inference
- **[Secure AI Sandboxing](secure-ai-sandboxing-explained.md)**: Isolating LLM data access for security
- **[Hybrid SQL Search](hybrid-sql-search-explained.md)**: Combining search with AI-powered enrichment
- **[SQL Federation](sql-federation-explained.md)**: Analyzing data across sources with AI

---

## Further Reading

- [AI Gateway Documentation](https://spiceai.org/docs/features/ai-gateway)
- [OpenAI-Compatible API Reference](https://spiceai.org/docs/api/openai)
- [Text-to-SQL Recipe](https://github.com/spiceai/cookbook/blob/trunk/text-to-sql/README.md)
- [AI Gateway Cookbook Recipe](https://github.com/spiceai/cookbook/blob/trunk/openai_sdk/README.md)
- [Semantic Model Documentation](https://spiceai.org/docs/features/semantic-model)

