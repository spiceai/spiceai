# Retrieval-Augmented Generation: Grounding AI in Enterprise Data

*How to build RAG pipelines that combine live, structured, and unstructured data with SQL and hybrid search.*

> **Part of the Spice Use Case Series**: This article is one of nine deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why RAG Matters](#why-rag-matters)
3. [The RAG Architecture Challenge](#the-rag-architecture-challenge)
4. [How Spice Enables Enterprise RAG](#how-spice-enables-enterprise-rag)
5. [Building RAG Pipelines](#building-rag-pipelines)
6. [Advanced RAG Patterns](#advanced-rag-patterns)
7. [Real-World Use Cases](#real-world-use-cases)
8. [Getting Started](#getting-started)
9. [Conclusion](#conclusion)

---

## Introduction

Large Language Models are powerful, but they have fundamental limitations: knowledge cutoffs, hallucinations, and no access to your organization's proprietary data. **Retrieval-Augmented Generation (RAG)** solves these problems by grounding LLM responses in relevant, retrieved context.

But enterprise RAG is more complex than retrieving similar documents. Real applications need:

- **Live data**: Current inventory, real-time prices, today's orders
- **Structured data**: SQL databases, data warehouses, business metrics
- **Unstructured data**: Documents, emails, knowledge bases
- **Hybrid search**: Combining semantic, keyword, and filtered retrieval

This article explores how to build production RAG pipelines that combine all these elements using SQL as the unified interface.

---

## Why RAG Matters

### The Fundamental Problem with LLMs

```text
User: "What's the status of order #12345?"

LLM without RAG:
"I don't have access to your order database. I can provide 
general information about order tracking..."

LLM with RAG:
"Your order #12345 shipped yesterday via FedEx (tracking: 
7891234567) and is expected to arrive on Thursday. The order 
contains: 1x Widget Pro ($49.99) and 2x Gadget Mini ($24.99 each)."
```

RAG bridges the gap between LLM capabilities and enterprise data:

| Without RAG           | With RAG              |
| --------------------- | --------------------- |
| Generic responses     | Data-grounded answers |
| Knowledge cutoff      | Real-time information |
| Hallucinations likely | Verifiable facts      |
| No personalization    | Context-aware         |

### The RAG Process

```text
┌─────────────────────────────────────────────────────────────────┐
│                        RAG Pipeline                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. RETRIEVE                                                     │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ User Query → Embedding → Search → Relevant Context      │ │
│     └─────────────────────────────────────────────────────────┘ │
│                              │                                   │
│  2. AUGMENT                  ▼                                   │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ System Prompt + Retrieved Context + User Query          │ │
│     └─────────────────────────────────────────────────────────┘ │
│                              │                                   │
│  3. GENERATE                 ▼                                   │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ LLM generates response grounded in retrieved data       │ │
│     └─────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## The RAG Architecture Challenge

Enterprise RAG is harder than demos suggest:

### Challenge 1: Data Lives Everywhere

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Enterprise Data Landscape                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Structured               Semi-Structured       Unstructured     │
│  ┌──────────────┐        ┌──────────────┐      ┌──────────────┐ │
│  │ PostgreSQL   │        │ JSON APIs    │      │ Documents    │ │
│  │ Snowflake    │        │ Logs         │      │ Emails       │ │
│  │ SAP          │        │ Config       │      │ Wikis        │ │
│  └──────────────┘        └──────────────┘      └──────────────┘ │
│                                                                  │
│  → Different connectors, schemas, access patterns                │
│  → ETL pipelines to synchronize                                  │
│  → Consistency challenges                                        │
└─────────────────────────────────────────────────────────────────┘
```

### Challenge 2: Stale Embeddings

Traditional vector databases require batch embedding updates:

```text
Document updated at 10:00 AM
   ↓
ETL job runs at 2:00 AM  
   ↓
Embeddings updated
   ↓
User queries at 11:00 AM → Gets stale data (4 hours old minimum)
```

### Challenge 3: Retrieval Limitations

Pure vector search often isn't enough:

```text
User: "Orders over $1000 in California last month"

Vector Search Result:
✓ Semantically similar to "large California orders"
✗ Doesn't filter by dollar amount
✗ Doesn't filter by date range
✗ Doesn't filter by state

→ Returns irrelevant results that "sound" similar
```

### Challenge 4: Context Integration

Combining retrieval with structured lookups is complex:

```python
# Typical multi-step RAG implementation
docs = vector_db.search("customer complaint")        # Vector search
customer = postgres.query("SELECT * FROM customers")  # SQL query
orders = api.get("/orders?customer_id=...")           # API call
tickets = zendesk.search(customer_id)                 # SaaS query

# Manual combination
context = combine(docs, customer, orders, tickets)    # Your problem
response = llm.generate(context)
```

---

## How Spice Enables Enterprise RAG

Spice addresses these challenges with a unified SQL-based approach:

### Unified Data Access

Connect to any data source through a single interface:

```yaml
datasets:
  # Structured data
  - name: orders
    from: postgres:ecommerce.orders
    
  # Data warehouse
  - name: customer_analytics
    from: snowflake:analytics.customers
    
  # Documents
  - name: knowledge_base
    from: s3://docs/knowledge-base/
    params:
      file_format: pdf
    embeddings:
      - column: content
        use: openai_embeddings
        
  # SaaS data
  - name: support_tickets
    from: zendesk:tickets
```

### Real-Time Embeddings

Embeddings generated on-demand or with configurable refresh:

```yaml
datasets:
  - name: documents
    from: sharepoint:documents/
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512
          overlap_size: 64
    acceleration:
      enabled: true
      engine: duckdb
      refresh_schedule: "*/10 * * * *"  # Every 10 minutes
```

### Hybrid Search

Combine vector similarity with SQL filtering:

```sql
-- Semantic search + business filters
SELECT 
    d.title,
    d.content,
    vs.score
FROM vector_search(documents, 'customer refund policy', 50) vs
JOIN documents d ON vs.id = d.id
WHERE d.category = 'policies'
  AND d.status = 'published'
  AND d.last_updated > CURRENT_DATE - INTERVAL '1 year'
ORDER BY vs.score DESC
LIMIT 10;
```

### SQL-Based Context Assembly

Build context with familiar SQL:

```sql
-- Assemble RAG context with a single query
WITH customer_context AS (
    SELECT 
        c.name,
        c.account_tier,
        c.lifetime_value
    FROM customers c
    WHERE c.id = '${customer_id}'
),
recent_orders AS (
    SELECT 
        order_id,
        total,
        status,
        order_date
    FROM orders
    WHERE customer_id = '${customer_id}'
    ORDER BY order_date DESC
    LIMIT 5
),
relevant_docs AS (
    SELECT 
        title,
        content
    FROM vector_search(knowledge_base, '${user_query}', 5) vs
    JOIN knowledge_base kb ON vs.id = kb.id
)
SELECT 
    json_build_object(
        'customer', (SELECT row_to_json(c) FROM customer_context c),
        'orders', (SELECT json_agg(o) FROM recent_orders o),
        'documents', (SELECT json_agg(d) FROM relevant_docs d)
    ) as rag_context;
```

### AI() SQL Function

Generate responses inline with SQL:

```sql
SELECT ai(
    'You are a customer service agent. Use the following context to answer the question.

    Customer Profile:
    ' || customer_profile || '

    Recent Orders:
    ' || recent_orders || '

    Relevant Policies:
    ' || policy_docs || '

    Question: ' || user_question || '

    Provide a helpful, accurate response based only on the provided context.',
    'gpt-4'
) as response;
```

---

## Building RAG Pipelines

### Step 1: Configure Embedding Models

```yaml
# spicepod.yaml
embeddings:
  # OpenAI embeddings
  - name: openai_embeddings
    from: openai
    params:
      model: text-embedding-3-small
      openai_api_key: ${secrets:OPENAI_API_KEY}

  # AWS Bedrock
  - name: titan_embeddings
    from: bedrock:amazon.titan-embed-text-v2
    params:
      aws_region: us-east-1

  # Local/HuggingFace
  - name: local_embeddings
    from: huggingface:sentence-transformers/all-MiniLM-L6-v2
```

### Step 2: Enable Dataset Embeddings

```yaml
datasets:
  - name: knowledge_base
    from: s3://company-docs/
    params:
      file_format: markdown
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512
          overlap_size: 64
          trim_whitespace: true
    acceleration:
      enabled: true
      engine: duckdb
```

### Step 3: Configure LLM Models

```yaml
models:
  - name: gpt-4
    from: openai
    params:
      openai_api_key: ${secrets:OPENAI_API_KEY}
      model: gpt-4-turbo

  - name: claude
    from: anthropic
    params:
      anthropic_api_key: ${secrets:ANTHROPIC_API_KEY}
      model: claude-3-5-sonnet
```

### Step 4: Build the RAG Query

```sql
-- Complete RAG pipeline in SQL
WITH context AS (
    SELECT 
        string_agg(content, E'\n\n---\n\n') as documents
    FROM (
        SELECT kb.content
        FROM vector_search(knowledge_base, 'How do I process a refund?', 5) vs
        JOIN knowledge_base kb ON vs.id = kb.id
        ORDER BY vs.score DESC
    ) top_docs
)
SELECT ai(
    'Based on the following documentation, answer the question.

    Documentation:
    ' || documents || '

    Question: How do I process a refund?

    Answer:',
    'gpt-4'
) as response
FROM context;
```

---

## Advanced RAG Patterns

### Pattern 1: Hybrid Retrieval with RRF

Combine multiple retrieval methods using Reciprocal Rank Fusion:

```sql
-- Hybrid: vector + full-text with RRF scoring
WITH vector_results AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY score DESC) as v_rank
    FROM vector_search(docs, '${query}', 50)
),
text_results AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY score DESC) as t_rank
    FROM text_search(docs, '${query}', 50)
),
fused AS (
    SELECT 
        COALESCE(v.id, t.id) as id,
        -- RRF formula: k=60 is standard
        COALESCE(1.0/(60 + v.v_rank), 0) + 
        COALESCE(1.0/(60 + t.t_rank), 0) as rrf_score
    FROM vector_results v
    FULL OUTER JOIN text_results t ON v.id = t.id
)
SELECT d.*, f.rrf_score
FROM fused f
JOIN docs d ON f.id = d.id
ORDER BY f.rrf_score DESC
LIMIT 10;
```

### Pattern 2: Structured + Unstructured RAG

Combine SQL queries with document retrieval:

```sql
-- Context: structured customer data + relevant documents
WITH customer AS (
    SELECT 
        name, account_tier, region, industry
    FROM customers 
    WHERE id = '${customer_id}'
),
order_summary AS (
    SELECT 
        COUNT(*) as total_orders,
        SUM(amount) as total_spent,
        MAX(order_date) as last_order
    FROM orders 
    WHERE customer_id = '${customer_id}'
),
relevant_docs AS (
    SELECT title, content
    FROM vector_search(documentation, '${question}', 5) vs
    JOIN documentation d ON vs.id = d.id
    WHERE d.access_level <= (
        SELECT CASE account_tier 
            WHEN 'enterprise' THEN 3 
            WHEN 'pro' THEN 2 
            ELSE 1 
        END 
        FROM customer
    )
)
SELECT ai(
    'Customer: ' || (SELECT row_to_json(c)::text FROM customer c) ||
    'Order History: ' || (SELECT row_to_json(o)::text FROM order_summary o) ||
    'Documentation: ' || (SELECT string_agg(content, E'\n') FROM relevant_docs) ||
    'Question: ' || '${question}',
    'gpt-4'
) as response;
```

### Pattern 3: Multi-Hop RAG

Chain retrievals for complex questions:

```sql
-- Step 1: Find relevant product categories
WITH relevant_categories AS (
    SELECT DISTINCT category
    FROM vector_search(products, '${query}', 100) vs
    JOIN products p ON vs.id = p.id
    LIMIT 5
),
-- Step 2: Find detailed docs for those categories
category_docs AS (
    SELECT d.content
    FROM documents d
    WHERE d.category IN (SELECT category FROM relevant_categories)
),
-- Step 3: Find related troubleshooting
troubleshooting AS (
    SELECT content
    FROM vector_search(troubleshooting_guides, '${query}', 5) vs
    JOIN troubleshooting_guides tg ON vs.id = tg.id
)
SELECT ai(
    'Product Information:
    ' || (SELECT string_agg(content, E'\n') FROM category_docs) || '
    
    Troubleshooting Guides:
    ' || (SELECT string_agg(content, E'\n') FROM troubleshooting) || '
    
    Question: ' || '${query}',
    'gpt-4'
) as response;
```

### Pattern 4: RAG with Metadata Filtering

Filter context by metadata before semantic search:

```sql
-- Pre-filter by metadata, then semantic search
SELECT 
    d.title,
    d.content,
    vs.score
FROM vector_search(
    -- Subquery filters by metadata first
    (SELECT * FROM documents 
     WHERE category = 'technical'
       AND language = 'en'
       AND publish_date > '2024-01-01'),
    '${query}',
    10
) vs
JOIN documents d ON vs.id = d.id
ORDER BY vs.score DESC;
```

---

## Real-World Use Cases

### Customer Support Assistant

```yaml
datasets:
  # Customer data
  - name: customers
    from: postgres:crm.customers
    
  # Order history
  - name: orders
    from: postgres:ecommerce.orders
    
  # Knowledge base
  - name: articles
    from: s3://support-docs/
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 500

  # Past support tickets
  - name: tickets
    from: zendesk:tickets
    embeddings:
      - column: description
        use: openai_embeddings
```

```sql
-- Support assistant RAG query
WITH customer_info AS (
    SELECT name, email, account_tier, created_at
    FROM customers WHERE id = '${customer_id}'
),
order_history AS (
    SELECT order_id, status, total, order_date
    FROM orders 
    WHERE customer_id = '${customer_id}'
    ORDER BY order_date DESC LIMIT 5
),
similar_tickets AS (
    SELECT subject, resolution
    FROM vector_search(tickets, '${question}', 3) vs
    JOIN tickets t ON vs.id = t.id
    WHERE t.status = 'resolved'
),
help_articles AS (
    SELECT title, content
    FROM vector_search(articles, '${question}', 5) vs
    JOIN articles a ON vs.id = a.id
)
SELECT ai(
    'You are a support agent. Use this context to help the customer.
    
    Customer: ' || (SELECT row_to_json(c)::text FROM customer_info c) || '
    Recent Orders: ' || (SELECT json_agg(o)::text FROM order_history o) || '
    Similar Resolved Issues: ' || (SELECT json_agg(t)::text FROM similar_tickets t) || '
    Help Articles: ' || (SELECT string_agg(content, E'\n\n') FROM help_articles) || '
    
    Customer Question: ' || '${question}',
    'gpt-4'
) as response;
```

### Legal Document Analysis

```yaml
datasets:
  - name: contracts
    from: s3://legal/contracts/
    params:
      file_format: pdf
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 1000
          overlap_size: 200

  - name: case_law
    from: legal_db:cases
    embeddings:
      - column: summary
        use: openai_embeddings
```

```sql
-- Find relevant contract clauses and precedents
WITH contract_clauses AS (
    SELECT c.section, c.content, vs.score
    FROM vector_search(contracts, 'indemnification liability limits', 10) vs
    JOIN contracts c ON vs.id = c.id
    WHERE c.contract_type = 'SaaS'
),
relevant_cases AS (
    SELECT case_name, summary, outcome
    FROM vector_search(case_law, 'indemnification software services', 5) vs
    JOIN case_law cl ON vs.id = cl.id
    WHERE cl.jurisdiction = 'Federal'
)
SELECT ai(
    'Analyze these contract clauses in light of relevant case law.
    
    Contract Clauses:
    ' || (SELECT string_agg(content, E'\n\n') FROM contract_clauses) || '
    
    Relevant Cases:
    ' || (SELECT json_agg(row_to_json(c))::text FROM relevant_cases c) || '
    
    Provide analysis of the indemnification provisions.',
    'claude'
) as analysis;
```

### Product Recommendation Engine

```yaml
datasets:
  - name: products
    from: catalog:products
    embeddings:
      - column: description
        use: openai_embeddings

  - name: reviews
    from: postgres:reviews
    embeddings:
      - column: review_text
        use: openai_embeddings

  - name: purchase_history
    from: postgres:orders
```

```sql
-- Personalized product recommendations with RAG
WITH user_preferences AS (
    SELECT 
        array_agg(DISTINCT p.category) as categories,
        AVG(o.amount) as avg_spend
    FROM orders o
    JOIN products p ON o.product_id = p.id
    WHERE o.user_id = '${user_id}'
),
similar_products AS (
    SELECT p.id, p.name, p.description, p.price, vs.score
    FROM vector_search(products, '${query}', 20) vs
    JOIN products p ON vs.id = p.id
    WHERE p.price BETWEEN 
        (SELECT avg_spend * 0.5 FROM user_preferences) AND
        (SELECT avg_spend * 2.0 FROM user_preferences)
),
top_reviews AS (
    SELECT r.product_id, r.review_text, r.rating
    FROM reviews r
    WHERE r.product_id IN (SELECT id FROM similar_products)
      AND r.rating >= 4
    ORDER BY r.helpful_votes DESC
    LIMIT 10
)
SELECT ai(
    'Recommend products based on this context.
    
    User Preferences: ' || (SELECT row_to_json(u)::text FROM user_preferences u) || '
    
    Matching Products:
    ' || (SELECT json_agg(p)::text FROM similar_products p) || '
    
    Top Reviews:
    ' || (SELECT json_agg(r)::text FROM top_reviews r) || '
    
    User Query: ' || '${query}' || '
    
    Provide personalized recommendations with explanations.',
    'gpt-4'
) as recommendations;
```

---

## Getting Started

### 1. Install Spice

```bash
curl https://install.spiceai.org | /bin/bash
```

### 2. Create Your Spicepod

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: rag-demo

embeddings:
  - name: openai_embeddings
    from: openai
    params:
      model: text-embedding-3-small
      openai_api_key: ${secrets:OPENAI_API_KEY}

models:
  - name: gpt-4
    from: openai
    params:
      model: gpt-4-turbo
      openai_api_key: ${secrets:OPENAI_API_KEY}

datasets:
  - name: documents
    from: file:./docs/
    params:
      file_format: markdown
    embeddings:
      - column: content
        use: openai_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512
```

### 3. Start Spice

```bash
spiced
```

### 4. Test RAG Queries

```sql
-- Simple RAG
SELECT ai(
    'Based on: ' || 
    (SELECT string_agg(content, E'\n') 
     FROM vector_search(documents, 'getting started guide', 5) vs
     JOIN documents d ON vs.id = d.id) ||
    ' Answer: How do I get started?',
    'gpt-4'
);
```

---

## Conclusion

Enterprise RAG requires more than just vector similarity search. Real applications need:

- **Unified data access** across structured and unstructured sources
- **Hybrid retrieval** combining semantic, keyword, and filtered search
- **Real-time data** without stale embedding pipelines
- **SQL integration** for context assembly and filtering

Spice provides the infrastructure to build these capabilities with familiar SQL interfaces, enabling teams to build production RAG applications that are grounded in accurate, current enterprise data.

---

## Related Articles in This Series

- **[Hybrid SQL Search](hybrid-sql-search-explained.md)**: The search techniques powering RAG retrieval
- **[Application Search](application-search-explained.md)**: Broader search patterns for applications
- **[LLM Inference](llm-inference-explained.md)**: The generation side of RAG—calling AI models from SQL
- **[Secure AI Agents](secure-ai-agents-explained.md)**: Building RAG-powered agents with proper governance
- **[Operational Data Lakehouse](operational-data-lakehouse-explained.md)**: Serving lakehouse data for RAG contexts

---

## Further Reading

- [RAG Use Case Documentation](https://spiceai.org/docs/use-cases/rag)
- [Search Documentation](https://spiceai.org/docs/features/search)
- [Amazon S3 Vectors Cookbook](https://github.com/spiceai/cookbook/tree/trunk/vectors/s3/README.md)
- [Text-to-SQL Recipe](https://github.com/spiceai/cookbook/blob/trunk/text-to-sql/README.md)
- [Semantic Model Documentation](https://spiceai.org/docs/features/semantic-model)

