# Cost-Optimized AI Infrastructure

*Strategies for reducing AI compute costs while maintaining quality—from intelligent caching to model routing.*

> **Part of the Spice Use Case Series**: This article is one of our deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [Understanding AI Costs](#understanding-ai-costs)
3. [Cost Optimization Strategies](#cost-optimization-strategies)
4. [Implementation with Spice](#implementation-with-spice)
5. [Measuring Cost Efficiency](#measuring-cost-efficiency)
6. [Case Studies](#case-studies)
7. [Getting Started](#getting-started)
8. [Conclusion](#conclusion)

---

## Introduction

AI is expensive. A single enterprise application can easily spend:

| Cost Category        | Monthly Spend      |
| -------------------- | ------------------ |
| LLM API calls        | $10,000 - $100,000 |
| Embedding generation | $5,000 - $20,000   |
| Vector database      | $2,000 - $10,000   |
| GPU inference        | $5,000 - $50,000   |

The good news: **50-80% of these costs are avoidable** with the right architecture.

This guide covers practical strategies for cutting AI infrastructure costs without sacrificing quality.

---

## Understanding AI Costs

### The AI Cost Pyramid

```text
                    ▲
                   ╱ ╲
                  ╱   ╲     GPT-4, Claude Opus
                 ╱ $$$ ╲    ($30-60/M tokens)
                ╱───────╲
               ╱         ╲    GPT-4o-mini, Claude Haiku
              ╱    $$     ╲   ($0.50-2/M tokens)
             ╱─────────────╲
            ╱               ╲    Local LLMs (Llama, Mistral)
           ╱       $        ╲    ($0.05-0.20/M tokens)
          ╱───────────────────╲
         ╱                     ╲    Cached Responses
        ╱         FREE          ╲   ($0)
       ╱─────────────────────────╲
```

### Where Costs Hide

#### 1. Redundant Inference

```text
User A: "What are your business hours?"
User B: "What hours are you open?"
User C: "When do you open?"
User D: "Business hours?"

4 API calls × $0.01 = $0.04
With semantic caching: 1 call + 3 cache hits = $0.01

75% savings on this pattern alone
```

#### 2. Over-provisioned Models

```text
Query: "What's 2+2?"

Using GPT-4: $0.03/1K tokens
Using GPT-4o-mini: $0.00015/1K tokens

200x cost difference for a simple query
```

#### 3. Unnecessary Embedding Regeneration

```text
Document unchanged since last week
But embedding regenerated daily

7 API calls instead of 1
85% wasted spend
```

#### 4. Inefficient Context Windows

```text
Prompt: "Summarize: [10,000 word document]"

Tokens: 15,000 input + 500 output
Cost: $0.48 per request

With chunking + map-reduce:
Tokens: 4,000 input + 500 output (processed efficiently)
Cost: $0.14 per request

70% savings
```

---

## Cost Optimization Strategies

### Strategy 1: Semantic Response Caching

Cache LLM responses for semantically similar queries:

```text
Query Flow:
                         ┌─────────────────┐
User Query ─────────────▶│ Semantic Cache  │
                         │                 │
                         │ 1. Embed query  │
                         │ 2. Search cache │
                         │    (cosine sim) │
                         │                 │
                         └────────┬────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
               Cache Hit                   Cache Miss
               (sim > 0.95)                (sim < 0.95)
                    │                           │
                    ▼                           ▼
             Return cached               Call LLM
             response                    Cache response
             Cost: $0                    Cost: $0.03
```

**Implementation**:

```yaml
# Enable semantic caching for AI inference
runtime:
  ai_cache:
    enabled: true
    similarity_threshold: 0.95
    ttl: 24h
    
models:
  - name: gpt-4
    from: openai
    params:
      cache_enabled: true
```

### Strategy 2: Intelligent Model Routing

Route queries to the cheapest capable model:

```text
                         ┌─────────────────┐
                         │  Model Router   │
                         │                 │
User Query ─────────────▶│ Analyze query:  │
                         │ • Complexity    │
                         │ • Domain        │
                         │ • Required cap. │
                         └────────┬────────┘
                                  │
          ┌───────────────────────┼───────────────────────┐
          │                       │                       │
     Simple Query           Medium Query            Complex Query
   "What is X?"          "Compare A and B"       "Analyze this..."
          │                       │                       │
          ▼                       ▼                       ▼
    ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
    │ Llama 3     │        │ GPT-4o-mini │        │   GPT-4     │
    │ $0.0001     │        │  $0.0015    │        │   $0.03     │
    └─────────────┘        └─────────────┘        └─────────────┘
```

**Implementation**:

```yaml
models:
  # Cheap, fast model for simple queries
  - name: simple_model
    from: ollama:llama3:8b
    
  # Mid-tier for most queries
  - name: standard_model
    from: openai
    params:
      model: gpt-4o-mini
      
  # Premium for complex reasoning
  - name: complex_model
    from: openai
    params:
      model: gpt-4
```

```sql
-- Route based on query complexity
SELECT 
    CASE 
        WHEN LENGTH(question) < 50 
        THEN ai(question, 'simple_model')
        
        WHEN question LIKE '%analyze%' OR question LIKE '%compare%'
        THEN ai(question, 'complex_model')
        
        ELSE ai(question, 'standard_model')
    END as response
FROM queries;
```

### Strategy 3: Data Acceleration

Reduce compute by caching data locally:

```text
Without Acceleration:
  Query → Network → Remote DB → Process → Network → Result
         [50ms]     [200ms]     [100ms]    [50ms]
         
  Total: 400ms, plus remote compute costs

With Acceleration:
  Query → Local DuckDB → Result
          [5ms]
          
  Total: 5ms, zero remote costs
  
  80x faster, eliminates remote compute charges
```

**Implementation**:

```yaml
datasets:
  - name: orders
    from: snowflake:analytics.orders
    acceleration:
      enabled: true
      engine: duckdb
      mode: memory
      refresh_check_interval: 5m
```

### Strategy 4: Incremental Embedding Updates

Only embed what changes:

```text
Daily Full Rebuild:
  1M documents × $0.0001/embed = $100/day = $3,000/month

Incremental Updates:
  10K changed docs × $0.0001/embed = $1/day = $30/month

99% cost reduction
```

**Implementation**:

```yaml
datasets:
  - name: documents
    from: postgres:docs
    acceleration:
      enabled: true
      refresh_mode: changes  # Only process changes
    embeddings:
      - column: content
        use: openai_embeddings
        params:
          # Track content changes
          change_detection: content_hash
```

### Strategy 5: Prompt Optimization

Reduce token usage through efficient prompts:

```text
Before (Verbose):
──────────────────────────────────────────────────────────────────
You are a helpful AI assistant. Your task is to analyze the 
following customer support ticket and determine the appropriate
category for routing. Please consider all aspects of the request
and provide your analysis in a structured format. The categories
available are: billing, technical, general, sales. Think step by
step and explain your reasoning.

Ticket: {ticket_content}

Please provide your response:
──────────────────────────────────────────────────────────────────
Tokens: ~150 + ticket

After (Optimized):
──────────────────────────────────────────────────────────────────
Categorize as billing/technical/general/sales:
{ticket_content}
Category:
──────────────────────────────────────────────────────────────────
Tokens: ~15 + ticket

90% prompt overhead reduction
```

### Strategy 6: Batch Processing

Aggregate requests for efficiency:

```text
Individual Requests:
  100 queries × API overhead = 100 API calls
  Latency: ~500ms each
  
Batched Requests:
  100 queries in 1 batch = 1 API call
  Latency: ~2s total
  
  Lower per-request cost, API overhead amortized
```

---

## Implementation with Spice

### Complete Cost-Optimized Configuration

```yaml
# spicepod.yaml - Cost-optimized AI infrastructure
version: v1
kind: Spicepod
name: cost-optimized-ai

# Data layer - accelerate to reduce remote costs
datasets:
  - name: customer_data
    from: snowflake:crm.customers
    acceleration:
      enabled: true
      engine: duckdb
      mode: file  # Persist to disk
      refresh_check_interval: 15m
      
  - name: knowledge_base
    from: s3://docs/
    acceleration:
      enabled: true
      refresh_mode: changes  # Incremental updates
    embeddings:
      - column: content
        use: efficient_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512  # Optimal chunk size

# Embedding models - cost-optimized
embeddings:
  - name: efficient_embeddings
    from: openai
    params:
      model: text-embedding-3-small  # Cheaper than ada-002
      
  - name: local_embeddings
    from: huggingface:sentence-transformers/all-MiniLM-L6-v2
    # Free, runs locally

# LLM models - tiered by cost
models:
  # Tier 1: Free local model
  - name: local_llm
    from: ollama:llama3:8b
    
  # Tier 2: Cheap API model  
  - name: cheap_llm
    from: openai
    params:
      model: gpt-4o-mini
      
  # Tier 3: Premium model (use sparingly)
  - name: premium_llm
    from: openai
    params:
      model: gpt-4

# Enable result caching
runtime:
  results_cache:
    enabled: true
    cache_max_size: 128MiB
    item_ttl: 1h
```

### Cost-Aware Query Patterns

#### Pattern 1: Tiered Inference

```sql
-- Start cheap, escalate if needed
WITH cheap_response AS (
    SELECT ai(
        'Quick answer: ' || question,
        'local_llm'
    ) as answer
    FROM questions
),
evaluated AS (
    SELECT 
        question,
        answer,
        -- Check if response is confident
        CASE 
            WHEN answer LIKE '%I don''t know%' 
              OR answer LIKE '%I''m not sure%'
            THEN 'low'
            ELSE 'high'
        END as confidence
    FROM cheap_response
)
SELECT 
    question,
    CASE 
        WHEN confidence = 'high' THEN answer
        ELSE ai(question, 'premium_llm')  -- Escalate
    END as final_answer
FROM evaluated;
```

#### Pattern 2: Cached Semantic Search

```sql
-- Results are cached automatically
SELECT 
    vs.content,
    vs.score
FROM vector_search(
    knowledge_base, 
    'How do I reset my password?',  -- Common query
    5
) vs;
-- First call: computes embeddings, searches
-- Subsequent similar queries: returns cached
```

#### Pattern 3: Batch Embedding Generation

```sql
-- Process in batches during off-peak hours
INSERT INTO document_embeddings
SELECT 
    id,
    content,
    embed(content, 'efficient_embeddings') as embedding
FROM new_documents
WHERE created_at > NOW() - INTERVAL '1 hour';
-- Batch API call, not individual requests
```

---

## Measuring Cost Efficiency

### Key Metrics

| Metric              | Description                      | Target     |
| ------------------- | -------------------------------- | ---------- |
| Cache Hit Rate      | % of queries served from cache   | > 60%      |
| Cheap Model %       | % of requests to low-cost models | > 80%      |
| Tokens/Query        | Average tokens per user query    | Decreasing |
| Cost/Query          | Average $ per query              | < $0.01    |
| Embedding Freshness | Age of oldest embedding          | Acceptable |

### Cost Dashboard Query

```sql
-- Track AI cost metrics
SELECT 
    DATE_TRUNC('day', timestamp) as day,
    
    -- Cache metrics
    SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END)::FLOAT / 
        COUNT(*) * 100 as cache_hit_rate,
    
    -- Model usage
    SUM(CASE WHEN model = 'local_llm' THEN 1 ELSE 0 END) as local_calls,
    SUM(CASE WHEN model = 'cheap_llm' THEN 1 ELSE 0 END) as cheap_calls,
    SUM(CASE WHEN model = 'premium_llm' THEN 1 ELSE 0 END) as premium_calls,
    
    -- Cost
    SUM(tokens_used * cost_per_token) as total_cost,
    AVG(tokens_used * cost_per_token) as avg_cost_per_query
    
FROM ai_request_log
GROUP BY 1
ORDER BY 1 DESC;
```

---

## Case Studies

### Case Study 1: Customer Support Bot

**Before optimization**:

- All queries to GPT-4
- No caching
- Full document context every request
- Monthly cost: $15,000

**After optimization**:

- Semantic caching: 65% hit rate
- Model routing: 70% to local, 25% to cheap, 5% to premium
- Chunk-based context
- Monthly cost: $2,500

Savings: 83%

### Case Study 2: E-Commerce Search

**Before optimization**:

- Real-time embedding generation
- Vector search on every query
- Premium embeddings model
- Monthly cost: $8,000

**After optimization**:

- Pre-computed embeddings with CDC updates
- Results caching for popular queries
- Cheaper embedding model (comparable quality)
- Monthly cost: $1,200

Savings: 85%

### Case Study 3: Document Analysis Platform

**Before optimization**:

- GPT-4 for all analysis
- Full document in each prompt
- No batching
- Monthly cost: $25,000

**After optimization**:

- Map-reduce with cheap model, combine with premium
- Chunked processing
- Batch overnight processing for non-urgent
- Monthly cost: $6,000

Savings: 76%

---

## Getting Started

### Step 1: Audit Current Costs

```sql
-- What's driving costs?
SELECT 
    model,
    COUNT(*) as requests,
    SUM(input_tokens + output_tokens) as total_tokens,
    AVG(input_tokens) as avg_input_tokens,
    SUM(cost) as total_cost
FROM ai_requests
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY model
ORDER BY total_cost DESC;
```

### Step 2: Enable Quick Wins

```yaml
# Immediate cost reduction
runtime:
  results_cache:
    enabled: true
    item_ttl: 1h
```

### Step 3: Add Model Tiers

```yaml
models:
  - name: default
    from: ollama:llama3:8b  # Local, free
    
  - name: fallback
    from: openai
    params:
      model: gpt-4o-mini  # Cheap
```

### Step 4: Accelerate Data

```yaml
datasets:
  - name: hot_data
    from: snowflake:prod.data
    acceleration:
      enabled: true
      engine: duckdb
```

### Step 5: Monitor and Iterate

```bash
# Track cost metrics
spice sql

sql> SELECT * FROM runtime.metrics 
     WHERE name LIKE 'ai_%' OR name LIKE 'cache_%';
```

---

## Conclusion

AI cost optimization isn't about doing less—it's about doing more with less:

| Strategy               | Typical Savings |
| ---------------------- | --------------- |
| Semantic caching       | 40-70%          |
| Model routing          | 50-80%          |
| Data acceleration      | 30-60%          |
| Incremental embeddings | 80-95%          |
| Prompt optimization    | 20-50%          |

**Combined**: 50-80% total cost reduction is achievable.

Start with the highest-impact changes:

1. Enable caching
2. Add a cheap model tier
3. Accelerate frequently-accessed data

Then iterate based on your cost metrics.

---

## Related Articles in This Series

- **[Caching Explained](caching-explained.md)**: Deep dive into caching strategies
- **[LLM Inference](llm-inference-explained.md)**: Building AI-native applications
- **[Data Acceleration](data-acceleration-explained.md)**: Materializing data for performance
- **[The Data Substrate](data-substrate-explained.md)**: Unified data foundation

---

## Further Reading

- [Spice Cost Optimization Guide](https://spiceai.org/docs/optimization)
- [OpenAI Pricing](https://openai.com/pricing)
- [LLM Cost Comparison Tools](https://artificial-analysis.com/)

