# Hybrid SQL Search: Combining Vector, Full-Text, and Relational Filters

*How to perform multi-modal search with semantic understanding, keyword precision, and SQL filtering in unified queries.*

> **Part of the Spice Use Case Series**: This article is one of nine deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [The Limitations of Single-Modal Search](#the-limitations-of-single-modal-search)
3. [What is Hybrid Search?](#what-is-hybrid-search)
4. [How Spice Implements Hybrid SQL Search](#how-spice-implements-hybrid-sql-search)
5. [Ranking and Fusion Strategies](#ranking-and-fusion-strategies)
6. [Search Patterns](#search-patterns)
7. [Real-World Use Cases](#real-world-use-cases)
8. [Getting Started](#getting-started)
9. [Conclusion](#conclusion)

---

## Introduction

Modern applications require search that understands user intent *and* respects business constraints. A user searching for "comfortable office chair under $300 with lumbar support" expects results that:

1. Understand the semantic meaning of "comfortable" and "office chair"
2. Match the specific phrase "lumbar support"
3. Filter by the price constraint "under $300"

Traditional search systems force you to choose one approach. **Hybrid SQL Search** combines all three—vector similarity, full-text search, and relational filtering—in a single SQL query.

---

## The Science of Search Ranking

To understand hybrid search, we need to understand how different ranking algorithms work:

### BM25: The Foundation of Full-Text Search

BM25 (Best Matching 25) is the industry-standard ranking function used by Elasticsearch, Lucene, and most search engines:

```text
BM25(D, Q) = Σ IDF(qᵢ) × [f(qᵢ, D) × (k₁ + 1)] / [f(qᵢ, D) + k₁ × (1 - b + b × |D|/avgdl)]

Where:
- D = document
- Q = query terms
- f(qᵢ, D) = frequency of term qᵢ in document D
- |D| = document length
- avgdl = average document length
- k₁, b = tuning parameters (typically k₁=1.2, b=0.75)
- IDF = Inverse Document Frequency
```

**Key insights**:

- Rare terms (high IDF) contribute more to relevance
- Term frequency has diminishing returns (saturation)
- Longer documents are normalized to prevent bias

### Vector Similarity: Semantic Understanding

Vector search uses embedding models to encode text into dense numerical vectors:

```text
Text: "ergonomic office chair"     Text: "comfortable desk seating"
          ↓                                    ↓
   Embedding Model                      Embedding Model
          ↓                                    ↓
[0.23, -0.45, 0.82, ...]           [0.21, -0.43, 0.79, ...]
          \                                  /
           \        Cosine Similarity       /
            \            ↓                 /
             ──────→   0.94   ←───────────
                    (very similar!)
```

Common similarity metrics:

| Metric          | Formula             | Use Case                |
| --------------- | ------------------- | ----------------------- |
| **Cosine**      | `A·B / (‖A‖ × ‖B‖)` | Most common, normalized |
| **Dot Product** | `A·B`               | When magnitude matters  |
| **Euclidean**   | `‖A - B‖`           | Spatial relationships   |

### The Complementary Nature

Vector and keyword search have opposite strengths:

```text
Query: "comfortable chair"

BM25 finds:                          Vector search finds:
├── "comfortable chair" (exact)      ├── "ergonomic seating" (semantic)
├── "chair comfortable" (reordered)  ├── "cozy office furniture"
└── "comfortable office chair"       └── "relaxing desk chair"

     │                                      │
     └──────────────────┬───────────────────┘
                        ▼
                 Hybrid combines both!
```

### Reciprocal Rank Fusion (RRF)

RRF is the standard algorithm for combining ranked lists from different sources:

```text
RRF_score(d) = Σ 1 / (k + rank_r(d))

Where:
- d = document
- k = ranking constant (typically 60)
- rank_r(d) = rank of document d in ranker r
```

**Example**:

```text
Document X:
  - Rank 1 in vector search  → 1/(60+1) = 0.0164
  - Rank 5 in BM25 search    → 1/(60+5) = 0.0154
  - RRF Score                = 0.0318

Document Y:
  - Rank 10 in vector search → 1/(60+10) = 0.0143
  - Rank 2 in BM25 search    → 1/(60+2) = 0.0161  
  - RRF Score                = 0.0304

Final ranking: X > Y (even though Y was #2 in BM25)
```

RRF elegantly balances contributions without needing to normalize scores between different rankers.

---

## The Limitations of Single-Modal Search

### Vector Search Alone

Vector search excels at semantic understanding but struggles with precision:

```sql
-- Finds semantically similar products
SELECT * FROM vector_search(products, 'comfortable office seating', 20);
```

**Strengths**:

- Understands synonyms ("seating" → "chair")
- Captures semantic relationships
- Works across languages

**Weaknesses**:

- Can't filter by price, category, or stock
- Exact phrases get lost in embedding
- Numerical constraints ignored

### Full-Text Search Alone

Full-text search provides precision but misses semantic relationships:

```sql
-- BM25 ranking based on term frequency
SELECT * FROM text_search(products, 'office chair lumbar', 20);
```

**Strengths**:

- Precise phrase matching
- Proven BM25 relevance ranking
- Fast for known-item searches

**Weaknesses**:

- Misses synonyms ("ergonomic" doesn't match "comfortable")
- No understanding of intent
- Keyword variations cause misses

### SQL Filtering Alone

SQL filtering is exact but has no relevance ranking:

```sql
-- Exact match filtering
SELECT * FROM products 
WHERE category = 'furniture' AND price < 300;
```

**Strengths**:

- Precise constraint enforcement
- Familiar SQL semantics
- Supports complex logic

**Weaknesses**:

- No semantic understanding
- Results not ranked by relevance
- Requires exact matches

---

## What is Hybrid Search?

Hybrid search combines multiple search modalities to leverage their complementary strengths:

```text
                User Query
    "comfortable office chair under $300 with lumbar support"
                    │
    ┌───────────────┼───────────────┬───────────────┐
    │               │               │               │
    ▼               ▼               ▼               ▼
┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
│ Vector  │   │  Text   │   │  SQL    │   │ Facet   │
│ Search  │   │ Search  │   │ Filter  │   │ Filter  │
│         │   │         │   │         │   │         │
│semantic │   │ "lumbar │   │ price < │   │category │
│meaning  │   │support" │   │  300    │   │=office  │
└────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘
     │             │             │             │
     └─────────────┴──────┬──────┴─────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │   Fusion    │
                   │   (RRF)     │
                   └──────┬──────┘
                          │
                          ▼
                 Ranked Results
```

### The Hybrid Search Equation

```text
Final_Score = f(vector_score, text_score, filter_matches)
```

Where:

- `vector_score`: Semantic similarity (0-1)
- `text_score`: BM25 relevance score
- `filter_matches`: Boolean constraint satisfaction

---

## How Spice Implements Hybrid SQL Search

Spice provides SQL-native primitives for hybrid search:

### vector_search() UDTF

Find semantically similar content:

```sql
SELECT * FROM vector_search(table_name, 'query text', limit);
```

Returns:

- `id`: Row identifier
- `score`: Similarity score (higher = more similar)

**Example**:

```sql
SELECT 
    vs.id,
    vs.score,
    p.name,
    p.description
FROM vector_search(products, 'ergonomic home office furniture', 50) vs
JOIN products p ON vs.id = p.id
ORDER BY vs.score DESC;
```

### text_search() UDTF

Find keyword/phrase matches with BM25 ranking:

```sql
SELECT * FROM text_search(table_name, 'search terms', limit);
```

Returns:

- `id`: Row identifier  
- `score`: BM25 relevance score

**Example**:

```sql
SELECT 
    ts.id,
    ts.score,
    d.title,
    d.content
FROM text_search(documents, 'installation troubleshooting guide', 25) ts
JOIN documents d ON ts.id = d.id
ORDER BY ts.score DESC;
```

### SQL Filtering

Apply relational constraints with standard SQL:

```sql
SELECT *
FROM products
WHERE category = 'furniture'
  AND price BETWEEN 100 AND 500
  AND in_stock = true
  AND brand IN ('Herman Miller', 'Steelcase', 'Autonomous');
```

### Combining All Three

The real power comes from combination:

```sql
SELECT 
    p.id,
    p.name,
    p.price,
    p.description,
    vs.score as semantic_score,
    ts.score as keyword_score
FROM vector_search(products, 'comfortable ergonomic seating', 100) vs
JOIN text_search(products, 'lumbar support', 100) ts ON vs.id = ts.id
JOIN products p ON vs.id = p.id
WHERE p.price < 300
  AND p.category = 'office-furniture'
  AND p.in_stock = true
ORDER BY (vs.score + ts.score) / 2 DESC
LIMIT 20;
```

---

## Ranking and Fusion Strategies

### RRF in Spice SQL

RRF combines rankings from different search methods:

```text
RRF_score(d) = Σ 1/(k + rank_i(d))
```

Where `k` is typically 60 (balances high and low rankings).

**How Spice implements RRF internally:**

Spice's `ReciprocalRankFusion` struct in the search crate performs RRF aggregation when combining results from multiple `CandidateGeneration` sources. The algorithm:

1. **Collects candidate streams** from each search modality (vector, full-text)
2. **Assigns rankings** based on each stream's score ordering
3. **Computes RRF scores** using the formula with k=60
4. **Joins results** using the primary key to correlate across search methods
5. **Returns unified results** ordered by combined RRF score

```rust
// From Spice's ReciprocalRankFusion implementation
// score_a = 1 / (rank_i + 60) + 1 / (rank_j + 60) + ...
// The constant 60 prevents high-ranking items from dominating
```

**Manual RRF in SQL:**

```sql
WITH vector_ranked AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY score DESC) as v_rank
    FROM vector_search(products, 'comfortable office chair', 100)
),
text_ranked AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (ORDER BY score DESC) as t_rank
    FROM text_search(products, 'lumbar support ergonomic', 100)
)
SELECT 
    COALESCE(v.id, t.id) as id,
    1.0/(60 + COALESCE(v.v_rank, 1000)) + 
    1.0/(60 + COALESCE(t.t_rank, 1000)) as rrf_score
FROM vector_ranked v
FULL OUTER JOIN text_ranked t ON v.id = t.id
ORDER BY rrf_score DESC
LIMIT 20;
```

### Weighted Linear Combination

Simple score weighting:

```sql
SELECT 
    p.id,
    p.name,
    (0.6 * vs.score + 0.4 * ts.score) as combined_score
FROM vector_search(products, 'query', 100) vs
JOIN text_search(products, 'query', 100) ts ON vs.id = ts.id
JOIN products p ON vs.id = p.id
ORDER BY combined_score DESC;
```

Weights can be tuned:

- Higher vector weight: More semantic understanding
- Higher text weight: More keyword precision

### Cascaded Filtering

Filter first, then rank:

```sql
-- Step 1: Apply filters
WITH filtered AS (
    SELECT *
    FROM products
    WHERE price < 500
      AND category = 'electronics'
      AND in_stock = true
)
-- Step 2: Semantic search on filtered set
SELECT 
    f.*,
    vs.score
FROM vector_search(filtered, 'wireless noise canceling headphones', 20) vs
JOIN filtered f ON vs.id = f.id
ORDER BY vs.score DESC;
```

### Re-Ranking with Metadata Boost

Boost scores based on metadata:

```sql
SELECT 
    p.id,
    p.name,
    vs.score * (1 + 0.1 * p.review_score) as boosted_score
FROM vector_search(products, 'query', 50) vs
JOIN products p ON vs.id = p.id
WHERE p.review_score >= 4.0
ORDER BY boosted_score DESC;
```

---

## Search Patterns

### Pattern 1: Multi-Field Search

Search across multiple columns:

```yaml
datasets:
  - name: products
    from: postgres:catalog.products
    embeddings:
      - column: title
        use: openai_embeddings
      - column: description  
        use: openai_embeddings
    full_text_search:
      - column: title
      - column: description
      - column: brand
```

```sql
-- Search both title and description
WITH title_matches AS (
    SELECT id, score as title_score
    FROM vector_search(products_title_embedding, 'wireless earbuds', 50)
),
desc_matches AS (
    SELECT id, score as desc_score
    FROM vector_search(products_description_embedding, 'wireless earbuds', 50)
)
SELECT 
    p.*,
    COALESCE(t.title_score, 0) + COALESCE(d.desc_score, 0) as combined_score
FROM products p
LEFT JOIN title_matches t ON p.id = t.id
LEFT JOIN desc_matches d ON p.id = d.id
WHERE t.id IS NOT NULL OR d.id IS NOT NULL
ORDER BY combined_score DESC;
```

### Pattern 2: Faceted Search

Combine search with facet filtering:

```sql
-- Get results with facet counts
WITH search_results AS (
    SELECT id
    FROM vector_search(products, 'running shoes', 500)
)
SELECT 
    p.brand,
    p.color,
    p.price_range,
    COUNT(*) as count
FROM search_results sr
JOIN products p ON sr.id = p.id
GROUP BY p.brand, p.color, p.price_range
ORDER BY count DESC;
```

### Pattern 3: Similarity with Exclusions

Find similar items, excluding certain criteria:

```sql
-- Find similar products but different brand
SELECT 
    p.id,
    p.name,
    p.brand,
    vs.score
FROM vector_search(
    products, 
    (SELECT embedding FROM products WHERE id = 'reference-product-123'),
    50
) vs
JOIN products p ON vs.id = p.id
WHERE p.brand != (SELECT brand FROM products WHERE id = 'reference-product-123')
  AND p.id != 'reference-product-123'
ORDER BY vs.score DESC
LIMIT 10;
```

### Pattern 4: Time-Weighted Search

Weight recency in results:

```sql
SELECT 
    d.id,
    d.title,
    d.published_at,
    vs.score,
    -- Decay factor: newer content scores higher
    vs.score * EXP(-0.1 * EXTRACT(DAY FROM CURRENT_TIMESTAMP - d.published_at)) as time_weighted_score
FROM vector_search(documents, 'kubernetes deployment strategies', 50) vs
JOIN documents d ON vs.id = d.id
ORDER BY time_weighted_score DESC
LIMIT 20;
```

### Pattern 5: Query Expansion

Expand queries for better recall:

```sql
-- Original + expanded query
WITH original AS (
    SELECT id, score
    FROM vector_search(docs, 'ML model training', 30)
),
expanded AS (
    SELECT id, score
    FROM vector_search(docs, 'machine learning neural network deep learning', 30)
)
SELECT 
    COALESCE(o.id, e.id) as id,
    GREATEST(COALESCE(o.score, 0), COALESCE(e.score, 0)) as best_score
FROM original o
FULL OUTER JOIN expanded e ON o.id = e.id
ORDER BY best_score DESC
LIMIT 20;
```

---

## Real-World Use Cases

### E-Commerce Product Search

```yaml
datasets:
  - name: products
    from: postgres:catalog.products
    embeddings:
      - column: description
        use: openai_embeddings
```

```sql
-- "wireless headphones under $200 with noise canceling"
SELECT 
    p.id,
    p.name,
    p.price,
    p.brand,
    p.features,
    vs.score as relevance,
    ts.score as keyword_match
FROM vector_search(products, 'wireless headphones noise canceling', 100) vs
JOIN text_search(products, 'noise canceling wireless', 100) ts ON vs.id = ts.id
JOIN products p ON vs.id = p.id
WHERE p.price <= 200
  AND p.category = 'audio'
  AND p.in_stock = true
ORDER BY (vs.score * 0.6 + ts.score * 0.4) DESC
LIMIT 20;
```

### Support Ticket Resolution

```sql
-- Find similar resolved tickets
SELECT 
    t.id,
    t.subject,
    t.resolution,
    t.resolution_time_hours,
    vs.score as similarity
FROM vector_search(tickets, 'login page not loading after update', 20) vs
JOIN tickets t ON vs.id = t.id
WHERE t.status = 'resolved'
  AND t.resolution_time_hours < 4  -- Quick resolutions
  AND t.customer_satisfaction >= 4
ORDER BY vs.score DESC
LIMIT 5;
```

### Document Discovery

```sql
-- Find contracts with specific clauses
WITH semantic_matches AS (
    SELECT id, score as semantic_score
    FROM vector_search(contracts, 'indemnification liability limitation', 50)
),
keyword_matches AS (
    SELECT id, score as keyword_score
    FROM text_search(contracts, '"limitation of liability" OR "indemnification"', 50)
)
SELECT 
    c.id,
    c.title,
    c.contract_type,
    c.effective_date,
    s.semantic_score,
    k.keyword_score,
    1.0/(60 + ROW_NUMBER() OVER (ORDER BY s.semantic_score DESC)) +
    1.0/(60 + ROW_NUMBER() OVER (ORDER BY k.keyword_score DESC)) as rrf_score
FROM semantic_matches s
JOIN keyword_matches k ON s.id = k.id
JOIN contracts c ON s.id = c.id
WHERE c.status = 'active'
  AND c.contract_type IN ('SaaS', 'Enterprise')
ORDER BY rrf_score DESC;
```

### Content Recommendations

```sql
-- Recommend articles based on reading history
WITH user_interests AS (
    -- Get embedding centroid of user's read articles
    SELECT AVG(embedding) as interest_vector
    FROM articles a
    JOIN reading_history rh ON a.id = rh.article_id
    WHERE rh.user_id = 'user-123'
      AND rh.read_at > CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    a.id,
    a.title,
    a.category,
    a.published_at,
    vs.score as relevance
FROM vector_search(
    articles, 
    (SELECT interest_vector FROM user_interests), 
    50
) vs
JOIN articles a ON vs.id = a.id
WHERE a.id NOT IN (
    SELECT article_id FROM reading_history WHERE user_id = 'user-123'
)
  AND a.published_at > CURRENT_DATE - INTERVAL '7 days'
ORDER BY vs.score DESC
LIMIT 10;
```

---

## Getting Started

### 1. Configure Embeddings

```yaml
# spicepod.yaml
embeddings:
  - name: openai_embeddings
    from: openai
    params:
      model: text-embedding-3-small
      openai_api_key: ${secrets:OPENAI_API_KEY}
```

### 2. Enable Dataset Embeddings and Search

```yaml
datasets:
  - name: products
    from: postgres:catalog.products
    embeddings:
      - column: description
        use: openai_embeddings
    acceleration:
      enabled: true
      engine: duckdb
```

### 3. Start Searching

```sql
-- Simple hybrid search
SELECT 
    p.*,
    vs.score
FROM vector_search(products, 'ergonomic office chair', 20) vs
JOIN products p ON vs.id = p.id
WHERE p.price < 500
ORDER BY vs.score DESC;
```

### 4. Add Full-Text Search

```sql
-- Combine with text search
SELECT 
    p.*,
    vs.score as vector_score,
    ts.score as text_score
FROM vector_search(products, 'ergonomic office chair', 50) vs
JOIN text_search(products, 'lumbar support', 50) ts ON vs.id = ts.id
JOIN products p ON vs.id = p.id
WHERE p.price < 500
ORDER BY (vs.score + ts.score) DESC
LIMIT 10;
```

---

## Conclusion

Hybrid SQL Search combines the best of all search modalities:

| Modality         | Provides               | Example                          |
| ---------------- | ---------------------- | -------------------------------- |
| Vector Search    | Semantic understanding | "comfortable" → ergonomic chairs |
| Full-Text Search | Keyword precision      | "lumbar support" exact match     |
| SQL Filtering    | Business constraints   | price < $300, in_stock = true    |

Spice makes this accessible through familiar SQL with:

- `vector_search()` for semantic similarity
- `text_search()` for BM25 ranking
- Standard SQL `WHERE` clauses for filtering
- Fusion strategies (RRF, weighted combination) for ranking

The result: search that understands users *and* respects your business logic.

---

## Related Articles in This Series

- **[Application Search](application-search-explained.md)**: Overview of search patterns for applications
- **[RAG (Retrieval-Augmented Generation)](rag-explained.md)**: Using hybrid search for RAG context retrieval
- **[LLM Inference](llm-inference-explained.md)**: Enriching search results with AI-generated insights
- **[SQL Federation](sql-federation-explained.md)**: Searching across federated data sources

---

## Further Reading

- [Search Documentation](https://spiceai.org/docs/features/search)
- [Amazon S3 Vectors Cookbook](https://github.com/spiceai/cookbook/tree/trunk/vectors/s3/README.md)
- [Hybrid Search Tutorial](https://spiceai.org/docs/tutorials/hybrid-search)
- [Embedding Models Reference](https://spiceai.org/docs/components/embeddings)

