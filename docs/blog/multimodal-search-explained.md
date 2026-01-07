# Multi-Modal Search: Combining Text, Images, and Structured Data

*How to build search experiences that understand content across multiple modalities.*

> **Part of the Spice Use Case Series**: This article is one of our deep-dives exploring how Spice enables modern data and AI applications. See [all articles in this series](#related-articles-in-this-series).

---

## Table of Contents

1. [Introduction](#introduction)
2. [What is Multi-Modal Search](#what-is-multi-modal-search)
3. [Architecture Patterns](#architecture-patterns)
4. [Implementation Strategies](#implementation-strategies)
5. [Cross-Modal Retrieval](#cross-modal-retrieval)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Getting Started](#getting-started)
8. [Conclusion](#conclusion)

---

## Introduction

Modern content is multi-modal. Users expect to search across:

| Modality   | Examples                          |
| ---------- | --------------------------------- |
| Text       | Documents, articles, chat logs    |
| Images     | Photos, diagrams, screenshots     |
| Audio      | Podcasts, call recordings, music  |
| Video      | Tutorials, meetings, clips        |
| Structured | Databases, spreadsheets, metadata |

Traditional search handles one modality well. **Multi-modal search** unifies them.

```text
Query: "red dress under $100"

Single-modal (text only):
→ Searches product descriptions
→ Misses products with poor descriptions but matching images

Multi-modal:
→ Searches text: descriptions, titles
→ Searches images: visual features
→ Filters structured: price < $100
→ Combines results for comprehensive coverage
```

---

## What is Multi-Modal Search

### The Multi-Modal Spectrum

```text
Level 1: Metadata Search
────────────────────────────────────────
Search structured metadata about multi-modal content

Query: "Find images from last week tagged 'product'"
Implementation: SQL on metadata table

Level 2: Content-Based Single-Modal
────────────────────────────────────────
Search within each modality separately

Query: "Find images similar to this one"
Implementation: Image embeddings + vector search

Level 3: Cross-Modal Retrieval
────────────────────────────────────────
Use one modality to find another

Query: "Find images of a sunset over mountains" (text→image)
Implementation: CLIP or similar cross-modal embeddings

Level 4: Unified Multi-Modal
────────────────────────────────────────
Seamlessly search across all modalities

Query: "Products with red color, good reviews, matching this style"
Implementation: Unified embedding space + structured filters
```

### Enabling Technologies

| Technology       | Purpose                    | Examples              |
| ---------------- | -------------------------- | --------------------- |
| CLIP             | Text↔Image embeddings      | OpenAI CLIP, OpenCLIP |
| CLAP             | Text↔Audio embeddings      | LAION CLAP            |
| ImageBind        | Universal embeddings       | Meta ImageBind        |
| Multi-modal LLMs | Understanding + generation | GPT-4V, Claude 3      |
| Vector databases | Similarity search          | Spice, Pinecone       |

---

## Architecture Patterns

### Pattern 1: Parallel Search Aggregation

Search each modality, combine results:

```text
                          User Query
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
           ▼                  ▼                  ▼
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │ Text Search │    │Image Search │    │Struct Query │
    │             │    │             │    │             │
    │  Semantic   │    │  Visual     │    │   SQL       │
    │  Embedding  │    │  Embedding  │    │   Filters   │
    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
           │                  │                  │
           └──────────────────┼──────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │    Fusion       │
                    │                 │
                    │ • Score merge   │
                    │ • Reranking     │
                    │ • Filtering     │
                    └────────┬────────┘
                             │
                             ▼
                       Final Results
```

**Implementation**:

```python
async def multi_modal_search(query: str, filters: dict):
    # Parallel search across modalities
    text_results, image_results, struct_results = await asyncio.gather(
        text_search(query),
        image_search(query),  # Uses CLIP text encoder
        structured_query(query, filters)
    )
    
    # Fusion
    combined = merge_results(text_results, image_results)
    filtered = apply_filters(combined, struct_results)
    reranked = rerank(filtered, query)
    
    return reranked
```

### Pattern 2: Unified Embedding Space

Single embedding space for all modalities:

```text
                    Unified Embedding Space
    ┌─────────────────────────────────────────────────┐
    │                                                  │
    │    Text Embedding ────► [0.1, 0.3, ..., 0.2]    │
    │                              ▲                   │
    │                              │ Similar           │
    │                              ▼                   │
    │   Image Embedding ────► [0.12, 0.28, ..., 0.19] │
    │                              ▲                   │
    │                              │ Similar           │
    │                              ▼                   │
    │   Audio Embedding ────► [0.11, 0.31, ..., 0.21] │
    │                                                  │
    └─────────────────────────────────────────────────┘

Query in any modality → Find similar in any modality
```

**Implementation**:

```yaml
# Configure unified embedding model
embeddings:
  - name: multimodal_embeddings
    from: clip
    params:
      model: ViT-B-32

datasets:
  - name: products
    from: postgres:catalog.products
    embeddings:
      - column: description
        use: multimodal_embeddings
      - column: image_url
        use: multimodal_embeddings
        modality: image
```

### Pattern 3: Late Fusion with Reranking

Search separately, rerank with multi-modal understanding:

```text
Stage 1: Retrieval (fast, broad)
┌─────────────────────────────────────────────────────────────────┐
│  Text Retrieval ──────────► Top 100 text matches               │
│  Image Retrieval ─────────► Top 100 image matches              │
│  Structured Query ────────► Pre-filtered candidates            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Stage 2: Reranking (slower, precise)
┌─────────────────────────────────────────────────────────────────┐
│  Multi-modal LLM evaluates each candidate:                      │
│  "How well does this product match: 'red summer dress'?"       │
│  Considers: image, description, reviews, attributes            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                        Top 20 Results
```

---

## Implementation Strategies

### Strategy 1: Text + Image Search

Common e-commerce pattern:

```yaml
# spicepod.yaml
version: v1
kind: Spicepod
name: product-search

embeddings:
  - name: text_embeddings
    from: openai
    params:
      model: text-embedding-3-small
      
  - name: clip_embeddings
    from: clip
    params:
      model: ViT-B-32

datasets:
  - name: products
    from: postgres:catalog.products
    embeddings:
      # Text embeddings for descriptions
      - column: description
        use: text_embeddings
      # CLIP embeddings for images (text can query these)
      - column: image_embedding
        use: clip_embeddings
    acceleration:
      enabled: true
```

```sql
-- Multi-modal product search
WITH text_matches AS (
    SELECT product_id, score as text_score
    FROM vector_search(products.description, 'red summer dress', 50)
),
image_matches AS (
    SELECT product_id, score as image_score
    FROM vector_search(products.image_embedding, 'red summer dress', 50)
)
SELECT 
    p.*,
    COALESCE(t.text_score, 0) * 0.4 + 
    COALESCE(i.image_score, 0) * 0.6 as combined_score
FROM products p
LEFT JOIN text_matches t ON p.id = t.product_id
LEFT JOIN image_matches i ON p.id = i.product_id
WHERE t.product_id IS NOT NULL OR i.product_id IS NOT NULL
ORDER BY combined_score DESC
LIMIT 20;
```

### Strategy 2: Document + Figure Search

Technical documentation with embedded images:

```yaml
datasets:
  - name: documentation
    from: s3://docs/
    embeddings:
      - column: content
        use: text_embeddings
        chunking:
          enabled: true
          target_chunk_size: 512
    acceleration:
      enabled: true
      
  - name: figures
    from: s3://docs/figures/
    embeddings:
      - column: caption
        use: text_embeddings
      - column: image
        use: clip_embeddings
    acceleration:
      enabled: true
```

```sql
-- Find documentation and related figures
WITH doc_chunks AS (
    SELECT * FROM vector_search(documentation, $query, 10)
),
related_figures AS (
    SELECT DISTINCT f.*
    FROM figures f
    JOIN doc_chunks d ON f.document_id = d.document_id
    WHERE f.caption_score > 0.7 
       OR vector_search(f.image, $query, 5).score > 0.8
)
SELECT 
    d.content,
    d.section,
    array_agg(f.image_url) as figures
FROM doc_chunks d
LEFT JOIN related_figures f ON f.document_id = d.document_id
GROUP BY d.content, d.section;
```

### Strategy 3: Video Search

Search video content via transcripts and keyframes:

```yaml
datasets:
  # Video transcripts (temporal text)
  - name: video_transcripts
    from: postgres:media.transcripts
    columns:
      - name: video_id
      - name: timestamp
      - name: text
    embeddings:
      - column: text
        use: text_embeddings
        
  # Video keyframes (sampled images)
  - name: video_keyframes
    from: s3://media/keyframes/
    embeddings:
      - column: image
        use: clip_embeddings
```

```sql
-- Find video moments matching query
WITH transcript_matches AS (
    SELECT video_id, timestamp, score
    FROM vector_search(video_transcripts, 'explain neural networks', 20)
),
visual_matches AS (
    SELECT video_id, timestamp, score
    FROM vector_search(video_keyframes, 'neural network diagram', 20)
)
SELECT 
    v.video_id,
    v.title,
    COALESCE(t.timestamp, k.timestamp) as moment_timestamp,
    t.score as transcript_score,
    k.score as visual_score
FROM videos v
LEFT JOIN transcript_matches t ON v.id = t.video_id
LEFT JOIN visual_matches k ON v.id = k.video_id
WHERE t.video_id IS NOT NULL OR k.video_id IS NOT NULL
ORDER BY COALESCE(t.score, 0) + COALESCE(k.score, 0) DESC;
```

---

## Cross-Modal Retrieval

### Text-to-Image

Find images from text descriptions:

```python
# Using CLIP for text-to-image search
from transformers import CLIPProcessor, CLIPModel

model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

# Encode text query
text_query = "a cat sitting on a windowsill"
inputs = processor(text=[text_query], return_tensors="pt", padding=True)
text_embedding = model.get_text_features(**inputs)

# Search image embeddings with text query
results = await client.query(f"""
    SELECT image_url, score
    FROM vector_search(images, '{text_embedding.tolist()}', 10)
""")
```

### Image-to-Text

Find text content from image queries:

```python
# Encode query image
image = Image.open("query_image.jpg")
inputs = processor(images=image, return_tensors="pt")
image_embedding = model.get_image_features(**inputs)

# Search text documents with image embedding
results = await client.query(f"""
    SELECT document_id, content, score
    FROM vector_search(documents, '{image_embedding.tolist()}', 10)
""")
```

### Image-to-Image

Visual similarity search:

```sql
-- Find visually similar products
SELECT 
    p.id,
    p.name,
    p.image_url,
    vs.score
FROM vector_search(
    products.image_embedding,
    (SELECT image_embedding FROM products WHERE id = $reference_id),
    20
) vs
JOIN products p ON vs.id = p.id
WHERE p.id != $reference_id
ORDER BY vs.score DESC;
```

---

## Real-World Use Cases

### Use Case 1: E-Commerce Visual Search

```text
User uploads photo of a dress they like
                    │
                    ▼
         ┌──────────────────┐
         │ Visual Encoding  │
         │   (CLIP/DINO)    │
         └────────┬─────────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
    ▼             ▼             ▼
┌────────┐  ┌──────────┐  ┌──────────┐
│Similar │  │ Style    │  │ Color    │
│Products│  │ Matching │  │ Matching │
└───┬────┘  └────┬─────┘  └────┬─────┘
    │            │             │
    └────────────┴──────┬──────┘
                        │
                        ▼
              "Shop the Look" Results
```

### Use Case 2: Medical Image Search

```yaml
# Medical imaging with reports
datasets:
  - name: radiology_images
    from: s3://medical/xrays/
    embeddings:
      - column: image
        use: medical_clip  # Specialized medical CLIP
        
  - name: radiology_reports
    from: postgres:medical.reports
    embeddings:
      - column: findings
        use: biobert  # Medical text embeddings
```

```sql
-- Find similar cases with matching findings
SELECT 
    r.patient_id,
    r.findings,
    r.diagnosis,
    i.image_url,
    vs.score
FROM vector_search(radiology_images, $query_image, 20) vs
JOIN radiology_images i ON vs.id = i.id
JOIN radiology_reports r ON i.study_id = r.study_id
WHERE r.modality = 'chest_xray'
ORDER BY vs.score DESC;
```

### Use Case 3: Content Moderation

```yaml
# Multi-modal content moderation
datasets:
  - name: user_posts
    from: postgres:social.posts
    embeddings:
      - column: text_content
        use: text_embeddings
      - column: image_content
        use: clip_embeddings

models:
  - name: moderator
    from: openai
    params:
      model: gpt-4-vision-preview
```

```python
async def moderate_post(post_id: str):
    post = await get_post(post_id)
    
    # Check text against known violations
    text_matches = await vector_search(
        violation_patterns,
        post.text_content,
        limit=5,
        threshold=0.85
    )
    
    # Check image against known violations
    image_matches = await vector_search(
        violation_images,
        post.image_content,
        limit=5,
        threshold=0.9
    )
    
    # If suspicious, use multi-modal LLM for final decision
    if text_matches or image_matches:
        decision = await ai_moderate(
            image=post.image_content,
            text=post.text_content,
            context=f"Suspicious matches: {text_matches + image_matches}"
        )
        return decision
    
    return {"action": "approve"}
```

---

## Getting Started

### Step 1: Choose Your Embedding Models

| Use Case        | Text Model             | Image Model   |
| --------------- | ---------------------- | ------------- |
| General         | text-embedding-3-small | CLIP ViT-B/32 |
| E-commerce      | E5-large               | Fashion-CLIP  |
| Medical         | BioBERT                | MedCLIP       |
| Code + Diagrams | CodeBERT               | CLIP          |

### Step 2: Configure Spice

```yaml
version: v1
kind: Spicepod
name: multimodal-search

embeddings:
  - name: text_embeddings
    from: openai
    
  - name: clip_embeddings
    from: clip

datasets:
  - name: content
    from: postgres:app.content
    embeddings:
      - column: text
        use: text_embeddings
      - column: image_url
        use: clip_embeddings
        modality: image
```

### Step 3: Build Search Queries

```sql
-- Start simple
SELECT * FROM vector_search(content.text, 'your query', 10);

-- Add multi-modal
SELECT * FROM (
    SELECT id, 'text' as source, score 
    FROM vector_search(content.text, 'your query', 10)
    UNION ALL
    SELECT id, 'image' as source, score
    FROM vector_search(content.image_url, 'your query', 10)
) combined
ORDER BY score DESC;
```

### Step 4: Iterate and Refine

```python
# Evaluate result quality
def evaluate_results(query, results, relevance_judgments):
    # Calculate precision, recall, NDCG
    pass

# A/B test fusion strategies
def compare_fusion_strategies(queries):
    strategies = [
        lambda t, i: 0.5*t + 0.5*i,  # Equal weight
        lambda t, i: 0.7*t + 0.3*i,  # Text-heavy
        lambda t, i: max(t, i),      # Max fusion
    ]
    # Evaluate each
```

---

## Conclusion

Multi-modal search enables richer discovery experiences:

| Capability       | Single-Modal  | Multi-Modal                |
| ---------------- | ------------- | -------------------------- |
| Query types      | Text OR image | Text AND image             |
| Content coverage | One modality  | All modalities             |
| User experience  | Limited       | Natural                    |
| Use cases        | Basic search  | Visual search, cross-modal |

Key implementation strategies:

1. **Parallel aggregation**: Search each modality, merge results
2. **Unified embeddings**: Single space for all modalities
3. **Late fusion**: Rerank with multi-modal understanding

Start with text + image, expand to other modalities as needed.

---

## Related Articles in This Series

- **[Application Search](application-search-explained.md)**: Search fundamentals
- **[Hybrid SQL Search](hybrid-sql-search-explained.md)**: Combining search types
- **[RAG Explained](rag-explained.md)**: Retrieval-augmented generation
- **[LLM Inference](llm-inference-explained.md)**: AI-native capabilities

---

## Further Reading

- [Spice Documentation](https://spiceai.org/docs)
- [CLIP Paper](https://arxiv.org/abs/2103.00020)
- [Multi-Modal Search Survey](https://arxiv.org/abs/2302.05713)

