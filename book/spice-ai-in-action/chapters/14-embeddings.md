# 14. Embeddings and semantic retrieval

A customer asks, “Can I send back unworn hiking footwear?” Northstar's policy says, “Unused trail boots may be returned within 30 days.” Exact word matching may miss the relationship. An embedding model maps text into a numerical representation that can make related meanings close under a chosen similarity measure. That gives retrieval another signal; it does not make the retrieved policy correct for every customer.

## 14.1 What an embedding represents

An embedding is a vector produced by a specific model and preprocessing pipeline. Its dimensions are learned coordinates, not named business attributes. Two texts can have similar vectors because the model learned related usage patterns. The score is not a probability that one text answers the other.

A vector only has meaning in its model's representation space. Do not mix vectors from different model versions merely because their dimensions match. Treat the model, revision or artifact digest, normalization, chunking, and relevant preprocessing as part of the index version.

Similarity measures also matter. Cosine similarity compares direction. Dot product is affected by magnitude unless vectors are normalized or the model's intended use accounts for it. Euclidean distance measures geometric separation. Use the metric supported and intended for the selected model–engine combination; changing it is an evaluation experiment.

## 14.2 Add an embedding component

The companion vector variant uses a public Model2Vec model:

```yaml
embeddings:
  - from: model2vec:minishlab/potion-base-8M
    name: policy_embed
```

This is a lab choice, not a claim that it is the best model for Northstar. The first load requires access to the model repository and downloads model assets. The model then runs locally through the supported runtime integration. Check model licensing and store the artifact identity in a reproducible deployment.

The dataset's text column refers to the named component:

```yaml
# Dataset fragment
columns:
  - name: body
    embeddings:
      - from: policy_embed
        row_id: [article_id]
```

`row_id` associates retrieval results with source rows. In the fixture, article IDs are globally unique. In production, choose identity that remains unique across tenants, versions, and any indexed partitions. A document title is usually not a safe key.

Start `spicepod.vectors.yaml` in an isolated lab directory or after stopping the preceding runtime. Wait for the model and dataset to be ready before querying. Model availability and data availability are separate startup conditions.

## 14.3 Query the representation

A representative SQL call is:

```sql
SELECT article_id, tenant_id, title, _score
FROM vector_search(articles, 'return unused hiking boots', body)
WHERE tenant_id = 'north'
ORDER BY _score DESC, article_id
LIMIT 3;
```

The tested source and binary expose the search score as `_score`. Some documentation examples use `score`; inspect the returned schema for the deployed version and use the field it actually provides. The book's full-text experiment captured the schema error from using `score` and the successful query using `_score`.

The desired article is 1, “Returning a trail boot.” Treat that as a relevance judgment, not as a promise that every embedding model will rank it first. Run the query, save IDs and scores, and compare several phrasings. The evidence appendix records the results of the authoring environment where available.

## 14.4 Chunking changes the retrieval unit

A short policy paragraph can be embedded as one row. A long handbook may need chunks. A chunk that is too large can mix topics and hide the relevant passage; one that is too small can omit qualifications needed to interpret it. Overlap can retain context across boundaries but also creates redundant candidates.

The column embedding configuration supports chunking on compatible paths. A representative fragment based on the source and cookbook is:

```yaml
embeddings:
  - from: policy_embed
    row_id: [article_id]
    chunking:
      enabled: true
      target_chunk_size: 256
      overlap_size: 64
      file_format: md
```

Verify the units and splitter behavior in the matching release before translating the target size into a token budget. The snippet demonstrates the configuration shape; the fixture's short paragraphs do not require chunking.

Retain document identity, chunk identity, source version, and offsets or another way to recover the cited passage. A user should be able to open the policy that supported the answer. If the document changes, a citation should identify whether it refers to the historical version or the current one.

## 14.5 Metadata is part of retrieval

Tenant, language, effective date, product family, and access classification are not decorations around the vector. They define which candidates are eligible. A highly similar policy for the wrong tenant is a bad result.

Apply authorization before exposing candidate text to the model, reranker, logs, or end user. Verify predicate placement and execution behavior for the selected search engine with plans and adversarial fixtures. An outer SQL predicate can be useful, but it is not a complete security proof of every internal data flow.

If strict isolation requires that an external reranker never receive another tenant's text, use an architecture that enforces that property before the external call. Separate datasets or indexes may be appropriate. Rechecking returned IDs is an additional guard, not permission to leak candidates earlier in the pipeline.

## 14.6 Exact and approximate search

An exact vector scan can evaluate every eligible vector under the chosen metric. An approximate index trades some retrieval completeness for an access strategy designed for scale. “Approximate” refers to finding nearest candidates, not to permission to return another tenant's rows or stale deleted documents.

Evaluate candidate recall against an exact or independently established reference on a representative sample. Measure it at the candidate depth used by later reranking. A fast top-five retrieval is not useful if the correct passage never enters the reranker's candidate pool.

Index choice also affects build time, update behavior, memory, and persistence. A model change can require rebuilding vectors and indexes. Plan the migration as a versioned data operation with a shadow evaluation and a rollback path.

## 14.7 Measure useful retrieval

Build a small judgment set before tuning. Include exact product names, paraphrases, abbreviations, ambiguous questions, unrelated questions, and questions whose answer exists only in another tenant. Label relevant article IDs and, where needed, the passage that actually supports the answer.

Track recall at K, reciprocal rank, and failure categories. A mean score across a mixed set can hide a disastrous tenant-isolation failure, so keep critical categories separate. When comparing models, hold the corpus, query set, filters, and evaluation method constant.

**Exercise.** Write ten questions about the six policies. Include two that should produce no supported answer and two that have different answers for `north` and `south`. Compare exact lexical matching and vector retrieval. Save ranked IDs, not merely a subjective impression of the answer.

**Further reading.** See [embeddings](https://spiceai.org/docs/features/embeddings), [Model2Vec](https://spiceai.org/docs/components/embeddings/model2vec), cookbook `models/openai/`, and the embedding definitions in `crates/spicepod/src/component/embeddings.rs`.
