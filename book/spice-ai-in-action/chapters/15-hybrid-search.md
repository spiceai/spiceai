# 15. Full-text, hybrid search, and reranking

Semantic retrieval is useful for paraphrases. Exact terminology still matters: a product code, a policy number, or a named error may be the strongest clue in a question. A robust search system evaluates lexical and semantic signals together and retains enough evidence to understand why a result appeared.

## 15.1 Build a full-text baseline

The companion `spicepod.search.yaml` adds a full-text index to article bodies:

```yaml
# The articles dataset's column configuration
columns:
  - name: body
    full_text_search:
      enabled: true
      row_id: [article_id]
```

The final companion variant also indexes `title`, so the Search API can retrieve a policy whose topic appears only in its heading. The explicit SQL below selects the body index. The dataset is accelerated with Arrow in the lab. The full-text path uses the built-in integration available in the tested binary. Its configuration does not require an external search service or a paid model API.

Start the variant and run:

```sql
SELECT article_id, tenant_id, title
FROM text_search(articles, 'shipping', body)
WHERE tenant_id = 'north'
ORDER BY _score DESC, article_id
LIMIT 5;
```

Observed output on the development binary used for the search check:

```json
[{"article_id":3,"tenant_id":"north","title":"Shipping delays"}]
```

Article 6 has the same title for `south`, with a different policy. Keeping both in the fixture makes tenant scope visible. The result proves this particular request returned article 3; it does not by itself prove every possible authorization path.

## 15.2 Understand the lexical signal

Full-text systems tokenize text and rank matching documents using their configured analysis and scoring. BM25 is a common lexical relevance model. Its score depends on term frequency, document length, and corpus statistics; it is not on the same scale as a cosine-similarity score.

Tokenization affects exact identifiers. Test hyphens, punctuation, mixed case, and product codes from the actual domain. A tokenizer that works well for prose may split `TRAIL-BOOT` in a way that needs a separate exact-match field or query path. Preserve a lexical baseline when introducing embeddings so regressions are visible.

Different languages can need different analysis. Evaluate the supported tokenization and model behavior on the language distribution you actually serve. Translating every document into one language is a separate data pipeline with its own fidelity and provenance questions.

## 15.3 Candidate generation precedes final ranking

Search commonly has two stages: retrieve a candidate pool, then choose a final small set. The number of final results and the number of candidates are separate parameters. If the correct passage is absent from the pool, no reranker can recover it.

Filters can interact with candidate limits. If a system finds global top-K results and filters afterward, a tenant may receive too few results even though relevant eligible documents exist. A system that filters before ranking can search the eligible set directly. Verify the behavior of the selected engine and query path using a fixture designed to distinguish the two, together with the plan.

For a conclusive filter experiment, put many highly matching documents in an unauthorized tenant and a relevant document in the authorized tenant. Request a small candidate count and inspect what is scored, returned, and sent to any downstream provider. Ordinary balanced data will not expose this boundary.

## 15.4 Fuse ranks instead of incompatible raw scores

Reciprocal rank fusion combines ranked lists. A common form adds `1 / (k + rank)` for each list in which a document appears. The constant controls how sharply early positions dominate. The method avoids pretending that BM25 and vector scores are directly comparable.

The inspected Spice source provides an `rrf` SQL table function. A representative integration query is:

```sql
SELECT article_id, tenant_id, title, _fused_score
FROM rrf(
  vector_search(articles, 'return unused hiking boots', body),
  text_search(articles, 'trail boots return', body),
  join_key => 'article_id',
  k => 60.0
)
WHERE tenant_id = 'north'
ORDER BY _fused_score DESC, article_id
LIMIT 3;
```

The source names the fused output `_fused_score`; verify the deployed schema if a documentation example uses another name. Use a stable join key. Title-based fusion can merge unrelated policies with identical headings, including the two shipping documents in our fixture.

RRF is a retrieval method to evaluate, not a guarantee of improvement. A strong exact-match query can already be excellent. A poor second signal can add noise. Keep the lexical, vector, and fused rankings with each evaluation result.

## 15.5 Reranking spends work on a smaller set

A reranker evaluates candidate–query relevance more directly, often with a model that reads both together. It can improve ordering when the initial signals are broad. It also adds model capacity, latency, and possibly an external data transfer.

The source and matching documentation describe reranker components and SQL integration. Configure the provider and model explicitly, then test the exact function signature in the target release. Do not invent a universal reranker syntax by analogy with embedding calls.

Give the reranker a bounded candidate set and enough text to judge the question. Truncating away the exception clause can make a policy appear applicable when it is not. Keep the candidate's source identity and version through the reranking stage.

## 15.6 Deletions and updates belong in search tests

A document removed from the source should stop appearing according to the system's stated update and freshness contract. An edited policy should not leave its old wording indefinitely available through a stale index or result cache. Test both cases through the real query interface.

Create a disposable copy of the corpus, retrieve an identifiable policy, update its return window, and poll until the expected version is served. Then delete it and verify that its ID and text are absent. Capture the source mutation, index or refresh observations, query results, and cache state.

Do not rely on “the SQL table no longer has the row” as complete proof that an independently maintained search index has converged. Likewise, an index hit does not prove that the current source row still exists. The integration must reconcile them.

## 15.7 Explain search failures by category

When a question fails, identify whether the relevant document was absent from the corpus, excluded by a correct filter, missed during candidate generation, ranked too low, truncated before reranking, or retrieved correctly but misused by the generator. These categories point to different fixes.

Changing an LLM prompt cannot restore a missing source document. Increasing candidate K cannot resolve a wrong tenant identity. A larger embedding model cannot repair a policy whose effective date was discarded during ingestion.

**Exercise.** Build a table with each question's lexical top three, vector top three, fused top three, and judged relevant IDs. Add a query containing an exact product code. Determine whether hybrid search helps that query or whether an exact-match route should be preserved.

**Further reading.** See cookbook `full-text-search/`, the [search reference](https://spiceai.org/docs/reference/sql/search), and source files `crates/runtime-search/src/full_text_udtf.rs`, `rrf.rs`, and `crates/search/src/lib.rs`. These source files establish the score-column names used in this edition.
