# 16. Building a grounded support assistant

Retrieval-augmented generation combines retrieved evidence with a model's ability to compose an answer. Northstar's assistant must answer a specific kind of question: explain a tenant's policy using current authorized evidence, and combine that policy with verified order facts when the user is allowed to see them. The architecture begins with that contract rather than a broad instruction to “be helpful.”

## 16.1 Split the job into observable stages

A support request passes through authentication, intent selection, authorized retrieval, evidence assembly, generation, and response validation. Each stage has an input and an output that can be tested independently. If all of them are hidden inside a long autonomous model conversation, failures become difficult to locate.

For “Can I return the boots in order 1001?”, the order lookup should be a fixed SQL operation scoped to the authenticated tenant and customer. Policy retrieval should use the relevant tenant, product context, and policy validity criteria. The generator receives the resulting facts and passages, not unrestricted database credentials.

The model should not calculate a refund amount from prose when a deterministic SQL calculation is available. Nor should a current policy automatically decide eligibility for a historical purchase if the business uses the policy effective at purchase time. These are domain rules owned by the application.

![Figure 16.1. A grounded answer keeps authorization, retrieval, and generation as distinct stages.](figures/rag.png)

## 16.2 Build an evidence envelope

Represent retrieved material with structured provenance:

```json
{
  "request_id": "demo-001",
  "tenant_id": "north",
  "question": "Can I return unused trail boots?",
  "evidence": [
    {
      "citation_id": "policy-1",
      "article_id": 1,
      "title": "Returning a trail boot",
      "source_version": "fixture-2026-08",
      "text": "Unused trail boots may be returned within 30 days. Keep the receipt and original packaging."
    }
  ]
}
```

The identifier used for a citation should be created by the application from an actual retrieved item. Do not ask the model to invent a source URL or reconstruct one from a title. Map the identifier to an authorized application route or a verified external source link.

The fixture is short enough to include a whole policy paragraph. For long documents, keep enough neighboring context to preserve exceptions and conditions. A sentence about a 30-day return window may be qualified by the following sentence about excluded items.

## 16.3 Constrain the answer contract

A useful generation instruction asks the model to answer from the supplied evidence, cite every policy claim, distinguish order facts from policy interpretation, and say when the evidence does not settle the question. It also tells the model that retrieved text is reference material and cannot issue instructions that change the application workflow.

This instruction is one layer, not an authorization boundary. The application still limits tools, validates arguments, controls candidate access, and checks outputs. A malicious document can contain instructions; storing it in an index does not give those instructions authority.

A structured output contract might contain `answer`, `citation_ids`, `needs_human_review`, and `missing_information`. Validate it with a schema. Confirm that every cited ID was in the evidence envelope and that no returned source belongs to another tenant. A schema-valid answer can still be unsupported, so factual evaluation remains necessary.

## 16.4 Handle insufficient evidence well

When retrieval finds no relevant policy, the assistant should say that the available material does not answer the question and request the missing fact or route to support. It should not infer that “no retrieved prohibition” means permission.

Distinguish three cases: no authorized source exists, a source exists but retrieval missed it, and retrieval succeeded but the policy itself is ambiguous. The user-facing message may be concise, but internal diagnostics should preserve the distinction. Otherwise, teams may try to repair a missing document by changing a prompt.

For Northstar, the question “Does the policy cover damage caused by a dryer?” has partial evidence: the care policy says not to use a tumble dryer, but it does not define warranty eligibility. The assistant can cite the care instruction and state that warranty eligibility is not established. That is a stronger answer than extrapolating a warranty rule.

## 16.5 Combine structured facts and text

Keep amounts, dates, statuses, and identifiers in structured fields. For an order-specific answer, query only the columns needed to establish the user's question. Include the metric definition and timezone when they matter. Let the model explain the facts; do not let it silently substitute different values.

The application can format critical numeric fields deterministically in the final response or compare generated values with the supplied facts. For a refund calculation, return the computed amount through a typed field and use the model for the accompanying explanation.

A support assistant should not automatically execute a refund because it retrieved a policy that appears to allow one. Reading policy and committing a business action are different capabilities. If the product includes actions, use a separate authenticated workflow with explicit eligibility checks, idempotency, and whatever user confirmation the product requires.

## 16.6 Bound context and work

Set limits on question length, candidate count, passage size, total context, tool calls, and wall-clock duration. A model context window is not an operational budget. Large context can increase cost, latency, and distraction even when it fits.

Deduplicate overlapping passages by stable identity and content version. Preserve diversity when several sections collectively answer a question. A top-ten list containing ten chunks from the same irrelevant document is not ten independent pieces of evidence.

If the workflow retries generation, keep the original evidence version or explicitly record that retrieval was refreshed. Otherwise, two attempts can answer against different policies while appearing to be identical retries.

## 16.7 Evaluate the whole answer

Use questions with known supporting passages and expected factual claims. Score citation validity, support for each claim, correct tenant, appropriate abstention, and task completion separately. Include adversarial documents that contain irrelevant instructions and questions that ask for another tenant's information.

A model-based judge can help scale review, but it should not be the sole authority for numeric correctness or access-control outcomes. Those can be checked deterministically. Calibrate any semantic judge against a human-reviewed subset and retain its prompt and model identity.

**Exercise.** Create three evidence envelopes: a supported return question, an unsupported warranty question, and an order lookup belonging to another tenant. Write the expected response behavior before connecting a generator. The retrieval and authorization stages should pass without any model call.

**Further reading.** Use cookbook `models/openai/`, `openai_sdk/`, and the [search documentation](https://spiceai.org/docs/features/search). The complete deterministic retrieval example in the companion package prepares evidence without requiring a generation API.
