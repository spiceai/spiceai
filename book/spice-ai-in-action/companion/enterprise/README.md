# Enterprise integration labs

These templates accompany Chapters 30–36 and Appendix G. They target the `spice.ai/v2` schema from operator commit `4de706cabe416e20afe9ef343fc7fb2d375cd00f`. They are **integration templates**, not a deployment that was run for the book.

Prerequisites: an entitled Enterprise runtime image, installed compatible operator/CRDs, a dedicated Kubernetes namespace, private registry access, runtime credentials, and (for the cluster) workload identity, object storage with the required conditional-write behavior, and a tested storage class. The templates use the `spice-book` namespace, `northstar-registry` pull Secret, `northstar-runtime` Secret with `api-key`, and cluster ServiceAccount `northstar-data`.

`render.py` uses only Python's standard library. Set `BOOK_ENTERPRISE_REPOSITORY` and `BOOK_ENTERPRISE_TAG`. For the cluster, also set `BOOK_STATE_BUCKET` and `BOOK_STORAGE_CLASS`. Then run from the parent companion directory:

```sh
python3 enterprise/render.py spicepodset --output enterprise/rendered/spicepodset.json
python3 enterprise/render.py spicepodcluster --output enterprise/rendered/spicepodcluster.json
```

Choose one topology. Inspect the result and the target context before any server-side dry-run or apply. The renderer does not contact Kubernetes, provision secrets, or confirm image access. The JSON templates are renderer inputs; YAML copies are supplied for reading. No datasets are registered in the initial Kubernetes templates, so `SELECT 1` is the connectivity smoke test. Add supported sources after that stage succeeds.

`spicepod.policy.fragment.yaml` demonstrates the Enterprise OIDC/policy shape using a fictional issuer and an `orders` resource that you must register. It is not a working identity deployment. Extend the policies to the exact resources and operations the application needs and test every exposed route.

`spicepod.functions.yaml` is a separate local SQL-function lab. It ran on the authoring development binary and returned 11000, NULL, and 0 for the three cases in Chapter 35. It is not proof that Enterprise-only capabilities or a cluster were executed.

Offline chart and CRD evidence is under `evidence/enterprise/`. The schema validates the outer Kubernetes object; the nested Spicepod and Cedar expressions require additional validation. The full workbook is Appendix G.
