# Enterprise authoring evidence

Source identities are in `source-versions.json`. The book's existing development binary was used only for the SQL function lab; it was not represented as a build of the Enterprise checkout.

Executed chart commands:

```sh
helm template spice-book /Users/lukim/dev/spice-k8s-operator/deploy/chart --namespace spice-system --kube-version 1.33.0 > operator-rendered.yaml
helm lint /Users/lukim/dev/spice-k8s-operator/deploy/chart --kube-version 1.33.0
```

`helm-lint.txt` is the actual output. The rendered chart contained nine documents, including both CRDs. `manifest-validation.json` and `portable-validation.json` contain the actual structural-validation results. The portable validator is included in `companion/enterprise/validate.py`; it requires PyYAML and jsonschema. It reads the rendered chart without connecting to Kubernetes. `render-smoke-*.json` use synthetic image, bucket, and storage names solely to check the companion renderer; they are not deployment evidence.

Executed function runtime command (from `companion/enterprise/`):

```sh
/Users/lukim/dev/spice2/target/debug/spiced spicepod.functions.yaml --http 127.0.0.1:18095 --flight 127.0.0.1:15056
```

`functions-sql.json` contains each SQL statement and its real returned rows. `functions-runtime.log` records the isolated process. Normal, NULL-input, and zero-input cases passed. No remote function, WASM module, model ring, OIDC issuer, Cedar-governed runtime, Kubernetes cluster, snapshot bootstrap, or failover test was executed.
