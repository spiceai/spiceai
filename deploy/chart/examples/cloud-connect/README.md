# Spice Cloud Connect — direct single-replica Helm bootstrap

Enrolls one `spiced` instance with Spice Cloud using a one-time
`spice-enroll-` enrollment key, in two phases:

| Phase | Values | What it does |
| --- | --- | --- |
| 1. Bootstrap | `values-bootstrap.yaml` | `spiced --token "$(SPICE_ENROLL_KEY)"` enrolls before readiness and persists the identity at `SPICE_CONFIG_DIR` on the PVC. |
| 2. Connected | `values-connected.yaml` | Same release without the key: the stored identity alone reconnects. The single-use Secret is deleted last. |

## Phase 1 — bootstrap

Mint an enrollment key in the Spice Cloud portal, then:

```console
kubectl create secret generic spice-cloud-connect \
  --from-literal=enroll-key=<enrollment key>
helm install spiceai deploy/chart -f deploy/chart/examples/cloud-connect/values-bootstrap.yaml
```

The runtime enrolls **before** it reports ready: a `Ready` pod means the
identity is durable on the volume. Terminal enrollment failures (an invalid,
expired, or consumed key) exit the pod with code 1; transient failures are
retried with backoff for up to ten minutes while the pod stays unready. A
restart in phase 1 is safe — an existing identity always wins and the key is
not redeemed again, and if the first attempt's response was lost the retried
enrollment resumes the same operation instead of enrolling a sibling.
The bootstrap values give the startup probe an eleven-minute failure budget,
so Kubernetes does not restart the process during its ten-minute enrollment
retry window.

## Phase 2 — remove the key, then delete the Secret

```console
deploy/chart/examples/cloud-connect/transition-to-connected.sh spiceai [namespace]
```

The script waits for readiness (identity durable), upgrades the release to
`values-connected.yaml` — removing `--token` and the Secret env reference
while reusing all other installed values and keeping the volume — verifies the
replacement pod reconnects from the identity alone, and only then deletes the
Secret. Deleting a Secret that a pod template still references is forbidden.
The script is idempotent. `SPICE_WAIT_TIMEOUT`, when set, must be positive
integer seconds such as `900s`.

All later upgrades and restarts use `values-connected.yaml` only.

## Guardrails

One enrollment key enrolls exactly one identity, so `helm template` fails —
before anything renders — when a `--token` command is combined with:

- `replicaCount` other than 1 (scaling a direct `--token` deployment above
  one replica is unsupported; multi-replica enrollment belongs to the
  Kubernetes operator),
- `stateful.enabled: false` (the identity must survive pod replacement),
- `SPICE_CONFIG_DIR` missing or outside the `stateful.mountPath` volume,
- a literal enrollment key in `command`, an undefined token environment
  variable, or a literal/ambiguous matching environment entry — the token
  argument must expand exactly one Secret-backed environment variable
  (`"$(SPICE_ENROLL_KEY)"`). No chart value accepts a literal key.

Kubernetes retains the key expansion in the phase-1 pod's argv and pod spec
for that pod's full lifetime, even though `spiced` consumes the key before
readiness and drops its in-memory copy. Run phase 2 promptly after readiness:
it replaces the pod and removes the Secret reference. The key is single-use
and short-lived, but that does not scrub the already-created phase-1 pod.

Docs: https://spiceai.org/docs
