# Spice Cloud Connect — direct Docker bootstrap

Enrolls one `spiced` container with Spice Cloud using a one-time
`spice-enroll-` enrollment key, with the identity persisted on a named
volume so it survives container replacement.

## 1. Bootstrap (first start only)

Mint an enrollment key in the Spice Cloud portal, then:

```console
cd deploy/docker/cloud-connect
SPICE_ENROLL_KEY=<enrollment key> \
  docker compose -f compose.yaml -f compose.bootstrap.yaml up -d
```

The runtime enrolls **before** it reports ready and persists the issued
identity at `SPICE_CONFIG_DIR` (`/data/.spice` on the `spice-identity`
volume). Wait for readiness:

```console
curl -fsS http://localhost:8090/v1/ready
```

Terminal enrollment failures (an invalid, expired, or consumed key) exit the
container with code 1; transient failures are retried with backoff for up to
ten minutes while the container stays unready. The bootstrap overlay disables
automatic restarts so a terminal failure or exhausted retry window remains
visible. After correcting the cause, explicitly rerun the bootstrap command;
an existing identity wins without redeeming the key again, and a persisted
draft resumes the same operation instead of enrolling a sibling instance.

## 2. Drop the key

Once ready, recreate the container from the base file alone so the spent key
leaves the container's argv:

```console
docker compose up -d
```

The container now starts with no key and no flag, restores the base service's
`unless-stopped` restart policy, and reconnects from the stored identity alone.
Prove it survives replacement:

```console
docker compose down && docker compose up -d
curl -fsS http://localhost:8090/v1/ready
docker compose logs spiced | grep "Spice Cloud Connect: enabled"
```

Only `docker compose down -v` (which deletes the volume) forgets the
identity; after that, a fresh enrollment key is needed.

## Notes

- One enrollment key enrolls exactly one identity: do not scale this service
  above one replica. Multi-replica enrollment belongs to the Spice
  Kubernetes operator.
- The key is interpolated by Compose from the host environment (or an
  `.env` file), and the container runtime retains it in the phase-1
  container's argv/config for that container's full lifetime. `spiced`
  consumes the key before readiness and drops its in-memory copy, but cannot
  scrub the already-created container metadata. Recreate from the base file
  promptly after readiness; step 2 removes it. Never write the key into a
  compose file.
- The `init-volume` service exists because the runtime image is `FROM
  scratch` and runs as uid 65534: a freshly created named volume is
  root-owned, so it is handed to the runtime user before first write.

Docs: https://spiceai.org/docs
