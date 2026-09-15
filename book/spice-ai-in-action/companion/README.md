# Spice.ai in Action — companion labs

The Northstar fixtures and Python programs accompany the September 2026 manuscript. All customer and policy data is fictional. Python programs use only the standard library. Install a compatible Spice CLI/runtime with the official instructions at https://spiceai.org/docs/getting-started. Record the actual runtime version; build-specific results and limitations are in Appendix A and the sibling `evidence/` directory.

## First lab

From this directory, in terminal 1:

```sh
spiced --http 127.0.0.1:8090 --flight 127.0.0.1:50051 --metrics 127.0.0.1:9090
```

In terminal 2:

```sh
python3 verify.py --url http://127.0.0.1:8090 --output verification.json
python3 app_client.py north
```

The northern gross-sales total is 22,200 cents and the southern total is 24,900 cents. The verifier includes deliberately wrong business queries to expose SQL pitfalls. `wrong-join` returns 74,600 cents; the correct total is 47,100. `wrong-not-in` should return no rows because the subquery contains NULL.

## Alternative accelerators

Stop your lab runtime before launching a replacement on the same ports. Create the file-mode parent directories, then choose one configuration:

```sh
mkdir -p .spice/duckdb .spice/sqlite
spiced spicepod.duckdb.yaml --http 127.0.0.1:8090 --flight 127.0.0.1:50051
```

Other variants are `spicepod.arrow.yaml`, `spicepod.sqlite.yaml`, and `spicepod.cayenne.yaml`. Run the same verifier after switching. The recorded Cayenne runs disagree on the NULL-sensitive NOT IN query; the verifier intentionally reports that discrepancy. Read Appendix A before treating an engine variant as accepted.

## Search and the application service

Start `spicepod.search.yaml` in place of the starter configuration. Both `title` and `body` are indexed. In a second terminal:

```sh
export NORTHSTAR_NORTH_TOKEN='local-north-token-1234'
export NORTHSTAR_SOUTH_TOKEN='local-south-token-5678'
export SPICE_URL='http://127.0.0.1:8090'
python3 service.py
```

These public demonstration tokens are for the loopback lab only. In a third terminal:

```sh
python3 verify_service.py --url http://127.0.0.1:8088
python3 app_client.py north --search shipping
```

The teaching service binds to loopback and demonstrates fixed, tenant-scoped operations; it is not a production HTTP or identity stack. Chapter 25 describes the deployment work still required.

`spicepod.vectors.yaml` adds the public Model2Vec model `minishlab/potion-base-8M`. First launch needs network access and downloads model artifacts. No paid model provider is required. Its upstream locator is not an immutable revision pin; pin and review model artifacts for deployment. Chapters 14–16 contain the vector and hybrid SQL.

## Runtime authentication

Set `BOOK_API_KEY` to your own lab secret, then launch `spicepod.auth.yaml`. Use the `X-API-Key` HTTP header. The application service supports `SPICE_API_KEY` for its upstream requests. The standalone SQL verifier targets the unauthenticated loopback labs and does not inject a runtime key. The generated schema's API-key representation differs from the runtime's accepted string syntax; see `evidence/config-validation.json` and Appendix A.

## Evidence and scope

SQL results, plans, runtime identities, search outputs, and HTTP service transcripts are supplied as JSON/text. `final-config/` records rechecks after schema-oriented formatting changes. Earlier observations are retained rather than rewritten. Local ports in authoring records differ from the conventional ports above to avoid conflicts with other applications.

External databases, CDC infrastructure, paid generation, cloud deployment, ADBC transport, and cluster failover are integration procedures in the book, not completed local tests. No production performance claim is made from this fixture. Stop your own lab processes with Ctrl-C when finished. Keep credentials and `.spice/` state out of version control.
