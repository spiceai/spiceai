We are testing a new version of spiced. Specifically the HTTP connector is our focus. 


HTTP Connector: OAuth2 refresh-token auth, query-parameter pagination, and map-to-array conversion were already present in rc.3 — no new changes in those areas.

What's new post-rc.3:

JSON Schema Decomposition — PR #10393 (crates/data_components/src/http/)
- New json_object: "*" metadata marker on a spicepod columns: entry enables decomposing wide/nested JSON API responses into declared static columns + one catch-all JSON column. This is
the "JSON union handling" feature.
- New file http/json_nest.rs implements HttpJsonNesting / decompose_json_row.
- https.rs wires parse_http_json_nesting() into dataset initialisation.
- Non-object rows (bare arrays/primitives) are preserved verbatim in the catch-all; static columns become NULL.

HTTP Auth Security fix — PR #10526 (crates/runtime-auth/src/layer/http.rs, runtime/src/http/)
- HTTP auth principals are now propagated into the request context; MCP is gated when auth is unavailable; read-only API keys enforced on SQL, async query submit/cancel, and mutating
HTTP endpoints.
