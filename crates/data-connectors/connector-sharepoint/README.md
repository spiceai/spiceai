# `connector-sharepoint`

SharePoint / OneDrive data connector for Spice.ai. Supports **reading and
writing** files on a SharePoint drive as either:

1. **Tabular formats** (CSV, TSV, JSON / NDJSON / JSONL / LDJSON, Parquet,
   Vortex non-Windows, Socrata) — via DataFusion's `ListingTable` plus a
   custom `ObjectStore` implementation. Enables `SELECT`, `INSERT INTO`,
   `COPY TO`, `COPY FROM`, and `CREATE EXTERNAL TABLE`.
2. **Document formats with text extraction** (PDF, DOCX, PPTX, XLSX) — read
   as a `content` column via the `ObjectStore` text-table path or the legacy
   `SharepointTableProvider`, with parsing dispatched through the shared
   `document_parse` registry. Adding a new parser there immediately lights
   up that format for SharePoint.
3. **Plain blobs** (Markdown, plain text, raw byte round-trips) — same paths,
   with content surfaced as-is.

`file_format` is auto-inferred from the URL extension when the param is
omitted, so `from: sharepoint://me/Documents/Q4.xlsx` works without
spelling out `file_format: xlsx`. Unknown extensions fall through to the
text-table path, which calls into `document_parse::get_parser_factory`
based on the extension.

## URL schemes

The dataset's `from:` field selects behavior:

| `from:` syntax                             | Routing                                      | Use for                        |
|--------------------------------------------|----------------------------------------------|--------------------------------|
| `sharepoint://me/{path}`                   | `ObjectStore` + `ListingTable`               | personal OneDrive              |
| `sharepoint://drives/{drive-id}/{path}`    | `ObjectStore` + `ListingTable`               | specific drive by ID           |
| `sharepoint://sites/{site-id}/{path}`      | `ObjectStore` + `ListingTable`               | a site's default drive         |
| `sharepoint://users/{user-id}/{path}`      | `ObjectStore` + `ListingTable`               | a user's default drive         |
| `sharepoint://groups/{group-id}/{path}`    | `ObjectStore` + `ListingTable`               | a group's default drive        |
| `sharepoint:me/root`                       | `SharepointTableProvider` (metadata listing) | legacy format — PDF/PPTX lists |
| `sharepoint:driveId:{id}/id:{item-id}`     | `SharepointTableProvider` (metadata listing) | legacy format                  |
| `sharepoint:site:{name}/path:/{path}`      | `SharepointTableProvider` (metadata listing) | legacy name-based resolution   |

Path segments are percent-decoded automatically, so site IDs containing `,`
(e.g. `contoso.sharepoint.com,abc-def,ghi-jkl`) and file paths containing
spaces work without extra escaping beyond normal URL encoding.

## Authentication

Choose exactly one auth flow via connector parameters.

| Flow                           | Params (besides `client_id` / `tenant_id`)                    | Notes |
|--------------------------------|---------------------------------------------------------------|-------|
| Bearer token (passthrough)     | `bearer_token`                                                | short-lived test / broker-minted |
| Client credentials (secret)    | `client_secret`                                               | primary flow for service / daemon workloads |
| Authorization code             | `auth_code` + `client_secret` + `redirect_uri`                | caller has already completed user-agent redirect |
| Refresh token                  | `refresh_token` + `client_secret`                             | renewal from a prior grant |
| Device code                    | `device_code`                                                 | caller has already obtained a device code |
| SAML 2.0 bearer (RFC 7522)     | `saml_assertion`                                              | federated IdP (Okta / Ping / ADFS) → Azure AD |

Optional:
- `scope` — OAuth2 scope (default `https://graph.microsoft.com/.default`).
- `redirect_uri` — required for `auth_code`.

The flow is inferred automatically from which auth param is set. Providing
more than one auth credential yields a `DuplicateAuthentication` error.

### Required Microsoft Graph scopes

Write workflows (INSERT / COPY TO / CREATE EXTERNAL TABLE) need the
app registration to have been granted:

- `Files.ReadWrite` (for personal drive / specific drive writes)
- `Sites.ReadWrite.All` (for site-scoped writes)

Read-only workflows work with `Files.Read` / `Sites.Read.All`.

## Write semantics

### Versioning (`sharepoint_conflict_behavior`)

When writing to an existing path, SharePoint's default behavior is to create
a new version of the file, preserving prior content in version history. This
matches the connector default (`conflict_behavior=replace`). Alternatives:

| `conflict_behavior` | Behavior                                                   |
|---------------------|------------------------------------------------------------|
| `replace` (default) | Overwrite; SharePoint stores a new version of the content. |
| `fail`              | Reject the write if the path already exists.               |
| `rename`            | Write under a SharePoint-chosen unique name.               |

Only `replace` is compatible with the `ObjectStore` contract used by
`INSERT`/`COPY TO` (those writes must produce an object at the requested
path on success). Configuring `fail` or `rename` causes `put` and
`put_multipart` to reject writes with a clear error; those values are
reserved for non-`ObjectStore` paths that may be added in the future.

Non-`replace` conflict behaviors always route through a resumable upload
session (`createUploadSession`) because the single-shot `PUT /content`
endpoint doesn't cleanly expose conflict behavior headers.

### Small vs large writes

- Files ≤ ~4 MiB go through `PUT /items/{id}/content` directly.
- Files above that threshold use a resumable upload session and chunked
  `PUT`s (handled by `graph-rs-sdk::UploadSession`).
- `put_multipart` buffers all parts in memory and flushes at `complete()`;
  simpler and more robust than streaming SharePoint's upload-session chunks
  with unknown total size.

## Examples

Reading a CSV:
```yaml
datasets:
  - from: sharepoint://sites/contoso.sharepoint.com,11111111-2222-3333-4444-555555555555,66666666-7777-8888-9999-aaaaaaaaaaaa/Shared%20Documents/reports/sales.csv
    name: sales
    params:
      sharepoint_client_id: ${secrets:sharepoint_client_id}
      sharepoint_tenant_id: ${secrets:sharepoint_tenant_id}
      sharepoint_client_secret: ${secrets:sharepoint_client_secret}
      file_format: csv
```

Writing (INSERT INTO):
```sql
INSERT INTO sales VALUES ('Q2', 123456.78);
```

Writing (COPY TO Parquet):
```sql
COPY (SELECT * FROM orders WHERE year = 2026)
TO 'sharepoint://me/Documents/exports/orders-2026.parquet'
(FORMAT parquet);
```

Creating an external table with DDL:
```sql
CREATE EXTERNAL TABLE reports
STORED AS PARQUET
LOCATION 'sharepoint://sites/{site-id}/Shared%20Documents/reports/';
```

Blob upload of a PDF (binary round-trip via `COPY TO`):
```sql
COPY (SELECT content FROM cache WHERE name = 'Q2-report.pdf')
TO 'sharepoint://me/Documents/Q2-report.pdf'
(FORMAT binary);
```

Legacy metadata listing (one row per drive item):
```yaml
datasets:
  - from: sharepoint:site:Engineering/path:/Shared Documents/handbook
    name: handbook_docs
    params:
      sharepoint_client_id: ${secrets:sharepoint_client_id}
      sharepoint_tenant_id: ${secrets:sharepoint_tenant_id}
      sharepoint_client_secret: ${secrets:sharepoint_client_secret}
      file_format: pdf
```

## Testing

Unit + mocked-HTTP tests:
```bash
cargo test -p data_components \
    --features sharepoint,sharepoint-mock-host \
    --no-default-features \
    --lib sharepoint::
cargo test -p connector-sharepoint --lib
```

Live round-trip tests (see `crates/runtime/tests/sharepoint/`) require:
```bash
export SHAREPOINT_TEST_TENANT_ID=...
export SHAREPOINT_TEST_CLIENT_ID=...
export SHAREPOINT_TEST_CLIENT_SECRET=...
export SHAREPOINT_TEST_DRIVE=sharepoint://me  # or sharepoint://sites/...
cargo test -p runtime --test integration sharepoint:: -- --include-ignored
```
Tests silently skip when the required env vars are not set.

## Architectural notes

- The object-store-backed path lives in `data_components::sharepoint::object_store`.
- URL parsing (drive target + in-drive path) is in `data_components::sharepoint::url`.
- Auth flows are in `data_components::sharepoint::auth` (OAuth/OIDC) and
  `data_components::sharepoint::auth::saml` (SAML bearer grant).
- The `Sharepoint` top-level connector dispatches reads/writes between the
  object-store path and the legacy `SharepointTableProvider` by `from:` URL syntax.
- Store registration happens in `Sharepoint::register_object_stores`, which
  pre-registers a `SharepointObjectStore` into DataFusion's object-store registry
  keyed on the dataset's `sharepoint://…` URL. The blanket
  `ListingTableConnector` implementation then picks up the pre-registered
  store when building the `ListingTable`.
