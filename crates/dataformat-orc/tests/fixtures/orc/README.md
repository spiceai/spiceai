# Nested ORC listing fixtures

These files are written by PyArrow's ORC encoder, not `orc-rust`.
`orc-rust` 0.8.0 can read struct and map columns but cannot write them
(`unimplemented!("unsupported datatype")` in the stripe encoder).

| File | Schema | Row |
| --- | --- | --- |
| `payload_id.orc` | `id: int64`, `payload: struct<id: int64>` | `(1, {id: 10})` |
| `payload_id_extra.orc` | `id: int64`, `payload: struct<id: int64, extra: int64>` | `(2, {id: 10, extra: 99})` |
| `id_only.orc` | `id: int64` | `(1)` |
| `id_and_labels.orc` | `id: int64`, `labels: map<string, string>` | `(2, {env: prod})` |

The payload pair exists so a listing table can infer a merged nested
struct and still scan the file that only has `payload.id`. The labels
pair covers a map present in only some files: the merged `labels`
column is nullable, but its `entries` field stays required so
`MapArray::try_new` accepts the scan.
