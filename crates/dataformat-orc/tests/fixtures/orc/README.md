# Nested ORC listing fixtures

`payload_id.orc` and `payload_id_extra.orc` are written by PyArrow's ORC
encoder, not `orc-rust`. `orc-rust` 0.8.0 can read struct columns but cannot
write them (`unimplemented!("unsupported datatype")` in the stripe encoder).

| File | Schema | Row |
| --- | --- | --- |
| `payload_id.orc` | `id: int64`, `payload: struct<id: int64>` | `(1, {id: 10})` |
| `payload_id_extra.orc` | `id: int64`, `payload: struct<id: int64, extra: int64>` | `(2, {id: 10, extra: 99})` |

They exist so a listing table can infer a merged nested type and still scan
the file that only has `payload.id`.
