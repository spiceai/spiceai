#!/usr/bin/env python3
"""Generate retention test data relative to 'now' (UTC). Writes data/events.parquet + data/meta.json"""
import datetime as dt, json, random, sys, pyarrow as pa, pyarrow.parquet as pq
out = sys.argv[1] if len(sys.argv) > 1 else 'data'
UTC = dt.timezone.utc
T0 = dt.datetime.now(UTC).replace(microsecond=0)
P = dt.timedelta(hours=24)
rows = []
for k in range(2880):  rows.append(T0 - dt.timedelta(minutes=k))          # 48h, 1/min
for s in range(-30, 121): rows.append(T0 - P + dt.timedelta(seconds=s))   # boundary, 1/s
for f in (dt.timedelta(hours=1), dt.timedelta(days=1), dt.timedelta(days=365)): rows.append(T0 + f)  # future
rows += [None] * 4                                                        # NULL time
random.seed(7)
offs = [dt.timezone(dt.timedelta(hours=h, minutes=m)) for h, m in ((0,0),(9,0),(-7,0),(5,30),(-3,-30))]
regions = ['us', 'eu', 'ap', 'sa']
ids, naive, tz, us_s, us_ms, iso_z, iso_off, iso_naive, d, region, val = ([] for _ in range(11))
for i, t in enumerate(rows):
    ids.append(i); region.append(regions[i % 4]); val.append(i * 3 % 1000)
    if t is None:
        for L in (naive, tz, us_s, us_ms, iso_z, iso_off, iso_naive, d): L.append(None)
        continue
    naive.append(t.replace(tzinfo=None)); tz.append(t)
    us_s.append(int(t.timestamp())); us_ms.append(int(t.timestamp() * 1000))
    iso_z.append(t.strftime('%Y-%m-%dT%H:%M:%SZ'))
    iso_off.append(t.astimezone(offs[i % len(offs)]).isoformat())
    iso_naive.append(t.strftime('%Y-%m-%d %H:%M:%S'))
    d.append(t.date())
tbl = pa.table({
    'id': pa.array(ids, pa.int64()), 'region': region, 'val': pa.array(val, pa.int64()),
    'ts': pa.array(naive, pa.timestamp('us')), 'ts_tz': pa.array(tz, pa.timestamp('us', tz='UTC')),
    'ts_s': pa.array(us_s, pa.int64()), 'ts_ms': pa.array(us_ms, pa.int64()),
    'ts_iso': pa.array(iso_z, pa.string()), 'ts_iso_off': pa.array(iso_off, pa.string()),
    'ts_iso_naive': pa.array(iso_naive, pa.string()), 'ts_date': pa.array(d, pa.date32()),
})
import os; os.makedirs(out, exist_ok=True)
pq.write_table(tbl, f'{out}/events.parquet')
with open(f'{out}/meta.json', 'w') as f:
    json.dump({'T0': T0.isoformat(), 'rows': len(rows)}, f)
print('T0', T0.isoformat(), 'rows', len(rows))
