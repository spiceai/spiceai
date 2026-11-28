# Generates data/orders.parquet (1000 rows) used by the PK-insert verify script.
import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq

os.makedirs("data", exist_ok=True)
n = 1000
base = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
table = pa.table(
    {
        "id": pa.array(range(1, n + 1), pa.int64()),
        "customer": pa.array([f"cust_{i % 50:03d}" for i in range(n)]),
        "amount": pa.array([round(10 + i * 0.37, 2) for i in range(n)], pa.float64()),
        "status": pa.array(["new", "paid", "shipped", "cancelled"][i % 4] for i in range(n)),
        "updated_at": pa.array(
            [base + dt.timedelta(minutes=i) for i in range(n)], pa.timestamp("us", tz="UTC")
        ),
    }
)
pq.write_table(table, "data/orders.parquet")
print("rows", table.num_rows, "sum_amount", round(sum(table["amount"].to_pylist()), 2))
