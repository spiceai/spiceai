from datafusion import SessionContext
from datafusion_table_providers import duckdb

ctx = SessionContext()
pool = duckdb.DuckDBTableFactory("../../core/examples/duckdb_example.db", duckdb.AccessMode.ReadOnly)
tables = pool.tables()

for t in tables:
    ctx.register_table_provider(t, pool.get_table(t))
    print("Checking table:", t)
    ctx.table(t).show()
