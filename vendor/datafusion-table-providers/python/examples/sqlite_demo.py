from datafusion import SessionContext
from datafusion_table_providers import sqlite

ctx = SessionContext()
pool = sqlite.SqliteTableFactory("../../core/examples/sqlite_example.db", "file", 3.0, None)
tables = pool.tables()

for t in tables:
    ctx.register_table_provider(t, pool.get_table(t))
    print("Checking table:", t)
    ctx.table(t).show()
