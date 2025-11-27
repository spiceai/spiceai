from datafusion import SessionContext
from datafusion_table_providers import clickhouse

ctx = SessionContext()
connection_param = {
    "url": "http://localhost:8123",
    "database": "default",
    "user": "user",
    "password": "secret"
}
pool = clickhouse.ClickHouseTableFactory(connection_param)
tables = pool.tables()

for t in tables:
    ctx.register_table_provider(t, pool.get_table(t))
    print("Checking table:", t)
    ctx.table(t).show()
