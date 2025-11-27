from datafusion import SessionContext
from datafusion_table_providers import postgres

ctx = SessionContext()
connection_param = {
    "host": "localhost",
    "user": "postgres",
    "db": "postgres_db",
    "pass": "password",
    "port": "5432",
    "sslmode": "disable"}
pool = postgres.PostgresTableFactory(connection_param)
tables = pool.tables()

for t in tables:
    ctx.register_table_provider(t, pool.get_table(t))
    print("Checking table:", t)
    ctx.table(t).show()
