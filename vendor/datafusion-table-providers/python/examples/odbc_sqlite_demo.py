from datafusion import SessionContext
from datafusion_table_providers import odbc

ctx = SessionContext()
connection_param: dict = {'connection_string': 'driver=SQLite3;database=../../core/examples/sqlite_example.db;'}
pool = odbc.ODBCTableFactory(connection_param)

ctx.register_table_provider(name = "companies", provider = pool.get_table("companies"))
ctx.table("companies").show()
