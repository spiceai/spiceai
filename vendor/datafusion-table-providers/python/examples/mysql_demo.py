from datafusion import SessionContext
from datafusion_table_providers import mysql

ctx = SessionContext()
connection_param = {
    "connection_string": "mysql://root:password@localhost:3306/mysql_db",
    "sslmode": "disabled"}
pool = mysql.MySQLTableFactory(connection_param)
tables = pool.tables()

for t in tables:
    ctx.register_table_provider(t, pool.get_table(t))
    print("Checking table:", t)
    ctx.table(t).show()
