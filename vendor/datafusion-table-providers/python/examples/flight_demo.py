from datafusion import SessionContext
from datafusion_table_providers import flight

ctx = SessionContext()
pool = flight.FlightTableFactory()
table_provider = pool.get_table("http://localhost:32010", {"flight.sql.query": "SELECT * FROM taxi"})
table_name = "taxi_flight_table"
ctx.register_table_provider(table_name, table_provider)
ctx.sql(f"""
        SELECT "VendorID", COUNT(*), SUM(passenger_count), SUM(total_amount)
            FROM {table_name}
            GROUP BY "VendorID"
            ORDER BY COUNT(*) DESC
        """).show()
