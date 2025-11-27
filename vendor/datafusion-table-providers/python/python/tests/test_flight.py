from datafusion import SessionContext
from datafusion_table_providers import flight
import pytest
import subprocess
import time


class TestFlightIntegration:     
    @classmethod
    def setup_class(self):
        """Called once before all test methods in the class"""
        self.ctx = SessionContext()
        self.pool = flight.FlightTableFactory()
        self.process = subprocess.Popen(
            ["roapi", "-t", "taxi=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        # 20s timeout is required to ensure the server is running and data is loaded
        # The timeout is determined by empirical testing
        time.sleep(20)

    @classmethod 
    def teardown_class(self):
        """Called once after all test methods in the class"""
        self.process.kill()

    def test_query_companies(self):
        """Test querying companies table with SQL"""
        print("Running test_query_companies")
        table_name = "taxi_flight_table"
        self.ctx.register_table_provider(table_name, self.pool.get_table("http://localhost:32010", {
            "flight.sql.query": "SELECT * FROM taxi"
        }))
        df = self.ctx.sql(f"""
            SELECT "VendorID", COUNT(*) as count, SUM(passenger_count) as passenger_counts, SUM(total_amount) as total_amounts
                FROM {table_name}
                GROUP BY "VendorID"
                ORDER BY COUNT(*) DESC
            """)
        result = df.collect()
        
        # Verify the results
        vendor_ids = result[0]['VendorID'].tolist()
        assert vendor_ids == [2, 1, 6]
        
        counts = result[0]['count'].tolist()
        assert counts == [2234632, 729732, 260]
        
        passenger_counts = result[0]['passenger_counts'].tolist()
        assert passenger_counts == [2971865, 810883, None]
        
        total_amounts = result[0]['total_amounts'].tolist()
        assert total_amounts == pytest.approx([60602721.27, 18841261.98, 12401.03])
