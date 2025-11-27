import pytest
import os
from datafusion import SessionContext
from datafusion_table_providers import odbc

class TestOdbcIntegration:
    def setup_method(self):
        """Set up the test environment"""
        self.ctx = SessionContext()
        connection_param: dict = {'connection_string': 'driver=SQLite3;database=../../../core/examples/sqlite_example.db;'}
        self.pool = odbc.ODBCTableFactory(connection_param)
        
    def test_query_companies(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool.get_table("companies"))
        
        # Run SQL query to select Microsoft row
        df = self.ctx.sql("SELECT name FROM companies WHERE ticker = 'MSFT'")
        result = df.collect()
        
        # Verify single row returned with name = Microsoft
        assert len(result) == 1
        assert str(result[0]['name'][0]) == "Microsoft"
        
    def test_complex_query(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool.get_table("companies"))
        self.ctx.register_table_provider("projects", self.pool.get_table("projects"))
        
        # Run SQL query to select Microsoft row
        df = self.ctx.sql(
            """SELECT companies.id, companies.name as company_name, projects.name as project_name
            FROM companies, projects
            WHERE companies.id = projects.id"""
        )
        result = df.collect()
    
        assert len(result) == 1
        assert str(result[0]['company_name'][0]) == "Microsoft"
        assert str(result[0]['project_name'][0]) == "DataFusion"
        
    def test_write_fails(self):
        """Test that writing fails because it is not supported"""
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool.get_table("companies"))
        
        with pytest.raises(Exception):
            tmp = self.ctx.sql("INSERT INTO companies VALUES (3, 'Test Corp', 'TEST')")
            tmp.collect() # this will trigger the execution of the query
