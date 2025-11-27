import pytest
import os
from datafusion import SessionContext
from datafusion_table_providers import duckdb

class TestDuckDBIntegration:
    def setup_method(self):
        """Set up the test environment"""
        self.ctx = SessionContext()
        self.db_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "core", "examples", "duckdb_example.db")
        self.pool_readonly = duckdb.DuckDBTableFactory(self.db_path, duckdb.AccessMode.ReadOnly)
        self.pool_readwrite = duckdb.DuckDBTableFactory(self.db_path)

    def test_get_tables(self):
        """Test retrieving tables from the database"""
        tables = self.pool_readonly.tables()
        assert isinstance(tables, list)
        assert len(tables) == 2
        assert tables == ["companies", "projects"]
        
    def test_query_companies(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool_readonly.get_table("companies"))
        
        # Run SQL query to select Microsoft row
        df = self.ctx.sql("SELECT name FROM companies WHERE ticker = 'MSFT'")
        result = df.collect()
        
        # Verify single row returned with name = Microsoft
        assert len(result) == 1
        assert str(result[0]['name'][0]) == "Microsoft"
        
    def test_complex_query(self):
        """Test querying companies table with SQL"""
        self.ctx.register_table_provider("companies", self.pool_readonly.get_table("companies"))
        self.ctx.register_table_provider("projects", self.pool_readonly.get_table("projects"))
        
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
        """Test that writing fails on read-only mode"""
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool_readonly.get_table("companies"))
        
        with pytest.raises(Exception):
            tmp = self.ctx.sql("INSERT INTO companies VALUES (3, 'Test Corp', 'TEST')")
            tmp.collect() # this will trigger the execution of the query

    def test_write_fails_readwrite(self):
        """Test that writing fails because it is not supported"""
        # Insertion fails because duckdb does not implement write operations even when
        # database is opened in read-write mode.
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool_readwrite.get_table("companies"))
        
        with pytest.raises(Exception):
            tmp = self.ctx.sql("INSERT INTO companies VALUES (3, 'Test Corp', 'TEST')")
            tmp.collect()
