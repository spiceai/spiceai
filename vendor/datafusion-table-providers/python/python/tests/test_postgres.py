import subprocess
import time

from datafusion import SessionContext
from datafusion_table_providers import postgres

def run_docker_container():
    """Run the Docker container with the postgres image"""
    result = subprocess.run(
        ["docker", "run", "--name", "postgres", "-e", "POSTGRES_PASSWORD=password", "-e", "POSTGRES_DB=postgres_db", 
         "-p", "5432:5432", "-d", "postgres:16-alpine"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to start postgres container: {result.stderr.decode()}")

def create_table_and_insert_data():
    """Create a table and insert data into postgres"""
    sql_commands = """
        CREATE TABLE companies (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        );

        INSERT INTO companies (id, name) VALUES
            (1, 'Acme Corporation'),
            (2, 'Widget Inc.'),
            (3, 'Gizmo Corp.'),
            (4, 'Tech Solutions'),
            (5, 'Data Innovations');

        CREATE VIEW companies_view AS
        SELECT id, name FROM companies;

        CREATE MATERIALIZED VIEW companies_materialized_view AS
        SELECT id, name FROM companies;
    """
    
    result = subprocess.run(
        ["docker", "exec", "-i", "postgres", "psql", "-U", "postgres", "-d", "postgres_db"],
        input=sql_commands.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Error executing SQL commands: {result.stderr.decode()}")

def stop_and_remove_container() -> None:
    """Stop and remove the postgres container after use."""
    subprocess.run(["docker", "stop", "postgres"], check=True)
    subprocess.run(["docker", "rm", "postgres"], check=True)


class TestPostgresIntegration:
    @classmethod
    def setup_class(self):
        run_docker_container()
        time.sleep(30)
        create_table_and_insert_data()
        time.sleep(10)
        self.ctx = SessionContext()
        connection_param = {
            "host": "localhost",
            "user": "postgres",
            "db": "postgres_db",
            "pass": "password",
            "port": "5432",
            "sslmode": "disable"}
        self.pool = postgres.PostgresTableFactory(connection_param)

    @classmethod
    def teardown_class(self):
        stop_and_remove_container()

    def test_get_tables(self):
        """Test retrieving tables from the database"""
        tables = self.pool.tables()
        assert isinstance(tables, list)
        assert len(tables) == 1
        assert tables == ["companies"]

    def test_query_companies(self):
        """Test querying companies table with SQL."""
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool.get_table("companies"))
        
        # Test basic query
        query = "SELECT * FROM companies ORDER BY id"
        result = self.ctx.sql(query).collect()
        assert result is not None
        
        # Test data integrity
        record_batch = result[0]
        name_column = record_batch["name"]
        
        expected_companies = [
            "Acme Corporation",
            "Widget Inc.",
            "Gizmo Corp.",
            "Tech Solutions",
            "Data Innovations"
        ]
        
        assert len(name_column) == len(expected_companies)
        for i, expected in enumerate(expected_companies):
            assert str(name_column[i]) == expected
