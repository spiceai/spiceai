import subprocess
import time
from datafusion import SessionContext
from datafusion_table_providers import clickhouse  # hypothetical provider

def run_docker_container():
    subprocess.run(
        ["docker", "run", "--name", "clickhouse",
        "-e", "CLICKHOUSE_USER=user",
        "-e", "CLICKHOUSE_PASSWORD=secret",
        "-p", "8123:8123",
        "-d", "clickhouse/clickhouse-server:latest"],
        check=True,
    )
    time.sleep(20)

def create_schema():
    sql = r"""
        CREATE TABLE companies (
            id UInt32,
            name String,
            founded Date,
            revenue Decimal(18,2),
            is_active Bool,
            tags Array(String)
        ) ENGINE = MergeTree()
        ORDER BY id;

        INSERT INTO companies VALUES
            (1, 'Acme Corporation', '1999-03-12', 12500000.50, 1, ['manufacturing', 'global']),
            (2, 'Widget Inc.',      '2005-07-01',  4500000.00, 1, ['gadgets', 'innovation']),
            (3, 'Gizmo Corp.',       '2010-11-23',   780000.75, 0, ['hardware']),
            (4, 'Tech Solutions',    '2018-01-15',   220000.00, 1, ['consulting','it']),
            (5, 'Data Innovations',  '2021-05-10',    98000.99, 1, ['analytics','startup']);

        CREATE VIEW companies_param_view AS
        SELECT id, name, founded, revenue, is_active, tags
        FROM companies
        WHERE name = {name:String};
    """
    subprocess.run(
        ["docker", "exec", "-i", "clickhouse", "clickhouse-client", "--multiquery", "--query", sql],
        check=True
    )

def stop_container():
    subprocess.run(["docker", "stop", "clickhouse"], check=True)
    subprocess.run(["docker", "rm",   "clickhouse"], check=True)

class TestClickHouseParameterized:
    @classmethod
    def setup_class(cls):
        run_docker_container()
        create_schema()
        cls.ctx = SessionContext()
        connection_param = {
            "url": "http://localhost:8123",
            "database": "default",
            "user": "user",
            "password": "secret"
        }
        cls.pool = clickhouse.ClickHouseTableFactory(connection_param)

    @classmethod
    def teardown_class(cls):
        stop_container()

    def test_get_tables(self):
        tables = self.pool.tables()
        assert "companies" in tables
        assert "companies_param_view" in tables

    def test_parameterized_view(self):
        # Register provider so DF can access the view\

        self.ctx.register_table_provider(
            "companies_param_view",
            self.pool.get_table("companies_param_view", [("name", "Gizmo Corp.")])
        )

        # Query using parameter
        df = self.ctx.sql(
            "SELECT id, name, revenue FROM companies_param_view"
        )
        rows = df.collect()[0]

        assert len(rows["name"]) == 1
        assert rows["name"][0].as_py() == "Gizmo Corp."
        assert rows["revenue"][0].as_py() == 780000.75
