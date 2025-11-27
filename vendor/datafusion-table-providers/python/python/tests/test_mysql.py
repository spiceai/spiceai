import subprocess
import time

from datafusion import SessionContext
from datafusion_table_providers import mysql

def run_docker_container():
    """Run the Docker container with the MySQL image"""
    result = subprocess.run(
        ["docker", "run", "--name", "mysql", "-e", "MYSQL_ROOT_PASSWORD=password", "-e", "MYSQL_DATABASE=mysql_db", 
         "-p", "3306:3306", "-d", "mysql:9.0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        print(f"Failed to start MySQL container: {result.stderr.decode()}")

def create_table_and_insert_data():
    """Create a table and insert data into MySQL"""
    sql_commands = """
        CREATE TABLE companies (
            id INT PRIMARY KEY,
            name VARCHAR(100)
        );
        
        INSERT INTO companies (id, name) VALUES (1, 'Acme Corporation');
    """
    
    # Execute the SQL commands inside the Docker container
    result = subprocess.run(
        ["docker", "exec", "-i", "mysql", "mysql", "-uroot", "-ppassword", "mysql_db"],
        input=sql_commands.encode(),  # Pass SQL commands to stdin
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Check if the SQL execution was successful
    if result.returncode != 0:
        print(f"Error executing SQL commands: {result.stderr.decode()}")
    else:
        print(f"SQL commands executed successfully:\n{result.stdout.decode()}")

def stop_and_remove_container():
    """Stop and remove the MySQL container after use"""
    subprocess.run(["docker", "stop", "mysql"])
    subprocess.run(["docker", "rm", "mysql"])
    print("MySQL container stopped and removed.")


class TestMySQLIntegration:
    @classmethod
    def setup_class(self):
        run_docker_container()
        time.sleep(30)
        create_table_and_insert_data()
        time.sleep(10)
        self.ctx = SessionContext()
        connection_param = {
            "connection_string": "mysql://root:password@localhost:3306/mysql_db",
            "sslmode": "disabled"}
        self.pool = mysql.MySQLTableFactory(connection_param)

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
        """Test querying companies table with SQL"""
        table_name = "companies"
        self.ctx.register_table_provider(table_name, self.pool.get_table("companies"))
        query = "SELECT * FROM companies"
        df = self.ctx.sql(query).collect()
        assert df is not None
        name_column = df[0]['name']
        assert str(name_column[0]) == "Acme Corporation"
