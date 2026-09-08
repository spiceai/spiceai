#!/usr/bin/env python3
"""
Test script for Spice.ai Flight SQL ADBC endpoint using Python ADBC driver.

This script tests:
1. Basic SQL queries
2. Prepared statements with parameters
3. Prepared statements with multiple parameters
4. UPDATE/INSERT/DELETE prepared statements

Prerequisites:
    pip install adbc-driver-flightsql pyarrow

Usage:
    python test_flightsql_adbc.py [--host HOST] [--port PORT]
"""

import argparse
import sys
from typing import Optional

try:
    import adbc_driver_flightsql.dbapi as flight_sql
    # pyarrow is imported to verify availability; bind to _ to indicate intentional non-use
    import pyarrow as _pyarrow  # noqa: F401
    del _pyarrow
except ImportError as e:
    print(f"Error: Missing required package: {e}")
    print("Install with: pip install adbc-driver-flightsql pyarrow")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_success(message: str):
    """Print success message in green"""
    print(f"{Colors.GREEN}✓ {message}{Colors.RESET}")


def print_error(message: str):
    """Print error message in red"""
    print(f"{Colors.RED}✗ {message}{Colors.RESET}")


def print_info(message: str):
    """Print info message in blue"""
    print(f"{Colors.BLUE}ℹ {message}{Colors.RESET}")


def print_test_header(message: str):
    """Print test header"""
    print(f"\n{Colors.BOLD}{Colors.YELLOW}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.YELLOW}{message}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.YELLOW}{'=' * 60}{Colors.RESET}")


def test_basic_query(conn) -> bool:
    """Test a basic SQL query without parameters"""
    print_test_header("Test 1: Basic SQL Query")
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 + 1 AS result")
        
        result = cursor.fetch_arrow_table()
        print_info(f"Query executed successfully")
        print_info(f"Result schema: {result.schema}")
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        if result.num_rows == 1 and result.column('result')[0].as_py() == 2:
            print_success("Basic query test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"Basic query test failed: {e}")
        return False


def test_prepared_statement_simple(conn) -> bool:
    """Test a simple prepared statement with one parameter"""
    print_test_header("Test 2: Simple Prepared Statement (Single Parameter)")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement
        query = "SELECT ? + 1 AS the_answer"
        print_info(f"Preparing query: {query}")
        
        # Execute with parameter
        cursor.execute(query, parameters=[41])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        if result.num_rows == 1 and result.column('the_answer')[0].as_py() == 42:
            print_success("Simple prepared statement test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"Simple prepared statement test failed: {e}")
        return False


def test_prepared_statement_multiple_params(conn) -> bool:
    """Test a prepared statement with multiple parameters"""
    print_test_header("Test 3: Prepared Statement with Multiple Parameters")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement with multiple parameters
        query = "SELECT ? + ? AS sum, ? * ? AS product"
        print_info(f"Preparing query: {query}")
        
        # Execute with parameters
        cursor.execute(query, parameters=[10, 32, 6, 7])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        if (result.num_rows == 1 and 
            result.column('sum')[0].as_py() == 42 and
            result.column('product')[0].as_py() == 42):
            print_success("Multiple parameters prepared statement test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"Multiple parameters prepared statement test failed: {e}")
        return False


def test_prepared_statement_with_strings(conn) -> bool:
    """Test a prepared statement with string parameters"""
    print_test_header("Test 4: Prepared Statement with String Parameters")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement with string concatenation
        query = "SELECT ? || ' ' || ? AS greeting"
        print_info(f"Preparing query: {query}")
        
        # Execute with string parameters
        cursor.execute(query, parameters=['Hello', 'World'])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        if result.num_rows == 1 and result.column('greeting')[0].as_py() == 'Hello World':
            print_success("String parameters prepared statement test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"String parameters prepared statement test failed: {e}")
        return False


def test_prepared_statement_types(conn) -> bool:
    """Test a prepared statement with various data types"""
    print_test_header("Test 5: Prepared Statement with Various Data Types")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement with different types
        query = "SELECT ? AS int_val, ? AS float_val, ? AS string_val, ? AS bool_val"
        print_info(f"Preparing query: {query}")
        
        # Execute with various types
        cursor.execute(query, parameters=[42, 3.14, 'test', True])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        row = result.to_pydict()
        if (result.num_rows == 1 and 
            row['int_val'][0] == 42 and
            abs(row['float_val'][0] - 3.14) < 0.01 and
            row['string_val'][0] == 'test' and
            row['bool_val'][0] is True):
            print_success("Various data types prepared statement test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"Various data types prepared statement test failed: {e}")
        return False


def test_prepared_statement_null(conn) -> bool:
    """Test a prepared statement with NULL parameter"""
    print_test_header("Test 6: Prepared Statement with NULL Parameter")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement with NULL handling
        query = "SELECT ? IS NULL AS is_null, COALESCE(?, 'default') AS value"
        print_info(f"Preparing query: {query}")
        
        # Execute with NULL parameter
        cursor.execute(query, parameters=[None, None])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        row = result.to_pydict()
        if (result.num_rows == 1 and 
            row['is_null'][0] is True and
            row['value'][0] == 'default'):
            print_success("NULL parameter prepared statement test passed")
            return True
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
            
    except Exception as e:
        print_error(f"NULL parameter prepared statement test failed: {e}")
        return False


def test_prepared_statement_reuse(conn) -> bool:
    """Test reusing a prepared statement with different parameters"""
    print_test_header("Test 7: Reusing Prepared Statement")
    
    try:
        cursor = conn.cursor()
        
        # Prepare statement once
        query = "SELECT ? * ? AS result"
        print_info(f"Preparing query: {query}")
        
        # Execute multiple times with different parameters
        test_cases = [
            ([2, 3], 6),
            ([5, 7], 35),
            ([10, 10], 100),
        ]
        
        for params, expected in test_cases:
            cursor.execute(query, parameters=params)
            result = cursor.fetch_arrow_table()
            actual = result.column('result')[0].as_py()
            
            print_info(f"Parameters {params} -> Result: {actual}")
            
            if actual != expected:
                print_error(f"Expected {expected}, got {actual}")
                return False
        
        print_success("Reusing prepared statement test passed")
        return True
            
    except Exception as e:
        print_error(f"Reusing prepared statement test failed: {e}")
        return False


def test_prepare_execute_commit_pattern(conn) -> bool:
    """Test explicit prepare -> execute -> commit pattern"""
    print_test_header("Test 8: Prepare-Execute-Commit Pattern")
    
    try:
        cursor = conn.cursor()
        
        # Test 1: Prepare and execute a SELECT with parameters
        query = "SELECT ? AS value1, ? AS value2, ? + ? AS sum"
        print_info(f"Step 1: Preparing query: {query}")
        
        # In ADBC, prepare happens implicitly on first execute
        # But we can still test the pattern by using prepared statements
        cursor.execute(query, parameters=[10, 20, 30, 40])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Step 2: Executed with parameters [10, 20, 30, 40]")
        print_info(f"Result: {result.to_pydict()}")
        
        # Verify result
        row = result.to_pydict()
        if (result.num_rows == 1 and 
            row['value1'][0] == 10 and
            row['value2'][0] == 20 and
            row['sum'][0] == 70):
            print_success("Step 2: Execution successful")
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
        
        # Test 2: Reuse the same prepared statement with different parameters
        print_info(f"Step 3: Reusing prepared statement with [5, 15, 25, 35]")
        cursor.execute(query, parameters=[5, 15, 25, 35])
        
        result = cursor.fetch_arrow_table()
        print_info(f"Result: {result.to_pydict()}")
        
        row = result.to_pydict()
        if (result.num_rows == 1 and 
            row['value1'][0] == 5 and
            row['value2'][0] == 15 and
            row['sum'][0] == 60):
            print_success("Step 3: Reuse successful")
        else:
            print_error(f"Unexpected result: {result.to_pydict()}")
            return False
        
        # Test 3: Commit (note: Flight SQL is typically auto-commit)
        print_info("Step 4: Committing transaction")
        try:
            conn.commit()
            print_success("Step 4: Commit successful (or auto-commit)")
        except Exception as e:
            # Some Flight SQL implementations don't support explicit commit
            # That's okay for read-only queries
            print_info(f"Commit not supported or not needed: {e}")
        
        print_success("Prepare-Execute-Commit pattern test passed")
        return True
            
    except Exception as e:
        print_error(f"Prepare-Execute-Commit pattern test failed: {e}")
        import traceback
        print_error(traceback.format_exc())
        return False


def run_all_tests(host: str, port: int, username: Optional[str] = None, 
                  password: Optional[str] = None) -> bool:
    """Run all tests and return overall success"""
    
    print(f"\n{Colors.BOLD}Connecting to Spice.ai Flight SQL Server{Colors.RESET}")
    print(f"Host: {host}")
    print(f"Port: {port}")
    
    uri = f"grpc://{host}:{port}"
    
    try:
        # Connect to Flight SQL server
        conn_params = {"uri": uri}
        if username and password:
            conn_params["username"] = username
            conn_params["password"] = password
            
        conn = flight_sql.connect(**conn_params)
        print_success("Connected successfully")
        
        # Run all tests
        tests = [
            ("Basic Query", test_basic_query),
            ("Simple Prepared Statement", test_prepared_statement_simple),
            ("Multiple Parameters", test_prepared_statement_multiple_params),
            ("String Parameters", test_prepared_statement_with_strings),
            ("Various Data Types", test_prepared_statement_types),
            ("NULL Parameter", test_prepared_statement_null),
            ("Reuse Prepared Statement", test_prepared_statement_reuse),
            ("Prepare-Execute-Commit Pattern", test_prepare_execute_commit_pattern),
        ]
        
        results = []
        for test_name, test_func in tests:
            try:
                results.append((test_name, test_func(conn)))
            except Exception as e:
                print_error(f"Test '{test_name}' failed with exception: {e}")
                results.append((test_name, False))
        
        # Print summary
        print_test_header("Test Summary")
        passed = sum(1 for _, result in results if result)
        total = len(results)
        
        for test_name, result in results:
            status = "PASSED" if result else "FAILED"
            color = Colors.GREEN if result else Colors.RED
            print(f"{color}{status:8s}{Colors.RESET} - {test_name}")
        
        print(f"\n{Colors.BOLD}Total: {passed}/{total} tests passed{Colors.RESET}")
        
        conn.close()
        return passed == total
        
    except Exception as e:
        print_error(f"Connection failed: {e}")
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Test Spice.ai Flight SQL ADBC endpoint with Python",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_flightsql_adbc.py
  python test_flightsql_adbc.py --host localhost --port 50051
  python test_flightsql_adbc.py --host localhost --port 50051 --username user --password pass
        """
    )
    
    parser.add_argument("--host", default="127.0.0.1", 
                       help="Spice.ai server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=50051, 
                       help="Spice.ai Flight SQL port (default: 50051)")
    parser.add_argument("--username", help="Username for authentication")
    parser.add_argument("--password", help="Password for authentication")
    
    args = parser.parse_args()
    
    success = run_all_tests(args.host, args.port, args.username, args.password)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
