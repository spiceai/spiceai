# Python ADBC Flight SQL Test for Spice.ai

This directory contains Python test scripts for testing the Spice.ai Flight SQL ADBC endpoint using the Python ADBC driver.

## Prerequisites

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver. Install it first:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using Homebrew on macOS
brew install uv

# Or using pip
pip install uv
```

Then install the required Python packages:

```bash
uv pip install adbc-driver-flightsql pyarrow
```

### Using pip (Alternative)

If you prefer traditional pip:

```bash
pip install adbc-driver-flightsql pyarrow
```

Or using a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install adbc-driver-flightsql pyarrow
```

## Running the Tests

### Basic Usage

Run the test script against a local Spice.ai instance:

```bash
# Using uv (recommended)
uv run test_flightsql_adbc.py

# Or using python directly
python test_flightsql_adbc.py
```

This will connect to `127.0.0.1:50051` by default.

### Custom Host and Port

Specify a different host and port:

```bash
# Using uv
uv run test_flightsql_adbc.py --host localhost --port 50051

# Or using python
python test_flightsql_adbc.py --host localhost --port 50051
```

### With Authentication

If your Spice.ai instance requires authentication:

```bash
# Using uv
uv run test_flightsql_adbc.py --host localhost --port 50051 --username myuser --password mypass

# Or using python
python test_flightsql_adbc.py --host localhost --port 50051 --username myuser --password mypass
```

## Test Coverage

The test script covers the following scenarios:

1. **Basic SQL Query**: Simple query without parameters
2. **Simple Prepared Statement**: Single parameter binding
3. **Multiple Parameters**: Prepared statement with multiple parameters
4. **String Parameters**: Prepared statement with string concatenation
5. **Various Data Types**: Testing int, float, string, and boolean parameters
6. **NULL Parameter**: Testing NULL value handling
7. **Reuse Prepared Statement**: Executing the same prepared statement multiple times with different parameters
8. **Prepare-Execute-Commit Pattern**: Full workflow testing with prepare, execute, reuse, and commit

## Expected Output

When all tests pass, you should see output like:

```
============================================================
Test Summary
============================================================
PASSED   - Basic Query
PASSED   - Simple Prepared Statement
PASSED   - Multiple Parameters
PASSED   - String Parameters
PASSED   - Various Data Types
PASSED   - NULL Parameter
PASSED   - Reuse Prepared Statement
PASSED   - Prepare-Execute-Commit Pattern

Total: 8/8 tests passed
```

### Current Known Issues

Some tests currently fail due to DataFusion parameter type inference during prepared statement creation:

- **Multiple Parameters**: Fails when multiple parameters can't be type-inferred (e.g., `SELECT ? + ?`)
- **Various Data Types**: Fails when field types can't be determined from NULL placeholders
- **Reuse Prepared Statement**: Fails with arithmetic operations on untyped parameters
- **Prepare-Execute-Commit Pattern**: Fails with multiple untyped parameters

These issues are being addressed in the runtime. The tests work correctly once the fixes are deployed.

## Running Against a Running Spice.ai Instance

If you have a Spice.ai runtime already running:

```bash
# In one terminal, start Spice.ai
cd /path/to/spice
spice run

# In another terminal, run the tests
cd /path/to/spice2/test/adbc/python
python test_flightsql_adbc.py --port $(spice config get flight_port)
```

## Troubleshooting

### Connection Refused

If you get a "Connection refused" error:

- Make sure Spice.ai is running
- Verify the correct port (default Flight SQL port is 50051)
- Check firewall settings

### Import Errors

If you get import errors:

- Make sure you've installed the required packages: `uv pip install adbc-driver-flightsql pyarrow` or `pip install -r requirements.txt`
- Verify you're using Python 3.7 or later
- If using uv, ensure it's properly installed: `uv --version`

### Authentication Errors

If you get authentication errors:

- Verify your username and password are correct
- Check if the Spice.ai instance requires authentication

## Integration with Automated Testing

To integrate this test into automated testing:

```bash
# Run tests and check exit code (using uv)
uv run test_flightsql_adbc.py
if [ $? -eq 0 ]; then
    echo "All tests passed!"
else
    echo "Some tests failed!"
    exit 1
fi

# Or using python directly
python test_flightsql_adbc.py
if [ $? -eq 0 ]; then
    echo "All tests passed!"
else
    echo "Some tests failed!"
    exit 1
fi
```

## Development

To add new tests:

1. Create a new test function following the pattern:

   ```python
   def test_my_new_test(conn) -> bool:
       """Test description"""
       print_test_header("Test N: My New Test")
       try:
           # Test implementation
           cursor = conn.cursor()
           # ... test code ...
           print_success("Test passed")
           return True
       except Exception as e:
           print_error(f"Test failed: {e}")
           return False
   ```

2. Add the test to the `tests` list in `run_all_tests()`:
   ```python
   tests = [
       # ... existing tests ...
       ("My New Test", test_my_new_test),
   ]
   ```

## See Also

- [Apache Arrow ADBC Documentation](https://arrow.apache.org/adbc/)
- [Flight SQL Documentation](https://arrow.apache.org/docs/format/FlightSql.html)
- [Spice.ai Documentation](https://docs.spiceai.org/)
