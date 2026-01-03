.PHONY: help install install-uv test test-pip clean

help:
	@echo "Spice.ai Python ADBC Test Suite"
	@echo ""
	@echo "Available targets:"
	@echo "  install      - Install Python dependencies using uv (recommended)"
	@echo "  install-pip  - Install Python dependencies using pip"
	@echo "  test         - Run tests using uv (assumes Spice is running)"
	@echo "  test-pip     - Run tests using python directly"
	@echo "  clean        - Clean up Python cache files"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "Note: uv is recommended for faster execution. Install with:"
	@echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"

install:
	@echo "Installing Python dependencies using uv..."
	@command -v uv >/dev/null 2>&1 || { echo "Error: uv is not installed. Run: curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }
	uv pip install -r requirements.txt

install-pip:
	@echo "Installing Python dependencies using pip..."
	pip install -r requirements.txt

test:
	@echo "Running Python ADBC tests using uv..."
	@command -v uv >/dev/null 2>&1 || { echo "Error: uv is not installed. Use 'make test-pip' or install uv"; exit 1; }
	uv run test_flightsql_adbc.py

test-pip:
	@echo "Running Python ADBC tests using python..."
	python3 test_flightsql_adbc.py

clean:
	@echo "Cleaning up..."
	rm -rf __pycache__ *.pyc .pytest_cache
	@echo "Done!"
