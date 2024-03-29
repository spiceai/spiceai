# Makefile

.PHONY: all

all: tpch-gen pg-load

tpch-init:
	@if [ ! -d "tpch-kit" ]; then \
		git clone https://github.com/gregrahn/tpch-kit.git; \
	fi
	@OS=`uname`; \
	if [ -z "$(MACHINE)" ]; then \
		if [ "$$OS" = "Linux" ]; then \
			MACHINE=LINUX; \
		elif [ "$$OS" = "Darwin" ]; then \
			MACHINE=MACOS; \
		else \
			echo "Unsupported operating system: $$OS."; \
			exit 1; \
		fi; \
	fi; \
	$(MAKE) -C tpch-kit/dbgen MACHINE=$$MACHINE DATABASE=POSTGRESQL

	@echo "Initialized successfully."

tpch-gen: tpch-init
	@# Check if DBGEN_SCALE is set, else default to 0.1
	$(eval DBGEN_SCALE ?= 0.1)
	(cd tpch-kit/dbgen && ./dbgen -vf -s $(DBGEN_SCALE))

	@mkdir -p ./tmp/queries

	@# temporarily skip 13.sql, 15.sql, 20.sql as they are unable to run (use `create view`)
	@for i in tpch-kit/dbgen/queries/*.sql; do \
		if [ `basename $$i` = "13.sql" ] || [ `basename $$i` = "15.sql" ] || [ `basename $$i` = "20.sql" ]; then \
			touch ./tmp/queries/`basename $$i`; \
			continue; \
		fi; \
		sed 's/;//g' $$i > ./tmp/queries/`basename $$i`; \
	done

	@(cd tpch-kit/dbgen && DSS_QUERY=../../tmp/queries ./qgen | sed 's/limit -1//' | sed 's/day (3)/day/' > ../../tpch_queries.sql)

	@echo "Test data and queries generated successfully."

# Example: PGPORT=5432 PGUSER=postgres make pg-init
pg-init:
	@psql -tAc "SELECT 1 FROM pg_database WHERE datname='tpch'" | grep -q 1 && \
	(echo "Database 'tpch' already exists, skipping creation." ) || \
	(createdb tpch && echo "Database 'tpch' created successfully.")
	
	@psql tpch -c "DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem;"
	@psql tpch -f ./tpch-kit/dbgen/dss.ddl

	@echo "Database 'tpch' has been successfuly created or updated."

# Example: PGPORT=5432 PGUSER=postgres make pg-load
pg-load:
	@for i in tpch-kit/dbgen/*.tbl; do \
		table=$$(basename $$i .tbl); \
		echo "Loading $$table..."; \
		sed 's/|$$//' $$i > ./tmp/$$table.tbl; \
		psql tpch -q -c "TRUNCATE $$table"; \
		psql tpch -c "\\copy $$table FROM './tmp/$$table.tbl' CSV DELIMITER '|';"; \
	done
	@echo "Benchmark dataset has been successfully loaded to 'tpch' database"

# Example: PGPORT=5432 PGUSER=postgres make tpch-run-pq
tpch-run-pq:
	psql tpch < ./tpch_queries.sql
