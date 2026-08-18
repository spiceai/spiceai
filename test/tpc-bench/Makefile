# Makefile

.PHONY: all

all: tpch-gen pg-load

# Directory the *-load targets read TPC-H .tbl files from. dbgen writes its output
# next to the binary, so that is the default; tpch-fleet-gen writes to
# $(TPCH_FLEET_DIR) instead, so a caller using it must pass TPCH_DATA_DIR to match.
TPCH_DATA_DIR ?= tpch-kit/dbgen

# Directory the *-tpcds-load targets read TPC-DS .dat files from. dsdgen writes to
# tpcds-kit/tools/tmp/data (see tpcds-gen), so that is the default; tpcds-fleet-gen
# writes to $(TPCDS_FLEET_DIR) instead, so a caller using it must pass
# TPCDS_DATA_DIR to match.
TPCDS_DATA_DIR ?= tpcds-kit/tools/tmp/data

tpch-clone:
	@if [ ! -d "tpch-kit" ]; then \
		git clone https://github.com/spiceai/tpch-kit.git; \
		cd tpch-kit && git checkout 319892381ff1213ca794d449c04c5a31f2252d57; \
	fi

tpch-init: tpch-clone
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

tpcds-clone:
	@if [ ! -d "tpcds-kit" ]; then \
		git clone https://github.com/spiceai/tpcds-kit.git; \
		cd tpcds-kit && git checkout dd01f906992e1e39837526bffb3f5b759e83b3a8; \
	fi

tpcds-init: tpcds-clone
	@OS=`uname`; \
	if [ -z "$(OS)" ]; then \
		if [ "$$OS" = "Linux" ]; then \
			OS=LINUX; \
			$(MAKE) -C tpcds-kit/tools CC=gcc-9 OS=$$OS; \
		elif [ "$$OS" = "Darwin" ]; then \
			OS=MACOS; \
			$(MAKE) -C tpcds-kit/tools OS=$$OS; \
		else \
			echo "Unsupported operating system: $$OS."; \
			exit 1; \
		fi; \
	fi; \
	$(MAKE) -C tpcds-kit/tools OS=$$OS

	@echo "Initialized successfully."

tpcds-gen:
	@(cd tpcds-kit/tools && mkdir -p ./tmp/data)

	@# Check if DBGEN_SCALE is set, else default to 1
	$(eval DBGEN_SCALE ?= 1)
	(cd tpcds-kit/tools && ./dsdgen -dir ./tmp/data -scale $(DBGEN_SCALE))

	@echo "Test data is generated successfully."

tpch-gen: tpch-init
	@# Check if DBGEN_SCALE is set, else default to 1
	$(eval DBGEN_SCALE ?= 1)
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

# tpch-fleet-gen generates the same TPC-H dataset the rest of the benchmark fleet
# runs on - s3://spiceai-public-datasets/tpch and the hosted MySQL, Iceberg, Glue,
# Databricks, Snowflake and Spice Cloud copies - which is what
# crates/test-framework/src/queries/validation/tpch/*.csv holds the expected
# answers for. That dataset comes from DuckDB's tpch extension, and DuckDB's
# generator disagrees with tpch-kit's dbgen on the free-text columns (c_address,
# s_address, and every *_comment): keys, numerics, dates and even the string
# lengths are identical, but the characters differ. That changes 4 of the 22 TPC-H
# answers - Q2 and Q20 (s_address), Q10 (c_address), and Q13, whose predicate reads
# o_comment - so a Postgres database seeded with dbgen data fails result validation
# on exactly those four queries no matter how correct the engine under test is.
# dbgen is the officially correct generator (its output matches the TPC-supplied
# answers in tpch-kit's dbgen/answers/); the fleet's dataset is the one that
# diverges, tracked separately in #12137. Until that is resolved, seeding from the
# same generator the answers were produced with is what makes a result mismatch
# mean a real bug.
#
# Data only - qgen/tpch_queries.sql still comes from tpch-gen.
# Example: DBGEN_SCALE=1 make tpch-fleet-gen
TPCH_FLEET_DIR ?= ./tmp/fleet

# Pinned so the generated dataset cannot drift with whatever duckdb happens to be
# on PATH. v1.5.4's output was verified row-for-row identical to the fleet dataset
# across all 8 tables (0 rows differing in either direction, 6,001,215 lineitem
# rows). Override DUCKDB to use an already-installed CLI.
DUCKDB_VERSION ?= v1.5.4
DUCKDB ?= ./tmp/bin/duckdb

$(DUCKDB):
	@mkdir -p ./tmp/bin
	@OS=`uname`; ARCH=`uname -m`; \
	case "$$OS/$$ARCH" in \
		Linux/x86_64) ASSET=duckdb_cli-linux-amd64.zip ;; \
		Linux/aarch64 | Linux/arm64) ASSET=duckdb_cli-linux-arm64.zip ;; \
		Darwin/*) ASSET=duckdb_cli-osx-universal.zip ;; \
		*) echo "No DuckDB CLI release asset for $$OS/$$ARCH - install duckdb and re-run with DUCKDB=\`command -v duckdb\`"; exit 1 ;; \
	esac; \
	echo "Downloading DuckDB CLI $(DUCKDB_VERSION) ($$ASSET)..."; \
	curl -fsSL --retry 3 -o ./tmp/bin/duckdb.zip "https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/$$ASSET"
	@unzip -oq ./tmp/bin/duckdb.zip -d ./tmp/bin
	@rm -f ./tmp/bin/duckdb.zip
	@chmod +x "$(DUCKDB)"
	@"$(DUCKDB)" --version

tpch-fleet-gen: $(DUCKDB)
	@# Check if DBGEN_SCALE is set, else default to 1
	$(eval DBGEN_SCALE ?= 1)
	@mkdir -p "$(TPCH_FLEET_DIR)"
	@rm -f "$(TPCH_FLEET_DIR)"/*.tbl "$(TPCH_FLEET_DIR)/gen.duckdb" "$(TPCH_FLEET_DIR)/gen.duckdb.wal"
	@echo "Generating TPC-H scale factor $(DBGEN_SCALE)..."
	@"$(DUCKDB)" "$(TPCH_FLEET_DIR)/gen.duckdb" -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=$(DBGEN_SCALE));" > /dev/null
	@# QUOTE '' disables CSV quoting so no field is ever wrapped, and the trailing
	@# empty column reproduces dbgen's trailing '|'; the result is byte-compatible
	@# with dbgen's .tbl format, so every *-load target reads it unchanged. Tables
	@# are dropped as they are written to bound peak disk at the larger scales.
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Writing $$table.tbl..."; \
		"$(DUCKDB)" "$(TPCH_FLEET_DIR)/gen.duckdb" -c "\
			COPY (SELECT *, '' FROM $$table) TO '$(TPCH_FLEET_DIR)/$$table.tbl' (FORMAT CSV, DELIMITER '|', HEADER false, QUOTE ''); \
			DROP TABLE $$table; \
			CHECKPOINT;" > /dev/null || exit 1; \
	done
	@rm -f "$(TPCH_FLEET_DIR)/gen.duckdb" "$(TPCH_FLEET_DIR)/gen.duckdb.wal"
	@echo "Test data generated successfully in $(TPCH_FLEET_DIR) - pass TPCH_DATA_DIR=$(TPCH_FLEET_DIR) to the load targets."

# tpcds-fleet-gen is the TPC-DS analog of tpch-fleet-gen above: it generates the
# same TPC-DS dataset the rest of the benchmark fleet runs on - the s3/file
# parquet copies and the hosted MySQL and Spice Cloud databases - which is what
# crates/test-framework/src/queries/validation/tpcds/*.csv holds the expected
# answers for. That dataset comes from DuckDB's tpcds extension, a
# reimplementation whose RNG diverges from tpcds-kit's C dsdgen: schemas and row
# counts are identical, but which customer bought which item on which ticket
# differs, so cross-fact-table join incidence differs. Queries whose canonical
# SF1 answers hold only 1-2 rows (q17, q25, q85 - store/catalog/web sales
# joined to their returns) come back genuinely empty from a tpcds-kit-seeded
# database no matter how correct the engine under test is, and value-level
# result snapshots differ broadly. Seeding from the same generator the expected
# answers were produced with is what makes a result mismatch mean a real bug.
#
# Data only - the DDL (tpcds.sql) and foreign keys (tpcds_ri.sql) still come
# from tpcds-kit, which tpcds-clone fetches without compiling the C tools.
# Example: DBGEN_SCALE=1 make tpcds-fleet-gen
TPCDS_FLEET_DIR ?= ./tmp/tpcds-fleet

TPCDS_TABLES = call_center catalog_page catalog_returns catalog_sales customer \
	customer_address customer_demographics date_dim household_demographics \
	income_band inventory item promotion reason ship_mode store store_returns \
	store_sales time_dim warehouse web_page web_returns web_sales web_site

tpcds-fleet-gen: $(DUCKDB)
	@# Check if DBGEN_SCALE is set, else default to 1
	$(eval DBGEN_SCALE ?= 1)
	@mkdir -p "$(TPCDS_FLEET_DIR)"
	@rm -f "$(TPCDS_FLEET_DIR)"/*.dat "$(TPCDS_FLEET_DIR)/gen.duckdb" "$(TPCDS_FLEET_DIR)/gen.duckdb.wal"
	@echo "Generating TPC-DS scale factor $(DBGEN_SCALE)..."
	@"$(DUCKDB)" "$(TPCDS_FLEET_DIR)/gen.duckdb" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=$(DBGEN_SCALE));" > /dev/null
	@# QUOTE '' disables CSV quoting so no field is ever wrapped, and the trailing
	@# empty column reproduces dsdgen's trailing '|'; NULLs export as empty fields,
	@# exactly as dsdgen writes them. The result is byte-compatible with dsdgen's
	@# .dat format, so every *-tpcds-load target reads it unchanged. Tables are
	@# dropped as they are written to bound peak disk at the larger scales.
	@for table in $(TPCDS_TABLES); do \
		echo "Writing $$table.dat..."; \
		"$(DUCKDB)" "$(TPCDS_FLEET_DIR)/gen.duckdb" -c "\
			COPY (SELECT *, '' FROM $$table) TO '$(TPCDS_FLEET_DIR)/$$table.dat' (FORMAT CSV, DELIMITER '|', HEADER false, QUOTE ''); \
			DROP TABLE $$table; \
			CHECKPOINT;" > /dev/null || exit 1; \
	done
	@rm -f "$(TPCDS_FLEET_DIR)/gen.duckdb" "$(TPCDS_FLEET_DIR)/gen.duckdb.wal"
	@echo "Test data generated successfully in $(TPCDS_FLEET_DIR) - pass TPCDS_DATA_DIR=$(TPCDS_FLEET_DIR) to the load targets."

# Default value for DB_NAME
DB_NAME ?= tpch

# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-init
pg-init:
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -tAc "SELECT 1 FROM pg_database WHERE datname='$(DB_NAME)'" | grep -q 1 && \
	(echo "Database $(DB_NAME) already exists, skipping creation." ) || \
	(createdb -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) && echo "Database $(DB_NAME) created successfully.")
	
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem;"
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -f ./tpch-kit/dbgen/dss.ddl

	@echo "Database $(DB_NAME) has been successfuly created or updated."

# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-load
pg-load:
	@mkdir -p ./tmp
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' "$(TPCH_DATA_DIR)/$$table.tbl" > ./tmp/$$table.tbl; \
		psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "\\copy $$table FROM './tmp/$$table.tbl' CSV DELIMITER '|';"; \
	done
	@echo "Benchmark dataset has been successfully loaded to $(DB_NAME) database"

# tpch-kit's dbgen/dss.ddl (used by pg-init) creates bare tables with no
# PRIMARY KEY, FOREIGN KEY, or index of any kind - unlike a hand-written TPC-H
# DDL (e.g. the postgres cookbook's load-tpch.py), which declares real keys.
# So this target must create the nominal TPC-H primary/unique-key indexes
# itself, not assume dss.ddl already created them, plus the secondary
# indexes covering join/filter columns the TPC-H queries in
# crates/test-framework/src/queries/tpch actually use.
# idx_lineitem_l_partkey_l_suppkey must be the composite (l_partkey, l_suppkey),
# not a standalone l_partkey index: Q20's correlated subquery filters lineitem
# on both columns together, and without the composite it falls back to a full
# scan per outer row (observed ~1h49m vs ~0.1s with the composite).
# Missing the supplier(s_suppkey) key index specifically starves Q2's
# correlated MIN(ps_supplycost) subquery of an index-driven nested loop,
# forcing Postgres to rebuild a full supplier/nation/region hash join on every
# one of tens of thousands of correlated invocations instead of a cheap probe.
# ANALYZE runs after index creation so the planner has fresh statistics on the
# just-bulk-loaded tables; without it, correlated-subquery queries (Q2, Q20)
# can get planned as per-row full/seq scans instead of index lookups.
# statement_timeout is set on the database as a backstop: a cancelled CI run
# only stops the GitHub runner, not the Postgres backend connection - we've
# observed cancelled benchmark queries keep running server-side for hours,
# competing for resources with every subsequent run. 90 minutes gives ample
# headroom over the slowest known-good query (~52 min at SF100) while
# bounding how long a leaked connection can hold resources.
# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-create-index
pg-create-index:
	@echo "Creating primary/unique-key indexes in $(DB_NAME) database"
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "DROP INDEX IF EXISTS idx_part_p_partkey;";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "DROP INDEX IF EXISTS idx_partsupp_ps_partkey;";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_region_pk ON region (r_regionkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_nation_pk ON nation (n_nationkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_part_pk ON part (p_partkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_pk ON supplier (s_suppkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_pk ON customer (c_custkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_pk ON orders (o_orderkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_partsupp_pk ON partsupp (ps_partkey, ps_suppkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE UNIQUE INDEX IF NOT EXISTS idx_lineitem_pk ON lineitem (l_orderkey, l_linenumber);";
	@echo "Creating secondary indexes in $(DB_NAME) database"
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "DROP INDEX IF EXISTS idx_lineitem_l_partkey;";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_lineitem_l_partkey_l_suppkey ON lineitem (l_partkey, l_suppkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_lineitem_l_suppkey ON lineitem (l_suppkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_lineitem_l_shipdate ON lineitem (l_shipdate);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_orders_o_custkey ON orders (o_custkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_orders_o_orderdate ON orders (o_orderdate);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_customer_c_nationkey ON customer (c_nationkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_supplier_s_nationkey ON supplier (s_nationkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_nation_n_regionkey ON nation (n_regionkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_partsupp_ps_suppkey ON partsupp (ps_suppkey);";
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "CREATE INDEX IF NOT EXISTS idx_part_p_brand_p_container ON part (p_brand, p_container);";
	@echo "Indexes have been successfully created to $(DB_NAME) database"
	@echo "Analyzing $(DB_NAME) database"
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "ANALYZE;";
	@echo "Statistics have been refreshed for $(DB_NAME) database"
	@echo "Setting statement_timeout on $(DB_NAME) database"
	@psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} $(DB_NAME) -c "ALTER DATABASE \"$(DB_NAME)\" SET statement_timeout = '5400000';";

# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make tpch-run-pq
tpch-run-pq:
	psql $(DB_NAME) < ./tpch_queries.sql

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root DB_NAME=tpch ADD_INDEXES=true make mysql-init
mysql-init:
	@echo "Checking if database '$(DB_NAME)' exists..."
	@if mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "USE $(DB_NAME);" 2>/dev/null; then \
		echo "Database '$(DB_NAME)' already exists, skipping creation."; \
	else \
		echo "Creating database '$(DB_NAME)'..."; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "CREATE DATABASE $(DB_NAME);"; \
		echo "Database '$(DB_NAME)' created successfully."; \
	fi

	@echo "Dropping existing tables if they exist..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) -e "\
		DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem;"

	@echo "Creating tables from dss.ddl..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) < ./tpch-kit/dbgen/dss.ddl

	@if [ "$(ADD_INDEXES)" = "true" ] || [ "$(ADD_INDEXES)" = "1" ]; then \
		echo "Applying indexes from tpch_index.sql..."; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) < tpch_index.sql; \
		echo "Indexes applied successfully."; \
	fi

	@echo "Database '$(DB_NAME)' has been successfully created or updated."

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root DB_NAME=tpch make mysql-load
mysql-load:
	@mkdir -p ./tmp
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' "$(TPCH_DATA_DIR)/$$table.tbl" > ./tmp/$$table.tbl; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) --local-infile=1 $(DB_NAME) -e "LOAD DATA LOCAL INFILE './tmp/$$table.tbl' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';"; \
	done
	@echo "Benchmark dataset has been successfully loaded to '$(DB_NAME)' database."

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root DB_NAME=tpcds make mysql-tpcds-init
mysql-tpcds-init:
	@echo "Checking if database '$(DB_NAME)' exists..."
	@if mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "USE $(DB_NAME);" 2>/dev/null; then \
		echo "Database '$(DB_NAME)' already exists, skipping creation."; \
	else \
		echo "Creating database '$(DB_NAME)'..."; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "CREATE DATABASE $(DB_NAME);"; \
		echo "Database '$(DB_NAME)' created successfully."; \
	fi

	@echo "Dropping existing tables if they exist..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) -e "\
		DROP TABLE IF EXISTS call_center, catalog_page, catalog_returns, catalog_sales, customer, customer_address, customer_demographics, date_dim, dbgen_version, household_demographics, income_band, inventory, item, promotion, reason, ship_mode, store, store_returns, store_sales, time_dim, warehouse, web_page, web_returns, web_sales, web_site;"

	@echo "Creating tables from tpcds.sql..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) < ./tpcds-kit/tools/tpcds.sql

	@if [ "$(ADD_INDEXES)" = "true" ] || [ "$(ADD_INDEXES)" = "1" ]; then \
        echo "Applying indexes from tpcds_index.sql..."; \
        mysql -h$(DB_HOST) -u$(DB_USER)  -p$(DB_PASS) -P$(DB_PORT) $(DB_NAME) < tpcds_index.sql; \
        echo "Indexes applied successfully."; \
    fi

	@echo "Database '$(DB_NAME)' has been successfully created or updated."

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root DB_NAME=tpcds make mysql-tpcds-load
mysql-tpcds-load:
	@mkdir -p ./tmp
	@for table in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' "$(TPCDS_DATA_DIR)/$$table.dat" > ./tmp/$$table.dat; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) --local-infile=1 $(DB_NAME) -e "LOAD DATA LOCAL INFILE './tmp/$$table.dat' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';"; \
	done
	@echo "Benchmark dataset has been successfully loaded to '$(DB_NAME)' database."


# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-init
pg-tpcds-init:
	@echo "Checking if database '$(DB_NAME)' exists..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -tAc "SELECT 1 FROM pg_database WHERE datname='$(DB_NAME)'" | grep -q 1 && \
		(echo "Database '$(DB_NAME)' already exists, skipping creation.") || \
		(createdb -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) && echo "Database '$(DB_NAME)' created successfully.")

	@echo "Dropping existing tables if they exist..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "\
		DROP TABLE IF EXISTS call_center, catalog_page, catalog_returns, catalog_sales, customer, customer_address, customer_demographics, date_dim, dbgen_version, household_demographics, income_band, inventory, item, promotion, reason, ship_mode, store, store_returns, store_sales, time_dim, warehouse, web_page, web_returns, web_sales, web_site;"

	@echo "Creating tables from tpcds.sql..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -f ./tpcds-kit/tools/tpcds.sql

	@echo "Database '$(DB_NAME)' has been successfully created or updated."

# Indexes are built after pg-tpcds-load, not as part of pg-tpcds-init: building
# them against empty tables means every bulk \copy in pg-tpcds-load has to
# incrementally maintain each index row-by-row, instead of one efficient
# sort-and-build pass over already-loaded data.
# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpcds make pg-tpcds-create-index
pg-tpcds-create-index:
	@echo "Applying indexes from tpcds_index.sql..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -f tpcds_index.sql
	@echo "Indexes applied successfully."
	@echo "Analyzing $(DB_NAME) database"
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "ANALYZE;";
	@echo "Statistics have been refreshed for $(DB_NAME) database"
	@echo "Setting statement_timeout on $(DB_NAME) database"
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "ALTER DATABASE \"$(DB_NAME)\" SET statement_timeout = '5400000';";

# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpcds make pg-tpcds-load
pg-tpcds-load:
	@mkdir -p ./tmp
	@for table in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' "$(TPCDS_DATA_DIR)/$$table.dat" > ./tmp/$$table.dat; \
		psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "\\copy $$table FROM './tmp/$$table.dat' CSV DELIMITER '|';"; \
	done
	@echo "Applying foreign keys from tpcds_ri.sql..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -f ./tpcds-kit/tools/tpcds_ri.sql
	@echo "Benchmark dataset has been successfully loaded to '$(DB_NAME)' database."

file-tpch-init:
	@rm -rf ./tmp
	@mkdir -p ./tmp
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Preparing $$table..."; \
		cp "$(TPCH_DATA_DIR)/$$table.tbl" ./tmp/$$table.csv; \
		python ./tpch_file.py ./tmp/$$table; \
		rm ./tmp/$$table.csv; \
	done

file-tpch-load:
	@mkdir -p ./file-tpch-spicepod
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Loading $$table..."; \
		cp ./tmp/$$table.parquet ./file-tpch-spicepod/$$table.parquet; \
	done
	@echo "version: v1" >> ./file-tpch-spicepod/spicepod.yaml
	@echo "kind: Spicepod" >> ./file-tpch-spicepod/spicepod.yaml
	@echo "name: file" >> ./file-tpch-spicepod/spicepod.yaml
	@echo "datasets:" >> ./file-tpch-spicepod/spicepod.yaml
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "  - name: $$table" >> ./file-tpch-spicepod/spicepod.yaml; \
		echo "    from: file:./$$table.parquet" >> ./file-tpch-spicepod/spicepod.yaml; \
	done

file-tpcds-init:
	@echo "Generating TPC-DS data at scale factor $(SCALE_FACTOR) and exporting to Parquet..."
	@echo "Running DuckDB to generate TPC-DS data..."
	duckdb tpcds.db -c "LOAD tpcds; CALL dsdgen(sf = 1);"
	@mkdir -p ./tmp
    # Export each table as a Parquet file
	@for table in call_center catalog_page catalog_returns catalog_sales customer \
                  customer_address customer_demographics date_dim household_demographics \
                  income_band inventory item promotion reason ship_mode store store_returns \
                  store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "Exporting $$table to $$table.parquet"; \
		duckdb tpcds.db "COPY $$table TO './tmp/$$table.parquet' (FORMAT 'parquet');"; \
	done
	@rm -rf tpcds.db

file-tpcds-load:
	@mkdir -p ./file-tpcds-spicepod
	@for table in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "Loading $$table..."; \
		cp ./tmp/$$table.parquet ./file-tpcds-spicepod/$$table.parquet; \
	done
	@echo "version: v1" >> ./file-tpcds-spicepod/spicepod.yaml
	@echo "kind: Spicepod" >> ./file-tpcds-spicepod/spicepod.yaml
	@echo "name: file" >> ./file-tpcds-spicepod/spicepod.yaml
	@echo "datasets:" >> ./file-tpcds-spicepod/spicepod.yaml
	@for table in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "  - name: $$table" >> ./file-tpcds-spicepod/spicepod.yaml; \
		echo "    from: file:./$$table.parquet" >> ./file-tpcds-spicepod/spicepod.yaml; \
	done

mssql-init:
	sqlcmd -U $(DB_USER) -P $(DB_PASS) -S $(DB_HOST) -d master -Q "IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'$(DB_NAME)') CREATE DATABASE $(DB_NAME);"
	sqlcmd -U $(DB_USER) -P $(DB_PASS) -S $(DB_HOST) -d $(DB_NAME) -i ./mssql-tpch/up.sql
	@echo "Database $(DB_NAME) has been successfuly created or updated."

mssql-load:
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		cp "$(TPCH_DATA_DIR)/$$table.tbl" ./mssql-tpch/data/$$table.tbl; \
	done
	chmod 755 ./mssql-tpch/data
	sqlcmd -U $(DB_USER) -P $(DB_PASS) -S $(DB_HOST) -d $(DB_NAME) -i ./mssql-tpch/insert.sql

mssql-backup:
	chmod 777 ./mssql-tpch/data
	sqlcmd -U $(DB_USER) -P $(DB_PASS) -S $(DB_HOST) -Q "BACKUP DATABASE $(DB_NAME) TO DISK = '/data/$(DB_NAME).bak' WITH FORMAT"
	chmod 755 ./mssql-tpch/data

mssql-restore:
	chmod 755 ./mssql-tpch/data
	sqlcmd -U $(DB_USER) -P $(DB_PASS) -S $(DB_HOST) -Q "RESTORE DATABASE $(DB_NAME) FROM DISK = '/data/$(DB_NAME).bak' WITH REPLACE"