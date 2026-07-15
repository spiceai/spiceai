# Makefile

.PHONY: all

all: tpch-gen pg-load

tpch-init:
	@if [ ! -d "tpch-kit" ]; then \
		git clone https://github.com/spiceai/tpch-kit.git; \
		cd tpch-kit && git checkout 319892381ff1213ca794d449c04c5a31f2252d57; \
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

tpcds-init:
	@if [ ! -d "tpcds-kit" ]; then \
		git clone https://github.com/spiceai/tpcds-kit.git; \
		cd tpcds-kit && git checkout dd01f906992e1e39837526bffb3f5b759e83b3a8; \
	fi
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
	@for table in region nation customer supplier part partsupp orders lineitem; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' tpch-kit/dbgen/$$table.tbl > ./tmp/$$table.tbl; \
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
		sed 's/|$$//' tpch-kit/dbgen/$$table.tbl > ./tmp/$$table.tbl; \
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
		sed 's/|$$//' tpcds-kit/tools/tmp/data/$$table.dat > ./tmp/$$table.dat; \
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
# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-tpcds-create-index
pg-tpcds-create-index:
	@echo "Applying indexes from tpcds_index.sql..."
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -f tpcds_index.sql
	@echo "Indexes applied successfully."
	@echo "Analyzing $(DB_NAME) database"
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "ANALYZE;";
	@echo "Statistics have been refreshed for $(DB_NAME) database"
	@echo "Setting statement_timeout on $(DB_NAME) database"
	@psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) $(DB_NAME) -c "ALTER DATABASE \"$(DB_NAME)\" SET statement_timeout = '5400000';";

# Example: DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_NAME=tpch make pg-tpcds-load
pg-tpcds-load:
	@mkdir -p ./tmp
	@for table in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do \
		echo "Loading $$table..."; \
		sed 's/|$$//' tpcds-kit/tools/tmp/data/$$table.dat > ./tmp/$$table.dat; \
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
		cp tpch-kit/dbgen/$$table.tbl ./tmp/$$table.csv; \
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
		cp tpch-kit/dbgen/$$table.tbl ./mssql-tpch/data/$$table.tbl; \
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