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

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root make mysql-init
mysql-init:
	@echo "Checking if database 'tpch' exists..."
	@if mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "USE tpch;" 2>/dev/null; then \
		echo "Database 'tpch' already exists, skipping creation."; \
	else \
		echo "Creating database 'tpch'..."; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) -e "CREATE DATABASE tpch;"; \
		echo "Database 'tpch' created successfully."; \
	fi

	@echo "Dropping existing tables if they exist..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) tpch -e "\
		DROP TABLE IF EXISTS nation, region, part, supplier, partsupp, customer, orders, lineitem;"

	@echo "Creating tables from dss.ddl..."
	@mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) tpch < ./tpch-kit/dbgen/dss.ddl

	@echo "Database 'tpch' has been successfully created or updated."

# Example: DB_HOST=localhost DB_PORT=3306 DB_USER=root DB_PASS=root make mysql-load
mysql-load:
	@mkdir -p ./tmp
	@for i in tpch-kit/dbgen/*.tbl; do \
		table=$$(basename $$i .tbl); \
		echo "Loading $$table..."; \
		sed 's/|$$//' $$i > ./tmp/$$table.tbl; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) tpch -e "TRUNCATE TABLE $$table;"; \
		mysql -h$(DB_HOST) -u$(DB_USER) -p$(DB_PASS) -P$(DB_PORT) --local-infile=1 tpch -e "LOAD DATA LOCAL INFILE './tmp/$$table.tbl' INTO TABLE $$table FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';"; \
	done
	@echo "Benchmark dataset has been successfully loaded to 'tpch' database"