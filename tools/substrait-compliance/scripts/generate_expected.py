#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb==1.5.6"]
# ///
"""Compute Mode A TPC-H goldens for a `--scale-factor` run.

`DuckDB` runs its standard TPC-H queries (the `tpch` extension's
`tpch_queries()`: the specification's text with its validation parameters)
over the CSVs the harness wrote with `--write-data`, so the goldens describe
exactly the rows a generated run registers. The tables take the TPC-H types —
`DECIMAL(15,2)` money, `DATE`, integer keys — so every sum is exact, and a
`DECIMAL` result column is declared `decimal(p,s)` in the golden's typed header,
which the harness compares exactly as a scaled integer.

From the spiceai repository root:

    cargo run -p spice-substrait-compliance -- --scale-factor 1 --write-data target/tpch-sf1
    uv run tools/substrait-compliance/scripts/generate_expected.py \\
        --data-dir target/tpch-sf1 --out tools/substrait-compliance/expected/sf1

`uv` installs the pinned `duckdb` from the header above; with plain `python3`,
`pip install duckdb` first. The first run downloads `DuckDB`'s `tpch` extension.
The script also writes `<out>/README.md`: the `DuckDB` version and a SHA-256 of
each input CSV, so a reviewer can regenerate the data and confirm the goldens
were computed on the same rows.
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import hashlib
import pathlib
import re

import duckdb

# TPC-H tables with the specification's column types, in the column order of
# the harness's `--write-data` CSVs (`src/schema.rs`).
TABLES: dict[str, str] = {
    "region": "r_regionkey INTEGER, r_name VARCHAR, r_comment VARCHAR",
    "nation": "n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR",
    "part": (
        "p_partkey INTEGER, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR, p_type VARCHAR, "
        "p_size INTEGER, p_container VARCHAR, p_retailprice DECIMAL(15,2), p_comment VARCHAR"
    ),
    "supplier": (
        "s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR, s_nationkey INTEGER, "
        "s_phone VARCHAR, s_acctbal DECIMAL(15,2), s_comment VARCHAR"
    ),
    "partsupp": (
        "ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER, "
        "ps_supplycost DECIMAL(15,2), ps_comment VARCHAR"
    ),
    "customer": (
        "c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR, c_nationkey INTEGER, "
        "c_phone VARCHAR, c_acctbal DECIMAL(15,2), c_mktsegment VARCHAR, c_comment VARCHAR"
    ),
    "orders": (
        "o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR, o_totalprice DECIMAL(15,2), "
        "o_orderdate DATE, o_orderpriority VARCHAR, o_clerk VARCHAR, o_shippriority INTEGER, "
        "o_comment VARCHAR"
    ),
    "lineitem": (
        "l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, "
        "l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), "
        "l_tax DECIMAL(15,2), l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, "
        "l_commitdate DATE, l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR, "
        "l_comment VARCHAR"
    ),
}

DECIMAL_TYPE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")


def type_token(duck_type: str) -> str:
    """The golden's typed-header token for a `DuckDB` result type.

    A type this does not know is an error rather than `string`: a numeric
    column compared as text would pass on formatting alone.
    """
    t = duck_type.upper()
    match = DECIMAL_TYPE.match(t)
    if match:
        return f"decimal({match.group(1)},{match.group(2)})"
    if t in ("BIGINT", "HUGEINT", "UBIGINT", "UINTEGER"):
        return "bigint"
    if t in ("INTEGER", "SMALLINT", "TINYINT", "USMALLINT", "UTINYINT"):
        return "integer"
    if t == "DOUBLE":
        return "double"
    if t == "FLOAT":
        return "float"
    if t == "DATE":
        return "date"
    if t == "VARCHAR":
        return "string"
    if t == "BOOLEAN":
        return "boolean"
    raise SystemExit(f"no golden type token for DuckDB type {duck_type}")


def cell(value: object) -> str:
    """One golden cell in the suite's pipe-delimited format.

    NULL is the empty cell; a string that is empty or would break the row is
    quoted, with embedded quotes doubled. Decimals print fixed-point at their
    scale, doubles as their shortest round-trip form.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value)
    if text == "" or any(c in text for c in '|"\r\n'):
        return '"' + text.replace('"', '""') + '"'
    return text


def load_tables(con: duckdb.DuckDBPyConnection, data_dir: pathlib.Path) -> dict[str, tuple[int, str]]:
    """Load every table from `data_dir`; return its row count and CSV SHA-256."""
    loaded = {}
    for table, columns in TABLES.items():
        path = data_dir / f"{table}.csv"
        if not path.is_file():
            raise SystemExit(f"missing {path}: write the tables with `--write-data {data_dir}` first")
        con.execute(f"CREATE TABLE {table} ({columns})")
        # Bind the path: a `--data-dir` may contain `'`, which would break a
        # quoted SQL literal. Explicit CSV quoting matches the harness writer;
        # no field is trimmed.
        con.execute(
            f"COPY {table} FROM ? (DELIMITER '|', HEADER false, QUOTE '\"', ESCAPE '\"')",
            [str(path)],
        )
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        lines = sum(1 for _ in path.open("rb"))
        if rows != lines:
            raise SystemExit(f"{table}: loaded {rows} rows from {lines} CSV lines")
        loaded[table] = (rows, hashlib.sha256(path.read_bytes()).hexdigest())
    return loaded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=pathlib.Path, required=True, help="CSVs from the harness's --write-data")
    parser.add_argument("--out", type=pathlib.Path, required=True, help="golden directory to (re)write")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    loaded = load_tables(con, args.data_dir)

    args.out.mkdir(parents=True, exist_ok=True)
    queries = con.execute("SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr").fetchall()
    if len(queries) != 22:
        raise SystemExit(f"tpch_queries() returned {len(queries)} queries, expected 22")
    for query_nr, sql in queries:
        rel = con.sql(sql)
        header = "|".join(f"{name}:{type_token(str(t))}" for name, t in zip(rel.columns, rel.types))
        rows = rel.fetchall()
        lines = [header] + ["|".join(cell(v) for v in row) for row in rows]
        path = args.out / f"q{query_nr:02d}.csv"
        path.write_text("\n".join(lines) + "\n")
        print(f"q{query_nr:02d}: {len(rows)} rows, {len(rel.columns)} columns -> {path}")

    readme = [
        "# Mode A TPC-H goldens",
        "",
        "Generated by `tools/substrait-compliance/scripts/generate_expected.py`: `DuckDB`",
        f"`{duckdb.__version__}` ran its `tpch_queries()` over the tables the harness wrote with",
        "`--write-data`. Do not edit by hand; regenerate (see the script) after a `tpchgen`",
        "version change.",
        "",
        "| Table | Rows | SHA-256 of the `--write-data` CSV |",
        "|-------|------|-----------------------------------|",
    ]
    readme += [f"| `{t}` | {rows} | `{digest}` |" for t, (rows, digest) in loaded.items()]
    (args.out / "README.md").write_text("\n".join(readme) + "\n")
    print(f"DuckDB {duckdb.__version__}; wrote {len(queries)} goldens to {args.out}")


if __name__ == "__main__":
    main()
