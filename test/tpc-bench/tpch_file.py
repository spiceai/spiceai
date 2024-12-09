import sys
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
import os

# Define column names and their respective data types for TPC-H tables
COLUMN_DEFINITIONS = {
    "customer": {
        "columns": [
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
        "dtypes": {
            "c_custkey": "int64",
            "c_name": "string",
            "c_address": "string",
            "c_nationkey": "int64",
            "c_phone": "string",
            "c_acctbal": "decimal",
            "c_mktsegment": "string",
            "c_comment": "string",
        },
    },
    "lineitem": {
        "columns": [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        ],
        "dtypes": {
            "l_orderkey": "int64",
            "l_partkey": "int64",
            "l_suppkey": "int64",
            "l_linenumber": "int64",
            "l_quantity": "decimal",
            "l_extendedprice": "decimal",
            "l_discount": "decimal",
            "l_tax": "decimal",
            "l_returnflag": "string",
            "l_linestatus": "string",
            "l_shipdate": "datetime64[ns]",
            "l_commitdate": "datetime64[ns]",
            "l_receiptdate": "datetime64[ns]",
            "l_shipinstruct": "string",
            "l_shipmode": "string",
            "l_comment": "string",
        },
    },
    "nation": {
        "columns": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
        "dtypes": {
            "n_nationkey": "int64",
            "n_name": "string",
            "n_regionkey": "int64",
            "n_comment": "string",
        },
    },
    "orders": {
        "columns": [
            "o_orderkey",
            "o_custkey",
            "o_orderstatus",
            "o_totalprice",
            "o_orderdate",
            "o_orderpriority",
            "o_clerk",
            "o_shippriority",
            "o_comment",
        ],
        "dtypes": {
            "o_orderkey": "int64",
            "o_custkey": "int64",
            "o_orderstatus": "string",
            "o_totalprice": "decimal",
            "o_orderdate": "datetime64[ns]",
            "o_orderpriority": "string",
            "o_clerk": "string",
            "o_shippriority": "int64",
            "o_comment": "string",
        },
    },
    "part": {
        "columns": [
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
        ],
        "dtypes": {
            "p_partkey": "int64",
            "p_name": "string",
            "p_mfgr": "string",
            "p_brand": "string",
            "p_type": "string",
            "p_size": "int64",
            "p_container": "string",
            "p_retailprice": "decimal",
            "p_comment": "string",
        },
    },
    "partsupp": {
        "columns": [
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ],
        "dtypes": {
            "ps_partkey": "int64",
            "ps_suppkey": "int64",
            "ps_availqty": "int64",
            "ps_supplycost": "decimal",
            "ps_comment": "string",
        },
    },
    "region": {
        "columns": ["r_regionkey", "r_name", "r_comment"],
        "dtypes": {"r_regionkey": "int64", "r_name": "string", "r_comment": "string"},
    },
    "supplier": {
        "columns": [
            "s_suppkey",
            "s_name",
            "s_address",
            "s_nationkey",
            "s_phone",
            "s_acctbal",
            "s_comment",
        ],
        "dtypes": {
            "s_suppkey": "int64",
            "s_name": "string",
            "s_address": "string",
            "s_nationkey": "int64",
            "s_phone": "string",
            "s_acctbal": "decimal",
            "s_comment": "string",
        },
    },
}


def csv_to_parquet(csv_file, parquet_file):
    """
    Converts a CSV file without a header row into a Parquet file with custom column names.
    Enforces column data types and appends the column names as the header row in the input CSV file.

    Args:
        csv_file (str): Path to the input CSV file.
        parquet_file (str): Path to the output Parquet file.
        delimiter (str): Delimiter used in the CSV file. Default is '|'.
        compression (str): Compression type for the Parquet file. Default is 'snappy'.
    """
    # Extract the table name from the file name (without extension)
    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    # Get column definitions for the table
    table_def = COLUMN_DEFINITIONS.get(table_name)
    if not table_def:
        raise ValueError(f"No column definitions found for table '{table_name}'.")

    column_names = table_def["columns"]
    column_dtypes = table_def["dtypes"]

    # Read the CSV file without a header
    df = pd.read_csv(
        csv_file,
        header=None,
        delimiter="|",
        names=column_names,
        parse_dates=[
            col for col, dtype in column_dtypes.items() if dtype.startswith("datetime")
        ],
    )

    for col, dtype in column_dtypes.items():
        if dtype == "decimal":
            df[col] = df[col].apply(
                lambda x: Decimal(x).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            )

    # Save the DataFrame as a Parquet file with compression
    df.to_parquet(parquet_file, index=False, compression="gzip")
    print(f"Parquet file saved as {parquet_file}")


### Converts the TPC-H CSV file to a Parquet file
### The TPC-H CSV files generate without headers, so column names and types are defined in COLUMN_DEFINITIONS
###
### Usage: python tpch_file.py <input file name without extension>
if __name__ == "__main__":
    input = sys.argv.pop()
    input_csv = input + ".csv"
    output_parquet = input + ".parquet"

    # Convert the CSV to Parquet
    csv_to_parquet(input_csv, output_parquet)
