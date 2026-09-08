# Copyright 2024-2025 The Spice.ai OSS Authors
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

import os
import pyarrow as pa
import pyarrow.parquet as pq
from decimal import Decimal
import duckdb

fields = [
    pa.field('t_boolean', pa.bool_()),
    pa.field('t_int8', pa.int8()),
    pa.field('t_int16', pa.int16()),
    pa.field('t_int32', pa.int32()),
    pa.field('t_int64', pa.int64()),
    pa.field('t_float32', pa.float32()),
    pa.field('t_float64', pa.float64()),
    pa.field('t_date32', pa.date32()),
    pa.field('t_date64', pa.date64()),
    pa.field('t_timestamp_ms', pa.timestamp('ms')),
    # pa.field('t_time32_s', pa.time32('s')), # DuckDB fails to load this column
    pa.field('t_time64_us', pa.time64('us')), # Spice runtime is crashed
    pa.field('t_string', pa.string()),
    pa.field('t_binary', pa.binary()),
    pa.field('t_decimal128', pa.decimal128(10, 2)),
    pa.field('t_decimal256', pa.decimal256(20, 4)),
    pa.field('t_list_int', pa.list_(pa.int32())),
    pa.field('t_list_string', pa.list_(pa.string())),
    pa.field('t_duration_ms', pa.duration('ms')),
]

schema = pa.schema(fields)

data = [
    [True, False, None],  # f_boolean
    [127, 0, None],  # f_int8
    [32767, 0, None],  # f_int16
    [2147483647, 0, None],  # f_int32
    [9223372036854775807, 0, None],  # f_int64
    [3.402823e+38, 0.0, None],  # f_float32
    [1.7976931348623157e+308, 0.0, None],  # f_float64
    [19737, 18628, None],  # f_date32
    [1705352433999, 1609459201999, None],  # f_date64
    [1705352433000, 1609459200001, None],  # f_timestamp_ms
    # [8639, 0, None],  # f_time32_ms
    [86399999999, 0, None],  # f_time64_us
    ['hello', 'world', None],  # f_string
    [b'abc', b'def', None],  # f_binary
    [Decimal('99999.99'), Decimal('-99999.99'), None],  # f_decimal128
    [Decimal('9999999999.9999'), Decimal('-9999999999.9999'), None],  # f_decimal256
    [[1, 2, 3], [], None],  # f_list_int
    [['abc', 'def'], [], None],  # f_list_string
    [1000, 2000, None],  # f_duration_ms (duration in milliseconds)
]
table = pa.Table.from_arrays(data, schema=schema)

pq.write_table(table, 'arrow_types.parquet')

print("Successfully generated 'arrow_types.parquet'")

print("Exporting generated data to DuckDB database..")
if os.path.exists('arrow_types.db'):
    os.remove('arrow_types.db')

conn = duckdb.connect('arrow_types.db')
for field in fields:
    table_name = field.name
    conn.execute(f"CREATE TABLE arrow_types.{table_name} AS SELECT {field.name} FROM read_parquet('arrow_types.parquet');")
    print(conn.execute(f"DESCRIBE arrow_types.{table_name};").fetchall())

# Generate the spicepod.yml that could be used to load the data
config_content = """version: v1
kind: Spicepod
name: arrow-data-types
datasets:
"""

for field in fields:
    table_name = field.name
    config_content += f"""
- from: duckdb:arrow_types.{table_name}
  name: {table_name}
  params:
      open: arrow_types.db
  acceleration:
    enabled: true
    #engine: engine_name
"""

# Write the configuration to a YAML file
with open("spicepod.yaml", "w") as f:
    f.write(config_content)

print("Successfully generated test 'spicepod.yaml'")