-- TODO: Add Postgres BOOLEAN[] type back when DucKDB support accelerating Arrow Boolean List Type
-- Issue: https://github.com/spiceai/spiceai/issues/3138
CREATE TABLE test_postgresql_table (
  id SERIAL PRIMARY KEY,
  int2_column SMALLINT,
  int4_column INTEGER,
  int8_column BIGINT,
  float4_column REAL,
  float8_column DOUBLE PRECISION,
  text_column TEXT,
  varchar_column VARCHAR,
  bpchar_column BPCHAR,
  bool_column BOOLEAN,
  numeric_column NUMERIC,
  timestamp_column TIMESTAMP,
  date_column DATE,
  int2_array_column SMALLINT[],
  int4_array_column INTEGER[],
  int8_array_column BIGINT[],
  float4_array_column REAL[],
  float8_array_column DOUBLE PRECISION[],
  text_array_column TEXT[]
);

INSERT INTO test_postgresql_table (
  int2_column, 
  int4_column, 
  int8_column, 
  float4_column, 
  float8_column, 
  text_column, 
  varchar_column, 
  bpchar_column, 
  bool_column, 
  numeric_column, 
  timestamp_column, 
  date_column, 
  int2_array_column, 
  int4_array_column, 
  int8_array_column, 
  float4_array_column, 
  float8_array_column, 
  text_array_column
) VALUES (
  1, 
  2, 
  3, 
  4.0, 
  5.0, 
  'test', 
  'test', 
  'test', 
  true, 
  6.0, 
  CURRENT_TIMESTAMP, 
  CURRENT_DATE, 
  ARRAY[1, 2], 
  ARRAY[3, 4], 
  ARRAY[5, 6], 
  ARRAY[7.0, 8.0], 
  ARRAY[9.0, 10.0], 
  ARRAY['test1', 'test2']
), (
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
);