CREATE TABLE test_mysql_table (
  id SERIAL PRIMARY KEY,
  col_bit BIT(1),
  col_tiny TINYINT,
  col_short SMALLINT,
  col_long INT,
  col_longlong BIGINT,
  col_float FLOAT,
  col_double DOUBLE,
  col_timestamp TIMESTAMP,
  col_date DATE,
  col_blob BLOB,
  col_varchar VARCHAR(255),
  col_string TEXT,
  col_var_string VARCHAR(255)
);

INSERT INTO test_mysql_table (
  col_bit,
  col_tiny,
  col_short,
  col_long,
  col_longlong,
  col_float,
  col_double,
  col_timestamp,
  col_date,
  col_blob,
  col_varchar,
  col_string,
  col_var_string
) VALUES (
  1,
  1,
  1,
  1,
  1,
  1.1,
  1.1,
  '2019-01-01 00:00:00',
  '2019-01-01',
  'blob',
  'varchar',
  'string',
  'var_string'
);

-- null not supported yet
-- , (
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL,
--   NULL
-- );
