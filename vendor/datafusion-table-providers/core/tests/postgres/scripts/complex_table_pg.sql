CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

CREATE TYPE complex_type AS (
    x INTEGER,
    y INTEGER,
    z BIGINT,
    x1 NUMERIC(10, 2)
);

CREATE TABLE example_table (
    -- Numeric types
    id SERIAL PRIMARY KEY,
    small_int_col SMALLINT,
    integer_col INTEGER,
    big_int_col BIGINT,
    decimal_col DECIMAL(10, 2),
    numeric_col NUMERIC(10, 2),
    real_col REAL,
    double_col DOUBLE PRECISION,
    
    -- Character types
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    
    -- Binary data types
    bytea_col BYTEA,
    
    -- Date/Time types
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL,
    
    -- Boolean type
    boolean_col BOOLEAN,
    
    -- Enumerated type
    mood_col mood,
    
    -- UUID type
    uuid_col UUID,
    
    -- JSON types
    json_col JSON,
    
    -- Arrays
    int_array_col INTEGER[],
    text_array_col TEXT[],
    
    -- Range types
    int_range_col INT4RANGE,
    
    -- Custom composite type
    composite_col complex_type
);

CREATE VIEW example_view AS
SELECT * FROM example_table;

CREATE MATERIALIZED VIEW example_materialized_view AS
SELECT * FROM example_table;
