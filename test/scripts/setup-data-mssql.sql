CREATE DATABASE MSSQLTestDB;
GO

USE MSSQLTestDB;
GO

CREATE TABLE test_mssql_table (
    col_int INT,
    col_bigint BIGINT,
    col_smallint SMALLINT,
    col_tinyint TINYINT,
    col_float FLOAT,
    col_real REAL,
    col_decimal DECIMAL(10, 2),
    col_char CHAR(10),
    col_varchar VARCHAR(50),
    col_text TEXT,
    col_nchar NCHAR(10),
    col_nvarchar NVARCHAR(50),
    col_ntext NTEXT,
    col_uniqueidentifier UNIQUEIDENTIFIER,
    col_binary BINARY(10),
    col_varbinary VARBINARY(50),
    col_xml XML,
    col_money MONEY,
    col_smallmoney SMALLMONEY,
    col_date DATE,
    col_time TIME,
    col_datetime DATETIME,
    col_smalldatetime SMALLDATETIME,
    col_datetime2 DATETIME2,
    col_datetimeoffset DATETIMEOFFSET,
    col_bit BIT,
    col_geography GEOGRAPHY
);
GO

INSERT INTO test_mssql_table (
    col_int, col_bigint, col_smallint, col_tinyint, col_float, col_real, col_decimal, col_char, col_varchar, col_text, 
    col_nchar, col_nvarchar, col_ntext, col_uniqueidentifier, col_binary, col_varbinary, col_xml, col_money, col_smallmoney, 
    col_date, col_time, col_datetime, col_smalldatetime, col_datetime2, col_datetimeoffset, col_bit, col_geography
)
VALUES (
    123, 9223372036854775807, 32767, 255, 123.456, 123.456, 12345.67, 'char_val', 'varchar_val', 'text_val', 
    N'nchar_val', N'nvarchar_val', N'ntext_val', '6F9619FF-8B86-D011-B42D-00C04FC964FF', 
    0x1234567890, 0xabcdef, '<root><value>test</value></root>', 12345.67, 1234.56, '2024-01-01', 
    '12:34:56', '2024-01-01 12:34:56', '2024-01-01 12:00:00', '2024-01-01 12:34:56.1234567', '2024-01-01 12:34:56.1234567 +02:00', 1, 
    geography::STGeomFromText('POINT(47.6062 -22.3321)', 4326)
);
GO

INSERT INTO test_mssql_table (
    col_int, col_bigint, col_smallint, col_tinyint, col_float, col_real, col_decimal, col_char, col_varchar, col_text, 
    col_nchar, col_nvarchar, col_ntext, col_uniqueidentifier, col_binary, col_varbinary, col_xml, col_money, col_smallmoney, 
    col_date, col_time, col_datetime, col_smalldatetime, col_datetime2, col_datetimeoffset, col_bit, col_geography
)
VALUES (
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);
GO