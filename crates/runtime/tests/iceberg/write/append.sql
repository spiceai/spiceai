INSERT INTO <table-name>
  (batch_id, boolean_col, int_col, long_col, float_col, double_col, decimal_col, date_col, timestamp_col, binary_col)
VALUES
  ('<batch-uuid>', TRUE,  1,  10000000001, REAL '1.5',  2.25, DECIMAL '12345.6789', DATE '2024-01-01', TIMESTAMP '2024-01-01 02:03:04', X'00FFAB'),
  ('<batch-uuid>', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);