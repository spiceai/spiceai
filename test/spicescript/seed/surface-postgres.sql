-- Rows for corpus/surface.sql. Times straddle the 2024 US daylight-saving
-- changes and the epoch; JSON holds nesting, arrays, nulls and numbers; arrays
-- hold NULL-free and empty values (PostgreSQL arrays with a NULL element are a
-- separate known failure).
SET TIME ZONE 'UTC';
CREATE TABLE ev (
  id int PRIMARY KEY, tz timestamptz, ts timestamp, dt date, n int, x double precision, d numeric(12, 4),
  s text, j jsonb, a int[], t text[], grp text);
INSERT INTO ev
SELECT i,
  TIMESTAMPTZ '2024-03-09 12:00:00+00' + i * INTERVAL '97 minutes',
  TIMESTAMP '1999-12-25 00:00:00' + i * INTERVAL '3 days 7 hours',
  DATE '2024-01-01' + (i * 11) % 400,
  CASE WHEN i % 10 = 0 THEN NULL ELSE (i * 37) % 101 - 50 END,
  CASE WHEN i % 13 = 0 THEN NULL ELSE ((i * 7919) % 1000) / 37.0 END,
  ((i * 31) % 2000) / 16.0 - 60,
  CASE i % 7 WHEN 0 THEN NULL WHEN 1 THEN 'alpha beta' WHEN 2 THEN 'Gamma_delta' WHEN 3 THEN 'x,y,z'
             WHEN 4 THEN '  spaced  ' WHEN 5 THEN 'naïve café' ELSE '' END,
  CASE WHEN i % 9 = 0 THEN NULL
       ELSE jsonb_build_object('k', i % 5, 'name', 'n' || i, 'tags', jsonb_build_array('a', 'b' || (i % 3)),
              'nested', jsonb_build_object('v', i * 1.5, 'flag', i % 2 = 0), 'maybe', CASE WHEN i % 4 = 0 THEN NULL ELSE i END) END,
  CASE WHEN i % 11 = 0 THEN '{}'::int[] WHEN i % 17 = 0 THEN NULL ELSE ARRAY[i % 4, i % 7, (i * 3) % 10] END,
  CASE WHEN i % 6 = 0 THEN '{}'::text[] ELSE ARRAY['t' || (i % 3), 'u' || (i % 2)] END,
  (ARRAY['red', 'green', 'blue'])[1 + i % 3]
FROM generate_series(1, 240) i;
CREATE TABLE edges (id serial PRIMARY KEY, src int, dst int);
INSERT INTO edges (src, dst) SELECT i, i * 2 FROM generate_series(1, 60) i;
INSERT INTO edges (src, dst) SELECT i, i + 3 FROM generate_series(1, 60) i WHERE i % 4 = 0;
