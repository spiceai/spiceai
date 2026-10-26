-- Rows for partitioning.yaml: NULLs in a partition key (name) and in a
-- numeric column (score), dates across months.
CREATE TABLE t (id int PRIMARY KEY, name text, age int, city text, score int, order_date date, amount numeric(10, 2));
INSERT INTO t SELECT i,
  CASE WHEN i % 11 = 0 THEN NULL ELSE 'name' || (i % 37) END,
  18 + i % 60, (ARRAY['Austin', 'Boston', 'Chicago', 'Denver'])[1 + i % 4],
  CASE WHEN i % 13 = 0 THEN NULL ELSE i % 100 END,
  DATE '2024-01-01' + (i * 7) % 366, (i % 500) * 1.25
FROM generate_series(1, 4000) i;
CREATE TABLE b (id int PRIMARY KEY, name text);
INSERT INTO b SELECT i, 'n' || i FROM generate_series(1, 10) i;
