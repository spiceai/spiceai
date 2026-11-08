-- Partitioned accelerations answer as PostgreSQL does, and a filter on a
-- partition expression scans fewer partitions: Cayenne plans one scan per
-- partition it reads, so `plan-count` pins how many it reads at each place.
-- (A filter `IS NULL` on a partition key still reads every partition.)
-- plan-count cayenne-bucket: 4 CayenneAccelerationExec
-- plan-count cayenne-multi: 9 CayenneAccelerationExec
-- plan-count cayenne-name: 4 CayenneAccelerationExec
-- plan-count cayenne-date: 12 CayenneAccelerationExec
SELECT * FROM t;
SELECT count(*) FROM t;
-- plan-count cayenne-bucket: 1 CayenneAccelerationExec
-- plan-count cayenne-multi: 3 CayenneAccelerationExec
SELECT * FROM t WHERE id = 1;
-- plan-count cayenne-bucket: 2 CayenneAccelerationExec
SELECT * FROM t WHERE id IN (1, 5, 999);
SELECT id FROM t WHERE id >= 3995;
SELECT id FROM t WHERE score > 95;
SELECT id FROM t WHERE score IS NULL;
SELECT id FROM t WHERE name IS NULL;
-- plan-count cayenne-name: 1 CayenneAccelerationExec
SELECT id FROM t WHERE name = 'name3';
SELECT id FROM t WHERE name IS NOT NULL AND id < 60;
SELECT id, amount FROM t WHERE order_date >= DATE '2024-03-01' AND order_date < DATE '2024-04-01';
-- plan-count cayenne-date: 1 CayenneAccelerationExec
SELECT id FROM t WHERE date_part('month', order_date) = 3;
-- plan-count cayenne-bucket: 1 CayenneAccelerationExec
-- plan-count cayenne-multi: 1 CayenneAccelerationExec
SELECT id FROM t WHERE id = 7 AND score = 7;
SELECT city, count(*), sum(amount) FROM t GROUP BY city;
-- expect-empty
SELECT id, name, city FROM t WHERE id = 999999;
