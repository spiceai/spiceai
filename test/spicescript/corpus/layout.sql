-- Filters and aggregates whose answer a physical layout (sort order, shards,
-- partitions, indexes, a cold tier) must not change, with predicates on the
-- columns those layouts organize by. Portable to PostgreSQL.

SELECT count(*), count(DISTINCT grp), sum(amount), min(ts), max(ts) FROM facts;
SELECT * FROM facts WHERE id = 4242;
SELECT * FROM facts WHERE id IN (1, 77, 150000, 299999, 999999);
SELECT id, grp, amount FROM facts WHERE id BETWEEN 120000 AND 120050;
SELECT count(*), sum(amount) FROM facts WHERE grp = 17;
SELECT id, label FROM facts WHERE grp IN (3, 99, 150) AND label = 'beta';
SELECT count(*) FROM facts WHERE grp BETWEEN 10 AND 20 AND amount > 500;
SELECT count(*), min(id), max(id) FROM facts WHERE ts >= TIMESTAMP '2024-03-01 00:00:00' AND ts < TIMESTAMP '2024-03-02 00:00:00';
SELECT grp, count(*), sum(amount), max(ts) FROM facts GROUP BY grp;
SELECT label, count(*), count(note) FROM facts GROUP BY label;
SELECT id, amount FROM facts ORDER BY id LIMIT 20;
SELECT id, amount FROM facts WHERE grp = 42 ORDER BY id DESC LIMIT 10;
SELECT count(*) FROM facts WHERE note IS NULL;
-- expect-empty
SELECT * FROM facts WHERE grp = 9999;
