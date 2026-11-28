-- Queries over the boundary values of each type (NaN, infinities, negative
-- zero, the smallest and largest doubles, 38-digit decimals, the integer
-- limits, empty, whitespace-only, multibyte and very long strings, bytes with
-- zero bytes, times either side of the epoch and across a daylight-saving
-- change), checked against PostgreSQL over the same table. PostgreSQL orders
-- NaN above every other value and treats -0 and 0 as equal, as SQL requires
-- of a total order on doubles. A query known to fail somewhere names those
-- places; the finding behind each is in the PR or issue tracker.

-- Every value round-trips
SELECT * FROM e;
SELECT id, f, r FROM e;
SELECT id, d, n FROM e;
SELECT id, i8, i4, i2 FROM e;
SELECT id, s FROM e;
SELECT id, tz, ts, dt FROM e;
SELECT id, bin FROM e;

-- Doubles: NaN, infinities, negative zero
SELECT id, f FROM e ORDER BY f NULLS FIRST, id;
SELECT id, f FROM e ORDER BY f DESC NULLS LAST, id;
SELECT id FROM e WHERE f = 0;
SELECT id FROM e WHERE f = CAST('NaN' AS DOUBLE PRECISION);
SELECT id FROM e WHERE f > 1e300;
SELECT id FROM e WHERE f < 0;
SELECT f, count(*) FROM e GROUP BY f;
SELECT count(DISTINCT f) FROM e;
SELECT min(f), max(f), count(f) FROM e;
SELECT min(f), max(f) FROM e WHERE f <> CAST('NaN' AS DOUBLE PRECISION);
SELECT id, f * 0 AS z, -f AS neg, abs(f) AS a FROM e;
SELECT sum(f) FROM e WHERE id IN (6, 7, 8);
SELECT id, r FROM e ORDER BY r NULLS LAST, id;
SELECT a.id, b.id AS other FROM e a JOIN e b ON a.f = b.f WHERE a.id < b.id;

-- Decimals at 38 digits
SELECT id, d FROM e ORDER BY d NULLS LAST, id;
SELECT sum(d), min(d), max(d), count(d) FROM e;
SELECT sum(n), min(n), max(n) FROM e;
SELECT id, d * 2 AS twice FROM e WHERE id IN (4, 5);
SELECT id FROM e WHERE d > 0;
SELECT d, count(*) FROM e GROUP BY d;
SELECT id, CAST(d AS DOUBLE PRECISION) AS df FROM e;

-- Integer limits
SELECT min(i8), max(i8), min(i4), max(i4), min(i2), max(i2) FROM e;
SELECT id, i8 FROM e ORDER BY i8 NULLS FIRST, id;
SELECT id, -i4 AS neg FROM e WHERE id IN (2, 4, 5);
SELECT id, CAST(i4 AS BIGINT) + 1 AS widened FROM e;
-- The literal is BIGINT, so the sum widens; PostgreSQL types the literal as INTEGER.
-- oracle-sql: SELECT id, i4::bigint + 1 AS next FROM e WHERE id = 1
SELECT id, i4 + 1 AS next FROM e WHERE id = 1;
-- expect-error
SELECT id, i4 + CAST(1 AS INTEGER) AS next FROM e WHERE id = 1;
-- expect-error
SELECT id, i8 + 1 AS next FROM e WHERE id = 1;
-- expect-error
SELECT id, i8 * 2 AS twice FROM e WHERE id = 1;
-- expect-error
SELECT id, -i8 AS neg FROM e WHERE id = 3;
-- PostgreSQL sums BIGINT as NUMERIC, so the total of the two largest values is exact.
-- expect-error
SELECT sum(i8) FROM e WHERE id IN (1, 6);
-- Both widen an INTEGER sum to BIGINT.
SELECT sum(i4) FROM e WHERE id IN (1, 6);

-- Strings
SELECT id, char_length(s) AS chars, octet_length(s) AS bytes FROM e;
SELECT id, substr(s, 2, 2) AS mid FROM e WHERE id <> 8;
SELECT id, upper(s) AS u, lower(s) AS l FROM e WHERE id IN (3, 4, 5, 10);
SELECT id, reverse(s) AS rev FROM e WHERE id IN (3, 4, 5, 10);
SELECT id FROM e WHERE s = '';
SELECT id FROM e WHERE s IS NOT NULL AND trim(s) = '';
SELECT id FROM e WHERE s LIKE '%本%';
SELECT id FROM e WHERE s LIKE '%\_%';
SELECT id, position('語' IN s) AS p FROM e WHERE s IS NOT NULL;
SELECT s, count(*) FROM e GROUP BY s;
SELECT id, s || '|' AS delimited FROM e WHERE id IN (6, 7, 10);
SELECT id, replace(s, '"', '''') AS r FROM e WHERE id = 10;
SELECT id, left(s, 3) AS l3, right(s, 3) AS r3 FROM e WHERE id = 8;

-- Bytes
SELECT id, octet_length(bin) AS n FROM e;
SELECT id FROM e WHERE bin = CAST('' AS BYTEA);

-- Times: before the epoch, sub-second, around a daylight-saving change
SELECT id, ts FROM e ORDER BY ts NULLS LAST, id;
SELECT id, tz FROM e ORDER BY tz NULLS LAST, id;
SELECT id FROM e WHERE tz < TIMESTAMPTZ '1970-01-01 00:00:00+00' OR dt < DATE '1970-01-01';
SELECT id, CAST(ts AS DATE) AS day, CAST(tz AS DATE) AS tzday FROM e;
SELECT id, EXTRACT(YEAR FROM ts) AS y, EXTRACT(HOUR FROM tz) AS h FROM e;
SELECT id, date_trunc('day', tz) AS day FROM e;
SELECT id, date_trunc('hour', ts) AS hour, date_trunc('second', ts) AS second FROM e;
SELECT id, tz + INTERVAL '1 hour' AS later FROM e WHERE tz IS NOT NULL;
SELECT id, dt - DATE '1970-01-01' AS days FROM e;
SELECT max(ts) - min(ts) AS span FROM e;
