-- DataFusion built-ins across engines. Every place must answer as DataFusion
-- does locally (the `arrow` variant). Where Spice sends a query to an
-- accelerator, `-- sends <place>: <fragment>` / `-- keeps <place>: <fragment>`
-- say what the SQL sent must contain or must leave out: a built-in reaches
-- the engine only in a rendering that answers the same, and stays local where
-- the engine has none.

-- Trims: DuckDB has no btrim; Unicode spaces are not trimmed by default
-- sends duckdb: trim(
-- keeps duckdb: btrim(
SELECT id, trim(name) AS t, btrim(name, 'xy') AS b FROM names ORDER BY id;
SELECT id, character_length(trim(name)) AS len FROM names ORDER BY id;

-- Hex and digests: rendered inside lower(..) / unhex(..) for DuckDB
-- sends duckdb: lower(to_hex(
SELECT id, to_hex(h) AS hx FROM hexes ORDER BY id;
SELECT id FROM hexes WHERE to_hex(h) = 'deadbeef' ORDER BY id;
-- sends duckdb: unhex(sha256(
SELECT id, sha256(name) AS d FROM names ORDER BY id;
SELECT id, sha256(coalesce(name, '')) AS d FROM names WHERE id = 4;
SELECT id, encode(sha256(name), 'hex') AS h FROM names ORDER BY id;
SELECT id, md5(name) AS d FROM names ORDER BY id;

-- regexp_match/instr: kept local (DuckDB has no equivalent with the same NULLs and groups)
-- keeps duckdb: regexp_extract
-- keeps duckdb: regexp_match
SELECT id, regexp_match(s, '(a)(b)') AS m FROM rx ORDER BY id;
SELECT id, regexp_match(s, 'zzz') IS NULL AS unmatched FROM rx ORDER BY id;
SELECT id, regexp_instr(s, 'b') AS i FROM rx ORDER BY id;
SELECT id, regexp_like(s, '(a)(b)') AS l, regexp_replace(s, '(a)(b)', 'X') AS r, regexp_count(s, '(a)(b)') AS c FROM rx ORDER BY id;
SELECT regexp_count(s, 'a') AS c FROM rx WHERE s IS NULL ORDER BY id;

-- regexp_count: pushed as len(regexp_extract_all(..)) for patterns RE2 reads alike
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'a') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'a', 2) AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'a', 9) AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '^a+$') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '[a-c]{2,}') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'a+?') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '(a)(b)') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '(?:ab)+') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'x.') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '\x61') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'x\ny') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '١') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '(a{1}){3}') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '(a{100}){10}') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '[Kkx]|a') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, 'k|K') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '([K-Lk]|a)') AS c FROM rx ORDER BY id;
-- sends duckdb: len(regexp_extract_all(
-- keeps duckdb: coalesce(
SELECT id, regexp_count(s, '[^Kk]|a') AS c FROM rx ORDER BY id;
SELECT id, regexp_count(s, 'a') AS plain, regexp_count(s, 'a', 2) AS from_2 FROM rx ORDER BY id;
SELECT id FROM rx WHERE regexp_count(s, 'a') = 0 ORDER BY id;

-- regexp_count: kept local where DuckDB would read the pattern or arguments differently
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a', id) AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, p) AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a*') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a|\b') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '\d') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '[a&&a]') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '(?x)a b') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '(a{100}){11}') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a++') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a{01}') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '([Kk]|a)') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '([Ss]|a)') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '([KkK]|a)') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '([K-Kk]|a)') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '[^\x00-\x4A\x4C-\x6A\x6C-\x{10FFFF}]|a') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a', 4294967296) AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a', 1, 'm') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, 'a', 1, 'i') AS c FROM rx ORDER BY id;
-- keeps duckdb: regexp_extract_all
SELECT id, regexp_count(s, '(?i)a') AS c FROM rx ORDER BY id;

-- regexp_like/replace: pushed only where the engines agree
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '\d') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '\w') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '\ba') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '[Kk]|a') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, 'a{01}') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '(?i)k') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, '\x{1C89}', 'i') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, p) AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '\d', 'X') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '\w', 'X') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '\x{1C89}', 'X', 'i') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '$2$1') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_matches(
-- keeps duckdb: regexp_replace(
SELECT id, regexp_like(s, 'a++') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\2\1') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_matches(
SELECT id, regexp_like(s, 'a') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_matches(
SELECT id, regexp_like(s, 'a*') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', 'X') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, 'a*', 'X') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, 'a', 'X', 'g') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, 'a*', 'X', 'g') AS v FROM rx ORDER BY id;
SELECT id FROM rx WHERE regexp_like(s, '\d') ORDER BY id;
SELECT id FROM rx WHERE regexp_like(s, '\w') ORDER BY id;

-- regexp_replace group references
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)?b', '<\1>') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\0-\2\1') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
SELECT id, regexp_replace(s, 'a(b)?', '[\1]', 'g') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\10') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\\1') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\3') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(?:a)(b)', '\2') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\1١') AS v FROM rx ORDER BY id;
-- keeps duckdb: regexp_replace(
SELECT id, regexp_replace(s, '(a)(b)', '\q') AS v FROM rx ORDER BY id;
-- sends duckdb: regexp_replace(
-- sends duckdb: GROUP BY
SELECT regexp_replace(s, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, count(*) AS c FROM rx WHERE s <> '' GROUP BY 1;

-- arrow_typeof is DataFusion's own
-- keeps duckdb: arrow_typeof
SELECT id, arrow_typeof(id) AS id_type, arrow_typeof(name) AS name_type FROM names ORDER BY id;

-- Ordered aggregates: the ordering reaches DuckDB inside the call
-- sends duckdb: string_agg(DISTINCT "cust"."customer", '|' ORDER BY "cust"."customer"
SELECT string_agg(DISTINCT customer, '|' ORDER BY customer) AS customers FROM cust WHERE region = 'eu';
-- sends duckdb: array_agg("cust"."customer" ORDER BY "cust"."customer"
SELECT array_agg(customer ORDER BY customer) AS customers FROM cust WHERE region = 'eu';
-- sends duckdb: first("cust"."customer" ORDER BY "cust"."customer"
-- sends duckdb: last("cust"."customer" ORDER BY "cust"."id" DESC
-- keeps duckdb: first_value
-- keeps duckdb: last_value
SELECT first_value(customer ORDER BY customer) AS f, last_value(customer ORDER BY id DESC) AS l FROM cust WHERE region = 'eu';
-- keeps duckdb: nth_value
SELECT nth_value(customer, 2 ORDER BY customer) AS n FROM cust WHERE region = 'eu';
SELECT array_agg(customer ORDER BY id) AS a FROM cust;

-- Windows ignoring NULLs stay local (the unparser drops IGNORE NULLS); respecting them federates
-- keeps duckdb: lag(
SELECT id, lag(v) IGNORE NULLS OVER (ORDER BY id) AS w FROM vals ORDER BY id;
-- keeps duckdb: lead(
SELECT id, lead(v) IGNORE NULLS OVER (ORDER BY id) AS w FROM vals ORDER BY id;
-- keeps duckdb: last_value(
SELECT id, last_value(v) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w FROM vals ORDER BY id;
-- keeps duckdb: nth_value(
SELECT id, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w FROM vals ORDER BY id;
-- sends duckdb: lag("vals"."v")
SELECT id, lag(v) OVER (ORDER BY id) AS w FROM vals ORDER BY id;

-- Approximate aggregates stay local; an aggregate over a constant still answers
-- keeps duckdb: approx_distinct
-- keeps duckdb: approx_count_distinct
SELECT approx_distinct(customer) AS n FROM cust;
-- sends duckdb: SELECT 1 FROM "cust"
SELECT approx_distinct(1) AS a FROM cust;
SELECT approx_median(qty) AS m FROM cust;

-- SQLite: built-ins it lacks or answers differently stay local
-- keeps sqlite: upper(
SELECT id, upper(customer) AS u, lower(customer) AS l FROM cust ORDER BY id;
-- keeps sqlite: concat(
SELECT id, concat(customer, '-', region) AS c FROM cust ORDER BY id;
-- keeps sqlite: concat(
SELECT id, concat(customer, CAST(NULL AS VARCHAR)) AS c FROM cust ORDER BY id;
-- keeps sqlite: concat(
SELECT id, concat(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS c FROM cust ORDER BY id;
-- keeps sqlite: concat(
SELECT id, concat(customer, '') AS c FROM cust ORDER BY id;
-- keeps sqlite: concat(
SELECT id, concat('', region) AS c FROM cust ORDER BY id;
-- keeps sqlite: to_hex(
SELECT id, to_hex(id) AS h FROM cust ORDER BY id;
-- keeps sqlite: md5(
SELECT id, md5(customer) AS d FROM cust ORDER BY id;
-- keeps sqlite: sha256(
SELECT id, sha256(customer) AS d FROM cust ORDER BY id;
-- keeps sqlite: encode(
SELECT id, encode(sha256(customer), 'hex') AS h FROM cust ORDER BY id;
-- keeps sqlite: date_part(
SELECT id, date_part('month', try_cast(ts AS timestamp)) AS m FROM cust ORDER BY id;
-- keeps sqlite: date_part(
SELECT id, extract(month FROM try_cast(ts AS timestamp)) AS m FROM cust ORDER BY id;
-- keeps sqlite: date_trunc(
SELECT id, date_trunc('month', try_cast(ts AS timestamp)) AS m FROM cust ORDER BY id;
-- keeps sqlite: LIKE
SELECT id FROM cust WHERE customer ILIKE '%alice%' ORDER BY id;
-- keeps sqlite: LIKE
-- expect-empty
SELECT id FROM cust WHERE customer LIKE '%ALICE%' ORDER BY id;
-- keeps sqlite: regexp_like(
SELECT id, regexp_like(customer, 'ali') AS m FROM cust ORDER BY id;
-- keeps sqlite: regexp_replace(
SELECT id, regexp_replace(customer, 'a', 'X') AS r FROM cust ORDER BY id;
-- keeps sqlite: regexp_match(
SELECT id, regexp_match(customer, '(a)') AS m FROM cust ORDER BY id;
-- keeps sqlite: regexp_instr(
SELECT id, regexp_instr(customer, 'a') AS i FROM cust ORDER BY id;
-- keeps sqlite: regexp_count(
SELECT id, regexp_count(customer, 'a') AS c FROM cust ORDER BY id;
-- keeps sqlite: median(
SELECT median(qty) AS m FROM cust;
-- keeps sqlite: string_agg(
SELECT string_agg(DISTINCT customer, '|' ORDER BY customer) AS customers FROM cust;
-- keeps sqlite: first_value(
SELECT first_value(customer ORDER BY id) AS f FROM cust;
-- keeps sqlite: last_value(
SELECT last_value(customer ORDER BY id) AS l FROM cust;
-- keeps sqlite: nth_value(
SELECT nth_value(customer, 2 ORDER BY id) AS n FROM cust;
-- keeps sqlite: stddev(
SELECT stddev(qty) AS s FROM cust;
-- keeps sqlite: var_pop(
SELECT var_pop(qty) AS v FROM cust;
-- keeps sqlite: corr(
SELECT corr(qty, id) AS c FROM cust;
-- keeps sqlite: bool_and(
SELECT bool_and(qty > 10) AS b FROM cust;
-- keeps sqlite: bit_or(
SELECT bit_or(qty) AS b FROM cust;
-- keeps sqlite: count(
SELECT count(region, customer) AS n FROM cust;
-- keeps sqlite: stddev(
SELECT id, stddev(qty) OVER (PARTITION BY region) AS s FROM cust ORDER BY id;
-- keeps sqlite: count(
SELECT id, count(DISTINCT qty) OVER (PARTITION BY region) AS n FROM cust ORDER BY id;
-- keeps sqlite: lag(
SELECT id, lag(region) IGNORE NULLS OVER (ORDER BY id) AS r FROM cust ORDER BY id;
-- keeps sqlite: lead(
SELECT id, lead(region) IGNORE NULLS OVER (ORDER BY id) AS r FROM cust ORDER BY id;
-- keeps sqlite: ROLLUP
SELECT region, sum(qty) AS s FROM cust GROUP BY ROLLUP(region) ORDER BY region NULLS LAST, s;
-- sends sqlite: GROUP BY
SELECT region, count(qty) AS n, sum(qty) AS s, min(qty) AS lo, max(qty) AS hi FROM cust GROUP BY region ORDER BY region NULLS LAST;
-- sends sqlite: lag(
SELECT id, row_number() OVER (ORDER BY id) AS n, lag(region) OVER (ORDER BY id) AS r FROM cust ORDER BY id;

-- A correlated subquery above a join: SQLite's LIKE folds ASCII case, so the
-- LIKE stays local while the scans are still sent.
-- keeps sqlite: LIKE
SELECT p.p_partkey, ps.ps_suppkey, ps.ps_supplycost FROM part p JOIN partsupp ps ON p.p_partkey = ps.ps_partkey WHERE p.p_type LIKE '%BRASS' AND ps.ps_supplycost = (SELECT min(ps2.ps_supplycost) FROM partsupp ps2 WHERE ps2.ps_partkey = p.p_partkey) ORDER BY p.p_partkey, ps.ps_suppkey;

-- Constant-only selects: a scan with no column is sent as SELECT 1.
-- sends sqlite: SELECT 1 FROM `cust`
SELECT upper('a') AS u FROM cust;
