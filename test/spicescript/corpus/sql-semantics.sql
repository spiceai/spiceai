-- Operators and aggregates whose SQL means something else on some backends,
-- answered with DataFusion's semantics. The reference is PostgreSQL, which
-- shares those semantics or is given its own spelling (`-- oracle-sql:`).
-- Same rules as pushdown.sql otherwise: no collation-dependent comparison and
-- ORDER BY only on non-null unique keys.

-- `||` concatenates, and is NULL when either side is (#16225).
SELECT id, s || 'x' FROM t;
SELECT id, coalesce(s, '') || ', ' || coalesce(s, '') FROM t;
SELECT id, 'n' || id FROM t WHERE id < 50;
SELECT id FROM t WHERE s || '!' = 'banana!';
SELECT id FROM t WHERE char_length(s || s) = 10;

-- Integer `/` truncates toward zero and `%` keeps the dividend's sign (#16226).
SELECT id, a / 3, a % 3, a / -2, a % -2 FROM w;
SELECT id, a / -1, a % -1 FROM w;
SELECT id, c / 3, c % 7, c / -4 FROM w;
SELECT id, a / k, a % k FROM w WHERE k <> 0;
SELECT id, c / k FROM w WHERE k <> 0;
SELECT id FROM w WHERE CASE WHEN k <> 0 THEN a / k END > 1;
SELECT id FROM w WHERE CASE WHEN k > 0 THEN a / k ELSE NULL END > 1.2;
SELECT id FROM w WHERE c / 1000000000000000000 > 1;
SELECT a / 4, count(*) FROM w GROUP BY a / 4;
SELECT id, a / k / 3.0 * 100 FROM w WHERE k <> 0;
-- A float literal divides as a float.
SELECT id, a / 30000.0, a * 0.25 FROM w;
-- A decimal quotient keeps 4 more digits of scale than the dividend, truncated.
-- The text compares every digit, which the float tolerance would not.
SELECT id, dd / e FROM w;
-- oracle-sql: SELECT id, CAST(trunc(dd / e, 6) AS text) FROM w
SELECT id, CAST(dd / e AS VARCHAR) FROM w;

-- `stddev` and `var` are the sample figures; a sample figure over one row,
-- and every figure over no rows, is NULL (#16227).
SELECT g, stddev(x), stddev_samp(x), stddev_pop(x), var_samp(x), var_pop(x) FROM w GROUP BY g;
-- oracle-sql: SELECT g, variance(x), variance(x), var_pop(x), stddev_samp(a), variance(a) FROM w GROUP BY g
SELECT g, var(x), var_sample(x), var_population(x), stddev(a), var(a) FROM w GROUP BY g;
SELECT stddev(x), stddev_pop(x), var_samp(x), var_pop(x) FROM w;
SELECT stddev(f), stddev_pop(f), var_samp(f), var_pop(f) FROM t WHERE id < 0;
SELECT id, stddev(x) OVER (PARTITION BY g), var_samp(x) OVER (PARTITION BY g) FROM w;
SELECT id, stddev_pop(x) OVER (PARTITION BY g), var_pop(x) OVER (PARTITION BY g) FROM w;
SELECT g, stddev(DISTINCT x), var_samp(DISTINCT x), stddev_pop(DISTINCT x), var_pop(DISTINCT x) FROM w GROUP BY g;
-- Two DISTINCT arguments keep `SingleDistinctToGroupBy` from rewriting the
-- DISTINCTs into a GROUP BY, so `bit_xor(DISTINCT a)` runs on its distinct
-- accumulator wherever it is computed locally.
SELECT g, bit_xor(DISTINCT a), count(DISTINCT k), sum(DISTINCT a) FROM w GROUP BY g;
-- PostgreSQL has no DISTINCT in a window function.
-- oracle-sql: SELECT w.id, d.n FROM w JOIN (SELECT g, count(DISTINCT x) AS n FROM w GROUP BY g) AS d ON w.g = d.g
SELECT id, count(DISTINCT x) OVER (PARTITION BY g) FROM w;

-- A float constant compared with a decimal compares as a decimal: the decimal
-- its literal spells, or its float value rounded to the comparison's scale, so
-- `0.06 + 0.01` is 0.07 and not the double 0.06999999999999999.
SELECT id FROM q WHERE disc BETWEEN 0.06 - 0.01 AND 0.06 + 0.01;
SELECT id FROM q WHERE disc = 0.1 + 0.2 - 0.23;
SELECT id FROM q WHERE disc IN (0.05, 0.03 + 0.04);
SELECT id FROM q WHERE 0.06 + 0.01 <= disc;
SELECT id FROM q WHERE disc <> 0.07;
