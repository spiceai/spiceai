-- Filters and projections over `v` that the Vortex scan under a Cayenne
-- acceleration converts and evaluates itself, checked against PostgreSQL over
-- the same rows. A filter the scan has to receive says so with `plan-has`, so the
-- comparison cannot pass by evaluating everything above the scan.

-- An IN list holding a column is not a constant list.
SELECT id FROM v WHERE a IN (b, 4, 5, 6, 7, 8);
SELECT id FROM v WHERE a NOT IN (b, 4, 5, 6, 7, 8);

-- SQL's three-valued IN: a null needle, and a null in the list.
-- plan-has cayenne: predicate:
SELECT id FROM v WHERE a IN (1, 2, 4, 5);
-- plan-has cayenne: predicate:
SELECT id FROM v WHERE a NOT IN (1, 2, 4, 5);
SELECT id FROM v WHERE NOT (a IN (1, 2, 4, 5));
SELECT id FROM v WHERE a IN (1, 2, 4, NULL);
-- expect-empty
SELECT id FROM v WHERE a NOT IN (1, 2, 4, NULL);
-- plan-has cayenne: predicate:
SELECT id FROM v WHERE s IN ('xy', 'q', 'zz', 'abc');
SELECT id FROM v WHERE s NOT IN ('xy', 'q', 'zz', 'abc');

-- A CASE that guards a division by zero.
SELECT id FROM v WHERE CASE WHEN b <> 0 THEN 12 / b ELSE 0 END = 6;
SELECT id FROM v WHERE CASE WHEN b = 0 THEN 0 ELSE 12 / b END = 6;
-- A CASE whose unselected branch overflows: `a + a` stays INT, and only the
-- extremes, which the THEN takes, overflow it.
SELECT id FROM v WHERE CASE WHEN a > 1000 OR a < -1000 THEN 0 ELSE a + a END > 1;
-- The same guards under a cast: a CASE that guards a division by zero, and a
-- comparison of two float columns, where -0.0 and 0.0 are equal.
SELECT id FROM v WHERE CAST(CASE WHEN b <> 0 THEN 12 / b ELSE 0 END AS DOUBLE PRECISION) = 6;
SELECT id FROM v WHERE CAST(f = g AS INTEGER) = 1;

-- Arithmetic over operands of different decimal types.
SELECT id FROM v WHERE p * 2 > 3;
SELECT id FROM v WHERE p * r > 3;
SELECT id FROM v WHERE p + r > 3;
SELECT id FROM v WHERE p - r > 0;
-- plan-has cayenne: predicate:
SELECT id FROM v WHERE p * p > 3;
-- Comparisons across decimal types, and integer arithmetic.
SELECT id FROM v WHERE p > r;
SELECT id FROM v WHERE p = r;
SELECT id FROM v WHERE a + b > 3;

-- A cast past the target's range is an error, not a row.
-- expect-error
SELECT id FROM v WHERE CAST(n AS BIGINT) > 0;

-- Signed zeros compare equal.
-- plan-has cayenne: predicate:
SELECT id FROM v WHERE f = -0.0;
SELECT id FROM v WHERE f = 0.0;

-- Projections the Vortex scan can evaluate. Cayenne's plan keeps them above its
-- scan today, so these check the answer wherever the projection runs.
-- An alias that shares a column's name.
SELECT id, b AS a, abs(a) AS x FROM v WHERE id <> 1;
SELECT id, a AS b, b AS a FROM v;
-- Expressions the scan has no conversion for, alone and beside ones it has.
SELECT id, a % 2 AS x FROM v;
SELECT id, -a AS x FROM v WHERE id <> 1;
SELECT id, a + CAST(character_length(s) AS INTEGER) AS x FROM v WHERE id > 1;
SELECT id, CASE WHEN b > 0 THEN a END AS x FROM v;
SELECT id, abs(a) AS x, a % 3 AS y FROM v WHERE id <> 1;
