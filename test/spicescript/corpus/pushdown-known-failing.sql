-- Portable queries from pushdown.sql that Spice answers wrongly or rejects
-- today. Move each back to pushdown.sql once it passes.

-- MySQL BOOLEAN is TINYINT(1): accelerated, `b = true` fails to plan (Int8 = Boolean).
SELECT id FROM t WHERE b = true;
SELECT id FROM t WHERE b = false AND i > 0;
-- Federated, an outer join's nullable side keeps the source's NOT NULL and the
-- result fails to build ("declared as non-nullable but contains null values").
SELECT t.id, u.id FROM t LEFT JOIN u ON t.id = u.t_id WHERE t.id < 30;
