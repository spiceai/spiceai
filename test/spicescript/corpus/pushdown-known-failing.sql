-- Portable queries from pushdown.sql that Spice answers wrongly or rejects
-- today. Move each back to pushdown.sql once it passes.

-- MySQL BOOLEAN is TINYINT(1): accelerated, `b = true` fails to plan (Int8 = Boolean).
SELECT id FROM t WHERE b = true;
SELECT id FROM t WHERE b = false AND i > 0;
