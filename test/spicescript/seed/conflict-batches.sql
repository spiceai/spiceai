-- Rows for conflict-batches.yaml. `across`: 8,192 distinct keys fill the first
-- batch and key 0 repeats with another value in the second. `wsame` and
-- `wdiff`: key 1 twice in one batch, identical and differing.
CREATE TABLE across (id bigint, region text, ts timestamp, v text);
INSERT INTO across SELECT i, 'us', '2026-01-01', 'first' FROM generate_series(0, 8191) i;
INSERT INTO across VALUES (0, 'us', '2026-01-01', 'last');
CREATE TABLE wsame (id bigint, region text, ts timestamp, v text);
INSERT INTO wsame VALUES (1, 'us', '2026-01-01', 'a'), (2, 'us', '2026-01-01', 'b'), (1, 'us', '2026-01-01', 'a');
CREATE TABLE wdiff (id bigint, region text, ts timestamp, v text);
INSERT INTO wdiff VALUES (1, 'us', '2026-01-01', 'a'), (2, 'us', '2026-01-01', 'b'), (1, 'us', '2026-01-01', 'c');
CREATE TABLE apsrc (id bigint, region text, ts timestamp, v text);
INSERT INTO apsrc SELECT i, 'us', '2026-01-01', 'first' FROM generate_series(0, 8191) i;
CREATE TABLE lpsrc (id bigint, region text, ts timestamp, v text);
INSERT INTO lpsrc SELECT i, 'us', '2026-01-01', 'first' FROM generate_series(0, 8191) i;
CREATE TABLE t (id bigint, region text, ts timestamp, v text);
INSERT INTO t SELECT i, 'us', '2026-01-01', 'v' || i FROM generate_series(0, 8192) i;
CREATE TABLE tp (id bigint, region text, ts timestamp, v text);
INSERT INTO tp SELECT k, r, '2026-01-01', r || k FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e']) r, unnest(ARRAY[1, 3]) k;
INSERT INTO tp VALUES (1, 'z', '2026-01-01', 'z1'), (2, 'z', '2026-01-01', 'z2');
CREATE TABLE ti (id bigint, region text, ts timestamp, v text);
INSERT INTO ti VALUES (1, 'us', '2026-01-01', 'stored');
CREATE TABLE tc (user_id bigint, product_id bigint, quantity bigint, price double precision);
INSERT INTO tc VALUES (1, 10, 1, 10.0), (1, 20, 2, 20.0), (2, 10, 3, 30.0), (2, 20, 4, 40.0);
