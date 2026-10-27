-- Versions of keys, inserted so that a key's older version arrives AFTER its
-- newer one (a 300,000-row filler between them puts them in different
-- batches). Each table feeds one dataset in newest-by-time.yaml.
CREATE TABLE full_src (id bigint, occurred_at timestamp, v text);
INSERT INTO full_src VALUES (2, '2026-01-07', 'id2-newest'), (1, '2026-01-09', 'id1-newest'), (4, '2026-01-02', 'id4-older'),
  (4, '2026-01-03', 'id4-newest'), (5, '2026-01-04', 'id5-tie-a'), (3, '2026-01-05', 'id3-only');
INSERT INTO full_src SELECT 1000 + i, '2026-01-01', 'filler' FROM generate_series(0, 299999) i;
INSERT INTO full_src VALUES (1, '2026-01-08', 'id1-older'), (2, '2026-01-06', 'id2-older'), (5, '2026-01-04', 'id5-tie-b');
CREATE TABLE app_src (id bigint, occurred_at timestamp, v text);
INSERT INTO app_src VALUES (1, '2026-01-10', 'stored-jan10'), (9, '2026-01-10', 'other-key');
CREATE TABLE ret_src (id bigint, occurred_at timestamp, v text);
INSERT INTO ret_src VALUES (2, '2026-01-07', 'id2-newest'), (1, '2026-01-09', 'id1-newest'), (7, '2026-01-09', 'expired');
INSERT INTO ret_src SELECT 1000 + i, '2026-01-01', 'filler' FROM generate_series(0, 299999) i;
INSERT INTO ret_src VALUES (1, '2026-01-08', 'id1-older'), (2, '2026-01-06', 'id2-older');
CREATE TABLE nodet_src (id bigint, occurred_at timestamp, v text);
INSERT INTO nodet_src VALUES (1, '2026-01-10', 'id1'), (2, '2026-01-05', 'id2'), (3, '2026-01-08', 'id3');
CREATE TABLE del_src (id bigint, occurred_at timestamp, v text);
INSERT INTO del_src VALUES (1, '2026-01-10', 'id1');
INSERT INTO del_src SELECT 1000 + i, '2026-01-01', 'filler' FROM generate_series(0, 299999) i;
CREATE TABLE null_src (id bigint, occurred_at timestamp, v text);
INSERT INTO null_src VALUES (1, NULL, 'only-null'), (2, '2026-01-03', 'timed'), (2, NULL, 'null'), (3, NULL, 'null'), (3, '2026-01-01', 'timed');
INSERT INTO null_src SELECT 1000 + i, '2026-01-01', 'filler' FROM generate_series(0, 299999) i;
CREATE TABLE lp_src (id bigint, occurred_at timestamp, v text);
INSERT INTO lp_src VALUES (1, '2026-01-03', 'newer'), (2, '2026-01-01', 'two'), (1, '2026-01-02', 'initial');
