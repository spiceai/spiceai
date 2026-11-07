-- Rows for corpus/builtins.sql: padding and Unicode spaces for trims, hashes
-- and hex over text and integers, regular expressions over non-ASCII letters
-- whose case folds to ASCII (U+212A KELVIN SIGN, U+017F LONG S, U+1C89/U+1C8A),
-- URLs for group references, and NULLs between values for windows.
CREATE TABLE names (id int PRIMARY KEY, name text);
INSERT INTO names VALUES
  (1, '  padded  '), (2, 'xyhelloyx'), (3, 'x hello x'), (4, NULL),
  (5, E'\u00a0x\u00a0'), (6, E'\u2003x\u2003'), (7, E'\u3000x\u3000'),
  (8, 'alpha'), (9, 'BeTa gamma'), (10, 'Ünïcödé'), (11, '');
CREATE TABLE hexes (id int PRIMARY KEY, h bigint);
INSERT INTO hexes VALUES (1, 255), (2, 3735928559), (3, 0), (4, -1), (5, NULL);
CREATE TABLE rx (id int PRIMARY KEY, s text, p text);
INSERT INTO rx VALUES
  (1, 'ab', 'a'), (2, 'xyz', 'a'), (3, 'aXbAb', 'a'), (4, NULL, 'a'), (5, 'ab', NULL),
  (6, NULL, NULL), (7, 'aaa', 'a'), (8, E'xy\u0661', 'a'), (9, E'\u212a', 'a'),
  (10, E'\u017f', 'a'), (11, E'\u1c89', 'a'), (12, E'\u1c8a', 'a'),
  (13, 'https://www.google.com/search?q=1', 'a'), (14, 'http://yandex.ru/a/b', 'a'),
  (15, 'https://example.org/', 'a'), (16, 'aab', 'a'), (17, 'b', 'a'), (18, 'aXbY', 'a'),
  (19, '', 'a');
CREATE TABLE cust (id int PRIMARY KEY, customer text, region text, qty int, ts text);
INSERT INTO cust VALUES
  (1, 'carol', 'eu', 5, '2026-01-15T10:00:00Z'), (2, 'alice', 'eu', 10, '2026-02-01T00:00:00Z'),
  (3, 'dave', 'eu', 15, '2026-03-01T12:00:00Z'), (4, 'alice', 'us', 10, '2026-01-20T08:00:00Z'),
  (5, 'bob', NULL, 20, NULL), (6, '  bob  ', NULL, NULL, '2026-04-01T00:00:00Z'),
  (7, 'Ångström', 'eu', 7, '2026-05-05T05:05:05Z');
CREATE TABLE vals (id int PRIMARY KEY, v int);
INSERT INTO vals VALUES (1, 10), (2, NULL), (3, 30), (4, NULL), (5, 50);
CREATE TABLE part (p_partkey int PRIMARY KEY, p_type text);
INSERT INTO part VALUES (1, 'LARGE BRASS'), (2, 'SMALL COPPER'), (3, 'ECONOMY BRASS'), (4, 'large brass');
CREATE TABLE partsupp (ps_partkey int, ps_suppkey int, ps_supplycost double precision, PRIMARY KEY (ps_partkey, ps_suppkey));
INSERT INTO partsupp VALUES (1, 10, 5.0), (1, 11, 3.0), (2, 12, 7.0), (3, 13, 9.0), (3, 14, 9.0), (4, 15, 1.0);
