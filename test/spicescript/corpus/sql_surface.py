#!/usr/bin/env python3
# Copyright 2024-2026 The Spice.ai OSS Authors
# Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0
"""Generate deterministic protocol cases and audit the documented SQL inventory.

Run with --check in CI. --docs-dir points at website/docs/reference/sql in a
checkout of spiceai/docs; --refresh-inventory records that checkout's inventory.
The catalog uses explicit, type-correct arguments; it never treats recognition
of a function name in an arbitrary SQL file as execution coverage.
"""
import argparse
import hashlib
import fnmatch
import json
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
SCALARS = {}


def unary(names, argument):
    for name in names.split():
        SCALARS[name] = f"{name}({argument})"


unary("abs acos asin asinh atan atanh cbrt ceil cos cosh cot degrees exp floor isnan iszero radians rint round signum sin sinh tan tanh trunc", "$n")
unary("acosh", "abs($n) + 1")
unary("ln log log10 log2 sqrt", "abs($n) + 1")
unary("factorial", "abs($i)")
unary("ascii bit_length char_length character_length initcap length lower ltrim octet_length reverse rtrim upper", "$s")
unary("bit_count to_hex", "$i")
unary("array_any_value array_avg array_dims array_distinct array_length array_max array_min array_ndims array_pop_back array_pop_front array_product array_reverse array_sum cardinality empty", "$a")
unary("map_keys map_values map_entries", "map(['a', 'b'], [$i, CAST(NULL AS BIGINT)])")
unary("md5 sha224 sha256 sha384 sha512", "$s")

# $-prefixed operands are substituted with typed literals or input columns.
for line in r"""
atan2 = atan2($n, $n + 1)
gcd = gcd(abs($i), CAST(6 AS BIGINT))
lcm = lcm(abs($i), CAST(6 AS BIGINT))
mod = mod($i, 3)
pmod = pmod($i, 3)
nanvl = nanvl($n, CAST(0 AS DOUBLE))
pi = pi()
pow = pow(abs($n) + 1, 2)
power = power(abs($n) + 1, 2)
random = random() >= 0 AND random() < 1
width_bucket = width_bucket($n, -1, 1, 4)
CASE = CASE WHEN $n > 0 THEN $d ELSE CAST(0 AS DECIMAL(10,3)) END
coalesce = coalesce($n, CAST(1 AS DOUBLE))
greatest = greatest($n, CAST(0 AS DOUBLE))
if = if($n > 0, $s, 'other')
least = least($n, CAST(0 AS DOUBLE))
nullif = nullif($i, CAST(2 AS BIGINT))
nvl = nvl($s, 'missing')
nvl2 = nvl2($s, 'present', 'missing')
btrim = btrim($s, 'ab ')
chr = chr(CAST(65 AS BIGINT) + abs($i))
concat = concat($s, ':', $s)
concat_ws = concat_ws(':', $s, $s)
contains = contains($s, 'a')
like = like($s, '%a%')
ilike = ilike($s, '%A%')
ends_with = ends_with($s, 'a')
find_in_set = find_in_set($s, 'apple,banana,')
instr = instr($s, 'a')
left = left($s, 2)
levenshtein = levenshtein($s, 'apple')
luhn_check = luhn_check('79927398713')
lpad = lpad($s, 15, '*')
overlay = overlay($s PLACING 'x' FROM 2 FOR 1)
parse_url = parse_url('https://example.com/a?q=1#fragment', 'HOST')
position = position('a' IN $s)
repeat = repeat($s, 2)
replace = replace($s, 'a', 'A')
right = right($s, 2)
rpad = rpad($s, 15, '*')
split_part = split_part($s, 'a', 2)
starts_with = starts_with($s, 'a')
strpos = strpos($s, 'a')
substr = substr($s, 2, 3)
substr_index = substr_index($s, 'a', 1)
substring = substring($s FROM 2 FOR 3)
substring_index = substring_index($s, 'a', 1)
translate = translate($s, 'ab', 'AB')
trim = trim(BOTH 'a' FROM $s)
uuid = length(uuid()) = 36
bit_get = bit_get($i, 1)
bitmap_count = bitmap_count(decode('0000000000000001', 'hex'))
regexp_like = regexp_like($s, 'a')
regexp_match = regexp_match($s, '(a)(.*)')
regexp_replace = regexp_replace($s, 'a', 'A', 'g')
regexp_count = regexp_count($s, 'a')
regexp_instr = regexp_instr($s, 'a')
current_date = current_date() = current_date()
current_time = current_time() = current_time()
current_timestamp = current_timestamp() = current_timestamp()
date_bin = date_bin(INTERVAL '1 hour', $ts, TIMESTAMP '2000-01-01 00:00:00')
date_add = date_add($dt, CAST($i AS INT))
date_sub = date_sub($dt, CAST($i AS INT))
last_day = last_day($dt)
next_day = next_day($dt, 'MON')
date_format = date_format($ts, 'yyyy-MM-dd')
date_part = date_part('month', $ts)
date_trunc = date_trunc('day', $ts)
datepart = datepart('month', $ts)
datetrunc = datetrunc('day', $ts)
from_unixtime = from_unixtime($i)
make_date = make_date(2024, 2, CAST(abs($i) AS INT) + 1)
now = now() = now()
to_char = to_char($ts, '%Y-%m-%d')
to_date = to_date('2024-02-29', '%Y-%m-%d')
to_local_time = to_local_time(arrow_cast($ts, 'Timestamp(Nanosecond, Some("UTC"))'))
to_timestamp = to_timestamp('2024-02-29T12:34:56Z')
to_timestamp_micros = to_timestamp_micros($i)
to_timestamp_millis = to_timestamp_millis($i)
to_timestamp_nanos = to_timestamp_nanos($i)
to_timestamp_seconds = to_timestamp_seconds($i)
to_unixtime = to_unixtime($ts)
today = today() = today()
array = array($i, CAST(NULL AS BIGINT), $i)
array_add = array_add($a, $a)
array_append = array_append($a, CAST(3 AS BIGINT))
array_cat = array_cat($a, [1,2])
array_concat = array_concat($a, [1,2])
array_contains = array_contains($a, CAST(2 AS BIGINT))
array_distance = array_distance([1.0, 2.0], [2.0, 3.0])
array_element = array_element($a, 2)
array_except = array_except($a, [2])
array_has = array_has($a, CAST(2 AS BIGINT))
array_has_all = array_has_all($a, [1,2])
array_has_any = array_has_any($a, [1,3])
array_intersect = array_intersect($a, [1,2])
array_position = array_position($a, CAST(2 AS BIGINT))
array_positions = array_positions($a, CAST(2 AS BIGINT))
array_prepend = array_prepend(CAST(0 AS BIGINT), $a)
array_remove = array_remove($a, CAST(2 AS BIGINT))
array_remove_n = array_remove_n($a, CAST(2 AS BIGINT), 1)
array_remove_all = array_remove_all($a, CAST(2 AS BIGINT))
array_repeat = array_repeat($i, 2)
array_replace = array_replace($a, CAST(2 AS BIGINT), CAST(9 AS BIGINT))
array_replace_n = array_replace_n($a, CAST(2 AS BIGINT), CAST(9 AS BIGINT), 1)
array_replace_all = array_replace_all($a, CAST(2 AS BIGINT), CAST(9 AS BIGINT))
array_resize = array_resize($a, 6, CAST(9 AS BIGINT))
array_scale = array_scale($a, 2)
array_slice = array_slice($a, 1, 3)
array_sort = array_sort($a)
array_subtract = array_subtract($a, $a)
array_to_string = array_to_string($a, '|', '<null>')
array_union = array_union($a, [2,3])
arrays_zip = arrays_zip($a, [1,2,3,4])
flatten = flatten([$a, $a])
make_array = make_array($i, CAST(NULL AS BIGINT))
range = range(0, abs($i), 1)
generate_series = generate_series(0, abs($i), 1)
string_to_array = string_to_array($s, 'a')
struct = struct($i, $s)
named_struct = named_struct('n', $i, 's', $s)
get_field = get_field(named_struct('n', $i), 'n')
map = map(['a', 'b'], [$i, CAST(NULL AS BIGINT)])
map_extract = map_extract(map(['a', 'b'], [$i, CAST(NULL AS BIGINT)]), 'a')
digest = digest($s, 'sha256')
encode = encode(CAST($s AS BYTEA), 'hex')
decode = decode('6162', 'hex')
union_extract = union_extract(json_get('{"x": 42}', 'x'), 'int')
union_tag = CAST(union_tag(json_get('{"x": 42}', 'x')) AS VARCHAR)
obj_description = obj_description('t')
col_description = col_description('t', 'id')
arrow_cast = arrow_cast($i, 'Int32')
arrow_try_cast = arrow_try_cast($s, 'Int64')
arrow_typeof = arrow_typeof($d)
arrow_metadata = arrow_metadata($i)
version = version()
bucket = bucket(4, $i)
truncate = truncate(3, $i)
""".strip().splitlines():
    name, expr = line.split(" = ", 1)
    SCALARS[name] = expr

JSON_LITERAL = "'{\"text\":\"hello\",\"int\":42,\"float\":1.5,\"bool\":true,\"array\":[1,null,3],\"object\":{\"x\":1}}'"
JSON_DOCUMENT = "$j"
for name, key in {"json_get_str": "text", "json_get_int": "int", "json_get_float": "float", "json_get_bool": "bool", "json_get_array": "array", "json_get_json": "object"}.items():
    SCALARS[name] = f"{name}({JSON_DOCUMENT}, '{key}')"
SCALARS.update({
    "json_contains": f"json_contains({JSON_DOCUMENT}, 'int')",
    "json_get": f"json_as_text(json_get({JSON_DOCUMENT}, 'int'))",
    "json_as_text": f"json_as_text(json_get({JSON_DOCUMENT}, 'text'))",
    "json_length": f"json_length({JSON_DOCUMENT}, 'array')",
    "json_object_keys": f"json_object_keys({JSON_DOCUMENT})",
    "flatten_json": f"flatten_json({JSON_DOCUMENT})",
    "flatten_json_properties": "flatten_json_properties('{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"}}}')",
    "json_tree": f"json_tree({JSON_DOCUMENT})",
})

AGGREGATES = {}
for name in "any_value array_agg avg bit_and bit_or bit_xor count first_value last_value max mean median min sum var var_pop var_population var_samp var_sample stddev stddev_pop stddev_samp approx_distinct approx_median".split():
    AGGREGATES[name] = f"{name}(i)"
for name in "bool_and bool_or".split():
    AGGREGATES[name] = f"{name}(i > 0)"
for name in "corr covar covar_pop covar_samp regr_avgx regr_avgy regr_count regr_intercept regr_r2 regr_slope regr_sxx regr_sxy regr_syy".split():
    AGGREGATES[name] = f"{name}(n, CAST(i AS DOUBLE))"
AGGREGATES.update({
    "grouping": "grouping(i)",
    "percentile_cont": "percentile_cont(0.5) WITHIN GROUP (ORDER BY n)",
    "string_agg": "string_agg(s, ',' ORDER BY id)",
    "nth_value": "nth_value(i, 2 ORDER BY id)",
    "approx_percentile_cont": "approx_percentile_cont(n, 0.5)",
    "approx_percentile_cont_with_weight": "approx_percentile_cont_with_weight(n, CAST(1 AS DOUBLE), 0.5)",
})

LITERALS = {
    "n": "CAST(0.5 AS DOUBLE)", "i": "CAST(2 AS BIGINT)",
    "d": "CAST(1.255 AS DECIMAL(10,3))", "s": "'banana naïve'",
    "a": "[CAST(1 AS BIGINT), 2, 2, NULL]", "dt": "DATE '2024-02-29'",
    "ts": "TIMESTAMP '2024-02-29 12:34:56'", "j": JSON_LITERAL,
}
INPUT = """WITH input(id, n, i, d, s, a, dt, ts, j) AS (
  VALUES (1, CAST(0.5 AS DOUBLE), CAST(2 AS BIGINT), CAST(1.255 AS DECIMAL(10,3)),
          'banana naïve', [CAST(1 AS BIGINT),2,2,NULL], DATE '2024-02-29', TIMESTAMP '2024-02-29 12:34:56', JSON_LITERAL),
         (2, CAST(-0.5 AS DOUBLE), CAST(-2 AS BIGINT), CAST(-1.255 AS DECIMAL(10,3)),
          '', CAST([] AS BIGINT[]), DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59', '{}'),
         (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
         (4, CAST(0.5 AS DOUBLE), CAST(2 AS BIGINT), CAST(1.255 AS DECIMAL(10,3)),
          'banana naïve', [CAST(1 AS BIGINT),2,2,NULL], DATE '2024-02-29', TIMESTAMP '2024-02-29 12:34:56', JSON_LITERAL)
)""".replace("JSON_LITERAL", JSON_LITERAL)


def substitute(expr, literals=False):
    return re.sub(r"\$(\w+)", lambda match: LITERALS[match[1]] if literals else match[1], expr)


OVERLOADS = {
    "log": ["log(2, abs($n) + 1)"],
    "round": ["round($d, 2)", "round($d, CAST($i AS INT))"],
    "trunc": ["trunc($d, 2)", "trunc($d, CAST($i AS INT))"],
    "btrim": ["btrim($s)"], "ltrim": ["ltrim($s, 'a')"], "rtrim": ["rtrim($s, 'a')"],
    "lpad": ["lpad($s, 15)"], "rpad": ["rpad($s, 15)"],
    "overlay": ["overlay($s PLACING 'x' FROM 2)"],
    "parse_url": ["parse_url('https://example.com/?q=value', 'QUERY', 'q')"],
    "substr": ["substr($s, 2)"], "substring": ["substring($s FROM 2)"],
    "regexp_like": ["regexp_like($s, 'A', 'i')"],
    "regexp_match": ["regexp_match($s, '(A)', 'i')"],
    "regexp_count": ["regexp_count($s, 'a', 2)", "regexp_count($s, 'A', 1, 'i')"],
    "regexp_instr": ["regexp_instr($s, '(a)', 1)", "regexp_instr($s, '(a)', 1, 1)",
                     "regexp_instr($s, '(A)', 1, 1, 'i')", "regexp_instr($s, '(A)', 1, 1, 'i', 1)"],
    "from_unixtime": ["from_unixtime($i, 'UTC')"],
    "to_date": ["to_date('2024-02-29')"],
    "to_timestamp": ["to_timestamp('2024-02-29', '%Y-%m-%d')"],
    "array_length": ["array_length($a, 1)"],
    "array_position": ["array_position(array_concat($a, [1,2]), CAST(2 AS BIGINT), 2)"],
    "array_resize": ["array_resize($a, 2)"],
    "array_slice": ["array_slice($a, 1, 4, 2)"],
    "array_sort": ["array_sort($a, 'DESC')", "array_sort($a, 'ASC', 'NULLS FIRST')"],
    "array_to_string": ["array_to_string($a, '|')"],
    "range": ["range(abs($i))", "range(0, abs($i))"],
    "generate_series": ["generate_series(0, abs($i))"],
    "string_to_array": ["string_to_array($s, 'a', '')"],
    "map": ["map('a', $i, 'b', CAST(NULL AS BIGINT))"],
    "obj_description": ["obj_description('t', 'pg_class')", "obj_description('public', 't')",
                        "obj_description('spice', 'public', 't')"],
    "col_description": ["col_description('spice', 'public', 't', 'id')"],
    "encode": ["encode(CAST($s AS BYTEA), 'base64')"],
    "decode": ["decode('YWI=', 'base64')"],
}
for algorithm in ["md5", "sha224", "sha256", "sha384", "sha512", "blake2s", "blake2b", "blake3"]:
    OVERLOADS.setdefault("digest", []).append(f"digest($s, '{algorithm}')")


def scalar_cases():
    for name, expr in SCALARS.items():
        yield f"scalar.{name}.literal", f"SELECT {substitute(expr, True)} AS value", False
        yield f"scalar.{name}.column", f"{INPUT}\nSELECT id, {substitute(expr)} AS value FROM input ORDER BY id", False
        yield f"scalar.{name}.empty", f"{INPUT}\nSELECT id, {substitute(expr)} AS value FROM input WHERE id < 0", True
    for name, expressions in OVERLOADS.items():
        for index, expr in enumerate(expressions):
            yield f"overload.{name}.{index}", f"{INPUT}\nSELECT id, {substitute(expr)} AS value FROM input ORDER BY id", False
    # Literal-sensitive return types must be exercised after implicit casts,
    # literal folding, column evaluation and aggregation.
    for function in ["round", "trunc"]:
        for dp in [None, "2", "CAST(2 AS BIGINT)", "1 + 1", "-1", "0", "4"]:
            args = "d" + (", " + dp if dp is not None else "")
            yield f"schema.{function}.{dp}", f"{INPUT}\nSELECT id, {function}({args}) AS value FROM input ORDER BY id", False
        yield f"schema.{function}.aggregate", f"{INPUT}\nSELECT {function}(avg(d), 2) AS value FROM input", False


def aggregate_cases():
    for name, expr in AGGREGATES.items():
        if name == "grouping":
            suffix = " GROUP BY GROUPING SETS ((i), ())"
        else:
            suffix = ""
        yield f"aggregate.{name}.column", f"{INPUT}\nSELECT {expr} AS value FROM input{suffix}", False
        yield f"aggregate.{name}.empty", f"{INPUT}\nSELECT {expr} AS value FROM input WHERE id < 0{suffix}", name == "grouping"



SYNTAX = {
    "select.all": "SELECT ALL i AS value FROM input",
    "select.distinct": "SELECT DISTINCT i AS value FROM input",
    "select.alias": 'SELECT i AS "MixedCase" FROM input',
    "select.exclude": "SELECT * EXCLUDE (a) FROM input",
    "select.except": "SELECT * EXCEPT (a) FROM input",
    "select.replace": "SELECT * REPLACE (abs(i) AS i) FROM input",
    "select.ilike": "SELECT * ILIKE 'd%' FROM input",
    "where": "SELECT id, i FROM input WHERE i > 0 AND s IS NOT NULL",
    "group.all": "SELECT i, count(*) AS n FROM input GROUP BY ALL",
    "order.all": "SELECT i, id FROM input ORDER BY ALL",
    "group.having": "SELECT i, count(*) AS n FROM input GROUP BY i HAVING count(*) > 1",
    "group.rollup": "SELECT i, count(*) AS n FROM input GROUP BY ROLLUP(i)",
    "group.cube": "SELECT i, n, count(*) AS total FROM input GROUP BY CUBE(i,n)",
    "group.sets": "SELECT i, n, grouping(i,n) AS g FROM input GROUP BY GROUPING SETS ((i),(n),())",
    "qualify": "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM input QUALIFY rn <= 2",
    "order.limit": "SELECT id, i FROM input ORDER BY i DESC NULLS LAST, id LIMIT 2 OFFSET 1",
    "order.alias": "SELECT id AS key, i FROM input ORDER BY key DESC",
    "order.ordinal": "SELECT id, i FROM input ORDER BY 2 NULLS LAST, 1",
    "order.expression": "SELECT id, i FROM input ORDER BY abs(i) NULLS LAST, id",
    "order.hidden": "SELECT i FROM input ORDER BY id",
    "limit.zero": "SELECT id FROM input LIMIT 0",
    "limit.offset": "SELECT id FROM input ORDER BY id OFFSET 2",
    "with": "SELECT id FROM (WITH x AS (SELECT id FROM input) SELECT * FROM x) q",
    "with.recursive": "SELECT * FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT sum(n) AS total FROM c) q",
    "subquery.scalar": "SELECT id, (SELECT max(i) FROM input) AS value FROM input",
    "subquery.correlated": "SELECT x.id, (SELECT max(y.i) FROM t y WHERE y.b = x.b) AS value FROM t x",
    "subquery.correlated_range": "SELECT x.id, (SELECT max(y.i) FROM t y WHERE y.id <= x.id) AS value FROM t x",
    "subquery.where_correlated": "SELECT x.id FROM t x WHERE x.i > (SELECT avg(y.i) FROM t y WHERE y.b = x.b)",
    "subquery.exists": "SELECT x.id FROM input x WHERE EXISTS (SELECT 1 FROM input y WHERE y.i = x.i)",
    "subquery.not_exists": "SELECT x.id FROM input x WHERE NOT EXISTS (SELECT 1 FROM input y WHERE y.i = x.i)",
    "subquery.in": "SELECT id FROM input WHERE i IN (SELECT i FROM input WHERE id < 3)",
    "subquery.not_in": "SELECT id FROM input WHERE i NOT IN (SELECT i FROM input WHERE i IS NOT NULL AND id = 2)",
    "subquery.having": "SELECT i, count(*) AS n FROM input GROUP BY i HAVING count(*) >= (SELECT min(id) FROM input)",
    "subquery.any": "SELECT id, i = ANY(SELECT i FROM t WHERE id < 3) AS value FROM t",
    "subquery.all": "SELECT id, i >= ALL(SELECT i FROM t WHERE id < 3) AS value FROM t",
    "unnest": "SELECT id, unnest(a) AS value FROM input",
    "array.index": "SELECT id, a[1] AS value, a[99] AS missing FROM input",
    "aggregate.filter": "SELECT count(*) FILTER (WHERE i > 0) AS positive, sum(d) FILTER (WHERE i < 0) AS negative FROM input",
    "aggregate.distinct": "SELECT count(DISTINCT i) AS n, sum(DISTINCT d) AS total FROM input",
    "window.rows": "SELECT id, sum(i) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS value FROM input",
    "window.range": "SELECT id, sum(i) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value FROM input",
    "window.ignore_nulls": "SELECT id, first_value(i) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS value FROM input",
    "json.arrow": "SELECT json_as_text('{\"x\":42}' -> 'x') AS value",
    "json.arrow_text": "SELECT '{\"x\":42}' ->> 'x' AS value",
    "json.contains_operator": "SELECT '{\"x\":42}' ? 'x' AS value",
    "json.udtf.flatten_json": "SELECT * FROM flatten_json('{\"x\":[1,null,3]}')",
    "json.udtf.flatten_json_properties": "SELECT * FROM flatten_json_properties('{\"properties\":{\"x\":{\"type\":\"integer\"}}}')",
    "json.udtf.json_tree": "SELECT * FROM json_tree('{\"x\":[1,null,3]}')",
    "metadata.tables": "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'",
    "metadata.columns": "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 't'",
    "metadata.show_tables": "SHOW TABLES",
    "metadata.show_columns": "SHOW COLUMNS FROM t",
    "metadata.show_all": "SHOW ALL",
    "explain": "EXPLAIN SELECT id FROM input WHERE i > 0",
    # EXPLAIN ANALYZE contains timing measurements, so compare the query rows
    # separately; it has a dedicated existing explain-step fixture.
}
for join in ["INNER", "LEFT", "RIGHT", "FULL", "LEFT SEMI", "LEFT ANTI"]:
    SYNTAX[f"join.{join.lower().replace(' ', '_')}"] = f"SELECT x.id AS left_id FROM input x {join} JOIN input y ON x.i = y.i"
SYNTAX["join.cross"] = "SELECT x.id AS left_id, y.id AS right_id FROM input x CROSS JOIN input y"
SYNTAX["join.using"] = "SELECT id FROM input x JOIN input y USING (id)"
SYNTAX["join.natural"] = "SELECT id FROM input x NATURAL JOIN input y"
for operation in ["UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]:
    SYNTAX[f"set.{operation.lower().replace(' ', '_')}"] = f"SELECT i AS value FROM input WHERE id <= 3 {operation} SELECT i AS value FROM input WHERE id > 1"
for function in ["row_number", "rank", "dense_rank", "percent_rank", "cume_dist"]:
    SYNTAX[f"window.{function}"] = f"SELECT id, {function}() OVER (PARTITION BY i ORDER BY id) AS value FROM input"
for function, arguments in {"ntile": "2", "lag": "i, 1, CAST(0 AS BIGINT)", "lead": "i, 1, CAST(0 AS BIGINT)", "first_value": "i", "last_value": "i", "nth_value": "i, 2"}.items():
    SYNTAX[f"window.{function}"] = f"SELECT id, {function}({arguments}) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS value FROM input"

for format_name in ["indent", "tree", "pgjson", "graphviz"]:
    SYNTAX[f"explain.format.{format_name}"] = f"EXPLAIN FORMAT {format_name} SELECT id FROM t WHERE i > 0"
SYNTAX["explain.verbose"] = "EXPLAIN VERBOSE SELECT id FROM t WHERE i > 0"
SYNTAX["explain.options"] = "EXPLAIN (ANALYZE OFF, VERBOSE FALSE, FORMAT indent, COSTS TRUE) SELECT id FROM t WHERE i > 0"


OPERATORS = {
    "+": "i + 1", "-": "i - 1", "*": "i * 2", "/": "i / 2", "%": "i % 2",
    "=": "i = 2", "!=": "i != 2", "<": "i < 2", "<=": "i <= 2", ">": "i > 2", ">=": "i >= 2",
    "<=>": "i <=> CAST(NULL AS BIGINT)", "IS DISTINCT FROM": "i IS DISTINCT FROM 2",
    "IS NOT DISTINCT FROM": "i IS NOT DISTINCT FROM NULL",
    "~": "s ~ 'a'", "~*": "s ~* 'A'", "!~": "s !~ 'a'", "!~*": "s !~* 'A'",
    "~~": "s ~~ '%a%'", "~~*": "s ~~* '%A%'", "!~~": "s !~~ '%a%'", "!~~*": "s !~~* '%A%'",
    "AND": "(i > 0) AND (s IS NOT NULL)", "OR": "(i > 0) OR (s IS NULL)", "NOT": "NOT (i > 0)",
    "&": "i & 3", "|": "i | 3", "#": "i # 3", "^": "i ^ 3", ">>": "i >> 1", "<<": "i << 1",
    "CAST(expr AS type)": "CAST(i AS VARCHAR)", "::": "i::VARCHAR", "||": "s || '!'",
    "@>": "a @> [1,2]", "<@": "a <@ [1,2,3]",
    "BETWEEN": "i BETWEEN -2 AND 2", "NOT BETWEEN": "i NOT BETWEEN -2 AND 2",
    "IN": "i IN (-2,2,NULL)", "NOT IN": "i NOT IN (-2,2,NULL)",
    "IS TRUE": "(i > 0) IS TRUE", "IS NOT TRUE": "(i > 0) IS NOT TRUE",
    "IS FALSE": "(i > 0) IS FALSE", "IS UNKNOWN": "(i > 0) IS UNKNOWN",
    "LIKE ESCAPE": r"s LIKE '%\_%' ESCAPE '\'",
}
LITERAL_CASES = {
    "integer": "SELECT -9223372036854775808 AS lo, 9223372036854775807 AS hi",
    "decimal": "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS value",
    "decimal256": "SELECT arrow_cast('1234567890123456789012345678901234567890', 'Decimal256(40, 0)') AS value",
    "boolean": "SELECT TRUE AS yes, FALSE AS no, CAST(NULL AS BOOLEAN) AS unknown",
    "string": "SELECT 'O''Brien' AS quoted, 'naïve 日本語' AS unicode, '' AS empty",
    "escape": r"SELECT E'line1\nline2\t\\' AS value",
    "date": "SELECT DATE '1969-12-31' AS value",
    "time": "SELECT TIME '12:34:56' AS value",
    "timestamp": "SELECT TIMESTAMP '1969-12-31 23:59:59.123456789' AS value",
    "interval": "SELECT CAST(INTERVAL '1 day 2 hours' AS VARCHAR) AS value",
    "binary": "SELECT decode('0001ff', 'hex') AS value",
    "empty_array": "SELECT CAST([] AS BIGINT[]) AS value",
    "null_array": "SELECT CAST(NULL AS BIGINT[]) AS value",
    "try_cast": "SELECT TRY_CAST('bad' AS BIGINT) AS value",
    "float_special": "SELECT CAST('NaN' AS DOUBLE) AS nan, CAST('Infinity' AS DOUBLE) AS inf, CAST('-Infinity' AS DOUBLE) AS neg_inf, CAST('-0.0' AS DOUBLE) AS zero",
    "case_coercion": "SELECT CASE WHEN TRUE THEN CAST(1.23 AS DECIMAL(6,2)) ELSE 0 END AS value",
    "nullif_coercion": "SELECT nullif(CAST(1.25 AS DECIMAL(6,2)), 0) AS value",
}


def syntax_cases():
    for name, sql in SYNTAX.items():
        # Self-contained metadata and table functions do not need the CTE.
        if "input" in sql:
            if sql.startswith("EXPLAIN "):
                sql = "EXPLAIN " + INPUT + "\n" + sql.removeprefix("EXPLAIN ")
            else:
                sql = INPUT + "\n" + sql
        # EXCEPT ALL and INTERSECT ALL need not be empty for this fixture;
        # the two fully equal multisets are only the explicit empty probe.
        yield "syntax." + name, sql, name in {"set.except", "set.except_all", "limit.zero"}
    for name, expr in OPERATORS.items():
        yield "operator." + name, f"{INPUT}\nSELECT id, {expr} AS value FROM input ORDER BY id", False
    for name, sql in LITERAL_CASES.items():
        yield "literal." + name, sql, False

# Fixture-dependent features are tracked separately and never counted as
# deterministic cross-protocol executions by this generator.
EXTERNAL = {
    "ai": ("crates/runtime-datafusion-udfs/src/ai.rs", "Uses deterministic chat-model fixtures, separate from protocol parity."),
    "embed": ("test/spicescript/search-hybrid.yaml", "Requires a configured embedding model."),
    "ai_if": ("crates/runtime-decide/tests/decisions.rs", "Uses deterministic decision-model fixtures, separate from protocol parity."),
    "ai_probability": ("crates/runtime-decide/tests/decisions.rs", "Uses deterministic decision-model fixtures, separate from protocol parity."),
    "ai_classify": ("crates/runtime-decide/tests/decisions.rs", "Uses deterministic decision-model fixtures, separate from protocol parity."),
    "ai_score": ("crates/runtime-decide/tests/decisions.rs", "Uses deterministic decision-model fixtures, separate from protocol parity."),
    "ai_decide": ("crates/runtime-decide/tests/decisions.rs", "Uses deterministic decision-model fixtures, separate from protocol parity."),
    "vector_search": ("test/spicescript/search-hybrid.yaml", "Requires indexed embeddings."),
    "text_search": ("test/spicescript/search.yaml", "Requires a full-text index."),
    "rrf": ("test/spicescript/search-hybrid.yaml", "Requires search indexes."),
    "rerank": ("crates/runtime-search/src/rerank.rs", "Requires a configured reranking model."),
}


# The SQL reference index defines the surface categories. Each category maps
# to explicit generated cases or to a fixture that can exercise its effects.
TOPICS = {
    "SELECT Syntax": "syntax.*",
    "Subqueries": "syntax.subquery.*",
    "EXPLAIN": "syntax.explain.*; test/spicescript/sql-explain-protocols.yaml",
    "Information Schema": "syntax.metadata.*",
    "AI Functions": "fixture: model-backed AI functions",
    "Operators and Literals": "operator.*; literal.*; test/spicescript/sql-operator-values.yaml",
    "Scalar Functions": "scalar.*",
    "Aggregate Functions": "aggregate.*",
    "Window Functions": "syntax.window.*",
    "JSON Functions and Operators": "scalar.json_*; syntax.json.*",
    "Search": "fixture: test/spicescript/search.yaml and search-hybrid.yaml",
    "Prepared Statements": "fixture: test/spicescript/flight-prepared.yaml and flight-sessions.yaml",
    "DML (Data Manipulation Language)": "fixture: test/spicescript/flight-writes.yaml and cayenne-writes.yaml",
    "Data Types": "literal.*; scalar.arrow_cast.*",
}


SELECT_FEATURES = {
    "Window Functions (OVER Clause)": "syntax.window.*",
    "WITH clause": "syntax.with*", "SELECT clause": "syntax.select.*",
    "FROM clause": "syntax.with", "WHERE clause": "syntax.where",
    "JOIN clause": "syntax.join.*", "INNER JOIN": "syntax.join.inner",
    "LEFT OUTER JOIN": "syntax.join.left", "RIGHT OUTER JOIN": "syntax.join.right",
    "FULL OUTER JOIN": "syntax.join.full", "NATURAL JOIN": "syntax.join.natural",
    "CROSS JOIN": "syntax.join.cross", "GROUP BY clause": "syntax.group.*",
    "GROUP BY ALL": "syntax.group.all", "HAVING clause": "syntax.group.having",
    "QUALIFY clause": "syntax.qualify", "UNION clause": "syntax.set.union*",
    "ORDER BY clause": "syntax.order.*", "ORDER BY ALL": "syntax.order.all",
    "LIMIT clause": "syntax.limit.*",
    "EXCLUDE, EXCEPT, REPLACE, and ILIKE clauses": "syntax.select.*",
}


def documented_syntax(docs):
    select = re.findall(r"^#{3,4} (.+)$", (docs / "select.md").read_text(), re.M)
    select = [title.strip("`") for title in select if title != "Additional Example"]
    options = re.findall(r"^\|\s*`([A-Z]+)`", (docs / "explain.md").read_text(), re.M)
    return {"select_features": select, "explain_options": options}


def documented_topics(docs):
    titles = re.findall(r"^### (.+)$", (docs / "index.md").read_text(), re.M)
    return [re.sub(r"^\[([^]]+)\]\([^)]*\)$", r"\1", title) for title in titles]


def documented_operators(docs):
    return re.findall(r"^### `([^`]+)`", (docs / "operators.md").read_text(), re.M)


def documentation_hashes(docs):
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(docs.glob("*.md"))}


def documented_functions(docs):
    entries = []
    for filename in ["scalar_functions.md", "aggregate_functions.md", "json.md", "ai.md", "search.md"]:
        text = (docs / filename).read_text()
        sections = list(re.finditer(r"^#{2,3} (.+)$", text, re.M))
        for index, section in enumerate(sections):
            names = re.findall(r"`([A-Za-z_]\w*)`", section[1])
            if not names:
                continue
            body = text[section.end():sections[index + 1].start() if index + 1 < len(sections) else len(text)]
            aliases = []
            for line in re.findall(r"(?:Alias|Aliases):([^\n]+)", body):
                aliases += re.findall(r"`([a-z_]\w*)`", line)
            for block in re.findall(r"#### Aliases\n(.*?)(?=\n####|\Z)", body, re.S):
                aliases += re.findall(r"^- (?:`)?([a-z_]\w*)", block, re.M)
            for name in names + aliases:
                entries.append({"name": name, "canonical": names[0], "page": filename.removesuffix(".md")})
    return list({(e["page"], e["name"]): e for e in entries}.values())


def generated_sql(cases):
    chunks = ["-- Generated by sql_surface.py; edit the generator and regenerate.\n"]
    for name, sql, empty in cases:
        chunks += [f"-- surface: {name}\n", "-- expect-empty\n" if empty else "", sql + ";\n\n"]
    return "".join(chunks).rstrip() + "\n"


def operator_value_fixture(known_only=False):
    cases = {
        "2 + 3": "5", "2 - 3": "-1", "2 * 3": "6", "-7 / 3": "-2", "-7 % 3": "-1",
        "2 = 3": "false", "2 != 3": "true", "2 < 3": "true", "2 <= 2": "true",
        "3 > 2": "true", "3 >= 3": "true", "NULL <=> NULL": "true",
        "NULL IS DISTINCT FROM 1": "true", "NULL IS NOT DISTINCT FROM NULL": "true",
        "TRUE AND NULL": None, "FALSE AND NULL": "false", "TRUE OR NULL": "true", "NOT NULL::BOOLEAN": None,
        "6 & 3": "2", "6 | 3": "7", "6 # 3": "5", "6 ^ 3": "5", "6 >> 1": "3", "3 << 1": "6",
        "'ab' || 'cd'": "abcd", "CAST(42 AS VARCHAR)": "42", "'42'::BIGINT": "42",
        "'Apple' ~* 'apple'": "true", "'Apple' !~ 'apple'": "true",
        "[1,2,3] @> [1,2]": "true", "[1,2] <@ [1,2,3]": "true",
    }
    known_values = json.loads((HERE / "sql-surface-known-values.json").read_text())
    stale = set(known_values) - cases.keys()
    if stale:
        raise SystemExit(f"Known operator expressions no longer exist: {stale}")
    steps = [{"start": {}}]
    for expression, expected in cases.items():
        if (expression in known_values) != known_only:
            continue
        steps.append({"query": f"SELECT {expression} AS value", "protocols": ["flightsql", "http"],
                      "expect": {"rows": [[expected]]}})
    return json.dumps({"name": "sql-operator-values-known" if known_only else "sql-operator-values", "description": "Documented operator semantics checked against literal expected values over both protocols.",
                       "spicepod": {"version": "v1", "kind": "Spicepod", "name": "sql-operator-values"},
                       "steps": steps}, indent=2) + "\n"


def explain_fixture():
    statements = ["EXPLAIN ANALYZE SELECT 1 AS n", "EXPLAIN ANALYZE VERBOSE SELECT 1 AS n"]
    statements += [f"EXPLAIN ANALYZE FORMAT {format_name} SELECT 1 AS n"
                   for format_name in ["indent", "tree", "pgjson", "graphviz"]]
    options = ["METRICS 'all'", "METRICS 'none'", "METRICS 'rows,bytes'", "LEVEL summary",
               "LEVEL dev", "TIMING TRUE", "TIMING FALSE", "SUMMARY TRUE", "SUMMARY FALSE"]
    statements += [f"EXPLAIN (ANALYZE TRUE, {option}) SELECT 1 AS n" for option in options]
    statements += [f"EXPLAIN (ANALYZE {boolean}) SELECT 1 AS n" for boolean in ["ON", "OFF", "1", "0"]]
    steps = [{"start": {}}]
    for sql in statements:
        unsupported = any(f"FORMAT {name}" in sql for name in ["tree", "graphviz"])
        query = {"query": sql, "protocols": ["flightsql"]}
        if unsupported:
            query["expect-error"] = "is not supported"
        else:
            query["expect"] = {}
        steps.append(query)
        steps.append({"http": {"method": "POST", "path": "/v1/sql",
                               "headers": {"content-type": "text/plain"}, "body": sql},
                      "expect": {"status": 400 if unsupported else 200,
                                 "body-contains": "is not supported" if unsupported else "plan"}})
    for option in ["BUFFERS", "WAL", "SETTINGS", "GENERIC_PLAN", "MEMORY"]:
        steps.append({"query": f"EXPLAIN ({option}) SELECT 1", "protocols": ["flightsql"],
                      "expect-error": option})
    return json.dumps({"name": "sql-explain-protocols", "description": "Documented EXPLAIN options over Flight SQL and HTTP; metric text is execution-dependent.",
                       "spicepod": {"version": "v1", "kind": "Spicepod", "name": "sql-explain-protocols"},
                       "steps": steps}, indent=2) + "\n"


def write_or_check(path, text, check):
    if check:
        if not path.exists() or path.read_text() != text:
            raise SystemExit(f"{path}: stale; run python3 {HERE / 'sql_surface.py'}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--docs-dir", type=Path)
    parser.add_argument("--refresh-inventory", action="store_true")
    args = parser.parse_args()
    if args.check and args.refresh_inventory:
        parser.error("--check and --refresh-inventory cannot be combined")
    inventory_path = HERE / "sql-surface-inventory.json"
    if args.refresh_inventory:
        if not args.docs_dir:
            parser.error("--refresh-inventory requires --docs-dir")
        revision = subprocess.check_output(["git", "-C", str(args.docs_dir), "rev-parse", "HEAD"], text=True).strip()
        inventory = {"repository": "https://github.com/spiceai/docs", "revision": revision,
                     "path": "website/docs/reference/sql", "functions": documented_functions(args.docs_dir),
                     "topics": documented_topics(args.docs_dir), "operators": documented_operators(args.docs_dir),
                     "document_sha256": documentation_hashes(args.docs_dir), "syntax": documented_syntax(args.docs_dir)}
        inventory_path.write_text(json.dumps(inventory, indent=2) + "\n")
    inventory = json.loads(inventory_path.read_text())
    if args.docs_dir and (documented_functions(args.docs_dir) != inventory["functions"]
                          or documented_topics(args.docs_dir) != inventory["topics"]
                          or documented_operators(args.docs_dir) != inventory["operators"]
                          or documentation_hashes(args.docs_dir) != inventory["document_sha256"]
                          or documented_syntax(args.docs_dir) != inventory["syntax"]):
        raise SystemExit("SQL docs inventory changed; refresh it and add explicit coverage")
    # Every explicitly documented alias receives its own executable query.
    for entry in inventory["functions"]:
        name, canonical = entry["name"], entry["canonical"]
        for catalog in [SCALARS, AGGREGATES]:
            if canonical in catalog and name not in catalog:
                catalog[name] = re.sub(r"\b" + re.escape(canonical) + r"\s*\(", name + "(", catalog[canonical], count=1)
    missing_topics = set(inventory["topics"]) - TOPICS.keys()
    missing_operators = set(inventory["operators"]) - OPERATORS.keys()
    if missing_topics or missing_operators:
        raise SystemExit(f"Uncovered docs: topics={missing_topics}, operators={missing_operators}")
    root = HERE.parents[2]
    for feature, (fixture, _) in EXTERNAL.items():
        if not (root / fixture).exists():
            raise SystemExit(f"{feature}: fixture does not exist: {fixture}")
    missing = []
    for entry in inventory["functions"]:
        name = entry["canonical"]
        if name not in SCALARS and name not in AGGREGATES and name not in EXTERNAL:
            missing.append(entry)
    if missing:
        raise SystemExit("Missing explicit SQL cases: " + ", ".join(f"{e['page']}/{e['name']}" for e in missing))
    cases = list(scalar_cases()) + list(aggregate_cases()) + list(syntax_cases())
    case_ids = [name for name, _, _ in cases]
    for feature in inventory["syntax"]["select_features"]:
        pattern = SELECT_FEATURES.get(feature)
        if pattern is None or not any(fnmatch.fnmatchcase(case, pattern) for case in case_ids):
            raise SystemExit(f"Missing SELECT feature case: {feature}")
    explain_statements = [sql for name, sql, _ in cases if name.startswith("syntax.explain")]
    explain_statements += [step["query"] for step in json.loads(explain_fixture())["steps"] if "query" in step]
    for option in inventory["syntax"]["explain_options"]:
        if not any(re.search(r"\b" + option + r"\b", sql.upper()) for sql in explain_statements):
            raise SystemExit(f"Missing EXPLAIN option case: {option}")
    known_path = HERE / "sql-surface-known-failures.json"
    known = json.loads(known_path.read_text())
    stale = set(known) - {name for name, _, _ in cases}
    if stale:
        raise SystemExit(f"Known-case identifiers no longer exist: {stale}")
    passing_cases = [case for case in cases if case[0] not in known]
    known_cases = [case for case in cases if case[0] in known]
    write_or_check(HERE / "sql-surface.sql", generated_sql(passing_cases), args.check)
    write_or_check(HERE / "sql-surface-known.sql", generated_sql(known_cases), args.check)
    known_script = HERE.parent / "known-failing/sql-protocols/sql-surface.yaml"
    if known_cases:
        # Use the identical input data and runtime settings as the main suite.
        main_script = (HERE.parent / "query-protocols.yaml").read_text()
        setup = main_script.split("  - corpus:", 1)[0]
        setup = setup.replace("query-protocols", "sql-surface-known")
        setup = re.sub(r"description: >.*?(?=\nsources:)",
                       "description: >\n  Generated documented-SQL cases with reproduced errors or schema mismatches.\n  These cases remain executable until their tracked failures are resolved.\n",
                       setup, flags=re.S)
        setup += "  - corpus: ../../corpus/sql-surface-known.sql\n    protocols: [flightsql, http]\n    expect: {}\n"
        write_or_check(known_script, setup, args.check)
    elif known_script.exists():
        if args.check:
            raise SystemExit(f"{known_script}: stale; run python3 {HERE / 'sql_surface.py'}")
        known_script.unlink()
    write_or_check(HERE.parent / "sql-explain-protocols.yaml", explain_fixture(), args.check)
    write_or_check(HERE.parent / "sql-operator-values.yaml", operator_value_fixture(), args.check)
    write_or_check(HERE.parent / "known-failing/sql-protocols/operator-values.yaml", operator_value_fixture(True), args.check)
    coverage = {"documentation_revision": inventory["revision"], "topics": TOPICS,
                "select_features": SELECT_FEATURES, "explain_options": inventory["syntax"]["explain_options"],
                "fixture_dependent": {name: {"fixture": path, "reason": reason}
                                      for name, (path, reason) in EXTERNAL.items()},
                "case_ids": [name for name, _, _ in cases], "known_failures": known,
                "known_operator_values": json.loads((HERE / "sql-surface-known-values.json").read_text())}
    write_or_check(HERE / "sql-surface-coverage.json", json.dumps(coverage, indent=2) + "\n", args.check)
    print(f"{len(inventory['functions'])} documentation entries ({len({e['name'] for e in inventory['functions']})} distinct function names/aliases) accounted for; {len(cases)} deterministic protocol cases ({len(known_cases)} tracked failures)")


if __name__ == "__main__":
    main()
