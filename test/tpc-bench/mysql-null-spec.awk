# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Print the column spec a MySQL `LOAD DATA` needs so that an empty field becomes
# NULL in the columns where it can only mean NULL, and stays an empty string in
# the columns where it might not.
#
# Reads `column_name<TAB>data_type` on stdin, in ordinal position order — i.e. the
# output of
#
#   SELECT column_name, data_type FROM information_schema.columns
#    WHERE table_schema='<db>' AND table_name='<t>' ORDER BY ordinal_position;
#
# and writes one line, e.g.
#
#   (@v1,c_customer_id,@v3) SET c_customer_sk=NULLIF(@v1,''),c_current_cdemo_sk=NULLIF(@v3,'')
#
# Why this is type-directed rather than a rewrite of the data file. The bench
# files spell a NULL as an empty field, and `LOAD DATA` reads an empty field as
# the column type's zero value, so a NULL foreign key arrives as 0 (#13152). The
# tempting repair is to rewrite every empty field to `\N` before loading, but the
# file cannot distinguish a NULL from an empty string — dsdgen quotes nothing —
# so that rewrite nulls legitimate empty strings too. Measured on DuckDB's
# `dsdgen(sf=0.01)`: `time_dim.t_meal_time` is 50,400 empty strings and *zero*
# NULLs, all of which a blanket rewrite would turn into NULL.
#
# The column's type is the missing information, and MySQL already has it. An
# empty field cannot be a valid INTEGER/DECIMAL/DATE, so there it can only mean
# NULL; in a CHAR/VARCHAR it is ambiguous, and this leaves it alone rather than
# guessing. String columns therefore bind positionally and are untouched.
#
# Unit-tested by `scripts/test_check_bench_mysql_load_nulls.py`, which runs this
# program over a fixture listing — no database required.

BEGIN { FS = "\t"; QUOTE = "\047\047" }   # \047 is a single quote; QUOTE is ''

# A type whose values are text, where an empty field is a legitimate empty string
# and must not be read as NULL.
$2 ~ /char|text|binary|blob|enum|set|json/ {
    columns = columns sep $1
    sep = ","
    next
}

# Everything else — numeric, date, time, timestamp, year, bit. An empty field is
# not a valid value of any of these, so it can only mean NULL.
{
    columns = columns sep "@v" NR
    assignments = assignments asep sprintf("%s=NULLIF(@v%d,%s)", $1, NR, QUOTE)
    sep = ","
    asep = ","
}

END {
    if (columns == "") exit 0
    printf "(%s)%s", columns, (assignments ? " SET " assignments : "")
}
