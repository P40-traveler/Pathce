#!/bin/bash
set -eu
set -o pipefail

# Compute the true cardinality of a given pattern
# Argument: db, pathce_schema, pattern, 

db=$(realpath $1)
schema=$(realpath $2)
pattern=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)
duckdb=$workspace/tools/duckdb

sql=$($workspace/tools/pattern2sql.py -p $pattern -s $schema)
$duckdb $db -readonly -csv -noheader <<EOF
$sql
EOF
