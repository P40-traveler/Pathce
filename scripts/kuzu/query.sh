#!/bin/bash
set -eu
set -o pipefail

# Compute the true cardinality of a given pattern
# Argument: db, pathce_schema, pattern, 

db=$(realpath $1)
schema=$(realpath $2)
pattern=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)

$workspace/tools/kuzu/query.py -p $pattern -d $db -s $schema
