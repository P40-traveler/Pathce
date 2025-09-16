#!/bin/bash
set -eu
set -o pipefail

# Estimate the cardinality of a given pattern
# Argument: catalog, pattern 

catalog=$(realpath $1)
pattern=$(realpath $2)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce estimate-manual -c $catalog -p $pattern