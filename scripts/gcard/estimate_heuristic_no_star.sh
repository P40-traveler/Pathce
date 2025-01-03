#!/bin/bash
set -eu
set -o pipefail

# Estimate the cardinality of a given pattern
# Argument: catalog, pattern 

catalog=$(realpath $1)
k=$2
d=$3
pattern=$(realpath $4)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce estimate -c $catalog -p $pattern --max-path-length $k --max-star-degree $d --disable-star