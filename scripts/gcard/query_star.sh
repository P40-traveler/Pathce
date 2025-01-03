#!/bin/bash
set -eu
set -o pipefail

# Compute the true cardinality of a given pattern
# Argument: graph, pattern, 

graph=$(realpath $1)
pattern=$(realpath $2)
threads=$3

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce count -g $graph -p $pattern -t $threads -s star
