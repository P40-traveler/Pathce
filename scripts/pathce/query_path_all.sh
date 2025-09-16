#!/bin/bash
set -eu
set -o pipefail

# Compute the true cardinalities of all patterns in a given directory, and write the results into a new directory.
# Argument: graph, pattern_dir, output_dir 

graph=$(realpath $1)
pattern_dir=$(realpath $2)
mkdir -p $3
output_dir=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)

patterns=$(find $pattern_dir -name '*.json' -type f | sort)
for pattern in $patterns; do
    count=$($workspace/scripts/pathce/query_path.sh $graph $pattern)
    echo "$pattern: $count"
    filename=$(basename $pattern)
    jq ".count=$count" $pattern > $output_dir/$filename.tmp
    mv $output_dir/$filename.tmp $output_dir/$filename
done
