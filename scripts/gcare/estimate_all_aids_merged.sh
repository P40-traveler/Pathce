#!/bin/bash
set -u
set -o pipefail

method=$1
pattern_dir=$(realpath $2)
mkdir -p $3
output_dir=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)

graph_dir=$workspace/datasets/aids_merged/aids_merged

patterns=$(find $pattern_dir -name '*.json' -type f | sort)
for pattern in $patterns; do
    count_time=$(timeout -v 11m $workspace/scripts/gcare/estimate.sh $method $graph_dir $pattern)
    IFS="," read -r count time <<< "$count_time"
    echo "$pattern: $count, $time"
    filename=$(basename $pattern)
    jq ".count=$count" $pattern > $output_dir/$filename.tmp
    mv $output_dir/$filename.tmp $output_dir/$filename
done