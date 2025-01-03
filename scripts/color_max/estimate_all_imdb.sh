#!/bin/bash
set -u
set -o pipefail

pattern_dir=$(realpath $1)
mkdir -p $2
output_dir=$(realpath $2)

workspace=$(realpath $(dirname $0)/../../)

summary=$workspace/catalogs/imdb/color_max/imdb_mix_6_50000.obj

patterns=$(find $pattern_dir -name '*.json' -type f | sort)
for pattern in $patterns; do
    count_time=$($workspace/scripts/color_max/estimate.sh $summary $pattern)
    IFS="," read -r count time <<< "$count_time"
    echo "$pattern: $count, $time"
    filename=$(basename $pattern)
    jq ".count=$count" $pattern > $output_dir/$filename.tmp
    mv $output_dir/$filename.tmp $output_dir/$filename
done