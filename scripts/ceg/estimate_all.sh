#!/bin/bash
set -u
set -o pipefail

catalog=$(realpath $1)
schema=$(realpath $2)
pattern_dir=$(realpath $3)
mkdir -p $4
output_dir=$(realpath $4)

workspace=$(realpath $(dirname $0)/../../)

patterns=$(find $pattern_dir -name '*.json' -type f | sort)
for pattern in $patterns; do
    count_time=$($workspace/scripts/ceg/estimate.sh $catalog $schema $pattern)
    IFS="," read -r count time <<< "$count_time"
    echo "$pattern: $count, $time"
    filename=$(basename $pattern)
    jq ".count=$count" $pattern > $output_dir/$filename.tmp
    mv $output_dir/$filename.tmp $output_dir/$filename
done
