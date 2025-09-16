#!/bin/bash
set -eu
set -o pipefail

catalog=$(realpath $1)
pattern_dir=$(realpath $2)
mkdir -p $3
output_dir=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)

patterns=$(find $pattern_dir -name '*.json' -type f | sort)
patterns_paths=()
pattern_arg_list=""
for path in $patterns; do
    patterns_paths+=($path)
    pattern_arg_list="$pattern_arg_list -p $path"
done
num_patterns=${#patterns_paths[@]}
output=$($workspace/pathce/target/release/pathce estimate -c $catalog --max-path-length 1 --max-star-degree 0 --max-star-length 0 --disable-prune $pattern_arg_list)

i=0
while IFS=" " read -r line; do
    IFS="," read -r count time <<<"$line"
    path=${patterns_paths[$i]}
    echo "$path: $count, $time"
    filename=$(basename $path)
    jq ".count=$count" $path >$output_dir/$filename.tmp
    mv $output_dir/$filename.tmp $output_dir/$filename
    i=$((i + 1))
done <<<"$output"
