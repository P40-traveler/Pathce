#!/bin/bash
set -eu
set -o pipefail

graph_csv=$(realpath $1)
pattern=$(realpath $2)

workspace=$(realpath $(dirname $0)/../../)

pattern_name=$(basename $pattern)

uuid=$(uuid)
dir=$workspace/ceg/ceg-$pattern_name-$uuid
mkdir $dir
cd $workspace/ceg/

# Transform pattern format
$workspace/tools/pattern2ceg.py -p $pattern -o $dir/pattern.csv

./runCyclicOld $dir/pattern.csv $graph_csv $dir/result.csv > /dev/null

# Transform result format
$workspace/tools/print_ceg_result.py -i $dir/result.csv -t cyclic

# Clean up
rm -rf $dir