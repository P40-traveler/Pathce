#!/bin/bash
set -eu
set -o pipefail

catalog=$(realpath $1)
schema=$(realpath $2)
pattern=$(realpath $3)
k=$4

workspace=$(realpath $(dirname $0)/../../)

pattern_name=$(basename $pattern)

uuid=$(uuid)
dir=$workspace/ceg/ceg-$pattern_name-$uuid
mkdir $dir
cd $workspace/ceg/

# Transform pattern format
$workspace/tools/pattern2ceg.py -p $pattern -o $dir/pattern.csv

./runCyclic $dir/pattern.csv $catalog $schema $dir/result.csv $k > /dev/null

# Transform result format
$workspace/tools/print_ceg_result.py -i $dir/result.csv -t acyclic

# Clean up
rm -rf $dir