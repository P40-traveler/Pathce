#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
graph=$workspace/datasets/aids_merged/aids_merged.txt
output_dir=$workspace/catalogs/aids_merged/color
mkdir -p $output_dir
output=$output_dir/aids_merged_mix_6_50000.obj
julia --project=$workspace/color $workspace/color/scripts/build.jl -d $graph -o $output
