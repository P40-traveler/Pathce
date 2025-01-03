#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
graph=$workspace/datasets/imdb/imdb.txt
output_dir=$workspace/catalogs/imdb/color_max
mkdir -p $output_dir
output=$output_dir/imdb_mix_6_50000.obj
julia --project=$workspace/color $workspace/color/scripts/build_max.jl -d $graph -o $output
