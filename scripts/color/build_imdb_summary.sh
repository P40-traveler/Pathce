#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
graph=$workspace/datasets/imdb/imdb.txt
output_dir=$workspace/catalogs/imdb/color
mkdir -p $output_dir
output=$output_dir/imdb_mix_6_50000.obj
julia --project=$workspace/color $workspace/color/scripts/build.jl -d $graph -o $output
