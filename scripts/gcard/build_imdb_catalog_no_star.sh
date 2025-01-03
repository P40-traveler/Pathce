#!/bin/bash
set -eu
set -o pipefail

threads=$1
k=$2
l=$3

workspace=$(realpath $(dirname $0)/../../)
output_dir=$workspace/catalogs/imdb/pathce
mkdir -p $output_dir
output=$output_dir/imdb_"$k"_"$l"_no_star

$workspace/pathce/target/release/pathce analyze -s $workspace/schemas/imdb/imdb_pathce_schema.json -g $workspace/graphs/imdb/pathce/imdb.bincode --extend --greedy -t $threads -o $output --base-length $k --max-length $l --ignore-internal-mvertex
