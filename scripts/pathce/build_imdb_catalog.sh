#!/bin/bash
set -eu
set -o pipefail

threads=$1
k=$2
d=$3
m=$4

workspace=$(realpath $(dirname $0)/../../)
output_dir=$workspace/catalogs/imdb/pathce
mkdir -p $output_dir
output=$output_dir/imdb_"$k"_"$d"_"$m"

$workspace/pathce/target/release/pathce analyze -s $workspace/schemas/imdb/imdb_pathce_schema.json -g $workspace/graphs/imdb/pathce/imdb.bincode --greedy -t $threads -o $output --buckets $m --max-path-length $k --max-star-degree $d
