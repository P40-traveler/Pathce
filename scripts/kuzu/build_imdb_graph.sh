#!/bin/bash
set -eu
set -o pipefail

# Build kuzu database from IMDB dataset

workspace=$(realpath $(dirname $0)/../../)
dataset=$workspace/datasets/imdb/imdb
schema=$workspace/schemas/imdb/imdb_pathce_schema.json
output=$workspace/graphs/imdb/kuzu/imdb

mkdir -p $workspace/graphs/imdb/kuzu/
rm -rf $output
$workspace/tools/kuzu/create_database.py -d $dataset -s $schema -o $output
