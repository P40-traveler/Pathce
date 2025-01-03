#!/bin/bash
set -eu
set -o pipefail

# Build kuzu database from IMDB dataset
sf=$1

workspace=$(realpath $(dirname $0)/../../)
dataset=$workspace/datasets/ldbc/sf$sf
schema=$workspace/schemas/ldbc/ldbc_pathce_schema.json
output=$workspace/graphs/ldbc/kuzu/ldbc_sf$sf

mkdir -p $workspace/graphs/ldbc/kuzu/
rm -rf $output
$workspace/tools/kuzu/create_database.py -d $dataset -s $schema -o $output
