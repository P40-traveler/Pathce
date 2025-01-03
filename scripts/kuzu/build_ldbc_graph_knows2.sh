#!/bin/bash
set -eu
set -o pipefail

# Build kuzu database from IMDB dataset
sf=$1

workspace=$(realpath $(dirname $0)/../../)
dataset=$workspace/datasets/ldbc/sf$sf
schema=$workspace/schemas/ldbc/ldbc_pathce_schema_knows2.json
output=$workspace/graphs/ldbc/kuzu/ldbc_"sf$sf"_knows2

mkdir -p $workspace/graphs/ldbc/kuzu/
rm -rf $output
$workspace/tools/kuzu/create_database.py -d $dataset -s $schema -o $output
