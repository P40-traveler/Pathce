#!/bin/bash
set -eu
set -o pipefail

sf=$1
threads=$2

workspace=$(realpath $(dirname $0)/../../)
output_dir=$workspace/catalogs/ldbc/glogs
mkdir -p $output_dir
output=$output_dir/ldbc_sf$sf.bincode

SCHEMA_PATH=$workspace/schemas/ldbc/ldbc_glogs_schema.json \
SAMPLE_PATH=$workspace/graphs/ldbc/glogs/ldbc_sf$sf \
$workspace/glogs/ir/target/release/build_catalog -m from_meta -t $threads -p $output