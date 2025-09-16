#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
dataset=$workspace/datasets/aids/aids
schema=$workspace/schemas/aids/aids_pathce_schema.json
output_dir=$workspace/graphs/aids/pathce
mkdir -p $output_dir
output=$output_dir/aids.bincode

$workspace/pathce/target/release/pathce serialize -i $dataset -s $schema -o $output
