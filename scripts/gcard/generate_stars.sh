#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
schema=$1
length=$2
limit=$3
output=$4

$workspace/pathce/target/release/pathce generate-patterns -s $schema -l $length -t star --limit $limit -o $output
