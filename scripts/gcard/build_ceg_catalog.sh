#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
graph=$(realpath $1)
schema=$(realpath $2)
decom=$(realpath $3)
output=$(realpath -m $4)

$workspace/pathce/target/release/pathce build-ceg-catalog -g $graph -s $schema -d $decom -o $output
