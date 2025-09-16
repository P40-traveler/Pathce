#!/bin/bash
set -eu
set -o pipefail

graph=$(realpath $1)
schema=$(realpath $2)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce graph -g $graph -s $schema