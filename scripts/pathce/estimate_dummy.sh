#!/bin/bash
set -eu
set -o pipefail

catalog=$(realpath $1)
pattern=$(realpath $2)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce estimate -c $catalog --max-path-length 1 --max-star-degree 0 --max-star-length 0 --disable-prune -p $pattern
