#!/bin/bash
set -eu
set -o pipefail

catalog=$(realpath $1)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce show -c $catalog