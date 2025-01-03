#!/bin/bash
set -eu
set -o pipefail

pattern=$(realpath $1)

workspace=$(realpath $(dirname $0)/../../)

$workspace/pathce/target/release/pathce check -p $pattern
