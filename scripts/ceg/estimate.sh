#!/bin/bash
set -eu
set -o pipefail

# Estimate the cardinality of a given pattern with CEG
# Argument: glogs_catalog, glogs_schema, pathce_pattern, output

catalog=$(realpath $1)
schema=$(realpath $2)
pattern=$(realpath $3)

workspace=$(realpath $(dirname $0)/../../)

ty=$($workspace/scripts/pathce/check_pattern.sh $pattern)

if [ $ty == "vertex" ]; then
    $workspace/scripts/glogs/estimate.sh $catalog $pattern
elif [ $ty == "edge" ]; then
    $workspace/scripts/ceg/estimate_acyclic.sh $catalog $schema $pattern 1
elif [ $ty == "acyclic" ]; then
    $workspace/scripts/ceg/estimate_acyclic.sh $catalog $schema $pattern 2
elif [ $ty == "cyclic" ]; then
    $workspace/scripts/ceg/estimate_cyclic_large.sh $catalog $schema $pattern 2
else
    echo "invalid pattern: $pattern"
    exit 1
fi
