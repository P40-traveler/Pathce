#!/bin/bash
set -eu
set -o pipefail

pattern_dir=$(realpath $1)

workspace=$(realpath $(dirname $0)/../../)

lens=""
patterns=$(find $pattern_dir -name '*.json' -type f | sort)
for pattern in $patterns; do
    len=$($workspace/scripts/pathce/pattern_statistics.sh $pattern)
    lens="$len $lens"
done
echo $lens |  python3 -c "import statistics as stat; i = [float(l.strip()) for l in input().split(' ')]; print('min:', min(i), ' max: ', max(i), ' avg: ', stat.mean(i), ' median: ', stat.median(i))"