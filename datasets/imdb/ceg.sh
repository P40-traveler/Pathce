#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
basedir=$(dirname $(realpath $0))
schema=$workspace/schemas/imdb/imdb_pathce_schema.json
$workspace/tools/merge_csv.py -d $basedir/imdb -s $schema -o $basedir/imdb.csv