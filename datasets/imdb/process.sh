#!/bin/bash
set -eu
set -o pipefail

workspace=$(realpath $(dirname $0)/../../)
basedir=$(dirname $(realpath $0))
echo "Convert dataset format..."
$workspace/tools/convert_imdb.py -d $basedir/imdb -o $basedir/imdb_tmp
rm -r $basedir/imdb
mv $basedir/imdb_tmp $basedir/imdb

echo "Convert unique vertex id..."
schema=$workspace/schemas/imdb/imdb_pathce_schema.json
$workspace/tools/unique_vid.py -s $schema -d $basedir/imdb
