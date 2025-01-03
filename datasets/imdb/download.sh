#!/bin/bash
set -eu
set -o pipefail

basedir=$(dirname $(realpath $0))
if ! [ -f $basedir/imdb.tgz ]; then
    wget -O $basedir/imdb.tgz http://homepages.cwi.nl/~boncz/job/imdb.tgz
fi
mkdir -p $basedir/imdb
tar -xzvf $basedir/imdb.tgz -C $basedir/imdb